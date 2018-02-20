// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/row-batch.h"

#include <stdint.h> // for intptr_t
#include <memory>
#include <boost/scoped_ptr.hpp>

#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/compress.h"
#include "util/debug-util.h"
#include "util/decompress.h"
#include "util/fixed-size-hash-table.h"
#include "util/scope-exit-trigger.h"

#include "gen-cpp/Results_types.h"
#include "gen-cpp/row_batch.pb.h"

#include "common/names.h"

namespace impala {

const int RowBatch::AT_CAPACITY_MEM_USAGE;
const int RowBatch::FIXED_LEN_BUFFER_LIMIT;

RowBatch::RowBatch(const RowDescriptor* row_desc, int capacity, MemTracker* mem_tracker)
  : num_rows_(0),
    capacity_(capacity),
    flush_(FlushMode::NO_FLUSH_RESOURCES),
    needs_deep_copy_(false),
    num_tuples_per_row_(row_desc->tuple_descriptors().size()),
    tuple_ptrs_size_(capacity * num_tuples_per_row_ * sizeof(Tuple*)),
    attached_buffer_bytes_(0),
    tuple_data_pool_(mem_tracker),
    row_desc_(row_desc),
    mem_tracker_(mem_tracker) {
  DCHECK(mem_tracker_ != NULL);
  DCHECK_GT(capacity, 0);
  DCHECK_GT(tuple_ptrs_size_, 0);
  // TODO: switch to Init() pattern so we can check memory limit and return Status.
  mem_tracker_->Consume(tuple_ptrs_size_);
  tuple_ptrs_ = reinterpret_cast<Tuple**>(malloc(tuple_ptrs_size_));
  DCHECK(tuple_ptrs_ != NULL);
}

// TODO: we want our input_batch's tuple_data to come from our (not yet implemented)
// global runtime memory segment; how do we get thrift to allocate it from there?
// maybe change line (in Data_types.cc generated from Data.thrift)
//              xfer += iprot->readString(this->tuple_data[_i9]);
// to allocated string data in special mempool
// (change via python script that runs over Data_types.cc)
RowBatch::RowBatch(
    const RowDescriptor* row_desc, const TRowBatch& input_batch, MemTracker* mem_tracker)
  : num_rows_(input_batch.num_rows),
    capacity_(input_batch.num_rows),
    flush_(FlushMode::NO_FLUSH_RESOURCES),
    needs_deep_copy_(false),
    num_tuples_per_row_(input_batch.row_tuples.size()),
    tuple_ptrs_size_(capacity_ * num_tuples_per_row_ * sizeof(Tuple*)),
    attached_buffer_bytes_(0),
    tuple_data_pool_(mem_tracker),
    row_desc_(row_desc),
    mem_tracker_(mem_tracker) {
  DCHECK(mem_tracker_ != nullptr);
  DCHECK_EQ(num_tuples_per_row_, row_desc_->tuple_descriptors().size());
  DCHECK_GT(tuple_ptrs_size_, 0);
  kudu::Slice input_tuple_data =
      kudu::Slice(input_batch.tuple_data.c_str(), input_batch.tuple_data.size());
  kudu::Slice input_tuple_offsets = kudu::Slice(
      reinterpret_cast<const char*>(input_batch.tuple_offsets.data()),
      input_batch.tuple_offsets.size() * sizeof(int32_t));
  const THdfsCompression::type& compression_type = input_batch.compression_type;
  DCHECK(compression_type == THdfsCompression::NONE ||
      compression_type == THdfsCompression::LZ4)
      << "Unexpected compression type: " << input_batch.compression_type;

  mem_tracker_->Consume(tuple_ptrs_size_);
  tuple_ptrs_ = reinterpret_cast<Tuple**>(malloc(tuple_ptrs_size_));
  DCHECK(tuple_ptrs_ != nullptr) << "Failed to allocate tuple pointers";

  const uint64_t uncompressed_size = input_batch.uncompressed_size;
  uint8_t* tuple_data = tuple_data_pool_.Allocate(uncompressed_size);
  DCHECK(tuple_data != nullptr) << "Failed to allocate tuple data";

  Deserialize(input_tuple_offsets, input_tuple_data, uncompressed_size,
      compression_type == THdfsCompression::LZ4, tuple_data);
}

RowBatch::RowBatch(const RowDescriptor* row_desc, const RowBatchHeaderPB& header,
    MemTracker* mem_tracker)
  : num_rows_(0),
    capacity_(0),
    flush_(FlushMode::NO_FLUSH_RESOURCES),
    needs_deep_copy_(false),
    num_tuples_per_row_(header.num_tuples_per_row()),
    tuple_ptrs_size_(header.num_rows() * num_tuples_per_row_ * sizeof(Tuple*)),
    attached_buffer_bytes_(0),
    tuple_data_pool_(mem_tracker),
    row_desc_(row_desc),
    mem_tracker_(mem_tracker) {
  DCHECK(mem_tracker_ != nullptr);
  DCHECK_EQ(num_tuples_per_row_, row_desc_->tuple_descriptors().size());
  DCHECK_GT(tuple_ptrs_size_, 0);
}

void RowBatch::Deserialize(const kudu::Slice& input_tuple_offsets,
    const kudu::Slice& input_tuple_data, int64_t uncompressed_size,
    bool is_compressed, uint8_t* tuple_data) {
  DCHECK(tuple_ptrs_ != nullptr);
  DCHECK(tuple_data != nullptr);
  if (is_compressed) {
    // Decompress tuple data into data pool
    const uint8_t* compressed_data = input_tuple_data.data();
    size_t compressed_size = input_tuple_data.size();

    Lz4Decompressor decompressor(nullptr, false);
    Status status = decompressor.Init();
    DCHECK(status.ok()) << status.GetDetail();
    auto compressor_cleanup =
        MakeScopeExitTrigger([&decompressor]() { decompressor.Close(); });

    status = decompressor.ProcessBlock(
        true, compressed_size, compressed_data, &uncompressed_size, &tuple_data);
    DCHECK_NE(uncompressed_size, -1) << "RowBatch decompression failed";
    DCHECK(status.ok()) << "RowBatch decompression failed.";
  } else {
    // Tuple data uncompressed, copy directly into data pool
    DCHECK_EQ(uncompressed_size, input_tuple_data.size());
    memcpy(tuple_data, input_tuple_data.data(), input_tuple_data.size());
  }

  // Convert input_batch.tuple_offsets into pointers
  const int32_t* tuple_offsets =
      reinterpret_cast<const int32_t*>(input_tuple_offsets.data());
  DCHECK_EQ(input_tuple_offsets.size() % sizeof(int32_t), 0);
  int num_tuples = input_tuple_offsets.size() / sizeof(int32_t);
  for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
    int32_t offset = tuple_offsets[tuple_idx];
    if (offset == -1) {
      tuple_ptrs_[tuple_idx] = nullptr;
    } else {
      tuple_ptrs_[tuple_idx] = reinterpret_cast<Tuple*>(tuple_data + offset);
    }
  }

  // Check whether we have slots that require offset-to-pointer conversion.
  if (!row_desc_->HasVarlenSlots()) return;

  // For every unique tuple, convert string offsets contained in tuple data into
  // pointers. Tuples were serialized in the order we are deserializing them in,
  // so the first occurrence of a tuple will always have a higher offset than any
  // tuple we already converted.
  Tuple* last_converted = nullptr;
  for (int i = 0; i < num_rows_; ++i) {
    for (int j = 0; j < num_tuples_per_row_; ++j) {
      const TupleDescriptor* desc = row_desc_->tuple_descriptors()[j];
      if (!desc->HasVarlenSlots()) continue;
      Tuple* tuple = GetRow(i)->GetTuple(j);
      // Handle NULL or already converted tuples with one check.
      if (tuple <= last_converted) continue;
      last_converted = tuple;
      tuple->ConvertOffsetsToPointers(*desc, tuple_data);
    }
  }
}

Status RowBatch::FromProtobuf(const RowDescriptor* row_desc,
    const RowBatchHeaderPB& header, const kudu::Slice& input_tuple_offsets,
    const kudu::Slice& input_tuple_data, MemTracker* mem_tracker,
    BufferPool::ClientHandle* client, unique_ptr<RowBatch>* row_batch_ptr) {
  unique_ptr<RowBatch> row_batch(new RowBatch(row_desc, header, mem_tracker));

  DCHECK(client != nullptr);
  row_batch->tuple_ptrs_info_.reset(new BufferInfo());
  row_batch->tuple_ptrs_info_->client = client;
  BufferPool::BufferHandle* tuple_ptrs_buffer = &(row_batch->tuple_ptrs_info_->buffer);
  RETURN_IF_ERROR(
      row_batch->AllocateBuffer(client, row_batch->tuple_ptrs_size_, tuple_ptrs_buffer));
  row_batch->tuple_ptrs_ = reinterpret_cast<Tuple**>(tuple_ptrs_buffer->data());

  const int64_t uncompressed_size = header.uncompressed_size();
  BufferPool::BufferHandle tuple_data_buffer;
  RETURN_IF_ERROR(
      row_batch->AllocateBuffer(client, uncompressed_size, &tuple_data_buffer));
  uint8_t* tuple_data = tuple_data_buffer.data();
  row_batch->AddBuffer(client, move(tuple_data_buffer), FlushMode::NO_FLUSH_RESOURCES);

  row_batch->num_rows_ = header.num_rows();
  row_batch->capacity_ = header.num_rows();
  const CompressionType& compression_type = header.compression_type();
  DCHECK(compression_type == CompressionType::NONE ||
      compression_type == CompressionType::LZ4)
      << "Unexpected compression type: " << compression_type;
  row_batch->Deserialize(input_tuple_offsets, input_tuple_data, uncompressed_size,
      compression_type == CompressionType::LZ4, tuple_data);
  *row_batch_ptr = std::move(row_batch);
  return Status::OK();
}

RowBatch::~RowBatch() {
  tuple_data_pool_.FreeAll();
  FreeBuffers();
  if (tuple_ptrs_info_.get() != nullptr) {
    ExecEnv::GetInstance()->buffer_pool()->FreeBuffer(
        tuple_ptrs_info_->client, &(tuple_ptrs_info_->buffer));
  } else {
    DCHECK(tuple_ptrs_ != nullptr);
    free(tuple_ptrs_);
    mem_tracker_->Release(tuple_ptrs_size_);
  }
  tuple_ptrs_ = nullptr;
}

Status RowBatch::Serialize(TRowBatch* output_batch) {
  return Serialize(output_batch, UseFullDedup());
}

Status RowBatch::Serialize(TRowBatch* output_batch, bool full_dedup) {
  // why does Thrift not generate a Clear() function?
  output_batch->row_tuples.clear();
  output_batch->tuple_offsets.clear();
  int64_t uncompressed_size;
  bool is_compressed;
  RETURN_IF_ERROR(Serialize(full_dedup, &output_batch->tuple_offsets,
      &output_batch->tuple_data, &uncompressed_size, &is_compressed));
  // TODO: max_size() is much larger than the amount of memory we could feasibly
  // allocate. Need better way to detect problem.
  DCHECK_LE(uncompressed_size, output_batch->tuple_data.max_size());
  output_batch->__set_num_rows(num_rows_);
  output_batch->__set_uncompressed_size(uncompressed_size);
  output_batch->__set_compression_type(
      is_compressed ? THdfsCompression::LZ4 : THdfsCompression::NONE);
  row_desc_->ToThrift(&output_batch->row_tuples);
  return Status::OK();
}

Status RowBatch::Serialize(OutboundRowBatch* output_batch) {
  int64_t uncompressed_size;
  bool is_compressed;
  output_batch->tuple_offsets_.clear();
  RETURN_IF_ERROR(Serialize(UseFullDedup(), &output_batch->tuple_offsets_,
      &output_batch->tuple_data_, &uncompressed_size, &is_compressed));

  // Initialize the RowBatchHeaderPB
  RowBatchHeaderPB* header = &output_batch->header_;
  header->Clear();
  header->set_num_rows(num_rows_);
  header->set_num_tuples_per_row(row_desc_->tuple_descriptors().size());
  header->set_uncompressed_size(uncompressed_size);
  header->set_compression_type(
      is_compressed ? CompressionType::LZ4 : CompressionType::NONE);
  return Status::OK();
}

Status RowBatch::Serialize(bool full_dedup, vector<int32_t>* tuple_offsets,
    string* tuple_data, int64_t* uncompressed_size, bool* is_compressed) {
  // As part of the serialization process we deduplicate tuples to avoid serializing a
  // Tuple multiple times for the RowBatch. By default we only detect duplicate tuples
  // in adjacent rows only. If full deduplication is enabled, we will build a
  // map to detect non-adjacent duplicates. Building this map comes with significant
  // overhead, so is only worthwhile in the uncommon case of many non-adjacent duplicates.
  int64_t size;
  if (full_dedup) {
    // Maps from tuple to offset of its serialized data in output_batch->tuple_data.
    DedupMap distinct_tuples;
    RETURN_IF_ERROR(distinct_tuples.Init(num_rows_ * num_tuples_per_row_ * 2, 0));
    size = TotalByteSize(&distinct_tuples);
    distinct_tuples.Clear(); // Reuse allocated hash table.
    RETURN_IF_ERROR(SerializeInternal(size, &distinct_tuples, tuple_offsets, tuple_data));
  } else {
    size = TotalByteSize(nullptr);
    RETURN_IF_ERROR(SerializeInternal(size, nullptr, tuple_offsets, tuple_data));
  }
  *uncompressed_size = size;
  *is_compressed = false;

  if (size > 0) {
    // Try compressing tuple_data to compression_scratch_, swap if compressed data is
    // smaller
    Lz4Compressor compressor(nullptr, false);
    RETURN_IF_ERROR(compressor.Init());
    auto compressor_cleanup =
        MakeScopeExitTrigger([&compressor]() { compressor.Close(); });

    // If the input size is too large for LZ4 to compress, MaxOutputLen() will return 0.
    int64_t compressed_size = compressor.MaxOutputLen(size);
    if (compressed_size == 0) {
      return Status(TErrorCode::LZ4_COMPRESSION_INPUT_TOO_LARGE, size);
    }
    DCHECK_GT(compressed_size, 0);
    if (compression_scratch_.size() < compressed_size) {
      compression_scratch_.resize(compressed_size);
    }
    uint8_t* input = (uint8_t*)tuple_data->c_str();
    uint8_t* compressed_output = (uint8_t*)compression_scratch_.c_str();
    RETURN_IF_ERROR(
        compressor.ProcessBlock(true, size, input, &compressed_size, &compressed_output));
    if (LIKELY(compressed_size < size)) {
      compression_scratch_.resize(compressed_size);
      tuple_data->swap(compression_scratch_);
      *is_compressed = true;
    }
    VLOG_ROW << "uncompressed size: " << size << ", compressed size: " << compressed_size;
  }
  return Status::OK();
}

bool RowBatch::UseFullDedup() {
  // Switch to using full deduplication in cases where severe size blow-ups are known to
  // be common: when a row contains tuples with collections and where there are three or
  // more tuples per row so non-adjacent duplicate tuples may have been created when
  // joining tuples from multiple sources into a single row.
  if (row_desc_->tuple_descriptors().size() < 3) return false;
  vector<TupleDescriptor*>::const_iterator tuple_desc =
      row_desc_->tuple_descriptors().begin();
  for (; tuple_desc != row_desc_->tuple_descriptors().end(); ++tuple_desc) {
    if (!(*tuple_desc)->collection_slots().empty()) return true;
  }
  return false;
}

Status RowBatch::SerializeInternal(int64_t size, DedupMap* distinct_tuples,
    vector<int32_t>* tuple_offsets, string* tuple_data_str) {
  DCHECK(distinct_tuples == nullptr || distinct_tuples->size() == 0);

  // The maximum uncompressed RowBatch size that can be serialized is INT_MAX. This
  // is because the tuple offsets are int32s and will overflow for a larger size.
  if (size > numeric_limits<int32_t>::max()) {
    return Status(TErrorCode::ROW_BATCH_TOO_LARGE, size, numeric_limits<int32_t>::max());
  }

  // TODO: track memory usage
  // TODO: detect if serialized size is too large to allocate and return proper error.
  tuple_data_str->resize(size);
  tuple_offsets->reserve(num_rows_ * num_tuples_per_row_);

  // Copy tuple data of unique tuples, including strings, into output_batch (converting
  // string pointers into offsets in the process).
  int offset = 0; // current offset into output_batch->tuple_data
  char* tuple_data = const_cast<char*>(tuple_data_str->c_str());

  for (int i = 0; i < num_rows_; ++i) {
    vector<TupleDescriptor*>::const_iterator desc =
        row_desc_->tuple_descriptors().begin();
    for (int j = 0; desc != row_desc_->tuple_descriptors().end(); ++desc, ++j) {
      Tuple* tuple = GetRow(i)->GetTuple(j);
      if (UNLIKELY(tuple == nullptr)) {
        // NULLs are encoded as -1
        tuple_offsets->push_back(-1);
        continue;
      } else if (LIKELY(i > 0) && UNLIKELY(GetRow(i - 1)->GetTuple(j) == tuple)) {
        // Fast tuple deduplication for adjacent rows.
        int prev_row_idx = tuple_offsets->size() - num_tuples_per_row_;
        tuple_offsets->push_back((*tuple_offsets)[prev_row_idx]);
        continue;
      } else if (UNLIKELY(distinct_tuples != nullptr)) {
        if ((*desc)->byte_size() == 0) {
          // Zero-length tuples can be represented as nullptr.
          tuple_offsets->push_back(-1);
          continue;
        }
        int* dedupd_offset = distinct_tuples->FindOrInsert(tuple, offset);
        if (*dedupd_offset != offset) {
          // Repeat of tuple
          DCHECK_GE(*dedupd_offset, 0);
          tuple_offsets->push_back(*dedupd_offset);
          continue;
        }
      }
      // Record offset before creating copy (which increments offset and tuple_data)
      tuple_offsets->push_back(offset);
      tuple->DeepCopy(**desc, &tuple_data, &offset, /* convert_ptrs */ true);
      DCHECK_LE(offset, size);
    }
  }
  DCHECK_EQ(offset, size);
  return Status::OK();
}

Status RowBatch::AllocateBuffer(BufferPool::ClientHandle* client, int64_t len,
    BufferPool::BufferHandle* buffer_handle) {
  BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
  int64_t buffer_len = BitUtil::RoundUpToPowerOfTwo(len);
  buffer_len = max(buffer_pool->min_buffer_len(), buffer_len);
  RETURN_IF_ERROR(
      buffer_pool->AllocateUnreservedBuffer(client, buffer_len, buffer_handle));
  if (UNLIKELY(!buffer_handle->is_open())) {
    return mem_tracker_->MemLimitExceeded(
        nullptr, "Failed to allocate row batch", buffer_len);
  }
  return Status::OK();
}

void RowBatch::AddBuffer(BufferPool::ClientHandle* client,
    BufferPool::BufferHandle&& buffer, FlushMode flush) {
  attached_buffer_bytes_ += buffer.len();
  BufferInfo buffer_info;
  buffer_info.client = client;
  buffer_info.buffer = std::move(buffer);
  buffers_.push_back(std::move(buffer_info));
  if (flush == FlushMode::FLUSH_RESOURCES) MarkFlushResources();
}

void RowBatch::FreeBuffers() {
  for (BufferInfo& buffer_info : buffers_) {
    ExecEnv::GetInstance()->buffer_pool()->FreeBuffer(
        buffer_info.client, &buffer_info.buffer);
  }
  buffers_.clear();
}

void RowBatch::Reset() {
  num_rows_ = 0;
  capacity_ = tuple_ptrs_size_ / (num_tuples_per_row_ * sizeof(Tuple*));
  // TODO: Change this to Clear() and investigate the repercussions.
  tuple_data_pool_.FreeAll();
  FreeBuffers();
  attached_buffer_bytes_ = 0;
  flush_ = FlushMode::NO_FLUSH_RESOURCES;
  needs_deep_copy_ = false;
}

void RowBatch::TransferResourceOwnership(RowBatch* dest) {
  dest->tuple_data_pool_.AcquireData(&tuple_data_pool_, false);
  for (BufferInfo& buffer_info : buffers_) {
    dest->AddBuffer(
        buffer_info.client, std::move(buffer_info.buffer), FlushMode::NO_FLUSH_RESOURCES);
  }
  buffers_.clear();
  if (needs_deep_copy_) {
    dest->MarkNeedsDeepCopy();
  } else if (flush_ == FlushMode::FLUSH_RESOURCES) {
    dest->MarkFlushResources();
  }
  Reset();
}

int64_t RowBatch::GetDeserializedSize(const TRowBatch& batch) {
  return batch.uncompressed_size + batch.tuple_offsets.size() * sizeof(Tuple*);
}

int64_t RowBatch::GetSerializedSize(const TRowBatch& batch) {
  int64_t result = batch.tuple_data.size();
  result += batch.row_tuples.size() * sizeof(TTupleId);
  result += batch.tuple_offsets.size() * sizeof(int32_t);
  return result;
}

int64_t RowBatch::GetDeserializedSize(const RowBatchHeaderPB& header,
    const kudu::Slice& tuple_offsets) {
  DCHECK_EQ(tuple_offsets.size() % sizeof(int32_t), 0);
  return header.uncompressed_size() +
      (tuple_offsets.size() / sizeof(int32_t)) * sizeof(Tuple*);
}

int64_t RowBatch::GetDeserializedSize(const OutboundRowBatch& batch) {
  return batch.header_.uncompressed_size() + batch.tuple_offsets_.size() * sizeof(Tuple*);
}

int64_t RowBatch::GetSerializedSize(const OutboundRowBatch& batch) {
  return batch.tuple_data_.size() + batch.tuple_offsets_.size() * sizeof(int32_t);
}

void RowBatch::AcquireState(RowBatch* src) {
  DCHECK(row_desc_->LayoutEquals(*src->row_desc_));
  DCHECK_EQ(num_tuples_per_row_, src->num_tuples_per_row_);
  DCHECK_EQ(tuple_ptrs_size_, src->tuple_ptrs_size_);

  // The destination row batch should be empty.
  DCHECK(!needs_deep_copy_);
  DCHECK_EQ(num_rows_, 0);
  DCHECK_EQ(attached_buffer_bytes_, 0);

  num_rows_ = src->num_rows_;
  capacity_ = src->capacity_;
  // tuple_ptrs_ were allocated with malloc so can be swapped between batches.
  DCHECK(tuple_ptrs_info_.get() == nullptr);
  std::swap(tuple_ptrs_, src->tuple_ptrs_);
  src->TransferResourceOwnership(this);
}

void RowBatch::DeepCopyTo(RowBatch* dst) {
  DCHECK(dst->row_desc_->Equals(*row_desc_));
  DCHECK_EQ(dst->num_rows_, 0);
  DCHECK_GE(dst->capacity_, num_rows_);
  dst->AddRows(num_rows_);
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* src_row = GetRow(i);
    TupleRow* dst_row = reinterpret_cast<TupleRow*>(dst->tuple_ptrs_ +
        i * num_tuples_per_row_);
    src_row->DeepCopy(
        dst_row, row_desc_->tuple_descriptors(), &dst->tuple_data_pool_, false);
  }
  dst->CommitRows(num_rows_);
}

// TODO: consider computing size of batches as they are built up
int64_t RowBatch::TotalByteSize(DedupMap* distinct_tuples) {
  DCHECK(distinct_tuples == nullptr || distinct_tuples->size() == 0);
  int64_t result = 0;
  vector<int> tuple_count(row_desc_->tuple_descriptors().size(), 0);

  // Sum total variable length byte sizes.
  for (int i = 0; i < num_rows_; ++i) {
    for (int j = 0; j < num_tuples_per_row_; ++j) {
      Tuple* tuple = GetRow(i)->GetTuple(j);
      if (UNLIKELY(tuple == nullptr)) continue;
      // Only count the data of unique tuples.
      if (LIKELY(i > 0) && UNLIKELY(GetRow(i - 1)->GetTuple(j) == tuple)) {
        // Fast tuple deduplication for adjacent rows.
        continue;
      } else if (UNLIKELY(distinct_tuples != nullptr)) {
        if (row_desc_->tuple_descriptors()[j]->byte_size() == 0) continue;
        bool inserted = distinct_tuples->InsertIfNotPresent(tuple, -1);
        if (!inserted) continue;
      }
      result += tuple->VarlenByteSize(*row_desc_->tuple_descriptors()[j]);
      ++tuple_count[j];
    }
  }
  // Compute sum of fixed component of tuple sizes.
  for (int j = 0; j < num_tuples_per_row_; ++j) {
    result += row_desc_->tuple_descriptors()[j]->byte_size() * tuple_count[j];
  }
  return result;
}

Status RowBatch::ResizeAndAllocateTupleBuffer(
    RuntimeState* state, int64_t* buffer_size, uint8_t** buffer) {
  return ResizeAndAllocateTupleBuffer(
      state, &tuple_data_pool_, row_desc_->GetRowSize(), &capacity_, buffer_size, buffer);
}

Status RowBatch::ResizeAndAllocateTupleBuffer(RuntimeState* state, MemPool* pool,
    int row_size, int* capacity, int64_t* buffer_size, uint8_t** buffer) {
  // Avoid divide-by-zero. Don't need to modify capacity for empty rows anyway.
  if (row_size != 0) {
    *capacity = max(1, min(*capacity, FIXED_LEN_BUFFER_LIMIT / row_size));
  }
  *buffer_size = static_cast<int64_t>(row_size) * *capacity;
  *buffer = pool->TryAllocate(*buffer_size);
  if (*buffer == nullptr) {
    return pool->mem_tracker()->MemLimitExceeded(
        state, "Failed to allocate tuple buffer", *buffer_size);
  }
  return Status::OK();
}

void RowBatch::VLogRows(const string& context) {
  if (!VLOG_ROW_IS_ON) return;
  VLOG_ROW << context << ": #rows=" << num_rows_;
  for (int i = 0; i < num_rows_; ++i) {
    VLOG_ROW << PrintRow(GetRow(i), *row_desc_);
  }
}

}
