// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/row-batch.h"

#include <stdint.h>  // for intptr_t
#include <boost/scoped_ptr.hpp>

#include "runtime/buffered-tuple-stream.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/compress.h"
#include "util/decompress.h"
#include "util/fixed-size-hash-table.h"
#include "gen-cpp/Results_types.h"

#include "common/names.h"

// Used to determine memory ownership of a RowBatch's tuple pointers.
DECLARE_bool(enable_partitioned_hash_join);
DECLARE_bool(enable_partitioned_aggregation);

namespace impala {

const int RowBatch::AT_CAPACITY_MEM_USAGE = 8 * 1024 * 1024;

RowBatch::RowBatch(const RowDescriptor& row_desc, int capacity,
    MemTracker* mem_tracker)
  : mem_tracker_(mem_tracker),
    has_in_flight_row_(false),
    num_rows_(0),
    capacity_(capacity),
    num_tuples_per_row_(row_desc.tuple_descriptors().size()),
    row_desc_(row_desc),
    auxiliary_mem_usage_(0),
    need_to_return_(false),
    tuple_data_pool_(new MemPool(mem_tracker_)) {
  DCHECK(mem_tracker_ != NULL);
  DCHECK_GT(capacity, 0);
  tuple_ptrs_size_ = capacity_ * num_tuples_per_row_ * sizeof(Tuple*);
  DCHECK_GT(tuple_ptrs_size_, 0);
  // TODO: switch to Init() pattern so we can check memory limit and return Status.
  if (FLAGS_enable_partitioned_aggregation && FLAGS_enable_partitioned_hash_join) {
    mem_tracker_->Consume(tuple_ptrs_size_);
    tuple_ptrs_ = reinterpret_cast<Tuple**>(malloc(tuple_ptrs_size_));
    DCHECK(tuple_ptrs_ != NULL);
  } else {
    tuple_ptrs_ = reinterpret_cast<Tuple**>(
        tuple_data_pool_->Allocate(tuple_ptrs_size_));
  }
}

// TODO: we want our input_batch's tuple_data to come from our (not yet implemented)
// global runtime memory segment; how do we get thrift to allocate it from there?
// maybe change line (in Data_types.cc generated from Data.thrift)
//              xfer += iprot->readString(this->tuple_data[_i9]);
// to allocated string data in special mempool
// (change via python script that runs over Data_types.cc)
RowBatch::RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch,
    MemTracker* mem_tracker)
  : mem_tracker_(mem_tracker),
    has_in_flight_row_(false),
    num_rows_(input_batch.num_rows),
    capacity_(num_rows_),
    num_tuples_per_row_(input_batch.row_tuples.size()),
    row_desc_(row_desc),
    auxiliary_mem_usage_(0),
    tuple_data_pool_(new MemPool(mem_tracker)) {
  DCHECK(mem_tracker_ != NULL);
  tuple_ptrs_size_ = num_rows_ * input_batch.row_tuples.size() * sizeof(Tuple*);
  DCHECK_GT(tuple_ptrs_size_, 0);
  // TODO: switch to Init() pattern so we can check memory limit and return Status.
  if (FLAGS_enable_partitioned_aggregation && FLAGS_enable_partitioned_hash_join) {
    mem_tracker_->Consume(tuple_ptrs_size_);
    tuple_ptrs_ = reinterpret_cast<Tuple**>(malloc(tuple_ptrs_size_));
    DCHECK(tuple_ptrs_ != NULL);
  } else {
    tuple_ptrs_ = reinterpret_cast<Tuple**>(
        tuple_data_pool_->Allocate(tuple_ptrs_size_));
  }
  uint8_t* tuple_data;
  if (input_batch.compression_type != THdfsCompression::NONE) {
    // Decompress tuple data into data pool
    uint8_t* compressed_data = (uint8_t*)input_batch.tuple_data.c_str();
    size_t compressed_size = input_batch.tuple_data.size();

    scoped_ptr<Codec> decompressor;
    Status status = Codec::CreateDecompressor(NULL, false, input_batch.compression_type,
        &decompressor);
    DCHECK(status.ok()) << status.GetDetail();

    int64_t uncompressed_size = input_batch.uncompressed_size;
    DCHECK_NE(uncompressed_size, -1) << "RowBatch decompression failed";
    tuple_data = tuple_data_pool_->Allocate(uncompressed_size);
    status = decompressor->ProcessBlock(true, compressed_size, compressed_data,
        &uncompressed_size, &tuple_data);
    DCHECK(status.ok()) << "RowBatch decompression failed.";
    decompressor->Close();
  } else {
    // Tuple data uncompressed, copy directly into data pool
    tuple_data = tuple_data_pool_->Allocate(input_batch.tuple_data.size());
    memcpy(tuple_data, input_batch.tuple_data.c_str(), input_batch.tuple_data.size());
  }

  // Convert input_batch.tuple_offsets into pointers
  int tuple_idx = 0;
  for (vector<int32_t>::const_iterator offset = input_batch.tuple_offsets.begin();
       offset != input_batch.tuple_offsets.end(); ++offset) {
    if (*offset == -1) {
      tuple_ptrs_[tuple_idx++] = NULL;
    } else {
      tuple_ptrs_[tuple_idx++] = reinterpret_cast<Tuple*>(tuple_data + *offset);
    }
  }

  // Check whether we have slots that require offset-to-pointer conversion.
  if (!row_desc_.HasVarlenSlots()) return;

  // For every unique tuple, convert string offsets contained in tuple data into
  // pointers. Tuples were serialized in the order we are deserializing them in,
  // so the first occurrence of a tuple will always have a higher offset than any tuple
  // we already converted.
  Tuple* last_converted = NULL;
  for (int i = 0; i < num_rows_; ++i) {
    for (int j = 0; j < num_tuples_per_row_; ++j) {
      const TupleDescriptor* desc = row_desc_.tuple_descriptors()[j];
      if (!desc->HasVarlenSlots()) continue;
      Tuple* tuple = GetRow(i)->GetTuple(j);
      // Handle NULL or already converted tuples with one check.
      if (tuple <= last_converted) continue;
      last_converted = tuple;
      tuple->ConvertOffsetsToPointers(*desc, tuple_data);
    }
  }
}

RowBatch::~RowBatch() {
  tuple_data_pool_->FreeAll();
  for (int i = 0; i < io_buffers_.size(); ++i) {
    io_buffers_[i]->Return();
  }
  CloseTupleStreams();
  for (int i = 0; i < blocks_.size(); ++i) {
    // TODO: Delete() returns a Status.
    blocks_[i]->Delete();
  }
  if (FLAGS_enable_partitioned_aggregation && FLAGS_enable_partitioned_hash_join) {
    DCHECK(tuple_ptrs_ != NULL);
    free(tuple_ptrs_);
    mem_tracker_->Release(tuple_ptrs_size_);
    tuple_ptrs_ = NULL;
  }
}

Status RowBatch::Serialize(TRowBatch* output_batch) {
  return Serialize(output_batch, UseFullDedup());
}

Status RowBatch::Serialize(TRowBatch* output_batch, bool full_dedup) {
  // why does Thrift not generate a Clear() function?
  output_batch->row_tuples.clear();
  output_batch->tuple_offsets.clear();
  output_batch->compression_type = THdfsCompression::NONE;

  output_batch->num_rows = num_rows_;
  row_desc_.ToThrift(&output_batch->row_tuples);

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
    SerializeInternal(size, &distinct_tuples, output_batch);
  } else {
    size = TotalByteSize(NULL);
    SerializeInternal(size, NULL, output_batch);
  }

  if (size > 0) {
    // Try compressing tuple_data to compression_scratch_, swap if compressed data is
    // smaller
    scoped_ptr<Codec> compressor;
    RETURN_IF_ERROR(Codec::CreateCompressor(NULL, false, THdfsCompression::LZ4,
                                            &compressor));

    int64_t compressed_size = compressor->MaxOutputLen(size);
    if (compression_scratch_.size() < compressed_size) {
      compression_scratch_.resize(compressed_size);
    }
    uint8_t* input = (uint8_t*)output_batch->tuple_data.c_str();
    uint8_t* compressed_output = (uint8_t*)compression_scratch_.c_str();
    RETURN_IF_ERROR(compressor->ProcessBlock(true, size, input, &compressed_size,
        &compressed_output));
    if (LIKELY(compressed_size < size)) {
      compression_scratch_.resize(compressed_size);
      output_batch->tuple_data.swap(compression_scratch_);
      output_batch->compression_type = THdfsCompression::LZ4;
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
  if (row_desc_.tuple_descriptors().size() < 3) return false;
  vector<TupleDescriptor*>::const_iterator tuple_desc =
      row_desc_.tuple_descriptors().begin();
  for (; tuple_desc != row_desc_.tuple_descriptors().end(); ++tuple_desc) {
    if (!(*tuple_desc)->collection_slots().empty()) return true;
  }
  return false;
}

void RowBatch::SerializeInternal(int64_t size, DedupMap* distinct_tuples,
    TRowBatch* output_batch) {
  DCHECK(distinct_tuples == NULL || distinct_tuples->size() == 0);
  // TODO: max_size() is much larger than the amount of memory we could feasibly
  // allocate. Need better way to detect problem.
  DCHECK_LE(size, output_batch->tuple_data.max_size());

  // TODO: track memory usage
  // TODO: detect if serialized size is too large to allocate and return proper error.
  output_batch->tuple_data.resize(size);
  output_batch->uncompressed_size = size;
  output_batch->tuple_offsets.reserve(num_rows_ * num_tuples_per_row_);

  // Copy tuple data of unique tuples, including strings, into output_batch (converting
  // string pointers into offsets in the process).
  int offset = 0; // current offset into output_batch->tuple_data
  char* tuple_data = const_cast<char*>(output_batch->tuple_data.c_str());

  for (int i = 0; i < num_rows_; ++i) {
    vector<TupleDescriptor*>::const_iterator desc = row_desc_.tuple_descriptors().begin();
    for (int j = 0; desc != row_desc_.tuple_descriptors().end(); ++desc, ++j) {
      Tuple* tuple = GetRow(i)->GetTuple(j);
      if (UNLIKELY(tuple == NULL)) {
        // NULLs are encoded as -1
        output_batch->tuple_offsets.push_back(-1);
        continue;
      } else if (LIKELY(i > 0) && UNLIKELY(GetRow(i - 1)->GetTuple(j) == tuple)) {
        // Fast tuple deduplication for adjacent rows.
        int prev_row_idx = output_batch->tuple_offsets.size() - num_tuples_per_row_;
        output_batch->tuple_offsets.push_back(
            output_batch->tuple_offsets[prev_row_idx]);
        continue;
      } else if (UNLIKELY(distinct_tuples != NULL)) {
        if ((*desc)->byte_size() == 0) {
          // Zero-length tuples can be represented as NULL.
          output_batch->tuple_offsets.push_back(-1);
          continue;
        }
        int* dedupd_offset = distinct_tuples->FindOrInsert(tuple, offset);
        if (*dedupd_offset != offset) {
          // Repeat of tuple
          DCHECK_GE(*dedupd_offset, 0);
          output_batch->tuple_offsets.push_back(*dedupd_offset);
          continue;
        }
      }
      // Record offset before creating copy (which increments offset and tuple_data)
      output_batch->tuple_offsets.push_back(offset);
      tuple->DeepCopy(**desc, &tuple_data, &offset, /* convert_ptrs */ true);
      DCHECK_LE(offset, size);
    }
  }
  DCHECK_EQ(offset, size);
}

void RowBatch::AddIoBuffer(DiskIoMgr::BufferDescriptor* buffer) {
  DCHECK(buffer != NULL);
  io_buffers_.push_back(buffer);
  auxiliary_mem_usage_ += buffer->buffer_len();
  buffer->SetMemTracker(mem_tracker_);
}

void RowBatch::AddTupleStream(BufferedTupleStream* stream) {
  DCHECK(stream != NULL);
  tuple_streams_.push_back(stream);
  auxiliary_mem_usage_ += stream->byte_size();
}

void RowBatch::AddBlock(BufferedBlockMgr::Block* block) {
  DCHECK(block != NULL);
  blocks_.push_back(block);
  auxiliary_mem_usage_ += block->buffer_len();
}

void RowBatch::Reset() {
  DCHECK(tuple_data_pool_.get() != NULL);
  num_rows_ = 0;
  has_in_flight_row_ = false;
  // TODO: Change this to Clear() and investigate the repercussions.
  tuple_data_pool_->FreeAll();
  for (int i = 0; i < io_buffers_.size(); ++i) {
    io_buffers_[i]->Return();
  }
  io_buffers_.clear();
  CloseTupleStreams();
  for (int i = 0; i < blocks_.size(); ++i) {
    // TODO: Delete() returns a Status.
    blocks_[i]->Delete();
  }
  blocks_.clear();
  auxiliary_mem_usage_ = 0;
  if (!FLAGS_enable_partitioned_aggregation || !FLAGS_enable_partitioned_hash_join) {
    tuple_ptrs_ = reinterpret_cast<Tuple**>(
        tuple_data_pool_->Allocate(tuple_ptrs_size_));
  }
  need_to_return_ = false;
}

void RowBatch::CloseTupleStreams() {
  for (int i = 0; i < tuple_streams_.size(); ++i) {
    tuple_streams_[i]->Close();
    delete tuple_streams_[i];
  }
  tuple_streams_.clear();
}

void RowBatch::TransferResourceOwnership(RowBatch* dest) {
  dest->auxiliary_mem_usage_ += tuple_data_pool_->total_allocated_bytes();
  dest->tuple_data_pool_->AcquireData(tuple_data_pool_.get(), false);
  for (int i = 0; i < io_buffers_.size(); ++i) {
    DiskIoMgr::BufferDescriptor* buffer = io_buffers_[i];
    dest->io_buffers_.push_back(buffer);
    dest->auxiliary_mem_usage_ += buffer->buffer_len();
    buffer->SetMemTracker(dest->mem_tracker_);
  }
  io_buffers_.clear();
  for (int i = 0; i < tuple_streams_.size(); ++i) {
    dest->tuple_streams_.push_back(tuple_streams_[i]);
    dest->auxiliary_mem_usage_ += tuple_streams_[i]->byte_size();
  }
  tuple_streams_.clear();
  for (int i = 0; i < blocks_.size(); ++i) {
    dest->blocks_.push_back(blocks_[i]);
    dest->auxiliary_mem_usage_ += blocks_[i]->buffer_len();
  }
  blocks_.clear();
  dest->need_to_return_ |= need_to_return_;
  auxiliary_mem_usage_ = 0;
  if (!FLAGS_enable_partitioned_aggregation || !FLAGS_enable_partitioned_hash_join) {
    tuple_ptrs_ = NULL;
  }
  Reset();
}

int RowBatch::GetBatchSize(const TRowBatch& batch) {
  int result = batch.tuple_data.size();
  result += batch.row_tuples.size() * sizeof(TTupleId);
  result += batch.tuple_offsets.size() * sizeof(int32_t);
  return result;
}

void RowBatch::AcquireState(RowBatch* src) {
  DCHECK(row_desc_.Equals(src->row_desc_));
  DCHECK_EQ(num_tuples_per_row_, src->num_tuples_per_row_);
  DCHECK_EQ(tuple_ptrs_size_, src->tuple_ptrs_size_);
  DCHECK_EQ(capacity_, src->capacity_);
  DCHECK_EQ(auxiliary_mem_usage_, 0);

  // The destination row batch should be empty.
  DCHECK(!has_in_flight_row_);
  DCHECK_EQ(num_rows_, 0);

  for (int i = 0; i < src->io_buffers_.size(); ++i) {
    DiskIoMgr::BufferDescriptor* buffer = src->io_buffers_[i];
    io_buffers_.push_back(buffer);
    auxiliary_mem_usage_ += buffer->buffer_len();
    buffer->SetMemTracker(mem_tracker_);
  }
  src->io_buffers_.clear();
  src->auxiliary_mem_usage_ = 0;

  DCHECK(src->tuple_streams_.empty());
  DCHECK(src->blocks_.empty());

  has_in_flight_row_ = src->has_in_flight_row_;
  num_rows_ = src->num_rows_;
  capacity_ = src->capacity_;
  need_to_return_ = src->need_to_return_;
  std::swap(tuple_ptrs_, src->tuple_ptrs_);
  tuple_data_pool_->AcquireData(src->tuple_data_pool_.get(), false);
  auxiliary_mem_usage_ += src->tuple_data_pool_->total_allocated_bytes();
}

void RowBatch::DeepCopyTo(RowBatch* dst) {
  DCHECK(dst->row_desc_.Equals(row_desc_));
  DCHECK_EQ(dst->num_rows_, 0);
  DCHECK_GE(dst->capacity_, num_rows_);
  dst->AddRows(num_rows_);
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* src_row = GetRow(i);
    TupleRow* dst_row = reinterpret_cast<TupleRow*>(dst->tuple_ptrs_ +
        i * num_tuples_per_row_);
    src_row->DeepCopy(dst_row, row_desc_.tuple_descriptors(),
        dst->tuple_data_pool_.get(), false);
  }
  dst->CommitRows(num_rows_);
}

// TODO: consider computing size of batches as they are built up
int64_t RowBatch::TotalByteSize(DedupMap* distinct_tuples) {
  DCHECK(distinct_tuples == NULL || distinct_tuples->size() == 0);
  int64_t result = 0;
  vector<int> tuple_count(row_desc_.tuple_descriptors().size(), 0);

  // Sum total variable length byte sizes.
  for (int i = 0; i < num_rows_; ++i) {
    for (int j = 0; j < num_tuples_per_row_; ++j) {
      Tuple* tuple = GetRow(i)->GetTuple(j);
      if (UNLIKELY(tuple == NULL)) continue;
      // Only count the data of unique tuples.
      if (LIKELY(i > 0) && UNLIKELY(GetRow(i - 1)->GetTuple(j) == tuple)) {
        // Fast tuple deduplication for adjacent rows.
        continue;
      } else if (UNLIKELY(distinct_tuples != NULL)) {
        if (row_desc_.tuple_descriptors()[j]->byte_size() == 0) continue;
        bool inserted = distinct_tuples->InsertIfNotPresent(tuple, -1);
        if (!inserted) continue;
      }
      result += tuple->VarlenByteSize(*row_desc_.tuple_descriptors()[j]);
      ++tuple_count[j];
    }
  }
  // Compute sum of fixed component of tuple sizes.
  for (int j = 0; j < num_tuples_per_row_; ++j) {
    result += row_desc_.tuple_descriptors()[j]->byte_size() * tuple_count[j];
  }
  return result;
}

int RowBatch::MaxTupleBufferSize() {
  int row_size = row_desc_.GetRowSize();
  if (row_size > AT_CAPACITY_MEM_USAGE) return row_size;
  int num_rows = 0;
  if (row_size != 0) {
    num_rows = std::min(capacity_, AT_CAPACITY_MEM_USAGE / row_size);
  }
  int tuple_buffer_size = num_rows * row_size;
  DCHECK_LE(tuple_buffer_size, AT_CAPACITY_MEM_USAGE);
  return tuple_buffer_size;
}
}
