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

#include "exec/puffin/puffin-writer.h"

#include <limits>
#include <string_view>

#include <arpa/inet.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "common/logging.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exec/blob-reader.h"
#include "exec/hdfs-table-writer.h"
#include "exec/output-partition.h"
#include "exec/puffin/blob.h"
#include "exec/table-sink-base.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec-env.h"
#include "runtime/io/request-context.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/coding-util.h"
#include "util/hash-util.h"
#include "util/hdfs-util.h"
#include "util/runtime-profile-counters.h"
#include "util/time.h"

// Initial capacity for the per-file position buffer in AppendRows. Caps the upfront
// allocation for the common case where many files each have few deleted rows. The vector
// grows geometrically if a single file has many deletions. Mirrors the convention in
// iceberg-delete-builder.cc.
static constexpr int POSITIONS_BUFFER_INITIAL_CAPACITY = 128;

namespace impala {

PuffinWriter::PuffinWriter(TableSinkBase* parent, RuntimeState* state,
    OutputPartition* output, const HdfsPartitionDescriptor* partition,
    const HdfsTableDescriptor* table_desc)
  : HdfsTableWriter(parent, state, output, partition, table_desc),
    current_offset_(0),
    io_request_context_(nullptr),
    obj_pool_(nullptr),
    blob_mem_pool_(new MemPool(parent_->mem_tracker())) {}

PuffinWriter::~PuffinWriter() {}

Status PuffinWriter::Init() {

  // Initialize IO infrastructure for loading existing DVs
  // Actual DV loading is deferred to Finalize() after all rows have arrived
  io_request_context_ = ExecEnv::GetInstance()->disk_io_mgr()->RegisterContext();
  obj_pool_ = state_->obj_pool();
  DCHECK(obj_pool_ != nullptr);

  // Initialize deletion vector blob reader
  dv_blob_reader_.reset(new DeletionVectorBlobReader());

  // Initialize timer for tracking DV loading performance
  load_dv_timer_ = ADD_TIMER(state_->runtime_profile(), "PuffinOldDVLoadTimer");

  return Status::OK();
}

void PuffinWriter::Close() {
  // Release blob reader
  dv_blob_reader_.reset();

  // Ensure blob memory is released
  if (blob_mem_pool_ != nullptr) {
    blob_mem_pool_->FreeAll();
  }
  if (io_request_context_ != nullptr) {
    ExecEnv::GetInstance()->disk_io_mgr()->UnregisterContext(io_request_context_.get());
  }
}

Status PuffinWriter::InitNewFile() {
  current_offset_ = MAGIC_SIZE;
  uint32_t magic_be = htonl(PUFFIN_MAGIC);
  RETURN_IF_ERROR(Write(magic_be));

  return Status::OK();
}

Status PuffinWriter::AppendRows(
    RowBatch* batch, const std::vector<int32_t>& row_group_indices, bool* new_file) {
  SCOPED_TIMER(parent_->encode_timer());
  *new_file = false;

  if (batch->num_rows() == 0) return Status::OK();

  // We expect the row batch to have two columns: filepath (string) and position (bigint)
  // Similar to how IcebergBufferedDeleteSink processes rows
  DCHECK_GE(output_expr_evals_.size(), 2);
  ScalarExprEvaluator* filepath_eval = output_expr_evals_[0];
  ScalarExprEvaluator* position_eval = output_expr_evals_[1];

  // File paths are ordered, so rows for the same data file always arrive as a
  // contiguous run. We accumulate positions for the current file into a buffer and
  // flush via AddElements when the file changes or the batch ends.
  std::vector<uint64_t> pending_positions;
  pending_positions.reserve(POSITIONS_BUFFER_INITIAL_CAPACITY);

  auto flush_pending = [&]() {
    if (pending_positions.empty()) return;
    last_bitmap_it_->second.AddElements(pending_positions);
    output_->current_file_rows += pending_positions.size();
    pending_positions.clear();
  };

  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);

    // Extract filepath
    impala_udf::StringVal filepath_sv = filepath_eval->GetStringVal(row);
    DCHECK(!filepath_sv.is_null);
    std::string_view filepath_sv_view(
        reinterpret_cast<char*>(filepath_sv.ptr), filepath_sv.len);

    // Extract position (row number)
    impala_udf::BigIntVal position_bi = position_eval->GetBigIntVal(row);
    DCHECK(!position_bi.is_null);
    int64_t position = position_bi.val;

    // Use the cached iterator when the filepath matches the previous row, otherwise
    // flush the accumulated positions for the outgoing file, then look up (or insert)
    // the entry in the map and refresh the cache.
    if (last_bitmap_it_ == deletion_vectors_.end() ||
        last_filepath_ != filepath_sv_view) {
      flush_pending();
      last_filepath_.assign(filepath_sv_view.data(), filepath_sv_view.size());
      last_bitmap_it_ = deletion_vectors_.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(last_filepath_),
          std::forward_as_tuple()).first;
    }
    pending_positions.push_back(static_cast<uint64_t>(position));
  }
  flush_pending(); // flush the final (or only) file's positions

  return Status::OK();
}

Status PuffinWriter::Finalize() {
  // Load old deletion vectors ONLY for data files that have new deletes
  // This optimizes for the common case where only a subset of files are modified:
  // - Iterate over deletion_vectors_ (files with new deletes)
  // - Lookup each in referenced_deletion_vectors (files with old DVs)
  // - Load only when both conditions are true
  for (const auto& entry : deletion_vectors_) {
    const std::string& data_file_path = entry.first;

    // Check if this data file has an old DV that needs to be loaded and merged
    auto old_dv_it = output_->referenced_deletion_vectors.find(
        THash128FromFilePath(data_file_path));
    if (old_dv_it != output_->referenced_deletion_vectors.end()) {
      const TIcebergDeletionVector& old_dv_metadata = old_dv_it->second;

      VLOG(2) << "Loading old DV for data file with new deletes: " << data_file_path;

      // Load the old DV and merge it with new deletes
      SCOPED_TIMER(load_dv_timer_);
      RETURN_IF_ERROR(LoadExistingDeletionVector(
          old_dv_metadata.path, data_file_path,
          old_dv_metadata.content_offset + DELETION_VECTOR_BLOB_HEADER_SIZE,
          old_dv_metadata.content_size_in_bytes));

      // Track that this old DV is being replaced
      output_->puffin_result.old_deletion_vectors[data_file_path] = old_dv_metadata;
    }
  }

  SCOPED_TIMER(parent_->hdfs_write_timer());

  // Convert accumulated deletion vectors to blobs
  for (auto& entry : deletion_vectors_) {
    const std::string& data_file_path = entry.first;
    RoaringBitmap64& bitmap = entry.second;

    // Add deletion vector blob for this data file
    // Using default snapshot_id=0 and sequence_number=0
    // These should be set appropriately by the caller if needed
    RETURN_IF_ERROR(AddDeletionVector(data_file_path, bitmap, 0, 0));
  }

  // Write all buffered blobs to the file
  for (const auto& blob : file_.GetBlobs()) {
    if (blob.data.data != nullptr && blob.metadata.length > 0) {
      RETURN_IF_ERROR(Write(blob.data.data, blob.metadata.length));
    }
  }

  // Write the footer containing blob metadata
  RETURN_IF_ERROR(WriteFooter());

  blob_mem_pool_->Clear();

  return Status::OK();
}

Status PuffinWriter::AddBlob(puffin::Blob& blob) {
  // Set the offset for this blob
  blob.metadata.offset = current_offset_;

  // Add to the file structure
  file_.AddBlob(blob);

  // Update the current offset for the next blob
  current_offset_ += blob.data.length;

  return Status::OK();
}

Status PuffinWriter::AddDeletionVector(const std::string& data_file_path,
    RoaringBitmap64& bitmap, int64_t snapshot_id, int64_t sequence_number) {
  // Get the serialized size of the bitmap
  size_t bitmap_serialized_size = bitmap.BitmapSizeInBytes();
  if (bitmap_serialized_size == 0) {
    return Status("Cannot add deletion vector: bitmap is empty");
  }

  // Calculate the total size for the blob following the serialization pattern:
  // 4 bytes: combined length (vector size + magic bytes size)
  // 4 bytes: magic sequence (0xD1D33964)
  // N bytes: serialized vector (bitmap)
  // 4 bytes: CRC-32 checksum
  const size_t magic_size = 4;
  const size_t length_field_size = 4;
  const size_t crc_size = 4;
  const size_t combined_length = bitmap_serialized_size + magic_size;
  const size_t total_blob_size =
      length_field_size + magic_size + bitmap_serialized_size + crc_size;

  // Allocate buffer for the complete serialized blob
  uint8_t* data = blob_mem_pool_->TryAllocate(total_blob_size);
  if (UNLIKELY(data == nullptr)) {
    return parent_->mem_tracker()->MemLimitExceeded(state_,
        "Failed to allocate memory for deletion vector blob serialization.",
        total_blob_size);
  }
  uint8_t* write_ptr = data;

  uint32_t combined_length_be = htonl(static_cast<uint32_t>(combined_length));
  memcpy(write_ptr, &combined_length_be, length_field_size);
  write_ptr += length_field_size;

  uint32_t magic_be = htonl(BLOB_MAGIC);
  memcpy(write_ptr, &magic_be, magic_size);
  write_ptr += magic_size;

  size_t written = bitmap.Serialize(write_ptr);
  DCHECK_EQ(written, bitmap_serialized_size);
  write_ptr += bitmap_serialized_size;

  const uint8_t* crc_start = data + length_field_size;
  const size_t crc_data_length = magic_size + bitmap_serialized_size;
  uint32_t crc = CRC32::ComputeChecksum(crc_start, crc_data_length);
  // Write CRC as 4-byte big-endian integer
  uint32_t crc_be = htonl(crc);
  memcpy(write_ptr, &crc_be, crc_size);

  // Create blob data wrapper
  puffin::BlobData blob_data = {data, total_blob_size};

  // Create blob metadata
  puffin::BlobMetadata metadata(puffin::BlobType::DELETION_VECTOR_V1, total_blob_size);
  metadata.snapshot_id = snapshot_id;
  metadata.sequence_number = sequence_number;

  // Store the data file path in properties
  metadata.properties["referenced-data-file"] = data_file_path;

  // Create the deletion vector object to track the newly written blob
  TIcebergDeletionVector new_dv;
  // Note: path will be set later in createIcebergDataFileString() when final
  // path is known
  new_dv.__set_path("");
  new_dv.__set_content_offset(current_offset_);
  new_dv.__set_content_size_in_bytes(total_blob_size);
  new_dv.__set_record_count(bitmap.Cardinality());

  // Create blob and add it
  puffin::Blob blob(metadata, blob_data);
  Status status = AddBlob(blob);

  // Add the new deletion vector to the result, keyed by the data file path
  output_->puffin_result.new_deletion_vectors[data_file_path] = new_dv;
  return status;
}

Status PuffinWriter::WriteFooter() {
  std::string footer_json = SerializeFooterToJson();
  uint32_t magic_be = htonl(PUFFIN_MAGIC);
  RETURN_IF_ERROR(Write(magic_be));
  RETURN_IF_ERROR(Write(footer_json.data(), footer_json.size()));
  uint32_t footer_size = static_cast<uint32_t>(footer_json.size());
  RETURN_IF_ERROR(Write(footer_size));
  RETURN_IF_ERROR(Write(0));
  RETURN_IF_ERROR(Write(magic_be));

  return Status::OK();
}

std::string PuffinWriter::SerializeFooterToJson() {
  using namespace rapidjson;

  // Create JSON document
  Document doc;
  doc.SetObject();
  Document::AllocatorType& allocator = doc.GetAllocator();

  // Create blobs array
  Value blobs_array(kArrayType);
  for (const auto& blob : file_.GetBlobs()) {
    Value blob_obj(kObjectType);

    // Add type
    Value type_val;
    type_val.SetString(puffin::BlobTypeToString(blob.metadata.type).c_str(), allocator);
    blob_obj.AddMember("type", type_val, allocator);

    // Add fields array
    Value fields_array(kArrayType);
    for (int64_t field_id : blob.metadata.fields) {
      fields_array.PushBack(field_id, allocator);
    }

    blob_obj.AddMember("fields", fields_array, allocator);
    blob_obj.AddMember("snapshot-id", blob.metadata.snapshot_id, allocator);
    blob_obj.AddMember("sequence-number", blob.metadata.sequence_number, allocator);
    blob_obj.AddMember("offset", blob.metadata.offset, allocator);
    blob_obj.AddMember("length", static_cast<int64_t>(blob.metadata.length), allocator);

    Value codec_val;
    codec_val.SetString(
        puffin::CompressionCodecToString(blob.metadata.compression_codec).c_str(),
        allocator);
    blob_obj.AddMember("compression-codec", codec_val, allocator);

    Value props_obj(kObjectType);
    for (const auto& prop : blob.metadata.properties) {
      Value key(prop.first.c_str(), allocator);
      Value val(prop.second.c_str(), allocator);
      props_obj.AddMember(key, val, allocator);
    }
    blob_obj.AddMember("properties", props_obj, allocator);

    blobs_array.PushBack(blob_obj, allocator);
  }
  doc.AddMember("blobs", blobs_array, allocator);

  // Add file-level properties
  Value file_props_obj(kObjectType);
  for (const auto& prop : file_.GetFileMetadata()) {
    Value key(prop.first.c_str(), allocator);
    Value val(prop.second.c_str(), allocator);
    file_props_obj.AddMember(key, val, allocator);
  }
  doc.AddMember("properties", file_props_obj, allocator);

  // Convert to string
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);

  return std::string(buffer.GetString(), buffer.GetSize());
}

Status PuffinWriter::ValidateBlobLocation(const std::string& file_path,
    int64_t content_offset, int64_t content_size) {

  // Validate offset and size are non-negative
  if (content_offset < 0) {
    return Status(strings::Substitute(
        "Invalid content offset $0 for Puffin file '$1': must be non-negative",
        content_offset, file_path));
  }
  if (content_size <= 0) {
    return Status(strings::Substitute(
        "Invalid content size $0 for Puffin file '$1': must be positive",
        content_size, file_path));
  }

  // Get file size to validate bounds
  int64_t file_size;
  RETURN_IF_ERROR(GetFileSize(output_->hdfs_connection, file_path.c_str(), &file_size));

  // Validate that blob is within file bounds
  if (content_offset + content_size > file_size) {
    return Status(strings::Substitute(
        "Blob location out of bounds in Puffin file '$0': "
        "offset=$1 + size=$2 = $3 exceeds file size $4",
        file_path, content_offset, content_size,
        content_offset + content_size, file_size));
  }

  return Status::OK();
}

Status PuffinWriter::LoadExistingDeletionVector(
    const std::string& puffin_file_path,
    const std::string& data_file_path,
    int64_t content_offset,
    int64_t content_size) {

  DCHECK(output_->hdfs_connection != nullptr)
      << "HDFS connection must be initialized before loading DVs. "
      << "Ensure Init() was called successfully.";
  DCHECK(dv_blob_reader_ != nullptr)
      << "DV blob reader not initialized";

  VLOG(2) << "Loading existing deletion vector from Puffin file '"
          << puffin_file_path << "' at offset " << content_offset
          << " with size " << content_size << " for data file '"
          << data_file_path << "'";

  RETURN_IF_ERROR(ValidateBlobLocation(
      puffin_file_path, content_offset, content_size));

  RoaringBitmap64 loaded_dv;
  Status load_status = dv_blob_reader_->Load(
      io_request_context_.get(), parent_->mem_tracker(), obj_pool_,
      puffin_file_path, content_offset, content_size, &loaded_dv);

  if (!load_status.ok()) {
    return Status(strings::Substitute(
        "Failed to load deletion vector from '$0': $1",
        puffin_file_path, load_status.msg().msg()));
  }

  VLOG(2) << "Successfully loaded DV with " << loaded_dv.Cardinality()
          << " deleted positions";

  auto& target_dv = deletion_vectors_[data_file_path];

  target_dv.Or(loaded_dv);

  return Status::OK();
}

} // namespace impala
