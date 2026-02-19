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


#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/blob-reader.h"
#include "exec/hdfs-table-writer.h"
#include "exec/puffin/blob.h"
#include "util/roaring-bitmap.h"
#include "util/thash128-util.h"

namespace impala {

class MemPool;
class ObjectPool;

namespace io {
class RequestContext;
}

/// Writer for Puffin files containing deletion vectors and statistics.
/// Implements the Puffin file format specification.
/// Format: [Magic(4)][Blob1][Blob2]...[BlobN][Footer JSON][Footer Length(4)][Magic(4)]
class PuffinWriter : public HdfsTableWriter {
 public:
  PuffinWriter(TableSinkBase* parent, RuntimeState* state, OutputPartition* output,
      const HdfsPartitionDescriptor* partition, const HdfsTableDescriptor* table_desc);

  ~PuffinWriter() override;

  /// Initialize HDFS connection and allocate buffers
  Status Init() override WARN_UNUSED_RESULT;

  /// Create new Puffin file and write magic bytes
  Status InitNewFile() override WARN_UNUSED_RESULT;

  /// Map rows to blobs
  Status AppendRows(RowBatch* batch, const std::vector<int32_t>& row_group_indices,
      bool* new_file) override WARN_UNUSED_RESULT;

  /// Write footer and close file
  Status Finalize() override WARN_UNUSED_RESULT;

  void Close() override;

  uint64_t default_block_size() const override { return 0; }

  /// Return file extension
  std::string file_extension() const override { return "puffin"; }

  /// Add a blob to the in-memory buffer
  Status AddBlob(puffin::Blob& blob) WARN_UNUSED_RESULT;

  /// Add a deletion vector for a specific data file.
  /// Creates a DeleteVector blob from the bitmap and adds it to the file.
  /// 'data_file_path' identifies which data file this DV applies to.
  /// 'bitmap' contains the deleted row positions.
  /// 'snapshot_id' and 'sequence_number' are Iceberg metadata.
  Status AddDeletionVector(const std::string& data_file_path,
      RoaringBitmap64& bitmap, int64_t snapshot_id = 0,
      int64_t sequence_number = 0) WARN_UNUSED_RESULT;

  /// Load an existing deletion vector from a Puffin file and merge it with
  /// in-memory deletion vectors for the same data file.
  ///
  /// The loaded DV is merged (union operation) with any existing DV for the same
  /// data_file_path in the deletion_vectors_ map. If no DV exists yet, it creates one.
  ///
  /// @param puffin_file_path Full HDFS path to the Puffin file containing the DV
  /// @param data_file_path Path to the data file this DV references
  /// @param content_offset Byte offset of the DV blob within the Puffin file
  /// @param content_size Size of the DV blob in bytes
  /// @return Status::OK() on success, error status on failure
  Status LoadExistingDeletionVector(
      const std::string& puffin_file_path,
      const std::string& data_file_path,
      int64_t content_offset,
      int64_t content_size) WARN_UNUSED_RESULT;

 private:

  /// Serialize footer to JSON and write to file
  Status WriteFooter() WARN_UNUSED_RESULT;

  /// Serialize footer metadata to JSON string
  std::string SerializeFooterToJson();

  /// Validate that the blob location is within file bounds.
  /// @param file_path Path to the Puffin file
  /// @param content_offset Offset of the blob
  /// @param content_size Size of the blob
  /// @return Status::OK() if valid, error status otherwise
  Status ValidateBlobLocation(const std::string& file_path,
      int64_t content_offset, int64_t content_size) WARN_UNUSED_RESULT;

  /// Puffin file structure containing all blobs
  puffin::File file_;

  /// Track the current offset for blob writing
  int64_t current_offset_;

  /// Map from data file path to RoaringBitmap of deleted row positions
  /// This is populated by AppendRows() and converted to deletion vector
  /// blobs in Finalize()
  std::map<std::string, RoaringBitmap64> deletion_vectors_;

  /// Cache of the last filepath seen in AppendRows() and its iterator into
  /// deletion_vectors_. Rows for the same data file tend to arrive in sequence, so
  /// this avoids a full map lookup on every row in the common case.
  std::string last_filepath_;
  std::map<std::string, RoaringBitmap64>::iterator last_bitmap_it_ =
      deletion_vectors_.end();

  /// IO request context for loading existing deletion vectors
  std::unique_ptr<io::RequestContext> io_request_context_;

  /// Object pool for scan range allocations
  ObjectPool* obj_pool_;

  /// Deletion vector blob reader for deserializing DV blobs
  std::unique_ptr<DeletionVectorBlobReader> dv_blob_reader_;

  /// Memory pool for blob serialization buffers (tracked by parent's mem_tracker)
  /// Lifetime: Init() → Close(). Cleared after each file write.
  std::unique_ptr<MemPool> blob_mem_pool_;

  /// Timer for loading existing deletion vectors
  RuntimeProfile::Counter* load_dv_timer_;

  /// Magic bytes "PFA1" as per Puffin spec
  static constexpr uint32_t PUFFIN_MAGIC = 0x50464131;

  /// Magic bytes for blob serialization
  static constexpr uint32_t BLOB_MAGIC = 0xD1D33964;

  /// Size of magic bytes
  static const int MAGIC_SIZE = 4;

  /// Size of footer length field (4 bytes)
  static const int FOOTER_LENGTH_SIZE = 4;
};

} // namespace impala
