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
#include <memory>
#include <string>

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/request-context.h"
#include "runtime/io/request-ranges.h"
#include "runtime/scoped-buffer.h"
#include "util/roaring-bitmap.h"

namespace impala {

class MemTracker;
class ObjectPool;
class RoaringBitmap64;

namespace io {
class RequestContext;
class ScanRange;
}

/// Generic blob reader class that provides a generalized Load method for reading
/// blob data from HDFS. This class handles the common infrastructure for:
/// - Allocating buffers with memory tracking
/// - Setting up HDFS connections and scan ranges
/// - Reading data from the specified file path and offset
///
/// The deserialization logic is provided via the Deserializer template parameter,
/// which must provide a static Deserialize method with the signature:
///   static Status Deserialize(const uint8_t* data, int64_t length, OutputType* output)
///
/// Template parameters:
///   OutputType - The type of object to deserialize into
///   Deserializer - A type providing the deserialization logic
template <typename OutputType, typename Deserializer>
class BlobReader {
 public:
  BlobReader() = default;
  ~BlobReader() = default;

  /// Load and deserialize blob data from a file path.
  ///
  /// This method handles the following steps:
  /// 1. Allocates a buffer for reading the blob data
  /// 2. Establishes an HDFS connection to the file
  /// 3. Creates a scan range for the specified offset and length
  /// 4. Reads the data into the buffer
  /// 5. Calls Deserializer::Deserialize to parse the data into the output parameter
  ///
  /// @param request_context The IO request context for managing scan operations
  /// @param mem_tracker Memory tracker for tracking buffer allocations
  /// @param obj_pool Object pool for allocating scan ranges
  /// @param path File path to read from
  /// @param content_offset Byte offset within the file where the blob starts
  /// @param content_length Length of the blob content in bytes
  /// @param output Output parameter to store the deserialized blob data
  /// @return Status::OK() on success, error status otherwise
  Status Load(io::RequestContext* request_context, MemTracker* mem_tracker,
      ObjectPool* obj_pool, const std::string& path, int64_t content_offset,
      int64_t content_length, OutputType* output) WARN_UNUSED_RESULT;

 private:
  /// Local cache of HDFS connections (retained for the lifetime of the reader).
  /// This avoids taking a process-wide lock on repeated calls to GetConnection.
  HdfsFsCache::HdfsFsMap fs_cache_;
};

// Template implementation

template <typename OutputType, typename Deserializer>
Status BlobReader<OutputType, Deserializer>::Load(io::RequestContext* request_context,
    MemTracker* mem_tracker, ObjectPool* obj_pool, const std::string& path,
    int64_t content_offset, int64_t content_length, OutputType* output) {
  DCHECK(request_context != nullptr);
  DCHECK(mem_tracker != nullptr);
  DCHECK(obj_pool != nullptr);
  DCHECK(output != nullptr);

  // Allocate buffer for reading the blob
  ScopedBuffer buffer(mem_tracker);
  if (!buffer.TryAllocate(content_length)) {
    return Status(strings::Substitute(
        "Could not allocate buffer of $0 bytes for blob file '$1'.",
        content_length, path));
  }

  // Get HDFS connection
  io::ScanRange::FileInfo file_info;
  file_info.mtime = 1;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
      path, &file_info.fs, &fs_cache_));
  file_info.filename = path.c_str();

  // Create scan range for the blob content
  io::ScanRange* scan_range = io::ScanRange::AllocateScanRange(obj_pool, file_info,
      content_length, content_offset, {}, nullptr, -1, false,
      io::BufferOpts::ReadInto(buffer.buffer(), buffer.Size(),
          io::BufferOpts::USE_DATA_CACHE));

  // Read the blob data
  std::unique_ptr<io::BufferDescriptor> io_buffer;
  bool needs_buffers;
  RETURN_IF_ERROR(request_context->StartScanRange(scan_range, &needs_buffers));
  DCHECK(!needs_buffers) << "Already provided a buffer";
  RETURN_IF_ERROR(scan_range->GetNext(&io_buffer));
  scan_range->ReturnBuffer(std::move(io_buffer));

  return Deserializer::Deserialize(buffer.buffer(), content_length, output);
}

/// Size of the blob header (length + magic) for deletion vector blobs
static constexpr int DELETION_VECTOR_BLOB_HEADER_SIZE = 8;

/// Deserializer for Puffin deletion vector blobs.
/// This class provides the deserialization logic for deletion vectors stored in
/// Puffin format (RoaringBitmap64 with an 8-byte header).
struct DeletionVectorDeserializer {
  /// Deserialize a Puffin deletion vector blob into a RoaringBitmap64.
  /// Puffin deletion vectors have an 8-byte header followed by the serialized bitmap.
  ///
  /// @param data Pointer to the raw blob data buffer
  /// @param length Length of the blob data in bytes
  /// @param output Output parameter for the deserialized RoaringBitmap64
  /// @return Status::OK() on success, error status otherwise
  static Status Deserialize(const uint8_t* data, int64_t length,
      RoaringBitmap64* output) {
    DCHECK(data != nullptr);
    DCHECK(output != nullptr);

    RETURN_IF_ERROR(RoaringBitmap64::Deserialize(data + DELETION_VECTOR_BLOB_HEADER_SIZE,
        length - DELETION_VECTOR_BLOB_HEADER_SIZE, output));

    VLOG(2) << "Deserialized deletion vector with " << output->Cardinality()
               << " deleted positions (min: " << output->Min()
               << ", max: " << output->Max() << ")";

    return Status::OK();
  }
};

/// Specialized blob reader for Puffin deletion vector blobs.
/// Uses the BlobReader template with DeletionVectorDeserializer for deserialization.
using DeletionVectorBlobReader = BlobReader<RoaringBitmap64, DeletionVectorDeserializer>;

} // namespace impala
