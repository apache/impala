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

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace impala {

namespace puffin {

enum class CompressionCodec { NONE = 0, LZ4 = 1, ZSTD = 2 };

enum class BlobType {
  DATA = 0,
  APACHE_DATA_SKETCHES_THETA_V1 = 1,
  DELETION_VECTOR_V1 = 2
};
struct BlobMetadata {
  /// Type of the blob
  BlobType type = BlobType::DATA;

  /// List of field IDs (Iceberg field IDs) that this blob applies to
  std::vector<int32_t> fields;

  /// Snapshot ID this blob is associated with
  int64_t snapshot_id = 0;

  /// Sequence number for ordering within a snapshot
  int64_t sequence_number = 0;

  /// Byte offset of the blob data within the Puffin file
  int64_t offset = 0;

  /// Length of the blob data in bytes
  size_t length = 0;

  /// Compression codec used for the blob data
  CompressionCodec compression_codec = CompressionCodec::NONE;

  /// Additional properties as key-value pairs
  std::map<std::string, std::string> properties;

  BlobMetadata(BlobType type, size_t length) : type(type), length(length) {}
};
struct BlobData {
  uint8_t* data;
  size_t length;
  BlobData(uint8_t* data, size_t length) : data(data), length(length) {}
};

struct Blob {
  Blob(const BlobMetadata& metadata, BlobData data) : metadata(metadata), data(data) {}
  BlobMetadata metadata;
  BlobData data;
};

class File {
  using FileMetadata = std::map<std::string, std::string>;

 public:
  File() = default;

  void AddBlob(const Blob& blob) { blobs_.push_back(blob); }

  const std::vector<Blob>& GetBlobs() const { return blobs_; }

  std::vector<Blob>& GetBlobs() { return blobs_; }

  const FileMetadata& GetFileMetadata() const { return file_metadata_; }

  FileMetadata& GetFileMetadata() { return file_metadata_; }

 private:
  FileMetadata file_metadata_;
  std::vector<Blob> blobs_;
};

inline std::string CompressionCodecToString(CompressionCodec codec) {
  switch (codec) {
    case CompressionCodec::NONE:
      return "none";
    case CompressionCodec::LZ4:
      return "lz4";
    case CompressionCodec::ZSTD:
      return "zstd";
    default:
      return "unknown";
  }
}

inline CompressionCodec StringToCompressionCodec(const std::string& codec_str) {
  if (codec_str == "none") return CompressionCodec::NONE;
  if (codec_str == "lz4") return CompressionCodec::LZ4;
  if (codec_str == "zstd") return CompressionCodec::ZSTD;
  return CompressionCodec::NONE;
}

inline std::string BlobTypeToString(BlobType type) {
  switch (type) {
    case BlobType::DATA:
      return "data";
    case BlobType::APACHE_DATA_SKETCHES_THETA_V1:
      return "apache-datasketches-theta-v1";
    case BlobType::DELETION_VECTOR_V1:
      return "deletion-vector-v1";
    default:
      return "unknown";
  }
}

inline BlobType StringToBlobType(const std::string& type_str) {
  if (type_str == "data") return BlobType::DATA;
  if (type_str == "apache-datasketches-theta-v1")
    return BlobType::APACHE_DATA_SKETCHES_THETA_V1;
  if (type_str == "deletion-vector-v1") return BlobType::DELETION_VECTOR_V1;
  return BlobType::DATA;
}

} // namespace puffin
} // namespace impala
