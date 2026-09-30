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

#include "util/compression-util.h"

#include "common/logging.h"

namespace impala {

CompressionTypePB THdfsCompressionToProto(const THdfsCompression::type& compression) {
  switch(compression) {
    case THdfsCompression::NONE: return CompressionTypePB::NONE;
    case THdfsCompression::DEFAULT: return CompressionTypePB::DEFAULT;
    case THdfsCompression::GZIP: return CompressionTypePB::GZIP;
    case THdfsCompression::DEFLATE: return CompressionTypePB::DEFLATE;
    case THdfsCompression::BZIP2: return CompressionTypePB::BZIP2;
    case THdfsCompression::SNAPPY: return CompressionTypePB::SNAPPY;
    case THdfsCompression::SNAPPY_BLOCKED: return CompressionTypePB::SNAPPY_BLOCKED;
    case THdfsCompression::LZO: return CompressionTypePB::LZO;
    case THdfsCompression::LZ4: return CompressionTypePB::LZ4;
    case THdfsCompression::ZLIB: return CompressionTypePB::ZLIB;
    case THdfsCompression::ZSTD: return CompressionTypePB::ZSTD;
    case THdfsCompression::BROTLI: return CompressionTypePB::BROTLI;
    case THdfsCompression::LZ4_BLOCKED: return CompressionTypePB::LZ4_BLOCKED;
  }
  DCHECK(false) << "Invalid compression type: " << compression;
  return CompressionTypePB::NONE;
}

THdfsCompression::type CompressionTypePBToThrift(const CompressionTypePB& compression) {
  switch(compression) {
    case CompressionTypePB::NONE: return THdfsCompression::NONE;
    case CompressionTypePB::DEFAULT: return THdfsCompression::DEFAULT;
    case CompressionTypePB::GZIP: return THdfsCompression::GZIP;
    case CompressionTypePB::DEFLATE: return THdfsCompression::DEFLATE;
    case CompressionTypePB::BZIP2: return THdfsCompression::BZIP2;
    case CompressionTypePB::SNAPPY: return THdfsCompression::SNAPPY;
    case CompressionTypePB::SNAPPY_BLOCKED: return THdfsCompression::SNAPPY_BLOCKED;
    case CompressionTypePB::LZO: return THdfsCompression::LZO;
    case CompressionTypePB::LZ4: return THdfsCompression::LZ4;
    case CompressionTypePB::ZLIB: return THdfsCompression::ZLIB;
    case CompressionTypePB::ZSTD: return THdfsCompression::ZSTD;
    case CompressionTypePB::BROTLI: return THdfsCompression::BROTLI;
    case CompressionTypePB::LZ4_BLOCKED: return THdfsCompression::LZ4_BLOCKED;
  }
  DCHECK(false) << "Invalid compression type: " << compression;
  return THdfsCompression::NONE;
}

} // namespace impala
