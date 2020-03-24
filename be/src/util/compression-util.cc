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
    case NONE: return CompressionTypePB::NONE;
    case DEFAULT: return CompressionTypePB::DEFAULT;
    case GZIP: return CompressionTypePB::GZIP;
    case DEFLATE: return CompressionTypePB::DEFLATE;
    case BZIP2: return CompressionTypePB::BZIP2;
    case SNAPPY: return CompressionTypePB::SNAPPY;
    case SNAPPY_BLOCKED: return CompressionTypePB::SNAPPY_BLOCKED;
    case LZO: return CompressionTypePB::LZO;
    case LZ4: return CompressionTypePB::LZ4;
    case ZLIB: return CompressionTypePB::ZLIB;
    case ZSTD: return CompressionTypePB::ZSTD;
    case BROTLI: return CompressionTypePB::BROTLI;
    case LZ4_BLOCKED: return CompressionTypePB::LZ4_BLOCKED;
  }
  DCHECK(false) << "Invalid compression type: " << compression;
  return CompressionTypePB::NONE;
}

THdfsCompression::type CompressionTypePBToThrift(const CompressionTypePB& compression) {
  switch(compression) {
    case NONE: return THdfsCompression::NONE;
    case DEFAULT: return THdfsCompression::DEFAULT;
    case GZIP: return THdfsCompression::GZIP;
    case DEFLATE: return THdfsCompression::DEFLATE;
    case BZIP2: return THdfsCompression::BZIP2;
    case SNAPPY: return THdfsCompression::SNAPPY;
    case SNAPPY_BLOCKED: return THdfsCompression::SNAPPY_BLOCKED;
    case LZO: return THdfsCompression::LZO;
    case LZ4: return THdfsCompression::LZ4;
    case ZLIB: return THdfsCompression::ZLIB;
    case ZSTD: return THdfsCompression::ZSTD;
    case BROTLI: return THdfsCompression::BROTLI;
    case LZ4_BLOCKED: return THdfsCompression::LZ4_BLOCKED;
  }
  DCHECK(false) << "Invalid compression type: " << compression;
  return THdfsCompression::NONE;
}

} // namespace impala
