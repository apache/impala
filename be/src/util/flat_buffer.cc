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

#include "util/flat_buffer.h"

#include <gutil/strings/substitute.h>

using namespace apache::thrift;
using namespace impala;
using namespace org::apache::impala::fb;

namespace impala {

Status FromFbCompression(
    FbCompression fb_compression, THdfsCompression::type* thrift_compression) {
  switch (fb_compression) {
    case FbCompression_NONE:
      *thrift_compression = THdfsCompression::NONE;
      break;
    case FbCompression_DEFAULT:
      *thrift_compression = THdfsCompression::DEFAULT;
      break;
    case FbCompression_GZIP:
      *thrift_compression = THdfsCompression::GZIP;
      break;
    case FbCompression_DEFLATE:
      *thrift_compression = THdfsCompression::DEFLATE;
      break;
    case FbCompression_BZIP2:
      *thrift_compression = THdfsCompression::BZIP2;
      break;
    case FbCompression_SNAPPY:
      *thrift_compression = THdfsCompression::SNAPPY;
      break;
    case FbCompression_SNAPPY_BLOCKED:
      *thrift_compression = THdfsCompression::SNAPPY_BLOCKED;
      break;
    case FbCompression_LZO:
      *thrift_compression = THdfsCompression::LZO;
      break;
    case FbCompression_LZ4:
      *thrift_compression = THdfsCompression::LZ4;
      break;
    case FbCompression_ZLIB:
      *thrift_compression = THdfsCompression::ZLIB;
      break;
    case FbCompression_ZSTD:
      *thrift_compression = THdfsCompression::ZSTD;
      break;
    default:
      return Status(strings::Substitute(
          "Invalid file descriptor compression code: $0", fb_compression));
  }
  return Status::OK();
}

} // namespace impala
