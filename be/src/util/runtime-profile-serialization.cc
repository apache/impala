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

#include "util/runtime-profile-counters.h"

#include <sstream>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include "gen-cpp/Data_types.h"
#include "rpc/thrift-util.h"
#include "util/coding-util.h"
#include "util/compress.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

namespace impala {

Status RuntimeProfile::Compress(vector<uint8_t>* out) const {
  Status status;
  TRuntimeProfileTree thrift_object;
  const_cast<RuntimeProfile*>(this)->ToThrift(&thrift_object);
  ThriftSerializer serializer(true);
  vector<uint8_t> serialized_buffer;
  RETURN_IF_ERROR(serializer.SerializeToVector(&thrift_object, &serialized_buffer));

  // Compress the serialized thrift string.  This uses string keys and is very
  // easy to compress.
  scoped_ptr<Codec> compressor;
  Codec::CodecInfo codec_info(THdfsCompression::DEFAULT);
  RETURN_IF_ERROR(Codec::CreateCompressor(NULL, false, codec_info, &compressor));
  const auto close_compressor =
      MakeScopeExitTrigger([&compressor]() { compressor->Close(); });

  int64_t max_compressed_size = compressor->MaxOutputLen(serialized_buffer.size());
  DCHECK_GT(max_compressed_size, 0);
  out->resize(max_compressed_size);
  int64_t result_len = out->size();
  uint8_t* compressed_buffer_ptr = out->data();
  RETURN_IF_ERROR(compressor->ProcessBlock(true, serialized_buffer.size(),
      serialized_buffer.data(), &result_len, &compressed_buffer_ptr));
  out->resize(result_len);
  return Status::OK();
}

Status RuntimeProfile::SerializeToArchiveString(string* out) const {
  stringstream ss;
  RETURN_IF_ERROR(SerializeToArchiveString(&ss));
  *out = ss.str();
  return Status::OK();
}

Status RuntimeProfile::SerializeToArchiveString(stringstream* out) const {
  vector<uint8_t> compressed_buffer;
  RETURN_IF_ERROR(Compress(&compressed_buffer));
  Base64Encode(compressed_buffer, out);
  return Status::OK();
}

}
