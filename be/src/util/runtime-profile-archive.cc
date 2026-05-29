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

#include "common/object-pool.h"
#include "rpc/thrift-util.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "util/coding-util.h"
#include "util/compress.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

namespace impala {

namespace {

int32_t GetProfileVersion(const TRuntimeProfileTree& tree) {
  // Assume version 1 if not set, because profile_version was only added in v2.
  return tree.__isset.profile_version ? tree.profile_version : 1;
}

}

Status RuntimeProfile::DecompressToThrift(
    const vector<uint8_t>& compressed_profile, TRuntimeProfileTree* out) {
  scoped_ptr<Codec> decompressor;
  MemTracker mem_tracker;
  MemPool mem_pool(&mem_tracker);
  const auto close_mem_tracker = MakeScopeExitTrigger([&mem_pool, &mem_tracker]() {
    mem_pool.FreeAll();
    mem_tracker.Close();
  });
  RETURN_IF_ERROR(Codec::CreateDecompressor(
      &mem_pool, false, THdfsCompression::DEFAULT, &decompressor));
  const auto close_decompressor =
      MakeScopeExitTrigger([&decompressor]() { decompressor->Close(); });

  int64_t result_len;
  uint8_t* decompressed_buffer;
  RETURN_IF_ERROR(decompressor->ProcessBlock(false, compressed_profile.size(),
      compressed_profile.data(), &result_len, &decompressed_buffer));

  uint32_t deserialized_len = static_cast<uint32_t>(result_len);
  RETURN_IF_ERROR(
      DeserializeThriftMsg(decompressed_buffer, &deserialized_len, true, out));
  return Status::OK();
}

Status RuntimeProfile::DecompressToProfile(const vector<uint8_t>& compressed_profile,
    ObjectPool* pool, RuntimeProfile** out) {
  TRuntimeProfileTree thrift_profile;
  RETURN_IF_ERROR(
      RuntimeProfile::DecompressToThrift(compressed_profile, &thrift_profile));
  *out = RuntimeProfile::CreateFromThrift(pool, thrift_profile);
  return Status::OK();
}

Status RuntimeProfile::DeserializeFromArchiveString(
    const std::string& archive_str, TRuntimeProfileTree* out) {
  int64_t decoded_max;
  if (!Base64DecodeBufLen(archive_str.c_str(), archive_str.size(), &decoded_max)) {
    return Status("Error in DeserializeFromArchiveString: Base64DecodeBufLen failed.");
  }

  vector<uint8_t> decoded_buffer;
  decoded_buffer.resize(decoded_max);
  unsigned decoded_len;
  if (!Base64Decode(archive_str.c_str(), archive_str.size(), decoded_max,
          reinterpret_cast<char*>(decoded_buffer.data()), &decoded_len)) {
    return Status("Error in DeserializeFromArchiveString: Base64Decode failed.");
  }
  decoded_buffer.resize(decoded_len);
  return DecompressToThrift(decoded_buffer, out);
}

Status RuntimeProfile::CreateFromArchiveString(const string& archive_str,
    ObjectPool* pool, RuntimeProfile** out, int32_t* profile_version) {
  TRuntimeProfileTree tree;
  RETURN_IF_ERROR(DeserializeFromArchiveString(archive_str, &tree));
  *profile_version = GetProfileVersion(tree);
  *out = RuntimeProfile::CreateFromThrift(pool, tree);
  return Status::OK();
}

}
