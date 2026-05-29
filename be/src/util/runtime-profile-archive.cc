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

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <vector>

#include <thrift/TConfiguration.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <zlib.h>

#include "common/object-pool.h"
#include "gutil/strings/escaping.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace impala {

namespace {

// Keep this in sync with the default thrift_external_rpc_max_message_size without
// depending on thrift-util.cc, which pulls RPC/TLS dependencies into impala-profile-tool.
static const int64_t PROFILE_ARCHIVE_MAX_MESSAGE_SIZE = 2L * 1024 * 1024 * 1024;

shared_ptr<TConfiguration> ProfileArchiveTConfiguration() {
  shared_ptr<TConfiguration> config = make_shared<TConfiguration>();
  config->setMaxMessageSize(PROFILE_ARCHIVE_MAX_MESSAGE_SIZE);
  return config;
}

int32_t GetProfileVersion(const TRuntimeProfileTree& tree) {
  // Assume version 1 if not set, because profile_version was only added in v2.
  return tree.__isset.profile_version ? tree.profile_version : 1;
}

shared_ptr<TProtocol> CreateProfileArchiveDeserializeProtocol(
    shared_ptr<TMemoryBuffer> mem) {
  TCompactProtocolFactoryT<TMemoryBuffer> factory;
  return factory.getProtocol(move(mem));
}

Status DeserializeRuntimeProfileTree(
    const uint8_t* buf, uint32_t* len, TRuntimeProfileTree* out) {
  shared_ptr<TMemoryBuffer> transport(
      new TMemoryBuffer(const_cast<uint8_t*>(buf), *len,
          TMemoryBuffer::MemoryPolicy::OBSERVE, ProfileArchiveTConfiguration()));
  shared_ptr<TProtocol> protocol = CreateProfileArchiveDeserializeProtocol(transport);
  try {
    out->read(protocol.get());
  } catch (std::exception& e) {
    stringstream msg;
    msg << "couldn't deserialize thrift msg:\n" << e.what();
    return Status::Expected(msg.str());
  } catch (...) {
    return Status::Expected("Unknown exception");
  }
  uint32_t bytes_left = transport->available_read();
  *len = *len - bytes_left;
  return Status::OK();
}

Status DecodeArchiveString(const string& archive_str, vector<uint8_t>* out) {
  // Match Base64DecodeBufLen()'s length validation without linking coding-util.cc
  // and its SASL dependency into impala-profile-tool.
  if ((archive_str.size() & 3) != 0) {
    return Status::Expected(
        "Error in DeserializeFromArchiveString: invalid base64 length.");
  }

  string decoded_buffer_str;
  if (!Base64Unescape(archive_str, &decoded_buffer_str)) {
    return Status::Expected(
        "Error in DeserializeFromArchiveString: invalid base64 data.");
  }
  out->assign(decoded_buffer_str.begin(), decoded_buffer_str.end());
  return Status::OK();
}

}

Status RuntimeProfile::DecompressToThrift(
    const vector<uint8_t>& compressed_profile, TRuntimeProfileTree* out) {
  z_stream stream;
  memset(&stream, 0, sizeof(stream));

  static const int WINDOW_BITS = 15;
  static const int DETECT_CODEC = 32;
  int ret = inflateInit2(&stream, WINDOW_BITS | DETECT_CODEC);
  if (ret != Z_OK) {
    return Status::Expected("Error in DecompressToThrift: inflateInit2 failed: "
        + std::to_string(ret));
  }
  const auto close_stream = MakeScopeExitTrigger([&stream]() { inflateEnd(&stream); });

  stream.next_in = const_cast<Bytef*>(compressed_profile.data());
  if (compressed_profile.size() > std::numeric_limits<uInt>::max()) {
    return Status::Expected("Error in DecompressToThrift: compressed profile is too "
        "large for zlib input buffer.");
  }
  stream.avail_in = static_cast<uInt>(compressed_profile.size());

  const size_t max_message_size = static_cast<size_t>(PROFILE_ARCHIVE_MAX_MESSAGE_SIZE);
  const size_t initial_buffer_size =
      min(max_message_size, max<size_t>(compressed_profile.size() * 2, 1024));
  vector<uint8_t> decompressed_buffer(initial_buffer_size);
  do {
    if (stream.total_out == decompressed_buffer.size()) {
      if (decompressed_buffer.size() >= max_message_size) {
        return Status::Expected("Error in DecompressToThrift: decompressed profile "
            "exceeds the max thrift message size.");
      }
      decompressed_buffer.resize(min(decompressed_buffer.size() * 2,
          max_message_size));
    }
    stream.next_out = decompressed_buffer.data() + stream.total_out;
    const size_t output_remaining = decompressed_buffer.size() - stream.total_out;
    stream.avail_out = static_cast<uInt>(output_remaining);
    ret = inflate(&stream, Z_NO_FLUSH);
    if (ret != Z_OK && ret != Z_STREAM_END) {
      return Status::Expected("Error in DecompressToThrift: inflate failed: "
          + std::to_string(ret));
    }
  } while (ret != Z_STREAM_END);

  uint32_t deserialized_len = static_cast<uint32_t>(stream.total_out);
  RETURN_IF_ERROR(DeserializeRuntimeProfileTree(
      decompressed_buffer.data(), &deserialized_len, out));
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
  vector<uint8_t> decoded_buffer;
  RETURN_IF_ERROR(DecodeArchiveString(archive_str, &decoded_buffer));
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
