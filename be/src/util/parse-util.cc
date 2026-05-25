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

#include "util/parse-util.h"

#include <sstream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "util/codec.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::algorithm::trim;

namespace impala {

Status ParseUtil::ParseCompressionCodec(
    const string& compression_codec, THdfsCompression::type* type,
    std::optional<int>* level) {
  // Acceptable values are:
  // - zstd, gzip :compression_level
  // - codec
  vector<string> tokens;
  split(tokens, compression_codec, is_any_of(":"), token_compress_on);
  if (tokens.size() > 2) return Status("Invalid compression codec value");

  string& codec_name = tokens[0];
  trim(codec_name);
  THdfsCompression::type enum_type;
  RETURN_IF_ERROR(GetThriftEnum(
      codec_name, "compression codec", _THdfsCompression_VALUES_TO_NAMES, &enum_type));

  *type = enum_type;

  if (tokens.size() == 2) {
    StringParser::ParseResult status;
    string& clevel = tokens[1];
    trim(clevel);
    int compression_level = StringParser::StringToInt<int>(
        clevel.c_str(), static_cast<int>(clevel.size()), &status);

    if (status == StringParser::PARSE_SUCCESS) {
      Status res = Codec::ValidateCompressionLevel(enum_type, compression_level);
      if (res.ok()) {
        level->emplace(compression_level);
      }
      return res;
    } else {
      return Status(Substitute("Invalid compression level value - $0"
          ", should be an integer", clevel));
    }
  }
  return Status::OK();
}

// Return all enum values in a string format, e.g. FOO(1), BAR(2), BAZ(3).
string GetThriftEnumValues(const map<int, const char*>& enum_values_to_names) {
  bool first = true;
  stringstream ss;
  for (const auto& e : enum_values_to_names) {
    if (!first) {
      ss << ", ";
    } else {
      first = false;
    }
    ss << e.second << "(" << e.first << ")";
  }
  return ss.str();
}
}
