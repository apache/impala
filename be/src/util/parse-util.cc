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

#include <zstd.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "util/mem-info.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::algorithm::trim;

namespace impala {

int64_t ParseUtil::ParseMemSpec(const string& mem_spec_str, bool* is_percent,
    int64_t relative_reference) {
  *is_percent = false;
  if (mem_spec_str.empty()) return 0;

  int64_t multiplier = -1;
  int32_t number_str_len = mem_spec_str.size();

  // Look for an accepted suffix such as "MB", "M", or "%".
  string::const_reverse_iterator suffix_char = mem_spec_str.rbegin();
  if (*suffix_char == 'b' || *suffix_char == 'B') {
    // Skip "B", the default is bytes anyways.
    if (suffix_char == mem_spec_str.rend()) return -1;
    suffix_char++;
    number_str_len--;
  }
  switch (*suffix_char) {
    case 't':
    case 'T':
      // Terabytes.
      number_str_len--;
      multiplier = 1024L * 1024L * 1024L * 1024L;
      break;
    case 'g':
    case 'G':
      // Gigabytes.
      number_str_len--;
      multiplier = 1024L * 1024L * 1024L;
      break;
    case 'm':
    case 'M':
      // Megabytes.
      number_str_len--;
      multiplier = 1024L * 1024L;
      break;
    case 'k':
    case 'K':
      // Kilobytes
      number_str_len--;
      multiplier = 1024L;
      break;
    case '%':
      // Don't allow a suffix of "%B".
      if (suffix_char != mem_spec_str.rbegin()) return -1;
      number_str_len--;
      *is_percent = true;
      break;
    // The default is bytes. If there was a trailing "B" it was handled above.
  }

  StringParser::ParseResult result;
  int64_t bytes;
  if (multiplier != -1) {
    // Parse float - MB or GB
    double limit_val = StringParser::StringToFloat<double>(mem_spec_str.data(),
        number_str_len, &result);
    if (result != StringParser::PARSE_SUCCESS) return -1;
    bytes = multiplier * limit_val;
  } else {
    // Parse int - bytes or percent
    int64_t limit_val = StringParser::StringToInt<int64_t>(mem_spec_str.data(),
        number_str_len, &result);
    if (result != StringParser::PARSE_SUCCESS) return -1;

    if (*is_percent) {
      bytes = (static_cast<double>(limit_val) / 100.0) * relative_reference;
    } else {
      bytes = limit_val;
    }
  }
  // Accept -1 as indicator for infinite memory that we report by a 0 return value.
  if (bytes == -1) {
    return 0;
  }

  return bytes;
}

Status ParseUtil::ParseCompressionCodec(
    const string& compression_codec, THdfsCompression::type* type, int* level) {
  // Acceptable values are:
  // - zstd:compression_level
  // - codec
  vector<string> tokens;
  split(tokens, compression_codec, is_any_of(":"), token_compress_on);
  if (tokens.size() > 2) return Status("Invalid compression codec value");

  string& codec_name = tokens[0];
  trim(codec_name);
  int compression_level = ZSTD_CLEVEL_DEFAULT;
  THdfsCompression::type enum_type;
  RETURN_IF_ERROR(GetThriftEnum(
      codec_name, "compression codec", _THdfsCompression_VALUES_TO_NAMES, &enum_type));

  if (tokens.size() == 2) {
    if (enum_type != THdfsCompression::ZSTD) {
      return Status("Compression level only supported for ZSTD");
    }
    StringParser::ParseResult status;
    string& clevel = tokens[1];
    trim(clevel);
    compression_level = StringParser::StringToInt<int>(
        clevel.c_str(), static_cast<int>(clevel.size()), &status);
    if (status != StringParser::PARSE_SUCCESS || compression_level < 1
        || compression_level > ZSTD_maxCLevel()) {
      return Status(Substitute("Invalid ZSTD compression level '$0'."
                               " Valid values are in [1,$1]",
          clevel, ZSTD_maxCLevel()));
    }
  }
  *type = enum_type;
  *level = compression_level;
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
