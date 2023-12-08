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

#include "util/coding-util.h"

#include <cctype>
#include <iomanip>
#include <limits>
#include <sstream>
#include <unordered_set>

#include <boost/algorithm/string.hpp>
#include <boost/function.hpp>
#include <sasl/sasl.h>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "common/names.h"
#include "sasl/saslutil.h"

using boost::algorithm::is_any_of;
using namespace impala;
using std::uppercase;

namespace impala {

// It is more convenient to maintain the complement of the set of
// characters to escape when not in Hive-compat mode.
static function<bool (char)> ShouldNotEscape = is_any_of("-_.~");

// Hive selectively encodes characters. This is the whitelist of
// characters it will encode.
// See common/src/java/org/apache/hadoop/hive/common/FileUtils.java
// in the Hive source code for the source of this list.
static const std::unordered_set<char> SpecialCharacters = {
    '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\b', '\t', '\n',
    '\v', '\f', '\r',  '\x0E', '\x0F', '\x10', '\x11', '\x12', '\x13', '\x14',
    '\x15', '\x16', '\x17', '\x18', '\x19', '\x1A', '\x1B', '\x1C', '\x1D', '\x1E',
    '\x1F', '\x7F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '{', '[', ']',
    '^'};

static inline void UrlEncode(const char* in, int in_len, string* out, bool hive_compat) {
  stringstream ss;
  // "uppercase" and "hex" only affect the insertion of integers, not that of char values.
  ss << uppercase << hex << setfill('0');
  for (char ch : std::string_view(in, in_len)) {
    // Escape the character iff a) we are in Hive-compat mode and the
    // character is in the Hive whitelist or b) we are not in Hive-compat mode and
    // the character is not alphanumeric and it is not one of the characters specifically
    // excluded from escaping (see ShouldNotEscape()).
    if ((hive_compat && SpecialCharacters.count(ch) > 0) || (!hive_compat &&
            !isalnum(static_cast<unsigned char>(ch)) && !ShouldNotEscape(ch))) {
      ss << '%' << setw(2) << static_cast<uint32_t>(static_cast<unsigned char>(ch));
    } else {
      ss << ch;
    }
  }
  (*out) = ss.str();
}

void UrlEncode(const vector<uint8_t>& in, string* out, bool hive_compat) {
  if (in.empty()) {
    *out = "";
  } else {
    UrlEncode(reinterpret_cast<const char*>(in.data()), in.size(), out, hive_compat);
  }
}

void UrlEncode(const string& in, string* out, bool hive_compat) {
  UrlEncode(in.c_str(), in.size(), out, hive_compat);
}

// Adapted from
// http://www.boost.org/doc/libs/1_40_0/doc/html/boost_asio/
//   example/http/server3/request_handler.cpp
// See http://www.boost.org/LICENSE_1_0.txt for license for this method.
bool UrlDecode(const string& in, string* out, bool hive_compat) {
  out->clear();
  out->reserve(in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    if (in[i] == '%') {
      if (i + 3 <= in.size()) {
        int value = 0;
        istringstream is(in.substr(i + 1, 2));
        if (is >> hex >> value) {
          (*out) += static_cast<char>(value);
          i += 2;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else if (!hive_compat && in[i] == '+') { // Hive does not encode ' ' as '+'
      (*out) += ' ';
    } else {
      (*out) += in[i];
    }
  }
  return true;
}

bool Base64EncodeBufLen(int64_t in_len, int64_t* out_max) {
  // Base64 encoding turns every 3 bytes into 4 characters. If the length is not
  // divisible by 3, it pads the input with extra 0 bytes until it is divisible by 3.
  // One more character must be allocated to account for Base64Encode's null-padding
  // of its output.
  *out_max = 1 + 4 * ((in_len + 2) / 3);
  if (UNLIKELY(in_len < 0 ||
        *out_max > static_cast<unsigned>(std::numeric_limits<int>::max()))) {
    return false;
  }
  return true;
}

bool Base64Encode(const char* in, int64_t in_len, int64_t out_max, char* out,
    unsigned* out_len) {
  if (UNLIKELY(in_len < 0 || in_len > std::numeric_limits<unsigned>::max() ||
        out_max < 0 || out_max > std::numeric_limits<unsigned>::max())) {
    return false;
  }
  const int encode_result = sasl_encode64(in, static_cast<unsigned>(in_len), out,
      static_cast<unsigned>(out_max), out_len);
  if (UNLIKELY(encode_result != SASL_OK || *out_len != out_max - 1)) return false;
  return true;
}

void Base64Encode(const char* in, int64_t in_len, stringstream* out) {
  if (in_len == 0) {
    (*out) << "";
    return;
  }
  int64_t out_max = 0;
  if (UNLIKELY(!Base64EncodeBufLen(in_len, &out_max))) return;
  string result(out_max, '\0');
  unsigned out_len = 0;
  if (UNLIKELY(!Base64Encode(in, in_len, out_max, const_cast<char*>(result.c_str()),
          &out_len))) {
    return;
  }
  result.resize(out_len);
  (*out) << result;
}

void Base64Encode(const vector<uint8_t>& in, string* out) {
  if (in.empty()) {
    *out = "";
  } else {
    stringstream ss;
    Base64Encode(in, &ss);
    *out = ss.str();
  }
}

void Base64Encode(const vector<uint8_t>& in, stringstream* out) {
  if (!in.empty()) {
    // Boost does not like non-null terminated strings
    string tmp(reinterpret_cast<const char*>(in.data()), in.size());
    Base64Encode(tmp.c_str(), tmp.size(), out);
  }
}

void Base64Encode(const string& in, string* out) {
  stringstream ss;
  Base64Encode(in.c_str(), in.size(), &ss);
  *out = ss.str();
}

void Base64Encode(const string& in, stringstream* out) {
  Base64Encode(in.c_str(), in.size(), out);
}

bool Base64DecodeBufLen(const char* in, int64_t in_len, int64_t* out_max) {
  // Base64 decoding turns every 4 characters into 3 bytes. If the last character of the
  // encoded string is '=', that character (which represents 6 bits) and the last two bits
  // of the previous character is ignored, for a total of 8 ignored bits, therefore
  // producing one fewer byte of output. This is repeated if the second-to-last character
  // is '='. One more byte must be allocated to account for Base64Decode's null-padding
  // of its output.
  if (UNLIKELY((in_len & 3) != 0)) return false;
  *out_max = 1 + 3 * (in_len / 4);
  if (in[in_len - 1] == '=') {
    --(*out_max);
    if (in[in_len - 2] == '=') {
      --(*out_max);
    }
  }
  return true;
}

bool Base64Decode(const char* in, int64_t in_len, int64_t out_max, char* out,
    unsigned* out_len) {
  if (UNLIKELY((in_len & 3) != 0)) return false;
  const int decode_result = sasl_decode64(in, static_cast<unsigned>(in_len), out,
      static_cast<unsigned>(out_max), out_len);
  if (UNLIKELY(decode_result != SASL_OK || *out_len != out_max - 1)) return false;
  return true;
}

void EscapeForHtml(const string& in, stringstream* out) {
  DCHECK(out != NULL);
  for (const char c: in) {
    switch (c) {
      case '<': (*out) << "&lt;";
                break;
      case '>': (*out) << "&gt;";
                break;
      case '&': (*out) << "&amp;";
                break;
      default: (*out) << c;
    }
  }
}

}
