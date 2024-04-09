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

#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

namespace impala {

/// Utility method to URL-encode a string (that is, replace special
/// characters with %<hex value in ascii>).
/// The optional parameter hive_compat controls whether we mimic Hive's
/// behaviour when encoding a string, which is only to encode certain
/// characters (excluding, e.g., ' ')
void UrlEncode(const std::string& in, std::string* out, bool hive_compat = false);
void UrlEncode(const std::vector<uint8_t>& in, std::string* out,
    bool hive_compat = false);

/// Utility method to decode a string that was URL-encoded. Returns
/// true unless the string could not be correctly decoded.
/// The optional parameter hive_compat controls whether or not we treat
/// the strings as encoded by Hive, which means selectively ignoring
/// certain characters like ' '.
bool UrlDecode(const std::string& in, std::string* out, bool hive_compat = false);

/// Calculate the maximum output buffer size needed for Base64Encode. Returns false if
/// in_len is negative or too large.
bool Base64EncodeBufLen(int64_t in_len, int64_t* out_max);

/// Returns true if encoded successfully, otherwise false. out points to the output
/// data, the space of size out_max should be allocated before calling this function.
/// out_len saves the actual length of encoded string.
bool Base64Encode(const char* in, int64_t in_len, int64_t out_max, char* out,
    unsigned* out_len);

/// Utility method to encode input as base-64 encoded. This is not
/// very performant (multiple string copies) and should not be used
/// in a hot path.
void Base64Encode(const std::vector<uint8_t>& in, std::string* out);
void Base64Encode(const std::vector<uint8_t>& in, std::stringstream* out);
void Base64Encode(const std::string& in, std::string* out);
void Base64Encode(const std::string& in, std::stringstream* out);
void Base64Encode(const char* in, int64_t in_len, std::stringstream* out);

/// Calculate the maximum output buffer size needed for Base64Decode. Returns false if
/// in_len is invalid.
bool Base64DecodeBufLen(const char* in, int64_t in_len, int64_t* out_max);

/// Utility method to decode a base-64 encoded string. Returns true if decoded
/// successfully, otherwise false. out points to the output data, the space of size
/// out_max should be allocated before calling this function. out_len saves the actual
/// length of decoded string.
bool Base64Decode(const char* in, int64_t in_len, int64_t out_max, char* out,
    unsigned* out_len);

/// Replaces &, < and > with &amp;, &lt; and &gt; respectively. This is
/// not the full set of required encodings, but one that should be
/// added to on a case-by-case basis. Slow, since it necessarily
/// inspects each character in turn, and copies them all to *out; use
/// judiciously.
void EscapeForHtml(const std::string& in, std::stringstream* out);
}
