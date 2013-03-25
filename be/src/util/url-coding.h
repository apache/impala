// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef UTIL_URL_CODING_H
#define UTIL_URL_CODING_H

#include <string>
#include <vector>
#include <boost/cstdint.hpp>

namespace impala {

// Utility method to URL-encode a string (that is, replace special
// characters with %<hex value in ascii>).
void UrlEncode(const std::string& in, std::string* out);
void UrlEncode(const std::vector<uint8_t>& in, std::string* out);

// Utility method to decode a string that was URL-encoded. Returns
// true unless the string could not be correctly decoded.
bool UrlDecode(const std::string& in, std::string* out);

// Utility method to encode input as base-64 encoded.  This is not
// very performant (multiple string copies) and should not be used
// in a hot path.
void Base64Encode(const std::vector<uint8_t>& in, std::string* out);
void Base64Encode(const std::vector<uint8_t>& in, std::stringstream* out);
void Base64Encode(const std::string& in, std::string* out);
void Base64Encode(const std::string& in, std::stringstream* out);

// Utility method to decode base64 encoded strings.  Also not extremely
// performant.
// Returns true unless the string could not be correctly decoded.
bool Base64Decode(const std::string& in, std::string* out);

}

#endif
