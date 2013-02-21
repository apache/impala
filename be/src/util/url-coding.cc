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

#include <sstream>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>

#include "util/url-coding.h"

using namespace impala;
using namespace std;
using namespace boost;

namespace impala {

void UrlEncode(const string& in, string* out) {
  (*out).reserve(in.size());
  stringstream ss;
  BOOST_FOREACH(const char& ch, in) {
    if (isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
      ss << ch;
    } else {
      ss << '%' << uppercase << hex << static_cast<uint32_t>(ch);
    }
  }

  (*out) = ss.str();
}

// Adapted from
// http://www.boost.org/doc/libs/1_40_0/doc/html/boost_asio/
//   example/http/server3/request_handler.cpp
// See http://www.boost.org/LICENSE_1_0.txt for license for this method.
bool UrlDecode(const string& in, string* out) {
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
    } else if (in[i] == '+') {
      (*out) += ' ';
    } else {
      (*out) += in[i];
    }
  }
  return true;
}

}
