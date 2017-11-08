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

#include "util/string-parser.h"

namespace impala {

using ParseResult = StringParser::ParseResult;
Decimal4Value StringToDecimal4(const char* s, int len, int type_precision,
    int type_scale, bool round, StringParser::ParseResult* result) {
  return StringParser::StringToDecimal<int32_t>(s, len, type_precision,
      type_scale, round, result);
}

Decimal8Value StringToDecimal8(const char* s, int len, int type_precision,
    int type_scale, bool round, StringParser::ParseResult* result) {
  return StringParser::StringToDecimal<int64_t>(s, len, type_precision,
      type_scale, round, result);
}

Decimal16Value StringToDecimal16(const char* s, int len, int type_precision,
    int type_scale, bool round, StringParser::ParseResult* result) {
  return StringParser::StringToDecimal<int128_t>(s, len, type_precision,
      type_scale, round, result);
}
}
