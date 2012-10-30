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

#ifdef IR_COMPILE
#include "runtime/string-value.inline.h"

using namespace impala;

extern "C" 
bool StringValueEQ(const StringValue* s1, const StringValue* s2) {
  return s1->Eq(*s2);
}

extern "C"
bool StringValueNE(const StringValue* s1, const StringValue* s2) {
  return s1->Ne(*s2);
}

extern "C"
bool StringValueLT(const StringValue* s1, const StringValue* s2) {
  return s1->Lt(*s2);
}

extern "C"
bool StringValueLE(const StringValue* s1, const StringValue* s2) {
  return s1->Le(*s2);
}

extern "C"
bool StringValueGT(const StringValue* s1, const StringValue* s2) {
  return s1->Gt(*s2);
}

extern "C"
bool StringValueGE(const StringValue* s1, const StringValue* s2) {
  return s1->Ge(*s2);
}
#else
#error "This file should only be used for cross compiling to IR."
#endif

