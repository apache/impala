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


#ifndef IMPALA_EXPRS_EXPR_INLINE_H
#define IMPALA_EXPRS_EXPR_INLINE_H

#include "exprs/expr.h"

namespace impala {

// Specialized ExprValue setters for all types.
template<> inline void* ExprValue::Set(const bool& val) {
  bool_val = val;
  return &bool_val;
}

template<> inline void* ExprValue::Set(const int8_t& val) {
  tinyint_val = val;
  return &tinyint_val;
}

template<> inline void* ExprValue::Set(const int16_t& val) {
  smallint_val = val;
  return &smallint_val;
}

template<> inline void* ExprValue::Set(const int32_t& val) {
  int_val = val;
  return &int_val;
}

template<> inline void* ExprValue::Set(const int64_t& val) {
  bigint_val = val;
  return &bigint_val;
}

template<> inline void* ExprValue::Set(const float& val) {
  float_val = val;
  return &float_val;
}

template<> inline void* ExprValue::Set(const double& val) {
  double_val = val;
  return &double_val;
}

template<> inline void* ExprValue::Set(const TimestampValue& val) {
  timestamp_val = val;
  return &timestamp_val;
}

template<> inline void* ExprValue::Set(const std::string& val) {
  SetStringVal(val);
  return &string_val;
}

template<> inline void* ExprValue::Set(const StringValue& val) {
  SetStringVal(val);
  return &string_val;
}

// Specialized ExprValue getters for all types.
template<> inline void* ExprValue::Get<bool>() { return &bool_val; }
template<> inline void* ExprValue::Get<int8_t>() { return &tinyint_val; }
template<> inline void* ExprValue::Get<int16_t>() { return &smallint_val; }
template<> inline void* ExprValue::Get<int32_t>() { return &int_val; }
template<> inline void* ExprValue::Get<int64_t>() { return &bigint_val; }
template<> inline void* ExprValue::Get<float>() { return &float_val; }
template<> inline void* ExprValue::Get<double>() { return &double_val; }
template<> inline void* ExprValue::Get<TimestampValue>() { return &timestamp_val; }
template<> inline void* ExprValue::Get<StringValue>() { return &string_val; }

}

#endif
