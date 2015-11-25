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

#ifndef IMPALA_EXPRS_EXPR_VALUE_H
#define IMPALA_EXPRS_EXPR_VALUE_H

#include "runtime/collection-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"

namespace impala {

/// The materialized value returned by ExprContext::GetValue().
struct ExprValue {
  bool bool_val;
  int8_t tinyint_val;
  int16_t smallint_val;
  int32_t int_val;
  int64_t bigint_val;
  float float_val;
  double double_val;
  StringValue string_val;
  TimestampValue timestamp_val;
  Decimal4Value decimal4_val;
  Decimal8Value decimal8_val;
  Decimal16Value decimal16_val;
  CollectionValue collection_val;

  ExprValue()
    : bool_val(false),
      tinyint_val(0),
      smallint_val(0),
      int_val(0),
      bigint_val(0),
      float_val(0.0),
      double_val(0.0),
      string_val(NULL, 0),
      timestamp_val(),
      decimal4_val(),
      decimal8_val(),
      decimal16_val(),
      collection_val() {
  }

  ExprValue(bool v) : bool_val(v) {}
  ExprValue(int8_t v) : tinyint_val(v) {}
  ExprValue(int16_t v) : smallint_val(v) {}
  ExprValue(int32_t v) : int_val(v) {}
  ExprValue(int64_t v) : bigint_val(v) {}
  ExprValue(float v) : float_val(v) {}
  ExprValue(double v) : double_val(v) {}
  ExprValue(int64_t t, int64_t n) : timestamp_val(t, n) {}

  /// c'tor for string values
  ExprValue(const std::string& str)
    : string_data(str) {
    string_val.ptr = const_cast<char*>(string_data.data());
    string_val.len = string_data.size();
  }

  /// Sets the value for type to '0' and returns a pointer to the data
  void* SetToZero(const ColumnType& type) {
    switch (type.type) {
      case TYPE_NULL:
        return NULL;
      case TYPE_BOOLEAN:
        bool_val = false;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = 0;
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = 0;
        return &smallint_val;
      case TYPE_INT:
        int_val = 0;
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = 0;
        return &bigint_val;
      case TYPE_FLOAT:
        float_val = 0;
        return &float_val;
      case TYPE_DOUBLE:
        double_val = 0;
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }

  /// Sets the value for type to min and returns a pointer to the data
  void* SetToMin(const ColumnType& type) {
    switch (type.type) {
      case TYPE_NULL:
        return NULL;
      case TYPE_BOOLEAN:
        bool_val = false;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = std::numeric_limits<int8_t>::min();
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = std::numeric_limits<int16_t>::min();
        return &smallint_val;
      case TYPE_INT:
        int_val = std::numeric_limits<int32_t>::min();
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = std::numeric_limits<int64_t>::min();
        return &bigint_val;
      case TYPE_FLOAT:
        // For floats and doubles, numeric_limits::min() is the smallest positive
        // representable value.
        float_val = -std::numeric_limits<float>::max();
        return &float_val;
      case TYPE_DOUBLE:
        double_val = -std::numeric_limits<double>::max();
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }

  /// Sets the value for type to max and returns a pointer to the data
  void* SetToMax(const ColumnType& type) {
    switch (type.type) {
      case TYPE_NULL:
        return NULL;
      case TYPE_BOOLEAN:
        bool_val = true;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = std::numeric_limits<int8_t>::max();
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = std::numeric_limits<int16_t>::max();
        return &smallint_val;
      case TYPE_INT:
        int_val = std::numeric_limits<int32_t>::max();
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = std::numeric_limits<int64_t>::max();
        return &bigint_val;
      case TYPE_FLOAT:
        float_val = std::numeric_limits<float>::max();
        return &float_val;
      case TYPE_DOUBLE:
        double_val = std::numeric_limits<double>::max();
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }

 private:
  std::string string_data; // Stores the data for string_val if necessary.
};

}

#endif
