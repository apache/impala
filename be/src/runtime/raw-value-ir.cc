// Copyright 2015 Cloudera Inc.
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

#include "runtime/raw-value.h"

using namespace impala;

int RawValue::Compare(const void* v1, const void* v2, const ColumnType& type) {
  const StringValue* string_value1;
  const StringValue* string_value2;
  const TimestampValue* ts_value1;
  const TimestampValue* ts_value2;
  float f1, f2;
  double d1, d2;
  int32_t i1, i2;
  int64_t b1, b2;
  switch (type.type) {
    case TYPE_NULL:
      return 0;
    case TYPE_BOOLEAN:
      return *reinterpret_cast<const bool*>(v1) - *reinterpret_cast<const bool*>(v2);
    case TYPE_TINYINT:
      return *reinterpret_cast<const int8_t*>(v1) - *reinterpret_cast<const int8_t*>(v2);
    case TYPE_SMALLINT:
      return *reinterpret_cast<const int16_t*>(v1) -
             *reinterpret_cast<const int16_t*>(v2);
    case TYPE_INT:
      i1 = *reinterpret_cast<const int32_t*>(v1);
      i2 = *reinterpret_cast<const int32_t*>(v2);
      return i1 > i2 ? 1 : (i1 < i2 ? -1 : 0);
    case TYPE_BIGINT:
      b1 = *reinterpret_cast<const int64_t*>(v1);
      b2 = *reinterpret_cast<const int64_t*>(v2);
      return b1 > b2 ? 1 : (b1 < b2 ? -1 : 0);
    case TYPE_FLOAT:
      // TODO: can this be faster? (just returning the difference has underflow problems)
      f1 = *reinterpret_cast<const float*>(v1);
      f2 = *reinterpret_cast<const float*>(v2);
      if (UNLIKELY(isnan(f1) && isnan(f2))) return 0;
      if (UNLIKELY(isnan(f1))) return -1;
      if (UNLIKELY(isnan(f2))) return 1;
      return f1 > f2 ? 1 : (f1 < f2 ? -1 : 0);
    case TYPE_DOUBLE:
      // TODO: can this be faster?
      d1 = *reinterpret_cast<const double*>(v1);
      d2 = *reinterpret_cast<const double*>(v2);
      if (isnan(d1) && isnan(d2)) return 0;
      if (isnan(d1)) return -1;
      if (isnan(d2)) return 1;
      return d1 > d2 ? 1 : (d1 < d2 ? -1 : 0);
    case TYPE_STRING:
    case TYPE_VARCHAR:
      string_value1 = reinterpret_cast<const StringValue*>(v1);
      string_value2 = reinterpret_cast<const StringValue*>(v2);
      return string_value1->Compare(*string_value2);
    case TYPE_TIMESTAMP:
      ts_value1 = reinterpret_cast<const TimestampValue*>(v1);
      ts_value2 = reinterpret_cast<const TimestampValue*>(v2);
      return *ts_value1 > *ts_value2 ? 1 : (*ts_value1 < *ts_value2 ? -1 : 0);
    case TYPE_CHAR: {
      const char* v1ptr = StringValue::CharSlotToPtr(v1, type);
      const char* v2ptr = StringValue::CharSlotToPtr(v2, type);
      int64_t l1 = StringValue::UnpaddedCharLength(v1ptr, type.len);
      int64_t l2 = StringValue::UnpaddedCharLength(v2ptr, type.len);
      return StringCompare(v1ptr, l1, v2ptr, l2, std::min(l1, l2));
    }
    case TYPE_DECIMAL:
      switch (type.GetByteSize()) {
        case 4:
          return reinterpret_cast<const Decimal4Value*>(v1)->Compare(
                 *reinterpret_cast<const Decimal4Value*>(v2));
        case 8:
          return reinterpret_cast<const Decimal8Value*>(v1)->Compare(
                 *reinterpret_cast<const Decimal8Value*>(v2));
        case 16:
          return reinterpret_cast<const Decimal16Value*>(v1)->Compare(
                 *reinterpret_cast<const Decimal16Value*>(v2));
        default:
          DCHECK(false) << type;
          return 0;
      }
    default:
      DCHECK(false) << "invalid type: " << type.DebugString();
      return 0;
  };
}
