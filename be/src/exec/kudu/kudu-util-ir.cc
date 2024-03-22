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

#include "exec/kudu/kudu-util.h"
#include <kudu/common/partial_row.h>
#include "runtime/decimal-value.h"

#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"

namespace impala {

// Converts a TimestampValue to Kudu's representation which is returned in 'ts_micros'.
Status ConvertTimestampValueToKudu(const TimestampValue* tv, int64_t* ts_micros) {
  bool success = tv->UtcToUnixTimeMicros(ts_micros);
  DCHECK(success); // If the value was invalid the slot should've been null.
  if (UNLIKELY(!success)) {
    return Status(TErrorCode::RUNTIME_ERROR,
        "Invalid TimestampValue in function ConvertTimestampValueToKudu: "
        + tv->ToString());
  }
  return Status::OK();
}

// Converts a DateValue to Kudu's representation which is returned in 'days'.
Status ConvertDateValueToKudu(const DateValue* dv, int32_t* days) {
  bool success = dv->ToDaysSinceEpoch(days);
  DCHECK(success); // If the value was invalid the slot should've been null.
  if (UNLIKELY(!success)) {
    return Status(TErrorCode::RUNTIME_ERROR,
        "Invalid DateValue in function ConvertDateValueToKudu" + dv->ToString());
  }
  return Status::OK();
}

Status WriteKuduValue(int col, const ColumnType& col_type, const void* value,
    bool copy_strings, kudu::KuduPartialRow* row) {
  // TODO: codegen this to eliminate branching on type.
  PrimitiveType type = col_type.type;
  const char* VALUE_ERROR_MSG = "Could not set Kudu row value.";
  switch (type) {
    case TYPE_VARCHAR: {
      const StringValue* sv = reinterpret_cast<const StringValue*>(value);
      kudu::Slice slice(reinterpret_cast<uint8_t*>(sv->Ptr()), sv->Len());
      if (copy_strings) {
        KUDU_RETURN_IF_ERROR(row->SetVarchar(col, slice), VALUE_ERROR_MSG);
      } else {
        KUDU_RETURN_IF_ERROR(row->SetVarcharNoCopyUnsafe(col, slice), VALUE_ERROR_MSG);
      }
      break;
    }
    case TYPE_STRING: {
      const StringValue* sv = reinterpret_cast<const StringValue*>(value);
      kudu::Slice slice(reinterpret_cast<uint8_t*>(sv->Ptr()), sv->Len());
      if (col_type.IsBinaryType()) {
        if (copy_strings) {
          KUDU_RETURN_IF_ERROR(
              row->SetBinary(col, slice), VALUE_ERROR_MSG);
        } else {
          KUDU_RETURN_IF_ERROR(
              row->SetBinaryNoCopy(col, slice), VALUE_ERROR_MSG);
        }
      } else {
        if (copy_strings) {
          KUDU_RETURN_IF_ERROR(
              row->SetString(col, slice), VALUE_ERROR_MSG);
        } else {
          KUDU_RETURN_IF_ERROR(
              row->SetStringNoCopy(col, slice), VALUE_ERROR_MSG);
        }
      }
      break;
    }
    case TYPE_FLOAT:
      KUDU_RETURN_IF_ERROR(row->SetFloat(col, *reinterpret_cast<const float*>(value)),
          VALUE_ERROR_MSG);
      break;
    case TYPE_DOUBLE:
      KUDU_RETURN_IF_ERROR(row->SetDouble(col, *reinterpret_cast<const double*>(value)),
          VALUE_ERROR_MSG);
      break;
    case TYPE_BOOLEAN:
      KUDU_RETURN_IF_ERROR(row->SetBool(col, *reinterpret_cast<const bool*>(value)),
          VALUE_ERROR_MSG);
      break;
    case TYPE_TINYINT:
      KUDU_RETURN_IF_ERROR(row->SetInt8(col, *reinterpret_cast<const int8_t*>(value)),
          VALUE_ERROR_MSG);
      break;
    case TYPE_SMALLINT:
      KUDU_RETURN_IF_ERROR(row->SetInt16(col, *reinterpret_cast<const int16_t*>(value)),
          VALUE_ERROR_MSG);
      break;
    case TYPE_INT:
      KUDU_RETURN_IF_ERROR(row->SetInt32(col, *reinterpret_cast<const int32_t*>(value)),
          VALUE_ERROR_MSG);
      break;
    case TYPE_BIGINT:
      KUDU_RETURN_IF_ERROR(row->SetInt64(col, *reinterpret_cast<const int64_t*>(value)),
          VALUE_ERROR_MSG);
      break;
    case TYPE_TIMESTAMP:
      int64_t ts_micros;
      RETURN_IF_ERROR(ConvertTimestampValueToKudu(
          reinterpret_cast<const TimestampValue*>(value), &ts_micros));
      KUDU_RETURN_IF_ERROR(
          row->SetUnixTimeMicros(col, ts_micros), VALUE_ERROR_MSG);
      break;
    case TYPE_DATE:
    {
      int32_t days = 0;
      RETURN_IF_ERROR(ConvertDateValueToKudu(
            reinterpret_cast<const DateValue*>(value), &days));
      KUDU_RETURN_IF_ERROR(row->SetDate(col, days), VALUE_ERROR_MSG);
      break;
    }
    case TYPE_DECIMAL:
      switch (col_type.GetByteSize()) {
        case 4:
          KUDU_RETURN_IF_ERROR(
              row->SetUnscaledDecimal(
                  col, reinterpret_cast<const Decimal4Value*>(value)->value()),
              VALUE_ERROR_MSG);
          break;
        case 8:
          KUDU_RETURN_IF_ERROR(
              row->SetUnscaledDecimal(
                  col, reinterpret_cast<const Decimal8Value*>(value)->value()),
              VALUE_ERROR_MSG);
          break;
        case 16:
          KUDU_RETURN_IF_ERROR(
              row->SetUnscaledDecimal(
                  col, reinterpret_cast<const Decimal16Value*>(value)->value()),
              VALUE_ERROR_MSG);
          break;
        default:
          DCHECK(false) << "Unknown decimal byte size: " << col_type.GetByteSize();
      }
      break;
    default:
      return Status(TErrorCode::IMPALA_KUDU_TYPE_MISSING, TypeToString(type));
  }

  return Status::OK();
}

}
