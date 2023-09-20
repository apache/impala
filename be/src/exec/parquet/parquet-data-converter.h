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

#include <algorithm>

#include "gen-cpp/parquet_types.h"

#include "runtime/decimal-value.inline.h"
#include "runtime/runtime-state.h"

namespace impala {

/// Utility class for converting Parquet data to the proper data type that is used
/// in the tuple's slot.
template <typename InternalType, bool MATERIALIZED>
class ParquetDataConverter {
 public:
  ParquetDataConverter(const parquet::SchemaElement* elem, const ColumnType* col_type) :
      parquet_element_(elem), col_type_(col_type) {
    needs_conversion_ = CheckIfNeedsConversion();
  }

  /// Converts and writes 'src' into 'slot' while doing the necessary conversions
  /// It shouldn't be invoked when conversion is not needed.
  bool ConvertSlot(const InternalType* src, void* slot) const {
    DCHECK(false);
    return false;
  }

  bool NeedsConversion() const { return needs_conversion_; }

  /// Sets extra information that is only needed for decoding TIMESTAMPs.
  void SetTimestampDecoder(const ParquetTimestampDecoder& timestamp_decoder) {
    DCHECK_EQ(col_type_->type, TYPE_TIMESTAMP);
    timestamp_decoder_ = timestamp_decoder;
    needs_conversion_ = timestamp_decoder_.NeedsConversion();
  }

  ParquetTimestampDecoder& timestamp_decoder() {
    return timestamp_decoder_;
  }

 private:
  int32_t GetScale() const {
    if (parquet_element_->__isset.logicalType
        && parquet_element_->logicalType.__isset.DECIMAL) {
      return parquet_element_->logicalType.DECIMAL.scale;
    }

    if (parquet_element_->__isset.scale) return parquet_element_->scale;

    // If not specified, the scale is 0
    return 0;
  }

  int32_t GetPrecision() const {
    if (parquet_element_->__isset.logicalType
        && parquet_element_->logicalType.__isset.DECIMAL) {
      return parquet_element_->logicalType.DECIMAL.precision;
    }

    return parquet_element_->precision;
  }
  /// Returns true if we need to do a conversion from the Parquet type to the slot type.
  bool CheckIfNeedsConversion() {
    if (!MATERIALIZED) return false;
    if (col_type_->type == TYPE_TIMESTAMP) {
      return timestamp_decoder_.NeedsConversion();
    }
    if (col_type_->type == TYPE_CHAR) {
      return true;
    }
    if (col_type_->type == TYPE_DECIMAL) {
      if (col_type_->precision != GetPrecision()) {
        // Decimal values can be stored by Decimal4Value (4 bytes), Decimal8Value, and
        // Decimal16Value. We only need to do a conversion for different precision if
        // the values require different types (different byte size).
        if (ColumnType::GetDecimalByteSize(GetPrecision()) != col_type_->GetByteSize()) {
          return true;
        }
      }
      // Different scales require decimal conversion.
      if (col_type_->scale != GetScale()) return true;
    }
    return false;
  }

  /// Converts decimal slot to proper precision/scale.
  bool ConvertDecimalSlot(const InternalType* src, void* slot) const;
  template <typename DecimalType>
  bool ConvertDecimalScale(DecimalType* slot) const;

  /// Parquet schema node of the field.
  const parquet::SchemaElement* parquet_element_;
  /// Impala slot descriptor of the field.
  const ColumnType* col_type_;
  /// Contains extra data needed for Timestamp decoding.
  ParquetTimestampDecoder timestamp_decoder_;
  /// True if the slot needs conversion.
  bool needs_conversion_ = false;
};

template <>
inline bool ParquetDataConverter<StringValue, true>::ConvertSlot(
    const StringValue* src, void* slot) const {
  DCHECK_EQ(col_type_->type, TYPE_CHAR);
  int char_len = col_type_->len;
  int unpadded_len = std::min(char_len, src->Len());
  char* dst_char = reinterpret_cast<char*>(slot);
  memcpy(dst_char, src->Ptr(), unpadded_len);
  StringValue::PadWithSpaces(dst_char, char_len, unpadded_len);
  return true;
}

template <>
inline bool ParquetDataConverter<TimestampValue, true>::ConvertSlot(
    const TimestampValue* src, void* slot) const {
  // Conversion should only happen when this flag is enabled.
  DCHECK(timestamp_decoder_.NeedsConversion());
  TimestampValue* dst_ts = reinterpret_cast<TimestampValue*>(slot);
  *dst_ts = *src;
  // TODO: IMPALA-7862: converting timestamps after validating them can move them out of
  // range. We should either validate after conversion or require conversion to produce an
  // in-range value.
  timestamp_decoder_.ConvertToLocalTime(dst_ts);
  return true;
}

#define DECIMAL_CONVERT_SLOT(DecimalValue)                                               \
template <>                                                                              \
inline bool ParquetDataConverter<DecimalValue, true>::ConvertSlot(                       \
    const DecimalValue* src, void* slot) const {                                         \
  return ConvertDecimalSlot(src, slot);                                                  \
}

DECIMAL_CONVERT_SLOT(Decimal4Value)
DECIMAL_CONVERT_SLOT(Decimal8Value)
DECIMAL_CONVERT_SLOT(Decimal16Value)

template <typename InternalType, bool MATERIALIZED>
inline bool ParquetDataConverter<InternalType, MATERIALIZED>::ConvertDecimalSlot(
    const InternalType* src, void* slot) const {
  // 'overflow' is required for ToDecimal*(), but we only allow higher precision in the
  // table metadata than the file metadata, i.e. it should never overflow.
  bool overflow = false;
  switch (col_type_->GetByteSize()) {
    case 4: {
      auto dst_dec4 = reinterpret_cast<Decimal4Value*>(slot);
      *dst_dec4 = ToDecimal4(*src, &overflow);
      DCHECK(!overflow);
      return ConvertDecimalScale(dst_dec4);
    }
    case 8: {
      auto dst_dec8 = reinterpret_cast<Decimal8Value*>(slot);
      *dst_dec8 = ToDecimal8(*src, &overflow);
      DCHECK(!overflow);
      return ConvertDecimalScale(dst_dec8);
    }
    case 16: {
      auto dst_dec16 = reinterpret_cast<Decimal16Value*>(slot);
      *dst_dec16 = ToDecimal16(*src, &overflow);
      DCHECK(!overflow);
      return ConvertDecimalScale(dst_dec16);
    }
    default:
      DCHECK(false) << "Internal error: cannot handle decimals of precision "
                    << col_type_->precision;
      return false;
  }
}

template <typename InternalType, bool MATERIALIZED>
template <typename DecimalType>
inline bool ParquetDataConverter<InternalType, MATERIALIZED>
    ::ConvertDecimalScale(DecimalType* slot) const {
  int parquet_file_scale = GetScale();
  int slot_scale = col_type_->scale;
  if (LIKELY(parquet_file_scale == slot_scale)) return true;

  bool overflow = false;
  *slot = slot->ScaleTo(parquet_file_scale, slot_scale, col_type_->precision,
                        /*round=*/true, &overflow);
  if (UNLIKELY(overflow)) return false;
  return true;
}

} // namespace impala
