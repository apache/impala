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

#include "service/hs2-util.h"

#include "common/logging.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/types.h"
#include "util/bit-util.h"

#include <gutil/strings/substitute.h>

#include "common/names.h"

using namespace apache::hive::service::cli;
using namespace impala;
using namespace strings;

// Set the null indicator bit for row 'row_idx', assuming this will be called for
// successive increasing values of row_idx. If 'is_null' is true, the row_idx'th bit will
// be set in 'nulls' (taking the LSB as bit 0). If 'is_null' is false, the row_idx'th bit
// will be unchanged. If 'nulls' does not contain 'row_idx' bits, it will be extended by
// one byte.
inline void SetNullBit(uint32_t row_idx, bool is_null, string* nulls) {
  DCHECK_LE(row_idx / 8, nulls->size());
  int16_t mod_8 = row_idx % 8;
  if (mod_8 == 0) (*nulls) += '\0';
  (*nulls)[row_idx / 8] |= (1 << mod_8) * is_null;
}

inline bool GetNullBit(const string& nulls, uint32_t row_idx) {
  DCHECK_LE(row_idx / 8, nulls.size());
  return nulls[row_idx / 8] & (1 << row_idx % 8);
}

void impala::StitchNulls(uint32_t num_rows_before, uint32_t num_rows_added,
    uint32_t start_idx, const string& from, string* to) {
  // Round up to power-of-two to avoid accidentally quadratic behaviour from repeated
  // small increases in size.
  to->reserve(BitUtil::RoundUpToPowerOfTwo((num_rows_before + num_rows_added + 7) / 8));

  // TODO: This is very inefficient, since we could conceivably go one byte at a time
  // (although the operands should stay live in registers in the loop). However doing this
  // more efficiently leads to very complex code: we have to deal with the fact that
  // 'start_idx' and 'num_rows_before' might both lead to offsets into the null bitset
  // that don't start on a byte boundary. We should revisit this, ideally with a good
  // bitset implementation.
  for (int i = 0; i < num_rows_added; ++i) {
    SetNullBit(num_rows_before + i, GetNullBit(from, i + start_idx), to);
  }
}

// For V6 and above
void impala::TColumnValueToHS2TColumn(const TColumnValue& col_val,
    const TColumnType& type, uint32_t row_idx, thrift::TColumn* column) {
  string* nulls;
  bool is_null;
  switch (type.types[0].scalar_type.type) {
    case TPrimitiveType::NULL_TYPE:
    case TPrimitiveType::BOOLEAN:
      is_null = !col_val.__isset.bool_val;
      column->boolVal.values.push_back(col_val.bool_val);
      nulls = &column->boolVal.nulls;
      break;
    case TPrimitiveType::TINYINT:
      is_null = !col_val.__isset.byte_val;
      column->byteVal.values.push_back(col_val.byte_val);
      nulls = &column->byteVal.nulls;
      break;
    case TPrimitiveType::SMALLINT:
      is_null = !col_val.__isset.short_val;
      column->i16Val.values.push_back(col_val.short_val);
      nulls = &column->i16Val.nulls;
      break;
    case TPrimitiveType::INT:
      is_null = !col_val.__isset.int_val;
      column->i32Val.values.push_back(col_val.int_val);
      nulls = &column->i32Val.nulls;
      break;
    case TPrimitiveType::BIGINT:
      is_null = !col_val.__isset.long_val;
      column->i64Val.values.push_back(col_val.long_val);
      nulls = &column->i64Val.nulls;
      break;
    case TPrimitiveType::FLOAT:
    case TPrimitiveType::DOUBLE:
      is_null = !col_val.__isset.double_val;
      column->doubleVal.values.push_back(col_val.double_val);
      nulls = &column->doubleVal.nulls;
      break;
    case TPrimitiveType::TIMESTAMP:
    case TPrimitiveType::DATE:
    case TPrimitiveType::STRING:
    case TPrimitiveType::CHAR:
    case TPrimitiveType::VARCHAR:
    case TPrimitiveType::DECIMAL:
      is_null = !col_val.__isset.string_val;
      column->stringVal.values.push_back(col_val.string_val);
      nulls = &column->stringVal.nulls;
      break;
    default:
      DCHECK(false) << "Unhandled type: "
                    << TypeToString(ThriftToType(type.types[0].scalar_type.type));
      return;
  }

  SetNullBit(row_idx, is_null, nulls);
}

// Specialised per-type implementations of ExprValuesToHS2TColumn.

// Helper to reserve space in hs2Vals->values and hs2Vals->nulls for the values that the
// different implementations of ExprValuesToHS2TColumn will write.
template <typename T>
void ReserveSpace(int num_rows, uint32_t output_row_idx, T* hs2Vals) {
  DCHECK_GE(num_rows, 0);
  int64_t num_output_rows = output_row_idx + num_rows;
  int64_t num_null_bytes = BitUtil::RoundUpNumBytes(num_output_rows);
  // Round up reserve() arguments to power-of-two to avoid accidentally quadratic
  // behaviour from repeated small increases in size.
  hs2Vals->values.reserve(BitUtil::RoundUpToPowerOfTwo(num_output_rows));
  hs2Vals->nulls.reserve(BitUtil::RoundUpToPowerOfTwo(num_null_bytes));
}

// Implementation for BOOL.
static void BoolExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval, RowBatch* batch,
    int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->boolVal);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    BooleanVal val = expr_eval->GetBooleanVal(it.Get());
    column->boolVal.values.push_back(val.val);
    SetNullBit(output_row_idx, val.is_null, &column->boolVal.nulls);
    ++output_row_idx;
  }
}

// Implementation for TINYINT.
static void TinyIntExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval, RowBatch* batch,
    int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->byteVal);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    TinyIntVal val = expr_eval->GetTinyIntVal(it.Get());
    column->byteVal.values.push_back(val.val);
    SetNullBit(output_row_idx, val.is_null, &column->byteVal.nulls);
    ++output_row_idx;
  }
}

// Implementation for SMALLINT.
static void SmallIntExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval,
    RowBatch* batch, int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->i16Val);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    SmallIntVal val = expr_eval->GetSmallIntVal(it.Get());
    column->i16Val.values.push_back(val.val);
    SetNullBit(output_row_idx, val.is_null, &column->i16Val.nulls);
    ++output_row_idx;
  }
}

// Implementation for INT.
static void IntExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval, RowBatch* batch,
    int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->i32Val);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    DCHECK_EQ(output_row_idx, column->i32Val.values.size());
    IntVal val = expr_eval->GetIntVal(it.Get());
    column->i32Val.values.push_back(val.val);
    SetNullBit(output_row_idx, val.is_null, &column->i32Val.nulls);
    ++output_row_idx;
  }
}

// Implementation for BIGINT.
static void BigIntExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval, RowBatch* batch,
    int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->i64Val);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    BigIntVal val = expr_eval->GetBigIntVal(it.Get());
    column->i64Val.values.push_back(val.val);
    SetNullBit(output_row_idx, val.is_null, &column->i64Val.nulls);
    ++output_row_idx;
  }
}

// Implementation for FLOAT.
static void FloatExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval, RowBatch* batch,
    int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->doubleVal);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    FloatVal val = expr_eval->GetFloatVal(it.Get());
    column->doubleVal.values.push_back(val.val);
    SetNullBit(output_row_idx, val.is_null, &column->doubleVal.nulls);
    ++output_row_idx;
  }
}

// Implementation for DOUBLE.
static void DoubleExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval, RowBatch* batch,
    int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->doubleVal);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    DoubleVal val = expr_eval->GetDoubleVal(it.Get());
    column->doubleVal.values.push_back(val.val);
    SetNullBit(output_row_idx, val.is_null, &column->doubleVal.nulls);
    ++output_row_idx;
  }
}

// Implementation for TIMESTAMP.
static void TimestampExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval,
    RowBatch* batch, int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->stringVal);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    TimestampVal val = expr_eval->GetTimestampVal(it.Get());
    column->stringVal.values.emplace_back();
    if (!val.is_null) {
      TimestampValue value = TimestampValue::FromTimestampVal(val);
      RawValue::PrintValue(
          &value, TYPE_TIMESTAMP, -1, &(column->stringVal.values.back()));
    }
    SetNullBit(output_row_idx, val.is_null, &column->stringVal.nulls);
    ++output_row_idx;
  }
}

// Implementation for DATE.
static void DateExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval,
    RowBatch* batch, int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->stringVal);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    DateVal val = expr_eval->GetDateVal(it.Get());
    column->stringVal.values.emplace_back();
    if (!val.is_null) {
      DateValue value = DateValue::FromDateVal(val);
      RawValue::PrintValue(
          &value, TYPE_DATE, -1, &(column->stringVal.values.back()));
    }
    SetNullBit(output_row_idx, val.is_null, &column->stringVal.nulls);
    ++output_row_idx;
  }
}

// Implementation for STRING and VARCHAR.
static void StringExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval, RowBatch* batch,
    int start_idx, int num_rows, uint32_t output_row_idx,
    apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->stringVal);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    StringVal val = expr_eval->GetStringVal(it.Get());
    if (val.is_null) {
      column->stringVal.values.emplace_back();
    } else {
      column->stringVal.values.emplace_back(reinterpret_cast<char*>(val.ptr), val.len);
    }
    SetNullBit(output_row_idx, val.is_null, &column->stringVal.nulls);
    ++output_row_idx;
  }
}

// Implementation for CHAR.
static void CharExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval,
    const TColumnType& type, RowBatch* batch, int start_idx, int num_rows,
    uint32_t output_row_idx, apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->stringVal);
  ColumnType char_type = ColumnType::CreateCharType(type.types[0].scalar_type.len);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    StringVal val = expr_eval->GetStringVal(it.Get());
    if (val.is_null) {
      column->stringVal.values.emplace_back();
    } else {
      column->stringVal.values.emplace_back(
          reinterpret_cast<const char*>(val.ptr), char_type.len);
    }
    SetNullBit(output_row_idx, val.is_null, &column->stringVal.nulls);
    ++output_row_idx;
  }
}

static void DecimalExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval,
    const TColumnType& type, RowBatch* batch, int start_idx, int num_rows,
    uint32_t output_row_idx, apache::hive::service::cli::thrift::TColumn* column) {
  ReserveSpace(num_rows, output_row_idx, &column->stringVal);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    DecimalVal val = expr_eval->GetDecimalVal(it.Get());
    const ColumnType& decimalType = ColumnType::FromThrift(type);
    if (val.is_null) {
      column->stringVal.values.emplace_back();
    } else {
      switch (decimalType.GetByteSize()) {
        case 4:
          column->stringVal.values.emplace_back(
              Decimal4Value(val.val4).ToString(decimalType));
          break;
        case 8:
          column->stringVal.values.emplace_back(
              Decimal8Value(val.val8).ToString(decimalType));
          break;
        case 16:
          column->stringVal.values.emplace_back(
              Decimal16Value(val.val16).ToString(decimalType));
          break;
        default:
          DCHECK(false) << "bad type: " << decimalType;
      }
    }
    SetNullBit(output_row_idx, val.is_null, &column->stringVal.nulls);
    ++output_row_idx;
  }
}

// For V6 and above
void impala::ExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval,
    const TColumnType& type, RowBatch* batch, int start_idx, int num_rows,
    uint32_t output_row_idx, apache::hive::service::cli::thrift::TColumn* column) {
  // Dispatch to a templated function for the loop over rows. This avoids branching on
  // the type for every row.
  // TODO: instead of relying on stamped out implementations, we could codegen this loop
  // to inline the expression evaluation into the loop body.
  switch (type.types[0].scalar_type.type) {
    case TPrimitiveType::NULL_TYPE:
    case TPrimitiveType::BOOLEAN:
      BoolExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::TINYINT:
      TinyIntExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::SMALLINT:
      SmallIntExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::INT:
      IntExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::BIGINT:
      BigIntExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::FLOAT:
      FloatExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::DOUBLE:
      DoubleExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::DATE:
      DateExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      break;
    case TPrimitiveType::TIMESTAMP:
      TimestampExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::STRING:
    case TPrimitiveType::VARCHAR:
      StringExprValuesToHS2TColumn(
          expr_eval, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::CHAR:
      CharExprValuesToHS2TColumn(
          expr_eval, type, batch, start_idx, num_rows, output_row_idx, column);
      return;
    case TPrimitiveType::DECIMAL: {
      DecimalExprValuesToHS2TColumn(
          expr_eval, type, batch, start_idx, num_rows, output_row_idx, column);
      return;
    }
    default:
      DCHECK(false) << "Unhandled type: "
                    << TypeToString(ThriftToType(type.types[0].scalar_type.type));
  }
}

// For V1 -> V5
void impala::TColumnValueToHS2TColumnValue(const TColumnValue& col_val,
    const TColumnType& type, thrift::TColumnValue* hs2_col_val) {
  // TODO: Handle complex types.
  DCHECK_EQ(1, type.types.size());
  DCHECK_EQ(TTypeNodeType::SCALAR, type.types[0].type);
  DCHECK_EQ(true, type.types[0].__isset.scalar_type);
  switch (type.types[0].scalar_type.type) {
    case TPrimitiveType::BOOLEAN:
      hs2_col_val->__isset.boolVal = true;
      hs2_col_val->boolVal.value = col_val.bool_val;
      hs2_col_val->boolVal.__isset.value = col_val.__isset.bool_val;
      break;
    case TPrimitiveType::TINYINT:
      hs2_col_val->__isset.byteVal = true;
      hs2_col_val->byteVal.value = col_val.byte_val;
      hs2_col_val->byteVal.__isset.value = col_val.__isset.byte_val;
      break;
    case TPrimitiveType::SMALLINT:
      hs2_col_val->__isset.i16Val = true;
      hs2_col_val->i16Val.value = col_val.short_val;
      hs2_col_val->i16Val.__isset.value = col_val.__isset.short_val;
      break;
    case TPrimitiveType::INT:
      hs2_col_val->__isset.i32Val = true;
      hs2_col_val->i32Val.value = col_val.int_val;
      hs2_col_val->i32Val.__isset.value = col_val.__isset.int_val;
      break;
    case TPrimitiveType::BIGINT:
      hs2_col_val->__isset.i64Val = true;
      hs2_col_val->i64Val.value = col_val.long_val;
      hs2_col_val->i64Val.__isset.value = col_val.__isset.long_val;
      break;
    case TPrimitiveType::FLOAT:
    case TPrimitiveType::DOUBLE:
      hs2_col_val->__isset.doubleVal = true;
      hs2_col_val->doubleVal.value = col_val.double_val;
      hs2_col_val->doubleVal.__isset.value = col_val.__isset.double_val;
      break;
    case TPrimitiveType::DECIMAL:
    case TPrimitiveType::STRING:
    case TPrimitiveType::TIMESTAMP:
    case TPrimitiveType::DATE:
    case TPrimitiveType::VARCHAR:
    case TPrimitiveType::CHAR:
      // HiveServer2 requires timestamp to be presented as string. Note that the .thrift
      // spec says it should be a BIGINT; AFAICT Hive ignores that and produces a string.
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = col_val.__isset.string_val;
      if (col_val.__isset.string_val) {
        hs2_col_val->stringVal.value = col_val.string_val;
      }
      break;
    default:
      DCHECK(false) << "bad type: "
                     << TypeToString(ThriftToType(type.types[0].scalar_type.type));
      break;
  }
}

// For V1 -> V5
void impala::ExprValueToHS2TColumnValue(const void* value, const TColumnType& type,
    thrift::TColumnValue* hs2_col_val) {
  bool not_null = (value != NULL);
  // TODO: Handle complex types.
  DCHECK_EQ(1, type.types.size());
  DCHECK_EQ(TTypeNodeType::SCALAR, type.types[0].type);
  DCHECK_EQ(1, type.types[0].__isset.scalar_type);
  switch (type.types[0].scalar_type.type) {
    case TPrimitiveType::NULL_TYPE:
      // Set NULLs in the bool_val.
      hs2_col_val->__isset.boolVal = true;
      hs2_col_val->boolVal.__isset.value = false;
      break;
    case TPrimitiveType::BOOLEAN:
      hs2_col_val->__isset.boolVal = true;
      if (not_null) hs2_col_val->boolVal.value = *reinterpret_cast<const bool*>(value);
      hs2_col_val->boolVal.__isset.value = not_null;
      break;
    case TPrimitiveType::TINYINT:
      hs2_col_val->__isset.byteVal = true;
      if (not_null) hs2_col_val->byteVal.value = *reinterpret_cast<const int8_t*>(value);
      hs2_col_val->byteVal.__isset.value = not_null;
      break;
    case TPrimitiveType::SMALLINT:
      hs2_col_val->__isset.i16Val = true;
      if (not_null) hs2_col_val->i16Val.value = *reinterpret_cast<const int16_t*>(value);
      hs2_col_val->i16Val.__isset.value = not_null;
      break;
    case TPrimitiveType::INT:
      hs2_col_val->__isset.i32Val = true;
      if (not_null) hs2_col_val->i32Val.value = *reinterpret_cast<const int32_t*>(value);
      hs2_col_val->i32Val.__isset.value = not_null;
      break;
    case TPrimitiveType::BIGINT:
      hs2_col_val->__isset.i64Val = true;
      if (not_null) hs2_col_val->i64Val.value = *reinterpret_cast<const int64_t*>(value);
      hs2_col_val->i64Val.__isset.value = not_null;
      break;
    case TPrimitiveType::FLOAT:
      hs2_col_val->__isset.doubleVal = true;
      if (not_null) hs2_col_val->doubleVal.value = *reinterpret_cast<const float*>(value);
      hs2_col_val->doubleVal.__isset.value = not_null;
      break;
    case TPrimitiveType::DOUBLE:
      hs2_col_val->__isset.doubleVal = true;
      if (not_null) {
        hs2_col_val->doubleVal.value = *reinterpret_cast<const double*>(value);
      }
      hs2_col_val->doubleVal.__isset.value = not_null;
      break;
    case TPrimitiveType::STRING:
    case TPrimitiveType::VARCHAR:
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = not_null;
      if (not_null) {
        const StringValue* string_val = reinterpret_cast<const StringValue*>(value);
        hs2_col_val->stringVal.value.assign(static_cast<char*>(string_val->ptr),
                                            string_val->len);
      }
      break;
    case TPrimitiveType::CHAR:
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = not_null;
      if (not_null) {
        ColumnType char_type = ColumnType::CreateCharType(type.types[0].scalar_type.len);
        hs2_col_val->stringVal.value.assign(
           reinterpret_cast<const char*>(value), char_type.len);
      }
      break;
    case TPrimitiveType::DATE:
      // HiveServer2 requires date to be presented as string.
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = not_null;
      if (not_null) {
        hs2_col_val->stringVal.value =
            reinterpret_cast<const DateValue*>(value)->ToString();
      }
      break;
    case TPrimitiveType::TIMESTAMP:
      // HiveServer2 requires timestamp to be presented as string.
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = not_null;
      if (not_null) {
        RawValue::PrintValue(value, TYPE_TIMESTAMP, -1, &(hs2_col_val->stringVal.value));
      }
      break;
    case TPrimitiveType::DECIMAL: {
      // HiveServer2 requires decimal to be presented as string.
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = not_null;
      const ColumnType& decimalType = ColumnType::FromThrift(type);
      if (not_null) {
        switch (decimalType.GetByteSize()) {
          case 4:
            hs2_col_val->stringVal.value =
              reinterpret_cast<const Decimal4Value*>(value)->ToString(decimalType);
            break;
          case 8:
            hs2_col_val->stringVal.value =
              reinterpret_cast<const Decimal8Value*>(value)->ToString(decimalType);
            break;
          case 16:
            hs2_col_val->stringVal.value =
              reinterpret_cast<const Decimal16Value*>(value)->ToString(decimalType);
            break;
          default:
            DCHECK(false) << "bad type: " << decimalType;
        }
      }
      break;
    }
    default:
      DCHECK(false) << "bad type: "
                     << TypeToString(ThriftToType(type.types[0].scalar_type.type));
      break;
  }
}

template<typename T>
void PrintVal(const T& val, ostream* ss) {
  if (val.__isset.value) {
    (*ss) << val.value;
  } else {
    (*ss) << "NULL";
  }
}

// Specialisation for byte values that would otherwise be interpreted as character values,
// not integers, when printed to the stringstream.
template<>
void PrintVal(const apache::hive::service::cli::thrift::TByteValue& val, ostream* ss) {
  if (val.__isset.value) {
    (*ss) << static_cast<int16_t>(val.value);
  } else {
    (*ss) << "NULL";
  }
}

void impala::PrintTColumnValue(
    const apache::hive::service::cli::thrift::TColumnValue& colval, stringstream* out) {
  if (colval.__isset.boolVal) {
    if (colval.boolVal.__isset.value) {
      (*out) << ((colval.boolVal.value) ? "true" : "false");
    } else {
      (*out) << "NULL";
    }
  } else if (colval.__isset.doubleVal) {
    PrintVal(colval.doubleVal, out);
  } else if (colval.__isset.byteVal) {
    PrintVal(colval.byteVal, out);
  } else if (colval.__isset.i32Val) {
    PrintVal(colval.i32Val, out);
  } else if (colval.__isset.i16Val) {
    PrintVal(colval.i16Val, out);
  } else if (colval.__isset.i64Val) {
    PrintVal(colval.i64Val, out);
  } else if (colval.__isset.stringVal) {
    PrintVal(colval.stringVal, out);
  } else {
    (*out) << "NULL";
  }
}
