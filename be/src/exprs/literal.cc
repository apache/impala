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

#include "literal.h"

#include <cmath>
#include <limits>
#include <sstream>
#include <boost/date_time/posix_time/posix_time_types.hpp>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gen-cpp/Exprs_types.h"
#include "runtime/date-parse-util.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-parse-util.h"
#include "util/decimal-util.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

Literal::Literal(const TExprNode& node)
  : ScalarExpr(node) {
  switch (type_.type) {
    case TYPE_BOOLEAN:
      DCHECK_EQ(node.node_type, TExprNodeType::BOOL_LITERAL);
      DCHECK(node.__isset.bool_literal);
      value_.bool_val = node.bool_literal.value;
      break;
    case TYPE_TINYINT:
      DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
      DCHECK(node.__isset.int_literal);
      value_.tinyint_val = node.int_literal.value;
      break;
    case TYPE_SMALLINT:
      DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
      DCHECK(node.__isset.int_literal);
      value_.smallint_val = node.int_literal.value;
      break;
    case TYPE_INT:
      DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
      DCHECK(node.__isset.int_literal);
      value_.int_val = node.int_literal.value;
      break;
    case TYPE_BIGINT:
      DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
      DCHECK(node.__isset.int_literal);
      value_.bigint_val = node.int_literal.value;
      break;
    case TYPE_FLOAT: {
      DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
      DCHECK(node.__isset.float_literal);
      // 'node.float_literal.value' is actually a double. Casting from double to float
      // invokes undefined behaviour if the value is out of range for float.
      double literal_value = node.float_literal.value;
      const bool out_of_range = literal_value < std::numeric_limits<float>::lowest()
          || literal_value > std::numeric_limits<float>::max();
      if (UNLIKELY(out_of_range && std::isfinite(literal_value))) {
        DCHECK(false) << "Value out of range for FLOAT.";
        value_.float_val = literal_value > 0 ? std::numeric_limits<float>::infinity()
          : -std::numeric_limits<float>::infinity();
      } else {
        value_.float_val = literal_value;
      }
      break;
    }
    case TYPE_DOUBLE:
      DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
      DCHECK(node.__isset.float_literal);
      value_.double_val = node.float_literal.value;
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      DCHECK_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
      DCHECK(node.__isset.string_literal);
      value_.Init(node.string_literal.value);
      if (type_.type == TYPE_VARCHAR) {
        value_.string_val.SetLen(min(type_.len, value_.string_val.Len()));
      }
      break;
    }
    case TYPE_CHAR: {
      DCHECK_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
      DCHECK(node.__isset.string_literal);
      string str = node.string_literal.value;
      int str_len = str.size();
      DCHECK_GT(str_len, 0);
      str.resize(type_.len);
      if (str_len < type_.len) {
        // Pad out literal with spaces.
        str.replace(str_len, type_.len - str_len, type_.len - str_len, ' ');
      }
      value_.Init(str);
      break;
    }
    case TYPE_DECIMAL: {
      DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
      DCHECK(node.__isset.decimal_literal);
      const uint8_t* buffer =
          reinterpret_cast<const uint8_t*>(&node.decimal_literal.value[0]);
      int len = node.decimal_literal.value.size();
      DCHECK_GT(len, 0);

      switch (type().GetByteSize()) {
        case 4:
          DCHECK_LE(len, 4);
          DecimalUtil::DecodeFromFixedLenByteArray(buffer, len, &value_.decimal4_val);
          break;
        case 8:
          DCHECK_LE(len, 8);
          DecimalUtil::DecodeFromFixedLenByteArray(buffer, len, &value_.decimal8_val);
          break;
        case 16:
          DCHECK_LE(len, 16);
          DecimalUtil::DecodeFromFixedLenByteArray(buffer, len, &value_.decimal16_val);
          break;
        default:
          DCHECK(false) << type_.DebugString();
      }
      break;
    }
    case TYPE_TIMESTAMP: {
      DCHECK_EQ(node.node_type, TExprNodeType::TIMESTAMP_LITERAL);
      DCHECK(node.__isset.timestamp_literal);
      const string& ts_val = node.timestamp_literal.value;
      DCHECK_EQ(type_.GetSlotSize(), ts_val.size());
      memcpy(&value_.timestamp_val, ts_val.data(), type_.GetSlotSize());
      break;
    }
    case TYPE_DATE: {
      DCHECK_EQ(node.node_type, TExprNodeType::DATE_LITERAL);
      DCHECK(node.__isset.date_literal);
      value_.date_val = *reinterpret_cast<const DateValue*>(
          &node.date_literal.days_since_epoch);
      break;
    }
    default:
      DCHECK(false) << "Invalid type: " << TypeToString(type_.type);
  }
  DCHECK(cache_entry_ == nullptr);
}

Literal::Literal(ColumnType type, bool v)
  : ScalarExpr(type, true) {
  DCHECK_EQ(type.type, TYPE_BOOLEAN) << type;
  value_.bool_val = v;
}

Literal::Literal(ColumnType type, int8_t v)
  : ScalarExpr(type, true) {
  DCHECK_EQ(type.type, TYPE_TINYINT) << type;
  value_.tinyint_val = v;
}

Literal::Literal(ColumnType type, int16_t v)
  : ScalarExpr(type, true) {
  DCHECK_EQ(type.type, TYPE_SMALLINT) << type;
  value_.smallint_val = v;
}

Literal::Literal(ColumnType type, int32_t v)
  : ScalarExpr(type, true) {
  DCHECK_EQ(type.type, TYPE_INT) << type;
  value_.int_val = v;
}

Literal::Literal(ColumnType type, int64_t v)
  : ScalarExpr(type, true) {
  DCHECK_EQ(type.type, TYPE_BIGINT) << type;
  value_.bigint_val = v;
}

Literal::Literal(ColumnType type, float v)
  : ScalarExpr(type, true) {
  DCHECK_EQ(type.type, TYPE_FLOAT) << type;
  value_.float_val = v;
}

Literal::Literal(ColumnType type, double v)
  : ScalarExpr(type, true) {
  if (type.type == TYPE_DOUBLE) {
    value_.double_val = v;
  } else if (type.type == TYPE_DECIMAL) {
    bool overflow = false;
    switch (type.GetByteSize()) {
      case 4:
        value_.decimal4_val = Decimal4Value::FromDouble(type, v, true, &overflow);
        break;
      case 8:
        value_.decimal8_val = Decimal8Value::FromDouble(type, v, true, &overflow);
        break;
      case 16:
        value_.decimal16_val = Decimal16Value::FromDouble(type, v, true, &overflow);
        break;
    }
    DCHECK(!overflow);
  } else {
    DCHECK(false) << type.DebugString();
  }
}

Literal::Literal(ColumnType type, const string& v)
  : ScalarExpr(type, true) {
  value_.Init(v);
  DCHECK(type.type == TYPE_STRING || type.type == TYPE_CHAR || type.type == TYPE_VARCHAR)
      << type;
}

Literal::Literal(ColumnType type, const StringValue& v)
  : ScalarExpr(type, true) {
  value_.Init(v.DebugString());
  DCHECK(type.type == TYPE_STRING || type.type == TYPE_CHAR) << type;
}

Literal::Literal(ColumnType type, const TimestampValue& v)
  : ScalarExpr(type, true) {
  DCHECK_EQ(type.type, TYPE_TIMESTAMP) << type;
  value_.timestamp_val = v;
}

Literal::Literal(ColumnType type, const DateValue& v)
  : ScalarExpr(type, true) {
  DCHECK_EQ(type.type, TYPE_DATE) << type;
  value_.date_val = v;
}

BooleanVal Literal::GetBooleanValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN) << type_;
  return BooleanVal(value_.bool_val);
}

TinyIntVal Literal::GetTinyIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TINYINT) << type_;
  return TinyIntVal(value_.tinyint_val);
}

SmallIntVal Literal::GetSmallIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_SMALLINT) << type_;
  return SmallIntVal(value_.smallint_val);
}

IntVal Literal::GetIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_INT) << type_;
  return IntVal(value_.int_val);
}

BigIntVal Literal::GetBigIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BIGINT) << type_;
  return BigIntVal(value_.bigint_val);
}

FloatVal Literal::GetFloatValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_FLOAT) << type_;
  return FloatVal(value_.float_val);
}

DoubleVal Literal::GetDoubleValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DOUBLE) << type_;
  return DoubleVal(value_.double_val);
}

StringVal Literal::GetStringValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsStringType()) << type_;
  StringVal result;
  value_.string_val.ToStringVal(&result);
  return result;
}

DecimalVal Literal::GetDecimalValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DECIMAL) << type_;
  switch (type().GetByteSize()) {
    case 4:
      return DecimalVal(value_.decimal4_val.value());
    case 8:
      return DecimalVal(value_.decimal8_val.value());
    case 16:
      return DecimalVal(value_.decimal16_val.value());
    default:
      DCHECK(false) << type_.DebugString();
  }
  // Quieten GCC.
  return DecimalVal();
}

TimestampVal Literal::GetTimestampValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP) << type_;
  TimestampVal result;
  value_.timestamp_val.ToTimestampVal(&result);
  return result;
}

DateVal Literal::GetDateValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DATE) << type_;
  return value_.date_val.ToDateVal();
}

string Literal::DebugString() const {
  stringstream out;
  out << "Literal(value=";
  switch (type_.type) {
    case TYPE_BOOLEAN:
      out << std::to_string(value_.bool_val);
      break;
    case TYPE_TINYINT:
      out << std::to_string(value_.tinyint_val);
      break;
    case TYPE_SMALLINT:
      out << std::to_string(value_.smallint_val);
      break;
    case TYPE_INT:
      out << std::to_string(value_.int_val);
      break;
    case TYPE_BIGINT:
      out << std::to_string(value_.bigint_val);
      break;
    case TYPE_FLOAT:
      out << std::to_string(value_.float_val);
      break;
    case TYPE_DOUBLE:
      out << std::to_string(value_.double_val);
      break;
    case TYPE_STRING:
      out << value_.string_val;
      break;
    case TYPE_DECIMAL:
      switch (type().GetByteSize()) {
        case 4:
          out << value_.decimal4_val.ToString(type());
          break;
        case 8:
          out << value_.decimal8_val.ToString(type());
          break;
        case 16:
          out << value_.decimal16_val.ToString(type());
          break;
        default:
          DCHECK(false) << type_.DebugString();
      }
      break;
    case TYPE_TIMESTAMP:
      out << value_.timestamp_val;
      break;
    case TYPE_DATE:
      out << value_.date_val;
      break;
    default:
      out << "[bad type! " << type_ << "]";
  }
  out << ScalarExpr::DebugString() << ")";
  return out.str();
}

// IR produced for bigint literal 10:
//
// define { i8, i64 } @Literal(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   ret { i8, i64 } { i8 0, i64 10 }
// }
Status Literal::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
  DCHECK_EQ(GetNumChildren(), 0);
  llvm::Value* args[2];
  *fn = CreateIrFunctionPrototype("Literal", codegen, &args);
  llvm::BasicBlock* entry_block =
      llvm::BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmBuilder builder(entry_block);

  CodegenAnyVal v = CodegenAnyVal::GetNonNullVal(codegen, &builder, type_);
  switch (type_.type) {
    case TYPE_BOOLEAN:
      v.SetVal(value_.bool_val);
      break;
    case TYPE_TINYINT:
      v.SetVal(value_.tinyint_val);
      break;
    case TYPE_SMALLINT:
      v.SetVal(value_.smallint_val);
      break;
    case TYPE_INT:
      v.SetVal(value_.int_val);
      break;
    case TYPE_BIGINT:
      v.SetVal(value_.bigint_val);
      break;
    case TYPE_FLOAT:
      v.SetVal(value_.float_val);
      break;
    case TYPE_DOUBLE:
      v.SetVal(value_.double_val);
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
      v.SetLen(builder.getInt32(value_.string_val.Len()));
      v.SetPtr(codegen->GetStringConstant(
          &builder, value_.string_val.Ptr(), value_.string_val.Len()));
      break;
    case TYPE_DECIMAL:
      switch (type().GetByteSize()) {
        case 4:
          v.SetVal(value_.decimal4_val.value());
          break;
        case 8:
          v.SetVal(value_.decimal8_val.value());
          break;
        case 16:
          v.SetVal(value_.decimal16_val.value());
          break;
        default:
          DCHECK(false) << type_.DebugString();
      }
      break;
    case TYPE_TIMESTAMP:
      v.SetTimeOfDay(builder.getInt64(
          *reinterpret_cast<const int64_t*>(&value_.timestamp_val.time())));
      v.SetDate(builder.getInt32(
          *reinterpret_cast<const int32_t*>(&value_.timestamp_val.date())));
      break;
    case TYPE_DATE:
      v.SetVal(value_.date_val.Value());
      break;
    default:
      stringstream ss;
      ss << "Invalid type: " << type_;
      DCHECK(false) << ss.str();
      return Status(ss.str());
  }

  builder.CreateRet(v.GetLoweredValue());
  *fn = codegen->FinalizeFunction(*fn);
  if (UNLIKELY(*fn == nullptr)) return Status(TErrorCode::IR_VERIFY_FAILED, "Literal");
  return Status::OK();
}

bool operator==(const Literal& lhs, const Literal& rhs) {
  if (lhs.type() != rhs.type()) return false;
  return lhs.value_.EqualsWithType(rhs.value_, lhs.type());
}

}
