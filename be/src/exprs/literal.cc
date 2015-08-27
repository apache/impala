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

#include "literal.h"

#include <sstream>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "runtime/runtime-state.h"
#include "gen-cpp/Exprs_types.h"

#include "common/names.h"

using namespace llvm;
using namespace impala_udf;

namespace impala {

Literal::Literal(const TExprNode& node)
  : Expr(node) {
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
    case TYPE_FLOAT:
      DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
      DCHECK(node.__isset.float_literal);
      value_.float_val = node.float_literal.value;
      break;
    case TYPE_DOUBLE:
      DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
      DCHECK(node.__isset.float_literal);
      value_.double_val = node.float_literal.value;
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      DCHECK_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
      DCHECK(node.__isset.string_literal);
      value_ = ExprValue(node.string_literal.value);
      if (type_.type == TYPE_VARCHAR) {
        value_.string_val.len = min(type_.len, value_.string_val.len);
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
      value_ = ExprValue(str);
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
    default:
      DCHECK(false) << "Invalid type: " << TypeToString(type_.type);
  }
}

Literal::Literal(ColumnType type, bool v)
  : Expr(type) {
  DCHECK_EQ(type.type, TYPE_BOOLEAN) << type;
  value_.bool_val = v;
}

Literal::Literal(ColumnType type, int8_t v)
  : Expr(type) {
  DCHECK_EQ(type.type, TYPE_TINYINT) << type;
  value_.tinyint_val = v;
}

Literal::Literal(ColumnType type, int16_t v)
  : Expr(type) {
  DCHECK_EQ(type.type, TYPE_SMALLINT) << type;
  value_.smallint_val = v;
}

Literal::Literal(ColumnType type, int32_t v)
  : Expr(type) {
  DCHECK_EQ(type.type, TYPE_INT) << type;
  value_.int_val = v;
}

Literal::Literal(ColumnType type, int64_t v)
  : Expr(type) {
  DCHECK_EQ(type.type, TYPE_BIGINT) << type;
  value_.bigint_val = v;
}

Literal::Literal(ColumnType type, float v)
  : Expr(type) {
  DCHECK_EQ(type.type, TYPE_FLOAT) << type;
  value_.float_val = v;
}

Literal::Literal(ColumnType type, double v)
  : Expr(type) {
  if (type.type == TYPE_DOUBLE) {
    value_.double_val = v;
  } else if (type.type == TYPE_TIMESTAMP) {
    value_.timestamp_val = TimestampValue(v);
  } else if (type.type == TYPE_DECIMAL) {
    bool overflow = false;
    switch (type.GetByteSize()) {
      case 4:
        value_.decimal4_val = Decimal4Value::FromDouble(type, v, &overflow);
        break;
      case 8:
        value_.decimal8_val = Decimal8Value::FromDouble(type, v, &overflow);
        break;
      case 16:
        value_.decimal16_val = Decimal16Value::FromDouble(type, v, &overflow);
        break;
    }
    DCHECK(!overflow);
  } else {
    DCHECK(false) << type.DebugString();
  }
}

Literal::Literal(ColumnType type, const string& v)
  : Expr(type),
    value_(v) {
  DCHECK(type.type == TYPE_STRING || type.type == TYPE_CHAR || type.type == TYPE_VARCHAR)
      << type;
}

Literal::Literal(ColumnType type, const StringValue& v)
  : Expr(type),
    value_(v.DebugString()) {
  DCHECK(type.type == TYPE_STRING || type.type == TYPE_CHAR) << type;
}

template<class T>
bool ParseString(const string& str, T* val) {
  istringstream stream(str);
  stream >> *val;
  return !stream.fail();
}

Literal* Literal::CreateLiteral(const ColumnType& type, const string& str) {
  switch (type.type) {
    case TYPE_BOOLEAN: {
      bool v = false;
      DCHECK(ParseString<bool>(str, &v)) << str;
      return new Literal(type, v);
    }
    case TYPE_TINYINT: {
      int8_t v = 0;
      DCHECK(ParseString<int8_t>(str, &v)) << str;
      return new Literal(type, v);
    }
    case TYPE_SMALLINT: {
      int16_t v = 0;
      DCHECK(ParseString<int16_t>(str, &v)) << str;
      return new Literal(type, v);
    }
    case TYPE_INT: {
      int32_t v = 0;
      DCHECK(ParseString<int32_t>(str, &v)) << str;
      return new Literal(type, v);
    }
    case TYPE_BIGINT: {
      int64_t v = 0;
      DCHECK(ParseString<int64_t>(str, &v)) << str;
      return new Literal(type, v);
    }
    case TYPE_FLOAT: {
      float v = 0;
      DCHECK(ParseString<float>(str, &v)) << str;
      return new Literal(type, v);
    }
    case TYPE_DOUBLE: {
      double v = 0;
      DCHECK(ParseString<double>(str, &v)) << str;
      return new Literal(type, v);
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
      return new Literal(type, str);
    case TYPE_TIMESTAMP: {
      double v = 0;
      DCHECK(ParseString<double>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_DECIMAL: {
      double v = 0;
      DCHECK(ParseString<double>(str, &v)) << str;
      return new Literal(type, v);
    }
    default:
      DCHECK(false) << "Invalid type: " << type.DebugString();
      return NULL;
  }
}

BooleanVal Literal::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN) << type_;
  return BooleanVal(value_.bool_val);
}

TinyIntVal Literal::GetTinyIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TINYINT) << type_;
  return TinyIntVal(value_.tinyint_val);
}

SmallIntVal Literal::GetSmallIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_SMALLINT) << type_;
  return SmallIntVal(value_.smallint_val);
}

IntVal Literal::GetIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_INT) << type_;
  return IntVal(value_.int_val);
}

BigIntVal Literal::GetBigIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BIGINT) << type_;
  return BigIntVal(value_.bigint_val);
}

FloatVal Literal::GetFloatVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_FLOAT) << type_;
  return FloatVal(value_.float_val);
}

DoubleVal Literal::GetDoubleVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DOUBLE) << type_;
  return DoubleVal(value_.double_val);
}

StringVal Literal::GetStringVal(ExprContext* context, TupleRow* row) {
  DCHECK(type_.IsStringType()) << type_;
  StringVal result;
  value_.string_val.ToStringVal(&result);
  return result;
}

DecimalVal Literal::GetDecimalVal(ExprContext* context, TupleRow* row) {
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

string Literal::DebugString() const {
  stringstream out;
  out << "Literal(value=";
  switch (type_.type) {
    case TYPE_BOOLEAN:
      out << value_.bool_val;
      break;
    case TYPE_TINYINT:
      out << value_.tinyint_val;
      break;
    case TYPE_SMALLINT:
      out << value_.smallint_val;
      break;
    case TYPE_INT:
      out << value_.int_val;
      break;
    case TYPE_BIGINT:
      out << value_.bigint_val;
      break;
    case TYPE_FLOAT:
      out << value_.float_val;
      break;
    case TYPE_DOUBLE:
      out << value_.double_val;
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
    default:
      out << "[bad type! " << type_ << "]";
  }
  out << Expr::DebugString() << ")";
  return out.str();
}

// IR produced for bigint literal 10:
//
// define { i8, i64 } @Literal(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   ret { i8, i64 } { i8 0, i64 10 }
// }
Status Literal::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }

  DCHECK_EQ(GetNumChildren(), 0);
  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));
  Value* args[2];
  *fn = CreateIrFunctionPrototype(codegen, "Literal", &args);
  BasicBlock* entry_block = BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmCodeGen::LlvmBuilder builder(entry_block);

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
      v.SetLen(builder.getInt32(value_.string_val.len));
      v.SetPtr(codegen->CastPtrToLlvmPtr(codegen->ptr_type(), value_.string_val.ptr));
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
    default:
      stringstream ss;
      ss << "Invalid type: " << type_;
      DCHECK(false) << ss.str();
      return Status(ss.str());
  }

  builder.CreateRet(v.value());
  *fn = codegen->FinalizeFunction(*fn);
  ir_compute_fn_ = *fn;
  return Status::OK();
}

}
