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

#include "codegen/codegen-anyval.h"

using namespace impala;
using namespace impala_udf;
using namespace llvm;
using namespace std;

const char* CodegenAnyVal::LLVM_BOOLEANVAL_NAME   = "struct.impala_udf::BooleanVal";
const char* CodegenAnyVal::LLVM_TINYINTVAL_NAME   = "struct.impala_udf::TinyIntVal";
const char* CodegenAnyVal::LLVM_SMALLINTVAL_NAME  = "struct.impala_udf::SmallIntVal";
const char* CodegenAnyVal::LLVM_INTVAL_NAME       = "struct.impala_udf::IntVal";
const char* CodegenAnyVal::LLVM_BIGINTVAL_NAME    = "struct.impala_udf::BigIntVal";
const char* CodegenAnyVal::LLVM_FLOATVAL_NAME     = "struct.impala_udf::FloatVal";
const char* CodegenAnyVal::LLVM_DOUBLEVAL_NAME    = "struct.impala_udf::DoubleVal";
const char* CodegenAnyVal::LLVM_STRINGVAL_NAME    = "struct.impala_udf::StringVal";
const char* CodegenAnyVal::LLVM_TIMESTAMPVAL_NAME = "struct.impala_udf::TimestampVal";
const char* CodegenAnyVal::LLVM_DECIMALVAL_NAME   = "struct.impala_udf::DecimalVal";

Type* CodegenAnyVal::GetLoweredType(LlvmCodeGen* cg, const ColumnType& type) {
  switch(type.type) {
    case TYPE_BOOLEAN: // i16
      return cg->smallint_type();
    case TYPE_TINYINT: // i16
      return cg->smallint_type();
    case TYPE_SMALLINT: // i32
      return cg->int_type();
    case TYPE_INT: // i64
      return cg->bigint_type();
    case TYPE_BIGINT: // { i8, i64 }
      return StructType::get(cg->tinyint_type(), cg->bigint_type(), NULL);
    case TYPE_FLOAT: // i64
      return cg->bigint_type();
    case TYPE_DOUBLE: // { i8, double }
      return StructType::get(cg->tinyint_type(), cg->double_type(), NULL);
    case TYPE_STRING: // { i64, i8* }
      return StructType::get(cg->bigint_type(), cg->ptr_type(), NULL);
    case TYPE_TIMESTAMP: // { i64, i64 }
      return StructType::get(cg->bigint_type(), cg->bigint_type(), NULL);
    case TYPE_DECIMAL: // %"struct.impala_udf::DecimalVal" (isn't lowered)
                       // = { {i8}, [15 x i8], {i128} }
      return cg->GetType(LLVM_DECIMALVAL_NAME);
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return NULL;
  }
}

Type* CodegenAnyVal::GetUnloweredType(LlvmCodeGen* cg, const ColumnType& type) {
  Type* result;
  switch(type.type) {
    case TYPE_BOOLEAN:
      result = cg->GetType(LLVM_BOOLEANVAL_NAME);
      break;
    case TYPE_TINYINT:
      result = cg->GetType(LLVM_TINYINTVAL_NAME);
      break;
    case TYPE_SMALLINT:
      result = cg->GetType(LLVM_SMALLINTVAL_NAME);
      break;
    case TYPE_INT:
      result = cg->GetType(LLVM_INTVAL_NAME);
      break;
    case TYPE_BIGINT:
      result = cg->GetType(LLVM_BIGINTVAL_NAME);
      break;
    case TYPE_FLOAT:
      result = cg->GetType(LLVM_FLOATVAL_NAME);
      break;
    case TYPE_DOUBLE:
      result = cg->GetType(LLVM_DOUBLEVAL_NAME);
      break;
    case TYPE_STRING:
      result = cg->GetType(LLVM_STRINGVAL_NAME);
      break;
    case TYPE_TIMESTAMP:
      result = cg->GetType(LLVM_TIMESTAMPVAL_NAME);
      break;
    case TYPE_DECIMAL:
      result = cg->GetType(LLVM_DECIMALVAL_NAME);
      break;
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return NULL;
  }
  DCHECK(result != NULL) << type.DebugString();
  return result;
}

Value* CodegenAnyVal::CreateCall(
    LlvmCodeGen* cg, LlvmCodeGen::LlvmBuilder* builder, Function* fn,
    ArrayRef<Value*> args, const string& name, Value* result_ptr) {
  if (fn->getReturnType()->isVoidTy()) {
    // Void return type indicates that this function returns a DecimalVal via the first
    // argument (which should be a DecimalVal*).
    Function::arg_iterator ret_arg = fn->arg_begin();
    DCHECK(ret_arg->getType()->isPointerTy());
    Type* ret_type = ret_arg->getType()->getPointerElementType();
    DCHECK_EQ(ret_type, GetLoweredType(cg, ColumnType::CreateDecimalType(1, 0)));

    // We need to pass a DecimalVal pointer to 'fn' that will be populated with the result
    // value. Use 'result_ptr' if specified, otherwise alloca one.
    Value* ret_ptr = (result_ptr == NULL) ?
                     builder->CreateAlloca(ret_type, 0, name) : result_ptr;
    vector<Value*> new_args = args.vec();
    new_args.insert(new_args.begin(), ret_ptr);
    builder->CreateCall(fn, new_args);

    // If 'result_ptr' was specified, we're done. Otherwise load and return the result.
    if (result_ptr != NULL) return NULL;
    return builder->CreateLoad(ret_ptr, name);
  } else {
    // Function returns *Val normally
    // Note that 'fn' may return a DecimalVal. We generate non-ABI-compliant functions
    // internally, rather than special-casing DecimalVal everywhere. This is ok as long as
    // the non-compliant functions are called only by other code-generated functions,
    // since LLVM is aware of the non-compliant function signature and will generate the
    // correct call instructions.
    Value* ret = builder->CreateCall(fn, args, name);
    if (result_ptr == NULL) return ret;
    builder->CreateStore(ret, result_ptr);
    return NULL;
  }
}

CodegenAnyVal::CodegenAnyVal(LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder,
                             const ColumnType& type, Value* value, const char* name)
  : type_(type),
    value_(value),
    name_(name),
    codegen_(codegen),
    builder_(builder) {
  Type* value_type = GetLoweredType(codegen, type);
  if (value_ == NULL) {
    // No Value* was specified, so allocate one on the stack and load it.
    Value* ptr = builder_->CreateAlloca(value_type, 0);
    value_ = builder_->CreateLoad(ptr, name_);
  }
  DCHECK_EQ(value_->getType(), value_type);
}

void CodegenAnyVal::SetIsNull(Value* is_null) {
  switch(type_.type) {
    case TYPE_BIGINT:
    case TYPE_DOUBLE: {
      // Lowered type is of form { i8, * }. Set the i8 value to 'is_null'.
      Value* is_null_ext =
          builder_->CreateZExt(is_null, codegen_->tinyint_type(), "is_null_ext");
      value_ = builder_->CreateInsertValue(value_, is_null_ext, 0, name_);
      break;
    }
    case TYPE_DECIMAL: {
      // Lowered type is of form { {i8}, [15 x i8], {i128} }. Set the i8 value to
      // 'is_null'.
      Value* is_null_ext =
          builder_->CreateZExt(is_null, codegen_->tinyint_type(), "is_null_ext");
      // Index into the {i8} struct as well as the outer struct.
      uint32_t idxs[] = {0, 0};
      value_ = builder_->CreateInsertValue(value_, is_null_ext, idxs, name_);
      break;
    }
    case TYPE_STRING:
    case TYPE_TIMESTAMP: {
      // Lowered type is of the form { i64, * }. Set the first byte of the i64 value to
      // 'is_null'
      Value* v = builder_->CreateExtractValue(value_, 0);
      v = builder_->CreateAnd(v, -0x100LL, "masked");
      Value* is_null_ext = builder_->CreateZExt(is_null, v->getType(), "is_null_ext");
      v = builder_->CreateOr(v, is_null_ext);
      value_ = builder_->CreateInsertValue(value_, v, 0, name_);
      break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_FLOAT: {
      // Lowered type is an integer. Set the first byte to 'is_null'.
      value_ = builder_->CreateAnd(value_, -0x100LL, "masked");
      Value* is_null_ext = builder_->CreateZExt(is_null, value_->getType(), "is_null_ext");
      value_ = builder_->CreateOr(value_, is_null_ext, name_);
      break;
    }
    default:
      DCHECK(false) << "NYI: " << type_.DebugString();
  }
}

void CodegenAnyVal::SetVal(Value* val) {
  DCHECK(type_.type != TYPE_STRING) << "Use SetPtr and SetLen for StringVals";
  DCHECK(type_.type != TYPE_TIMESTAMP)
      << "Use SetDate and SetTimeOfDay for TimestampVals";
  switch(type_.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:{
      // Lowered type is an integer. Set the high bytes to 'val'.
      int num_bits = type_.GetByteSize() * 8;
      value_ = SetHighBits(num_bits, val, value_, name_);
      break;
    }
    case TYPE_FLOAT:
      // Same as above, but we must cast 'val' to an integer type.
      val = builder_->CreateBitCast(val, codegen_->int_type());
      value_ = SetHighBits(32, val, value_, name_);
      break;
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
      // Lowered type is of form { i8, * }. Set the second value to 'val'.
      value_ = builder_->CreateInsertValue(value_, val, 1, name_);
      break;
    case TYPE_DECIMAL: {
      // Lowered type is of the form { {i8}, [15 x i8], {i128} }. Set the i128 value to
      // 'val'. (The {i128} corresponds to the union of the different width int types.)
      DCHECK_EQ(val->getType()->getIntegerBitWidth(), type_.GetByteSize() * 8);
      val = builder_->CreateSExt(val, Type::getIntNTy(codegen_->context(), 128));
      uint32_t idxs[] = {2, 0};
      value_ = builder_->CreateInsertValue(value_, val, idxs, name_);
      break;
    }
    default:
      DCHECK(false) << "Unsupported type: " << type_;
  }
}

void CodegenAnyVal::SetPtr(Value* ptr) {
  // Set the second pointer value to 'ptr'.
  DCHECK_EQ(type_.type, TYPE_STRING);
  value_ = builder_->CreateInsertValue(value_, ptr, 1, name_);
}

void CodegenAnyVal::SetLen(Value* len) {
  // Set the high bytes of the first value to 'len'.
  DCHECK_EQ(type_.type, TYPE_STRING);
  Value* v = builder_->CreateExtractValue(value_, 0);
  v = SetHighBits(32, len, v);
  value_ = builder_->CreateInsertValue(value_, v, 0, name_);
}

void CodegenAnyVal::SetTimeOfDay(Value* time_of_day) {
  // Set the second i64 value to 'time_of_day'.
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  value_ = builder_->CreateInsertValue(value_, time_of_day, 1, name_);
}

void CodegenAnyVal::SetDate(Value* date) {
  // Set the high bytes of the first value to 'date'.
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  Value* v = builder_->CreateExtractValue(value_, 0);
  v = SetHighBits(32, date, v);
  value_ = builder_->CreateInsertValue(value_, v, 0, name_);
}

void CodegenAnyVal::SetFromRawPtr(Value* raw_ptr) {
  Value* val_ptr =
      builder_->CreateBitCast(raw_ptr, codegen_->GetPtrType(type_), "val_ptr");
  if (type_.type == TYPE_STRING) {
    // Convert StringValue to StringVal
    Value* ptr_ptr = builder_->CreateStructGEP(val_ptr, 0, "ptr_ptr");
    SetPtr(builder_->CreateLoad(ptr_ptr, "ptr"));
    Value* len_ptr = builder_->CreateStructGEP(val_ptr, 1, "len_ptr");
    SetLen(builder_->CreateLoad(len_ptr, "len"));
  } else if (type_.type == TYPE_TIMESTAMP) {
    // Convert TimestampValue to TimestampVal
    Value* time_of_day_ptr = builder_->CreateStructGEP(val_ptr, 0, "time_of_day_ptr");
    // Cast boost::posix_time::time_duration to i64
    Value* time_of_day_cast =
        builder_->CreateBitCast(time_of_day_ptr, codegen_->GetPtrType(TYPE_BIGINT));
    SetTimeOfDay(builder_->CreateLoad(time_of_day_cast, "time_of_day"));
    Value* date_ptr = builder_->CreateStructGEP(val_ptr, 1, "date_ptr");
    // Cast boost::gregorian::date to i32
    Value* date_cast = builder_->CreateBitCast(date_ptr, codegen_->GetPtrType(TYPE_INT));
    SetDate(builder_->CreateLoad(date_cast, "date"));
  } else {
    // val_ptr is a native type
    Value* val = builder_->CreateLoad(val_ptr, "val");
    SetVal(val);
  }
}

// Example output: (num_bits = 8)
// %1 = zext i1 %src to i16
// %2 = shl i16 %1, 8
// %3 = and i16 %dst1 255 ; clear the top half of dst
// %dst2 = or i16 %3, %2  ; set the top of half of dst to src
Value* CodegenAnyVal::SetHighBits(int num_bits, Value* src, Value* dst,
                                  const char* name) {
  Value* extended_src =
      builder_->CreateZExt(src, IntegerType::get(codegen_->context(), num_bits * 2));
  Value* shifted_src = builder_->CreateShl(extended_src, num_bits);
  Value* masked_dst = builder_->CreateAnd(dst, (1 << num_bits) - 1);
  return builder_->CreateOr(masked_dst, shifted_src, name);
}

Value* CodegenAnyVal::GetNullVal(LlvmCodeGen* codegen, const ColumnType& type) {
  Type* val_type = GetLoweredType(codegen, type);
  if (val_type->isStructTy()) {
    // Return the struct { 1, 0 } (the 'is_null' byte, i.e. the first value's first byte,
    // is set to 1, the other bytes don't matter)
    StructType* struct_type = cast<StructType>(val_type);
    DCHECK_EQ(struct_type->getNumElements(), 2);
    Type* type1 = struct_type->getElementType(0);
    Type* type2 = struct_type->getElementType(1);
    return ConstantStruct::get(
        struct_type, ConstantInt::get(type1, 1), ConstantInt::get(type2, 0), NULL);
  }
  // Return the int 1 ('is_null' byte is 1, other bytes don't matter)
  DCHECK(val_type->isIntegerTy());
  return ConstantInt::get(val_type, 1);
}

CodegenAnyVal CodegenAnyVal::GetNonNullVal(LlvmCodeGen* codegen,
    LlvmCodeGen::LlvmBuilder* builder, const ColumnType& type, const char* name) {
  Type* val_type = GetLoweredType(codegen, type);
  // All zeros => 'is_null' = false
  Value* value = Constant::getNullValue(val_type);
  return CodegenAnyVal(codegen, builder, type, value, name);
}

AnyVal* CreateAnyVal(ObjectPool* pool, const ColumnType& type) {
  switch(type.type) {
    case TYPE_NULL: return pool->Add(new AnyVal);
    case TYPE_BOOLEAN: return pool->Add(new BooleanVal);
    case TYPE_TINYINT: return pool->Add(new TinyIntVal);
    case TYPE_SMALLINT: return pool->Add(new SmallIntVal);
    case TYPE_INT: return pool->Add(new IntVal);
    case TYPE_BIGINT: return pool->Add(new BigIntVal);
    case TYPE_FLOAT: return pool->Add(new FloatVal);
    case TYPE_DOUBLE: return pool->Add(new DoubleVal);
    case TYPE_STRING: return pool->Add(new StringVal);
    case TYPE_TIMESTAMP: return pool->Add(new TimestampVal);
    case TYPE_DECIMAL: return pool->Add(new DecimalVal);
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return NULL;
  }
}
