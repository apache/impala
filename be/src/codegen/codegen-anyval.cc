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

#include "codegen/codegen-anyval.h"

#include "codegen/codegen-util.h"
#include "runtime/multi-precision.h"
#include "runtime/raw-value.h"
#include "common/names.h"

using namespace impala;
using namespace impala_udf;

const char* CodegenAnyVal::LLVM_BOOLEANVAL_NAME   = "struct.impala_udf::BooleanVal";
const char* CodegenAnyVal::LLVM_TINYINTVAL_NAME   = "struct.impala_udf::TinyIntVal";
const char* CodegenAnyVal::LLVM_SMALLINTVAL_NAME  = "struct.impala_udf::SmallIntVal";
const char* CodegenAnyVal::LLVM_INTVAL_NAME       = "struct.impala_udf::IntVal";
const char* CodegenAnyVal::LLVM_BIGINTVAL_NAME    = "struct.impala_udf::BigIntVal";
const char* CodegenAnyVal::LLVM_FLOATVAL_NAME     = "struct.impala_udf::FloatVal";
const char* CodegenAnyVal::LLVM_DOUBLEVAL_NAME    = "struct.impala_udf::DoubleVal";
const char* CodegenAnyVal::LLVM_STRINGVAL_NAME = "struct.impala_udf::StringVal";
const char* CodegenAnyVal::LLVM_TIMESTAMPVAL_NAME = "struct.impala_udf::TimestampVal";
const char* CodegenAnyVal::LLVM_DECIMALVAL_NAME   = "struct.impala_udf::DecimalVal";
const char* CodegenAnyVal::LLVM_DATEVAL_NAME      = "struct.impala_udf::DateVal";
const char* CodegenAnyVal::LLVM_COLLECTIONVAL_NAME = "struct.impala_udf::CollectionVal";

llvm::Type* CodegenAnyVal::GetLoweredType(LlvmCodeGen* cg, const ColumnType& type) {
  switch (type.type) {
    case TYPE_BOOLEAN: // i16
#ifndef __aarch64__
      return cg->i16_type();
#else
      return cg->i64_type();
#endif
    case TYPE_TINYINT: // i16
#ifndef __aarch64__
      return cg->i16_type();
#else
      return cg->i64_type();
#endif
    case TYPE_SMALLINT: // i32
#ifndef __aarch64__
      return cg->i32_type();
#else
      return cg->i64_type();
#endif
    case TYPE_INT: // i64
      return cg->i64_type();
    case TYPE_BIGINT: // { i8, i64 }
#ifndef __aarch64__
      return llvm::StructType::get(cg->i8_type(), cg->i64_type());
#else
      return llvm::ArrayType::get(cg->i64_type(), 2);
#endif
    case TYPE_FLOAT: // i64
      return cg->i64_type();
    case TYPE_DOUBLE: // { i8, double }
#ifndef __aarch64__
      return llvm::StructType::get(cg->i8_type(), cg->double_type());
#else
      return llvm::ArrayType::get(cg->i64_type(), 2);
#endif
    case TYPE_STRING: // { i64, i8* }
    case TYPE_VARCHAR: // { i64, i8* }
    case TYPE_CHAR: // Uses StringVal, so same as STRING/VARCHAR.
    case TYPE_FIXED_UDA_INTERMEDIATE: // { i64, i8* }
    case TYPE_ARRAY: // CollectionVal has same memory layout as StringVal.
    case TYPE_MAP: // CollectionVal has same memory layout as StringVal.
#ifndef __aarch64__
      return llvm::StructType::get(cg->i64_type(), cg->ptr_type());
#else
      return llvm::ArrayType::get(cg->i64_type(), 2);
#endif
    case TYPE_TIMESTAMP: // { i64, i64 }
#ifndef __aarch64__
      return llvm::StructType::get(cg->i64_type(), cg->i64_type());
#else
      return llvm::ArrayType::get(cg->i64_type(), 2);
#endif
    case TYPE_DECIMAL: // %"struct.impala_udf::DecimalVal" (isn't lowered)
                       // = { {i8}, [15 x i8], {i128} }
      return cg->GetNamedType(LLVM_DECIMALVAL_NAME);
    case TYPE_DATE: // i64
      return cg->i64_type();
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return NULL;
  }
}

llvm::PointerType* CodegenAnyVal::GetLoweredPtrType(
    LlvmCodeGen* cg, const ColumnType& type) {
  return GetLoweredType(cg, type)->getPointerTo();
}

llvm::Type* CodegenAnyVal::GetUnloweredType(LlvmCodeGen* cg, const ColumnType& type) {
  llvm::Type* result;
  switch(type.type) {
    case TYPE_BOOLEAN:
      result = cg->GetNamedType(LLVM_BOOLEANVAL_NAME);
      break;
    case TYPE_TINYINT:
      result = cg->GetNamedType(LLVM_TINYINTVAL_NAME);
      break;
    case TYPE_SMALLINT:
      result = cg->GetNamedType(LLVM_SMALLINTVAL_NAME);
      break;
    case TYPE_INT:
      result = cg->GetNamedType(LLVM_INTVAL_NAME);
      break;
    case TYPE_BIGINT:
      result = cg->GetNamedType(LLVM_BIGINTVAL_NAME);
      break;
    case TYPE_FLOAT:
      result = cg->GetNamedType(LLVM_FLOATVAL_NAME);
      break;
    case TYPE_DOUBLE:
      result = cg->GetNamedType(LLVM_DOUBLEVAL_NAME);
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE:
      result = cg->GetNamedType(LLVM_STRINGVAL_NAME);
      break;
    case TYPE_TIMESTAMP:
      result = cg->GetNamedType(LLVM_TIMESTAMPVAL_NAME);
      break;
    case TYPE_DECIMAL:
      result = cg->GetNamedType(LLVM_DECIMALVAL_NAME);
      break;
    case TYPE_DATE:
      result = cg->GetNamedType(LLVM_DATEVAL_NAME);
      break;
    case TYPE_ARRAY:
    case TYPE_MAP:
      result = cg->GetNamedType(LLVM_COLLECTIONVAL_NAME);
      break;
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return NULL;
  }
  DCHECK(result != NULL) << type.DebugString();
  return result;
}

llvm::PointerType* CodegenAnyVal::GetUnloweredPtrType(
    LlvmCodeGen* cg, const ColumnType& type) {
  return GetUnloweredType(cg, type)->getPointerTo();
}

llvm::Value* CodegenAnyVal::CreateCall(LlvmCodeGen* cg, LlvmBuilder* builder,
    llvm::Function* fn, llvm::ArrayRef<llvm::Value*> args, const char* name,
    llvm::Value* result_ptr) {
  if (fn->getReturnType()->isVoidTy()) {
    // Void return type indicates that this function returns a DecimalVal via the first
    // argument (which should be a DecimalVal*).
    llvm::Function::arg_iterator ret_arg = fn->arg_begin();
    DCHECK(ret_arg->getType()->isPointerTy());
    llvm::Type* ret_type = ret_arg->getType()->getPointerElementType();
    DCHECK_EQ(ret_type, cg->GetNamedType(LLVM_DECIMALVAL_NAME));

    // We need to pass a DecimalVal pointer to 'fn' that will be populated with the result
    // value. Use 'result_ptr' if specified, otherwise alloca one.
    llvm::Value* ret_ptr = (result_ptr == NULL) ?
        cg->CreateEntryBlockAlloca(*builder, ret_type, name) :
        result_ptr;
    vector<llvm::Value*> new_args = args.vec();
    new_args.insert(new_args.begin(), ret_ptr);
    // Bitcasting the args is often necessary when calling an IR UDF because the types
    // in the IR module may have been renamed while linking. Bitcasting them avoids a
    // type assertion.
    CodeGenUtil::CreateCallWithBitCasts(builder, fn, new_args);

    // If 'result_ptr' was specified, we're done. Otherwise load and return the result.
    if (result_ptr != NULL) return NULL;
    return builder->CreateLoad(ret_ptr, name);
  } else {
    // Function returns *Val normally (note that it could still be returning a
    // DecimalVal, since we generate non-compliant functions).
    // Bitcasting the args is often necessary when calling an IR UDF because the types
    // in the IR module may have been renamed while linking. Bitcasting them avoids a
    // type assertion.
    llvm::Value* ret = CodeGenUtil::CreateCallWithBitCasts(builder, fn, args, name);
    if (result_ptr == NULL) return ret;
    builder->CreateStore(ret, result_ptr);
    return NULL;
  }
}

CodegenAnyVal CodegenAnyVal::CreateCallWrapped(LlvmCodeGen* cg, LlvmBuilder* builder,
    const ColumnType& type, llvm::Function* fn, llvm::ArrayRef<llvm::Value*> args,
    const char* name) {
  llvm::Value* v = CreateCall(cg, builder, fn, args, name);
  return CodegenAnyVal(cg, builder, type, v, name);
}

CodegenAnyVal::CodegenAnyVal(LlvmCodeGen* codegen, LlvmBuilder* builder,
    const ColumnType& type, llvm::Value* value, const char* name)
  : type_(type), value_(value), name_(name), codegen_(codegen), builder_(builder) {
  llvm::Type* value_type = GetLoweredType(codegen, type);
  if (value_ == NULL) {
    // No Value* was specified, so allocate one on the stack and load it.
    llvm::Value* ptr = codegen_->CreateEntryBlockAlloca(*builder, value_type);
    value_ = builder_->CreateLoad(ptr, name_);
  }
  DCHECK_EQ(value_->getType(), value_type);
}

llvm::Value* CodegenAnyVal::GetIsNull(const char* name) const {
  switch (type_.type) {
    case TYPE_BIGINT:
    case TYPE_DOUBLE: {
      // Lowered type is of form { i8, * }. Get the i8 value.
      // On aarch64, Lowered type is of form { i64, * }
      llvm::Value* is_null = builder_->CreateExtractValue(value_, 0);
#ifndef __aarch64__
      DCHECK(is_null->getType() == codegen_->i8_type());
#else
      DCHECK(is_null->getType() == codegen_->i64_type());
#endif
      return builder_->CreateTrunc(is_null, codegen_->bool_type(), name);
    }
    case TYPE_DECIMAL: {
      // Lowered type is of the form { {i8}, ... }
      uint32_t idxs[] = {0, 0};
      llvm::Value* is_null_i8 = builder_->CreateExtractValue(value_, idxs);
      DCHECK(is_null_i8->getType() == codegen_->i8_type());
      return builder_->CreateTrunc(is_null_i8, codegen_->bool_type(), name);
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE:
    case TYPE_TIMESTAMP:
    case TYPE_ARRAY:
    case TYPE_MAP: {
      // Lowered type is of form { i64, *}. Get the first byte of the i64 value.
      llvm::Value* v = builder_->CreateExtractValue(value_, 0);
      DCHECK(v->getType() == codegen_->i64_type());
      return builder_->CreateTrunc(v, codegen_->bool_type(), name);
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_DATE:
    case TYPE_FLOAT:
      // Lowered type is an integer. Get the first byte.
      return builder_->CreateTrunc(value_, codegen_->bool_type(), name);
    default:
      DCHECK(false);
      return NULL;
  }
}

void CodegenAnyVal::SetIsNull(llvm::Value* is_null) {
  switch(type_.type) {
    case TYPE_BIGINT:
    case TYPE_DOUBLE: {
      // Lowered type is of form { i8, * }. Set the i8 value to 'is_null'.
      // On aarch64, lowered type is of form { i64, * }
#ifndef __aarch64__
      llvm::Value* is_null_ext =
          builder_->CreateZExt(is_null, codegen_->i8_type(), "is_null_ext");
#else
      llvm::Value* is_null_ext =
          builder_->CreateZExt(is_null, codegen_->i64_type(), "is_null_ext");
#endif
      value_ = builder_->CreateInsertValue(value_, is_null_ext, 0, name_);
      break;
    }
    case TYPE_DECIMAL: {
      // Lowered type is of form { {i8}, [15 x i8], {i128} }. Set the i8 value to
      // 'is_null'.
      llvm::Value* is_null_ext =
          builder_->CreateZExt(is_null, codegen_->i8_type(), "is_null_ext");
      // Index into the {i8} struct as well as the outer struct.
      uint32_t idxs[] = {0, 0};
      value_ = builder_->CreateInsertValue(value_, is_null_ext, idxs, name_);
      break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE:
    case TYPE_TIMESTAMP:
    case TYPE_ARRAY:
    case TYPE_MAP: {
      // Lowered type is of the form { i64, * }. Set the first byte of the i64 value to
      // 'is_null'
      llvm::Value* v = builder_->CreateExtractValue(value_, 0);
      v = builder_->CreateAnd(v, -0x100LL, "masked");
      llvm::Value* is_null_ext =
          builder_->CreateZExt(is_null, v->getType(), "is_null_ext");
      v = builder_->CreateOr(v, is_null_ext);
      value_ = builder_->CreateInsertValue(value_, v, 0, name_);
      break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_DATE:
    case TYPE_FLOAT: {
      // Lowered type is an integer. Set the first byte to 'is_null'.
      value_ = builder_->CreateAnd(value_, -0x100LL, "masked");
      llvm::Value* is_null_ext =
          builder_->CreateZExt(is_null, value_->getType(), "is_null_ext");
      value_ = builder_->CreateOr(value_, is_null_ext, name_);
      break;
    }
    default:
      DCHECK(false) << "NYI: " << type_.DebugString();
  }
}

llvm::Value* CodegenAnyVal::GetVal(const char* name) {
  DCHECK(type_.type != TYPE_STRING)
      << "Use GetPtr and GetLen for StringVal";
  DCHECK(type_.type != TYPE_VARCHAR)
      << "Use GetPtr and GetLen for Varchar";
  DCHECK(type_.type != TYPE_CHAR)
      << "Use GetPtr and GetLen for Char";
  DCHECK(type_.type != TYPE_FIXED_UDA_INTERMEDIATE)
      << "Use GetPtr and GetLen for FixedUdaIntermediate";
  DCHECK(type_.type != TYPE_TIMESTAMP)
      << "Use GetDate and GetTimeOfDay for TimestampVals";
  switch(type_.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_DATE: {
      // Lowered type is an integer. Get the high bytes.
      int num_bits = type_.GetByteSize() * 8;
      llvm::Value* val = GetHighBits(num_bits, value_, name);
      if (type_.type == TYPE_BOOLEAN) {
        // Return booleans as i1 (vs. i8)
        val = builder_->CreateTrunc(val, builder_->getInt1Ty(), name);
      }
      return val;
    }
    case TYPE_FLOAT: {
      // Same as above, but we must cast the value to a float.
      llvm::Value* val = GetHighBits(32, value_);
      return builder_->CreateBitCast(val, codegen_->float_type());
    }
    case TYPE_BIGINT:
      return builder_->CreateExtractValue(value_, 1, name);
    case TYPE_DOUBLE: {
      // Lowered type is of form { i8, * }. Get the second value.
      llvm::Value* val = builder_->CreateExtractValue(value_, 1, name);
#ifdef __aarch64__
      val = builder_->CreateBitCast(val, codegen_->double_type());
#endif
      return val;
    }
    case TYPE_DECIMAL: {
#ifdef __aarch64__
      // On aarch64, the Lowered type is of form { {i8}, {i128} }. No padding add.
      uint32_t idxs[] = {1, 0};
#else
      // On x86-64, Lowered type is of form { {i8}, [15 x i8], {i128} }.
      uint32_t idxs[] = {2, 0};
#endif
      // Get the i128 value and truncate it to the correct size.
      // (The {i128} corresponds to the union of the different width int types.)
      llvm::Value* val = builder_->CreateExtractValue(value_, idxs, name);
      return builder_->CreateTrunc(val,
          codegen_->GetSlotType(type_), name);
    }
    default:
      DCHECK(false) << "Unsupported type: " << type_;
      return NULL;
  }
}

void CodegenAnyVal::SetVal(llvm::Value* val) {
  DCHECK(type_.type != TYPE_STRING) << "Use SetPtr and SetLen for StringVals";
  DCHECK(type_.type != TYPE_VARCHAR) << "Use SetPtr and SetLen for StringVals";
  DCHECK(type_.type != TYPE_CHAR) << "Use SetPtr and SetLen for StringVals";
  DCHECK(type_.type != TYPE_FIXED_UDA_INTERMEDIATE)
      << "Use SetPtr and SetLen for FixedUdaIntermediate";
  DCHECK(type_.type != TYPE_TIMESTAMP)
      << "Use SetDate and SetTimeOfDay for TimestampVals";
  DCHECK(!type_.IsCollectionType()) << "Use SetPtr and SetLen for CollectionVal";
  switch(type_.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_DATE: {
      // Lowered type is an integer. Set the high bytes to 'val'.
      int num_bits = type_.GetByteSize() * 8;
      value_ = SetHighBits(num_bits, val, value_, name_);
      break;
    }
    case TYPE_FLOAT:
      // Same as above, but we must cast 'val' to an integer type.
      val = builder_->CreateBitCast(val, codegen_->i32_type());
      value_ = SetHighBits(32, val, value_, name_);
      break;
    case TYPE_BIGINT:
      value_ = builder_->CreateInsertValue(value_, val, 1, name_);
      break;
    case TYPE_DOUBLE:
#ifdef __aarch64__
      val = builder_->CreateBitCast(val, codegen_->i64_type());
#endif
      // Lowered type is of form { i8, * }. Set the second value to 'val'.
      value_ = builder_->CreateInsertValue(value_, val, 1, name_);
      break;
    case TYPE_DECIMAL: {
      //  Set the i128 value to 'val'.
      //  (The {i128} corresponds to the union of the different width int types.)
      DCHECK_EQ(val->getType()->getIntegerBitWidth(), type_.GetByteSize() * 8);
      val = builder_->CreateSExt(val, llvm::Type::getIntNTy(codegen_->context(), 128));
#ifdef __aarch64__
      // On aarch64, the Lowered type is of form { {i8}, {i128} }. No padding add.
      uint32_t idxs[] = {1, 0};
#else
      // On X86-64, the Lowered type is of the form { {i8}, [15 x i8], {i128} }
      uint32_t idxs[] = {2, 0};
#endif
      value_ = builder_->CreateInsertValue(value_, val, idxs, name_);
      break;
    }
    default:
      DCHECK(false) << "Unsupported type: " << type_;
  }
}

void CodegenAnyVal::SetVal(bool val) {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN);
  SetVal(builder_->getInt1(val));
}

void CodegenAnyVal::SetVal(int8_t val) {
  DCHECK_EQ(type_.type, TYPE_TINYINT);
  SetVal(builder_->getInt8(val));
}

void CodegenAnyVal::SetVal(int16_t val) {
  DCHECK_EQ(type_.type, TYPE_SMALLINT);
  SetVal(builder_->getInt16(val));
}

void CodegenAnyVal::SetVal(int32_t val) {
  DCHECK(type_.type == TYPE_INT || type_.type == TYPE_DECIMAL || type_.type == TYPE_DATE);
  SetVal(builder_->getInt32(val));
}

void CodegenAnyVal::SetVal(int64_t val) {
  DCHECK(type_.type == TYPE_BIGINT || type_.type == TYPE_DECIMAL);
  SetVal(builder_->getInt64(val));
}

void CodegenAnyVal::SetVal(__int128_t val) {
  DCHECK_EQ(type_.type, TYPE_DECIMAL);
  vector<uint64_t> vals({LowBits(val), HighBits(val)});
  llvm::Value* ir_val =
      llvm::ConstantInt::get(codegen_->context(), llvm::APInt(128, vals));
  SetVal(ir_val);
}

void CodegenAnyVal::SetVal(float val) {
  DCHECK_EQ(type_.type, TYPE_FLOAT);
  SetVal(llvm::ConstantFP::get(builder_->getFloatTy(), val));
}

void CodegenAnyVal::SetVal(double val) {
  DCHECK_EQ(type_.type, TYPE_DOUBLE);
  SetVal(llvm::ConstantFP::get(builder_->getDoubleTy(), val));
}

llvm::Value* CodegenAnyVal::GetPtr() {
  // Set the second pointer value to 'ptr'.
  DCHECK(type_.IsStringType() || type_.IsCollectionType());
  llvm::Value* val = builder_->CreateExtractValue(value_, 1, name_);
#ifdef __aarch64__
  val = builder_->CreateIntToPtr(val, codegen_->ptr_type());
#endif
  return val;
}

llvm::Value* CodegenAnyVal::GetLen() {
  // Get the high bytes of the first value.
  DCHECK(type_.IsStringType() || type_.IsCollectionType());
  llvm::Value* v = builder_->CreateExtractValue(value_, 0);
  return GetHighBits(32, v);
}

void CodegenAnyVal::SetPtr(llvm::Value* ptr) {
  // Set the second pointer value to 'ptr'.
  DCHECK(type_.IsStringType() || type_.type == TYPE_FIXED_UDA_INTERMEDIATE
      || type_.IsCollectionType());
#ifdef __aarch64__
  ptr = builder_->CreatePtrToInt(ptr, codegen_->i64_type());
#endif
  value_ = builder_->CreateInsertValue(value_, ptr, 1, name_);
}

void CodegenAnyVal::SetLen(llvm::Value* len) {
  // Set the high bytes of the first value to 'len'.
  DCHECK(type_.IsStringType() || type_.type == TYPE_FIXED_UDA_INTERMEDIATE
      || type_.IsCollectionType());
  llvm::Value* v = builder_->CreateExtractValue(value_, 0);
  v = SetHighBits(32, len, v);
  value_ = builder_->CreateInsertValue(value_, v, 0, name_);
}

llvm::Value* CodegenAnyVal::GetTimeOfDay() {
  // Get the second i64 value.
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  return builder_->CreateExtractValue(value_, 1);
}

llvm::Value* CodegenAnyVal::GetDate() {
  // Get the high bytes of the first value.
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  llvm::Value* v = builder_->CreateExtractValue(value_, 0);
  return GetHighBits(32, v);
}

void CodegenAnyVal::SetTimeOfDay(llvm::Value* time_of_day) {
  // Set the second i64 value to 'time_of_day'.
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  value_ = builder_->CreateInsertValue(value_, time_of_day, 1, name_);
}

void CodegenAnyVal::SetDate(llvm::Value* date) {
  // Set the high bytes of the first value to 'date'.
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  llvm::Value* v = builder_->CreateExtractValue(value_, 0);
  v = SetHighBits(32, date, v);
  value_ = builder_->CreateInsertValue(value_, v, 0, name_);
}

llvm::Value* CodegenAnyVal::ConvertToPositiveZero(llvm::Value* val) {
  // Replaces negative zero with positive, leaves everything else unchanged.
  llvm::Value* is_negative_zero = builder_->CreateFCmpOEQ(
      val, llvm::ConstantFP::getNegativeZero(val->getType()), "cmp_zero");
  return builder_->CreateSelect(is_negative_zero,
                llvm::ConstantFP::get(val->getType(), 0.0), val);
}

void CodegenAnyVal::ConvertToCanonicalForm() {
  // Convert the value to a bit pattern that is unambiguous.
  // Specifically, for floating point type values, NaN values are converted to
  // the same bit pattern, and -0 is converted to +0.
  switch(type_.type) {
    case TYPE_FLOAT:
    case TYPE_DOUBLE: {
      llvm::Value* raw = GetVal();
      llvm::Value* canonical_val;
      if (type_.type == TYPE_FLOAT) {
        canonical_val = llvm::ConstantFP::getNaN(codegen_->float_type());
      } else {
        canonical_val = llvm::ConstantFP::getNaN(codegen_->double_type());
      }
      llvm::Value* is_nan = builder_->CreateFCmpUNO(raw, raw, "cmp_nan");

      SetVal(builder_->CreateSelect(is_nan, canonical_val, ConvertToPositiveZero(raw)));
      break;
    }
    default:
      ;
  }
}

llvm::Value* CodegenAnyVal::GetLoweredPtr(const string& name) const {
  llvm::Value* lowered_ptr =
      codegen_->CreateEntryBlockAlloca(*builder_, value_->getType(), name.c_str());
  builder_->CreateStore(GetLoweredValue(), lowered_ptr);
  return lowered_ptr;
}

llvm::Value* CodegenAnyVal::GetUnloweredPtr(const string& name) const {
  // Get an unlowered pointer by creating a lowered pointer then bitcasting it.
  // TODO: if the original value was unlowered, this generates roundabout code that
  // lowers the value and casts it back. Generally LLVM's optimiser can reason
  // about what's going on and undo our shenanigans to generate sane code, but it
  // would be nice to just emit reasonable code in the first place.
  return builder_->CreateBitCast(
      GetLoweredPtr(), GetUnloweredPtrType(codegen_, type_), name);
}

void CodegenAnyVal::LoadFromNativePtr(llvm::Value* raw_val_ptr) {
  DCHECK(raw_val_ptr->getType()->isPointerTy());
  llvm::Type* raw_val_type = raw_val_ptr->getType()->getPointerElementType();
  DCHECK_EQ(raw_val_type, codegen_->GetSlotType(type_))
      << endl
      << LlvmCodeGen::Print(raw_val_ptr) << endl
      << type_ << " => " << LlvmCodeGen::Print(
          codegen_->GetSlotType(type_));
  switch (type_.type) {
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      // Convert StringValue to StringVal
      llvm::Value* string_value = builder_->CreateLoad(raw_val_ptr, "string_value");
      SetPtr(builder_->CreateExtractValue(string_value, 0, "ptr"));
      SetLen(builder_->CreateExtractValue(string_value, 1, "len"));
      break;
    }
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE: {
      // Convert fixed-size slot to StringVal.
      SetPtr(builder_->CreateBitCast(raw_val_ptr, codegen_->ptr_type()));
      SetLen(codegen_->GetI32Constant(type_.len));
      break;
    }
    case TYPE_TIMESTAMP: {
      // Convert TimestampValue to TimestampVal
      // TimestampValue has type
      //   { boost::posix_time::time_duration, boost::gregorian::date }
      // = { {{{i64}}}, {{i32}} }

      llvm::Value* ts_value = builder_->CreateLoad(raw_val_ptr, "ts_value");
      // Extract time_of_day i64 from boost::posix_time::time_duration.
      uint32_t time_of_day_idxs[] = {0, 0, 0, 0};
      llvm::Value* time_of_day =
          builder_->CreateExtractValue(ts_value, time_of_day_idxs, "time_of_day");
      DCHECK(time_of_day->getType()->isIntegerTy(64));
      SetTimeOfDay(time_of_day);
      // Extract i32 from boost::gregorian::date
      uint32_t date_idxs[] = {1, 0, 0};
      llvm::Value* date = builder_->CreateExtractValue(ts_value, date_idxs, "date");
      DCHECK(date->getType()->isIntegerTy(32));
      SetDate(date);
      break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMAL:
    case TYPE_DATE:
      SetVal(builder_->CreateLoad(raw_val_ptr, "raw_val"));
      break;
    default:
      DCHECK(false) << "NYI: " << type_.DebugString();
      break;
  }
}

void CodegenAnyVal::StoreToNativePtr(llvm::Value* raw_val_ptr, llvm::Value* pool_val) {
  llvm::Type* raw_type = codegen_->GetSlotType(type_);
  switch (type_.type) {
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      // Convert StringVal to StringValue
      llvm::Value* string_value = llvm::Constant::getNullValue(raw_type);
      llvm::Value* len = GetLen();
      string_value = builder_->CreateInsertValue(string_value, len, 1);
      if (pool_val == nullptr) {
        // Set string_value.ptr from this->ptr
        string_value = builder_->CreateInsertValue(string_value, GetPtr(), 0);
      } else {
        // Allocate string_value.ptr from 'pool_val' and copy this->ptr
        llvm::Value* new_ptr =
            codegen_->CodegenMemPoolAllocate(builder_, pool_val, len, "new_ptr");
        codegen_->CodegenMemcpy(builder_, new_ptr, GetPtr(), len);
        string_value = builder_->CreateInsertValue(string_value, new_ptr, 0);
      }
      builder_->CreateStore(string_value, raw_val_ptr);
      break;
    }
    case TYPE_CHAR:
      codegen_->CodegenMemcpy(builder_, raw_val_ptr, GetPtr(), type_.len);
      break;
    case TYPE_FIXED_UDA_INTERMEDIATE:
      DCHECK(false) << "FIXED_UDA_INTERMEDIATE does not need to be copied: the "
                    << "StringVal must be set up to point to the output slot";
      break;
    case TYPE_TIMESTAMP: {
      // Convert TimestampVal to TimestampValue
      // TimestampValue has type
      //   { boost::posix_time::time_duration, boost::gregorian::date }
      // = { {{{i64}}}, {{i32}} }
      llvm::Value* timestamp_value = llvm::Constant::getNullValue(raw_type);
      uint32_t time_of_day_idxs[] = {0, 0, 0, 0};
      timestamp_value =
          builder_->CreateInsertValue(timestamp_value, GetTimeOfDay(), time_of_day_idxs);
      uint32_t date_idxs[] = {1, 0, 0};
      timestamp_value =
          builder_->CreateInsertValue(timestamp_value, GetDate(), date_idxs);
      builder_->CreateStore(timestamp_value, raw_val_ptr);
      break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMAL:
    case TYPE_DATE:
      // The representations of the types match - just store the value.
      builder_->CreateStore(GetVal(), raw_val_ptr);
      break;
    default:
      DCHECK(false) << "NYI: " << type_.DebugString();
      break;
  }
}

llvm::Value* CodegenAnyVal::ToNativePtr(llvm::Value* pool_val) {
  llvm::Value* native_ptr = codegen_->CreateEntryBlockAlloca(*builder_,
      codegen_->GetSlotType(type_));
  StoreToNativePtr(native_ptr, pool_val);
  return native_ptr;
}

// Example output for materializing an int slot:
//
//   ; [insert point starts here]
//   %is_null = trunc i64 %src to i1
//   br i1 %is_null, label %null, label %non_null ;
//
// non_null:                                         ; preds = %entry
//   %slot = getelementptr inbounds { i8, i32, %"struct.impala::StringValue" }* %tuple,
//       i32 0, i32 1
//   %2 = ashr i64 %src, 32
//   %3 = trunc i64 %2 to i32
//   store i32 %3, i32* %slot
//   br label %end_write
//
// null:                                             ; preds = %entry
//   call void @SetNull6({ i8, i32, %"struct.impala::StringValue" }* %tuple)
//   br label %end_write
//
// end_write:                                        ; preds = %null, %non_null
//   ; [insert point ends here]
void CodegenAnyVal::WriteToSlot(const SlotDescriptor& slot_desc, llvm::Value* tuple_val,
    llvm::Value* pool_val, llvm::BasicBlock* insert_before) {
  DCHECK(tuple_val->getType()->isPointerTy());
  DCHECK(tuple_val->getType()->getPointerElementType()->isStructTy());
  llvm::LLVMContext& context = codegen_->context();
  llvm::Function* fn = builder_->GetInsertBlock()->getParent();

  // Create new block that will come after conditional blocks if necessary
  if (insert_before == nullptr) {
    insert_before = llvm::BasicBlock::Create(context, "end_write", fn);
  }

  // Create new basic blocks and br instruction
  llvm::BasicBlock* non_null_block =
      llvm::BasicBlock::Create(context, "non_null", fn, insert_before);
  llvm::BasicBlock* null_block =
      llvm::BasicBlock::Create(context, "null", fn, insert_before);
  builder_->CreateCondBr(GetIsNull(), null_block, non_null_block);

  // Non-null block: write slot
  builder_->SetInsertPoint(non_null_block);
  llvm::Value* slot =
      builder_->CreateStructGEP(nullptr, tuple_val, slot_desc.llvm_field_idx(), "slot");
  StoreToNativePtr(slot, pool_val);
  builder_->CreateBr(insert_before);

  // Null block: set null bit
  builder_->SetInsertPoint(null_block);
  slot_desc.CodegenSetNullIndicator(
      codegen_, builder_, tuple_val, codegen_->true_value());
  builder_->CreateBr(insert_before);

  // Leave builder_ after conditional blocks
  builder_->SetInsertPoint(insert_before);
}

llvm::Value* CodegenAnyVal::Eq(CodegenAnyVal* other) {
  DCHECK_EQ(type_, other->type_);
  switch (type_.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_DECIMAL:
    case TYPE_DATE:
      return builder_->CreateICmpEQ(GetVal(), other->GetVal(), "eq");
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      // Use the ordering version "OEQ" to ensure that 'nan' != 'nan'.
      return builder_->CreateFCmpOEQ(GetVal(), other->GetVal(), "eq");
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE: {
      llvm::Function* eq_fn =
          codegen_->GetFunction(IRFunction::CODEGEN_ANYVAL_STRING_VAL_EQ, false);
      return builder_->CreateCall(eq_fn,
          llvm::ArrayRef<llvm::Value*>({GetUnloweredPtr(), other->GetUnloweredPtr()}),
          "eq");
    }
    case TYPE_TIMESTAMP: {
      llvm::Function* eq_fn =
          codegen_->GetFunction(IRFunction::CODEGEN_ANYVAL_TIMESTAMP_VAL_EQ, false);
      return builder_->CreateCall(eq_fn,
          llvm::ArrayRef<llvm::Value*>({GetUnloweredPtr(), other->GetUnloweredPtr()}),
          "eq");
    }
    default:
      DCHECK(false) << "NYI: " << type_.DebugString();
      return NULL;
  }
}

llvm::Value* CodegenAnyVal::EqToNativePtr(llvm::Value* native_ptr,
    bool inclusive_equality) {
  llvm::Value* val = NULL;
  if (!type_.IsStringType()) {
     val = builder_->CreateLoad(native_ptr);
  }
  switch (type_.type) {
    case TYPE_NULL:
      return codegen_->false_value();
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_DECIMAL:
    case TYPE_DATE:
      return builder_->CreateICmpEQ(GetVal(), val, "cmp_raw");
    case TYPE_FLOAT:
    case TYPE_DOUBLE:{
      // Use the ordering version "OEQ" to ensure that 'nan' != 'nan'.
      llvm::Value* local_val = GetVal();
      llvm::Value* cmp_raw = builder_->CreateFCmpOEQ(local_val, val, "cmp_raw");
      if (!inclusive_equality) return cmp_raw;

      // Mirror logic in HashTableCtx::Equals - IMPALA-6661
      llvm::Value* local_is_nan = builder_->CreateFCmpUNO(local_val,
          local_val, "local_val_is_nan");
      llvm::Value* val_is_nan = builder_->CreateFCmpUNO(val, val, "val_is_nan");
      llvm::Value* both_nan = builder_->CreateAnd(local_is_nan, val_is_nan);

      return builder_->CreateOr(cmp_raw, both_nan, "cmp_raw_with_nan");
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE: {
      llvm::Function* eq_fn =
          codegen_->GetFunction(IRFunction::CODEGEN_ANYVAL_STRING_VALUE_EQ, false);
      return builder_->CreateCall(eq_fn,
          llvm::ArrayRef<llvm::Value*>({GetUnloweredPtr(), native_ptr}), "cmp_raw");
    }
    case TYPE_TIMESTAMP: {
      llvm::Function* eq_fn =
          codegen_->GetFunction(IRFunction::CODEGEN_ANYVAL_TIMESTAMP_VALUE_EQ, false);
      return builder_->CreateCall(eq_fn,
          llvm::ArrayRef<llvm::Value*>({GetUnloweredPtr(), native_ptr}), "cmp_raw");
    }
    default:
      DCHECK(false) << "NYI: " << type_.DebugString();
      return NULL;
  }
}

llvm::Value* CodegenAnyVal::Compare(CodegenAnyVal* other, const char* name) {
  DCHECK_EQ(type_, other->type_);
  llvm::Value* v1 = ToNativePtr();
  llvm::Value* void_v1 = builder_->CreateBitCast(v1, codegen_->ptr_type());
  llvm::Value* v2 = other->ToNativePtr();
  llvm::Value* void_v2 = builder_->CreateBitCast(v2, codegen_->ptr_type());
  // Create a global constant of the values' ColumnType. It needs to be a constant
  // for constant propagation and dead code elimination in 'compare_fn'.
  llvm::Type* col_type = codegen_->GetStructType<ColumnType>();
  llvm::Constant* type_ptr =
      codegen_->ConstantToGVPtr(col_type, type_.ToIR(codegen_), "type");
  llvm::Function* compare_fn =
      codegen_->GetFunction(IRFunction::RAW_VALUE_COMPARE, false);
  llvm::Value* args[] = {void_v1, void_v2, type_ptr};
  return builder_->CreateCall(compare_fn, args, name);
}

void CodegenAnyVal::CodegenBranchIfNull(
    LlvmBuilder* builder, llvm::BasicBlock* null_block) {
  llvm::Value* is_null = GetIsNull();
  llvm::BasicBlock* not_null_block = llvm::BasicBlock::Create(
      codegen_->context(), "not_null", builder->GetInsertBlock()->getParent());
  builder->CreateCondBr(is_null, null_block, not_null_block);
  builder->SetInsertPoint(not_null_block);
}

llvm::Value* CodegenAnyVal::GetHighBits(int num_bits, llvm::Value* v, const char* name) {
#ifndef __aarch64__
  DCHECK_EQ(v->getType()->getIntegerBitWidth(), num_bits * 2);
#endif
  llvm::Value* shifted = builder_->CreateAShr(v, num_bits);
  return builder_->CreateTrunc(
      shifted, llvm::IntegerType::get(codegen_->context(), num_bits));
}

// Example output: (num_bits = 8)
// %1 = zext i1 %src to i16
// %2 = shl i16 %1, 8
// %3 = and i16 %dst1 255 ; clear the top half of dst
// %dst2 = or i16 %3, %2  ; set the top of half of dst to src
llvm::Value* CodegenAnyVal::SetHighBits(
    int num_bits, llvm::Value* src, llvm::Value* dst, const char* name) {
  DCHECK_LE(src->getType()->getIntegerBitWidth(), num_bits);
#ifndef __aarch64__
  DCHECK_EQ(dst->getType()->getIntegerBitWidth(), num_bits * 2);
  llvm::Value* extended_src = builder_->CreateZExt(
      src, llvm::IntegerType::get(codegen_->context(), num_bits * 2));
#else
  llvm::Value* extended_src = builder_->CreateZExt(src,
        llvm::IntegerType::get(codegen_->context(), 64));
#endif
  llvm::Value* shifted_src = builder_->CreateShl(extended_src, num_bits);
  llvm::Value* masked_dst = builder_->CreateAnd(dst, (1LL << num_bits) - 1);
  return builder_->CreateOr(masked_dst, shifted_src, name);
}

llvm::Value* CodegenAnyVal::GetNullVal(LlvmCodeGen* codegen, const ColumnType& type) {
  llvm::Type* val_type = GetLoweredType(codegen, type);
  return GetNullVal(codegen, val_type);
}

llvm::Value* CodegenAnyVal::GetNullVal(LlvmCodeGen* codegen, llvm::Type* val_type) {
  if (val_type->isStructTy()) {
    llvm::StructType* struct_type = llvm::cast<llvm::StructType>(val_type);
    if (struct_type->getNumElements() == 3) {
      DCHECK_EQ(val_type, codegen->GetNamedType(LLVM_DECIMALVAL_NAME));
      // Return the struct { {1}, 0, 0 } (the 'is_null' byte, i.e. the first value's first
      // byte, is set to 1, the other bytes don't matter)
      llvm::StructType* anyval_struct_type =
          llvm::cast<llvm::StructType>(struct_type->getElementType(0));
      llvm::Type* is_null_type = anyval_struct_type->getElementType(0);
      llvm::Constant* null_anyval = llvm::ConstantStruct::get(
          anyval_struct_type, llvm::ConstantInt::get(is_null_type, 1));
      llvm::Type* type2 = struct_type->getElementType(1);
      llvm::Type* type3 = struct_type->getElementType(2);
      return llvm::ConstantStruct::get(struct_type, null_anyval,
          llvm::Constant::getNullValue(type2), llvm::Constant::getNullValue(type3));
    }
#ifdef __aarch64__
    else if (struct_type->getElementType(0)->isStructTy()) {
      llvm::StructType* anyval_struct_type =
          llvm::cast<llvm::StructType>(struct_type->getElementType(0));
      llvm::Type* is_null_type = anyval_struct_type->getElementType(0);
      llvm::Constant* null_anyval = llvm::ConstantStruct::get(
          anyval_struct_type, llvm::ConstantInt::get(is_null_type, 1));
      llvm::Type* type1 = struct_type->getElementType(1);
      return llvm::ConstantStruct::get(struct_type, null_anyval,
          llvm::Constant::getNullValue(type1));
    }
#endif
    // Return the struct { 1, 0 } (the 'is_null' byte, i.e. the first value's first byte,
    // is set to 1, the other bytes don't matter)
    DCHECK_EQ(struct_type->getNumElements(), 2);
    llvm::Type* type1 = struct_type->getElementType(0);
    DCHECK(type1->isIntegerTy()) << LlvmCodeGen::Print(type1);
    llvm::Type* type2 = struct_type->getElementType(1);
    return llvm::ConstantStruct::get(struct_type, llvm::ConstantInt::get(type1, 1),
        llvm::Constant::getNullValue(type2));
  }
#ifdef __aarch64__
  if (val_type->isArrayTy()) {
    llvm::ArrayType* array_type = llvm::cast<llvm::ArrayType>(val_type);
    DCHECK_EQ(array_type->getNumElements(), 2);
    llvm::Type* type1 = array_type->getElementType();
    DCHECK(type1->isIntegerTy()) << LlvmCodeGen::Print(type1);
    std::vector<llvm::Constant *> arrayElts;
    arrayElts.push_back(llvm::ConstantInt::get(type1, 1));
    arrayElts.push_back(llvm::Constant::getNullValue(type1));
    return llvm::ConstantArray::get(array_type, arrayElts);
  }
#endif
  // Return the int 1 ('is_null' byte is 1, other bytes don't matter)
  DCHECK(val_type->isIntegerTy());
  return llvm::ConstantInt::get(val_type, 1);
}

CodegenAnyVal CodegenAnyVal::GetNonNullVal(LlvmCodeGen* codegen, LlvmBuilder* builder,
    const ColumnType& type, const char* name) {
  llvm::Type* val_type = GetLoweredType(codegen, type);
  // All zeros => 'is_null' = false
  llvm::Value* value = llvm::Constant::getNullValue(val_type);
  return CodegenAnyVal(codegen, builder, type, value, name);
}
