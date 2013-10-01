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

#include "exprs/udf-util.h"

using namespace llvm;
using namespace impala_udf;

namespace impala {

CodegenAnyVal::CodegenAnyVal(LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder,
                             PrimitiveType type, Value* value, const char* name)
  : type_(type),
    value_(value),
    name_(name),
    codegen_(codegen),
    builder_(builder) {
  Type* value_type = GetType(codegen, type);
  if (value_ == NULL) {
    // No Value* was specified, so allocate one on the stack and load it.
    Value* ptr = builder_->CreateAlloca(value_type, 0);
    value_ = builder_->CreateLoad(ptr, name_);
  }
  DCHECK_EQ(value_->getType(), value_type);
}

void CodegenAnyVal::SetIsNull(Value* is_null) {
  if (type_ == TYPE_BIGINT || type_ == TYPE_DOUBLE) {
    // Lowered type is of form { i8, * }. Set the i8 value to 'is_null'.
    Value* is_null_ext =
        builder_->CreateZExt(is_null, codegen_->tinyint_type(), "is_null_ext");
    value_ = builder_->CreateInsertValue(value_, is_null_ext, 0, name_);
    return;
  }

  if (type_ == TYPE_STRING) {
    // Lowered type is of form { i64, i8* }. Set the first byte of the i64 value to
    // 'is_null'
    Value* v = builder_->CreateExtractValue(value_, 0);
    v = builder_->CreateAnd(v, -0x100LL, "masked");
    Value* is_null_ext = builder_->CreateZExt(is_null, v->getType(), "is_null_ext");
    v = builder_->CreateOr(v, is_null_ext);
    value_ = builder_->CreateInsertValue(value_, v, 0, name_);
    return;
  }

  // Lowered type is an integer. Set the first byte to 'is_null'.
  value_ = builder_->CreateAnd(value_, -0x100LL, "masked");
  Value* is_null_ext = builder_->CreateZExt(is_null, value_->getType(), "is_null_ext");
  value_ = builder_->CreateOr(value_, is_null_ext, name_);
}

void CodegenAnyVal::SetVal(Value* val) {
  switch(type_) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:{
      // Lowered type is an integer. Set the high bytes to 'val'.
      int num_bits = GetByteSize(type_) * 8;
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
    default:
      DCHECK(false) << "Unsupported type: " << type_;
  }
}

void CodegenAnyVal::SetPtr(Value* ptr) {
  // Set the second pointer value to 'ptr'.
  DCHECK_EQ(type_, TYPE_STRING);
  value_ = builder_->CreateInsertValue(value_, ptr, 1, name_);
}

void CodegenAnyVal::SetLen(Value* len) {
  // Set the high bytes of the first value to 'len'.
  DCHECK_EQ(type_, TYPE_STRING);
  Value* v = builder_->CreateExtractValue(value_, 0);
  v = SetHighBits(32, len, v);
  value_ = builder_->CreateInsertValue(value_, v, 0, name_);
}

void CodegenAnyVal::SetFromRawPtr(Value* raw_ptr) {
  Value* val_ptr =
      builder_->CreateBitCast(raw_ptr, codegen_->GetPtrType(type_), "val_ptr");
  if (type_ == TYPE_STRING) {
    Value* ptr_ptr = builder_->CreateStructGEP(val_ptr, 0, "ptr_ptr");
    Value* ptr = builder_->CreateLoad(ptr_ptr, "ptr");
    SetPtr(ptr);
    Value* len_ptr = builder_->CreateStructGEP(val_ptr, 1, "len_ptr");
    Value* len = builder_->CreateLoad(len_ptr, "len");
    SetLen(len);
  } else {
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

Type* CodegenAnyVal::GetType(LlvmCodeGen* cg, PrimitiveType type) {
  switch(type) {
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
    default:
      DCHECK(false) << "Unsupported type: " << type;
  }
}

Value* CodegenAnyVal::GetNullVal(LlvmCodeGen* codegen, PrimitiveType type) {
  Type* val_type = GetType(codegen, type);
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
    LlvmCodeGen::LlvmBuilder* builder, PrimitiveType type, const char* name) {
  Type* val_type = GetType(codegen, type);
  // All zeros => 'is_null' = false
  Value* value = Constant::getNullValue(val_type);
  return CodegenAnyVal(codegen, builder, type, value, name);
}

AnyVal* CreateAnyVal(ObjectPool* pool, PrimitiveType type) {
  switch(type) {
    case TYPE_BOOLEAN: return pool->Add(new BooleanVal);
    case TYPE_TINYINT: return pool->Add(new TinyIntVal);
    case TYPE_SMALLINT: return pool->Add(new SmallIntVal);
    case TYPE_INT: return pool->Add(new IntVal);
    case TYPE_BIGINT: return pool->Add(new BigIntVal);
    case TYPE_FLOAT: return pool->Add(new FloatVal);
    case TYPE_DOUBLE: return pool->Add(new DoubleVal);
    case TYPE_STRING: return pool->Add(new StringVal);
    case TYPE_TIMESTAMP: return pool->Add(new TimestampVal);
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return NULL;
  }
}

}
