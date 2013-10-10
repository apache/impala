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


#ifndef IMPALA_EXPRS_ANYVAL_UTIL_H
#define IMPALA_EXPRS_ANYVAL_UTIL_H

#include "codegen/llvm-codegen.h"
#include "runtime/primitive-type.h"
#include "udf/udf.h"
#include "util/hash-util.h"

using namespace impala_udf;

namespace impala {

// Utilities for AnyVals
class AnyValUtil {
 public:
  static uint32_t Hash(const BooleanVal& v, int seed) {
    return HashUtil::Hash(&v.val, 1, seed);
  }

  static uint32_t Hash(const TinyIntVal& v, int seed) {
    return HashUtil::Hash(&v.val, 1, seed);
  }

  static uint32_t Hash(const SmallIntVal& v, int seed) {
    return HashUtil::Hash(&v.val, 2, seed);
  }

  static uint32_t Hash(const IntVal& v, int seed) {
    return HashUtil::Hash(&v.val, 4, seed);
  }

  static uint32_t Hash(const BigIntVal& v, int seed) {
    return HashUtil::Hash(&v.val, 8, seed);
  }

  static uint32_t Hash(const FloatVal& v, int seed) {
    return HashUtil::Hash(&v.val, 4, seed);
  }

  static uint32_t Hash(const DoubleVal& v, int seed) {
    return HashUtil::Hash(&v.val, 8, seed);
  }

  static uint32_t Hash(const StringVal& v, int seed) {
    return HashUtil::Hash(v.ptr, v.len, seed);
  }

  static uint32_t Hash(const TimestampVal& v, int seed) {
    TimestampValue tv = TimestampValue::FromTimestampVal(v);
    return tv.Hash(seed);
  }
};

// Class for handling AnyVal subclasses during codegen. Codegen functions should use this
// wrapper instead of creating or manipulating *Val values directly in most cases. This is
// because the struct types must be lowered to integer types in many cases in order to
// conform to the standard calling convention (e.g., { i8, i32 } => i64). This class wraps
// the lowered types for each *Val struct.
//
// This class conceptually represents a single *Val that is mutated, but operates by
// generating IR instructions involving value_ (each of which generates a new Value*,
// since IR uses SSA), and then setting value_ to the most recent Value* generated. The
// generated instructions perform the integer manipulation equivalent to setting the
// fields of the original struct type.
//
// Lowered types:
// TYPE_BOOLEAN/BooleanVal: i16
// TYPE_TINYINT/TinyIntVal: i16
// TYPE_SMALLINT/SmallIntVal: i32
// TYPE_INT/INTVal: i64
// TYPE_BIGINT/BigIntVal: { i8, i64 }
// TYPE_FLOAT/FloatVal: i64
// TYPE_DOUBLE/DoubleVal: { i8, double }
// TYPE_STRING/StringVal: { i64, i8* }
// TYPE_TIMESTAMP/TimestampVal: { i64, i64 }
class CodegenAnyVal {
 public:
  // Returns the lowered AnyVal type associated with 'type'.
  // E.g.: TYPE_BOOLEAN (which corresponds to a BooleanVal) => i16
  static llvm::Type* GetType(LlvmCodeGen* cg, PrimitiveType type);

  // Return the constant type-lowered value corresponding to a null *Val.
  // E.g.: passing TYPE_DOUBLE (corresponding to the lowered DoubleVal { i8, double })
  // returns the constant struct { 1, 0.0 }
  static llvm::Value* GetNullVal(LlvmCodeGen* codegen, PrimitiveType type);

  // Return the constant type-lowered value corresponding to a non-null *Val.
  // E.g.: TYPE_DOUBLE (lowered DoubleVal: { i8, double }) => { 0, 0 }
  // This returns a CodegenAnyVal, rather than the unwrapped Value*, because the actual
  // value still needs to be set.
  static CodegenAnyVal GetNonNullVal(LlvmCodeGen* codegen,
      LlvmCodeGen::LlvmBuilder* builder, PrimitiveType type, const char* name = "");

  // Creates a wrapper around a lowered *Val value.
  //
  // Instructions for manipulating the value are generated using 'builder'. The insert
  // point of 'builder' is not modified by this class, and it is safe to call
  // 'builder'.SetInsertPoint() after passing 'builder' to this class.
  //
  // 'type' identified the type of wrapped value (e.g., TYPE_INT corresponds to IntVal,
  // which is lowered to i64).
  //
  // If 'value' is NULL, a new value of the lowered type is alloca'd. Otherwise 'value'
  // must be of the correct lowered type.
  //
  // If 'name' is specified, it will be used when generated instructions that set value_.
  CodegenAnyVal(LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder,
                PrimitiveType type, llvm::Value* value = NULL, const char* name = "");

  // Returns the current type-lowered value.
  llvm::Value* value() { return value_; }

  // Sets the 'is_null' field of the *Val.
  void SetIsNull(llvm::Value* is_null);

  // Sets the 'val' field of the *Val. Do not call if this represents a StringVal or
  // TimestampVal.
  void SetVal(llvm::Value* val);

  // Setters for StringVals.
  void SetPtr(llvm::Value* ptr);
  void SetLen(llvm::Value* len);

  // Setters for TimestampVals.
  void SetDate(llvm::Value* date);
  void SetTimeOfDay(llvm::Value* time_of_day);

  // Set this *Val's value based on void* 'raw_ptr'. 'raw_ptr' should be a pointer to a
  // native type, StringValue, or TimestampValue (i.e. the value returned by an
  // interpreted compute fn).
  void SetFromRawPtr(llvm::Value* raw_ptr);

 private:
  PrimitiveType type_;
  llvm::Value* value_;
  const char* name_;

  LlvmCodeGen* codegen_;
  LlvmCodeGen::LlvmBuilder* builder_;

  // Helper function for setting the top (most significant) half of a 'dst' to
  // 'src'.
  // 'src' must have width <= 'num_bits' and 'dst' must have width = 'num_bits' * 2.
  llvm::Value* SetHighBits(int num_bits, llvm::Value* src, llvm::Value* dst,
                           const char* name = "");
};

// Creates the corresponding AnyVal subclass for type. The object is added to the pool.
impala_udf::AnyVal* CreateAnyVal(ObjectPool* pool, PrimitiveType type);

}

#endif
