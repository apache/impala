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

#ifndef IMPALA_CODEGEN_CODEGEN_ANYVAL_H
#define IMPALA_CODEGEN_CODEGEN_ANYVAL_H

#include "codegen/llvm-codegen.h"

namespace llvm {
class Type;
class Value;
}

namespace impala {

/// Class for handling AnyVal subclasses during codegen. Codegen functions should use this
/// wrapper instead of creating or manipulating *Val values directly in most cases. This is
/// because the struct types must be lowered to integer types in many cases in order to
/// conform to the standard calling convention (e.g., { i8, i32 } => i64). This class wraps
/// the lowered types for each *Val struct.
//
/// This class conceptually represents a single *Val that is mutated, but operates by
/// generating IR instructions involving value_ (each of which generates a new Value*,
/// since IR uses SSA), and then setting value_ to the most recent Value* generated. The
/// generated instructions perform the integer manipulation equivalent to setting the
/// fields of the original struct type.
//
/// Lowered types:
/// TYPE_BOOLEAN/BooleanVal: i16
/// TYPE_TINYINT/TinyIntVal: i16
/// TYPE_SMALLINT/SmallIntVal: i32
/// TYPE_INT/INTVal: i64
/// TYPE_BIGINT/BigIntVal: { i8, i64 }
/// TYPE_FLOAT/FloatVal: i64
/// TYPE_DOUBLE/DoubleVal: { i8, double }
/// TYPE_STRING/StringVal: { i64, i8* }
/// TYPE_TIMESTAMP/TimestampVal: { i64, i64 }
//
/// TODO:
/// - unit tests
class CodegenAnyVal {
 public:
  static const char* LLVM_BOOLEANVAL_NAME;
  static const char* LLVM_TINYINTVAL_NAME;
  static const char* LLVM_SMALLINTVAL_NAME;
  static const char* LLVM_INTVAL_NAME;
  static const char* LLVM_BIGINTVAL_NAME;
  static const char* LLVM_FLOATVAL_NAME;
  static const char* LLVM_DOUBLEVAL_NAME;
  static const char* LLVM_STRINGVAL_NAME;
  static const char* LLVM_TIMESTAMPVAL_NAME;
  static const char* LLVM_DECIMALVAL_NAME;

  /// Creates a call to 'fn', which should return a (lowered) *Val, and returns the result.
  /// This abstracts over the x64 calling convention, in particular for functions returning
  /// a DecimalVal, which pass the return value as an output argument.
  //
  /// If 'result_ptr' is non-NULL, it should be a pointer to the lowered return type of
  /// 'fn' (e.g. if 'fn' returns a BooleanVal, 'result_ptr' should have type i16*). The
  /// result of calling 'fn' will be stored in 'result_ptr' and this function will return
  /// NULL. If 'result_ptr' is NULL, this function will return the result (note that the
  /// result will not be a pointer in this case).
  //
  /// 'name' optionally specifies the name of the returned value.
  static llvm::Value* CreateCall(LlvmCodeGen* cg, LlvmCodeGen::LlvmBuilder* builder,
      llvm::Function* fn, llvm::ArrayRef<llvm::Value*> args, const char* name = "",
      llvm::Value* result_ptr = NULL);

  /// Same as above but wraps the result in a CodegenAnyVal.
  static CodegenAnyVal CreateCallWrapped(LlvmCodeGen* cg,
      LlvmCodeGen::LlvmBuilder* builder, const ColumnType& type, llvm::Function* fn,
      llvm::ArrayRef<llvm::Value*> args, const char* name = "",
      llvm::Value* result_ptr = NULL);

  /// Returns the lowered AnyVal type associated with 'type'.
  /// E.g.: TYPE_BOOLEAN (which corresponds to a BooleanVal) => i16
  static llvm::Type* GetLoweredType(LlvmCodeGen* cg, const ColumnType& type);

  /// Returns the lowered AnyVal pointer type associated with 'type'.
  /// E.g.: TYPE_BOOLEAN => i16*
  static llvm::Type* GetLoweredPtrType(LlvmCodeGen* cg, const ColumnType& type);

  /// Returns the unlowered AnyVal type associated with 'type'.
  /// E.g.: TYPE_BOOLEAN => %"struct.impala_udf::BooleanVal"
  static llvm::Type* GetUnloweredType(LlvmCodeGen* cg, const ColumnType& type);

  /// Returns the unlowered AnyVal pointer type associated with 'type'.
  /// E.g.: TYPE_BOOLEAN => %"struct.impala_udf::BooleanVal"*
  static llvm::Type* GetUnloweredPtrType(LlvmCodeGen* cg, const ColumnType& type);

  /// Return the constant type-lowered value corresponding to a null *Val.
  /// E.g.: passing TYPE_DOUBLE (corresponding to the lowered DoubleVal { i8, double })
  /// returns the constant struct { 1, 0.0 }
  static llvm::Value* GetNullVal(LlvmCodeGen* codegen, const ColumnType& type);

  /// Return the constant type-lowered value corresponding to a null *Val.
  /// 'val_type' must be a lowered type (i.e. one of the types returned by GetType)
  static llvm::Value* GetNullVal(LlvmCodeGen* codegen, llvm::Type* val_type);

  /// Return the constant type-lowered value corresponding to a non-null *Val.
  /// E.g.: TYPE_DOUBLE (lowered DoubleVal: { i8, double }) => { 0, 0 }
  /// This returns a CodegenAnyVal, rather than the unwrapped Value*, because the actual
  /// value still needs to be set.
  static CodegenAnyVal GetNonNullVal(LlvmCodeGen* codegen,
      LlvmCodeGen::LlvmBuilder* builder, const ColumnType& type, const char* name = "");

  /// Creates a wrapper around a lowered *Val value.
  //
  /// Instructions for manipulating the value are generated using 'builder'. The insert
  /// point of 'builder' is not modified by this class, and it is safe to call
  /// 'builder'.SetInsertPoint() after passing 'builder' to this class.
  //
  /// 'type' identified the type of wrapped value (e.g., TYPE_INT corresponds to IntVal,
  /// which is lowered to i64).
  //
  /// If 'value' is NULL, a new value of the lowered type is alloca'd. Otherwise 'value'
  /// must be of the correct lowered type.
  //
  /// If 'name' is specified, it will be used when generated instructions that set value_.
  CodegenAnyVal(LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder,
                const ColumnType& type, llvm::Value* value = NULL, const char* name = "");

  /// Returns the current type-lowered value.
  llvm::Value* value() { return value_; }

  /// Gets the 'is_null' field of the *Val.
  llvm::Value* GetIsNull(const char* name = "is_null");

  /// Get the 'val' field of the *Val. Do not call if this represents a StringVal or
  /// TimestampVal. If this represents a DecimalVal, returns 'val4', 'val8', or 'val16'
  /// depending on the precision of 'type_'.  The returned value will have variable name
  /// 'name'.
  llvm::Value* GetVal(const char* name = "val");

  /// Sets the 'is_null' field of the *Val.
  void SetIsNull(llvm::Value* is_null);

  /// Sets the 'val' field of the *Val. Do not call if this represents a StringVal or
  /// TimestampVal.
  void SetVal(llvm::Value* val);

  /// Sets the 'val' field of the *Val. The *Val must correspond to the argument type.
  void SetVal(bool val);
  void SetVal(int8_t val);
  void SetVal(int16_t val);
  void SetVal(int32_t val);
  void SetVal(int64_t val);
  void SetVal(int128_t val);
  void SetVal(float val);
  void SetVal(double val);

  /// Getters for StringVals.
  llvm::Value* GetPtr();
  llvm::Value *GetLen();

  /// Setters for StringVals.
  void SetPtr(llvm::Value* ptr);
  void SetLen(llvm::Value* len);

  /// Getters for TimestampVals.
  llvm::Value* GetDate();
  llvm::Value* GetTimeOfDay();

  /// Setters for TimestampVals.
  void SetDate(llvm::Value* date);
  void SetTimeOfDay(llvm::Value* time_of_day);

  /// Allocas and stores this value in an unlowered pointer, and returns the pointer. This
  /// *Val should be non-null.
  llvm::Value* GetUnloweredPtr();

  /// Set this *Val's value based on 'raw_val'. 'raw_val' should be a native type,
  /// StringValue, or TimestampValue.
  void SetFromRawValue(llvm::Value* raw_val);

  /// Set this *Val's value based on void* 'raw_ptr'. 'raw_ptr' should be a pointer to a
  /// native type, StringValue, or TimestampValue (i.e. the value returned by an
  /// interpreted compute fn).
  void SetFromRawPtr(llvm::Value* raw_ptr);

  /// Converts this *Val's value to a native type, StringValue, TimestampValue, etc.
  /// This should only be used if this *Val is not null.
  llvm::Value* ToNativeValue();

  /// Sets 'native_ptr' to this *Val's value. If non-NULL, 'native_ptr' should be a
  /// pointer to a native type, StringValue, TimestampValue, etc. If NULL, a pointer is
  /// alloca'd. In either case the pointer is returned. This should only be used if this
  /// *Val is not null.
  llvm::Value* ToNativePtr(llvm::Value* native_ptr = NULL);

  /// Returns the i1 result of this == other. this and other must be non-null.
  llvm::Value* Eq(CodegenAnyVal* other);

  /// Compares this *Val to the value of 'native_ptr'. 'native_ptr' should be a pointer to
  /// a native type, StringValue, or TimestampValue. This *Val should match 'native_ptr's
  /// type (e.g. if this is an IntVal, 'native_ptr' should have type i32*). Returns the i1
  /// result of the equality comparison.
  llvm::Value* EqToNativePtr(llvm::Value* native_ptr);

  /// Returns the i32 result of comparing this value to 'other' (similar to
  /// RawValue::Compare()). This and 'other' must be non-null. Return value is < 0 if
  /// this < 'other', 0 if this == 'other', > 0 if this > 'other'.
  llvm::Value* Compare(CodegenAnyVal* other, const char* name = "result");

  /// Ctor for created an uninitialized CodegenAnYVal that can be assigned to later.
  CodegenAnyVal()
    : type_(INVALID_TYPE), value_(NULL), name_(NULL), codegen_(NULL), builder_(NULL) { }

 private:
  ColumnType type_;
  llvm::Value* value_;
  const char* name_;

  LlvmCodeGen* codegen_;
  LlvmCodeGen::LlvmBuilder* builder_;

  /// Helper function for getting the top (most significant) half of 'v'.
  /// 'v' should have width = 'num_bits' * 2 and be an integer type.
  llvm::Value* GetHighBits(int num_bits, llvm::Value* v, const char* name = "");

  /// Helper function for setting the top (most significant) half of a 'dst' to 'src'.
  /// 'src' must have width <= 'num_bits' and 'dst' must have width = 'num_bits' * 2.
  /// Both 'dst' and 'src' should be integer types.
  llvm::Value* SetHighBits(int num_bits, llvm::Value* src, llvm::Value* dst,
                           const char* name = "");
};

}

#endif
