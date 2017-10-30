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

#ifndef IMPALA_CODEGEN_CODEGEN_ANYVAL_H
#define IMPALA_CODEGEN_CODEGEN_ANYVAL_H

#include "codegen/llvm-codegen.h"
#include "runtime/descriptors.h"
#include "runtime/multi-precision.h"

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
/// TYPE_STRING,TYPE_VARCHAR,TYPE_CHAR,TYPE_FIXED_UDA_INTERMEDIATE/StringVal: { i64, i8* }
/// TYPE_TIMESTAMP/TimestampVal: { i64, i64 }
/// TYPE_DECIMAL/DecimalVal (isn't lowered):
/// %"struct.impala_udf::DecimalVal" { {i8}, [15 x i8], {i128} }
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
  static llvm::Value* CreateCall(LlvmCodeGen* cg, LlvmBuilder* builder,
      llvm::Function* fn, llvm::ArrayRef<llvm::Value*> args, const char* name = "",
      llvm::Value* result_ptr = nullptr);

  /// Same as above but wraps the result in a CodegenAnyVal.
  static CodegenAnyVal CreateCallWrapped(LlvmCodeGen* cg, LlvmBuilder* builder,
      const ColumnType& type, llvm::Function* fn, llvm::ArrayRef<llvm::Value*> args,
      const char* name = "");

  /// Returns the lowered AnyVal type associated with 'type'.
  /// E.g.: TYPE_BOOLEAN (which corresponds to a BooleanVal) => i16
  static llvm::Type* GetLoweredType(LlvmCodeGen* cg, const ColumnType& type);

  /// Returns the lowered AnyVal pointer type associated with 'type'.
  /// E.g.: TYPE_BOOLEAN => i16*
  static llvm::PointerType* GetLoweredPtrType(LlvmCodeGen* cg, const ColumnType& type);

  /// Returns the unlowered AnyVal type associated with 'type'.
  /// E.g.: TYPE_BOOLEAN => %"struct.impala_udf::BooleanVal"
  static llvm::Type* GetUnloweredType(LlvmCodeGen* cg, const ColumnType& type);

  /// Returns the unlowered AnyVal pointer type associated with 'type'.
  /// E.g.: TYPE_BOOLEAN => %"struct.impala_udf::BooleanVal"*
  static llvm::PointerType* GetUnloweredPtrType(LlvmCodeGen* cg, const ColumnType& type);

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
  static CodegenAnyVal GetNonNullVal(LlvmCodeGen* codegen, LlvmBuilder* builder,
      const ColumnType& type, const char* name = "");

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
  CodegenAnyVal(LlvmCodeGen* codegen, LlvmBuilder* builder, const ColumnType& type,
      llvm::Value* value = nullptr, const char* name = "");

  /// Returns the current type-lowered value.
  llvm::Value* GetLoweredValue() const { return value_; }

  /// Gets the 'is_null' field of the *Val.
  llvm::Value* GetIsNull(const char* name = "is_null") const;

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

  /// Stores this value in an alloca allocation, and returns the pointer, which has the
  /// lowered type. This *Val should be non-null. The output variable is called 'name'.
  llvm::Value* GetLoweredPtr(const std::string& name = "") const;

  /// Stores this value in an alloca allocation, and returns the pointer, which has the
  /// unlowered type. This *Val should be non-null. The output variable is called 'name'.
  llvm::Value* GetUnloweredPtr(const std::string& name = "") const;

  /// Load this *Val's value from 'raw_val_ptr', which must be a pointer to the matching
  /// native type, e.g. a StringValue or TimestampValue slot in a tuple.
  void LoadFromNativePtr(llvm::Value* raw_val_ptr);

  /// Stores this *Val's value into a native slot, e.g. a StringValue or TimestampValue.
  /// This should only be used if this *Val is not null.
  ///
  /// Not valid to call for FIXED_UDA_INTERMEDIATE: in that case the StringVal must be
  /// set up to point directly to the underlying slot, e.g. by LoadFromNativePtr().
  ///
  /// If 'pool_val' is non-NULL, var-len data will be copied into 'pool_val'.
  /// 'pool_val' has to be of type MemPool*.
  void StoreToNativePtr(llvm::Value* raw_val_ptr, llvm::Value* pool_val = nullptr);

  /// Creates a pointer, e.g. StringValue* to an alloca() allocation with the
  /// equivalent of this value. This should only be used if this Val is not null.
  ///
  /// If 'pool_val' is non-NULL, var-len data will be copied into 'pool_val'.
  /// 'pool_val' has to be of type MemPool*.
  llvm::Value* ToNativePtr(llvm::Value* pool_val = nullptr);

  /// Writes this *Val's value to the appropriate slot in 'tuple' if non-null, or sets the
  /// appropriate null bit if null. This assumes null bits are initialized to 0. Analogous
  /// to RawValue::Write(void* value, Tuple*, SlotDescriptor*, MemPool*). 'tuple' should
  /// be a pointer to the generated LLVM struct type, not an opaque Tuple*.
  ///
  /// Creates new basic blocks in order to branch on the 'is_null' fields, and leaves
  /// builder_'s insert point at the block after these new blocks. This block will be
  /// 'insert_before' if specified, or a new basic block created at the end of the
  /// function if 'insert_before' is NULL.
  ///
  /// If 'pool_val' is non-NULL, var-len data will be copied into 'pool_val'.
  /// 'pool_val' has to be of type MemPool*.
  void WriteToSlot(const SlotDescriptor& slot_desc, llvm::Value* tuple,
      llvm::Value* pool_val, llvm::BasicBlock* insert_before = nullptr);

  /// Returns the i1 result of this == other. this and other must be non-null.
  llvm::Value* Eq(CodegenAnyVal* other);

  /// Compares this *Val to the value of 'native_ptr'. 'native_ptr' should be a pointer to
  /// a native type, e.g. StringValue, or TimestampValue. This *Val should match
  /// 'native_ptr's type (e.g. if this is an IntVal, 'native_ptr' should have type i32*).
  /// Returns the i1 result of the equality comparison.
  llvm::Value* EqToNativePtr(llvm::Value* native_ptr);

  /// Returns the i32 result of comparing this value to 'other' (similar to
  /// RawValue::Compare()). This and 'other' must be non-null. Return value is < 0 if
  /// this < 'other', 0 if this == 'other', > 0 if this > 'other'.
  llvm::Value* Compare(CodegenAnyVal* other, const char* name = "result");

  /// Generate code to branch to 'null_block' if this value is NULL. The branch terminates
  /// the current BasicBlock, so a new BasicBlock for the non-NULL case is also created,
  /// and builder's insert position is set to the start of the non-NULL block.
  ///
  /// This corresponds to the C++ code:
  /// if (val.is_null) goto null_block;
  ///
  /// non_null_block:
  ///   <-- Builder insert position after this function returns.
  /// ...
  /// null_block:
  /// ...
  void CodegenBranchIfNull(LlvmBuilder* builder, llvm::BasicBlock* null_block);

  /// Ctor for created an uninitialized CodegenAnYVal that can be assigned to later.
  CodegenAnyVal()
    : type_(INVALID_TYPE), value_(nullptr), name_(nullptr),
      codegen_(nullptr), builder_(nullptr) {}

  LlvmCodeGen* codegen() const { return codegen_; }

 private:
  ColumnType type_;
  llvm::Value* value_;
  const char* name_;

  LlvmCodeGen* codegen_;
  LlvmBuilder* builder_;

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
