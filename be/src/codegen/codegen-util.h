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

#ifndef IMPALA_CODEGEN_CODEGEN_UTIL_H
#define IMPALA_CODEGEN_CODEGEN_UTIL_H

#include <vector>
#include <llvm/IR/IRBuilder.h>

#include "codegen/llvm-codegen.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"

namespace impala {

/// Miscellaneous codegen utility functions that don't depend on the rest of the Impala
/// codegen infrastructure.
class CodeGenUtil {
 public:
  /// Wrapper around IRBuilder::CreateCall() that automatically bitcasts arguments
  /// using CheckedBitCast(). This should be used instead of IRBuilder::CreateCall()
  /// when calling functions from a linked module because the LLVM linker may merge
  /// different struct types with the same memory layout during linking. E.g. if the
  /// IR UDF module has type TinyIntVal that has the same memory layout as BooleanVal:
  /// {i8, i8}, then the linker may substitute references to TinyIntVal with BooleanVal
  /// in the IR UDF. Calling a function which has a BooleanVal* argument with a TinyInt*
  /// argument without bitcasting then would result in hitting an internal LLVM assertion.
  static llvm::CallInst* CreateCallWithBitCasts(LlvmBuilder* builder,
      llvm::Function* callee, llvm::ArrayRef<llvm::Value*> args,
      const llvm::Twine& name="");

  /// Same as IRBuilder::CreateBitCast() except that it checks that the types are either
  /// exactly the same, or, if they are both struct types (or pointers to struct types),
  /// that they are structurally identical. Either returns 'value' if no conversion is
  /// necessary, or returns a bitcast instruction converting 'value' to 'dst_type'.
  static llvm::Value* CheckedBitCast(LlvmBuilder* builder,
      llvm::Value* value, llvm::Type* dst_type, const llvm::Twine& name);

  /// Return true if the two types are structurally identical.
  static bool TypesAreStructurallyIdentical(llvm::Type *t1, llvm::Type *t2);

  /// Returns the string representation of a llvm::Value* or llvm::Type*
  template <typename T> static std::string Print(T* value_or_type) {
    std::string str;
    llvm::raw_string_ostream stream(str);
    value_or_type->print(stream);
    return str;
  }

  /// Return the byte size of the given PrimitiveType.
  static int64_t GetTypeSize(const PrimitiveType type) {
    switch (type) {
      case TYPE_BOOLEAN:
      case TYPE_TINYINT:
        return 1;
      case TYPE_SMALLINT:
        return 2;
      case TYPE_INT:
      case TYPE_FLOAT:
      case TYPE_DATE:
        return 4;
      case TYPE_BIGINT:
      case TYPE_DOUBLE:
        return 8;
      case TYPE_TIMESTAMP:
        return sizeof(TimestampValue);
      case TYPE_STRING:
      case TYPE_VARCHAR:
        return sizeof(StringValue);
      default:
        DCHECK(false) << "NYI";
        return 0;
    }
  }

};
}

#endif
