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

#include "codegen/codegen-util.h"

#include <cctype>

#include "common/names.h"

using std::isdigit;

namespace impala {

llvm::CallInst* CodeGenUtil::CreateCallWithBitCasts(LlvmBuilder* builder,
    llvm::Function* callee, llvm::ArrayRef<llvm::Value*> args, const llvm::Twine& name) {
  vector<llvm::Value*> bitcast_args;
  bitcast_args.reserve(args.size());
  llvm::Function::arg_iterator fn_arg = callee->arg_begin();
  for (llvm::Value* arg : args) {
    bitcast_args.push_back(
        CheckedBitCast(builder, arg, fn_arg->getType(), "create_call_bitcast"));
    ++fn_arg;
  }
  return builder->CreateCall(callee, bitcast_args, name);
}

llvm::Value* CodeGenUtil::CheckedBitCast(LlvmBuilder* builder, llvm::Value* value,
    llvm::Type* dst_type, const llvm::Twine& name) {
  DCHECK(TypesAreStructurallyIdentical(value->getType(), dst_type))
      << Print(value->getType()) << " " << Print(dst_type);
  return builder->CreateBitCast(value, dst_type, name);
}

bool CodeGenUtil::TypesAreStructurallyIdentical(llvm::Type* t1, llvm::Type* t2) {
  // All primitive types are deduplicated by LLVM, so we can just compare the pointers.
  if (t1 == t2) return true;
  // Derived types are structurally identical if they are the same kind of compound type
  // and the elements are structurally identical. Check to see which of the Type
  // subclasses t1 belongs to.
  if (t1->isPointerTy()) {
    if (!t2->isPointerTy()) return false;
  } else if (t1->isStructTy()) {
    if (!t2->isStructTy()) return false;
  } else if (t1->isArrayTy()) {
    if (!t2->isArrayTy()) return false;
  } else if (t1->isFunctionTy()) {
    if (!t2->isFunctionTy()) return false;
  } else {
    DCHECK(t1->isVectorTy()) << Print(t1);
    if (!t2->isVectorTy()) return false;
  }
  if (t1->getNumContainedTypes() != t2->getNumContainedTypes()) return false;
  for (int i = 0; i < t1->getNumContainedTypes(); ++i) {
    if (!TypesAreStructurallyIdentical(
          t1->getContainedType(i), t2->getContainedType(i))) {
      return false;
    }
  }
  return true;
}
}
