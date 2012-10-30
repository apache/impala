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


#ifndef IMPALA_EXPRS_COMPOUND_PREDICATE_H_
#define IMPALA_EXPRS_COMPOUND_PREDICATE_H_

#include <string>
#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class CompoundPredicate: public Predicate {
 public:
  virtual llvm::Function* Codegen(LlvmCodeGen* codegen);

 protected:
  friend class Expr;

  CompoundPredicate(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& desc);
  virtual std::string DebugString() const;

 private:
  friend class OpcodeRegistry;

  llvm::Function* CodegenNot(LlvmCodeGen* codegen);
  llvm::Function* CodegenBinary(LlvmCodeGen* codegen);

  static void* AndComputeFn(Expr* e, TupleRow* row);
  static void* OrComputeFn(Expr* e, TupleRow* row);
  static void* NotComputeFn(Expr* e, TupleRow* row);
};

}

#endif
