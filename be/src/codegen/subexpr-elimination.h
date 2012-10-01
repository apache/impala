// Copyright (c) 2012 Cloudera, Inc.  All right reserved.

#ifndef IMPALA_CODEGEN_SUBEXPR_ELIMINATION_H
#define IMPALA_CODEGEN_SUBEXPR_ELIMINATION_H

#include "common/status.h"
#include "codegen/llvm-codegen.h"

namespace impala {

// Optimization pass to remove redundant exprs.
// TODO: make this into a llvm function pass (i.e. implement FunctionPass interface).
class SubExprElimination {
 public:
  SubExprElimination(LlvmCodeGen* codegen);

  // Perform subexpr elimination on function.
  bool Run(llvm::Function* function);

 private:
  // Parent codegen object.
  LlvmCodeGen* codegen_;
};

}

#endif

