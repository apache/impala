// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_BINARY_PREDICATE_H_
#define IMPALA_EXPRS_BINARY_PREDICATE_H_

#include <string>
#include <iostream>
#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class BinaryPredicate : public Predicate {
 public:
  virtual llvm::Function* Codegen(LlvmCodeGen* code_gen);
 
 protected:
  friend class Expr;

  BinaryPredicate(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& desc);
  virtual std::string DebugString() const;
};

}

#endif
