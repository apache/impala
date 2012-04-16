// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_IS_NULL_PREDICATE_H_
#define IMPALA_EXPRS_IS_NULL_PREDICATE_H_

#include <string>
#include "exprs/predicate.h"

namespace impala {

class TExprNode;

class IsNullPredicate: public Predicate {
 public:
  virtual llvm::Function* Codegen(LlvmCodeGen* code_gen);

 protected:
  friend class Expr;

  IsNullPredicate(const TExprNode& node);
  
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

 private:
  const bool is_not_null_;
  static void* ComputeFn(Expr* e, TupleRow* row);
};

}

#endif
