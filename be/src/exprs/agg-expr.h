// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_AGG_EXPR_H_
#define IMPALA_EXPRS_AGG_EXPR_H_

#include <string>
#include "expr.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class TExprNode;

class AggregateExpr: public Expr {
 public:
  // Returns the IR function for getting the agg expr input value
  // Returns null if is_star_ is true.
  virtual llvm::Function* Codegen(LlvmCodeGen* code_gen);
  TAggregationOp::type agg_op() const { return agg_op_; }
  bool is_star() const { return is_star_; }
  bool is_distinct() const { return is_distinct_; }
  virtual std::string DebugString() const;

 protected:
  friend class Expr;

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& desc);
  AggregateExpr(const TExprNode& node);

 private:
  const TAggregationOp::type agg_op_;
  const bool is_star_;
  const bool is_distinct_;
};

}

#endif
