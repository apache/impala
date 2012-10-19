// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_FLOAT_LITERAL_H_
#define IMPALA_EXPRS_FLOAT_LITERAL_H_

#include <string>
#include "exprs/expr.h"

namespace impala {

class TExprNode;

class FloatLiteral: public Expr {
 public:
  virtual llvm::Function* Codegen(LlvmCodeGen* code_gen);

 protected:
  friend class Expr;

  // Construct a FloatLiteral expr. type/value must be TYPE_FLOAT/float* or 
  // TYPE_DOUBLE/double*.  
  FloatLiteral(PrimitiveType type, void* value);
  FloatLiteral(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

 private:
  static void* ReturnFloatValue(Expr* e, TupleRow* row);
  static void* ReturnDoubleValue(Expr* e, TupleRow* row);
};

}

#endif
