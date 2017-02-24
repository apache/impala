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


#ifndef IMPALA_EXPRS_CASE_EXPR_H_
#define IMPALA_EXPRS_CASE_EXPR_H_

#include <string>
#include "expr.h"

using namespace impala_udf;

namespace impala {

class TExprNode;

class CaseExpr: public Expr {
 public:
  virtual Status GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn);

  virtual BooleanVal GetBooleanVal(ExprContext* ctx, const TupleRow* row);
  virtual TinyIntVal GetTinyIntVal(ExprContext* ctx, const TupleRow* row);
  virtual SmallIntVal GetSmallIntVal(ExprContext* ctx, const TupleRow* row);
  virtual IntVal GetIntVal(ExprContext* ctx, const TupleRow* row);
  virtual BigIntVal GetBigIntVal(ExprContext* ctx, const TupleRow* row);
  virtual FloatVal GetFloatVal(ExprContext* ctx, const TupleRow* row);
  virtual DoubleVal GetDoubleVal(ExprContext* ctx, const TupleRow* row);
  virtual StringVal GetStringVal(ExprContext* ctx, const TupleRow* row);
  virtual TimestampVal GetTimestampVal(ExprContext* ctx, const TupleRow* row);
  virtual DecimalVal GetDecimalVal(ExprContext* ctx, const TupleRow* row);

 protected:
  friend class Expr;
  friend class ConditionalFunctions;
  friend class DecimalOperators;

  CaseExpr(const TExprNode& node);
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc,
      ExprContext* context);
  virtual Status Open(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);
  virtual void Close(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);

  virtual std::string DebugString() const;

  bool has_case_expr() { return has_case_expr_; }
  bool has_else_expr() { return has_else_expr_; }

 private:
  const bool has_case_expr_;
  const bool has_else_expr_;

  /// Populates 'dst' with the result of calling the appropriate Get*Val() function on the
  /// specified child expr.
  void GetChildVal(int child_idx, ExprContext* ctx, const TupleRow* row, AnyVal* dst);

  /// Return true iff *v1 == *v2. v1 and v2 should both be of the specified type.
  bool AnyValEq(const ColumnType& type, const AnyVal* v1, const AnyVal* v2);
};

}

#endif
