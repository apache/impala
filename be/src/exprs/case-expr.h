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
#include "scalar-expr.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;
using impala_udf::DateVal;

class ScalarExprEvaluator;
class TExprNode;

class CaseExpr: public ScalarExpr {
 public:
  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn)
      override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override;

 protected:
  friend class ScalarExpr;
  friend class ConditionalFunctions;

  virtual bool HasFnCtx() const override { return true; }

  CaseExpr(const TExprNode& node);
  virtual Status OpenEvaluator(FunctionContext::FunctionStateScope scope,
      RuntimeState* state, ScalarExprEvaluator* eval)
      const override WARN_UNUSED_RESULT;
  virtual void CloseEvaluator(FunctionContext::FunctionStateScope scope,
      RuntimeState* state, ScalarExprEvaluator* eval)
      const override;

  GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES

  bool has_case_expr() const { return has_case_expr_; }
  bool has_else_expr() const { return has_else_expr_; }

 private:
  const bool has_case_expr_;
  const bool has_else_expr_;

  /// Populates 'dst' with the result of calling the appropriate Get*Val() function on the
  /// specified child expr.
  void GetChildVal(int child_idx, ScalarExprEvaluator* eval,
      const TupleRow* row, AnyVal* dst) const;

  /// Return true iff *v1 == *v2. v1 and v2 should both be of the specified type.
  bool AnyValEq(const ColumnType& type, const AnyVal* v1, const AnyVal* v2) const;
};

}

#endif
