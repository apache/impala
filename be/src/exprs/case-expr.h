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


#ifndef IMPALA_EXPRS_CASE_EXPR_H_
#define IMPALA_EXPRS_CASE_EXPR_H_

#include <string>
#include "expr.h"

namespace impala {

class TExprNode;

class CaseExpr: public Expr {
 protected:
  friend class Expr;
  friend class ComputeFunctions;
  friend class ConditionalFunctions;
  friend class DecimalOperators;

  CaseExpr(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

  bool has_case_expr() { return has_case_expr_; }
  bool has_else_expr() { return has_else_expr_; }

 private:
  const bool has_case_expr_;
  const bool has_else_expr_;
};

}

#endif
