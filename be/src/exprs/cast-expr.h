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


#ifndef IMPALA_EXPRS_CAST_EXPR_H_
#define IMPALA_EXPRS_CAST_EXPR_H_

#include <string>
#include "expr.h"

namespace impala {

class TExprNode;

class CastExpr: public Expr {
 public:
  virtual llvm::Function* Codegen(LlvmCodeGen* codegen);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& desc);
  virtual std::string DebugString() const;

  virtual bool IsJittable(LlvmCodeGen* codegen) const;

 protected:
  friend class Expr;
  CastExpr(const TExprNode& node);
};

}

#endif
