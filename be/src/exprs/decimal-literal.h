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


#ifndef IMPALA_EXPRS_DECIMAL_LITERAL_H
#define IMPALA_EXPRS_DECIMAL_LITERAL_H

#include <string>
#include "exprs/expr.h"

namespace impala {

class TExprNode;

class DecimalLiteral: public Expr {
 protected:
  friend class Expr;
  DecimalLiteral(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

 private:
  static void* ReturnDecimal4Value(Expr* e, TupleRow* row);
  static void* ReturnDecimal8Value(Expr* e, TupleRow* row);
  static void* ReturnDecimal16Value(Expr* e, TupleRow* row);
};

}

#endif
