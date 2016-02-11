// Copyright 2016 Cloudera Inc.
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

#ifndef IMPALA_EXPRS_IS_NOT_EMPTY_PREDICATE_H_
#define IMPALA_EXPRS_IS_NOT_EMPTY_PREDICATE_H_

#include "exprs/predicate.h"

namespace impala {

class TExprNode;

/// Predicate that checks whether a collection is empty or not.
/// TODO: Implement this predicate via the UDF interface once the
/// interface supports CollectionVals.
class IsNotEmptyPredicate: public Predicate {
 public:
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                         ExprContext* ctx);
  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);
  virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow* row);
  virtual std::string DebugString() const;

 protected:
  friend class Expr;

  IsNotEmptyPredicate(const TExprNode& node);
};

}

#endif
