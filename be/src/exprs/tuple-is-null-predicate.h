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


#ifndef IMPALA_EXPRS_TUPLE_IS_NULL_PREDICATE_H_
#define IMPALA_EXPRS_TUPLE_IS_NULL_PREDICATE_H_

#include "exprs/predicate.h"

namespace impala {

class TExprNode;

class TupleIsNullPredicate: public Predicate {
 protected:
  friend class Expr;

  TupleIsNullPredicate(const TExprNode& node);
  
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

 private:
  static void* ComputeFn(Expr* e, TupleRow* row);
  std::vector<TupleId> tuple_ids_;
  std::vector<int32_t> tuple_idxs_;
};

}

#endif
