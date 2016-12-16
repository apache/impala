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


#ifndef IMPALA_EXPRS_TUPLE_IS_NULL_PREDICATE_H_
#define IMPALA_EXPRS_TUPLE_IS_NULL_PREDICATE_H_

#include "exprs/predicate.h"

namespace impala {

class TExprNode;

/// Returns true if all of the given tuples are NULL, false otherwise.
/// It is important that this predicate not require the given tuples to be nullable,
/// because the FE may sometimes wrap expressions in this predicate that contain SlotRefs
/// on non-nullable tuples (see IMPALA-904).
/// TODO: Implement codegen to eliminate overhead on non-nullable tuples.
class TupleIsNullPredicate: public Predicate {
 protected:
  friend class Expr;

  TupleIsNullPredicate(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                         ExprContext* ctx);
  virtual Status GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn);
  virtual std::string DebugString() const;

  virtual BooleanVal GetBooleanVal(ExprContext* context, const TupleRow* row);

 private:
  /// Tuple ids to check for NULL. May contain ids of nullable and non-nullable tuples.
  std::vector<TupleId> tuple_ids_;

  /// Tuple indexes into the RowDescriptor. Only contains indexes of nullable tuples.
  std::vector<int32_t> tuple_idxs_;
};

}

#endif
