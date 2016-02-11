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

#include "exprs/is-not-empty-predicate.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

#include "common/names.h"

namespace impala {

IsNotEmptyPredicate::IsNotEmptyPredicate(const TExprNode& node)
  : Predicate(node) {
}

BooleanVal IsNotEmptyPredicate::GetBooleanVal(ExprContext* ctx, TupleRow* row) {
  CollectionVal coll = children_[0]->GetCollectionVal(ctx, row);
  if (coll.is_null) return BooleanVal::null();
  return BooleanVal(coll.num_tuples != 0);
}

Status IsNotEmptyPredicate::Prepare(RuntimeState* state,
    const RowDescriptor& row_desc, ExprContext* ctx) {
  RETURN_IF_ERROR(Expr::Prepare(state, row_desc, ctx));
  DCHECK_EQ(children_.size(), 1);
  return Status::OK();
}

Status IsNotEmptyPredicate::GetCodegendComputeFn(RuntimeState* state,
    llvm::Function** fn) {
  return GetCodegendComputeFnWrapper(state, fn);
}

string IsNotEmptyPredicate::DebugString() const {
  return Expr::DebugString("IsNotEmptyPredicate");
}

}
