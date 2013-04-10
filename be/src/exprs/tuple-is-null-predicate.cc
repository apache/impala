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

#include "exprs/tuple-is-null-predicate.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

void* TupleIsNullPredicate::ComputeFn(Expr* e, TupleRow* row) {
  TupleIsNullPredicate* p = static_cast<TupleIsNullPredicate*>(e);
  // Return true if all of the tuples in tuple_idxs_ are NULL.
  p->result_.bool_val = true;
  for (int i = 0; i < p->tuple_idxs_.size(); ++i) {
    if (row->GetTuple(p->tuple_idxs_[i]) != NULL) {
      p->result_.bool_val = false;
      break;
    }
  }
  return &p->result_.bool_val;
}

TupleIsNullPredicate::TupleIsNullPredicate(const TExprNode& node)
  : Predicate(node),
    tuple_ids_(node.tuple_is_null_pred.tuple_ids.begin(),
               node.tuple_is_null_pred.tuple_ids.end()) {
}

Status TupleIsNullPredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, row_desc));
  DCHECK_EQ(0, children_.size());
  // Resolve tuple ids to tuple indexes.
  for (int i = 0; i < tuple_ids_.size(); ++i) {
    int32_t tuple_idx = row_desc.GetTupleIdx(tuple_ids_[i]);
    DCHECK(row_desc.TupleIsNullable(tuple_idx));
    tuple_idxs_.push_back(tuple_idx);
  }
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string TupleIsNullPredicate::DebugString() const {
  stringstream out;
  out << "TupleIsNullPredicate(tupleids=[";
  for (int i = 0; i < tuple_ids_.size(); ++i) {
    out << (i == 0 ? "" : " ") << tuple_ids_[i];
  }
  out << "])";
  return out.str();
}

}
