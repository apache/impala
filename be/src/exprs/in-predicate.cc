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

#include <sstream>

#include "exprs/in-predicate.h"
#include "runtime/raw-value.h"
#include "runtime/string-value.inline.h"

using namespace std;

namespace impala {

InPredicate::InPredicate(const TExprNode& node)
  : Predicate(node),
    is_not_in_(node.in_predicate.is_not_in) {
}

Status InPredicate::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_GE(children_.size(), 2);
  Expr::PrepareChildren(state, desc);
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string InPredicate::DebugString() const {
  stringstream out;
  out << "InPredicate(" << GetChild(0)->DebugString() << " " << is_not_in_ << ",[";
  int num_children = GetNumChildren();
  for (int i = 1; i < num_children; ++i) {
    out << (i == 1 ? "" : " ") << GetChild(i)->DebugString();
  }
  out << "])";
  return out.str();
}

void* InPredicate::ComputeFn(Expr* e, TupleRow* row) {
  void* cmp_val = e->children()[0]->GetValue(row);
  if (cmp_val == NULL) return NULL;
  PrimitiveType type = e->children()[0]->type();
  InPredicate* in_pred = static_cast<InPredicate*>(e);
  int32_t num_children = e->GetNumChildren();
  bool found_null = false;
  for (int32_t i = 1; i < num_children; ++i) {
    DCHECK_EQ(type, e->children()[i]->type());
    void* in_list_val = e->children()[i]->GetValue(row);
    if (in_list_val == NULL) {
      found_null = true;
      continue;
    }
    if (RawValue::Eq(cmp_val, in_list_val, type)) {
      e->result_.bool_val = !in_pred->is_not_in_;
      return &e->result_.bool_val;
    }
  }
  if (found_null) return NULL;
  e->result_.bool_val = in_pred->is_not_in_;
  return &e->result_.bool_val;
}

}
