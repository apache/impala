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

#ifndef IMPALA_EXEC_SORT_EXEC_EXPRS_H
#define IMPALA_EXEC_SORT_EXEC_EXPRS_H

#include "exprs/expr.h"
#include "runtime/runtime-state.h"

namespace impala {

// Helper class to Prepare() , Open() and Close() the ordering expressions used
// to perform comparisons in a sort. Used by TopNNode, SortNode, and MergingExchangeNode.
// When two rows are compared, the ordering expressions are evaluated once for each
// side. Since the result of the evaluation is stored in the Expr itself, two copies
// of the ordering expressions are maintained so these can be compared without copying.
// TopN and Sort materialize input rows into a single tuple before sorting.
// If materialize_tuple_ is true, SortExecExprs also stores the slot expressions used to
// materialize the sort tuples.
class SortExecExprs {
 public:
  // Initialize the expressions from a thrift node using the specified pool.
  Status Init(const TSortInfo& sort_info, ObjectPool* pool);

  // Prepare all expressions used for sorting and tuple materialization.
  Status Prepare(RuntimeState* state, const RowDescriptor& child_row_desc,
    const RowDescriptor& output_row_desc);

  // Open all expressions used for sorting and tuple materialization.
  Status Open(RuntimeState* state);

  // Close all expressions used for sorting and tuple materialization.
  void Close(RuntimeState* state);

  const std::vector<Expr*>& sort_tuple_slot_exprs() const {
    return sort_tuple_slot_exprs_;
  }
  const std::vector<Expr*>& lhs_ordering_exprs() const {
    return lhs_ordering_exprs_;
  }
  const std::vector<Expr*>& rhs_ordering_exprs() const {
    return rhs_ordering_exprs_;
  }

 private:
  // Two copies of the ordering expressions. (See class comment)
  std::vector<Expr*> lhs_ordering_exprs_;
  std::vector<Expr*> rhs_ordering_exprs_;

  // If true, the tuples to be sorted are materialized by
  // sort_tuple_slot_exprs_ before the actual sort is performed.
  bool materialize_tuple_;

  // Expressions used to materialize slots in the tuples to be sorted.
  // One expr per slot in the materialized tuple. Valid only if
  // materialize_tuple_ is true.
  std::vector<Expr*> sort_tuple_slot_exprs_;
};

}

#endif
