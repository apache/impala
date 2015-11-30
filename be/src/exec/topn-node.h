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


#ifndef IMPALA_EXEC_TOPN_NODE_H
#define IMPALA_EXEC_TOPN_NODE_H

#include <queue>
#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "exec/sort-exec-exprs.h"
#include "runtime/descriptors.h"  // for TupleId
#include "util/tuple-row-compare.h"

namespace impala {

class MemPool;
class RuntimeState;
class Tuple;

/// Node for in-memory TopN (ORDER BY ... LIMIT)
/// This handles the case where the result fits in memory.
/// This node will materialize its input rows into a new tuple using the expressions
/// in sort_tuple_slot_exprs_ in its sort_exec_exprs_ member.
/// TopN is implemented by storing rows in a priority queue.
class TopNNode : public ExecNode {
 public:
  TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:

  friend class TupleLessThan;

  /// Inserts a tuple row into the priority queue if it's in the TopN.  Creates a deep
  /// copy of tuple_row, which it stores in tuple_pool_.
  void InsertTupleRow(TupleRow* tuple_row);

  /// Flatten and reverse the priority queue.
  void PrepareForOutput();

  /// Number of rows to skip.
  int64_t offset_;

  /// sort_exec_exprs_ contains the ordering expressions used for tuple comparison and
  /// the materialization exprs for the output tuple.
  SortExecExprs sort_exec_exprs_;
  std::vector<bool> is_asc_order_;
  std::vector<bool> nulls_first_;

  /// Cached descriptor for the materialized tuple. Assigned in Prepare().
  TupleDescriptor* materialized_tuple_desc_;

  /// Comparator for priority_queue_.
  boost::scoped_ptr<TupleRowComparator> tuple_row_less_than_;

  /// After computing the TopN in the priority_queue, pop them and put them in this vector
  std::vector<Tuple*> sorted_top_n_;

  /// Tuple allocated once from tuple_pool_ and reused in InsertTupleRow to
  /// materialize input tuples if necessary. After materialization, tmp_tuple_ may be
  /// copied into the tuple pool and inserted into the priority queue.
  Tuple* tmp_tuple_;

  /// Stores everything referenced in priority_queue_.
  boost::scoped_ptr<MemPool> tuple_pool_;

  // Iterator over elements in sorted_top_n_.
  std::vector<Tuple*>::iterator get_next_iter_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Number of rows skipped. Used for adhering to offset_.
  int64_t num_rows_skipped_;

  /// The priority queue will never have more elements in it than the LIMIT + OFFSET.
  /// The stl priority queue doesn't support a max size, so to get that functionality,
  /// the order of the queue is the opposite of what the ORDER BY clause specifies, such
  /// that the top of the queue is the last sorted element.
  boost::scoped_ptr<
      std::priority_queue<Tuple*, std::vector<Tuple*>, TupleRowComparator> >
          priority_queue_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////
};

};

#endif
