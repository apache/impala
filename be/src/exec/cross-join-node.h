// Copyright 2013 Cloudera Inc.
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


#ifndef IMPALA_EXEC_CROSS_JOIN_NODE_H
#define IMPALA_EXEC_CROSS_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread.hpp>
#include <string>

#include "exec/exec-node.h"
#include "exec/blocking-join-node.h"
#include "exec/row-batch-list.h"
#include "runtime/descriptors.h"  // for TupleDescriptor
#include "runtime/mem-pool.h"
#include "util/promise.h"

#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class RowBatch;
class TupleRow;

/// Node for cross joins.
/// Iterates over the left child rows and then the right child rows and, for
/// each combination, writes the output row if the conjuncts are satisfied. The
/// build batches are kept in a list that is fully constructed from the right child in
/// ConstructBuildSide() (called by BlockingJoinNode::Open()) while rows are fetched from
/// the left child as necessary in GetNext().
class CrossJoinNode : public BlockingJoinNode {
 public:
  CrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 protected:
  virtual Status InitGetNext(TupleRow* first_left_row);
  virtual Status ConstructBuildSide(RuntimeState* state);

 private:
  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Object pool for build RowBatches. Stores and owns all batches in build_batches_.
  ObjectPool build_batch_pool_;

  /// List of build batches. The batches are owned by build_batch_pool_.
  RowBatchList build_batches_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  RowBatchList::TupleRowIterator current_build_row_;

  /// Processes a batch from the left child.
  ///  output_batch: the batch for resulting tuple rows
  ///  batch: the batch from the left child to process.  This function can be called to
  ///    continue processing a batch in the middle
  ///  max_added_rows: maximum rows that can be added to output_batch
  /// return the number of rows added to output_batch
  int ProcessLeftChildBatch(RowBatch* output_batch, RowBatch* batch, int max_added_rows);

  /// Returns a debug string for build_rows_. This is used for debugging during the
  /// build list construction and before doing the join.
  std::string BuildListDebugString();
};

}

#endif
