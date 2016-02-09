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


#ifndef IMPALA_EXEC_HASH_JOIN_NODE_H
#define IMPALA_EXEC_HASH_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <string>

#include "exec/exec-node.h"
#include "exec/old-hash-table.h"
#include "exec/blocking-join-node.h"
#include "util/promise.h"

#include "gen-cpp/PlanNodes_types.h"  // for TJoinOp

namespace impala {

class MemPool;
class RowBatch;
class TupleRow;

/// Node for in-memory hash joins:
/// - builds up a hash table with the rows produced by our right input
///   (child(1)); build exprs are the rhs exprs of our equi-join predicates
/// - for each row from our left input, probes the hash table to retrieve
///   matching entries; the probe exprs are the lhs exprs of our equi-join predicates
//
/// Row batches:
/// - In general, we are not able to pass our output row batch on to our left child (when
///   we're fetching the probe rows): if we have a 1xn join, our output will contain
///   multiple rows per left input row
/// - TODO: fix this, so in the case of 1x1/nx1 joins (for instance, fact to dimension tbl)
///   we don't do these extra copies
class HashJoinNode : public BlockingJoinNode {
 public:
  HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  // Open() implemented in BlockingJoinNode
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

 protected:
  virtual void AddToDebugString(int indentation_level, std::stringstream* out) const;
  virtual Status InitGetNext(TupleRow* first_probe_row);
  virtual Status ConstructBuildSide(RuntimeState* state);

 private:
  boost::scoped_ptr<OldHashTable> hash_tbl_;
  OldHashTable::Iterator hash_tbl_iterator_;

  /// our equi-join predicates "<lhs> = <rhs>" are separated into
  /// build_exprs_ (over child(1)) and probe_exprs_ (over child(0))
    std::vector<ExprContext*> probe_expr_ctxs_;
    std::vector<ExprContext*> build_expr_ctxs_;

  /// is_not_distinct_from_[i] is true if and only if the ith equi-join predicate is IS
  /// NOT DISTINCT FROM, rather than equality.
  std::vector<bool> is_not_distinct_from_;

  /// non-equi-join conjuncts from the JOIN clause
  std::vector<ExprContext*> other_join_conjunct_ctxs_;

  /// Derived from join_op_
  /// Output all rows coming from the probe input. Used in LEFT_OUTER_JOIN and
  /// FULL_OUTER_JOIN.
  bool match_all_probe_;

  /// Match at most one build row to each probe row. Used in LEFT_SEMI_JOIN.
  bool match_one_build_;

  /// Output all rows coming from the build input. Used in RIGHT_OUTER_JOIN and
  /// FULL_OUTER_JOIN.
  bool match_all_build_;

  /// llvm function for build batch
  llvm::Function* codegen_process_build_batch_fn_;

  /// Function declaration for codegen'd function.  Signature must match
  /// HashJoinNode::ProcessBuildBatch
  typedef void (*ProcessBuildBatchFn)(HashJoinNode*, RowBatch*);
  ProcessBuildBatchFn process_build_batch_fn_;

  /// HashJoinNode::ProcessProbeBatch() exactly
  typedef int (*ProcessProbeBatchFn)(HashJoinNode*, RowBatch*, RowBatch*, int);
  /// Jitted ProcessProbeBatch function pointer.  Null if codegen is disabled.
  ProcessProbeBatchFn process_probe_batch_fn_;

  RuntimeProfile::Counter* build_buckets_counter_;   // num buckets in hash table
  RuntimeProfile::Counter* hash_tbl_load_factor_counter_;

  /// GetNext helper function for the common join cases: Inner join, left semi and left
  /// outer
  Status LeftJoinGetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  /// Processes a probe batch for the common (non right-outer join) cases.
  ///  out_batch: the batch for resulting tuple rows
  ///  probe_batch: the probe batch to process.  This function can be called to
  ///    continue processing a batch in the middle
  ///  max_added_rows: maximum rows that can be added to out_batch
  /// return the number of rows added to out_batch
  int ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, int max_added_rows);

  /// Construct the build hash table, adding all the rows in 'build_batch'
  void ProcessBuildBatch(RowBatch* build_batch);

  /// Codegen function to create output row
  llvm::Function* CodegenCreateOutputRow(LlvmCodeGen* codegen);

  /// Codegen processing build batches.  Identical signature to ProcessBuildBatch.
  /// hash_fn is the codegen'd function for computing hashes over tuple rows in the
  /// hash table.
  /// Returns NULL if codegen was not possible.
  llvm::Function* CodegenProcessBuildBatch(RuntimeState* state, llvm::Function* hash_fn);

  /// Codegen processing probe batches.  Identical signature to ProcessProbeBatch.
  /// hash_fn is the codegen'd function for computing hashes over tuple rows in the
  /// hash table.
  /// Returns NULL if codegen was not possible.
  llvm::Function* CodegenProcessProbeBatch(RuntimeState* state, llvm::Function* hash_fn);
};

}

#endif
