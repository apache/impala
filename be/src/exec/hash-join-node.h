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
#include <boost/unordered_set.hpp>
#include <boost/thread.hpp>
#include <string>

#include "exec/exec-node.h"
#include "exec/hash-table.h"

#include "gen-cpp/PlanNodes_types.h"  // for TJoinOp

namespace impala {

class MemPool;
class RowBatch;
class TupleRow;

// Node for in-memory hash joins:
// - builds up a hash table with the rows produced by our right input
//   (child(1)); build exprs are the rhs exprs of our equi-join predicates
// - for each row from our left input, probes the hash table to retrieve
//   matching entries; the probe exprs are the lhs exprs of our equi-join predicates
//
// Row batches:
// - In general, we are not able to pass our output row batch on to our left child (when
//   we're fetching the probe rows): if we have a 1xn join, our output will contain
//   multiple rows per left input row
// - TODO: fix this, so in the case of 1x1/nx1 joins (for instance, fact to dimension tbl)
//   we don't do these extra copies
class HashJoinNode : public ExecNode {
 public:
  HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~HashJoinNode();

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Close(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

 protected:
  void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  boost::scoped_ptr<HashTable> hash_tbl_;
  HashTable::Iterator hash_tbl_iterator_;

  // for right outer joins, keep track of what's been joined
  typedef boost::unordered_set<TupleRow*> BuildTupleRowSet;
  BuildTupleRowSet joined_build_rows_;

  TJoinOp::type join_op_;

  // our equi-join predicates "<lhs> = <rhs>" are separated into
  // build_exprs_ (over child(1)) and probe_exprs_ (over child(0))
  std::vector<Expr*> probe_exprs_;
  std::vector<Expr*> build_exprs_;

  // non-equi-join conjuncts from the JOIN clause
  std::vector<Expr*> other_join_conjuncts_;

  // derived from join_op_
  bool match_all_probe_;  // output all rows coming from the probe input
  bool match_one_build_;  // match at most one build row to each probe row
  bool match_all_build_;  // output all rows coming from the build input

  bool matched_probe_;  // if true, we have matched the current probe row
  bool eos_;  // if true, nothing left to return in GetNext()
  boost::scoped_ptr<MemPool> build_pool_;  // holds everything referenced in hash_tbl_

  // probe_batch_ must be cleared before calling GetNext().  The child node
  // does not initialize all tuple ptrs in the row, only the ones that it
  // is responsible for.
  boost::scoped_ptr<RowBatch> probe_batch_;
  int probe_batch_pos_;  // current scan pos in probe_batch_
  bool probe_eos_;  // if true, probe child has no more rows to process
  TupleRow* current_probe_row_;

  // build_tuple_idx_[i] is the tuple index of child(1)'s tuple[i] in the output row
  std::vector<int> build_tuple_idx_;
  int build_tuple_size_;

  // byte size of result tuple row (sum of the tuple ptrs, not the tuple data).
  // This should be the same size as the probe tuple row.
  int result_tuple_row_size_;

  // llvm function for build batch
  llvm::Function* codegen_process_build_batch_fn_;

  // Function declaration for codegen'd function.  Signature must match
  // HashJoinNode::ProcessBuildBatch
  typedef void (*ProcessBuildBatchFn)(HashJoinNode*, RowBatch*);
  ProcessBuildBatchFn process_build_batch_fn_;

  // llvm function object for probe batch
  llvm::Function* codegen_process_probe_batch_fn_;

  // HashJoinNode::ProcessProbeBatch() exactly
  typedef int (*ProcessProbeBatchFn)(HashJoinNode*, RowBatch*, RowBatch*, int);
  // Jitted ProcessProbeBatch function pointer.  Null if codegen is disabled.
  ProcessProbeBatchFn process_probe_batch_fn_;

  RuntimeProfile::Counter* build_timer_;   // time to build hash table
  RuntimeProfile::Counter* probe_timer_;   // time to probe
  RuntimeProfile::Counter* build_row_counter_;   // num build rows
  RuntimeProfile::Counter* probe_row_counter_;   // num probe rows
  RuntimeProfile::Counter* build_buckets_counter_;   // num buckets in hash table
  RuntimeProfile::Counter* hash_tbl_load_factor_counter_;

  // set up build_- and probe_exprs_
  Status Init(ObjectPool* pool, const TPlanNode& tnode);

  // Supervises ConstructHashTable in a separate thread, and
  // returns its status in the promise parameter.
  void BuildSideThread(RuntimeState* state, boost::promise<Status>* status);

  // We parallelise building the build-side with Open'ing the
  // probe-side. If, for example, the probe-side child is another
  // hash-join node, it can start to build its own build-side at the
  // same time.
  Status ConstructHashTable(RuntimeState* state);

  // GetNext helper function for the common join cases: Inner join, left semi and left
  // outer
  Status LeftJoinGetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // Processes a probe batch for the common (non right-outer join) cases.
  //  out_batch: the batch for resulting tuple rows
  //  probe_batch: the probe batch to process.  This function can be called to
  //    continue processing a batch in the middle
  //  max_added_rows: maximum rows that can be added to out_batch
  // return the number of rows added to out_batch
  int ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, int max_added_rows);

  // Construct the build hash table, adding all the rows in 'build_batch'
  void ProcessBuildBatch(RowBatch* build_batch);

  // Write combined row, consisting of probe_row and build_row, to out_row.
  // This is replaced by codegen.
  void CreateOutputRow(TupleRow* out_row, TupleRow* probe_row, TupleRow* build_row);

  // Returns a debug string for probe_rows.  Probe rows have tuple ptrs that are
  // uninitialized; the left hand child only populates the tuple ptrs it is responsible
  // for.  This function outputs just the probe row values and leaves the build
  // side values as NULL.
  // This is only used for debugging and outputting the left child rows before
  // doing the join.
  std::string GetProbeRowOutputString(TupleRow* probe_row);

  // Codegen function to create output row
  llvm::Function* CodegenCreateOutputRow(LlvmCodeGen* codegen);

  // Codegen processing build batches.  Identical signature to ProcessBuildBatch.
  // hash_fn is the codegen'd function for computing hashes over tuple rows in the
  // hash table.
  // Returns NULL if codegen was not possible.
  llvm::Function* CodegenProcessBuildBatch(LlvmCodeGen*, llvm::Function* hash_fn);

  // Codegen processing probe batches.  Identical signature to ProcessProbeBatch.
  // hash_fn is the codegen'd function for computing hashes over tuple rows in the
  // hash table.
  // Returns NULL if codegen was not possible.
  llvm::Function* CodegenProcessProbeBatch(LlvmCodeGen*, llvm::Function* hash_fn);
};

}

#endif
