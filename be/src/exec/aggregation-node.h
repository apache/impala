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


#ifndef IMPALA_EXEC_AGGREGATION_NODE_H
#define IMPALA_EXEC_AGGREGATION_NODE_H

#include <functional>
#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "exec/hash-table.h"
#include "runtime/descriptors.h"  // for TupleId
#include "runtime/mem-pool.h"
#include "runtime/string-value.h"

namespace llvm {
  class Function;
}

namespace impala {

class AggFnEvaluator;
class LlvmCodeGen;
class RowBatch;
class RuntimeState;
struct StringValue;
class Tuple;
class TupleDescriptor;
class SlotDescriptor;

// Node for in-memory hash aggregation.
// The node creates a hash set of aggregation output tuples, which
// contain slots for all grouping and aggregation exprs (the grouping
// slots precede the aggregation expr slots in the output tuple descriptor).
//
// TODO: codegen cross-compiled UDAs and get rid of handcrafted IR.
// TODO: investigate high compile time for wide tables
class AggregationNode : public ExecNode {
 public:
  AggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  boost::scoped_ptr<HashTable> hash_tbl_;
  HashTable::Iterator output_iterator_;

  std::vector<AggFnEvaluator*> aggregate_evaluators_;
  // Exprs used to evaluate input rows
  std::vector<ExprContext*> probe_expr_ctxs_;
  // Exprs used to insert constructed aggregation tuple into the hash table.
  // All the exprs are simply SlotRefs for the agg tuple.
  std::vector<ExprContext*> build_expr_ctxs_;
  TupleId agg_tuple_id_;
  TupleDescriptor* agg_tuple_desc_;
  // Result of aggregation w/o GROUP BY.
  // Note: can be NULL even if there is no grouping if the result tuple is 0 width
  Tuple* singleton_output_tuple_;

  boost::scoped_ptr<MemPool> tuple_pool_;

  // IR for process row batch.  NULL if codegen is disabled.
  llvm::Function* codegen_process_row_batch_fn_;

  typedef void (*ProcessRowBatchFn)(AggregationNode*, RowBatch*);
  // Jitted ProcessRowBatch function pointer.  Null if codegen is disabled.
  ProcessRowBatchFn process_row_batch_fn_;

  // If true, this aggregation node should use the aggregate evaluator's Merge()
  // instead of Update()
  bool is_merge_;

  // Certain aggregates require a finalize step, which is the final step of the
  // aggregate after consuming all input rows. The finalize step converts the aggregate
  // value into its final form. This is true if this node contains aggregate that requires
  // a finalize step.
  bool needs_finalize_;

  // Time spent processing the child rows
  RuntimeProfile::Counter* build_timer_;
  // Time spent returning the aggregated rows
  RuntimeProfile::Counter* get_results_timer_;
  // Num buckets in hash table
  RuntimeProfile::Counter* hash_table_buckets_counter_;
  // Load factor in hash table
  RuntimeProfile::Counter* hash_table_load_factor_counter_;

  // Constructs a new aggregation output tuple (allocated from tuple_pool_),
  // initialized to grouping values computed over 'current_row_'.
  // Aggregation expr slots are set to their initial values.
  Tuple* ConstructAggTuple();

  // Updates the aggregation output tuple 'tuple' with aggregation values
  // computed over 'row'.
  void UpdateAggTuple(Tuple* tuple, TupleRow* row);

  // Called when all rows have been aggregated for the aggregation tuple to compute final
  // aggregate values
  void FinalizeAggTuple(Tuple* tuple);

  // Do the aggregation for all tuple rows in the batch
  void ProcessRowBatchNoGrouping(RowBatch* batch);
  void ProcessRowBatchWithGrouping(RowBatch* batch);

  // Codegen the process row batch loop.  The loop has already been compiled to
  // IR and loaded into the codegen object.  UpdateAggTuple has also been
  // codegen'd to IR.  This function will modify the loop subsituting the
  // UpdateAggTuple function call with the (inlined) codegen'd 'update_tuple_fn'.
  llvm::Function* CodegenProcessRowBatch(
      RuntimeState* state, llvm::Function* update_tuple_fn);

  // Codegen for updating aggregate_exprs at slot_idx. Returns NULL if unsuccessful.
  // slot_idx is the idx into aggregate_exprs_ (does not include grouping exprs).
  llvm::Function* CodegenUpdateSlot(
      RuntimeState* state, AggFnEvaluator* evaluator, SlotDescriptor* slot_desc);

  // Codegen UpdateAggTuple.  Returns NULL if codegen is unsuccessful.
  llvm::Function* CodegenUpdateAggTuple(RuntimeState* state);
};

}

#endif
