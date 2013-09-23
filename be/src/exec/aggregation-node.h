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
#include "runtime/free-list.h"
#include "runtime/mem-pool.h"
#include "runtime/string-value.h"

namespace llvm {
  class Function;
}

namespace impala {

class AggregateExpr;
class AggregationTuple;
class LlvmCodeGen;
class RowBatch;
class RuntimeState;
struct StringValue;
class Tuple;
class TupleDescriptor;

// Node for in-memory hash aggregation.
// The node creates a hash set of aggregation output tuples, which
// contain slots for all grouping and aggregation exprs (the grouping
// slots precede the aggregation expr slots in the output tuple descriptor).
//
// For string aggregation, we need to append additional data to the tuple object
// to reduce the number of string allocations (since we cannot know the length of
// the output string beforehand).  For each string slot in the output tuple, a int32
// will be appended to the end of the normal tuple data that stores the size of buffer
// for that string slot.  This also results in the correct alignment because StringValue
// slots are 8-byte aligned and form the tail end of the tuple.
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

  std::vector<Expr*> aggregate_exprs_;
  // Exprs used to evaluate input rows
  std::vector<Expr*> probe_exprs_;
  // Exprs used to insert constructed aggregation tuple into the hash table.
  // All the exprs are simply SlotRefs for the agg tuple.
  std::vector<Expr*> build_exprs_;
  TupleId agg_tuple_id_;
  TupleDescriptor* agg_tuple_desc_;
  AggregationTuple* singleton_output_tuple_;  // result of aggregation w/o GROUP BY
  FreeList string_buffer_free_list_;
  int num_string_slots_; // number of string slots in the output tuple
  StringValue default_group_concat_delimiter_;

  boost::scoped_ptr<MemPool> tuple_pool_;

  // IR for process row batch.  NULL if codegen is disabled.
  llvm::Function* codegen_process_row_batch_fn_;

  typedef void (*ProcessRowBatchFn)(AggregationNode*, RowBatch*);
  // Jitted ProcessRowBatch function pointer.  Null if codegen is disabled.
  ProcessRowBatchFn process_row_batch_fn_;

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
  AggregationTuple* ConstructAggTuple();

  // Allocates a string buffer that is at least the new_size.  The actual size of
  // the allocated buffer is returned in allocated_size.
  // The function first tries to allocate from the free list.  If there is nothing
  // there, it will allocate from the MemPool.
  char* AllocateStringBuffer(int new_size, int* allocated_size);

  // Helper to update string value from src into dst.  This does a deep copy
  // and will allocate a buffer for dst as necessary.
  //  AggregationTuple: Contains the tuple data and the string buffer lengths
  //  string_slot_idx: The i-th string slot in the aggregation tuple
  //  dst: the target location to update the string.  This is part of the
  //          AggregationTuple that's also passed in.
  //  src: the value to update dst to.
  void UpdateStringSlot(AggregationTuple* agg_tuple, int string_slot_idx,
                        StringValue* dst, const StringValue* src);

  void ConcatStringSlot(AggregationTuple* agg_tuple,
      const NullIndicatorOffset& null_indicator_offset, int string_slot_idx,
      StringValue* dst, const StringValue* src, const StringValue* delimiter);

  // Helpers to compute Aggregate.MIN/Aggregate.MAX
  void UpdateMinStringSlot(AggregationTuple*, const NullIndicatorOffset&,
                           int slot_id, void* slot, void* value);
  void UpdateMaxStringSlot(AggregationTuple*, const NullIndicatorOffset&,
                           int slot_id, void* slot, void* value);

  // Updates the aggregation output tuple 'tuple' with aggregation values
  // computed over 'row'.
  void UpdateAggTuple(AggregationTuple* tuple, TupleRow* row);

  // Called when all rows have been aggregated for the aggregation tuple to compute final
  // aggregate values
  void FinalizeAggTuple(AggregationTuple* tuple);

  // Do the aggregation for all tuple rows in the batch
  void ProcessRowBatchNoGrouping(RowBatch* batch);
  void ProcessRowBatchWithGrouping(RowBatch* batch);

  // Codegen the process row batch loop.  The loop has already been compiled to
  // IR and loaded into the codegen object.  UpdateAggTuple has also been
  // codegen'd to IR.  This function will modify the loop subsituting the
  // UpdateAggTuple function call with the (inlined) codegen'd 'update_tuple_fn'.
  llvm::Function* CodegenProcessRowBatch(
      LlvmCodeGen* codegen, llvm::Function* update_tuple_fn);

  // Codegen for updating aggregate_exprs at slot_idx. Returns NULL if unsuccessful.
  // slot_idx is the idx into aggregate_exprs_ (does not include grouping exprs).
  llvm::Function* CodegenUpdateSlot(LlvmCodeGen* codegen, int slot_idx);

  // Codegen UpdateAggTuple.  Returns NULL if codegen is unsuccessful.
  llvm::Function* CodegenUpdateAggTuple(LlvmCodeGen* codegen);

  // Compute distinctpc and distinctpcsa using Flajolet and Martin's algorithm
  // (Probabilistic Counting Algorithms for Data Base Applications)
  // We have implemented two variants here: one with stochastic averaging (with PCSA
  // postfix) and one without.
  // There are 4 phases to compute the aggregate:
  //   1. allocate a bitmap, stored in the aggregation tuple's output string slot
  //   2. update the bitmap per row (UpdateDistinctEstimateSlot)
  //   3. for distribtued plan, merge the bitmaps from all the nodes
  //      (UpdateMergeEstimateSlot)
  //   4. compute the estimate using the bitmaps when all the rows are processed
  //      (FinalizeEstimateSlot)
  const static int NUM_PC_BITMAPS; // number of bitmaps
  const static int PC_BITMAP_LENGTH; // the length of each bit map
  const static float PC_THETA; // the magic number to compute the final result

  // Initialize the NUM_PC_BITMAPS * PC_BITMAP_LENGTH bitmaps. The bitmaps are allocated
  // as a string value of the slot. Both algorithms share the same bitmap structure.
  void ConstructDistinctEstimateSlot(AggregationTuple*, const NullIndicatorOffset&,
                                     int slot_id, void* slot);

  // Update the distinct estimate bitmaps for a single input row we consumed
  void UpdateDistinctEstimatePCSASlot(void* slot, void* value, PrimitiveType type);
  void UpdateDistinctEstimateSlot(void* slot, void* value, PrimitiveType type);

  // Merge the distinct estimate bitmaps from all slave nodes (multi-node plan) by
  // union-ing all the bits. Both algorithms share the same merging logic.
  void UpdateMergeEstimateSlot(AggregationTuple*, int slot_id, void* slot, void* value);

  // Convert the distinct estimate bitmaps into the final estimated number in string form
  // The logic differs slightly between the two algorithm. agg_op indicates which
  // algorithm we are executing.
  void FinalizeEstimateSlot(int slot_id, void* slot, TAggregationOp::type agg_op);

  // Helper function to print aggregation tuple's distinct estimate bitmap
  static std::string DistinctEstimateBitMapToString(char* v);
};

}

#endif
