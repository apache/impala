// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_AGGREGATION_NODE_H
#define IMPALA_EXEC_AGGREGATION_NODE_H

#include <functional>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/functional/hash.hpp>

#include "exec/exec-node.h"
#include "runtime/descriptors.h"  // for TupleId
#include "runtime/free-list.h"
#include "runtime/mem-pool.h"

namespace llvm {
  class Function;
}

namespace impala {

class AggregateExpr;
class AggregationTuple;
class LlvmCodeGen;
class RowBatch;
struct RuntimeState;
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

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Close(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;
  
 private:
  class GroupingExprHash : public std::unary_function<Tuple*, std::size_t> {
   public:
    GroupingExprHash(AggregationNode* node): node_(node) {}
    void Init(TupleDescriptor* agg_tuple_d, const std::vector<Expr*>& grouping_exprs);

    // Compute a combined hash value for the values stored in t's
    // grouping slots. If t is NULL, compute grouping hash values
    // from AggregationNode::current_row_ and grouping_exprs.
    std::size_t operator()(Tuple* const& t) const;

   private:
    AggregationNode* node_;
    const TupleDescriptor* agg_tuple_desc_;
    const std::vector<Expr*>* grouping_exprs_;
  };

  class GroupingExprEquals : public std::binary_function<Tuple*, Tuple*, bool> {
   public:
    GroupingExprEquals(AggregationNode* node): node_(node) {}
    void Init(TupleDescriptor* agg_tuple_d, const std::vector<Expr*>& grouping_exprs);

    // Return true if grouping slots of a and b are equal, otherwise false.
    // If a is NULL, compares b with computed grouping exprs of
    // AggregationNode::current_row_.
    bool operator()(Tuple* const& a, Tuple* const& b) const;

   private:
    AggregationNode* node_;
    const TupleDescriptor* agg_tuple_desc_;
    const std::vector<Expr*>* grouping_exprs_;
  };

  friend class GroupingExprHash;
  friend class GroupingExprEquals;

  typedef boost::unordered_set<Tuple*, GroupingExprHash, GroupingExprEquals> HashTable;

  boost::scoped_ptr<HashTable> hash_tbl_;
  GroupingExprHash hash_fn_;
  GroupingExprEquals equals_fn_;
  HashTable::iterator output_iterator_;

  std::vector<Expr*> grouping_exprs_;
  std::vector<Expr*> aggregate_exprs_;
  TupleId agg_tuple_id_;
  TupleDescriptor* agg_tuple_desc_;
  AggregationTuple* singleton_output_tuple_;  // result of aggregation w/o GROUP BY
  TupleRow* current_row_;  // needed for GroupingExprHash/-Equals
  FreeList string_buffer_free_list_;
  int num_string_slots_; // number of string slots in the output tuple

  std::vector<void*> grouping_values_cache_;
  boost::scoped_ptr<MemPool> tuple_pool_;

  typedef void (*ProcessRowBatchFn)(AggregationNode*, RowBatch*);
  // Jitted ProcessRowBatch function pointer.  Null if codegen is disabled.
  ProcessRowBatchFn process_row_batch_fn_;

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
  //  str: the value to update dst to.  
  void UpdateStringSlot(AggregationTuple*, int string_slot_idx, StringValue* dst, 
                           const StringValue* str);

  // Helpers to compute Aggregate.MIN/Aggregate.MAX
  void UpdateMinStringSlot(AggregationTuple*, const NullIndicatorOffset&, 
                           int slot_id, void* slot, void* value);
  void UpdateMaxStringSlot(AggregationTuple*, const NullIndicatorOffset&, 
                           int slot_id, void* slot, void* value);
  
  // Updates the aggregation output tuple 'tuple' with aggregation values
  // computed over 'row'.
  void UpdateAggTuple(AggregationTuple* tuple, TupleRow* row);

  // Eval grouping exprs over 'current_row_' and store the results in 
  // 'grouping_exprs_cache_'.  
  void ComputeGroupingValues();

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
};

}

#endif
