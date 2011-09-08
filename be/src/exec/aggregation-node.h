// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_AGGREGATION_NODE_H
#define IMPALA_EXEC_AGGREGATION_NODE_H

#include <functional>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/functional/hash.hpp>

#include "exec/exec-node.h"
#include "runtime/descriptors.h"  // for TupleId

namespace impala {

class AggregateExpr;
class MemPool;
class RowBatch;
struct RuntimeState;
class Tuple;
class TupleDescriptor;

// Node for in-memory hash aggregation.
// The node creates a hash set of aggregation output tuples, which
// contain slots for all grouping and aggregation exprs (the grouping
// slots precede the aggregation expr slots in the output tuple descriptor).
class AggregationNode : public ExecNode {
 public:
  AggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch);
  virtual Status Close(RuntimeState* state);

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
    // If a is NULL, compares a with computed grouping exprs of
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
  Tuple* singleton_output_tuple_;  // result of aggregation w/o GROUP BY
  TupleRow* current_row_;  // needed for GroupingExprHash/-Equals
  std::vector<TupleDescriptor*> input_tuple_descs_;

  boost::scoped_ptr<MemPool> tuple_pool_;

  // Constructs a new aggregation output tuple (allocated from tuple_pool_),
  // initialized to grouping values computed over 'row'.
  // Aggregation expr slots are set to their initial values.
  Tuple* ConstructAggTuple(TupleRow* row);

  // Updates the aggregation output tuple 'tuple' with aggregation values
  // computed over 'row'.
  void UpdateAggTuple(Tuple* tuple, TupleRow* row);
};

}

#endif
