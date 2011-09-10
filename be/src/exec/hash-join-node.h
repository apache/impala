// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HASH_JOIN_NODE_H
#define IMPALA_EXEC_HASH_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "exec/hash-table.h"

namespace impala {

class MemPool;
class TupleRow;

// Node for in-memory hash joins:
// - builds up a hash table with the tuples produced by our right input
//   (child(1)); build exprs are the rhs exprs of our equi-join predicates
// - for each row from our left input, probes the hash table to retrieve
//   matching entries; the probe exprs are the lhs exprs of our equi-join predicates
//
// Row batches:
// - In general, we are not able to pass our output row batch on to our left child (when we're
//   fetching the probe tuples): if we have a 1xn join, our output will contain
//   multiple rows per left input row
// - TODO: fix this, so in the case of 1x1/nx1 joins (for instance, fact to dimension tbl)
//   we don't do these extra copies
class HashJoinNode : public ExecNode {
 public:
  HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
  ~HashJoinNode();

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch);
  virtual Status Close(RuntimeState* state);

 protected:
  void DebugString(int indentation_level, std::stringstream* out) const;
  
 private:
  boost::scoped_ptr<HashTable> hash_tbl_;
  HashTable::Iterator hash_tbl_iterator_;

  // our equi-join predicates "<lhs> = <rhs>" are separated into
  // build_exprs_ (over child(1)) and probe_exprs_ (over child(0))
  std::vector<Expr*> probe_exprs_;
  std::vector<Expr*> build_exprs_;

  bool eos_;  // if true, nothing left to return in GetNext()
  int build_tuple_idx_;  // w/in our output row
  std::vector<MemPool*> build_pools_;  // everything handed to us by the scan of child(1)
  boost::scoped_ptr<RowBatch> probe_batch_;
  int probe_batch_pos_;  // current scan pos in probe_batch_
  TupleRow* current_probe_row_;

  // set up build_- and probe_exprs_
  Status Init(ObjectPool* pool, const TPlanNode& tnode);
};

}

#endif
