// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HASH_JOIN_NODE_H
#define IMPALA_EXEC_HASH_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>

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

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Close(RuntimeState* state);

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

  // we need two sets of build exprs to evaluate TupleRow equality
  // when constructing the build table.  The exprs is where expr results
  // are stored and we need separate locations for the two build tuples.
  std::vector<Expr*> build_exprs1_;
  std::vector<Expr*> build_exprs2_;

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
  TupleRow* current_probe_row_;

  // build_tuple_idx_[i] is the tuple index of child(1)'s tuple[i] in the output row
  std::vector<int> build_tuple_idx_;
  int build_tuple_size_;
  
  RuntimeProfile::Counter* build_timer_;   // time to build hash table
  RuntimeProfile::Counter* probe_timer_;   // time to probe
  RuntimeProfile::Counter* build_row_counter_;   // num build rows
  RuntimeProfile::Counter* probe_row_counter_;   // num probe rows

  // set up build_- and probe_exprs_
  Status Init(ObjectPool* pool, const TPlanNode& tnode);

  // Write combined row, consisting of probe_row and build_row, to out_batch
  // and return the combined row.
  TupleRow* CreateOutputRow(RowBatch* out_batch, TupleRow* probe_row,
      TupleRow* build_row);
};

}

#endif
