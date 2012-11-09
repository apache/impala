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
#include "runtime/descriptors.h"  // for TupleId

namespace impala {

class MemPool;
class RuntimeState;
class Tuple;

// Node for in-memory TopN (ORDER BY ... LIMIT)
// This handles the case where the result fits in memory.  This node will do a deep
// copy of the tuples that are necessary for the output.
// This is implemented by storing rows in a priority queue.  
class TopNNode : public ExecNode {
 public:
  TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  Status Init(ObjectPool* pool, const TPlanNode& tnode);

  class TupleRowLessThan {
   public:
    TupleRowLessThan() : node_(NULL) {}
    TupleRowLessThan(TopNNode* node) : node_(node) {}
    bool operator()(TupleRow* const& lhs, TupleRow* const& rhs) const;

   private:
    TopNNode* node_;
  };
    
  friend class TupleLessThan;

  // Inserts a tuple row into the priority queue if it's in the TopN.  Creates a deep 
  // copy of tuple_row, which it stores in tuple_pool_.
  void InsertTupleRow(TupleRow* tuple_row);

  // Flatten and reverse the priority queue.
  void PrepareForOutput();

  std::vector<TupleDescriptor*> tuple_descs_;
  std::vector<bool> is_asc_order_;

  // Create two copies of the exprs for evaluating over the TupleRows.
  // The result of the evaluation is stored in the Expr, so it's not efficient to use
  // one set of Expr to compare TupleRows.
  std::vector<Expr*> lhs_ordering_exprs_;
  std::vector<Expr*> rhs_ordering_exprs_;

  TupleRowLessThan tuple_row_less_than_;

  // The priority queue will never have more elements in it than the LIMIT.  The stl 
  // priority queue doesn't support a max size, so to get that functionality, the order
  // of the queue is the opposite of what the ORDER BY clause specifies, such that the top 
  // of the queue is the last sorted element.
  std::priority_queue<TupleRow*, std::vector<TupleRow*>, TupleRowLessThan> priority_queue_;

  // After computing the TopN in the priority_queue, pop them and put them in this vector
  std::vector<TupleRow*> sorted_top_n_;
  std::vector<TupleRow*>::iterator get_next_iter_;
    
  // Stores everything referenced in priority_queue_
  boost::scoped_ptr<MemPool> tuple_pool_;
};

};

#endif

