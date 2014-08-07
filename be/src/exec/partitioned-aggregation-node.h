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


#ifndef IMPALA_EXEC_PARTITIONED_AGGREGATION_NODE_H
#define IMPALA_EXEC_PARTITIONED_AGGREGATION_NODE_H

#include <functional>
#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "exec/hash-table.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/buffered-tuple-stream.h"
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

// Node for doing partitioned hash aggregation.
// This node consumes the input (which can be from the child(0) or a spilled partition).
//  1. Each row is hashed and we pick a dst partition (hash_partitions_).
//  2. If the dst partition is not spilled, we probe into the partitions hash table
//  to aggregate/insert the row.
//  3. If the partition is already spilled, the input row is spilled.
//  4. When all the input is consumed, we walk hash_partitions_, put the spilled ones
//  into spilled_partitions_ and the non-spilled ones into aggregated_partitions_.
//  aggregated_partitions_ contain partitions that are fully processed and the result
//  can just be returned. Partitions in spilled_partitions_ need to be repartitioned
//  and we just repeat these steps.
//
// Each partition contains these structures:
// 1) Hash Table for aggregated rows. This contains just the hash table directory
//    structure but not the rows themselves. This is NULL for spilled partitions when
//    we stop maintaining the hash table.
// 2) MemPool for var-len result data for rows in the hash table. If the aggregate
//    function returns a string, we cannot append it to the tuple stream as that
//    structure is immutable. Instead, when we need to spill, we sweep and copy the
//    rows into a tuple stream.
// 3) Aggregated tuple stream for rows that are/were in the hash table. This stream
//    contains rows that are aggregated. When the partition is not spilled, this stream
//    is pinned and contains the memory referenced by the hash table.
//    In the case where the aggregate function does not return a string (meaning the
//    size of all the slots is known when the row is constructed), this stream contains
//    all the memory for the result rows and the MemPool (2) is not used.
// 4) Unaggregated tuple stream. Stream to spill unaggregated rows.
//    Rows in this stream always have child(0)'s layout.
//
// TODO: Buffer rows before probing into the hash table?
// TODO: after spilling, we can still maintain a very small hash table just to remove
// some number of rows (from likely going to disk).
// TODO: consider allowing to spill the hash table structure in addition to the rows.
// TODO: do we want to insert a buffer before probing into the partition's hash table.
// TODO: use a prefetch/batched probe interface.
// TODO: return rows from the aggregated_row_stream rather than the HT.
// TODO: spill the HT as well
// TODO: think about spilling heuristic.
// TODO: when processing a spilled partition, we have a lot more information and can
// size the partitions/hash tables better.
class PartitionedAggregationNode : public ExecNode {
 public:
  PartitionedAggregationNode(ObjectPool* pool,
      const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  struct Partition;

  const TupleId agg_tuple_id_;
  TupleDescriptor* agg_tuple_desc_;

  // If true, this aggregation node should use the aggregate evaluator's Merge()
  // instead of Update()
  const bool is_merge_;

  // Certain aggregates require a finalize step, which is the final step of the
  // aggregate after consuming all input rows. The finalize step converts the aggregate
  // value into its final form. This is true if this node contains aggregate that requires
  // a finalize step.
  // TODO: push this to AggFnEvaluator after expr refactoring patch.
  const bool needs_finalize_;

  std::vector<AggFnEvaluator*> aggregate_evaluators_;

  // Exprs used to evaluate input rows
  std::vector<ExprContext*> probe_expr_ctxs_;

  // Exprs used to insert constructed aggregation tuple into the hash table.
  // All the exprs are simply SlotRefs for the agg tuple.
  std::vector<ExprContext*> build_expr_ctxs_;

  // True if the resulting tuple contains var-len agg/grouping expressions. This
  // means we need to do more work when allocating and spilling these rows.
  bool contains_var_len_agg_exprs_;
  bool contains_var_len_grouping_exprs_;

  RuntimeState* state_;
  BufferedBlockMgr::Client* block_mgr_client_;

  // Result of aggregation w/o GROUP BY.
  // Note: can be NULL even if there is no grouping if the result tuple is 0 width
  // e.g. select 1 from table group by col.
  Tuple* singleton_output_tuple_;
  bool singleton_output_tuple_returned_;

  // MemPool used to allocate memory for when we don't have grouping and don't initialize
  // the partitioning structures.
  boost::scoped_ptr<MemPool> mem_pool_;

  // TODO: we use this hash table to hash rows. Remove when the hashing interface is
  // cleaned up.
  boost::scoped_ptr<HashTable> hash_tbl_;

  // The current partition and iterator to the next row in its hash table that we need
  // to return in GetNext()
  Partition* output_partition_;
  HashTable::Iterator output_iterator_;

  // Time spent processing the child rows
  RuntimeProfile::Counter* build_timer_;

  // Time spent returning the aggregated rows
  RuntimeProfile::Counter* get_results_timer_;

  struct Partition {
    Partition(PartitionedAggregationNode* parent, int level)
      : parent(parent), is_closed(false), level(level) {}

    // Initializes a partition.
    Status Init();

    // Closes this partition. If finalize_rows is true, this iterates over all rows
    // in aggregated_row_stream and finalizes them (this is only used in the cancellation
    // path).
    void Close(bool finalize_rows);

    bool is_spilled() const { return hash_tbl.get() == NULL; }

    PartitionedAggregationNode* parent;

    // If true, this partition is closed and there is nothing left to do.
    bool is_closed;

    // How many times rows in this partition have been repartitioned. Partitions created
    // from the node's children's input is level 0, 1 after the first repartitionining,
    // etc.
    int level;

    // Hash table for this partition.
    // Can be NULL if this partition is no longer maintaining a hash table (i.e.
    // is spilled).
    boost::scoped_ptr<HashTable> hash_tbl;

    // MemPool used if the grouping expr needs to allocate strings.
    // TODO: we really need to plumb through CHAR(N) for intermediate types.
    boost::scoped_ptr<MemPool> mem_pool;

    // We want each partition's aggregate functions to use the per partition mem_pool.
    ExprContext* expr_ctx;

    // Tuple stream used to store aggregated rows. When the partition is not spilled,
    // (meaning the hash table is maintained), this stream is pinned and contains the
    // memory referenced by the hash table. When it is spilled, aggregate rows are
    // just appended to this stream.
    boost::scoped_ptr<BufferedTupleStream> aggregated_row_stream;

    // Unaggregated rows that are spilled.
    boost::scoped_ptr<BufferedTupleStream> unaggregated_row_stream;
  };

  // Current partitions we are partitioning into.
  std::vector<Partition*> hash_partitions_;

  // All partitions that have been spilled and need further processing.
  std::list<Partition*> spilled_partitions_;

  // All partitions that are aggregated and can just return the results in GetNext().
  // After consuming all the input, hash_partitions_ is split into spilled_partitions_
  // or aggregated_partitions_, depending on if it was spilled or not.
  std::list<Partition*> aggregated_partitions_;

  // Allocates a new allocated aggregation output tuple.
  // initialized to grouping values computed over 'current_row_'.
  // Aggregation expr slots are set to their initial values.
  // Pool/Stream specify where the memory (tuple and var len slots) should be allocated
  // from. Only one can be set.
  // Returns NULL if there was not enough memory to allocate the tuple.
  Tuple* ConstructAggTuple(MemPool* pool, BufferedTupleStream* stream);

  // Updates the aggregation output tuple 'tuple' with aggregation values
  // computed over 'row'.
  // If merge is true, then row is an intermediate row, otherwise it is
  // the original child(0) input.
  void UpdateAggTuple(Tuple* tuple, TupleRow* row, bool merge);

  // Called when all rows have been aggregated for the aggregation tuple to compute final
  // aggregate values
  void FinalizeAggTuple(Tuple* tuple);

  // Do the aggregation for all tuple rows in the batch when there is no grouping.
  void ProcessRowBatchNoGrouping(RowBatch* batch);

  // Processes a batch of rows. This is the core function of the algorithm. We partition
  // the rows into hash_partitions_, spilling as necessary.
  // If aggregated_rows is true, it means that the rows in the batch are already
  // pre-aggregated.
  // level is the level of repartitioning (0 for child(0)'s input, then 1, etc).
  // Each level needs to use a different hash function.
  Status ProcessBatch(RowBatch* batch, bool aggregated_rows, int level);

  // Reads all the rows from input_stream and process them by calling ProcessBatch().
  Status ProcessStream(BufferedTupleStream* input_stream, bool aggregated_rows,
      int level);

  // Initializes hash_partitions_. Level is the level for the partitions to create.
  Status CreateHashPartitions(int level);

  // Prepares the next partition to return results from. On return, this function
  // initializes output_iterator_ and output_partition_. This either removes
  // a partition from aggregated_partitions_ (and is done) or removes the next
  // partition from aggregated_partitions_ and repartitions it.
  Status NextPartition();

  // Picks a partition from hash_partitions_ to spill.
  Status SpillPartition();
};

}

#endif
