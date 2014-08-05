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


#ifndef IMPALA_EXEC_PARTITIONED_HASH_JOIN_NODE_H
#define IMPALA_EXEC_PARTITIONED_HASH_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread.hpp>
#include <string>

#include "exec/blocking-join-node.h"
#include "exec/exec-node.h"
#include "exec/hash-table.h"
#include "runtime/buffered-block-mgr.h"

#include "gen-cpp/PlanNodes_types.h"  // for TJoinOp

namespace impala {

class BufferedBlockMgr;
class MemPool;
class RowBatch;
class TupleRow;
class BufferedTupleStream;

// Operator to perform partitioned hash join, spilling to disk as necessary.
// A spilled partition is one that is not fully pinned.
// The operator runs in these distinct phases:
//  1. Consume all build input and partition them. No hash tables are maintained.
//  2. Construct hash tables from as many partitions as possible.
//  3. Consume all the probe rows. Rows belonging to partitions that are spilled
//     must be spilled as well.
//  4. Iterate over the spilled partitions, construct the hash table from the spilled
//     build rows and process the spilled probe rows. If the partition is still too
//     big, repeat steps 1-4, using this spilled partitions build and probe rows as
//     input.
//
// TODO: don't copy tuple rows so often.
// TODO: we need multiple hash functions. Each repartition needs new hash functions
// or new bits. Multiplicative hashing?
// TODO: think about details about multithreading. Multiple partitions in parallel?
// Multiple threads against a single partition? How to build hash tables in parallel?
// TODO: BuildHashTables should start with the partitions that are already pinned.
class PartitionedHashJoinNode : public BlockingJoinNode {
 public:
  PartitionedHashJoinNode(ObjectPool* pool, const TPlanNode& tnode,
      const DescriptorTbl& descs);
  ~PartitionedHashJoinNode();

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  // Open() implemented in BlockingJoinNode
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void AddToDebugString(int indentation_level, std::stringstream* out) const;
  virtual Status InitGetNext(TupleRow* first_probe_row);
  virtual Status ConstructBuildSide(RuntimeState* state);

 private:
  // Implementation details:
  // Logically, the algorithm runs in three modes.
  //   1. Read the build side rows and partition them into hash_partitions_. This is a
  //      fixed fan out of the input. The input can either come from child(1) OR from the
  //      build tuple stream of partition that needs to be repartitioned.
  //   2. Read the probe side rows, partition them and either perform the join or spill
  //      them into hash_partitions_. If the partition has the hash table in memory, we
  //      perform the join, otherwise we spill the probe row. Similar to step one, the
  //      rows can come from child(0) or a spilled partition.
  //   3. Read and construct a single spilled partition. In this case we're walking a
  //      spilled partition and the hash table fits in memory. Neither the build nor probe
  //      side need to be partitioned and we just perform the join.
  //
  // States:
  // The transition goes from PARTITIONING_BUILD -> PROCESSING_LEFT_CHILD ->
  //    PROBING_SPILLED_PARTITION/REPARTITIONING.
  // The last two steps will switch back and forth as many times as we need to
  // repartition.
  enum State {
    // Partitioning the build child's input. Corresponds to mode 1 above but
    // only when consuming from child(1).
    PARTITIONING_BUILD,

    // Processing the left child input. Corresponds to mode 2 but only when consuming
    // child(0).
    PROCESSING_LEFT_CHILD,

    // Probing a spilled partition. The hash table for this partition fits in memory.
    // Corresponds to mode 3.
    PROBING_SPILLED_PARTITION,

    // Repartitioning a single spilled partition (input_partition_) into
    // hash_partitions_.
    // Corresponds to mode 1 & 2 but reading from a spilled partition.
    REPARTITIONING,
  };

  MemTracker* join_node_mem_tracker() { return join_node_mem_tracker_.get(); }

  // Append the row to stream. In the common case, the row is just in memory. If we
  // run out of memory, this will spill a partition and try to add the row again.
  // returns true if the row was added and false otherwise. If false is returned,
  // status_ contains the error (doesn't return status because this is very perf
  // sensitive).
  bool AppendRow(BufferedTupleStream* stream, TupleRow* row);

  // Called when we need to free up memory by spilling 1 or more partitions.
  // This function walks hash_partitions_ and picks on to spill.
  Status SpillPartitions();

  // Partitions the entire build input (either from child(1) or input_partition_) into
  // hash_partitions_. When this call returns, hash_partitions_ is ready to consume
  // the probe input.
  Status ProcessBuildInput(RuntimeState* state);

  // Processes all the build rows by partitioning them.
  // Reads the rows in build_batch and partition them in hash_partitions_. Partition level
  // is how many times the rows have been partitioned. We need to use a different
  // hash function/bits for each level.
  Status ProcessBuildBatch(RowBatch* build_batch, int partition_level);

  // Call at the end of partitioning the build rows (which could be from the build child
  // or from repartitioning an existing partition). After this function returns, all
  // partitions in hash_partitions_ are ready to accept probe rows. This function
  // constructs hash tables for as many partitions as fit in memory (which can be none).
  // For the remaining partitions, this function initializes the probe spilling
  // structures.
  Status BuildHashTables(RuntimeState* state);

  // Call at the end of consuming the probe rows. Walks hash_partitions_ and
  //  - If this partition had a hash table, close it. This partition is fully processed
  //    on both the build and probe sides.
  //  - If this partition did not have a hash table, meaning both sides were spilled,
  //    move the partition to spilled_partitions_.
  Status CleanUpHashPartitions();

  // Process probe rows from probe_batch_. Returns either if out_batch is full or
  // probe_batch_ is entirely consumed.
  template<int const JoinOp>
  Status ProcessProbeBatch(RowBatch* out_batch);

  // Get the next row batch from child(0). If we are done consuming the input,
  // sets probe_batch_pos_ to -1, otherwise, sets it to 0.
  Status NextLeftChildRowBatch(RuntimeState*, RowBatch* out_batch);

  // Get the next row batch from input_partition_.
  // If we are done consuming the input, sets probe_batch_pos_ to -1,
  // otherwise, sets it to 0.
  Status NextSpilledRowBatch(RuntimeState*, RowBatch* out_batch);

  // Moves onto the next spilled partition and initializes input_partition_. This
  // function processes the entire build side of input_partition_ and when this function
  // returns, we are ready to consume the probe side of input_partition_.
  // If the build side's hash table fits in memory, we will construct input_partition_'s
  // hash table. If it does not, meaning we need to repartition, this function will
  // initialize hash_partitions_.
  Status PrepareNextPartition(RuntimeState*);

  // Returns the current state of the partition as a string.
  std::string PrintState() const;

  // Updates state_ to 's', logging the transition.
  void UpdateState(State s);

  std::string DebugString() const;

  // our equi-join predicates "<lhs> = <rhs>" are separated into
  // build_exprs_ (over child(1)) and probe_exprs_ (over child(0))
  std::vector<Expr*> probe_exprs_;
  std::vector<Expr*> build_exprs_;

  // non-equi-join conjuncts from the JOIN clause
  std::vector<Expr*> other_join_conjuncts_;

  // State of the algorithm. Used just for debugging.
  State state_;
  Status status_;

  // TODO: stop gap to limit the memory this join node can use. Set by debugging query
  // option. This should be replaced by having a quota on a shared BufferedBlockMgr.
  boost::scoped_ptr<MemTracker> join_node_mem_tracker_;

  // Client to the buffered block mgr.
  BufferedBlockMgr::Client* client_;

  // TODO: this has to go. This is only used to hash the current row to figure out
  // which partition the row belongs to (and then uses the hash table in that partition).
  // We need to split out the row hashing functionality from the HashTable.
  boost::scoped_ptr<HashTable> hash_tbl_;

  // The iterator that corresponds to the look up of current_probe_row_.
  HashTable::Iterator hash_tbl_iterator_;

  class Partition {
   public:
    Partition(RuntimeState* state, PartitionedHashJoinNode* parent, int level);
    ~Partition();

    BufferedTupleStream* build_rows() { return build_rows_.get(); }
    BufferedTupleStream* probe_rows() { return probe_rows_.get(); }
    HashTable* hash_tbl() { return hash_tbl_.get(); }

    bool is_closed() const { return is_closed_; }

    // Must be called once per partition to release any resources. This should be called
    // as soon as possible to release memory as quickly as possible.
    void Close();

    // Returns the estimated size of the in memory size for the build side of this
    // partition. This includes the entire build side and the hash table.
    int64_t EstimatedInMemSize() const;

    // Returns the actual size of the in memory build side. Only valid to call on
    // partitions after BuildPartition()
    int64_t InMemSize() const;

    // Pins the build tuples for this partition and constructs the hash_tbl_ from it.
    // Build rows cannot be added after calling this.
    // If the partition could not be built due to memory pressure, *built is set to false.
    Status BuildHashTable(RuntimeState* state, bool* built);

   private:
    friend class PartitionedHashJoinNode;

    PartitionedHashJoinNode* parent_;

    // This partition is completely processed and nothing needs to be done for it again.
    // All resources associated with this partition are returned.
    bool is_closed_;

    // How many times rows in this partition have been repartitioned. Partitions created
    // from the node's children's input is level 0, 1 after the first repartitionining,
    // etc.
    int level_;

    // The hash table for this partition.
    boost::scoped_ptr<HashTable> hash_tbl_;

    // Stream of build/probe tuples in this partition.
    boost::scoped_ptr<BufferedTupleStream> build_rows_;
    boost::scoped_ptr<BufferedTupleStream> probe_rows_;
  };

  // The list of partitions that have been spilled on both sides and still need more
  // processing. These partitions could need repartitioning, in which cases more
  // partitions will be added to this list after repartitioning.
  std::list<Partition*> spilled_partitions_;

  // The current set of partitions that are being built. This is only used in
  // mode 1 and 2 when we need to partition the build and probe inputs.
  // This is not used when processing a single partition.
  std::vector<Partition*> hash_partitions_;

  // The current input partition to be processed (not in spilled_partitions_).
  // This partition can either serve as the source for a repartitioning step, or
  // if the hash table fits in memory, the source of the probe rows.
  Partition* input_partition_;
};

}

#endif
