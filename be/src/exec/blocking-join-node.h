// Copyright 2013 Cloudera Inc.
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


#ifndef IMPALA_EXEC_BLOCKING_JOIN_NODE_H
#define IMPALA_EXEC_BLOCKING_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <string>

#include "exec/exec-node.h"
#include "util/promise.h"

#include "gen-cpp/PlanNodes_types.h"  // for TJoinOp

namespace impala {

class MemPool;
class RowBatch;
class TupleRow;

/// Abstract base class for join nodes that block while consuming all rows from their
/// right child in Open(). There is no implementation of Reset() because the Open()
/// sufficiently covers setting members into a 'reset' state.
class BlockingJoinNode : public ExecNode {
 public:
  BlockingJoinNode(const std::string& node_name, const TJoinOp::type join_op,
      ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual ~BlockingJoinNode();

  /// Subclasses should call BlockingJoinNode::Init() and then perform any other Init()
  /// work, e.g. creating expr trees.
  virtual Status Init(const TPlanNode& tnode);

  /// Subclasses should call BlockingJoinNode::Prepare() and then perform any other
  /// Prepare() work, e.g. codegen.
  virtual Status Prepare(RuntimeState* state);

  /// Open prepares the build side structures (subclasses should implement
  /// ConstructBuildSide()) and then prepares for GetNext with the first left child row
  /// (subclasses should implement InitGetNext()).
  virtual Status Open(RuntimeState* state);

  /// Subclasses should close any other structures and then call
  /// BlockingJoinNode::Close().
  virtual void Close(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

 protected:
  const std::string node_name_;
  TJoinOp::type join_op_;

  boost::scoped_ptr<MemPool> build_pool_;  // holds everything referenced from build side

  /// probe_batch_ must be cleared before calling GetNext().  The child node
  /// does not initialize all tuple ptrs in the row, only the ones that it
  /// is responsible for.
  boost::scoped_ptr<RowBatch> probe_batch_;

  bool eos_;  // if true, nothing left to return in GetNext()
  bool probe_side_eos_;  // if true, left child has no more rows to process

  /// TODO: These variables should move to a join control block struct, which is local to
  /// each probing thread.
  int probe_batch_pos_;  // current scan pos in probe_batch_
  TupleRow* current_probe_row_;  // The row currently being probed
  bool matched_probe_;  // if true, the current probe row is matched

  /// Size of the TupleRow (just the Tuple ptrs) from the build (right) and probe (left)
  /// sides. Set to zero if the build/probe tuples are not returned, e.g., for semi joins.
  /// Cached because it is used in the hot path.
  int probe_tuple_row_size_;
  int build_tuple_row_size_;

  /// Row assembled from all lhs and rhs tuples used for evaluating the non-equi-join
  /// conjuncts for semi joins. Semi joins only return the lhs or rhs output tuples,
  /// so this tuple is temporarily assembled for evaluating the conjuncts.
  TupleRow* semi_join_staging_row_;

  /// If true, this node can add filters to the probe (left child) node after processing
  /// the entire build side.
  /// Note that we disable probe filters if we are inside a subplan.
  bool can_add_probe_filters_;

  RuntimeProfile::Counter* build_timer_;   // time to prepare build side
  RuntimeProfile::Counter* probe_timer_;   // time to process the probe (left child) batch
  RuntimeProfile::Counter* build_row_counter_;   // num build rows
  RuntimeProfile::Counter* probe_row_counter_;   // num probe (left child) rows

  /// Stopwatch that measures the build child's Open/GetNext time that overlaps
  /// with the probe child Open().
  MonotonicStopWatch built_probe_overlap_stop_watch_;

  /// Init the build-side state for a new left child row (e.g. hash table iterator or list
  /// iterator) given the first row. Used in Open() to prepare for GetNext().
  /// A NULL ptr for first_left_child_row indicates the left child eos.
  virtual Status InitGetNext(TupleRow* first_left_child_row) = 0;

  /// We parallelize building the build-side with Open'ing the left child. If, for example,
  /// the left child is another join node, it can start to build its own build-side at the
  /// same time.
  virtual Status ConstructBuildSide(RuntimeState* state) = 0;

  /// Gives subclasses an opportunity to add debug output to the debug string printed by
  /// DebugString().
  virtual void AddToDebugString(int indentation_level, std::stringstream* out) const {
  }

  /// Subclasses should not override, use AddToDebugString() to add to the result.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

  /// Returns a debug string for the left child's 'row'. They have tuple ptrs that are
  /// uninitialized; the left child only populates the tuple ptrs it is responsible
  /// for.  This function outputs just the row values and leaves the build
  /// side values as NULL.
  /// This is only used for debugging and outputting the left child rows before
  /// doing the join.
  std::string GetLeftChildRowString(TupleRow* row);

  /// Write combined row, consisting of the left child's 'probe_row' and right child's
  /// 'build_row' to 'out_row'.
  /// This is replaced by codegen.
  void CreateOutputRow(TupleRow* out_row, TupleRow* probe_row, TupleRow* build_row);

  /// Returns true if the join needs to process unmatched build rows, false
  /// otherwise.
  bool NeedToProcessUnmatchedBuildRows() {
    return join_op_ == TJoinOp::RIGHT_ANTI_JOIN ||
        join_op_ == TJoinOp::RIGHT_OUTER_JOIN ||
        join_op_ == TJoinOp::FULL_OUTER_JOIN;
  }

  /// This function calculates the "local time" spent in the join node.
  ///
  /// The definition of "local time" is the wall clock time where this exec node is
  /// processing and it is not blocked by any of its children.
  ///
  /// The join node has two execution models:
  ///   1. The entire join execution is in a single thread.
  ///   2. The build(right) side is executed on a different thread while the main thread
  ///      opens the probe(left) side.
  ///
  /// In case 1, the "local time" spent in this node is as simple as:
  ///     total_time - left child time - right child time
  /// Because the entire right child time blocks the execution, the right child time is
  /// the same as right_child_blocking_stop_watch_.
  ///
  /// Case 2 is more complicated. The build thread is started first and then
  /// the main thread will "open" the left child. When the left child is ready
  /// (i.e. Open() returned), the main thread will wait for the build thread to finish.
  /// Because the left child is always executed in the main thread, all the left child
  /// time should not be counted towards the hash join "local time".
  /// For the right child (the build side), the child time in the build thread up to the
  /// point when the left child Open() returns should not be counted towards the hash
  /// join local time. This time period completely overlaps with the left child time.
  /// From the time when left child Open() returned, the right child time should be
  /// removed from the total time because this is the only child that is blocking the
  /// join execution.
  ///
  /// Here's the calculation:
  ///   total_time - left child time - (right child time - overlapped period)
  ///
  /// The "overlapped period" is measured by built_probe_overlap_stop_watch_. Using this
  /// overlap method, both children's "Prepare" time are also excluded.
  static int64_t LocalTimeCounterFn(const RuntimeProfile::Counter* total_time,
      const RuntimeProfile::Counter* left_child_time,
      const RuntimeProfile::Counter* right_child_time,
      const MonotonicStopWatch* child_overlap_timer);

 private:
  /// Supervises ConstructBuildSide in a separate thread, and returns its status in the
  /// promise parameter.
  void BuildSideThread(RuntimeState* state, Promise<Status>* status);
};

}

#endif
