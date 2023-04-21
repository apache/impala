// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_EXEC_BLOCKING_JOIN_NODE_H
#define IMPALA_EXEC_BLOCKING_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <string>

#include "exec/exec-node.h"
#include "exec/join-op.h"
#include "util/promise.h"
#include "util/stopwatch.h"

namespace impala {

class JoinBuilder;
class RowBatch;
class TupleRow;

class BlockingJoinPlanNode : public PlanNode {
 public:
  /// Subclasses should call BlockingJoinNode::Init() and then perform any other Init()
  /// work, e.g. creating expr trees.
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override = 0;

  /// Returns true if this join node will use a separate builder that is the root sink
  /// of a different fragment. Otherwise the builder is owned by this node and consumes
  /// input from the second child node.
  /// Note: This depends on the containing subplan being initialized, and isn't accurate
  /// until the whole PlanNode tree has been initialized.
  bool UseSeparateBuild(const TQueryOptions& query_options) const {
    return !IsInSubplan() && query_options.num_nodes != 1 && is_mt_fragment();
  }

  TJoinOp::type join_op() const { return join_op_; }

  const RowDescriptor& probe_row_desc() const { return *children_[0]->row_descriptor_; }
  const RowDescriptor& build_row_desc() const {
    DCHECK(build_row_desc_ != nullptr);
    return *build_row_desc_;
  }

 protected:
  TJoinOp::type join_op_;

  /// This is the same as the RowDescriptor of the build sink, if the join build is
  /// separate, or the right child, if the join build is integrated into the node.
  /// Owned by RuntimeState's object pool.
  RowDescriptor* build_row_desc_ = nullptr;
};

/// Abstract base class for join nodes that block in Open() until all rows from the
/// right input plan tree have been processed.
///
/// BlockingJoinNode and JoinBuilder subclasses interact together to implement a blocking
/// join: the builder. Two modes are supported: an integrated join build, where the
/// JoinBuilder is owned by the BlockingJoinNode, and a separate join build, where the
/// JoinBuilder is owned by a separate build fragment co-located in the same Impala
/// daemon, and the join node synchronizes with the builder to access build-side data
/// structures.
///
/// TODO: Remove the restriction that the tuples in the join's output row have to
/// correspond to the order of its child exec nodes. See the DCHECKs in Init().

class BlockingJoinNode : public ExecNode {
 public:
  BlockingJoinNode(const std::string& node_name, ObjectPool* pool,
      const BlockingJoinPlanNode& pnode, const DescriptorTbl& descs);

  virtual ~BlockingJoinNode();

  /// Subclasses should call BlockingJoinNode::Prepare() and then perform any other
  /// Prepare() work, e.g. codegen.
  virtual Status Prepare(RuntimeState* state);

  /// Helper called by subclass's Open() implementation.
  /// Calls ExecNode::Open() and initializes 'eos_' and 'probe_side_eos_'.
  /// If the join build is separate, the join builder is returned in *separate_builder.
  Status OpenImpl(RuntimeState* state, JoinBuilder** separate_builder);

  /// Transfers resources from 'probe_batch_' to 'row_batch'.
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch);

  /// Subclasses should close any other structures and then call
  /// BlockingJoinNode::Close().
  virtual void Close(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

 protected:
  const std::string node_name_;
  TJoinOp::type join_op_;

  /// True if OpenImpl() was called.
  bool open_called_ = false;

  /// True if this join node has called WaitForInitialBuild() on the corresponding
  /// separate join builder. This means that CloseFromProbe() needs to be called
  /// on the builder.
  bool waited_for_build_ = false;

  /// Store in node to avoid reallocating. Cleared after build completes.
  boost::scoped_ptr<RowBatch> build_batch_;

  /// probe_batch_ must be cleared before calling GetNext().  The child node
  /// does not initialize all tuple ptrs in the row, only the ones that it
  /// is responsible for.
  boost::scoped_ptr<RowBatch> probe_batch_;

  bool eos_;  // if true, nothing left to return in GetNext()
  bool probe_side_eos_;  // if true, left child has no more rows to process

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

  RuntimeProfile::Counter* probe_timer_;   // time to process the probe (left child) batch
  RuntimeProfile::Counter* probe_row_counter_;   // num probe (left child) rows

  /// Stopwatch that measures the build child's Open/GetNext time that overlaps
  /// with the probe child Open(). Not used for separate join builds.
  ConcurrentStopWatch built_probe_overlap_stop_watch_;

  // True for a join node subclass if the build side can be closed before the probe
  // side is opened. Should be true wherever possible to reduce resource consumption.
  // E.g. this is true or PartitionedHashJoinNode because it rematerializes the build rows
  // and false for NestedLoopJoinNode because it accumulates RowBatches that may reference
  // memory still owned by the build-side ExecNode tree.
  // Changes here must be kept in sync with the planner's resource profile computation.
  // TODO: IMPALA-4179: this should always be true once resource transfer has been fixed.
  virtual bool CanCloseBuildEarly() const { return false; }

  /// Acquire resources for this ExecNode required for the build phase.
  /// Called by BlockingJoinNode after opening child(1) succeeds and before
  /// this node either waits for the separate build or calls SendBuildInputToSink().
  virtual Status AcquireResourcesForBuild(RuntimeState* state) { return Status::OK(); }

  /// Processes the build-side input, which should be already open, by sending it to
  /// 'build_sink', and opens the probe side. Will do both concurrently if not in a
  /// subplan and an extra thread token is available.
  Status ProcessBuildInputAndOpenProbe(RuntimeState* state, JoinBuilder* build_sink);

  /// Set up 'current_probe_row_' to point to the first input row from the left child
  /// (probe side). Fills 'probe_batch_' with rows from the left child and updates
  /// 'probe_batch_pos_' to the index of the row in 'probe_batch_' after
  /// 'current_probe_row_'. 'probe_side_eos_' is set to true if 'probe_batch_' is the
  /// last batch to be returned from the child.
  /// If eos of the left child is reached and no rows are returned, 'current_probe_row_'
  /// is set to NULL and 'eos_' is set to true for join modes where unmatched rows from
  /// the build side do not need to be returned.
  Status GetFirstProbeRow(RuntimeState* state);

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
  inline void CreateOutputRow(
      TupleRow* out_row, TupleRow* probe_row, TupleRow* build_row);

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
      const ConcurrentStopWatch* child_overlap_timer);

  const BlockingJoinPlanNode& plan_node() const {
    return static_cast<const BlockingJoinPlanNode&>(plan_node_);
  }

  /// Returns true if this join node is using a separate builder that is the root sink
  /// of a different fragment. Otherwise the builder is owned by this node and consumes
  /// input from the second child node.
  bool UseSeparateBuild(const TQueryOptions& query_options) const {
    return plan_node().UseSeparateBuild(query_options);
  }

  const RowDescriptor& probe_row_desc() const {
    return plan_node().probe_row_desc();
  }

  const RowDescriptor& build_row_desc() const {
    return plan_node().build_row_desc();
  }

 private:
  /// Helper function to process the build input by sending it to the integrated
  /// JoinBuilder. The build input must already be open before calling this. ASYNC_BUILD
  /// enables timers that impose some overhead but are required if the build is processed
  /// concurrently with the Open() of the left child.
  template <bool ASYNC_BUILD>
  Status SendBuildInputToSink(RuntimeState* state, JoinBuilder* build_sink);

  /// The main function for the thread that opens the build side and processes the build
  /// input asynchronously.  Its status is returned in the 'status' promise. If
  /// 'build_sink' is non-NULL, it is used for the build. Otherwise, ProcessBuildInput()
  /// is called on the subclass.
  void ProcessBuildInputAsync(
      RuntimeState* state, JoinBuilder* build_sink, Status* status);
};
} // namespace impala

#endif
