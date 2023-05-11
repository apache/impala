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

#pragma once

#include <mutex>

#include "exec/data-sink.h"
#include "exec/filter-context.h"
#include "runtime/runtime-state.h"
#include "util/condition-variable.h"

namespace impala {

class NljBuilder;
class PhjBuilder;
class IcebergDeleteBuilder;

class JoinBuilderConfig : public DataSinkConfig {
 public:
  ~JoinBuilderConfig() override {}

 protected:
  friend class JoinBuilder;
  friend class NljBuilder;
  friend class PhjBuilder;
  friend class IcebergDeleteBuilder;

  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;

  /// The ID of the join plan node this is associated with.
  int join_node_id_;

  /// The join operation this is building for.
  TJoinOp::type join_op_;
};

/// Join builder for use with BlockingJoinNode.
///
/// Implements the DataSink interface but also exposes some methods for direct use by
/// BlockingJoinNode, e.g. the implemention of the protocol to hand off completed
/// builds to the join node when the plan has a separate join fragment.
///
/// Example of Synchronization
/// --------------------------
/// The below sequence diagram shows an example of how the probe and build finstance
/// threads synchronize to do the nested loop join build, hand it off to the probe thread
/// and then release resources at the end. For simplicitly, this particular sequence
/// assumes that Prepare() was already called on the build finstance thread so
/// GetFInstanceState() returns immediately (otherwise Open() would block waiting for
/// the build finstance to be prepared).
///
///
/// +------------------+ +--------------------+       +-------------+  +-----------------+
/// | probe finstance  | | NestedLoopJoinNode |       | NljBuilder  |  | build finstance |
/// | thread           | |                    |       |             |  | thread          |
/// +------------------+ +--------------------+       +-------------+  +-----------------+
///         |             |                              |                             |
///         | Open()      |                              |                             |
///         |------------>|                              |                             |
///         |             |                              |                      Open() |
///         |             |                              |<----------------------------|
///         |             |                              |       Send() multiple times |
///         |             |                              |<----------------------------|
///         |             | WaitForInitialBuild()        |                             |
///         |             | blocks on probe_wakeup_cv_   |                             |
///         |             |----------------------------->|                             |
///         |             |                              |       Send() multiple times |
///         |             |                              |<----------------------------|
///         |             |                              | FlushFinal()                |
///         |             |                              | calls HandoffToProbesAndWait()
///         |             |                              | signals probe_wakeup_cv_    |
///         |             |                              | blocks on build_wakeup_cv_  |
///         |             |                              |<----------------------------|
///         |             | WaitForInitialBuild() returns|                             |
///         |             |<-----------------------------|                             |
///         |             |                              |                             |
///         |             | accesses data structures in  |                             |
///         |             | read-only manner             |                             |
///         |             |----------------------------->|                             |
///         |             |                              |                             |
///         |             | CloseFromProbe()             |                             |
///         |             | signals build_wakeup_cv_     |                             |
///         |             |----------------------------->|                             |
///         |             |                              | FlushFinal() returns        |
///         |             |                              |---------------------------->|
///         |             |                              |                             |
///         |             |                              | Close()                     |
///         |             |                              |<----------------------------|
///
/// Various alternative flows are possible:
/// * WaitForInitialBuild() may be called after FlushFinal(), in which case it can return
///   immediately.
/// * The query may be cancelled while the threads are blocked in WaitForInitialBuild()
///   or HandoffToProbesAndWait(). In this case the threads need to get unblocked in a
///   safe manner. Details are documented in the respective functions.
///
/// Other implementations, such as the partitioned hash join, may do additional
/// synchronization in between WaitForInitialBuild() returning and CloseFromProbe() being
/// called, instead of the simple read-only access of the NLJ. The details are are left
/// to the specific implementations, but must be thread-safe if the builder is shared
/// between multiple threads.

class JoinBuilder : public DataSink {
 public:
  /// Construct the join builder. This is a separate build if 'sink_id' is valid, or
  /// an integrated build if 'sink_id' is -1.
  JoinBuilder(TDataSinkId sink_id, const JoinBuilderConfig& sink_config,
      const string& name, RuntimeState* state);
  virtual ~JoinBuilder();

  /// Waits for a separate join build to complete and be ready for probing. Always called
  /// from a BlockingJoinNode subclass in a different fragment instance.
  ///
  /// Returns OK if the initial build succeeded. Returns CANCELLED if the join node's
  /// fragment is cancelled. If an error occurs executing the build side of the join,
  /// we rely on query-wide cancellation to cancel the probe side of the join, instead
  /// of propagating the error/cancellation via the JoinBuilder. In case of cancellation
  /// or error, this function blocks until the join node's finstance is cancelled so
  /// that the CANCELLED status does not race with the root cause of the query failure.
  Status WaitForInitialBuild(RuntimeState* join_node_state);

  /// This is called from BlockingJoinNode to signal that the node is done with the
  /// builder. If this is an embedded join build, this simply closes the builder. If it is
  /// a separate join build, each BlockingJoinNode that called WaitForInitialBuild() needs
  /// to call CloseFromProbe(). The builder is closed when the last BlockingJoinNode using
  /// this builder calls CloseFromProbe(). BlockingJoinNode never calls Close() directly.
  void CloseFromProbe(RuntimeState* join_node_state);

  int num_probe_threads() const { return num_probe_threads_; }

  static string ConstructBuilderName(const char* name, int join_node_id) {
    return strings::Substitute("$0 Join Builder (join_node_id=$1)", name, join_node_id);
  }

 protected:
  /// ID of the join node that this builder is associated with.
  const int join_node_id_;

  /// The join operation this is building for.
  const TJoinOp::type join_op_;

  /// True if this is a separate DataSink at the root of its own fragment. Otherwise this
  /// is embedded in a PartitionedHashJoinNode.
  const bool is_separate_build_;


  /// Number of build rows. Initialized in Prepare().
  RuntimeProfile::Counter* num_build_rows_ = nullptr;

  /////////////////////////////////////////////////////////////////////
  /// BEGIN: Members that are used only when is_separate_build_ is true

  // Lock used for synchronization between threads from the build and probe side (i.e.
  // the build fragment thread and the probe-side thread executing the join node).
  std::mutex separate_build_lock_;

  // Probe-side threads block on this while waiting for initial_build_complete_ = true
  // (or for probe finstance cancellation).
  // Protected by 'separate_build_lock_'.
  ConditionVariable probe_wakeup_cv_;

  // The build-side thread blocks on this while waiting for 'outstanding_probes_' and
  // 'probe_refcount_' to go to zero (or for build finstance cancellation).
  // Protected by 'separate_build_lock_'.
  ConditionVariable build_wakeup_cv_;

  // Set to true when the builder is ready for the probe side to use. Set to true in
  // FlushFinal().
  // Protected by 'separate_build_lock_'.
  bool ready_to_probe_ = false;

  // Total number of probe-side fragment instances, i.e. threads.
  // Always 1 if 'is_separate_build_' is false.
  const int num_probe_threads_;

  // Number of probe-side threads that are expected to call WaitForInitialBuild()
  // but have not yet called it.
  // Protected by 'separate_build_lock_'.
  int outstanding_probes_ = 0;

  // Number of probe-side threads using this builder - i.e. WaitForInitialBuild() calls
  // without a corresponding CloseBuilder() call.
  // Protected by 'separate_build_lock_'.
  int probe_refcount_ = 0;

  /// END: Members that are used only when is_separate_build_ is true
  /////////////////////////////////////////////////////////////////////

  /// Called by the build-side thread of a separate join build once the initial build has
  /// been completed and it is ready to hand off to probe-side threads. Blocks until the
  /// probe side is finished using the build, or the build finstance is cancelled. When
  /// this returns, no probe-side threads will be using the build and
  /// WaitForInitialBuild() will return CANCELLED.
  ///
  /// This should be called by the FlushFinal() method of the subclass, after all other
  /// resources from the build fragment have been released.
  /// TODO: IMPALA-9255: reconsider this so that the build-side thread can exit instead
  /// of being blocked indefinitely.
  void HandoffToProbesAndWait(RuntimeState* build_side_state);

  /// Publish the runtime filters as described in 'filter_ctxs' to the fragment-local
  /// RuntimeFilterBank in 'runtime_state'. 'minmax_filter_threshold' specifies the
  /// threshold to determine the usefulness of a min/max filter. 'num_build_rows' is used
  /// to determine whether the computed filters have an unacceptably high false-positive
  /// rate.
  void PublishRuntimeFilters(const std::vector<FilterContext>& filter_ctxs,
      RuntimeState* runtime_state, float minmax_filter_threshold, int64_t num_build_rows);
};
}
