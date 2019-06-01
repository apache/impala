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


#ifndef IMPALA_EXEC_EXEC_NODE_H
#define IMPALA_EXEC_EXEC_NODE_H

#include <memory>
#include <sstream>
#include <vector>

#include "common/status.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gutil/threading/thread_collision_warner.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/descriptors.h" // for RowDescriptor
#include "runtime/reservation-manager.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"

namespace impala {

class DataSink;
class MemPool;
class MemTracker;
class ObjectPool;
class RowBatch;
class RuntimeState;
class ScalarExpr;
class SubplanNode;
class TPlan;
class TupleRow;
class TDebugOptions;

/// Superclass of all execution nodes.
///
/// All subclasses need to make sure to check RuntimeState::is_cancelled()
/// periodically in order to ensure timely termination after the cancellation
/// flag gets set.
/// TODO: Move static state of ExecNode into PlanNode, of which there is one instance
/// per fragment. ExecNode contains only runtime state and there can be up to MT_DOP
/// instances of it per fragment.
class ExecNode {
 public:
  /// Init conjuncts.
  ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual ~ExecNode();

  /// Initializes this object from the thrift tnode desc. The subclass should
  /// do any initialization that can fail in Init() rather than the ctor.
  /// If overridden in subclass, must first call superclass's Init().
  virtual Status Init(const TPlanNode& tnode, RuntimeState* state) WARN_UNUSED_RESULT;

  /// Sets up internal structures, etc., without doing any actual work.
  /// Must be called prior to Open(). Will only be called once in this
  /// node's lifetime.
  /// If overridden in subclass, must first call superclass's Prepare().
  virtual Status Prepare(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Recursively calls Codegen() on all children.
  /// Expected to be overriden in subclass to generate LLVM IR functions and register
  /// them with the LlvmCodeGen object. The function pointers of the compiled IR functions
  /// will be set up in PlanFragmentExecutor::Open(). If overridden in subclass, must also
  /// call superclass's Codegen() before or after the code generation for this exec node.
  /// Will only be called once in the node's lifetime.
  virtual void Codegen(RuntimeState* state);

  /// Performs any preparatory work prior to calling GetNext().
  /// If overridden in subclass, must first call superclass's Open().
  /// Open() is called after Prepare() or Reset(), i.e., possibly multiple times
  /// throughout the lifetime of this node.
  ///
  /// Memory resources must be acquired by an ExecNode only during or after the first
  /// call to Open(). Blocking ExecNodes outside of a subplan must call Open() on their
  /// child before acquiring their own resources to reduce the peak resource requirement.
  /// This is particularly important if there are multiple blocking ExecNodes in a
  /// pipeline because the lower nodes will release resources in Close() before the
  /// Open() of their parent returns. The resource profile calculation in the frontend
  /// relies on this when computing the peak resources required for a query.
  virtual Status Open(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Retrieves rows and returns them via row_batch. Sets eos to true
  /// if subsequent calls will not retrieve any more rows.
  /// Data referenced by any tuples returned in row_batch must not be overwritten
  /// by the callee until Close() is called. The memory holding that data
  /// can be returned via row_batch's tuple_data_pool (in which case it may be deleted
  /// by the caller) or held on to by the callee. The row_batch, including its
  /// tuple_data_pool, will be destroyed by the caller at some point prior to the final
  /// Close() call.
  /// In other words, if the memory holding the tuple data will be referenced
  /// by the callee in subsequent GetNext() calls, it must *not* be attached to the
  /// row_batch's tuple_data_pool.
  virtual Status GetNext(
      RuntimeState* state, RowBatch* row_batch, bool* eos) WARN_UNUSED_RESULT = 0;

  /// Resets the stream of row batches to be retrieved by subsequent GetNext() calls.
  /// Clears all internal state, returning this node to the state it was in after calling
  /// Prepare() and before calling Open(). This function must not clear memory
  /// still owned by this node that is backing rows returned in GetNext(). 'row_batch' can
  /// be used to transfer ownership of any such memory.
  /// Prepare() and Open() must have already been called before calling Reset().
  /// GetNext() may have optionally been called (not necessarily until eos).
  /// Close() must not have been called.
  /// Reset() is not idempotent. Calling it multiple times in a row without a preceding
  /// call to Open() is invalid.
  /// If overridden in a subclass, must call superclass's Reset() at the end. The default
  /// implementation calls Reset() on children.
  /// Note that this function may be called many times (proportional to the input data),
  /// so should be fast.
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Close() will get called for every exec node, regardless of what else is called and
  /// the status of these calls (i.e. Prepare() may never have been called, or
  /// Prepare()/Open()/GetNext() returned with an error).
  /// Close() releases all resources that were allocated in Open()/GetNext(), even if the
  /// latter ended with an error. Close() can be called if the node has been prepared or
  /// the node is closed.
  /// The default implementation updates runtime profile counters and calls
  /// Close() on the children. Subclasses should check if the node has already been
  /// closed (is_closed()), then close themselves, then call the base Close().
  /// Nodes that are using tuples returned by a child may call Close() on their children
  /// before their own Close() if the child node has returned eos.
  /// It is only safe to call Close() on the child node while the parent node is still
  /// returning rows if the parent node fully materializes the child's input.
  virtual void Close(RuntimeState* state);

  /// Creates exec node tree from list of nodes contained in plan via depth-first
  /// traversal. All nodes are placed in state->obj_pool() and have Init() called on them.
  /// Returns error if 'plan' is corrupted, otherwise success.
  static Status CreateTree(RuntimeState* state, const TPlan& plan,
      const DescriptorTbl& descs, ExecNode** root) WARN_UNUSED_RESULT;

  /// Set debug action in 'tree' according to debug_options.
  static void SetDebugOptions(const TDebugOptions& debug_options, ExecNode* tree);

  /// Collect all nodes of given 'node_type' that are part of this subtree, and return in
  /// 'nodes'.
  void CollectNodes(TPlanNodeType::type node_type, std::vector<ExecNode*>* nodes);

  /// Collect all scan node types.
  void CollectScanNodes(std::vector<ExecNode*>* nodes);

  /// Evaluates the predicate in 'eval' over 'row' and returns the result.
  static bool EvalPredicate(ScalarExprEvaluator* eval, TupleRow* row);

  /// Evaluate the conjuncts in 'evaluators' over 'row'.
  /// Returns true if all exprs return true.
  static bool EvalConjuncts(
      ScalarExprEvaluator* const* evals, int num_conjuncts, TupleRow* row);

  /// Codegen EvalConjuncts(). Returns a non-OK status if the function couldn't be
  /// codegen'd. The codegen'd version uses inlined, codegen'd GetBooleanVal() functions.
  static Status CodegenEvalConjuncts(LlvmCodeGen* codegen,
      const std::vector<ScalarExpr*>& conjuncts, llvm::Function** fn,
      const char* name = "EvalConjuncts") WARN_UNUSED_RESULT;

  /// Returns a string representation in DFS order of the plan rooted at this.
  std::string DebugString() const;

  /// Recursive helper method for generating a string for DebugString().
  /// Implementations should call DebugString(int, std::stringstream) on their children.
  /// Input parameters:
  ///   indentation_level: Current level in plan tree.
  /// Output parameters:
  ///   out: Stream to accumulate debug string.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

  const std::vector<ScalarExpr*>& conjuncts() const { return conjuncts_; }

  const std::vector<ScalarExprEvaluator*>& conjunct_evals() const {
    return conjunct_evals_;
  }

  int id() const { return id_; }
  TPlanNodeType::type type() const { return type_; }

  /// Returns the row descriptor for rows produced by this node. The RowDescriptor is
  /// constant for the lifetime of the fragment instance, and so is shared by reference
  /// across the plan tree, including in RowBatches. The lifetime of the descriptor is the
  /// same as the lifetime of this node.
  const RowDescriptor* row_desc() const { return &row_descriptor_; }

  ExecNode* child(int i) { return children_[i]; }
  int num_children() const { return children_.size(); }
  SubplanNode* get_containing_subplan() const { return containing_subplan_; }
  void set_containing_subplan(SubplanNode* sp) {
    DCHECK(containing_subplan_ == NULL);
    containing_subplan_ = sp;
  }

  int64_t limit() const { return limit_; }

  /// Returns the number of rows returned by this Node.
  int64_t rows_returned() const {
    DCHECK(getExecutionModel() != NON_TASK_BASED_SYNC);
    return num_rows_returned_;
  }

  /// Returns the number of rows returned by this Node. Thread safe version of
  /// rows_returned().
  /// TODO: The thread safe versions can be removed if we remove the legacy multi-threaded
  /// scan nodes.
  int64_t rows_returned_shared() const {
    // Ideally this function should only be called when num_rows_returned_ is shared by
    // multiple threads. Both the HdfsScanNodeMt and KuduScanNodeMt call this function
    // from the scanner code-path for simplicity.
    DCHECK(
        getExecutionModel() == NON_TASK_BASED_SYNC || getExecutionModel() == TASK_BASED);
    return base::subtle::Acquire_Load(&num_rows_returned_);
  }

  /// Returns true if a valid limit is set and number of rows returned by this node has
  /// exceeded the limit.
  bool ReachedLimit() {
    DCHECK(getExecutionModel() != NON_TASK_BASED_SYNC);
    return limit_ != -1 && num_rows_returned_ >= limit_;
  }

  /// Returns true if a valid limit is set and number of rows returned by this node has
  /// exceeded the limit. Thread safe version of ReachedLimit().
  bool ReachedLimitShared() {
    // Ideally this function should only be called when num_rows_returned_ is shared by
    // multiple threads. Both the HdfsScanNodeMt and KuduScanNodeMt call this function
    // from the scanner code-path for simplicity.
    DCHECK(
        getExecutionModel() == NON_TASK_BASED_SYNC || getExecutionModel() == TASK_BASED);
    return limit_ != -1 && base::subtle::Acquire_Load(&num_rows_returned_) >= limit_;
  }

  RuntimeProfile* runtime_profile() { return runtime_profile_; }
  MemTracker* mem_tracker() { return mem_tracker_.get(); }
  MemTracker* expr_mem_tracker() { return expr_mem_tracker_.get(); }
  MemPool* expr_perm_pool() { return expr_perm_pool_.get(); }
  MemPool* expr_results_pool() { return expr_results_pool_.get(); }
  const TBackendResourceProfile& resource_profile() { return resource_profile_; }
  bool is_closed() const { return is_closed_; }

  /// Return true if codegen was disabled by the planner for this ExecNode. Does not
  /// check to see if codegen was enabled for the enclosing fragment.
  bool IsNodeCodegenDisabled() const;

  /// Returns true if this node is inside the right-hand side plan tree of a SubplanNode.
  /// Valid to call in or after Prepare().
  bool IsInSubplan() const { return containing_subplan_ != NULL; }

  /// Names of counters shared by all exec nodes
  static const std::string ROW_THROUGHPUT_COUNTER;

 protected:
  friend class DataSink;
  friend class ScopedGetNextEventAdder;
  friend class ScopedOpenEventAdder;

  enum ExecutionModel {
    /// Exec nodes with single threaded execution. This is the default execution model
    /// for majority of the nodes. BlockingJoin node is an exception since it could spawn
    /// a build thread in some cases. Due to the blocking nature of such join operators,
    /// it can be considered as not requiring explicit synchronization.
    NON_TASK_BASED_NO_SYNC,
    /// Exec nodes which spawn multiple worker threads. Examples are HdfsScanNode and
    /// and KuduScanNode. Requires syncronization if the main thread and worker threads
    /// share resources.
    NON_TASK_BASED_SYNC,
    /// Task Based multi-threading. Examples are HdfsScanNodeMt and KuduScanNodeMt.
    TASK_BASED
  };

  virtual ExecutionModel getExecutionModel() const { return NON_TASK_BASED_NO_SYNC; }

  BufferPool::ClientHandle* buffer_pool_client() {
    return reservation_manager_.buffer_pool_client();
  }

  Status ClaimBufferReservation(RuntimeState* state) WARN_UNUSED_RESULT {
    return reservation_manager_.ClaimBufferReservation(state);
  }

  Status ReleaseUnusedReservation() WARN_UNUSED_RESULT {
    return reservation_manager_.ReleaseUnusedReservation();
  }

  /// Unique within a single plan tree.
  int id_;
  TPlanNodeType::type type_;
  ObjectPool* pool_;

  /// ExecNode lifecycle events for this ExecNode. Initialised for nodes outside subplan.
  /// Within a subplan, we iterate through the lifecycle multiple times so this would
  /// have a lot of overhead and not be particularly useful. All times are relative to
  /// QueryState::fragment_events_start_time().
  RuntimeProfile::EventSequence* events_ = nullptr;

  /// Used to track whether the first GetNext() event was added to 'events_' so that
  /// ScopedGetNextEventAdder can avoid adding a duplicate event.
  bool first_getnext_added_ = false;

  /// Conjuncts and their evaluators in this node. 'conjuncts_' live in the
  /// query-state's object pool while the evaluators live in this exec node's
  /// object pool.
  std::vector<ScalarExpr*> conjuncts_;
  std::vector<ScalarExprEvaluator*> conjunct_evals_;

  std::vector<ExecNode*> children_;
  RowDescriptor row_descriptor_;

  /// Resource information sent from the frontend.
  const TBackendResourceProfile resource_profile_;

  /// debug-only: if debug_action_ is not INVALID, node will perform action in
  /// debug_phase_
  TDebugOptions debug_options_;

  int64_t limit_;  // -1: no limit

  /// Runtime profile for this node. Owned by the QueryState's ObjectPool.
  RuntimeProfile* const runtime_profile_;
  RuntimeProfile::Counter* rows_returned_counter_;
  RuntimeProfile::Counter* rows_returned_rate_;

  /// Account for peak memory used by this node
  boost::scoped_ptr<MemTracker> mem_tracker_;

  /// MemTracker used by 'expr_perm_pool_' and 'expr_results_pool_'.
  boost::scoped_ptr<MemTracker> expr_mem_tracker_;

  /// MemPool for allocations made by expression evaluators in this node that are
  /// "permanent" and live until Close() is called. Created in Prepare().
  boost::scoped_ptr<MemPool> expr_perm_pool_;

  /// MemPool for allocations made by expression evaluators in this node that hold
  /// intermediate or final results of expression evaluation. Should be cleared
  /// periodically to free accumulated memory. QueryMaintenance() clears this pool, but
  /// it may be appropriate for ExecNode implementation to clear it at other points in
  /// execution where the memory is not needed.
  boost::scoped_ptr<MemPool> expr_results_pool_;

  /// Pointer to the containing SubplanNode or NULL if not inside a subplan.
  /// Set by SubplanNode::Init(). Not owned.
  SubplanNode* containing_subplan_;

  /// If true, codegen should be disabled for this exec node.
  const bool disable_codegen_;

  /// Create a single exec node derived from thrift node; place exec node in 'pool'.
  static Status CreateNode(ObjectPool* pool, const TPlanNode& tnode,
      const DescriptorTbl& descs, ExecNode** node,
      RuntimeState* state) WARN_UNUSED_RESULT;

  static Status CreateTreeHelper(RuntimeState* state,
      const std::vector<TPlanNode>& tnodes, const DescriptorTbl& descs, ExecNode* parent,
      int* node_idx, ExecNode** root) WARN_UNUSED_RESULT;

  virtual bool IsScanNode() const { return false; }

  /// Executes 'debug_action_' if 'phase' matches 'debug_phase_'.
  /// 'phase' must not be INVALID.
  Status ExecDebugAction(
      TExecNodePhase::type phase, RuntimeState* state) WARN_UNUSED_RESULT {
    DCHECK_NE(phase, TExecNodePhase::INVALID);
    // Fast path for the common case when an action is not enabled for this phase.
    if (LIKELY(debug_options_.phase != phase)) return Status::OK();
    return ExecDebugActionImpl(phase, state);
  }

  /// Clears 'expr_results_pool_' and returns the result of state->CheckQueryState().
  /// Nodes should call this periodically, e.g. once per input row batch. This should
  /// not be called outside the main execution thread.
  /// TODO: IMPALA-2399: replace QueryMaintenance() - see JIRA for more details.
  Status QueryMaintenance(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Sets the number of rows returned.
  void SetNumRowsReturned(int64_t value) {
    DCHECK(getExecutionModel() != NON_TASK_BASED_SYNC);
    num_rows_returned_ = value;
    DFAKE_SCOPED_LOCK_THREAD_LOCKED(single_thread_check_);
  }

  /// Increment the number of rows returned.
  void IncrementNumRowsReturned(int64_t value) {
    DCHECK(getExecutionModel() != NON_TASK_BASED_SYNC);
    num_rows_returned_ += value;
    DFAKE_SCOPED_LOCK_THREAD_LOCKED(single_thread_check_);
  }

  /// Increment the number of rows returned. Thread safe version of
  /// IncrementNumRowsReturned().
  void IncrementNumRowsReturnedShared(int64_t value) {
    DCHECK(getExecutionModel() == NON_TASK_BASED_SYNC);
    base::subtle::Barrier_AtomicIncrement(&num_rows_returned_, value);
  }

  /// Decrement the number of rows returned.
  void DecrementNumRowsReturned(int64_t value) { IncrementNumRowsReturned(-1 * value); }

  /// Decrement the number of rows returned. Thread safe version of
  /// DecrementNumRowsReturned().
  void DecrementNumRowsReturnedShared(int64_t value) {
    IncrementNumRowsReturnedShared(-1 * value);
  }

  /// Caps the input row batch to ensure that the limit is not exceeded.
  /// Sets the eos and returns true, if the limit is reached.
  bool CheckLimitAndTruncateRowBatchIfNeeded(RowBatch* row_batch, bool* eos);

  /// Caps the input row batch to ensure that the limit is not exceeded.
  /// Sets the eos and returns true, if the limit is reached.
  /// Uses thread safe functions.
  bool CheckLimitAndTruncateRowBatchIfNeededShared(RowBatch* row_batch, bool* eos);

 private:
  /// Keeps track of number of rows returned by an exec node. If this variable is shared
  /// by multiple threads, it should be accessed using thread-safe functions defined
  /// above. The single-threaded code-paths should use non-atomic functions defined
  /// above. The only exceptions are HdfsScanNodeMt and KuduScanNodeMt, which are single
  /// threaded (task based multi-threading support), but use the thread-safe functions
  /// in the scanner code-path for code simplicity. This is because both the task based
  /// MT scan nodes (HdfsScanNodeMt/KuduScanNodeMt) and regular scan nodes
  /// (HdfsScanNode/KuduScanNode) call common scanner functions.
  int64_t num_rows_returned_;
  DFAKE_MUTEX(single_thread_check_);
  /// Implementation of ExecDebugAction(). This is the slow path we take when there is
  /// actually a debug action enabled for 'phase'.
  Status ExecDebugActionImpl(
      TExecNodePhase::type phase, RuntimeState* state) WARN_UNUSED_RESULT;

  /// Set in ExecNode::Close(). Used to make Close() idempotent. This is not protected
  /// by a lock, it assumes all calls to Close() are made by the same thread.
  bool is_closed_;

  /// Wraps the buffer pool client for this node. Initialized with the node's minimum
  /// reservation in ClaimBufferReservation(). After initialization, the client must hold
  /// onto at least the minimum reservation so that it can be returned to the initial
  /// reservations pool in Close().
  ReservationManager reservation_manager_;
};

inline bool ExecNode::EvalPredicate(ScalarExprEvaluator* eval, TupleRow* row) {
  BooleanVal v = eval->GetBooleanVal(row);
  if (v.is_null || !v.val) return false;
  return true;
}

}
#endif
