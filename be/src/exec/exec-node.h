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


#ifndef IMPALA_EXEC_EXEC_NODE_H
#define IMPALA_EXEC_EXEC_NODE_H

#include <vector>
#include <sstream>

#include "common/status.h"
#include "exprs/expr-context.h"
#include "runtime/descriptors.h"  // for RowDescriptor
#include "util/runtime-profile.h"
#include "util/blocking-queue.h"
#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class Expr;
class ExprContext;
class ObjectPool;
class Counters;
class SortExecExprs;
class RowBatch;
class RuntimeState;
class TPlan;
class TupleRow;
class DataSink;
class MemTracker;
class SubplanNode;

/// Superclass of all executor nodes.
/// All subclasses need to make sure to check RuntimeState::is_cancelled()
/// periodically in order to ensure timely termination after the cancellation
/// flag gets set.
class ExecNode {
 public:
  /// Init conjuncts.
  ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual ~ExecNode();

  /// Initializes this object from the thrift tnode desc. The subclass should
  /// do any initialization that can fail in Init() rather than the ctor.
  /// If overridden in subclass, must first call superclass's Init().
  virtual Status Init(const TPlanNode& tnode);

  /// Sets up internal structures, etc., without doing any actual work.
  /// Must be called prior to Open(). Will only be called once in this
  /// node's lifetime.
  /// All code generation (adding functions to the LlvmCodeGen object) must happen
  /// in Prepare().  Retrieving the jit compiled function pointer must happen in
  /// Open().
  /// If overridden in subclass, must first call superclass's Prepare().
  virtual Status Prepare(RuntimeState* state);

  /// Performs any preparatory work prior to calling GetNext().
  /// Caller must not be holding any io buffers. This will cause deadlock.
  /// If overridden in subclass, must first call superclass's Open().
  /// If a parent exec node adds slot filters (see RuntimeState::AddBitmapFilter()),
  /// they need to be added before calling Open() on the child that will consume them.
  /// Open() is called after Prepare() or Reset(), i.e., possibly multiple times
  /// throughout the lifetime of this node.
  virtual Status Open(RuntimeState* state);

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
  /// Caller must not be holding any io buffers. This will cause deadlock.
  /// TODO: AggregationNode and HashJoinNode cannot be "re-opened" yet.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) = 0;

  /// Resets the stream of row batches to be retrieved by subsequent GetNext() calls.
  /// Clears all internal state, returning this node to the state it was in after calling
  /// Prepare() and before calling Open(). This function must not clear memory
  /// still owned by this node that is backing rows returned in GetNext().
  /// Prepare() and Open() must have already been called before calling Reset().
  /// GetNext() may have optionally been called (not necessarily until eos).
  /// Close() must not have been called.
  /// Reset() is not idempotent. Calling it multiple times in a row without a preceding
  /// call to Open() is invalid.
  /// If overridden in a subclass, must call superclass's Reset() at the end. The default
  /// implementation calls Reset() on children.
  /// Note that this function may be called many times (proportional to the input data),
  /// so should be fast.
  virtual Status Reset(RuntimeState* state);

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
  /// traversal. All nodes are placed in pool and have Init() called on them.
  /// Returns error if 'plan' is corrupted, otherwise success.
  static Status CreateTree(ObjectPool* pool, const TPlan& plan,
                           const DescriptorTbl& descs, ExecNode** root);

  /// Set debug action for node with given id in 'tree'
  static void SetDebugOptions(int node_id, TExecNodePhase::type phase,
                              TDebugAction::type action, ExecNode* tree);

  /// Collect all nodes of given 'node_type' that are part of this subtree, and return in
  /// 'nodes'.
  void CollectNodes(TPlanNodeType::type node_type, std::vector<ExecNode*>* nodes);

  /// Collect all scan node types.
  void CollectScanNodes(std::vector<ExecNode*>* nodes);

  /// Evaluate ExprContexts over row.  Returns true if all exprs return true.
  /// TODO: This doesn't use the vector<Expr*> signature because I haven't figured
  /// out how to deal with declaring a templated std:vector type in IR
  static bool EvalConjuncts(ExprContext* const* ctxs, int num_ctxs, TupleRow* row);

  /// Codegen EvalConjuncts(). Returns a non-OK status if the function couldn't be
  /// codegen'd. The codegen'd version uses inlined, codegen'd GetBooleanVal() functions.
  static Status CodegenEvalConjuncts(
      RuntimeState* state, const std::vector<ExprContext*>& conjunct_ctxs,
      llvm::Function** fn, const char* name = "EvalConjuncts");

  /// Returns a string representation in DFS order of the plan rooted at this.
  std::string DebugString() const;

  /// Recursive helper method for generating a string for DebugString().
  /// Implementations should call DebugString(int, std::stringstream) on their children.
  /// Input parameters:
  ///   indentation_level: Current level in plan tree.
  /// Output parameters:
  ///   out: Stream to accumulate debug string.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

  const std::vector<ExprContext*>& conjunct_ctxs() const { return conjunct_ctxs_; }
  int id() const { return id_; }
  TPlanNodeType::type type() const { return type_; }
  const RowDescriptor& row_desc() const { return row_descriptor_; }
  ExecNode* child(int i) { return children_[i]; }
  int num_children() const { return children_.size(); }
  SubplanNode* get_containing_subplan() const { return containing_subplan_; }
  void set_containing_subplan(SubplanNode* sp) {
    DCHECK(containing_subplan_ == NULL);
    containing_subplan_ = sp;
  }
  int64_t rows_returned() const { return num_rows_returned_; }
  int64_t limit() const { return limit_; }
  bool ReachedLimit() { return limit_ != -1 && num_rows_returned_ >= limit_; }

  RuntimeProfile* runtime_profile() { return runtime_profile_.get(); }
  MemTracker* mem_tracker() { return mem_tracker_.get(); }
  MemTracker* expr_mem_tracker() { return expr_mem_tracker_.get(); }

  /// Extract node id from p->name().
  static int GetNodeIdFromProfile(RuntimeProfile* p);

  /// Names of counters shared by all exec nodes
  static const std::string ROW_THROUGHPUT_COUNTER;

 protected:
  friend class DataSink;

  /// Extends blocking queue for row batches. Row batches have a property that
  /// they must be processed in the order they were produced, even in cancellation
  /// paths. Preceding row batches can contain ptrs to memory in subsequent row batches
  /// and we need to make sure those ptrs stay valid.
  /// Row batches that are added after Shutdown() are queued in another queue, which can
  /// be cleaned up during Close().
  /// All functions are thread safe.
  class RowBatchQueue : public BlockingQueue<RowBatch*> {
   public:
    /// max_batches is the maximum number of row batches that can be queued.
    /// When the queue is full, producers will block.
    RowBatchQueue(int max_batches);
    ~RowBatchQueue();

    /// Adds a batch to the queue. This is blocking if the queue is full.
    void AddBatch(RowBatch* batch);

    /// Gets a row batch from the queue. Returns NULL if there are no more.
    /// This function blocks.
    /// Returns NULL after Shutdown().
    RowBatch* GetBatch();

    /// Deletes all row batches in cleanup_queue_. Not valid to call AddBatch()
    /// after this is called.
    /// Returns the number of io buffers that were released (for debug tracking)
    int Cleanup();

   private:
    /// Lock protecting cleanup_queue_
    SpinLock lock_;

    /// Queue of orphaned row batches
    std::list<RowBatch*> cleanup_queue_;
  };

  /// Unique within a single plan tree.
  int id_;

  TPlanNodeType::type type_;
  ObjectPool* pool_;
  std::vector<ExprContext*> conjunct_ctxs_;

  std::vector<ExecNode*> children_;
  RowDescriptor row_descriptor_;

  /// debug-only: if debug_action_ is not INVALID, node will perform action in
  /// debug_phase_
  TExecNodePhase::type debug_phase_;
  TDebugAction::type debug_action_;

  int64_t limit_;  // -1: no limit
  int64_t num_rows_returned_;

  boost::scoped_ptr<RuntimeProfile> runtime_profile_;
  RuntimeProfile::Counter* rows_returned_counter_;
  RuntimeProfile::Counter* rows_returned_rate_;

  /// Account for peak memory used by this node
  boost::scoped_ptr<MemTracker> mem_tracker_;

  /// MemTracker that should be used for ExprContexts.
  boost::scoped_ptr<MemTracker> expr_mem_tracker_;

  /// Execution options that are determined at runtime.  This is added to the
  /// runtime profile at Close().  Examples for options logged here would be
  /// "Codegen Enabled"
  boost::mutex exec_options_lock_;
  std::string runtime_exec_options_;

  bool is_closed() const { return is_closed_; }

  /// Pointer to the containing SubplanNode or NULL if not inside a subplan.
  /// Set by SubplanNode::Init(). Not owned.
  SubplanNode* containing_subplan_;

  /// Returns true if this node is inside the right-hand side plan tree of a SubplanNode.
  /// Valid to call in or after Prepare().
  bool IsInSubplan() const { return containing_subplan_ != NULL; }

  /// Create a single exec node derived from thrift node; place exec node in 'pool'.
  static Status CreateNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs, ExecNode** node);

  static Status CreateTreeHelper(ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
      const DescriptorTbl& descs, ExecNode* parent, int* node_idx, ExecNode** root);

  virtual bool IsScanNode() const { return false; }

  void InitRuntimeProfile(const std::string& name);

  /// Executes debug_action_ if phase matches debug_phase_.
  /// 'phase' must not be INVALID.
  Status ExecDebugAction(TExecNodePhase::type phase, RuntimeState* state);

  /// Appends option to 'runtime_exec_options_'
  void AddRuntimeExecOption(const std::string& option);

  /// Helper wrapper around AddRuntimeExecOption() for adding "Codegen Enabled" or
  /// "Codegen Disabled" exec options. If specified, 'extra_info' is appended to the exec
  /// option, and 'extra_label' is prepended to the exec option.
  void AddCodegenExecOption(bool codegen_enabled, const string& extra_info = "",
      const string& extra_label = "");

  /// Helper wrapper that takes a status optionally describing why codegen was
  /// disabled. 'codegen_status' can be OK.
  void AddCodegenExecOption(bool codegen_enabled, const Status& codegen_status,
      const string& extra_label = "") {
    AddCodegenExecOption(codegen_enabled, codegen_status.GetDetail(), extra_label);
  }

  /// Frees any local allocations made by expr_ctxs_to_free_ and returns the result of
  /// state->CheckQueryState(). Nodes should call this periodically, e.g. once per input
  /// row batch. This should not be called outside the main execution thread.
  //
  /// Nodes may override this to add extra periodic cleanup, e.g. freeing other local
  /// allocations. ExecNodes overriding this function should return
  /// ExecNode::QueryMaintenance().
  virtual Status QueryMaintenance(RuntimeState* state);

  /// Add an ExprContext to have its local allocations freed by QueryMaintenance().
  /// Exprs that are evaluated in the main execution thread should be added. Exprs
  /// evaluated in a separate thread are generally not safe to add, since a local
  /// allocation may be freed while it's being used. Rather than using this mechanism,
  /// threads should call FreeLocalAllocations() on local ExprContexts periodically.
  void AddExprCtxToFree(ExprContext* ctx) { expr_ctxs_to_free_.push_back(ctx); }
  void AddExprCtxsToFree(const std::vector<ExprContext*>& ctxs);
  void AddExprCtxsToFree(const SortExecExprs& sort_exec_exprs);

  /// Free any local allocations made by expr_ctxs_to_free_.
  void FreeLocalAllocations() { ExprContext::FreeLocalAllocations(expr_ctxs_to_free_); }

 private:
  /// Set in ExecNode::Close(). Used to make Close() idempotent. This is not protected
  /// by a lock, it assumes all calls to Close() are made by the same thread.
  bool is_closed_;

  /// Expr contexts whose local allocations are safe to free in the main execution thread.
  std::vector<ExprContext*> expr_ctxs_to_free_;
};

}
#endif
