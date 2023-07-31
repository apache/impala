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

#include <boost/scoped_ptr.hpp>

#include "gen-cpp/ImpalaInternalService_types.h"
#include "runtime/query-state.h"
#include "util/runtime-profile.h"

namespace impala {

class FragmentInstanceState;
class QueryCtx;
class RuntimeProfile;

/// This encapsulates all the static state for a fragment that will be shared across its
/// instances which includes the thrift structures representing fragments and all its
/// instances, plan node tree and data sink config. It also contains state and methods
/// required for creating, invoking and managing codegen. Is not thread safe unless
/// specified.

class FragmentState {
 public:
  /// Create a map of fragment index to its FragmentState object and only populate the
  /// thrift and protobuf references of the fragment and instance context objects from
  /// 'fragment_info' and 'exec_request'.
  static Status CreateFragmentStateMap(const TExecPlanFragmentInfo& fragment_info,
      const ExecQueryFInstancesRequestPB& exec_request, QueryState* query_state,
      std::unordered_map<TFragmentIdx, FragmentState*>& fragment_map);
  FragmentState(QueryState* query_state, const TPlanFragment& fragment,
      const PlanFragmentCtxPB& fragment_ctx);
  ~FragmentState();

  /// Called by all the fragment instance threads that execute this fragment. The first
  /// fragment instance to call this does the actual codegen work. The rest either wait
  /// till codegen is complete or simple return immediately if it is already completed. In
  /// case codegen fails, it attempts to set an error status in the query state and
  /// returns that status on every subsequent call. Is thread-safe.
  Status InvokeCodegen(RuntimeProfile::EventSequence* event_sequence);

  /// Release resources held by codegen, the plan tree and data sink config.
  void ReleaseResources();

  ObjectPool* obj_pool() { return &obj_pool_; }
  int fragment_idx() const { return fragment_.idx; }
  const TQueryOptions& query_options() const { return query_state_->query_options(); }
  const TQueryCtx& query_ctx() const { return query_state_->query_ctx(); }
  const TPlanFragment& fragment() const { return fragment_; }
  const PlanFragmentCtxPB& fragment_ctx() const { return fragment_ctx_; }
  const std::vector<const TPlanFragmentInstanceCtx*>& instance_ctxs() const {
    return instance_ctxs_;
  }
  const std::vector<const PlanFragmentInstanceCtxPB*>& instance_ctx_pbs() const {
    return instance_ctx_pbs_;
  }
  /// Return whether the codegen cache is enabled. It relies on the setting of the query.
  bool codegen_cache_enabled() const { return query_state_->codegen_cache_enabled(); }
  /// Return the minimum per-fragment index of an instance on this backend.
  int min_per_fragment_instance_idx() const {
    // 'instance_ctxs_' is in ascending order, so can just return the first one.
    DCHECK(!instance_ctxs_.empty());
    return instance_ctxs_[0]->per_fragment_instance_idx;
  }
  const PlanNode* plan_tree() const { return plan_tree_; }
  const DataSinkConfig* sink_config() const { return sink_config_; }
  const TUniqueId& query_id() const { return query_state_->query_id(); }
  const DescriptorTbl& desc_tbl() const { return query_state_->desc_tbl(); }
  MemTracker* query_mem_tracker() const { return query_state_->query_mem_tracker(); }
  QueryState* query_state() const { return query_state_; }
  RuntimeProfile* runtime_profile() { return runtime_profile_; }

  static const std::string FSTATE_THREAD_GROUP_NAME;
  static const std::string FSTATE_THREAD_NAME_PREFIX;

  /// Methods relevant for codegen.

  /// Create a codegen object accessible via codegen() if it doesn't exist already.
  Status CreateCodegen();

  /// Codegen all ScalarExpr expressions in 'scalar_exprs_to_codegen_'. If codegen fails
  /// for any expressions, return immediately with the error status. Once IMPALA-4233 is
  /// fixed, it's not fatal to fail codegen if the expression can be interpreted.
  /// TODO: Now that IMPALA-4233 is fixed, revisit this comment.
  Status CodegenScalarExprs();

  /// Add ScalarExpr expression 'expr' to be codegen'd later if it's not disabled by query
  /// option. If 'is_codegen_entry_point' is true or 'interpretable' is false, 'expr' will
  /// be an entry point into codegen'd evaluation (i.e. it will have a function pointer
  /// populated) - if the expression is not interpretable, we need an entry point to
  /// evaluate it from interpreted code, e.g. GetConstValue().
  ///
  /// Adding an expr here ensures that it will be codegen'd (i.e. fragment execution will
  /// fail with an error if the expr cannot be codegen'd).
  void AddScalarExprToCodegen(ScalarExpr* expr, bool is_codegen_entry_point,
      bool interpretable) {
    is_interpretable_ = is_interpretable_ && interpretable;
    scalar_exprs_to_codegen_.push_back({expr, is_codegen_entry_point || !interpretable});
  }

  /// Returns true if there are ScalarExpr expressions in the fragments that we want
  /// to codegen (because they can't be interpreted or based on options/hints).
  /// This should only be used after the plan tree and the data sink configs have been
  /// created, init'ed in which all expressions' Prepare() are invoked.
  bool ScalarExprNeedsCodegen() const { return !scalar_exprs_to_codegen_.empty(); }

  /// Returns the number of scalar expressions to be codegen'd.
  int64_t NumScalarExprNeedsCodegen() const { return scalar_exprs_to_codegen_.size(); }

  /// Check if codegen was disabled and if so, add a message to the runtime profile.
  /// Call this only after expressions have been created.
  void CheckAndAddCodegenDisabledMessage(std::vector<std::string>& codegen_status_msgs) {
    if (CodegenDisabledByQueryOption()) {
      codegen_status_msgs.emplace_back(
          GenerateCodegenMsg(false, "disabled by query option DISABLE_CODEGEN"));
    } else if (CodegenDisabledByHint()) {
      codegen_status_msgs.emplace_back(
          GenerateCodegenMsg(false, "disabled due to optimization hints"));
    }
  }

  /// Returns true if there is a hint to disable codegen. This can be true for single node
  /// optimization or expression evaluation request from FE to BE (see fe-support.cc).
  /// Note that this internal flag is advisory and it may be ignored if the fragment has
  /// any UDF which cannot be interpreted. See ScalarExpr::Prepare() for details.
  inline bool CodegenHasDisableHint() const {
    return query_state_->query_ctx().disable_codegen_hint;
  }

  /// Returns true iff there is a hint to disable codegen and all expressions in the
  /// fragment can be interpreted. This should only be used after the Prepare() phase
  /// in which all expressions' Prepare() are invoked.
  inline bool CodegenDisabledByHint() const {
    return CodegenHasDisableHint() && !ScalarExprNeedsCodegen();
  }

  /// Returns true if codegen is disabled by query option.
  inline bool CodegenDisabledByQueryOption() const {
    return query_options().disable_codegen;
  }

  /// Returns true if codegen should be enabled for this fragment. Codegen is enabled
  /// if all the following conditions hold:
  /// 1. it's enabled by query option
  /// 2. it's not disabled by internal hints or there are expressions in the fragment
  ///    which cannot be interpreted.
  inline bool ShouldCodegen() const {
    return !CodegenDisabledByQueryOption() && !CodegenDisabledByHint();
  }

  /// Whether all expressions are interpretable or codegen is necessary because an
  /// expression cannot be interpreted.
  inline bool is_interpretable() const {
    return is_interpretable_;
  }

  LlvmCodeGen* codegen() { return codegen_.get(); }

  /// Utility methods for generating a messages from Status objects by adding context
  /// relevant to codegen.
  static std::string GenerateCodegenMsg(bool codegen_enabled,
      const Status& codegen_status, const std::string& extra_label = "");
  static std::string GenerateCodegenMsg(bool codegen_enabled,
      const std::string& extra_info = "", const std::string& extra_label = "");

 private:
  bool ScalarExprIsWithinStruct(const ScalarExpr* expr) const;

  ObjectPool obj_pool_;

  /// Reference to the query state object that owns this.
  QueryState* query_state_;

  /// References to the thrift structs for this fragment.
  const TPlanFragment& fragment_;
  std::vector<const TPlanFragmentInstanceCtx*> instance_ctxs_;
  /// References to the protobuf structs for this fragment.
  const PlanFragmentCtxPB& fragment_ctx_;
  std::vector<const PlanFragmentInstanceCtxPB*> instance_ctx_pbs_;

  /// Lives in obj_pool(). Not mutated after being initialized in InitAndCodegen() except
  /// for being closed.
  PlanNode* plan_tree_ = nullptr;
  DataSinkConfig* sink_config_ = nullptr;

  boost::scoped_ptr<LlvmCodeGen> codegen_;

  /// Stores the result of calling InitAndCodegen() to check for any errors encountered
  /// during that call.
  Status codegen_status_;

  /// Contains all ScalarExpr expressions which need to be codegen'd. The second element
  /// is true if we want to generate a codegen entry point for this expr.
  std::vector<std::pair<ScalarExpr*, bool>> scalar_exprs_to_codegen_;

  /// Whether all expressions are interpretable. If at least one expression is
  /// non-interpretable, codegen is necessary and we should return an error if it is
  /// disabled.
  bool is_interpretable_ = true;

  RuntimeProfile* runtime_profile_ = nullptr;

  /// Serializes access to InvokeCodegen().
  /// Lock ordering: QueryState::status_lock_ must *not be obtained* prior to this.
  std::mutex codegen_lock_;

  /// Indicates whether codegen has been invoked. Used to make sure only the first
  /// fragment instance to call InvokeCodegen() does the actual codegen work.
  bool codegen_invoked_ = false;

  /// Used by the CreateFragmentStateMap to add the TPlanFragmentInstanceCtx and the
  /// PlanFragmentInstanceCtxPB for the fragment that this object represents.
  void AddInstance(const TPlanFragmentInstanceCtx* instance_ctx,
      const PlanFragmentInstanceCtxPB* instance_ctx_pb) {
    instance_ctxs_.push_back(instance_ctx);
    instance_ctx_pbs_.push_back(instance_ctx_pb);
  }

  /// Helper method used by InvokeCodegen(). Does the actual codegen work.
  Status CodegenHelper(RuntimeProfile::EventSequence* event_sequence);

  /// Create the plan tree, data sink config.
  Status Init();

  /// Helper function to populate the filename to hosts mapping in 'sink_config_' from
  /// 'query_state_';
  Status PutFilesToHostsMappingToSinkConfig();
};
}
