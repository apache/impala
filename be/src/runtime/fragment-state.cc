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

#include "runtime/fragment-state.h"

#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "exec/exec-node.h"
#include "exec/data-sink.h"
#include "runtime/exec-env.h"
#include "runtime/query-state.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/runtime-profile.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

const string FragmentState::FSTATE_THREAD_GROUP_NAME = "fragment-init";
const string FragmentState::FSTATE_THREAD_NAME_PREFIX = "init-and-codegen";

Status FragmentState::CreateFragmentStateMap(const TExecPlanFragmentInfo& fragment_info,
    const ExecQueryFInstancesRequestPB& exec_request, QueryState* state,
    std::unordered_map<TFragmentIdx, FragmentState*>& fragment_map) {
  int fragment_ctx_idx = 0;
  const TPlanFragment& frag = fragment_info.fragments[fragment_ctx_idx];
  const PlanFragmentCtxPB& frag_ctx = exec_request.fragment_ctxs(fragment_ctx_idx);
  FragmentState* fragment_state =
      state->obj_pool()->Add(new FragmentState(state, frag, frag_ctx));
  fragment_map[fragment_state->fragment_idx()] = fragment_state;
  for (int i = 0; i < fragment_info.fragment_instance_ctxs.size(); ++i) {
    const TPlanFragmentInstanceCtx& instance_ctx =
        fragment_info.fragment_instance_ctxs[i];
    const PlanFragmentInstanceCtxPB& instance_ctx_pb =
        exec_request.fragment_instance_ctxs(i);
    DCHECK_EQ(instance_ctx.fragment_idx, instance_ctx_pb.fragment_idx());
    // determine corresponding TPlanFragment
    if (fragment_state->fragment_idx() != instance_ctx.fragment_idx) {
      ++fragment_ctx_idx;
      DCHECK_LT(fragment_ctx_idx, fragment_info.fragments.size());
      const TPlanFragment& fragment = fragment_info.fragments[fragment_ctx_idx];
      const PlanFragmentCtxPB& fragment_ctx =
          exec_request.fragment_ctxs(fragment_ctx_idx);
      DCHECK_EQ(fragment.idx, fragment_ctx.fragment_idx());
      fragment_state =
          state->obj_pool()->Add(new FragmentState(state, fragment, fragment_ctx));
      fragment_map[fragment_state->fragment_idx()] = fragment_state;
      // we expect fragment and instance contexts to follow the same order
      DCHECK_EQ(fragment_state->fragment_idx(), instance_ctx.fragment_idx);
    } else if (!fragment_state->instance_ctxs().empty()) {
      // This invariant is needed for min_per_fragment_instance_idx() to be correct.
      DCHECK_EQ(fragment_state->instance_ctxs().back()->per_fragment_instance_idx + 1,
          instance_ctx.per_fragment_instance_idx)
          << "Instance indexes must be sequential";
    }
    fragment_state->AddInstance(&instance_ctx, &instance_ctx_pb);
  }
  // Init all fragments.
  for (auto& elem : fragment_map) {
    RETURN_IF_ERROR(elem.second->Init());
  }
  return Status::OK();
}

Status FragmentState::Init() {
  RETURN_IF_ERROR(PlanNode::CreateTree(this, fragment_.plan, &plan_tree_));
  RETURN_IF_ERROR(DataSinkConfig::CreateConfig(
      fragment_.output_sink, plan_tree_->row_descriptor_, this, &sink_config_));
  return Status::OK();
}

Status FragmentState::InvokeCodegen(RuntimeProfile::EventSequence* event_sequence) {
  unique_lock<mutex> l(codegen_lock_);
  if (!codegen_invoked_) {
    codegen_invoked_ = true;
    codegen_status_ = CodegenHelper(event_sequence);
    if (!codegen_status_.ok()) {
      string error_ctx = Substitute(
          "Fragment failed during codegen, fragment index: $0", fragment_.display_name);
      codegen_status_.AddDetail(error_ctx);
      query_state_->ErrorDuringFragmentCodegen(codegen_status_);
    }
  }
  return codegen_status_;
}

Status FragmentState::CodegenHelper(RuntimeProfile::EventSequence* event_sequence) {
  DCHECK(plan_tree_ != nullptr);
  DCHECK(sink_config_ != nullptr);
  DCHECK(ShouldCodegen());
  RETURN_IF_ERROR(CreateCodegen());

  SCOPED_TIMER(codegen()->main_thread_timer());
  {
    SCOPED_TIMER2(codegen()->ir_generation_timer(),
        codegen()->runtime_profile()->total_time_counter());
    SCOPED_THREAD_COUNTER_MEASUREMENT(codegen()->llvm_thread_counters());
    plan_tree_->Codegen(this);
    sink_config_->Codegen(this);
    // It shouldn't be fatal to fail codegen. However, until IMPALA-4233 is fixed,
    // ScalarFnCall has no fall back to interpretation when codegen fails so propagates
    // the error status for now. Now that IMPALA-4233 is fixed, revisit this comment.
    RETURN_IF_ERROR(CodegenScalarExprs());
  }

  LlvmCodeGen* llvm_codegen = codegen();
  DCHECK(llvm_codegen != nullptr);

  // In case we need codegen, we cannot use asynchronous codegen because we cannot
  // interpret the query until codegen has run.
  const bool async_enabled = query_options().async_codegen;
  if (async_enabled && is_interpretable()) {
    RETURN_IF_ERROR(llvm_codegen->FinalizeModuleAsync(event_sequence));
  } else {
    RETURN_IF_ERROR(llvm_codegen->FinalizeModule());
  }

  return Status::OK();
}

FragmentState::FragmentState(QueryState* query_state, const TPlanFragment& fragment,
    const PlanFragmentCtxPB& fragment_ctx)
  : query_state_(query_state), fragment_(fragment), fragment_ctx_(fragment_ctx) {
  runtime_profile_ = RuntimeProfile::Create(
      query_state->obj_pool(), Substitute("Fragment $0", fragment_.display_name));
  query_state_->host_profile()->AddChild(runtime_profile_);
}

FragmentState::~FragmentState() {}

void FragmentState::ReleaseResources() {
  if (codegen_ != nullptr) codegen_->Close();
  if (plan_tree_ != nullptr) plan_tree_->Close();
  if (sink_config_ != nullptr) sink_config_->Close();
}


Status FragmentState::CreateCodegen() {
  if (codegen_.get() != NULL) return Status::OK();
  RETURN_IF_ERROR(LlvmCodeGen::CreateImpalaCodegen(
      this, query_mem_tracker(), PrintId(query_id()), &codegen_));
  codegen_->EnableOptimizations(true);
  runtime_profile_->AddChild(codegen_->runtime_profile());
  return Status::OK();
}

Status FragmentState::CodegenScalarExprs() {
  for (auto& item : scalar_exprs_to_codegen_) {
    llvm::Function* fn;
    RETURN_IF_ERROR(item.first->GetCodegendComputeFn(codegen_.get(), item.second, &fn));
  }
  return Status::OK();
}


std::string FragmentState::GenerateCodegenMsg(
    bool codegen_enabled, const Status& codegen_status, const std::string& extra_label) {
  const string& err_msg = codegen_status.ok() ? "" : codegen_status.msg().msg();
  return GenerateCodegenMsg(codegen_enabled, err_msg, extra_label);
}

std::string FragmentState::GenerateCodegenMsg(bool codegen_enabled,
    const std::string& extra_info, const std::string& extra_label) {
  std::stringstream str;
  if (!extra_label.empty()) str << extra_label << " ";
  str << (codegen_enabled ? "Codegen Enabled" : "Codegen Disabled");
  if (!extra_info.empty()) str << ": " + extra_info;
  return str.str();
}

}
