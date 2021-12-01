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

#include "exec/join-builder.h"

#include "service/hs2-util.h"
#include "util/debug-util.h"
#include "util/min-max-filter.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

Status JoinBuilderConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(DataSinkConfig::Init(tsink, input_row_desc, state));
  join_node_id_ = tsink.join_build_sink.dest_node_id;
  join_op_ = tsink.join_build_sink.join_op;
  return Status::OK();
}

JoinBuilder::JoinBuilder(TDataSinkId sink_id, const JoinBuilderConfig& sink_config,
    const string& name, RuntimeState* state)
  : DataSink(sink_id, sink_config, name, state),
    join_node_id_(sink_config.join_node_id_),
    join_op_(sink_config.join_op_),
    is_separate_build_(sink_id != -1),
    num_probe_threads_(
        is_separate_build_ ? state->instance_ctx().num_join_build_outputs : 1) {}

JoinBuilder::~JoinBuilder() {
  DCHECK_EQ(0, probe_refcount_);
}

void JoinBuilder::CloseFromProbe(RuntimeState* join_node_state) {
  if (is_separate_build_) {
    bool last_probe;
    {
      unique_lock<mutex> l(separate_build_lock_);
      --probe_refcount_;
      last_probe = probe_refcount_ == 0;
      VLOG(3) << "JoinBuilder (id=" << join_node_id_ << ")"
              << "closed from finstance "
              << PrintId(join_node_state->fragment_instance_id())
              << "probe_refcount_=" << probe_refcount_;
      DCHECK_GE(probe_refcount_, 0);
    }
    // Only need to notify when the probe count is zero.
    if (last_probe) build_wakeup_cv_.NotifyAll();
  } else {
    Close(join_node_state);
  }
}

Status JoinBuilder::WaitForInitialBuild(RuntimeState* join_node_state) {
  DCHECK(is_separate_build_);
  join_node_state->AddCancellationCV(&separate_build_lock_, &probe_wakeup_cv_);
  VLOG(2) << "JoinBuilder (id=" << join_node_id_ << ")"
          << " WaitForInitialBuild() called by finstance "
          << PrintId(join_node_state->fragment_instance_id());
  unique_lock<mutex> l(separate_build_lock_);
  // Wait until either the build is ready to use or this finstance has been cancelled.
  // We can't safely pick up the build side if the build side was cancelled - instead we
  // need to wait for this finstance to be cancelled.
  while (!ready_to_probe_ && !join_node_state->is_cancelled()) {
    probe_wakeup_cv_.Wait(l);
  }
  if (join_node_state->is_cancelled()) {
    VLOG(2) << "Finstance " << PrintId(join_node_state->fragment_instance_id())
            << " cancelled while waiting for JoinBuilder (id=" << join_node_id_ << ")";
    return Status::CANCELLED;
  }
  ++probe_refcount_;
  --outstanding_probes_;
  VLOG(2) << "JoinBuilder (id=" << join_node_id_ << ")"
          << " initial build handoff to finstance "
          << PrintId(join_node_state->fragment_instance_id())
          << " probe_refcount_=" << probe_refcount_
          << " outstanding_probes_=" << outstanding_probes_;
  DCHECK_GE(outstanding_probes_, 0);
  return Status::OK();
}

void JoinBuilder::HandoffToProbesAndWait(RuntimeState* build_side_state) {
  DCHECK(is_separate_build_) << "Doesn't make sense for embedded builder.";
  VLOG(2) << "Initial build ready JoinBuilder (id=" << join_node_id_ << ")";
  build_side_state->AddCancellationCV(&separate_build_lock_, &build_wakeup_cv_);
  {
    unique_lock<mutex> l(separate_build_lock_);
    ready_to_probe_ = true;
    outstanding_probes_ = num_probe_threads_;
    DCHECK_GE(outstanding_probes_, 1);
    VLOG(3) << "JoinBuilder (id=" << join_node_id_ << ")"
            << " waiting for " << outstanding_probes_ << " probes.";
    probe_wakeup_cv_.NotifyAll();
    while (probe_refcount_ > 0
        || (outstanding_probes_ > 0 && !build_side_state->is_cancelled())) {
      SCOPED_TIMER(profile_->inactive_timer());
      VLOG(3) << "JoinBuilder (id=" << join_node_id_ << ") waiting"
              << " probe_refcount_=" << probe_refcount_
              << " outstanding_probes_=" << outstanding_probes_
              << " cancelled=" << build_side_state->is_cancelled();
      build_wakeup_cv_.Wait(l);
    }
    // Don't let probe side pick up the builder when we're going to clean it up.
    // Query cancellation will propagate to the probe finstance.
    ready_to_probe_ = !build_side_state->is_cancelled();
    VLOG(2) << "JoinBuilder (id=" << join_node_id_ << ") all probes complete. "
            << " probe_refcount_=" << probe_refcount_
            << " outstanding_probes_=" << outstanding_probes_
            << " cancelled=" << build_side_state->is_cancelled();
  }
}

void JoinBuilder::PublishRuntimeFilters(const std::vector<FilterContext>& filter_ctxs,
    RuntimeState* runtime_state, float minmax_filter_threshold, int64_t num_build_rows) {
  VLOG(3) << name() << " publishing "
          << filter_ctxs.size() << " filters.";
  int32_t num_enabled_filters = 0;
  for (const FilterContext& ctx : filter_ctxs) {
    BloomFilter* bloom_filter = nullptr;
    if (ctx.local_bloom_filter != nullptr) {
      bloom_filter = ctx.local_bloom_filter;
      ++num_enabled_filters;
    } else if (ctx.local_min_max_filter != nullptr) {
      /// Apply the column min/max stats (if applicable) to shut down the min/max
      /// filter early by setting always true flag for the filter. Do this only if
      /// the min/max filter is too close in area to the column stats of all target
      /// scan columns.
      const TRuntimeFilterDesc& filter_desc = ctx.filter->filter_desc();
      VLOG(3) << "Check out the usefulness of the local minmax filter:"
              << " id=" << ctx.filter->id()
              << ", filter details=" << ctx.local_min_max_filter->DebugString()
              << ", column stats:"
              << " low=" << PrintTColumnValue(filter_desc.targets[0].low_value)
              << ", high=" << PrintTColumnValue(filter_desc.targets[0].high_value)
              << ", threshold=" << minmax_filter_threshold
              << ", #targets=" << filter_desc.targets.size();
      bool all_overlap = true;
      for (const auto& target_desc : filter_desc.targets) {
        if (!FilterContext::ShouldRejectFilterBasedOnColumnStats(
                target_desc, ctx.local_min_max_filter, minmax_filter_threshold)) {
          all_overlap = false;
          break;
        }
      }
      if (all_overlap) {
        ctx.local_min_max_filter->SetAlwaysTrue();
        VLOG(3) << "The local minmax filter is set to always true:"
                << " id=" << ctx.filter->id();
      }

      if (!ctx.local_min_max_filter->AlwaysTrue()) {
        ++num_enabled_filters;
      }
    } else if (ctx.local_in_list_filter != nullptr) {
      if (!ctx.local_in_list_filter->AlwaysTrue()) {
        ++num_enabled_filters;
      }
    }

    runtime_state->filter_bank()->UpdateFilterFromLocal(ctx.filter->id(),
        bloom_filter, ctx.local_min_max_filter, ctx.local_in_list_filter);

    if (ctx.local_min_max_filter != nullptr) {
      VLOG(3) << name() << " published min/max filter: "
              << " id=" << ctx.filter->id()
              << ", details=" << ctx.local_min_max_filter->DebugString();
    }
  }

  if (filter_ctxs.size() > 0) {
    string info_string;
    if (num_enabled_filters == filter_ctxs.size()) {
      info_string = Substitute("$0 of $0 Runtime Filter$1 Published", filter_ctxs.size(),
          filter_ctxs.size() == 1 ? "" : "s");
    } else {
      info_string = Substitute("$0 of $1 Runtime Filter$2 Published, $3 Disabled",
          num_enabled_filters, filter_ctxs.size(), filter_ctxs.size() == 1 ? "" : "s",
          filter_ctxs.size() - num_enabled_filters);
    }
    profile()->AddInfoString("Runtime filters", info_string);
  }
}
} // namespace impala
