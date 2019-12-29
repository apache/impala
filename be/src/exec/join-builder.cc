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

#include "common/names.h"

namespace impala {

Status JoinBuilderConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, RuntimeState* state) {
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
  join_node_state->AddCancellationCV(&probe_wakeup_cv_);
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
  build_side_state->AddCancellationCV(&build_wakeup_cv_);
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
} // namespace impala
