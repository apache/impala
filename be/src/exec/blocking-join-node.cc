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

#include "exec/blocking-join-node.h"

#include <algorithm>
#include <sstream>

#include "exec/join-builder.h"
#include "exprs/scalar-expr.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/thread-resource-mgr.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/uid-util.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;

const char* BlockingJoinNode::LLVM_CLASS_NAME = "class.impala::BlockingJoinNode";

Status BlockingJoinPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  DCHECK(tnode.__isset.join_node);
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  join_op_ = tnode_->join_node.join_op;
  build_row_desc_ = state->obj_pool()->Add(
      new RowDescriptor(state->desc_tbl(), tnode.join_node.build_tuples,
        tnode.join_node.nullable_build_tuples));
  DCHECK(!IsSemiJoin(tnode.join_node.join_op) || conjuncts_.size() == 0);
  return Status::OK();
}

BlockingJoinNode::BlockingJoinNode(const string& node_name, ObjectPool* pool,
    const BlockingJoinPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    node_name_(node_name),
    join_op_(pnode.join_op()),
    eos_(false),
    probe_side_eos_(false),
    probe_batch_pos_(-1),
    current_probe_row_(NULL),
    semi_join_staging_row_(NULL) {}

BlockingJoinNode::~BlockingJoinNode() {
  // probe_batch_ must be cleaned up in Close() to ensure proper resource freeing.
  DCHECK(probe_batch_ == NULL);
}

Status BlockingJoinNode::Prepare(RuntimeState* state) {
  DCHECK_EQ(UseSeparateBuild(state->query_options()) ? 1 : 2, children_.size());
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  probe_timer_ = ADD_TIMER(runtime_profile(), "ProbeTime");
  probe_row_counter_ = ADD_COUNTER(runtime_profile(), "ProbeRows", TUnit::UNIT);

  // The right child (if present) must match the build row layout.
  DCHECK(children_.size() == 1 || build_row_desc().Equals(*children_[1]->row_desc()))
      << build_row_desc().DebugString() << " " << children_[1]->row_desc()->DebugString();
  // Validate the row desc layout is what we expect because the current join
  // implementation relies on it to enable some optimizations.
  int num_probe_tuples = probe_row_desc().tuple_descriptors().size();
  int num_build_tuples = build_row_desc().tuple_descriptors().size();

#ifndef NDEBUG
  switch (join_op_) {
    case TJoinOp::LEFT_ANTI_JOIN:
    case TJoinOp::LEFT_SEMI_JOIN:
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
    case TJoinOp::ICEBERG_DELETE_JOIN: {
      // Only return the surviving probe-side tuples.
      DCHECK(row_desc()->Equals(probe_row_desc()));
      break;
    }
    case TJoinOp::RIGHT_ANTI_JOIN:
    case TJoinOp::RIGHT_SEMI_JOIN: {
      // Only return the surviving build-side tuples.
      DCHECK(row_desc()->Equals(build_row_desc()));
      break;
    }
    default: {
      // The join node returns a row that is a concatenation of the left side and build
      // side row desc's. For example if the probe row had 1 tuple and the build row had
      // 2, the resulting row desc of the join node would have 3 tuples with:
      //   result[0] = left[0]
      //   result[1] = build[0]
      //   result[2] = build[1]
      for (int i = 0; i < num_probe_tuples; ++i) {
        TupleDescriptor* desc = probe_row_desc().tuple_descriptors()[i];
        DCHECK_EQ(i, row_desc()->GetTupleIdx(desc->id()));
      }
      for (int i = 0; i < num_build_tuples; ++i) {
        TupleDescriptor* desc = build_row_desc().tuple_descriptors()[i];
        DCHECK_EQ(num_probe_tuples + i, row_desc()->GetTupleIdx(desc->id()))
            << row_desc()->DebugString() << "\n" << probe_row_desc().DebugString() << "\n"
            << build_row_desc().DebugString();
      }
      break;
    }
  }
#endif

  probe_tuple_row_size_ = num_probe_tuples * sizeof(Tuple*);
  build_tuple_row_size_ = num_build_tuples * sizeof(Tuple*);

  if (IsSemiJoin(join_op_)) {
    semi_join_staging_row_ = reinterpret_cast<TupleRow*>(
        new char[probe_tuple_row_size_ + build_tuple_row_size_]);
  }

  build_batch_.reset(new RowBatch(&build_row_desc(), state->batch_size(), mem_tracker()));
  probe_batch_.reset(new RowBatch(&probe_row_desc(), state->batch_size(), mem_tracker()));
  return Status::OK();
}

Status BlockingJoinNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  probe_batch_->TransferResourceOwnership(row_batch);
  return ExecNode::Reset(state, row_batch);
}

void BlockingJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  build_batch_.reset();
  probe_batch_.reset();
  if (semi_join_staging_row_ != NULL) delete[] semi_join_staging_row_;
  ExecNode::Close(state);
}

void BlockingJoinNode::ProcessBuildInputAsync(
    RuntimeState* state, JoinBuilder* build_sink, Status* status) {
  DCHECK(!UseSeparateBuild(state->query_options()));
  DCHECK(status != nullptr);
  SCOPED_THREAD_COUNTER_MEASUREMENT(state->total_thread_statistics());
  {
    SCOPED_CONCURRENT_STOP_WATCH(&built_probe_overlap_stop_watch_);
    *status = child(1)->Open(state);
  }
  if (status->ok()) *status = AcquireResourcesForBuild(state);
  if (status->ok()) *status = SendBuildInputToSink<true>(state, build_sink);
  // IMPALA-1863: If the build-side thread failed, then we need to close the right
  // (build-side) child to avoid a potential deadlock between fragment instances.  This
  // is safe to do because while the build may have partially completed, it will not be
  // probed. BlockingJoinNode::Open() will return failure as soon as child(0)->Open()
  // completes.
  if (CanCloseBuildEarly() || !status->ok()) {
    // Release resources in 'build_batch_' and 'build_sink' before closing the children
    // as some of the resources are still accounted towards the children node.
    build_batch_.reset();
    if (!status->ok()) build_sink->Close(state);
    child(1)->Close(state);
  }

  // Release the thread token as soon as possible (before the main thread joins
  // on it).  This way, if we had a chain of 10 joins using 1 additional thread,
  // we'd keep the additional thread busy the whole time.
  state->resource_pool()->ReleaseThreadToken(false);
}

Status BlockingJoinNode::OpenImpl(RuntimeState* state, JoinBuilder** separate_builder) {
  RETURN_IF_ERROR(ExecNode::Open(state));
  eos_ = false;
  probe_side_eos_ = false;

  if (open_called_) return Status::OK();
  // The below code should only run once.
  if (UseSeparateBuild(state->query_options())) {
    // Find the input fragment's build sink. We do this in the Open() phase so we don't
    // block this finstance's Prepare() phase on the build finstance's Prepare() phase.
    const google::protobuf::RepeatedPtrField<JoinBuildInputPB>& build_inputs =
        state->instance_ctx_pb().join_build_inputs();
    auto it = std::find_if(build_inputs.begin(), build_inputs.end(),
        [this](const JoinBuildInputPB& bi) { return bi.join_node_id() == id_; });
    DCHECK(it != build_inputs.end());
    FragmentInstanceState* build_finstance;
    TUniqueId input_finstance_id;
    UniqueIdPBToTUniqueId(it->input_finstance_id(), &input_finstance_id);
    RETURN_IF_ERROR(
        state->query_state()->GetFInstanceState(input_finstance_id, &build_finstance));
    TDataSinkType::type build_sink_type = build_finstance->fragment().output_sink.type;
    DCHECK(IsJoinBuildSink(build_sink_type));
    *separate_builder = build_finstance->GetJoinBuildSink();
    DCHECK(*separate_builder != nullptr);
  } else {
    // The integrated join build requires some tricky time accounting because two
    // threads execute concurrently with the time from the left and right child
    // overlapping. We also want to count the builder profile as local time.
    // The separate join build does not have this problem, because
    // the build is executed in a separate fragment with a separate profile tree.
    runtime_profile_->AddLocalTimeCounter(bind<int64_t>(
        &BlockingJoinNode::LocalTimeCounterFn, runtime_profile_->total_time_counter(),
        child(0)->runtime_profile()->total_time_counter(),
        child(1)->runtime_profile()->total_time_counter(),
        &built_probe_overlap_stop_watch_));
  }
  open_called_ = true;
  return Status::OK();
}

Status BlockingJoinNode::ProcessBuildInputAndOpenProbe(
    RuntimeState* state, JoinBuilder* build_sink) {
  // This function implements various strategies for executing Open() on the left child
  // and doing or waiting for the join build. Some allow the two to proceed in parallel,
  // while others do them serially. Generally parallelism yields better performance
  // except inside a subplan. There we expect Open() to be called a number of times
  // proportional to the input data of the SubplanNode, so processing the build input
  // in the main thread can be more efficient, assuming that thread creation is expensive
  // relative to a single subplan iteration.
  //
  // In this block, we also compute the 'overlap' time for the left and right child. This
  // is the time (i.e. clock reads) when the right child stops overlapping with the left
  // child. For the single threaded case, the left and right child never overlap. For the
  // build side in a different thread, the overlap stops when the left child Open()
  // returns.
  if (UseSeparateBuild(state->query_options())) {
    // Open the left child in parallel before waiting for the build fragment to maximise
    // parallelism. The build execution is done concurrently by the build finstance's
    // thread.
    RETURN_IF_ERROR(child(0)->Open(state));
    // AcquireResourcesForBuild() opens the buffer pool client, so that probe reservation
    // can be transferred.
    RETURN_IF_ERROR(AcquireResourcesForBuild(state));
    {
      SCOPED_TIMER(runtime_profile_->inactive_timer());
      events_->MarkEvent("Waiting for initial build");
      RETURN_IF_ERROR(build_sink->WaitForInitialBuild(state));
      events_->MarkEvent("Initial build available");
    }
    waited_for_build_ = true;
  } else if (!IsInSubplan() && state->resource_pool()->TryAcquireThreadToken()) {
    // The build is integrated into the join node and we got a thread token. Do the hash
    // table build in a separate thread.
    Status build_side_status;
    runtime_profile()->AppendExecOption("Join Build-Side Prepared Asynchronously");
    string thread_name = Substitute("join-build-thread (finst:$0, plan-node-id:$1)",
        PrintId(state->fragment_instance_id()), id());
    unique_ptr<Thread> build_thread;
    Status thread_status = Thread::Create(FragmentInstanceState::FINST_THREAD_GROUP_NAME,
        thread_name, [this, state, build_sink, status=&build_side_status]() {
          ProcessBuildInputAsync(state, build_sink, status);
        }, &build_thread, true);
    if (!thread_status.ok()) {
      state->resource_pool()->ReleaseThreadToken(false);
      return thread_status;
    }
    // Open the left child so that it may perform any initialisation in parallel.
    // Don't exit even if we see an error, we still need to wait for the build thread
    // to finish.
    Status open_status = child(0)->Open(state);

    // The left/right child overlap stops here.
    built_probe_overlap_stop_watch_.SetTimeCeiling();

    // Blocks until ProcessBuildInput has returned, after which the build side structures
    // are fully constructed.
    build_thread->Join();
    RETURN_IF_ERROR(build_side_status);
    RETURN_IF_ERROR(open_status);
  } else if (IsInSubplan()) {
    // When inside a subplan, open the first child before doing the build such that
    // UnnestNodes on the probe side are opened and project their unnested collection
    // slots. Otherwise, the build might unnecessarily deep-copy those collection slots,
    // and this node would return them in GetNext().
    // TODO: Remove this special-case behavior for subplans once we have proper
    // projection. See UnnestNode for details on the current projection implementation.
    RETURN_IF_ERROR(child(0)->Open(state));
    RETURN_IF_ERROR(child(1)->Open(state));
    RETURN_IF_ERROR(AcquireResourcesForBuild(state));
    RETURN_IF_ERROR(SendBuildInputToSink<false>(state, build_sink));
  } else {
    // The left/right child never overlap. The overlap stops here.
    built_probe_overlap_stop_watch_.SetTimeCeiling();
    // Open the build side before acquiring our own resources so that the build side
    // can release any resources only used during its Open().
    RETURN_IF_ERROR(child(1)->Open(state));
    RETURN_IF_ERROR(AcquireResourcesForBuild(state));
    RETURN_IF_ERROR(SendBuildInputToSink<false>(state, build_sink));
    if (CanCloseBuildEarly()) {
      // Release resources in 'build_batch_' before closing the children as some of the
      // resources are still accounted towards the children node.
      build_batch_.reset();
      child(1)->Close(state);
    }
    RETURN_IF_ERROR(child(0)->Open(state));
  }
  return Status::OK();
}

Status BlockingJoinNode::GetFirstProbeRow(RuntimeState* state) {
  DCHECK(!probe_side_eos_);
  DCHECK_EQ(probe_batch_->num_rows(), 0);
  while (true) {
    RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
    COUNTER_ADD(probe_row_counter_, probe_batch_->num_rows());
    probe_batch_pos_ = 0;
    if (probe_batch_->num_rows() > 0) {
      current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
      return Status::OK();
    } else if (probe_side_eos_) {
      // If the probe side is exhausted, set the eos_ to true for only those
      // join modes that don't need to process unmatched build rows.
      eos_ = !NeedToProcessUnmatchedBuildRows(join_op_);
      return Status::OK();
    }
    probe_batch_->Reset();
  }
}

template <bool ASYNC_BUILD>
Status BlockingJoinNode::SendBuildInputToSink(
    RuntimeState* state, JoinBuilder* build_sink) {
  DCHECK(!UseSeparateBuild(state->query_options()));
  RETURN_IF_ERROR(build_sink->Open(state));
  DCHECK_EQ(build_batch_->num_rows(), 0);
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    {
      CONDITIONAL_SCOPED_CONCURRENT_STOP_WATCH(
          &built_probe_overlap_stop_watch_, ASYNC_BUILD);
      RETURN_IF_ERROR(child(1)->GetNext(state, build_batch_.get(), &eos));
    }
    RETURN_IF_ERROR(build_sink->Send(state, build_batch_.get()));
    build_batch_->Reset();
  } while (!eos);
  RETURN_IF_ERROR(build_sink->FlushFinal(state));
  return Status::OK();
}

void BlockingJoinNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << node_name_;
  *out << "(eos=" << (eos_ ? "true" : "false")
       << " probe_batch_pos=" << probe_batch_pos_;
  AddToDebugString(indentation_level, out);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

string BlockingJoinNode::GetLeftChildRowString(TupleRow* row) {
  stringstream out;
  out << "[";
  int num_probe_tuple_rows = child(0)->row_desc()->tuple_descriptors().size();
  for (int i = 0; i < row_desc()->tuple_descriptors().size(); ++i) {
    if (i != 0) out << " ";
    if (i >= num_probe_tuple_rows) {
      // Build row is not yet populated, print NULL
      out << PrintTuple(NULL, *row_desc()->tuple_descriptors()[i]);
    } else {
      out << PrintTuple(row->GetTuple(i), *row_desc()->tuple_descriptors()[i]);
    }
  }
  out << "]";
  return out.str();
}

int64_t BlockingJoinNode::LocalTimeCounterFn(const RuntimeProfile::Counter* total_time,
    const RuntimeProfile::Counter* left_child_time,
    const RuntimeProfile::Counter* right_child_time,
    const ConcurrentStopWatch* child_overlap_timer) {
  int64_t local_time = total_time->value() - left_child_time->value() -
      (right_child_time->value() - child_overlap_timer->TotalRunningTime());
  // While the calculation is correct at the end of the execution, counter value
  // and the stop watch reading is not accurate during execution.
  // If the child time counter is updated before the parent time counter, then the child
  // time will be greater. Stop watch is not thread safe, which can return invalid value.
  // Don't return a negative number in those cases.
  return ::max<int64_t>(0, local_time);
}
