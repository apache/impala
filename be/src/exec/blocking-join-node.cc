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

#include "exec/blocking-join-node.h"

#include <sstream>

#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

BlockingJoinNode::BlockingJoinNode(const string& node_name, const TJoinOp::type join_op,
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    node_name_(node_name),
    join_op_(join_op) {
}

Status BlockingJoinNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  return Status::OK;
}

BlockingJoinNode::~BlockingJoinNode() {
  // left_batch_ must be cleaned up in Close() to ensure proper resource freeing.
  DCHECK(left_batch_ == NULL);
}

Status BlockingJoinNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  build_pool_.reset(new MemPool(mem_tracker()));
  build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
  left_child_timer_ = ADD_TIMER(runtime_profile(), "LeftChildTime");
  build_row_counter_ = ADD_COUNTER(runtime_profile(), "BuildRows", TCounterType::UNIT);
  left_child_row_counter_ = ADD_COUNTER(runtime_profile(), "LeftChildRows",
      TCounterType::UNIT);

  result_tuple_row_size_ = row_descriptor_.tuple_descriptors().size() * sizeof(Tuple*);

  // pre-compute the tuple index of build tuples in the output row
  build_tuple_size_ = child(1)->row_desc().tuple_descriptors().size();
  build_tuple_idx_.reserve(build_tuple_size_);
  for (int i = 0; i < build_tuple_size_; ++i) {
    TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
    build_tuple_idx_.push_back(row_descriptor_.GetTupleIdx(build_tuple_desc->id()));
  }

  left_batch_.reset(new RowBatch(row_descriptor_, state->batch_size(), mem_tracker()));
  return Status::OK;
}

void BlockingJoinNode::Close(RuntimeState* state) {
  if (build_pool_.get() != NULL) build_pool_->FreeAll();
  left_batch_.reset();
  ExecNode::Close(state);
}

void BlockingJoinNode::BuildSideThread(RuntimeState* state, Promise<Status>* status) {
  Status s;
  {
    SCOPED_TIMER(state->total_cpu_timer());
    s = ConstructBuildSide(state);
  }
  // Release the thread token as soon as possible (before the main thread joins
  // on it).  This way, if we had a chain of 10 joins using 1 additional thread,
  // we'd keep the additional thread busy the whole time.
  state->resource_pool()->ReleaseThreadToken(false);
  status->Set(s);
}

Status BlockingJoinNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::OPEN, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  eos_ = false;

  // Kick-off the construction of the build-side table in a separate
  // thread, so that the left child can do any initialisation in parallel.
  // Only do this if we can get a thread token.  Otherwise, do this in the
  // main thread
  Promise<Status> build_side_status;
  if (state->resource_pool()->TryAcquireThreadToken()) {
    AddRuntimeExecOption("Join Build-Side Prepared Asynchronously");
    Thread build_thread(node_name_, "build thread",
        bind(&BlockingJoinNode::BuildSideThread, this, state, &build_side_status));
  } else {
    build_side_status.Set(ConstructBuildSide(state));
  }

  // Open the left child so that it may perform any initialisation in parallel.
  // Don't exit even if we see an error, we still need to wait for the build thread
  // to finish.
  Status open_status = child(0)->Open(state);

  // Blocks until ConstructBuildSide has returned, after which the build side structures
  // are fully constructed.
  RETURN_IF_ERROR(build_side_status.Get());
  RETURN_IF_ERROR(open_status);
  // Seed left child in preparation for GetNext().
  while (true) {
    RETURN_IF_ERROR(child(0)->GetNext(state, left_batch_.get(), &left_side_eos_));
    COUNTER_UPDATE(left_child_row_counter_, left_batch_->num_rows());
    left_batch_pos_ = 0;
    if (left_batch_->num_rows() == 0) {
      if (left_side_eos_) {
        InitGetNext(NULL /* eos */);
        eos_ = true;
        break;
      }
      left_batch_->Reset();
      continue;
    } else {
      current_left_child_row_ = left_batch_->GetRow(left_batch_pos_++);
      VLOG_ROW << "left child row: " << GetLeftChildRowString(current_left_child_row_);
      InitGetNext(current_left_child_row_);
      break;
    }
  }
  return Status::OK;
}

void BlockingJoinNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << node_name_;
  *out << "(eos=" << (eos_ ? "true" : "false")
       << " left_batch_pos=" << left_batch_pos_;
  AddToDebugString(indentation_level, out);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

string BlockingJoinNode::GetLeftChildRowString(TupleRow* row) {
  stringstream out;
  out << "[";
  int* build_tuple_idx_ptr_ = &build_tuple_idx_[0];
  for (int i = 0; i < row_desc().tuple_descriptors().size(); ++i) {
    if (i != 0) out << " ";

    int* is_build_tuple =
        ::find(build_tuple_idx_ptr_, build_tuple_idx_ptr_ + build_tuple_size_, i);

    if (is_build_tuple != build_tuple_idx_ptr_ + build_tuple_size_) {
      out << PrintTuple(NULL, *row_desc().tuple_descriptors()[i]);
    } else {
      out << PrintTuple(row->GetTuple(i), *row_desc().tuple_descriptors()[i]);
    }
  }
  out << "]";
  return out.str();
}

// This function is replaced by codegen
void BlockingJoinNode::CreateOutputRow(TupleRow* out, TupleRow* left, TupleRow* build) {
  if (left == NULL) {
    memset(out, 0, result_tuple_row_size_);
  } else {
    memcpy(out, left, result_tuple_row_size_);
  }

  if (build != NULL) {
    for (int i = 0; i < build_tuple_size_; ++i) {
      out->SetTuple(build_tuple_idx_[i], build->GetTuple(i));
    }
  } else {
    for (int i = 0; i < build_tuple_size_; ++i) {
      out->SetTuple(build_tuple_idx_[i], NULL);
    }
  }
}
