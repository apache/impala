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

#include <gflags/gflags.h>

#include "exec/tuple-cache-node.h"
#include "exec/exec-node-util.h"
#include "exec/tuple-file-reader.h"
#include "exec/tuple-file-writer.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-cache-mgr.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"

#include "common/names.h"

namespace impala {

Status TupleCachePlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new TupleCacheNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

TupleCacheNode::TupleCacheNode(
    ObjectPool* pool, const TupleCachePlanNode& pnode, const DescriptorTbl& descs)
    : ExecNode(pool, pnode, descs)
    , subtree_hash_(pnode.tnode_->tuple_cache_node.subtree_hash) {
}

TupleCacheNode::~TupleCacheNode() = default;

Status TupleCacheNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  num_hits_counter_ = ADD_COUNTER(runtime_profile(), "NumTupleCacheHits", TUnit::UNIT);
  num_halted_counter_ =
      ADD_COUNTER(runtime_profile(), "NumTupleCacheHalted", TUnit::UNIT);
  num_skipped_counter_ =
      ADD_COUNTER(runtime_profile(), "NumTupleCacheSkipped", TUnit::UNIT);
  return Status::OK();
}

Status TupleCacheNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));

  // The frontend cannot create a TupleCacheNode if enable_tuple_cache=false
  // Fail the query if we see this.
  if (!state->query_options().enable_tuple_cache) {
    return Status("Invalid tuple caching configuration: enable_tuple_cache=false");
  }

  TupleCacheMgr* tuple_cache_mgr = ExecEnv::GetInstance()->tuple_cache_mgr();
  handle_ = tuple_cache_mgr->Lookup(subtree_hash_, true);
  if (tuple_cache_mgr->IsAvailableForRead(handle_)) {
    reader_ = make_unique<TupleFileReader>(
        tuple_cache_mgr->GetPath(handle_), mem_tracker(), runtime_profile());
    Status status = reader_->Open(state);
    // Clear reader if it's not usable
    if (!status.ok()) {
      LOG(WARNING) << "Could not read cache entry for "
                   << tuple_cache_mgr->GetPath(handle_);
      reader_.reset();
    }
  } else if (tuple_cache_mgr->IsAvailableForWrite(handle_)) {
    writer_ = make_unique<TupleFileWriter>(tuple_cache_mgr->GetPath(handle_),
        mem_tracker(), runtime_profile(), tuple_cache_mgr->MaxSize());
    Status status = writer_->Open(state);
    if (!status.ok()) {
      LOG(WARNING) << "Could not write cache entry for "
                   << tuple_cache_mgr->GetPath(handle_);
      tuple_cache_mgr->AbortWrite(move(handle_), false);
      writer_.reset();
    }
  }

  if (reader_) {
    COUNTER_ADD(num_hits_counter_, 1);
    tuple_cache_mgr->IncrementMetric(TupleCacheMgr::MetricType::HIT);
  } else {
    if (!writer_) {
      // May be skipped due to any of:
      // - the query requests caching but cache is disabled via startup option
      // - another fragment is currently writing this cache entry
      // - the cache entry is a tombstone to prevent retries for too large entries
      VLOG_FILE << "Tuple Cache: skipped for " << subtree_hash_;
      COUNTER_ADD(num_skipped_counter_, 1);
      tuple_cache_mgr->IncrementMetric(TupleCacheMgr::MetricType::SKIPPED);
    }
    tuple_cache_mgr->IncrementMetric(TupleCacheMgr::MetricType::MISS);
    // No reader, so open the child.
    RETURN_IF_ERROR(child(0)->Open(state));
  }

  // Claim reservation after the child has been opened to reduce the peak reservation
  // requirement.
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }
  return Status::OK();
}

Status TupleCacheNode::GetNext(
    RuntimeState* state, RowBatch* output_row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  // Save the number of rows in case GetNext() is called with a non-empty batch,
  // which can happen in a subplan.
  int num_rows_before = output_row_batch->num_rows();

  // If we have a Reader, return the next batch from it.
  // Else GetNext from child, write to Writer, and return the batch.
  if (reader_) {
    Status status = reader_->GetNext(state, buffer_pool_client(), output_row_batch, eos);
    if (status.ok()) {
      cached_rowbatch_returned_to_caller_ = true;
    } else {
      // If we have returned a cached row batch to the caller, then it is not safe
      // to try to get any rows from the child as they could be duplicates. Any
      // error needs to end the query.
      if (cached_rowbatch_returned_to_caller_) return status;

      // We haven't returned a RowBatch to the caller yet, so we can recover by aborting
      // the read from the cache and fetching from the child. We won't try to write to
      // the cache.
      LOG(WARNING) << "Unable to read cache file: " << status.GetDetail()
                   << "Falling back to regular non-cached path.";
      reader_.reset();
      // If reader_ is set, then the child was never opened and needs to be opened now
      RETURN_IF_ERROR(child(0)->Open(state));
      RETURN_IF_ERROR(child(0)->GetNext(state, output_row_batch, eos));
    }
  } else {
    RETURN_IF_ERROR(child(0)->GetNext(state, output_row_batch, eos));
    if (writer_) {
      Status status = writer_->Write(state, output_row_batch);
      TupleCacheMgr* tuple_cache_mgr = ExecEnv::GetInstance()->tuple_cache_mgr();
      // If there was an error or we exceeded the file size limit, stop caching but
      // continue reading from the child node.
      if (!status.ok()) {
        if (writer_->ExceededMaxSize()) {
          VLOG_FILE << "Tuple Cache entry for " << subtree_hash_
                    << " hit the maximum file size: " << status.GetDetail();
          COUNTER_ADD(num_halted_counter_, 1);
          tuple_cache_mgr->IncrementMetric(TupleCacheMgr::MetricType::HALTED);
          writer_->Abort();
          tuple_cache_mgr->AbortWrite(move(handle_), true);
        } else {
          LOG(WARNING) << "Unable to write cache file: " << status.GetDetail();
          writer_->Abort();
          tuple_cache_mgr->AbortWrite(move(handle_), false);
        }
        writer_.reset();
      } else if (*eos) {
        // If we hit end of stream, then we can complete the cache entry
        // If the child did not reach end of stream, then it clearly isn't the complete
        // result set. This is currently the only way a cache entry can be completed.
        size_t bytes_written = writer_->BytesWritten();
        Status status = writer_->Commit(state);
        if (status.ok()) {
          tuple_cache_mgr->CompleteWrite(move(handle_), bytes_written);
        } else {
          writer_->Abort();
          tuple_cache_mgr->AbortWrite(move(handle_), false);
        }
        writer_.reset();
      }
    }
  }

  // Note: TupleCacheNode does not alter its child's output (or the equivalent
  // output from the cache), so it does not enforce its own limit on the output.
  // Any limit should be enforced elsewhere, and this code omits the logic
  // to enforce a limit.
  int num_rows_added = output_row_batch->num_rows() - num_rows_before;
  DCHECK_GE(num_rows_added, 0);
  IncrementNumRowsReturned(num_rows_added);
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

void TupleCacheNode::ReleaseResult() {
  reader_.reset();
  writer_.reset();
  handle_.reset();
}

Status TupleCacheNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  // Reset() is not supported.
  DCHECK(false) << "Internal error: Tuple cache nodes should not appear in subplans.";
  return Status("Internal error: Tuple cache nodes should not appear in subplans.");
}

void TupleCacheNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // If we reach this point with an open writer_, then this cache entry is invalid. We
  // will delete the file and abort the write. This can happen if the query is cancelled,
  // if the query hits an error, or if a parent node has a limit and doesn't complete
  // fetching. This is intentionally restrictive.
  if (writer_) {
    TupleCacheMgr* tuple_cache_mgr = ExecEnv::GetInstance()->tuple_cache_mgr();
    writer_->Abort();
    tuple_cache_mgr->AbortWrite(move(handle_), false);
  }
  ReleaseResult();
  ExecNode::Close(state);
}

void TupleCacheNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "TupleCacheNode(" << subtree_hash_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
