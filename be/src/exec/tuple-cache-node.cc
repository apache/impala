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

#include "exec/exec-node-util.h"
#include "exec/tuple-cache-node.h"
#include "exec/tuple-file-reader.h"
#include "exec/tuple-file-writer.h"
#include "exec/tuple-text-file-reader.h"
#include "exec/tuple-text-file-util.h"
#include "exec/tuple-text-file-writer.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-cache-mgr.h"
#include "util/hash-util.h"
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
  : ExecNode(pool, pnode, descs) {
}

TupleCacheNode::~TupleCacheNode() = default;

static bool TupleCacheVerificationEnabled(RuntimeState* state) {
  DCHECK(state != nullptr);
  return state->query_options().enable_tuple_cache_verification;
}

Status TupleCacheNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  num_hits_counter_ = ADD_COUNTER(runtime_profile(), "NumTupleCacheHits", TUnit::UNIT);
  num_halted_counter_ =
      ADD_COUNTER(runtime_profile(), "NumTupleCacheHalted", TUnit::UNIT);
  num_skipped_counter_ =
      ADD_COUNTER(runtime_profile(), "NumTupleCacheSkipped", TUnit::UNIT);

  // Compute the combined cache key by computing the fragment instance key and
  // fusing it with the compile time key.
  ComputeFragmentInstanceKey(state);
  combined_key_ = plan_node().tnode_->tuple_cache_node.compile_time_key + "_" +
      std::to_string(fragment_instance_key_);
  runtime_profile()->AddInfoString("Combined Key", combined_key_);

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
  handle_ = tuple_cache_mgr->Lookup(combined_key_, true);
  if (tuple_cache_mgr->IsAvailableForRead(handle_)) {
    if (tuple_cache_mgr->DebugDumpEnabled() && TupleCacheVerificationEnabled(state)) {
      // We need the original fragment id to construct the path for the reference debug
      // cache file. If it's missing from the metadata, we return an error status
      // immediately.
      string org_fragment_id = tuple_cache_mgr->GetFragmentIdForTupleCache(combined_key_);
      if (org_fragment_id.empty()) {
        return Status(TErrorCode::TUPLE_CACHE_INCONSISTENCY,
            Substitute("Metadata of tuple cache '$0' is missing for correctness check",
                combined_key_));
      }
      string ref_sub_dir;
      string sub_dir;
      string ref_file_path = GetDebugDumpPath(state, org_fragment_id, &ref_sub_dir);
      string file_path = GetDebugDumpPath(state, string(), &sub_dir);
      DCHECK_EQ(ref_sub_dir, sub_dir);
      DCHECK(!ref_sub_dir.empty());
      DCHECK(!ref_file_path.empty());
      DCHECK(!file_path.empty());
      // Create the subdirectory for the debug caches if needed.
      RETURN_IF_ERROR(tuple_cache_mgr->CreateDebugDumpSubdir(ref_sub_dir));
      // Open the writer for writing the tuple data from the cache entries to be
      // the reference cache data.
      debug_dump_text_writer_ref_ = make_unique<TupleTextFileWriter>(ref_file_path);
      RETURN_IF_ERROR(debug_dump_text_writer_ref_->Open());
      // Open the writer for writing the tuple data from children in GetNext() to
      // compare with the reference debug cache file.
      debug_dump_text_writer_ = make_unique<TupleTextFileWriter>(file_path);
      RETURN_IF_ERROR(debug_dump_text_writer_->Open());
    } else {
      reader_ = make_unique<TupleFileReader>(
          tuple_cache_mgr->GetPath(handle_), mem_tracker(), runtime_profile());
      Status status = reader_->Open(state);
      // Clear reader if it's not usable
      if (!status.ok()) {
        LOG(WARNING) << "Could not read cache entry for "
                     << tuple_cache_mgr->GetPath(handle_);
        reader_.reset();
      }
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
      VLOG_FILE << "Tuple Cache: skipped for " << combined_key_;
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

// Helper function to rename the bad file.
static void MoveBadDebugCacheFile(const string& file_path) {
  DCHECK(!file_path.empty());
  string new_path = file_path + DEBUG_TUPLE_CACHE_BAD_POSTFIX;
  int result = rename(file_path.c_str(), new_path.c_str());
  if (result != 0) {
    string error_msg = GetStrErrMsg();
    LOG(ERROR) << "Failed to move debug tuple cache file from " << file_path << " to "
               << new_path << ". Error message: " << error_msg << ": " << result;
  } else {
    LOG(INFO) << "Moved bad debug tuple cache file from " << file_path << " to "
              << new_path;
  }
}

// Move the debug dump cache after verification.
// If the verification passed, we clear the cache file.
// Otherwise, we will move the file with a "bad" postfix.
// The writer will be reset after the function.
static void MoveDebugCache(bool suc, unique_ptr<TupleTextFileWriter>& writer) {
  DCHECK(writer != nullptr);
  if (suc) {
    writer->Delete();
  } else {
    MoveBadDebugCacheFile(writer->GetPath());
  }
  writer.reset();
}

Status TupleCacheNode::VerifyAndMoveDebugCache(RuntimeState* state) {
  DCHECK(debug_dump_text_writer_ref_ != nullptr);
  DCHECK(ExecEnv::GetInstance()->tuple_cache_mgr()->DebugDumpEnabled());
  DCHECK(TupleCacheVerificationEnabled(state));
  if (debug_dump_text_writer_->IsEmpty()) {
    return Status::OK();
  }
  string ref_file_path = debug_dump_text_writer_ref_->GetPath();
  string dump_file_path = debug_dump_text_writer_->GetPath();
  bool passed = false;

  DCHECK(!ref_file_path.empty());
  DCHECK(!dump_file_path.empty());

  VLOG_FILE << "Verify debug tuple cache file ref_file_path: " << ref_file_path
            << " and dump_file_path: " << dump_file_path
            << " with cache key:" << combined_key_;

  // Fast path to verify the cache.
  Status verify_status =
      TupleTextFileUtil::VerifyDebugDumpCache(dump_file_path, ref_file_path, &passed);
  if (verify_status.ok() && !passed) {
    // Slow path to compare all rows in an order-insensitive way if the files are not the
    // same.
    verify_status = TupleTextFileUtil::VerifyRows(ref_file_path, dump_file_path);
    passed = verify_status.ok();
  }

  // Move or clear the file after verification.
  MoveDebugCache(passed, debug_dump_text_writer_ref_);
  MoveDebugCache(passed, debug_dump_text_writer_);
  return verify_status;
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
          VLOG_FILE << "Tuple Cache entry for " << combined_key_
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
          if (tuple_cache_mgr->DebugDumpEnabled()) {
            // Store the metadata whenever the debug dump path is set, regardless of
            // whether the correctness verification is enabled in the query option. This
            // is because the tuple cache eviction function does not account for the query
            // option when removing metadata. Keeping this consistent ensures proper
            // handling.
            tuple_cache_mgr->StoreMetadataForTupleCache(
                combined_key_, PrintId(state->fragment_instance_id()));
          }
          tuple_cache_mgr->CompleteWrite(move(handle_), bytes_written);
        } else {
          writer_->Abort();
          tuple_cache_mgr->AbortWrite(move(handle_), false);
        }
        writer_.reset();
      }
    }
    if (debug_dump_text_writer_) {
      RETURN_IF_ERROR(debug_dump_text_writer_->Write(output_row_batch));
      if (*eos) debug_dump_text_writer_->Commit();
    }
  }

  // Note: TupleCacheNode does not alter its child's output (or the equivalent
  // output from the cache), so it does not enforce its own limit on the output.
  // Any limit should be enforced elsewhere, and this code omits the logic
  // to enforce a limit.
  int num_rows_added = output_row_batch->num_rows() - num_rows_before;
  DCHECK_GE(num_rows_added, 0);
  IncrementNumRowsReturned(num_rows_added);
  if (*eos && debug_dump_text_writer_) {
    DCHECK(debug_dump_text_writer_ref_ != nullptr);
    TupleFileReader cache_reader(
        ExecEnv::GetInstance()->tuple_cache_mgr()->GetPath(handle_), mem_tracker(),
        runtime_profile());
    RETURN_IF_ERROR(cache_reader.Open(state));
    // Read the cache entries from the cache reader, and write as the reference
    // debug cache file. If an error occurs, abort the verification, and return the
    // error status.
    RowBatch row_batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
    bool cache_eos = false;
    while (!cache_eos) {
      RETURN_IF_ERROR(
          cache_reader.GetNext(state, buffer_pool_client(), &row_batch, &cache_eos));
      DCHECK(row_batch.num_rows() > 0 || cache_eos);
      RETURN_IF_ERROR(debug_dump_text_writer_ref_->Write(&row_batch));
      row_batch.Reset();
    }
    DCHECK(cache_eos);
    debug_dump_text_writer_ref_->Commit();
    RETURN_IF_ERROR(VerifyAndMoveDebugCache(state));
  }
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
  if (debug_dump_text_writer_) {
    debug_dump_text_writer_->Delete();
    debug_dump_text_writer_.reset();
  }
  if (debug_dump_text_writer_ref_) {
    debug_dump_text_writer_ref_->Delete();
    debug_dump_text_writer_ref_.reset();
  }
  ReleaseResult();
  ExecNode::Close(state);
}

string TupleCacheNode::GetDebugDumpPath(const RuntimeState* state,
    const string& org_fragment_id, string* sub_dir_full_path) const {
  // The name of the subdirectory is hash key.
  // For non-reference files, the file name is the fragment instance id.
  // For reference files, the name includes the current fragment instance id, the original
  // fragment id, and a "ref" suffix.
  string file_name = PrintId(state->fragment_instance_id());
  if (!org_fragment_id.empty()) {
    // Adds the original fragment id of the cache to the path for debugging purpose.
    file_name += "_" + org_fragment_id + "_ref";
  }
  return ExecEnv::GetInstance()->tuple_cache_mgr()->GetDebugDumpPath(
      combined_key_, file_name, sub_dir_full_path);
}

void TupleCacheNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "TupleCacheNode(" << combined_key_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

void TupleCacheNode::ComputeFragmentInstanceKey(const RuntimeState* state) {
  const PlanFragmentInstanceCtxPB& ctx = state->instance_ctx_pb();
  uint32_t hash = 0;
  for (int32_t node_id : plan_node().tnode_->tuple_cache_node.input_scan_node_ids) {
    auto ranges = ctx.per_node_scan_ranges().find(node_id);
    if (ranges == ctx.per_node_scan_ranges().end()) continue;
    for (const ScanRangeParamsPB& params : ranges->second.scan_ranges()) {
      // This only supports HDFS right now
      DCHECK(params.scan_range().has_hdfs_file_split());
      const HdfsFileSplitPB& split = params.scan_range().hdfs_file_split();
      if (split.has_relative_path() && !split.relative_path().empty()) {
        hash = HashUtil::Hash(
            split.relative_path().data(), split.relative_path().length(), hash);
        DCHECK(split.has_partition_path_hash());
        int32_t partition_path_hash = split.partition_path_hash();
        hash = HashUtil::Hash(&partition_path_hash, sizeof(partition_path_hash), hash);
      } else if (split.has_absolute_path() && !split.absolute_path().empty()) {
        hash = HashUtil::Hash(
            split.absolute_path().data(), split.absolute_path().length(), hash);
      } else {
        DCHECK("Either relative_path or absolute_path must be set");
      }
      DCHECK(split.has_offset());
      int64_t offset = split.offset();
      hash = HashUtil::Hash(&offset, sizeof(offset), hash);
      DCHECK(split.has_length());
      int64_t length = split.length();
      hash = HashUtil::Hash(&length, sizeof(length), hash);
      DCHECK(split.has_mtime());
      int64_t mtime = split.mtime();
      hash = HashUtil::Hash(&mtime, sizeof(mtime), hash);
    }
  }
  fragment_instance_key_ = hash;
}

}
