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

#include "exec/grouping-aggregator.h"

#include <set>
#include <sstream>
#include <gutil/strings/substitute.h>

#include "exec/exec-node.h"
#include "exec/hash-table.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

namespace impala {

typedef HashTable::BucketType BucketType;

GroupingAggregator::Partition::~Partition() {
  DCHECK(is_closed);
}

Status GroupingAggregator::Partition::InitStreams() {
  agg_fn_perm_pool.reset(new MemPool(parent->expr_mem_tracker_.get()));
  DCHECK_EQ(agg_fn_evals.size(), 0);
  AggFnEvaluator::ShallowClone(parent->partition_pool_.get(), agg_fn_perm_pool.get(),
      parent->expr_results_pool_.get(), parent->agg_fn_evals_, &agg_fn_evals);
  // Varlen aggregate function results are stored outside of aggregated_row_stream because
  // BufferedTupleStream doesn't support relocating varlen data stored in the stream.
  auto agg_slot =
      parent->intermediate_tuple_desc_->slots().begin() + parent->grouping_exprs_.size();
  std::set<SlotId> external_varlen_slots;
  for (; agg_slot != parent->intermediate_tuple_desc_->slots().end(); ++agg_slot) {
    if ((*agg_slot)->type().IsVarLenStringType()) {
      external_varlen_slots.insert((*agg_slot)->id());
    }
  }

  aggregated_row_stream.reset(
      new BufferedTupleStream(parent->state_, &parent->intermediate_row_desc_,
          parent->buffer_pool_client(), parent->resource_profile_.spillable_buffer_size,
          parent->resource_profile_.max_row_buffer_size, external_varlen_slots));
  RETURN_IF_ERROR(aggregated_row_stream->Init(parent->exec_node_->label(), true));
  bool got_buffer;
  RETURN_IF_ERROR(aggregated_row_stream->PrepareForWrite(&got_buffer));
  DCHECK(got_buffer) << "Buffer included in reservation " << parent->id_ << "\n"
                     << parent->buffer_pool_client()->DebugString() << "\n"
                     << parent->DebugString(2);
  if (!parent->is_streaming_preagg_) {
    unaggregated_row_stream.reset(
        new BufferedTupleStream(parent->state_, &parent->input_row_desc_,
            parent->buffer_pool_client(), parent->resource_profile_.spillable_buffer_size,
            parent->resource_profile_.max_row_buffer_size));
    // This stream is only used to spill, no need to ever have this pinned.
    RETURN_IF_ERROR(unaggregated_row_stream->Init(parent->exec_node_->label(), false));
    // Save memory by waiting until we spill to allocate the write buffer for the
    // unaggregated row stream.
    DCHECK(!unaggregated_row_stream->has_write_iterator());
  }
  return Status::OK();
}

Status GroupingAggregator::Partition::InitHashTable(bool* got_memory) {
  DCHECK(aggregated_row_stream != nullptr);
  DCHECK(hash_tbl == nullptr);
  // We use the upper PARTITION_FANOUT num bits to pick the partition so only the
  // remaining bits can be used for the hash table.
  // TODO: we could switch to 64 bit hashes and then we don't need a max size.
  // It might be reasonable to limit individual hash table size for other reasons
  // though. Always start with small buffers.
  hash_tbl.reset(HashTable::Create(parent->ht_allocator_.get(), false, 1, nullptr,
      1L << (32 - NUM_PARTITIONING_BITS), PAGG_DEFAULT_HASH_TABLE_SZ));
  // Please update the error message in CreateHashPartitions() if initial size of
  // hash table changes.
  Status status = hash_tbl->Init(got_memory);
  if (!status.ok()) {
    hash_tbl->Close();
    hash_tbl.reset();
  }
  return status;
}

Status GroupingAggregator::Partition::SerializeStreamForSpilling() {
  DCHECK(!parent->is_streaming_preagg_);
  if (parent->needs_serialize_) {
    // We need to do a lot more work in this case. This step effectively does a merge
    // aggregation in this node. We need to serialize the intermediates, spill the
    // intermediates and then feed them into the aggregate function's merge step.
    // This is often used when the intermediate is a string type, meaning the current
    // (before serialization) in-memory layout is not the on-disk block layout.
    // The disk layout does not support mutable rows. We need to rewrite the stream
    // into the on disk format.
    // TODO: if it happens to not be a string, we could serialize in place. This is
    // a future optimization since it is very unlikely to have a serialize phase
    // for those UDAs.
    DCHECK(parent->serialize_stream_.get() != nullptr);
    DCHECK(!parent->serialize_stream_->is_pinned());

    // Serialize and copy the spilled partition's stream into the new stream.
    Status status;
    BufferedTupleStream* new_stream = parent->serialize_stream_.get();
    HashTable::Iterator it = hash_tbl->Begin(parent->ht_ctx_.get());
    // Marks if we have used the large write page reservation. We only reclaim it after we
    // finish writing to 'new_stream', because there are no other works interleaving that
    // could occupy it.
    bool used_large_page_reservation = false;
    while (!it.AtEnd()) {
      Tuple* tuple = it.GetTuple<BucketType::MATCH_UNSET>();
      it.Next();
      AggFnEvaluator::Serialize(agg_fn_evals, tuple);
      TupleRow* row = reinterpret_cast<TupleRow*>(&tuple);
      if (UNLIKELY(!new_stream->AddRow(row, &status))) {
        bool row_is_added = false;
        if (status.ok()) {
          // Don't get enough unused reservation to add the large row. Restore the saved
          // reservation for a large write page and try again.
          DCHECK(!used_large_page_reservation
              && parent->large_write_page_reservation_.GetReservation() > 0)
              << "Run out of large page reservation in spilling " << DebugString()
              << "\nused_large_page_reservation=" << used_large_page_reservation << "\n"
              << parent->DebugString() << "\n"
              << parent->buffer_pool_client()->DebugString() << "\n"
              << new_stream->DebugString() << "\n"
              << this->aggregated_row_stream->DebugString();
          used_large_page_reservation = true;
          parent->RestoreLargeWritePageReservation();
          row_is_added = new_stream->AddRow(row, &status);
        }
        if (UNLIKELY(!row_is_added)) {
          if (status.ok()) {
            // Still fail to write the large row after restoring all the extra reservation
            // for the large page. We can't spill anything else to free some reservation
            // since we are currently spilling a partition. This indicates a bug that some
            // of the min reservation are used incorrectly.
            status = Status(TErrorCode::INTERNAL_ERROR, strings::Substitute(
                "Internal error: couldn't serialize a large row in $0 of $1, only had $2 "
                "bytes of unused reservation:\n$3", DebugString(), parent->DebugString(),
                parent->buffer_pool_client()->GetUnusedReservation(),
                parent->buffer_pool_client()->DebugString()));
          }
          // Even if we can't add to new_stream, finish up processing this agg stream to
          // make clean up easier (someone has to finalize this stream and we don't want
          // to remember where we are).
          parent->CleanupHashTbl(agg_fn_evals, it);
          hash_tbl->Close();
          hash_tbl.reset();
          aggregated_row_stream->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
          return status;
        }
      }
    }

    aggregated_row_stream->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
    aggregated_row_stream.swap(parent->serialize_stream_);
    // Save back the large write page reservation if we have restored it.
    if (used_large_page_reservation) parent->SaveLargeWritePageReservation();
    // Recreate the serialize_stream (and reserve 1 buffer) now in preparation for
    // when we need to spill again. We need to have this available before we need
    // to spill to make sure it is available. This should be acquirable since we just
    // freed at least one buffer from this partition's (old) aggregated_row_stream.
    parent->serialize_stream_.reset(
        new BufferedTupleStream(parent->state_, &parent->intermediate_row_desc_,
            parent->buffer_pool_client(), parent->resource_profile_.spillable_buffer_size,
            parent->resource_profile_.max_row_buffer_size));
    status = parent->serialize_stream_->Init(parent->exec_node_->label(), false);
    if (status.ok()) {
      bool got_buffer;
      status = parent->serialize_stream_->PrepareForWrite(&got_buffer);
      DCHECK(!status.ok() || got_buffer) << "Accounted in min reservation";
    }
    if (!status.ok()) {
      hash_tbl->Close();
      hash_tbl.reset();
      return status;
    }
    DCHECK(parent->serialize_stream_->has_write_iterator());
  }
  return Status::OK();
}

Status GroupingAggregator::Partition::Spill(bool more_aggregate_rows) {
  DCHECK(!parent->is_streaming_preagg_);
  DCHECK(!is_closed);
  DCHECK(!is_spilled());
  RETURN_IF_ERROR(parent->state_->StartSpilling(parent->mem_tracker_.get()));

  RETURN_IF_ERROR(SerializeStreamForSpilling());

  // Free the in-memory result data.
  AggFnEvaluator::Close(agg_fn_evals, parent->state_);
  agg_fn_evals.clear();

  if (agg_fn_perm_pool.get() != nullptr) {
    agg_fn_perm_pool->FreeAll();
    agg_fn_perm_pool.reset();
  }

  hash_tbl->Close();
  hash_tbl.reset();

  // Unpin the stream to free memory, but leave a write buffer in place so we can
  // continue appending rows to one of the streams in the partition.
  DCHECK(aggregated_row_stream->has_write_iterator());
  DCHECK(!unaggregated_row_stream->has_write_iterator());
  if (more_aggregate_rows) {
    RETURN_IF_ERROR(aggregated_row_stream->UnpinStream(
        BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
  } else {
    RETURN_IF_ERROR(aggregated_row_stream->UnpinStream(BufferedTupleStream::UNPIN_ALL));
    bool got_buffer;
    RETURN_IF_ERROR(unaggregated_row_stream->PrepareForWrite(&got_buffer));
    DCHECK(got_buffer) << "Accounted in min reservation"
                       << parent->buffer_pool_client()->DebugString();
  }

  COUNTER_ADD(parent->num_spilled_partitions_, 1);
  if (parent->num_spilled_partitions_->value() == 1) {
    parent->runtime_profile()->AppendExecOption("Spilled");
  }
  return Status::OK();
}

void GroupingAggregator::Partition::Close(bool finalize_rows) {
  if (is_closed) return;
  is_closed = true;
  if (aggregated_row_stream.get() != nullptr) {
    if (finalize_rows && hash_tbl.get() != nullptr) {
      // We need to walk all the rows and Finalize them here so the UDA gets a chance
      // to cleanup. If the hash table is gone (meaning this was spilled), the rows
      // should have been finalized/serialized in Spill().
      parent->CleanupHashTbl(agg_fn_evals, hash_tbl->Begin(parent->ht_ctx_.get()));
    }
    aggregated_row_stream->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }
  if (hash_tbl.get() != nullptr) {
    hash_tbl->StatsCountersAdd(parent->ht_stats_profile_.get());
    hash_tbl->Close();
  }
  if (unaggregated_row_stream.get() != nullptr) {
    unaggregated_row_stream->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }
  for (AggFnEvaluator* eval : agg_fn_evals) eval->Close(parent->state_);
  if (agg_fn_perm_pool.get() != nullptr) agg_fn_perm_pool->FreeAll();
}

string GroupingAggregator::Partition::DebugString() const {
  std::stringstream ss;
  ss << "Partition " << this << " (id=" << idx << ", level=" << level << ", is_spilled="
     << is_spilled() << ", is_closed=" << is_closed
     << ", aggregated_row_stream=" << aggregated_row_stream->DebugString()
     << ",\nunaggregated_row_stream=" << unaggregated_row_stream->DebugString() << ")";
  return ss.str();
}
} // namespace impala
