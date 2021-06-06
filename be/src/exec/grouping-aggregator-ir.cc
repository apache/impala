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

#include "exec/hash-table.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"

using namespace impala;

typedef HashTable::BucketType BucketType;

template <bool AGGREGATED_ROWS>
Status GroupingAggregator::AddBatchImpl(RowBatch* batch,
    TPrefetchMode::type prefetch_mode, HashTableCtx* __restrict__ ht_ctx,
    bool has_more_rows) {
  DCHECK(!hash_partitions_.empty());
  DCHECK(!is_streaming_preagg_);

  // Make sure that no resizes will happen when inserting individual rows to the hash
  // table of each partition by pessimistically assuming that all the rows in each batch
  // will end up to the same partition.
  // TODO: Once we have a histogram with the number of rows per partition, we will have
  // accurate resize calls.
  RETURN_IF_ERROR(
      CheckAndResizeHashPartitions(AGGREGATED_ROWS, batch->num_rows(), ht_ctx));

  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  const int cache_size = expr_vals_cache->capacity();
  const int num_rows = batch->num_rows();
  for (int group_start = 0; group_start < num_rows; group_start += cache_size) {
    EvalAndHashPrefetchGroup<AGGREGATED_ROWS>(batch, group_start, prefetch_mode, ht_ctx);

    FOREACH_ROW_LIMIT(batch, group_start, cache_size, batch_iter) {
      RETURN_IF_ERROR(ProcessRow<AGGREGATED_ROWS>(batch_iter.Get(), ht_ctx,
          has_more_rows));
      expr_vals_cache->NextRow();
    }
    DCHECK(expr_vals_cache->AtEnd());
  }
  return Status::OK();
}

template <bool AGGREGATED_ROWS>
void IR_ALWAYS_INLINE GroupingAggregator::EvalAndHashPrefetchGroup(RowBatch* batch,
    int start_row_idx, TPrefetchMode::type prefetch_mode, HashTableCtx* ht_ctx) {
  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  const int cache_size = expr_vals_cache->capacity();

  expr_vals_cache->Reset();
  FOREACH_ROW_LIMIT(batch, start_row_idx, cache_size, batch_iter) {
    TupleRow* row = batch_iter.Get();
    bool is_null;
    if (AGGREGATED_ROWS) {
      is_null = !ht_ctx->EvalAndHashBuild(row);
    } else {
      is_null = !ht_ctx->EvalAndHashProbe(row);
    }
    // Hoist lookups out of non-null branch to speed up non-null case.
    const uint32_t hash = expr_vals_cache->CurExprValuesHash();
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    HashTable* hash_tbl = GetHashTable(partition_idx);
    if (is_null) {
      expr_vals_cache->SetRowNull();
    } else if (prefetch_mode != TPrefetchMode::NONE) {
      if (LIKELY(hash_tbl != nullptr)) hash_tbl->PrefetchBucket<false>(hash);
    }
    expr_vals_cache->NextRow();
  }

  expr_vals_cache->ResetForRead();
}

template <bool AGGREGATED_ROWS>
Status GroupingAggregator::ProcessRow(
    TupleRow* __restrict__ row, HashTableCtx* __restrict__ ht_ctx, bool has_more_rows) {
  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  // Hoist lookups out of non-null branch to speed up non-null case.
  const uint32_t hash = expr_vals_cache->CurExprValuesHash();
  const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
  if (expr_vals_cache->IsRowNull()) return Status::OK();
  // To process this row, we first see if it can be aggregated or inserted into this
  // partition's hash table. If we need to insert it and that fails, due to OOM, we
  // spill the partition. The partition to spill is not necessarily dst_partition,
  // so we can try again to insert the row.
  HashTable* hash_tbl = GetHashTable(partition_idx);
  Partition* dst_partition = hash_partitions_[partition_idx];
  DCHECK(dst_partition != nullptr);
  DCHECK_EQ(dst_partition->is_spilled(), hash_tbl == nullptr);
  if (hash_tbl == nullptr) {
    // This partition is already spilled, just append the row.
    return AppendSpilledRow<AGGREGATED_ROWS>(dst_partition, row);
  }

  DCHECK(dst_partition->aggregated_row_stream->is_pinned());
  bool found;
  // Find the appropriate bucket in the hash table. There will always be a free
  // bucket because we checked the size above.
  HashTable::Iterator it =
      hash_tbl->FindBuildRowBucket<BucketType::MATCH_UNSET>(ht_ctx, &found);
  DCHECK(!it.AtEnd()) << "Hash table had no free buckets";
  if (AGGREGATED_ROWS) {
    // If the row is already an aggregate row, it cannot match anything in the
    // hash table since we process the aggregate rows first. These rows should
    // have been aggregated in the initial pass.
    DCHECK(!found);
  } else if (found) {
    // Row is already in hash table. Do the aggregation and we're done.
    UpdateTuple(
        dst_partition->agg_fn_evals.data(), it.GetTuple<BucketType::MATCH_UNSET>(), row);
    return Status::OK();
  }

  // If we are seeing this result row for the first time, we need to construct the
  // result row and initialize it.
  return AddIntermediateTuple<AGGREGATED_ROWS>(dst_partition, row, hash, it,
      has_more_rows);
}

template <bool AGGREGATED_ROWS>
Status GroupingAggregator::AddIntermediateTuple(Partition* __restrict__ partition,
    TupleRow* __restrict__ row, uint32_t hash, HashTable::Iterator insert_it,
    bool has_more_rows) {
  while (true) {
    DCHECK(partition->aggregated_row_stream->is_pinned());
    Tuple* intermediate_tuple = ConstructIntermediateTuple(partition->agg_fn_evals,
        partition->aggregated_row_stream.get(), &add_batch_status_);

    if (LIKELY(intermediate_tuple != nullptr)) {
      UpdateTuple(
          partition->agg_fn_evals.data(), intermediate_tuple, row, AGGREGATED_ROWS);
      // After copying and initializing the tuple, insert it into the hash table.
      insert_it.SetTuple(intermediate_tuple, hash);
      return Status::OK();
    } else if (!add_batch_status_.ok()) {
      return std::move(add_batch_status_);
    }

    // If we don't need to reserve extra space for the serialize stream, restore them
    // before spilling any partitions. One case is we don't need the serialize stream at
    // all. The other case is this is the last row to add, and 'partition' will be read
    // out and closed after adding this row, so no partitions need to be spilled.
    if ((!needs_serialize_ || !has_more_rows)
        && large_write_page_reservation_.GetReservation() > 0) {
      RestoreLargeWritePageReservation();
      continue;
    }

    // We did not have enough memory to add intermediate_tuple to the stream.
    RETURN_IF_ERROR(SpillPartition(AGGREGATED_ROWS));
    if (partition->is_spilled()) {
      return AppendSpilledRow<AGGREGATED_ROWS>(partition, row);
    }
  }
}

Status GroupingAggregator::AddBatchStreamingImpl(int agg_idx, bool needs_serialize,
    TPrefetchMode::type prefetch_mode, RowBatch* in_batch, RowBatch* out_batch,
    HashTableCtx* __restrict__ ht_ctx, int remaining_capacity[PARTITION_FANOUT]) {
  DCHECK(is_streaming_preagg_);
  DCHECK(!out_batch->AtCapacity());
  const int out_batch_start = out_batch->num_rows();
  RowBatch::Iterator out_batch_iterator(out_batch, out_batch_start);
  HashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  const int num_rows = in_batch->num_rows();
  const int cache_size = expr_vals_cache->capacity();
  for (int group_start = streaming_idx_; group_start < num_rows;
       group_start += cache_size) {
    EvalAndHashPrefetchGroup<false>(in_batch, group_start, prefetch_mode, ht_ctx);

    FOREACH_ROW_LIMIT(in_batch, group_start, cache_size, in_batch_iter) {
      // Hoist lookups out of non-null branch to speed up non-null case.
      TupleRow* in_row = in_batch_iter.Get();
      const uint32_t hash = expr_vals_cache->CurExprValuesHash();
      const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
      if (!expr_vals_cache->IsRowNull()
          && !TryAddToHashTable(ht_ctx, hash_partitions_[partition_idx],
                 GetHashTable(partition_idx), in_row, hash,
                 &remaining_capacity[partition_idx], &add_batch_status_)) {
        RETURN_IF_ERROR(std::move(add_batch_status_));
        // Tuple is not going into hash table, add it to the output batch.
        Tuple* intermediate_tuple = ConstructIntermediateTuple(
            agg_fn_evals_, out_batch->tuple_data_pool(), &add_batch_status_);
        if (UNLIKELY(intermediate_tuple == nullptr)) {
          DCHECK(!add_batch_status_.ok());
          return std::move(add_batch_status_);
        }
        UpdateTuple(agg_fn_evals_.data(), intermediate_tuple, in_row);
        out_batch_iterator.Get()->SetTuple(agg_idx, intermediate_tuple);
        out_batch_iterator.Next();
        out_batch->CommitLastRow();
        if (out_batch->AtCapacity() && in_batch_iter.RowNum() + 1 < num_rows) {
          streaming_idx_ = in_batch_iter.RowNum() + 1;
          goto ret;
        }
      }
      DCHECK(add_batch_status_.ok());
      expr_vals_cache->NextRow();
    }
    DCHECK(expr_vals_cache->AtEnd());
  }
  streaming_idx_ = 0;
ret:
  if (needs_serialize) {
    // We only serialize the added rows in this call. The rows before out_batch_start may
    // be added for other agg_idx values.
    FOREACH_ROW(out_batch, out_batch_start, out_batch_iter) {
      AggFnEvaluator::Serialize(agg_fn_evals_, out_batch_iter.Get()->GetTuple(agg_idx));
    }
  }

  return Status::OK();
}

bool GroupingAggregator::TryAddToHashTable(HashTableCtx* __restrict__ ht_ctx,
    Partition* __restrict__ partition, HashTable* __restrict__ hash_tbl,
    TupleRow* __restrict__ in_row, uint32_t hash, int* __restrict__ remaining_capacity,
    Status* status) {
  DCHECK(remaining_capacity != nullptr);
  DCHECK_EQ(hash_tbl, partition->hash_tbl.get());
  DCHECK_GE(*remaining_capacity, 0);
  bool found;
  Tuple* intermediate_tuple;
  // This is called from ProcessBatchStreaming() so the rows are not aggregated.
  HashTable::Iterator it =
      hash_tbl->FindBuildRowBucket<BucketType::MATCH_UNSET>(ht_ctx, &found);
  if (found) {
    intermediate_tuple = it.GetTuple<BucketType::MATCH_UNSET>();
  } else if (*remaining_capacity == 0) {
    return false;
  } else {
    intermediate_tuple = ConstructIntermediateTuple(
        partition->agg_fn_evals, partition->aggregated_row_stream.get(), status);
    if (LIKELY(intermediate_tuple != nullptr)) {
      it.SetTuple(intermediate_tuple, hash);
      --(*remaining_capacity);
    } else {
      // Avoid repeatedly trying to add tuples when under memory pressure.
      *remaining_capacity = 0;
      return false;
    }
  }

  UpdateTuple(partition->agg_fn_evals.data(), intermediate_tuple, in_row);
  return true;
}

// Instantiate required templates.
template Status GroupingAggregator::AddBatchImpl<false>(
    RowBatch*, TPrefetchMode::type, HashTableCtx*, bool);
template Status GroupingAggregator::AddBatchImpl<true>(
    RowBatch*, TPrefetchMode::type, HashTableCtx*, bool);
