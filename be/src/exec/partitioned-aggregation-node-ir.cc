// Copyright 2012 Cloudera Inc.
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

#include "exec/partitioned-aggregation-node.h"

#include "exec/hash-table.inline.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"

using namespace impala;

Status PartitionedAggregationNode::ProcessBatchNoGrouping(
    RowBatch* batch, HashTableCtx* ht_ctx) {
  for (int i = 0; i < batch->num_rows(); ++i) {
    UpdateTuple(&agg_fn_ctxs_[0], singleton_output_tuple_, batch->GetRow(i));
  }
  return Status::OK();
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::ProcessBatch(RowBatch* batch, HashTableCtx* ht_ctx) {
  DCHECK(!hash_partitions_.empty());

  // Make sure that no resizes will happen when inserting individual rows to the hash
  // table of each partition by pessimistically assuming that all the rows in each batch
  // will end up to the same partition.
  // TODO: Once we have a histogram with the number of rows per partition, we will have
  // accurate resize calls.
  int num_rows = batch->num_rows();
  RETURN_IF_ERROR(CheckAndResizeHashPartitions(num_rows, ht_ctx));

  for (int i = 0; i < num_rows; ++i) {
    RETURN_IF_ERROR(ProcessRow<AGGREGATED_ROWS>(batch->GetRow(i), ht_ctx));
  }

  return Status::OK();
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::ProcessRow(TupleRow* row, HashTableCtx* ht_ctx) {
  uint32_t hash = 0;
  if (AGGREGATED_ROWS) {
    if (!ht_ctx->EvalAndHashBuild(row, &hash)) return Status::OK();
  } else {
    if (!ht_ctx->EvalAndHashProbe(row, &hash)) return Status::OK();
  }

  // To process this row, we first see if it can be aggregated or inserted into this
  // partition's hash table. If we need to insert it and that fails, due to OOM, we
  // spill the partition. The partition to spill is not necessarily dst_partition,
  // so we can try again to insert the row.
  Partition* dst_partition = hash_partitions_[hash >> (32 - NUM_PARTITIONING_BITS)];
  if (dst_partition->is_spilled()) {
    // This partition is already spilled, just append the row.
    return AppendSpilledRow<AGGREGATED_ROWS>(dst_partition, row);
  }

  HashTable* ht = dst_partition->hash_tbl.get();
  DCHECK(ht != NULL);
  DCHECK(dst_partition->aggregated_row_stream->is_pinned());
  bool found;
  // Find the appropriate bucket in the hash table. There will always be a free
  // bucket because we checked the size above.
  HashTable::Iterator it = ht->FindBucket(ht_ctx, hash, &found);
  DCHECK(!it.AtEnd()) << "Hash table had no free buckets";
  if (AGGREGATED_ROWS) {
    // If the row is already an aggregate row, it cannot match anything in the
    // hash table since we process the aggregate rows first. These rows should
    // have been aggregated in the initial pass.
    DCHECK(!found);
  } else if (found) {
    // Row is already in hash table. Do the aggregation and we're done.
    UpdateTuple(&dst_partition->agg_fn_ctxs[0], it.GetTuple(), row);
    return Status::OK();
  }

  // If we are seeing this result row for the first time, we need to construct the
  // result row and initialize it.
  return AddIntermediateTuple<AGGREGATED_ROWS>(dst_partition, ht_ctx, row, hash, it);
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::AddIntermediateTuple(Partition* partition,
    HashTableCtx* ht_ctx, TupleRow* row, uint32_t hash, HashTable::Iterator insert_it) {
  while (true) {
    DCHECK(partition->aggregated_row_stream->is_pinned());
    Tuple* intermediate_tuple = ConstructIntermediateTuple(partition->agg_fn_ctxs,
        NULL, partition->aggregated_row_stream.get(), &process_batch_status_);

    if (LIKELY(intermediate_tuple != NULL)) {
      UpdateTuple(&partition->agg_fn_ctxs[0], intermediate_tuple, row, AGGREGATED_ROWS);
      // After copying and initializing the tuple, insert it into the hash table.
      insert_it.SetTuple(intermediate_tuple, hash);
      return Status::OK();
    } else if (!process_batch_status_.ok()) {
      return process_batch_status_;
    }

    // We did not have enough memory to add intermediate_tuple to the stream.
    RETURN_IF_ERROR(SpillPartition());
    if (partition->is_spilled()) {
      return AppendSpilledRow<AGGREGATED_ROWS>(partition, row);
    }
  }
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::AppendSpilledRow(Partition* partition, TupleRow* row) {
  DCHECK(partition->is_spilled());
  BufferedTupleStream* stream = AGGREGATED_ROWS ?
      partition->aggregated_row_stream.get() :
      partition->unaggregated_row_stream.get();
  return AppendSpilledRow(stream, row);
}

Status PartitionedAggregationNode::ProcessBatch_false(
    RowBatch* batch, HashTableCtx* ht_ctx) {
  return ProcessBatch<false>(batch, ht_ctx);
}

Status PartitionedAggregationNode::ProcessBatch_true(
    RowBatch* batch, HashTableCtx* ht_ctx) {
  return ProcessBatch<true>(batch, ht_ctx);
}
