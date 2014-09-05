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
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"

using namespace impala;

Status PartitionedAggregationNode::ProcessBatchNoGrouping(
    RowBatch* batch, HashTableCtx* ht_ctx) {
  for (int i = 0; i < batch->num_rows(); ++i) {
    UpdateTuple(&agg_fn_ctxs_[0], singleton_output_tuple_, batch->GetRow(i));
  }
  return Status::OK;
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::ProcessBatch(RowBatch* batch, HashTableCtx* ht_ctx) {
  DCHECK(!hash_partitions_.empty());

  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    uint32_t hash = 0;
    if (AGGREGATED_ROWS) {
      ht_ctx_->EvalAndHashBuild(row, &hash);
    } else {
      ht_ctx_->EvalAndHashProbe(row, &hash);
    }

    Partition* dst_partition = hash_partitions_[hash >> (32 - NUM_PARTITIONING_BITS)];

    // To process this row, we first see if it can be aggregated or inserted into this
    // partition's hash table. If we need to insert it and that fails, due to OOM, we
    // spill the partition. The partition to spill is not necessarily dst_partition,
    // so we can try again to insert the row.
    if (!dst_partition->is_spilled()) {
      HashTable* ht = dst_partition->hash_tbl.get();
      DCHECK(ht != NULL);
      HashTable::Iterator it = ht->Find(ht_ctx_.get());
      if (!it.AtEnd()) {
        // Row is already in hash table. Do the aggregation and we're done.
        UpdateTuple(&dst_partition->agg_fn_ctxs[0], it.GetTuple(), row, AGGREGATED_ROWS);
        continue;
      }

      // Row was not in hash table, we need to (optionally) construct the intermediate
      // tuple and then insert it into the hash table.
      Tuple* intermediate_tuple = NULL;
      if (AGGREGATED_ROWS) {
        intermediate_tuple = row->GetTuple(0);
        DCHECK(intermediate_tuple != NULL);
        if (ht->Insert(ht_ctx_.get(), intermediate_tuple)) continue;
      } else {
        intermediate_tuple = ConstructIntermediateTuple(
            dst_partition->agg_fn_ctxs, NULL, dst_partition->aggregated_row_stream.get());
        if (intermediate_tuple != NULL && ht->Insert(ht_ctx_.get(), intermediate_tuple)) {
          UpdateTuple(&dst_partition->agg_fn_ctxs[0], intermediate_tuple, row);
          continue;
        }
      }

      // In this case, we either didn't have enough memory to allocate the result
      // tuple or we didn't have enough memory to insert it into the hash table.
      // We need to spill.
      RETURN_IF_ERROR(SpillPartition());
      if (!dst_partition->is_spilled()) {
        // We spilled a different partition, try to insert this tuple.
        if (ht->Insert(ht_ctx_.get(), intermediate_tuple)) continue;
        // TODO: can we get here?
        DCHECK(false) << "We spilled a different partition but still did not "
                      << "have enough memory.";
      }

      // In this case, we already constructed the intermediate tuple in the spill stream,
      // no need to do any more work.
      if (!AGGREGATED_ROWS && intermediate_tuple != NULL) continue;
    }

    // This partition is already spilled, just append the row.
    BufferedTupleStream* dst_stream = AGGREGATED_ROWS ?
        dst_partition->aggregated_row_stream.get() :
        dst_partition->unaggregated_row_stream.get();
    DCHECK(dst_stream != NULL);
    if (dst_stream->AddRow(row)) continue;
    Status status = dst_stream->status();
    DCHECK(!status.ok());
    status.AddErrorMsg("Could not append row even after spilling a partition.");
    return status;
  }

  return Status::OK;
}

Status PartitionedAggregationNode::ProcessBatch_false(
    RowBatch* batch, HashTableCtx* ht_ctx) {
  return ProcessBatch<false>(batch, ht_ctx);
}

Status PartitionedAggregationNode::ProcessBatch_true(
    RowBatch* batch, HashTableCtx* ht_ctx) {
  return ProcessBatch<true>(batch, ht_ctx);
}
