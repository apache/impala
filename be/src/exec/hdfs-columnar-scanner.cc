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

#include "exec/hdfs-columnar-scanner.h"

#include <algorithm>

namespace impala {

HdfsColumnarScanner::HdfsColumnarScanner(HdfsScanNodeBase* scan_node,
    RuntimeState* state) :
    HdfsScanner(scan_node, state),
    scratch_batch_(new ScratchTupleBatch(
        *scan_node->row_desc(), state_->batch_size(), scan_node->mem_tracker())) {
}

HdfsColumnarScanner::~HdfsColumnarScanner() {}

int HdfsColumnarScanner::TransferScratchTuples(RowBatch* dst_batch) {
  // This function must not be called when the output batch is already full. As long as
  // we always call CommitRows() after TransferScratchTuples(), the output batch can
  // never be empty.
  DCHECK_LT(dst_batch->num_rows(), dst_batch->capacity());
  DCHECK_EQ(dst_batch->row_desc()->tuple_descriptors().size(), 1);
  if (scratch_batch_->tuple_byte_size == 0) {
    Tuple** output_row =
        reinterpret_cast<Tuple**>(dst_batch->GetRow(dst_batch->num_rows()));
    // We are materializing a collection with empty tuples. Add a NULL tuple to the
    // output batch per remaining scratch tuple and return. No need to evaluate
    // filters/conjuncts.
    DCHECK(filter_ctxs_.empty());
    DCHECK(conjunct_evals_->empty());
    int num_tuples = std::min(dst_batch->capacity() - dst_batch->num_rows(),
        scratch_batch_->num_tuples - scratch_batch_->tuple_idx);
    memset(output_row, 0, num_tuples * sizeof(Tuple*));
    scratch_batch_->tuple_idx += num_tuples;
    // No data is required to back the empty tuples, so we should not attach any data to
    // these batches.
    DCHECK_EQ(0, scratch_batch_->total_allocated_bytes());
    return num_tuples;
  }

  int num_rows_to_commit;
  if (codegend_process_scratch_batch_fn_ != nullptr) {
    num_rows_to_commit = codegend_process_scratch_batch_fn_(this, dst_batch);
  } else {
    num_rows_to_commit = ProcessScratchBatch(dst_batch);
  }
  scratch_batch_->FinalizeTupleTransfer(dst_batch, num_rows_to_commit);
  return num_rows_to_commit;
}

}
