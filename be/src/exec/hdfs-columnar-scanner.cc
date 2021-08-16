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

#include "codegen/llvm-codegen.h"
#include "exec/hdfs-scan-node-base.h"
#include "exec/scratch-tuple-batch.h"
#include "runtime/fragment-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"

namespace impala {

const char* HdfsColumnarScanner::LLVM_CLASS_NAME = "class.impala::HdfsColumnarScanner";

HdfsColumnarScanner::HdfsColumnarScanner(HdfsScanNodeBase* scan_node,
    RuntimeState* state) :
    HdfsScanner(scan_node, state),
    scratch_batch_(new ScratchTupleBatch(
        *scan_node->row_desc(), state_->batch_size(), scan_node->mem_tracker())) {
}

HdfsColumnarScanner::~HdfsColumnarScanner() {}

int HdfsColumnarScanner::FilterScratchBatch(RowBatch* dst_batch) {
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
  return ProcessScratchBatchCodegenOrInterpret(dst_batch);
}

int HdfsColumnarScanner::TransferScratchTuples(RowBatch* dst_batch) {
  const int num_rows_to_commit = FilterScratchBatch(dst_batch);
  if (scratch_batch_->tuple_byte_size != 0) {
    scratch_batch_->FinalizeTupleTransfer(dst_batch, num_rows_to_commit);
  }
  return num_rows_to_commit;
}

Status HdfsColumnarScanner::Codegen(HdfsScanPlanNode* node, FragmentState* state,
    llvm::Function** process_scratch_batch_fn) {
  DCHECK(state->ShouldCodegen());
  *process_scratch_batch_fn = nullptr;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);

  llvm::Function* fn = codegen->GetFunction(IRFunction::PROCESS_SCRATCH_BATCH, true);
  DCHECK(fn != nullptr);

  llvm::Function* eval_conjuncts_fn;
  const vector<ScalarExpr*>& conjuncts = node->conjuncts_;
  RETURN_IF_ERROR(ExecNode::CodegenEvalConjuncts(codegen, conjuncts, &eval_conjuncts_fn));
  DCHECK(eval_conjuncts_fn != nullptr);

  int replaced = codegen->ReplaceCallSites(fn, eval_conjuncts_fn, "EvalConjuncts");
  DCHECK_REPLACE_COUNT(replaced, 1);

  llvm::Function* eval_runtime_filters_fn;
  RETURN_IF_ERROR(CodegenEvalRuntimeFilters(
      codegen, node->runtime_filter_exprs_, &eval_runtime_filters_fn));
  DCHECK(eval_runtime_filters_fn != nullptr);

  replaced = codegen->ReplaceCallSites(fn, eval_runtime_filters_fn, "EvalRuntimeFilters");
  DCHECK_REPLACE_COUNT(replaced, 1);

  fn->setName("ProcessScratchBatch");
  *process_scratch_batch_fn = codegen->FinalizeFunction(fn);
  if (*process_scratch_batch_fn == nullptr) {
    return Status("Failed to finalize process_scratch_batch_fn.");
  }
  return Status::OK();
}

int HdfsColumnarScanner::ProcessScratchBatchCodegenOrInterpret(RowBatch* dst_batch) {
  return CallCodegendOrInterpreted<ProcessScratchBatchFn>::invoke(this,
      codegend_process_scratch_batch_fn_, &HdfsColumnarScanner::ProcessScratchBatch,
      dst_batch);
}

}
