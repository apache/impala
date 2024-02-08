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
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "exec/hdfs-scan-node-base.h"
#include "exec/scratch-tuple-batch.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

using namespace std;
using namespace strings;

namespace impala {

PROFILE_DEFINE_COUNTER(
    NumColumns, STABLE_LOW, TUnit::UNIT, "Number of columns that need to be read.");
PROFILE_DEFINE_COUNTER(NumScannersWithNoReads, STABLE_LOW, TUnit::UNIT,
    "Number of scanners that end up doing no reads because their splits don't overlap "
    "with the midpoint of any row-group/stripe in the file.");
PROFILE_DEFINE_SUMMARY_STATS_TIMER(FooterProcessingTime, STABLE_LOW,
    "Average and min/max time spent processing the footer by each split.");
PROFILE_DEFINE_SUMMARY_STATS_COUNTER(ColumnarScannerIdealReservation, DEBUG, TUnit::BYTES,
    "Tracks stats about the ideal reservation for a scanning a row group (parquet) or "
    "stripe (orc). The ideal reservation is calculated based on min and max buffer "
    "size.");
PROFILE_DEFINE_SUMMARY_STATS_COUNTER(ColumnarScannerActualReservation, DEBUG,
    TUnit::BYTES,
    "Tracks stats about the actual reservation for a scanning a row group "
    "(parquet) or stripe (orc).");
PROFILE_DEFINE_COUNTER(IoReadSyncRequest, DEBUG, TUnit::UNIT,
    "Number of stream read request done in synchronized manner.");
PROFILE_DEFINE_COUNTER(IoReadAsyncRequest, DEBUG, TUnit::UNIT,
    "Number of stream read request done in asynchronized manner.");
PROFILE_DEFINE_COUNTER(
    IoReadTotalRequest, DEBUG, TUnit::UNIT, "Total number of stream read request.");
PROFILE_DEFINE_COUNTER(IoReadSyncBytes, DEBUG, TUnit::BYTES,
    "The total number of bytes read from streams in synchronized manner.");
PROFILE_DEFINE_COUNTER(IoReadAsyncBytes, DEBUG, TUnit::BYTES,
    "The total number of bytes read from streams in asynchronized manner.");
PROFILE_DEFINE_COUNTER(IoReadTotalBytes, DEBUG, TUnit::BYTES,
    "The total number of bytes read from streams.");
PROFILE_DEFINE_COUNTER(IoReadSkippedBytes, DEBUG, TUnit::BYTES,
    "The total number of bytes skipped from streams.");
PROFILE_DEFINE_COUNTER(NumFileMetadataRead, DEBUG, TUnit::UNIT,
    "The total number of file metadata reads done in place of rows or row groups / "
    "stripe iteration.");

const char* HdfsColumnarScanner::LLVM_CLASS_NAME = "class.impala::HdfsColumnarScanner";

HdfsColumnarScanner::HdfsColumnarScanner(HdfsScanNodeBase* scan_node,
    RuntimeState* state) :
    HdfsScanner(scan_node, state),
    scratch_batch_(new ScratchTupleBatch(
        *scan_node->row_desc(), state_->batch_size(), scan_node->mem_tracker())) {
}

HdfsColumnarScanner::~HdfsColumnarScanner() {}

Status HdfsColumnarScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Open(context));
  // Memorize 'is_footer_scanner_' here since 'stream_' can be released early.
  const io::ScanRange* range = stream_->scan_range();
  is_footer_scanner_ =
      range->offset() + range->bytes_to_read() >= stream_->file_desc()->file_length;

  RuntimeProfile* profile = scan_node_->runtime_profile();
  num_cols_counter_ = PROFILE_NumColumns.Instantiate(profile);
  num_scanners_with_no_reads_counter_ =
      PROFILE_NumScannersWithNoReads.Instantiate(profile);
  process_footer_timer_stats_ = PROFILE_FooterProcessingTime.Instantiate(profile);
  columnar_scanner_ideal_reservation_counter_ =
      PROFILE_ColumnarScannerIdealReservation.Instantiate(profile);
  columnar_scanner_actual_reservation_counter_ =
      PROFILE_ColumnarScannerActualReservation.Instantiate(profile);

  io_sync_request_ = PROFILE_IoReadSyncRequest.Instantiate(profile);
  io_sync_bytes_ = PROFILE_IoReadSyncBytes.Instantiate(profile);
  io_async_request_ = PROFILE_IoReadAsyncRequest.Instantiate(profile);
  io_async_bytes_ = PROFILE_IoReadAsyncBytes.Instantiate(profile);
  io_total_request_ = PROFILE_IoReadTotalRequest.Instantiate(profile);
  io_total_bytes_ = PROFILE_IoReadTotalBytes.Instantiate(profile);
  io_skipped_bytes_ = PROFILE_IoReadSkippedBytes.Instantiate(profile);
  num_file_metadata_read_ = PROFILE_NumFileMetadataRead.Instantiate(profile);
  return Status::OK();
}

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

HdfsColumnarScanner::ColumnReservations
HdfsColumnarScanner::DivideReservationBetweenColumnsHelper(int64_t min_buffer_size,
    int64_t max_buffer_size, const ColumnRangeLengths& col_range_lengths,
    int64_t reservation_to_distribute) {
  // Pair of (column index, reservation allocated).
  ColumnReservations tmp_reservations;
  tmp_reservations.reserve(col_range_lengths.size());
  for (int i = 0; i < col_range_lengths.size(); ++i) tmp_reservations.emplace_back(i, 0);

  // Sort in descending order of length, breaking ties by index so that larger columns
  // get allocated reservation first. It is common to have dramatically different column
  // sizes in a single file because of different value sizes and compressibility. E.g.
  // consider a large STRING "comment" field versus a highly compressible
  // dictionary-encoded column with only a few distinct values. We want to give max-sized
  // buffers to large columns first to maximize the size of I/Os that we do while reading
  // this row group.
  sort(tmp_reservations.begin(), tmp_reservations.end(),
      [&col_range_lengths](
          const pair<int, int64_t>& left, const pair<int, int64_t>& right) {
        int64_t left_len = col_range_lengths[left.first];
        int64_t right_len = col_range_lengths[right.first];
        return (left_len != right_len) ? (left_len > right_len) :
                                         (left.first < right.first);
      });

  // Set aside the minimum reservation per column.
  reservation_to_distribute -= min_buffer_size * col_range_lengths.size();

  // Allocate reservations to columns by repeatedly allocating either a max-sized buffer
  // or a large enough buffer to fit the remaining data for each column. Do this
  // round-robin up to the ideal number of I/O buffers.
  for (int i = 0; i < io::DiskIoMgr::IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE; ++i) {
    for (auto& tmp_reservation : tmp_reservations) {
      // Add back the reservation we set aside above.
      if (i == 0) reservation_to_distribute += min_buffer_size;

      int64_t bytes_left_in_range =
          col_range_lengths[tmp_reservation.first] - tmp_reservation.second;
      int64_t bytes_to_add;
      if (bytes_left_in_range >= max_buffer_size) {
        if (reservation_to_distribute >= max_buffer_size) {
          bytes_to_add = max_buffer_size;
        } else if (i == 0) {
          DCHECK_EQ(0, tmp_reservation.second);
          // Ensure this range gets at least one buffer on the first iteration.
          bytes_to_add = BitUtil::RoundDownToPowerOfTwo(reservation_to_distribute);
        } else {
          DCHECK_GT(tmp_reservation.second, 0);
          // We need to read more than the max buffer size, but can't allocate a
          // max-sized buffer. Stop adding buffers to this column: we prefer to use
          // the existing max-sized buffers without small buffers mixed in so that
          // we will alway do max-sized I/Os, which make efficient use of I/O devices.
          bytes_to_add = 0;
        }
      } else if (bytes_left_in_range > 0
          && reservation_to_distribute >= min_buffer_size) {
        // Choose a buffer size that will fit the rest of the bytes left in the range.
        bytes_to_add =
            max(min_buffer_size, BitUtil::RoundUpToPowerOfTwo(bytes_left_in_range));
        // But don't add more reservation than is available.
        bytes_to_add =
            min(bytes_to_add, BitUtil::RoundDownToPowerOfTwo(reservation_to_distribute));
      } else {
        bytes_to_add = 0;
      }
      DCHECK(bytes_to_add == 0 || bytes_to_add >= min_buffer_size) << bytes_to_add;
      reservation_to_distribute -= bytes_to_add;
      tmp_reservation.second += bytes_to_add;

      DCHECK_GE(reservation_to_distribute, 0);
      DCHECK_GT(tmp_reservation.second, 0);
    }
  }
  return tmp_reservations;
}

int64_t HdfsColumnarScanner::ComputeIdealReservation(
    const ColumnRangeLengths& col_range_lengths) {
  io::DiskIoMgr* io_mgr = ExecEnv::GetInstance()->disk_io_mgr();
  int64_t ideal_reservation = 0;
  for (int64_t len : col_range_lengths) {
    ideal_reservation += io_mgr->ComputeIdealBufferReservation(len);
  }
  return ideal_reservation;
}

Status HdfsColumnarScanner::DivideReservationBetweenColumns(
    const ColumnRangeLengths& col_range_lengths,
    ColumnReservations& reservation_per_column) {
  io::DiskIoMgr* io_mgr = ExecEnv::GetInstance()->disk_io_mgr();
  const int64_t min_buffer_size = io_mgr->min_buffer_size();
  const int64_t max_buffer_size = io_mgr->max_buffer_size();
  // The HdfsScanNode reservation calculation in the planner ensures that we have
  // reservation for at least one buffer per column.
  if (context_->total_reservation() < min_buffer_size * col_range_lengths.size()) {
    return Status(TErrorCode::INTERNAL_ERROR,
        Substitute("Not enough reservation in columnar scanner for file '$0'. "
                   "Need at least $1 bytes per column for $2 columns but had $3 bytes",
            filename(), min_buffer_size, col_range_lengths.size(),
            context_->total_reservation()));
  }

  // The scanner-wide stream was used only to read the file footer.  Each column has added
  // its own stream. We can use the total reservation now that 'stream_''s resources have
  // been released. We may benefit from increasing reservation further, so let's compute
  // the ideal reservation to scan all the columns.
  int64_t ideal_reservation = ComputeIdealReservation(col_range_lengths);
  if (ideal_reservation > context_->total_reservation()) {
    context_->TryIncreaseReservation(ideal_reservation);
  }
  columnar_scanner_actual_reservation_counter_->UpdateCounter(
      context_->total_reservation());
  columnar_scanner_ideal_reservation_counter_->UpdateCounter(ideal_reservation);

  reservation_per_column = DivideReservationBetweenColumnsHelper(
      min_buffer_size, max_buffer_size, col_range_lengths, context_->total_reservation());
  return Status::OK();
}

void HdfsColumnarScanner::AddSyncReadBytesCounter(int64_t total_bytes) {
  io_sync_request_->Add(1);
  io_total_request_->Add(1);
  io_sync_bytes_->Add(total_bytes);
  io_total_bytes_->Add(total_bytes);
}

void HdfsColumnarScanner::AddAsyncReadBytesCounter(int64_t total_bytes) {
  io_async_request_->Add(1);
  io_total_request_->Add(1);
  io_async_bytes_->Add(total_bytes);
  io_total_bytes_->Add(total_bytes);
}

void HdfsColumnarScanner::AddSkippedReadBytesCounter(int64_t total_bytes) {
  io_skipped_bytes_->Add(total_bytes);
}
}
