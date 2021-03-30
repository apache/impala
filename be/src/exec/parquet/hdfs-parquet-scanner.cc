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

#include "exec/parquet/hdfs-parquet-scanner.h"

#include <algorithm>
#include <queue>
#include <stack>

#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>

#include "codegen/codegen-anyval.h"
#include "exec/exec-node.inline.h"
#include "exec/hdfs-scan-node.h"
#include "exec/parquet/parquet-collection-column-reader.h"
#include "exec/parquet/parquet-column-readers.h"
#include "exec/scanner-context.inline.h"
#include "exec/scratch-tuple-batch.h"
#include "exprs/scalar-expr-evaluator.h"
#include "rpc/thrift-util.h"
#include "runtime/collection-value-builder.h"
#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/request-context.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/scoped-buffer.h"
#include "util/dict-encoding.h"
#include "util/pretty-printer.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

DECLARE_bool(convert_legacy_hive_parquet_utc_timestamps);

using std::move;
using std::sort;
using namespace impala;
using namespace impala::io;

// Max entries in the dictionary before switching to PLAIN encoding. If a dictionary
// has fewer entries, then the entire column is dictionary encoded. This threshold
// is guaranteed to be true for Impala versions 2.9 or below.
// THIS RECORDS INFORMATION ABOUT PAST BEHAVIOR. DO NOT CHANGE THIS CONSTANT.
const int LEGACY_IMPALA_MAX_DICT_ENTRIES = 40000;

static const string PARQUET_MEM_LIMIT_EXCEEDED =
    "HdfsParquetScanner::$0() failed to allocate $1 bytes for $2.";

namespace impala {

static const string IDEAL_RESERVATION_COUNTER_NAME = "ParquetRowGroupIdealReservation";
static const string ACTUAL_RESERVATION_COUNTER_NAME = "ParquetRowGroupActualReservation";

Status HdfsParquetScanner::IssueInitialRanges(HdfsScanNodeBase* scan_node,
    const vector<HdfsFileDesc*>& files) {
  DCHECK(!files.empty());
  // Add Parquet-specific counters.
  ADD_SUMMARY_STATS_COUNTER(
      scan_node->runtime_profile(), IDEAL_RESERVATION_COUNTER_NAME, TUnit::BYTES);
  ADD_SUMMARY_STATS_COUNTER(
      scan_node->runtime_profile(), ACTUAL_RESERVATION_COUNTER_NAME, TUnit::BYTES);

  for (HdfsFileDesc* file : files) {
    // If the file size is less than 12 bytes, it is an invalid Parquet file.
    if (file->file_length < 12) {
      return Status(Substitute("Parquet file $0 has an invalid file length: $1",
          file->filename, file->file_length));
    }
  }
  return IssueFooterRanges(scan_node, THdfsFileFormat::PARQUET, files);
}

HdfsParquetScanner::HdfsParquetScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
  : HdfsColumnarScanner(scan_node, state),
    row_group_idx_(-1),
    row_group_rows_read_(0),
    advance_row_group_(true),
    min_max_tuple_(nullptr),
    row_batches_produced_(0),
    metadata_range_(nullptr),
    dictionary_pool_(new MemPool(scan_node->mem_tracker())),
    assemble_rows_timer_(scan_node_->materialize_tuple_timer()),
    process_footer_timer_stats_(nullptr),
    num_cols_counter_(nullptr),
    num_stats_filtered_row_groups_counter_(nullptr),
    num_row_groups_counter_(nullptr),
    num_scanners_with_no_reads_counter_(nullptr),
    num_dict_filtered_row_groups_counter_(nullptr),
    parquet_compressed_page_size_counter_(nullptr),
    parquet_uncompressed_page_size_counter_(nullptr),
    coll_items_read_counter_(0),
    page_index_(this) {
  assemble_rows_timer_.Stop();
}

Status HdfsParquetScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Open(context));
  metadata_range_ = stream_->scan_range();
  num_cols_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumColumns", TUnit::UNIT);
  num_stats_filtered_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumStatsFilteredRowGroups",
          TUnit::UNIT);
  num_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumRowGroups", TUnit::UNIT);
  num_row_groups_with_page_index_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumRowGroupsWithPageIndex",
          TUnit::UNIT);
  num_stats_filtered_pages_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumStatsFilteredPages", TUnit::UNIT);
  num_pages_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumPages", TUnit::UNIT);
  num_scanners_with_no_reads_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumScannersWithNoReads", TUnit::UNIT);
  num_dict_filtered_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumDictFilteredRowGroups", TUnit::UNIT);
  process_footer_timer_stats_ =
      ADD_SUMMARY_STATS_TIMER(scan_node_->runtime_profile(), "FooterProcessingTime");
  parquet_compressed_page_size_counter_ = ADD_SUMMARY_STATS_COUNTER(
      scan_node_->runtime_profile(), "ParquetCompressedPageSize", TUnit::BYTES);
  parquet_uncompressed_page_size_counter_ = ADD_SUMMARY_STATS_COUNTER(
      scan_node_->runtime_profile(), "ParquetUncompressedPageSize", TUnit::BYTES);
  process_page_index_stats_ =
      ADD_SUMMARY_STATS_TIMER(scan_node_->runtime_profile(), "PageIndexProcessingTime");

  codegend_process_scratch_batch_fn_ = reinterpret_cast<ProcessScratchBatchFn>(
      scan_node_->GetCodegenFn(THdfsFileFormat::PARQUET));
  if (codegend_process_scratch_batch_fn_ == nullptr) {
    scan_node_->IncNumScannersCodegenDisabled();
  } else {
    scan_node_->IncNumScannersCodegenEnabled();
  }

  perm_pool_.reset(new MemPool(scan_node_->mem_tracker()));

  // Allocate tuple buffer to evaluate conjuncts on parquet::Statistics.
  const TupleDescriptor* min_max_tuple_desc = scan_node_->min_max_tuple_desc();
  if (min_max_tuple_desc != nullptr) {
    int64_t tuple_size = min_max_tuple_desc->byte_size();
    uint8_t* buffer = perm_pool_->TryAllocate(tuple_size);
    if (buffer == nullptr) {
      string details = Substitute("Could not allocate buffer of $0 bytes for Parquet "
          "statistics tuple for file '$1'.", tuple_size, filename());
      return scan_node_->mem_tracker()->MemLimitExceeded(state_, details, tuple_size);
    }
    min_max_tuple_ = reinterpret_cast<Tuple*>(buffer);
  }

  // Clone the min/max statistics conjuncts.
  RETURN_IF_ERROR(ScalarExprEvaluator::Clone(&obj_pool_, state_,
      expr_perm_pool_.get(), context_->expr_results_pool(),
      scan_node_->min_max_conjunct_evals(), &min_max_conjunct_evals_));

  for (int i = 0; i < context->filter_ctxs().size(); ++i) {
    const FilterContext* ctx = &context->filter_ctxs()[i];
    DCHECK(ctx->filter != nullptr);
    filter_ctxs_.push_back(ctx);
  }
  filter_stats_.resize(filter_ctxs_.size());

  DCHECK(parse_status_.ok()) << "Invalid parse_status_" << parse_status_.GetDetail();

  // Each scan node can process multiple splits. Each split processes the footer once.
  // We use a timer to measure the time taken to ProcessFooter() per split and add
  // this time to the averaged timer.
  MonotonicStopWatch single_footer_process_timer;
  single_footer_process_timer.Start();
  // First process the file metadata in the footer.
  Status footer_status = ProcessFooter();
  single_footer_process_timer.Stop();

  process_footer_timer_stats_->UpdateCounter(single_footer_process_timer.ElapsedTime());

  // Release I/O buffers immediately to make sure they are cleaned up
  // in case we return a non-OK status anywhere below.
  stream_ = nullptr;
  context_->ReleaseCompletedResources(true);
  context_->ClearStreams();
  RETURN_IF_ERROR(footer_status);

  // Parse the file schema into an internal representation for schema resolution.
  schema_resolver_.reset(new ParquetSchemaResolver(*scan_node_->hdfs_table(),
      state_->query_options().parquet_fallback_schema_resolution,
      state_->query_options().parquet_array_resolution));
  RETURN_IF_ERROR(schema_resolver_->Init(&file_metadata_, filename()));

  // We've processed the metadata and there are columns that need to be materialized.
  RETURN_IF_ERROR(CreateColumnReaders(
      *scan_node_->tuple_desc(), *schema_resolver_, &column_readers_));
  COUNTER_SET(num_cols_counter_,
      static_cast<int64_t>(CountScalarColumns(column_readers_)));
  // Set top-level template tuple.
  template_tuple_ = template_tuple_map_[scan_node_->tuple_desc()];

  RETURN_IF_ERROR(InitDictFilterStructures());
  return Status::OK();
}

void HdfsParquetScanner::Close(RowBatch* row_batch) {
  DCHECK(!is_closed_);
  if (row_batch != nullptr) {
    FlushRowGroupResources(row_batch);
    row_batch->tuple_data_pool()->AcquireData(template_tuple_pool_.get(), false);
    if (scan_node_->HasRowBatchQueue()) {
      static_cast<HdfsScanNode*>(scan_node_)->AddMaterializedRowBatch(
          unique_ptr<RowBatch>(row_batch));
    }
  } else {
    template_tuple_pool_->FreeAll();
    dictionary_pool_->FreeAll();
    context_->ReleaseCompletedResources(true);
    for (ParquetColumnReader* col_reader : column_readers_) col_reader->Close(nullptr);
    // The scratch batch may still contain tuple data. We can get into this case if
    // Open() fails or if the query is cancelled.
    scratch_batch_->ReleaseResources(nullptr);
  }
  if (perm_pool_ != nullptr) {
    perm_pool_->FreeAll();
    perm_pool_.reset();
  }

  // Verify all resources (if any) have been transferred.
  DCHECK_EQ(template_tuple_pool_->total_allocated_bytes(), 0);
  DCHECK_EQ(dictionary_pool_->total_allocated_bytes(), 0);
  DCHECK_EQ(scratch_batch_->total_allocated_bytes(), 0);

  // Collect compression types for reporting completed ranges.
  vector<THdfsCompression::type> compression_types;
  stack<ParquetColumnReader*> readers;
  for (ParquetColumnReader* r: column_readers_) readers.push(r);
  while (!readers.empty()) {
    ParquetColumnReader* reader = readers.top();
    readers.pop();
    if (reader->IsCollectionReader()) {
      CollectionColumnReader* coll_reader = static_cast<CollectionColumnReader*>(reader);
      for (ParquetColumnReader* r: *coll_reader->children()) readers.push(r);
      continue;
    }
    BaseScalarColumnReader* scalar_reader = static_cast<BaseScalarColumnReader*>(reader);
    compression_types.push_back(scalar_reader->codec());
  }
  assemble_rows_timer_.Stop();
  assemble_rows_timer_.ReleaseCounter();

  // If this was a metadata only read (i.e. count(*)), there are no columns.
  if (compression_types.empty()) {
    compression_types.push_back(THdfsCompression::NONE);
    scan_node_->RangeComplete(THdfsFileFormat::PARQUET, compression_types, true);
  } else {
    scan_node_->RangeComplete(THdfsFileFormat::PARQUET, compression_types);
  }
  if (schema_resolver_.get() != nullptr) schema_resolver_.reset();

  ScalarExprEvaluator::Close(min_max_conjunct_evals_, state_);

  for (int i = 0; i < filter_ctxs_.size(); ++i) {
    const FilterStats* stats = filter_ctxs_[i]->stats;
    const LocalFilterStats& local = filter_stats_[i];
    stats->IncrCounters(FilterStats::ROWS_KEY, local.total_possible,
        local.considered, local.rejected);
  }

  CloseInternal();
}

// Get the start of the column.
static int64_t GetColumnStartOffset(const parquet::ColumnMetaData& column) {
  if (column.__isset.dictionary_page_offset) {
    DCHECK_LT(column.dictionary_page_offset, column.data_page_offset);
    return column.dictionary_page_offset;
  }
  return column.data_page_offset;
}

// Get the file offset of the middle of the row group.
static int64_t GetRowGroupMidOffset(const parquet::RowGroup& row_group) {
  int64_t start_offset = GetColumnStartOffset(row_group.columns[0].meta_data);

  const parquet::ColumnMetaData& last_column =
      row_group.columns[row_group.columns.size() - 1].meta_data;
  int64_t end_offset =
      GetColumnStartOffset(last_column) + last_column.total_compressed_size;

  return start_offset + (end_offset - start_offset) / 2;
}

// Returns true if 'row_group' overlaps with 'split_range'.
static bool CheckRowGroupOverlapsSplit(const parquet::RowGroup& row_group,
    const ScanRange* split_range) {
  int64_t row_group_start = GetColumnStartOffset(row_group.columns[0].meta_data);

  const parquet::ColumnMetaData& last_column =
      row_group.columns[row_group.columns.size() - 1].meta_data;
  int64_t row_group_end =
      GetColumnStartOffset(last_column) + last_column.total_compressed_size;

  int64_t split_start = split_range->offset();
  int64_t split_end = split_start + split_range->len();

  return (split_start >= row_group_start && split_start < row_group_end) ||
      (split_end > row_group_start && split_end <= row_group_end) ||
      (split_start <= row_group_start && split_end >= row_group_end);
}

int HdfsParquetScanner::CountScalarColumns(
    const vector<ParquetColumnReader*>& column_readers) {
  DCHECK(!column_readers.empty() || scan_node_->optimize_parquet_count_star());
  int num_columns = 0;
  stack<ParquetColumnReader*> readers;
  for (ParquetColumnReader* r: column_readers_) readers.push(r);
  while (!readers.empty()) {
    ParquetColumnReader* col_reader = readers.top();
    readers.pop();
    if (col_reader->IsCollectionReader()) {
      CollectionColumnReader* collection_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      for (ParquetColumnReader* r: *collection_reader->children()) readers.push(r);
      continue;
    }
    ++num_columns;
  }
  return num_columns;
}

Status HdfsParquetScanner::ProcessSplit() {
  DCHECK(scan_node_->HasRowBatchQueue());
  HdfsScanNode* scan_node = static_cast<HdfsScanNode*>(scan_node_);
  do {
    if (FilterContext::CheckForAlwaysFalse(FilterStats::SPLITS_KEY,
        context_->filter_ctxs())) {
      eos_ = true;
      break;
    }
    unique_ptr<RowBatch> batch = make_unique<RowBatch>(scan_node_->row_desc(),
        state_->batch_size(), scan_node_->mem_tracker());
    if (scan_node_->is_partition_key_scan()) batch->limit_capacity(1);
    Status status = GetNextInternal(batch.get());

    // If we are doing a partition key scan, we are done scanning the file after
    // returning at least one row.
    if (scan_node_->is_partition_key_scan() && batch->num_rows() > 0) eos_ = true;

    // Always add batch to the queue because it may contain data referenced by previously
    // appended batches.
    scan_node->AddMaterializedRowBatch(move(batch));
    RETURN_IF_ERROR(status);
    ++row_batches_produced_;
    if ((row_batches_produced_ & (BATCHES_PER_FILTER_SELECTIVITY_CHECK - 1)) == 0) {
      CheckFiltersEffectiveness();
    }
  } while (!eos_ && !scan_node_->ReachedLimitShared());
  return Status::OK();
}

Status HdfsParquetScanner::GetNextInternal(RowBatch* row_batch) {
  DCHECK(parse_status_.ok()) << parse_status_.GetDetail();
  if (scan_node_->optimize_parquet_count_star()) {
    // Populate the single slot with the Parquet num rows statistic.
    int64_t tuple_buf_size;
    uint8_t* tuple_buf;
    // We try to allocate a smaller row batch here because in most cases the number row
    // groups in a file is much lower than the default row batch capacity.
    int capacity = min(
        static_cast<int>(file_metadata_.row_groups.size()), row_batch->capacity());
    RETURN_IF_ERROR(RowBatch::ResizeAndAllocateTupleBuffer(state_,
        row_batch->tuple_data_pool(), row_batch->row_desc()->GetRowSize(),
        &capacity, &tuple_buf_size, &tuple_buf));
    while (!row_batch->AtCapacity()) {
      RETURN_IF_ERROR(NextRowGroup());
      DCHECK_LE(row_group_idx_, file_metadata_.row_groups.size());
      DCHECK_LE(row_group_rows_read_, file_metadata_.num_rows);
      if (row_group_idx_ == file_metadata_.row_groups.size()) break;
      Tuple* dst_tuple = reinterpret_cast<Tuple*>(tuple_buf);
      TupleRow* dst_row = row_batch->GetRow(row_batch->AddRow());
      InitTuple(template_tuple_, dst_tuple);
      int64_t* dst_slot =
          dst_tuple->GetBigIntSlot(scan_node_->parquet_count_star_slot_offset());
      *dst_slot = file_metadata_.row_groups[row_group_idx_].num_rows;
      row_group_rows_read_ += *dst_slot;
      dst_row->SetTuple(0, dst_tuple);
      row_batch->CommitLastRow();
      tuple_buf += scan_node_->tuple_desc()->byte_size();
    }
    eos_ = row_group_idx_ == file_metadata_.row_groups.size();
    return Status::OK();
  } else if (scan_node_->IsZeroSlotTableScan()) {
    // There are no materialized slots and we are not optimizing count(*), e.g.
    // "select 1 from alltypes". We can serve this query from just the file metadata.
    // We don't need to read the column data.
    if (row_group_rows_read_ == file_metadata_.num_rows) {
      eos_ = true;
      return Status::OK();
    }
    assemble_rows_timer_.Start();
    DCHECK_LE(row_group_rows_read_, file_metadata_.num_rows);
    int64_t rows_remaining = file_metadata_.num_rows - row_group_rows_read_;
    int max_tuples = min<int64_t>(row_batch->capacity(), rows_remaining);
    TupleRow* current_row = row_batch->GetRow(row_batch->AddRow());
    int num_to_commit = WriteTemplateTuples(current_row, max_tuples);
    Status status = CommitRows(row_batch, num_to_commit);
    assemble_rows_timer_.Stop();
    RETURN_IF_ERROR(status);
    row_group_rows_read_ += max_tuples;
    COUNTER_ADD(scan_node_->rows_read_counter(), max_tuples);
    return Status::OK();
  }

  // Transfer remaining tuples from the scratch batch.
  if (!scratch_batch_->AtEnd()) {
    assemble_rows_timer_.Start();
    int num_row_to_commit = TransferScratchTuples(row_batch);
    assemble_rows_timer_.Stop();
    RETURN_IF_ERROR(CommitRows(row_batch, num_row_to_commit));
    if (row_batch->AtCapacity()) return Status::OK();
  }

  while (advance_row_group_ || column_readers_[0]->RowGroupAtEnd()) {
    // Transfer resources and clear streams if there is any leftover from the previous
    // row group. We will create new streams for the next row group.
    FlushRowGroupResources(row_batch);
    if (!advance_row_group_) {
      Status status =
          ValidateEndOfRowGroup(column_readers_, row_group_idx_, row_group_rows_read_);
      if (!status.ok()) RETURN_IF_ERROR(state_->LogOrReturnError(status.msg()));
    }
    RETURN_IF_ERROR(NextRowGroup());
    DCHECK_LE(row_group_idx_, file_metadata_.row_groups.size());
    if (row_group_idx_ == file_metadata_.row_groups.size()) {
      eos_ = true;
      DCHECK(parse_status_.ok());
      return Status::OK();
    }
  }

  // Apply any runtime filters to static tuples containing the partition keys for this
  // partition. If any filter fails, we return immediately and stop processing this
  // scan range.
  if (!scan_node_->PartitionPassesFilters(context_->partition_descriptor()->id(),
      FilterStats::ROW_GROUPS_KEY, context_->filter_ctxs())) {
    eos_ = true;
    DCHECK(parse_status_.ok());
    return Status::OK();
  }
  assemble_rows_timer_.Start();
  Status status = AssembleRows(column_readers_, row_batch, &advance_row_group_);
  assemble_rows_timer_.Stop();
  RETURN_IF_ERROR(status);
  if (!parse_status_.ok()) {
    RETURN_IF_ERROR(state_->LogOrReturnError(parse_status_.msg()));
    parse_status_ = Status::OK();
  }

  return Status::OK();
}

Status HdfsParquetScanner::EvaluateStatsConjuncts(
    const parquet::FileMetaData& file_metadata, const parquet::RowGroup& row_group,
    bool* skip_row_group) {
  *skip_row_group = false;

  if (!state_->query_options().parquet_read_statistics) return Status::OK();

  const TupleDescriptor* min_max_tuple_desc = scan_node_->min_max_tuple_desc();
  if (!min_max_tuple_desc) return Status::OK();

  int64_t tuple_size = min_max_tuple_desc->byte_size();

  DCHECK(min_max_tuple_ != nullptr);
  min_max_tuple_->Init(tuple_size);

  DCHECK_EQ(min_max_tuple_desc->slots().size(), min_max_conjunct_evals_.size());
  for (int i = 0; i < min_max_conjunct_evals_.size(); ++i) {
    SlotDescriptor* slot_desc = min_max_tuple_desc->slots()[i];
    ScalarExprEvaluator* eval = min_max_conjunct_evals_[i];

    // Resolve column path to determine col idx.
    SchemaNode* node = nullptr;
    bool pos_field;
    bool missing_field;
    RETURN_IF_ERROR(schema_resolver_->ResolvePath(slot_desc->col_path(),
        &node, &pos_field, &missing_field));

    if (missing_field) {
      // We are selecting a column that is not in the file. We would set its slot to NULL
      // during the scan, so any predicate would evaluate to false. Return early. NULL
      // comparisons cannot happen here, since predicates with NULL literals are filtered
      // in the frontend.
      *skip_row_group = true;
      break;
    }

    if (pos_field) {
      // The planner should not send predicates with 'pos' for stats filtering to the BE.
      // In case there is a bug, we return an error, which will abort the query.
      stringstream err;
      err << "Statistics not supported for pos fields: " << slot_desc->DebugString();
      DCHECK(false) << err.str();
      return Status(err.str());
    }

    int col_idx = node->col_idx;
    DCHECK_LT(col_idx, row_group.columns.size());

    const vector<parquet::ColumnOrder>& col_orders = file_metadata.column_orders;
    const parquet::ColumnOrder* col_order = col_idx < col_orders.size() ?
        &col_orders[col_idx] : nullptr;

    const parquet::ColumnChunk& col_chunk = row_group.columns[col_idx];
    const ColumnType& col_type = slot_desc->type();

    DCHECK(node->element != nullptr);

    ColumnStatsReader stat_reader = CreateColumnStatsReader(col_chunk, col_type,
        col_order, *node->element);

    int64_t null_count = 0;
    bool null_count_result = stat_reader.ReadNullCountStat(&null_count);
    if (null_count_result && null_count == col_chunk.meta_data.num_values) {
      *skip_row_group = true;
      break;
    }

    const string& fn_name = eval->root().function_name();
    ColumnStatsReader::StatsField stats_field;
    if (!ColumnStatsReader::GetRequiredStatsField(fn_name, &stats_field)) continue;

    void* slot = min_max_tuple_->GetSlot(slot_desc->tuple_offset());
    bool stats_read = stat_reader.ReadFromThrift(stats_field, slot);

    if (stats_read) {
      TupleRow row;
      row.SetTuple(0, min_max_tuple_);
      // Accept NULL as the predicate can contain a CAST which may fail.
      if (!eval->EvalPredicateAcceptNull(&row)) {
        *skip_row_group = true;
        break;
      }
    }
  }

  // Free any expr result allocations accumulated during conjunct evaluation.
  context_->expr_results_pool()->Clear();
  return Status::OK();
}

Status HdfsParquetScanner::NextRowGroup() {
  const ScanRange* split_range = static_cast<ScanRangeMetadata*>(
      metadata_range_->meta_data())->original_split;
  int64_t split_offset = split_range->offset();
  int64_t split_length = split_range->len();

  HdfsFileDesc* file_desc = scan_node_->GetFileDesc(
      context_->partition_descriptor()->id(), filename());

  bool start_with_first_row_group = row_group_idx_ == -1;
  bool misaligned_row_group_skipped = false;

  advance_row_group_ = false;
  row_group_rows_read_ = 0;

  // Loop until we have found a non-empty row group, and successfully initialized and
  // seeded the column readers. Return a non-OK status from within loop only if the error
  // is non-recoverable, otherwise log the error and continue with the next row group.
  while (true) {
    // Reset the parse status for the next row group.
    parse_status_ = Status::OK();
    // Make sure that we don't have leftover resources from the file metadata scan range
    // or previous row groups.
    DCHECK_EQ(0, context_->NumStreams());

    ++row_group_idx_;
    if (row_group_idx_ >= file_metadata_.row_groups.size()) {
      if (start_with_first_row_group && misaligned_row_group_skipped) {
        // We started with the first row group and skipped all the row groups because
        // they were misaligned. The execution flow won't reach this point if there is at
        // least one non-empty row group which this scanner can process.
        COUNTER_ADD(num_scanners_with_no_reads_counter_, 1);
      }
      break;
    }
    const parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx_];
    // Also check 'file_metadata_.num_rows' to make sure 'select count(*)' and 'select *'
    // behave consistently for corrupt files that have 'file_metadata_.num_rows == 0'
    // but some data in row groups.
    if (row_group.num_rows == 0 || file_metadata_.num_rows == 0) continue;

    RETURN_IF_ERROR(ParquetMetadataUtils::ValidateColumnOffsets(
        file_desc->filename, file_desc->file_length, row_group));

    // A row group is processed by the scanner whose split overlaps with the row
    // group's mid point.
    int64_t row_group_mid_pos = GetRowGroupMidOffset(row_group);
    if (!(row_group_mid_pos >= split_offset &&
        row_group_mid_pos < split_offset + split_length)) {
      // The mid-point does not fall within the split, this row group will be handled by a
      // different scanner.
      // If the row group overlaps with the split, we found a misaligned row group.
      misaligned_row_group_skipped |= CheckRowGroupOverlapsSplit(row_group, split_range);
      continue;
    }

    COUNTER_ADD(num_row_groups_counter_, 1);
    if (!row_group.columns.empty() &&
        row_group.columns.front().__isset.offset_index_offset) {
      COUNTER_ADD(num_row_groups_with_page_index_counter_, 1);
    }

    // Evaluate row group statistics.
    bool skip_row_group_on_stats;
    RETURN_IF_ERROR(
        EvaluateStatsConjuncts(file_metadata_, row_group, &skip_row_group_on_stats));
    if (skip_row_group_on_stats) {
      COUNTER_ADD(num_stats_filtered_row_groups_counter_, 1);
      continue;
    }

    // Evaluate page index.
    if (!min_max_conjunct_evals_.empty() &&
        state_->query_options().parquet_read_page_index) {
      bool filter_pages;
      Status page_index_status = ProcessPageIndex(&filter_pages);
      if (!page_index_status.ok()) {
        RETURN_IF_ERROR(state_->LogOrReturnError(page_index_status.msg()));
      }
      if (filter_pages && candidate_ranges_.empty()) {
        // Page level statistics filtered the whole row group. It can happen when there
        // is a gap in the data between the pages and the user's predicate hit that gap.
        // E.g. column chunk 'A' has two pages with statistics {min: 0, max: 5},
        // {min: 10, max: 20}, and query is 'select * from T where A = 8'.
        // It can also happen when there are predicates against different columns, and
        // the passing row ranges of the predicates don't have a common subset.
        COUNTER_ADD(num_stats_filtered_row_groups_counter_, 1);
        continue;
      }
    }

    InitCollectionColumns();
    RETURN_IF_ERROR(InitScalarColumns());

    // Start scanning dictionary filtering column readers, so we can read the dictionary
    // pages in EvalDictionaryFilters().
    RETURN_IF_ERROR(BaseScalarColumnReader::StartScans(dict_filterable_readers_));

    // StartScans() may have allocated resources to scan columns. If we skip this row
    // group below, we must call ReleaseSkippedRowGroupResources() before continuing.

    // If there is a dictionary-encoded column where every value is eliminated
    // by a conjunct, the row group can be eliminated. This initializes dictionaries
    // for all columns visited.
    bool skip_row_group_on_dict_filters;
    Status status = EvalDictionaryFilters(row_group, &skip_row_group_on_dict_filters);
    if (!status.ok()) {
      // Either return an error or skip this row group if it is ok to ignore errors
      RETURN_IF_ERROR(state_->LogOrReturnError(status.msg()));
      ReleaseSkippedRowGroupResources();
      continue;
    }
    if (skip_row_group_on_dict_filters) {
      COUNTER_ADD(num_dict_filtered_row_groups_counter_, 1);
      ReleaseSkippedRowGroupResources();
      continue;
    }

    // At this point, the row group has passed any filtering criteria
    // Start scanning non-dictionary filtering column readers and initialize their
    // dictionaries.
    RETURN_IF_ERROR(BaseScalarColumnReader::StartScans(non_dict_filterable_readers_));
    status = BaseScalarColumnReader::InitDictionaries(non_dict_filterable_readers_);
    if (!status.ok()) {
      // Either return an error or skip this row group if it is ok to ignore errors
      RETURN_IF_ERROR(state_->LogOrReturnError(status.msg()));
      ReleaseSkippedRowGroupResources();
      continue;
    }
    DCHECK(parse_status_.ok()) << "Invalid parse_status_" << parse_status_.GetDetail();
    break;
  }
  DCHECK(parse_status_.ok());
  return Status::OK();
}

bool HdfsParquetScanner::ReadStatFromIndex(const ColumnStatsReader& stats_reader,
    const parquet::ColumnIndex& column_index, int page_idx,
    ColumnStatsReader::StatsField stats_field, bool* is_null_page, void* slot) {
  *is_null_page = column_index.null_pages[page_idx];
  if (*is_null_page) return false;
  switch (stats_field) {
    case ColumnStatsReader::StatsField::MIN:
      return stats_reader.ReadFromString(
          stats_field, column_index.min_values[page_idx],
          &column_index.max_values[page_idx], slot);
    case ColumnStatsReader::StatsField::MAX:
      return stats_reader.ReadFromString(
          stats_field, column_index.max_values[page_idx],
          &column_index.min_values[page_idx], slot);
    default:
      DCHECK(false);
  }
  return false;
}

Status HdfsParquetScanner::ProcessPageIndex(bool* filter_pages) {
  MonotonicStopWatch single_process_page_index_timer;
  single_process_page_index_timer.Start();
  candidate_ranges_.clear();
  *filter_pages = false;
  for (auto& scalar_reader : scalar_readers_) scalar_reader->ResetPageFiltering();
  RETURN_IF_ERROR(page_index_.ReadAll(row_group_idx_));
  if (page_index_.IsEmpty()) return Status::OK();
  // We can release the raw page index buffer when we exit this function.
  const auto scope_exit = MakeScopeExitTrigger([this](){page_index_.Release();});
  RETURN_IF_ERROR(EvaluatePageIndex(filter_pages));
  RETURN_IF_ERROR(ComputeCandidatePagesForColumns(filter_pages));
  single_process_page_index_timer.Stop();
  process_page_index_stats_->UpdateCounter(single_process_page_index_timer.ElapsedTime());
  return Status::OK();
}

Status HdfsParquetScanner::EvaluatePageIndex(bool* filter_pages) {
  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx_];
  vector<RowRange> skip_ranges;

  for (int i = 0; i < min_max_conjunct_evals_.size(); ++i) {
    ScalarExprEvaluator* eval = min_max_conjunct_evals_[i];
    SlotDescriptor* slot_desc = scan_node_->min_max_tuple_desc()->slots()[i];

    // Resolve column path to determine col idx.
    SchemaNode* node = nullptr;
    bool pos_field, missing_field;
    RETURN_IF_ERROR(schema_resolver_->ResolvePath(slot_desc->col_path(), &node,
        &pos_field, &missing_field));
    if (pos_field || missing_field) continue;

    int col_idx = node->col_idx;
    DCHECK_LT(col_idx, row_group.columns.size());
    const parquet::ColumnChunk& col_chunk = row_group.columns[col_idx];
    if (col_chunk.column_index_length == 0) continue;

    parquet::ColumnIndex column_index;
    RETURN_IF_ERROR(page_index_.DeserializeColumnIndex(col_chunk, &column_index));

    const ColumnType& col_type = slot_desc->type();
    const vector<parquet::ColumnOrder>& col_orders = file_metadata_.column_orders;
    const parquet::ColumnOrder* col_order = col_idx < col_orders.size() ?
        &col_orders[col_idx] : nullptr;
    ColumnStatsReader stats_reader = CreateColumnStatsReader(col_chunk, col_type,
        col_order, *node->element);

    min_max_tuple_->Init(scan_node_->min_max_tuple_desc()->byte_size());
    void* slot = min_max_tuple_->GetSlot(slot_desc->tuple_offset());

    const int num_of_pages = column_index.null_pages.size();
    const string& fn_name = eval->root().function_name();
    ColumnStatsReader::StatsField stats_field;
    if (!ColumnStatsReader::GetRequiredStatsField(fn_name, &stats_field)) continue;

    for (int page_idx = 0; page_idx < num_of_pages; ++page_idx) {
      bool value_read, is_null_page;
      value_read = ReadStatFromIndex(stats_reader, column_index, page_idx, stats_field,
          &is_null_page, slot);
      if (!is_null_page && !value_read) continue;
      TupleRow row;
      row.SetTuple(0, min_max_tuple_);
      // Accept NULL as the predicate can contain a CAST which may fail.
      if (is_null_page || !eval->EvalPredicateAcceptNull(&row)) {
        BaseScalarColumnReader* scalar_reader = scalar_reader_map_[col_idx];
        RETURN_IF_ERROR(page_index_.DeserializeOffsetIndex(col_chunk,
            &scalar_reader->offset_index_));
        RowRange row_range;
        GetRowRangeForPage(row_group, scalar_reader->offset_index_, page_idx, &row_range);
        skip_ranges.push_back(row_range);
      }
    }
  }
  if (skip_ranges.empty()) return Status::OK();

  for (BaseScalarColumnReader* scalar_reader : scalar_readers_) {
    const parquet::ColumnChunk& col_chunk = row_group.columns[scalar_reader->col_idx()];
    if (col_chunk.offset_index_length > 0) {
      parquet::OffsetIndex& offset_index = scalar_reader->offset_index_;
      if (!offset_index.page_locations.empty()) continue;
      RETURN_IF_ERROR(page_index_.DeserializeOffsetIndex(col_chunk, &offset_index));
    } else {
      // We can only filter pages based on the page index if we have the offset index
      // for all columns.
      return Status(Substitute("Found column index, but no offset index for '$0' in "
          "file '$1'", scalar_reader->schema_element().name, filename()));
    }
  }
  if (!ComputeCandidateRanges(row_group.num_rows, &skip_ranges, &candidate_ranges_)) {
    return Status(Substitute("Invalid offset index in Parquet file $0.", filename()));
  }
  *filter_pages = true;
  return Status::OK();
}

Status HdfsParquetScanner::ComputeCandidatePagesForColumns(bool* filter_pages) {
  if (candidate_ranges_.empty()) return Status::OK();

  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx_];
  for (BaseScalarColumnReader* scalar_reader : scalar_readers_) {
    const auto& page_locations = scalar_reader->offset_index_.page_locations;
    if (!ComputeCandidatePages(page_locations, candidate_ranges_, row_group.num_rows,
        &scalar_reader->candidate_data_pages_)) {
      *filter_pages = false;
      return Status(Substitute("Invalid offset index in Parquet file $0.", filename()));
    }
  }
  for (BaseScalarColumnReader* scalar_reader : scalar_readers_) {
    const auto& page_locations = scalar_reader->offset_index_.page_locations;
    int total_page_count = page_locations.size();
    int candidate_pages_count = scalar_reader->candidate_data_pages_.size();
    COUNTER_ADD(num_stats_filtered_pages_counter_,
        total_page_count - candidate_pages_count);
    COUNTER_ADD(num_pages_counter_, total_page_count);
  }
  return Status::OK();
}

void HdfsParquetScanner::FlushRowGroupResources(RowBatch* row_batch) {
  DCHECK(row_batch != nullptr);
  row_batch->tuple_data_pool()->AcquireData(dictionary_pool_.get(), false);
  scratch_batch_->ReleaseResources(row_batch->tuple_data_pool());
  context_->ReleaseCompletedResources(true);
  for (ParquetColumnReader* col_reader : column_readers_) col_reader->Close(row_batch);
  context_->ClearStreams();
}

void HdfsParquetScanner::ReleaseSkippedRowGroupResources() {
  dictionary_pool_->FreeAll();
  scratch_batch_->ReleaseResources(nullptr);
  context_->ReleaseCompletedResources(true);
  for (ParquetColumnReader* col_reader : column_readers_) col_reader->Close(nullptr);
  context_->ClearStreams();
}

bool HdfsParquetScanner::IsDictFilterable(BaseScalarColumnReader* col_reader) {
  const SlotDescriptor* slot_desc = col_reader->slot_desc();
  // Some queries do not need the column to be materialized, so slot_desc is NULL.
  // For example, a count(*) with no predicates only needs to count records
  // rather than materializing the values.
  if (!slot_desc) return false;
  // Does this column reader have any dictionary filter conjuncts?
  auto dict_filter_it = dict_filter_map_.find(slot_desc->id());
  if (dict_filter_it == dict_filter_map_.end()) return false;

  // Certain datatypes (chars, timestamps) do not have the appropriate value in the
  // file format and must be converted before return. This is true for the
  // dictionary values, so skip these datatypes for now.
  // TODO: The values should be converted during dictionary construction and stored
  // in converted form in the dictionary.
  if (col_reader->NeedsConversion()) return false;

  // Certain datatypes (timestamps, date) need to validate the value, as certain bit
  // combinations are not valid. The dictionary values are not validated, so skip
  // these datatypes for now.
  // TODO: This should be pushed into dictionary construction.
  if (col_reader->NeedsValidation()) return false;

  return true;
}

void HdfsParquetScanner::PartitionReaders(
    const vector<ParquetColumnReader*>& readers, bool can_eval_dict_filters) {
  for (auto* reader : readers) {
    if (reader->IsCollectionReader()) {
      CollectionColumnReader* col_reader = static_cast<CollectionColumnReader*>(reader);
      collection_readers_.push_back(col_reader);
      PartitionReaders(*col_reader->children(), can_eval_dict_filters);
    } else {
      BaseScalarColumnReader* scalar_reader =
          static_cast<BaseScalarColumnReader*>(reader);
      scalar_readers_.push_back(scalar_reader);
      if (can_eval_dict_filters && IsDictFilterable(scalar_reader)) {
        dict_filterable_readers_.push_back(scalar_reader);
      } else {
        non_dict_filterable_readers_.push_back(scalar_reader);
      }
    }
  }
}

Status HdfsParquetScanner::InitDictFilterStructures() {
  bool can_eval_dict_filters =
      state_->query_options().parquet_dictionary_filtering && !dict_filter_map_.empty();

  // Separate column readers into scalar and collection readers.
  PartitionReaders(column_readers_, can_eval_dict_filters);

  // Allocate tuple buffers for all tuple descriptors that are associated with conjuncts
  // that can be dictionary filtered.
  for (auto* col_reader : dict_filterable_readers_) {
    const SlotDescriptor* slot_desc = col_reader->slot_desc();
    const TupleDescriptor* tuple_desc = slot_desc->parent();
    auto tuple_it = dict_filter_tuple_map_.find(tuple_desc);
    if (tuple_it != dict_filter_tuple_map_.end()) continue;
    int tuple_size = tuple_desc->byte_size();
    if (tuple_size > 0) {
      uint8_t* buffer = perm_pool_->TryAllocate(tuple_size);
      if (buffer == nullptr) {
        string details = Substitute(
            PARQUET_MEM_LIMIT_EXCEEDED, "InitDictFilterStructures", tuple_size,
            "Dictionary Filtering Tuple");
        return scan_node_->mem_tracker()->MemLimitExceeded(state_, details, tuple_size);
      }
      dict_filter_tuple_map_[tuple_desc] = reinterpret_cast<Tuple*>(buffer);
    }
  }
  return Status::OK();
}

bool HdfsParquetScanner::IsDictionaryEncoded(
    const parquet::ColumnMetaData& col_metadata) {
  // The Parquet spec allows for column chunks to have mixed encodings
  // where some data pages are dictionary-encoded and others are plain
  // encoded. For example, a Parquet file writer might start writing
  // a column chunk as dictionary encoded, but it will switch to plain
  // encoding if the dictionary grows too large.
  //
  // In order for dictionary filters to skip the entire row group,
  // the conjuncts must be evaluated on column chunks that are entirely
  // encoded with the dictionary encoding. There are two checks
  // available to verify this:
  // 1. The encoding_stats field on the column chunk metadata provides
  //    information about the number of data pages written in each
  //    format. This allows for a specific check of whether all the
  //    data pages are dictionary encoded.
  // 2. The encodings field on the column chunk metadata lists the
  //    encodings used. If this list contains the dictionary encoding
  //    and does not include unexpected encodings (i.e. encodings not
  //    associated with definition/repetition levels), then it is entirely
  //    dictionary encoded.

  if (col_metadata.__isset.encoding_stats) {
    // Condition #1 above
    for (const parquet::PageEncodingStats& enc_stat : col_metadata.encoding_stats) {
      if (enc_stat.page_type == parquet::PageType::DATA_PAGE &&
          enc_stat.encoding != parquet::Encoding::PLAIN_DICTIONARY &&
          enc_stat.count > 0) {
        return false;
      }
    }
  } else {
    // Condition #2 above
    bool has_dict_encoding = false;
    bool has_nondict_encoding = false;
    for (const parquet::Encoding::type& encoding : col_metadata.encodings) {
      if (encoding == parquet::Encoding::PLAIN_DICTIONARY) has_dict_encoding = true;

      // RLE and BIT_PACKED are used for repetition/definition levels
      if (encoding != parquet::Encoding::PLAIN_DICTIONARY &&
          encoding != parquet::Encoding::RLE &&
          encoding != parquet::Encoding::BIT_PACKED) {
        has_nondict_encoding = true;
        break;
      }
    }
    // Not entirely dictionary encoded if:
    // 1. No dictionary encoding listed
    // OR
    // 2. Some non-dictionary encoding is listed
    if (!has_dict_encoding || has_nondict_encoding) return false;
  }

  return true;
}

Status HdfsParquetScanner::EvalDictionaryFilters(const parquet::RowGroup& row_group,
    bool* row_group_eliminated) {
  *row_group_eliminated = false;
  // Check if there's anything to do here.
  if (dict_filterable_readers_.empty()) return Status::OK();

  // Legacy impala files (< 2.9) require special handling, because they do not encode
  // information about whether the column is 100% dictionary encoded.
  bool is_legacy_impala = false;
  if (file_version_.application == "impala" && file_version_.VersionLt(2,9,0)) {
    is_legacy_impala = true;
  }

  // Keeps track of column readers that need to be initialized. For example, if a
  // column cannot be filtered, then defer its dictionary initialization once we know
  // the row group cannot be filtered.
  vector<BaseScalarColumnReader*> deferred_dict_init_list;
  // Keeps track of the initialized tuple associated with a TupleDescriptor.
  unordered_map<const TupleDescriptor*, Tuple*> tuple_map;
  for (BaseScalarColumnReader* scalar_reader : dict_filterable_readers_) {
    const parquet::ColumnMetaData& col_metadata =
        row_group.columns[scalar_reader->col_idx()].meta_data;

    // Legacy impala files cannot be eliminated here, because the only way to
    // determine whether the column is 100% dictionary encoded requires reading
    // the dictionary.
    if (!is_legacy_impala && !IsDictionaryEncoded(col_metadata)) {
      // We cannot guarantee that this reader is 100% dictionary encoded,
      // so dictionary filters cannot be used. Defer initializing its dictionary
      // until after the other filters have been evaluated.
      deferred_dict_init_list.push_back(scalar_reader);
      continue;
    }

    RETURN_IF_ERROR(scalar_reader->InitDictionary());
    DictDecoderBase* dictionary = scalar_reader->GetDictionaryDecoder();
    if (!dictionary) continue;

    // Legacy (version < 2.9) Impala files do not spill to PLAIN encoding until
    // it reaches the maximum number of dictionary entries. If the dictionary
    // has fewer entries, then it is 100% dictionary encoded.
    if (is_legacy_impala &&
        dictionary->num_entries() >= LEGACY_IMPALA_MAX_DICT_ENTRIES) continue;

    const SlotDescriptor* slot_desc = scalar_reader->slot_desc();
    DCHECK(slot_desc != nullptr);
    const TupleDescriptor* tuple_desc = slot_desc->parent();
    auto dict_filter_it = dict_filter_map_.find(slot_desc->id());
    DCHECK(dict_filter_it != dict_filter_map_.end());
    const vector<ScalarExprEvaluator*>& dict_filter_conjunct_evals =
        dict_filter_it->second;
    Tuple* dict_filter_tuple = nullptr;
    auto dict_filter_tuple_it = tuple_map.find(tuple_desc);
    if (dict_filter_tuple_it == tuple_map.end()) {
      auto tuple_it = dict_filter_tuple_map_.find(tuple_desc);
      DCHECK(tuple_it != dict_filter_tuple_map_.end());
      dict_filter_tuple = tuple_it->second;
      dict_filter_tuple->Init(tuple_desc->byte_size());
      tuple_map[tuple_desc] = dict_filter_tuple;
    } else {
      dict_filter_tuple = dict_filter_tuple_it->second;
    }

    DCHECK(dict_filter_tuple != nullptr);
    void* slot = dict_filter_tuple->GetSlot(slot_desc->tuple_offset());
    bool column_has_match = false;
    for (int dict_idx = 0; dict_idx < dictionary->num_entries(); ++dict_idx) {
      if (dict_idx % 1024 == 0) {
        // Don't let expr result allocations accumulate too much for large dictionaries or
        // many row groups.
        context_->expr_results_pool()->Clear();
      }
      dictionary->GetValue(dict_idx, slot);

      // We can only eliminate this row group if no value from the dictionary matches.
      // If any dictionary value passes the conjuncts, then move on to the next column.
      TupleRow row;
      row.SetTuple(0, dict_filter_tuple);
      if (ExecNode::EvalConjuncts(dict_filter_conjunct_evals.data(),
              dict_filter_conjunct_evals.size(), &row)) {
        column_has_match = true;
        break;
      }
    }
    // Free all expr result allocations now that we're done with the filter.
    context_->expr_results_pool()->Clear();

    if (!column_has_match) {
      // The column contains no value that matches the conjunct. The row group
      // can be eliminated.
      *row_group_eliminated = true;
      return Status::OK();
    }
  }

  // Any columns that were not 100% dictionary encoded need to initialize
  // their dictionaries here.
  RETURN_IF_ERROR(BaseScalarColumnReader::InitDictionaries(deferred_dict_init_list));

  return Status::OK();
}

/// High-level steps of this function:
/// 1. Allocate 'scratch' memory for tuples able to hold a full batch
/// 2. Populate the slots of all scratch tuples one column reader at a time,
///    using the ColumnReader::Read*ValueBatch() functions.
/// 3. Evaluate runtime filters and conjuncts against the scratch tuples and
///    set the surviving tuples in the output batch. Transfer the ownership of
///    scratch memory to the output batch once the scratch memory is exhausted.
/// 4. Repeat steps above until we are done with the row group or an error
///    occurred.
/// TODO: Since the scratch batch is populated in a column-wise fashion, it is
/// difficult to maintain a maximum memory footprint without throwing away at least
/// some work. This point needs further experimentation and thought.
Status HdfsParquetScanner::AssembleRows(
    const vector<ParquetColumnReader*>& column_readers, RowBatch* row_batch,
    bool* skip_row_group) {
  DCHECK(!column_readers.empty());
  DCHECK(row_batch != nullptr);
  DCHECK_EQ(*skip_row_group, false);
  DCHECK(scratch_batch_ != nullptr);

  int64_t num_rows_read = 0;
  while (!column_readers[0]->RowGroupAtEnd()) {
    // Start a new scratch batch.
    RETURN_IF_ERROR(scratch_batch_->Reset(state_));
    InitTupleBuffer(template_tuple_, scratch_batch_->tuple_mem, scratch_batch_->capacity);

    // Materialize the top-level slots into the scratch batch column-by-column.
    int last_num_tuples = -1;
    for (int c = 0; c < column_readers.size(); ++c) {
      ParquetColumnReader* col_reader = column_readers[c];
      bool continue_execution;
      if (col_reader->max_rep_level() > 0) {
        continue_execution = col_reader->ReadValueBatch(&scratch_batch_->aux_mem_pool,
            scratch_batch_->capacity, tuple_byte_size_, scratch_batch_->tuple_mem,
            &scratch_batch_->num_tuples);
      } else {
        continue_execution = col_reader->ReadNonRepeatedValueBatch(
            &scratch_batch_->aux_mem_pool, scratch_batch_->capacity, tuple_byte_size_,
            scratch_batch_->tuple_mem, &scratch_batch_->num_tuples);
      }
      // Check that all column readers populated the same number of values.
      bool num_tuples_mismatch = c != 0 && last_num_tuples != scratch_batch_->num_tuples;
      if (UNLIKELY(!continue_execution || num_tuples_mismatch)) {
        // Skipping this row group. Free up all the resources with this row group.
        FlushRowGroupResources(row_batch);
        scratch_batch_->num_tuples = 0;
        DCHECK(scratch_batch_->AtEnd());
        *skip_row_group = true;
        if (num_tuples_mismatch && continue_execution) {
          Status err(Substitute("Corrupt Parquet file '$0': column '$1' "
              "had $2 remaining values but expected $3", filename(),
              col_reader->schema_element().name, last_num_tuples,
              scratch_batch_->num_tuples));
          parse_status_.MergeStatus(err);
        }
        return Status::OK();
      }
      last_num_tuples = scratch_batch_->num_tuples;
    }
    RETURN_IF_ERROR(CheckPageFiltering());
    num_rows_read += scratch_batch_->num_tuples;
    int num_row_to_commit = TransferScratchTuples(row_batch);
    RETURN_IF_ERROR(CommitRows(row_batch, num_row_to_commit));
    if (row_batch->AtCapacity()) break;
  }
  row_group_rows_read_ += num_rows_read;
  COUNTER_ADD(scan_node_->rows_read_counter(), num_rows_read);
  // Merge Scanner-local counter into HdfsScanNode counter and reset.
  COUNTER_ADD(scan_node_->collection_items_read_counter(), coll_items_read_counter_);
  coll_items_read_counter_ = 0;
  return Status::OK();
}

Status HdfsParquetScanner::CheckPageFiltering() {
  if (candidate_ranges_.empty() || scalar_readers_.empty()) return Status::OK();

  int64_t current_row = scalar_readers_[0]->LastProcessedRow();
  for (int i = 1; i < scalar_readers_.size(); ++i) {
    if (current_row != scalar_readers_[i]->LastProcessedRow()) {
      DCHECK(false);
      return Status(Substitute(
          "Top level rows aren't in sync during page filtering in file $0.", filename()));
    }
  }
  return Status::OK();
}

Status HdfsParquetScanner::CommitRows(RowBatch* dst_batch, int num_rows) {
  DCHECK(dst_batch != nullptr);
  dst_batch->CommitRows(num_rows);

  if (context_->cancelled()) return Status::CancelledInternal("Parquet scanner");
  // TODO: It's a really bad idea to propagate UDF error via the global RuntimeState.
  // Store UDF error in thread local storage or make UDF return status so it can merge
  // with parse_status_.
  RETURN_IF_ERROR(state_->GetQueryStatus());
  // Clear expr result allocations for this thread to avoid accumulating too much
  // memory from evaluating the scanner conjuncts.
  context_->expr_results_pool()->Clear();
  return Status::OK();
}

bool HdfsParquetScanner::AssembleCollection(
    const vector<ParquetColumnReader*>& column_readers, int new_collection_rep_level,
    CollectionValueBuilder* coll_value_builder) {
  DCHECK(!column_readers.empty());
  DCHECK_GE(new_collection_rep_level, 0);
  DCHECK(coll_value_builder != nullptr);

  const TupleDescriptor* tuple_desc = &coll_value_builder->tuple_desc();
  Tuple* template_tuple = template_tuple_map_[tuple_desc];
  const vector<ScalarExprEvaluator*> evals =
      conjunct_evals_map_[tuple_desc->id()];

  int64_t rows_read = 0;
  bool continue_execution = !scan_node_->ReachedLimitShared() && !context_->cancelled();
  // Note that this will be set to true at the end of the row group or the end of the
  // current collection (if applicable).
  bool end_of_collection = column_readers[0]->rep_level() == -1;
  // We only initialize end_of_collection to true here if we're at the end of the row
  // group (otherwise it would always be true because we're on the "edge" of two
  // collections), and only ProcessSplit() should call AssembleRows() at the end of the
  // row group.
  if (coll_value_builder != nullptr) DCHECK(!end_of_collection);

  while (!end_of_collection && continue_execution) {
    MemPool* pool;
    Tuple* tuple;
    TupleRow* row = nullptr;

    int64_t num_rows;
    // We're assembling item tuples into an CollectionValue
    parse_status_ =
        GetCollectionMemory(coll_value_builder, &pool, &tuple, &row, &num_rows);
    if (UNLIKELY(!parse_status_.ok())) {
      continue_execution = false;
      break;
    }
    // 'num_rows' can be very high if we're writing to a large CollectionValue. Limit
    // the number of rows we read at one time so we don't spend too long in the
    // 'num_rows' loop below before checking for cancellation or limit reached.
    num_rows = min(
        num_rows, static_cast<int64_t>(scan_node_->runtime_state()->batch_size()));

    int num_to_commit = 0;
    int row_idx = 0;
    for (row_idx = 0; row_idx < num_rows && !end_of_collection; ++row_idx) {
      DCHECK(continue_execution);
      // A tuple is produced iff the collection that contains its values is not empty and
      // non-NULL. (Empty or NULL collections produce no output values, whereas NULL is
      // output for the fields of NULL structs.)
      bool materialize_tuple = column_readers[0]->def_level() >=
          column_readers[0]->def_level_of_immediate_repeated_ancestor();
      InitTuple(tuple_desc, template_tuple, tuple);
      continue_execution =
          ReadCollectionItem(column_readers, materialize_tuple, pool, tuple);
      if (UNLIKELY(!continue_execution)) break;
      end_of_collection = column_readers[0]->rep_level() <= new_collection_rep_level;

      if (materialize_tuple) {
        if (ExecNode::EvalConjuncts(evals.data(), evals.size(), row)) {
          tuple = next_tuple(tuple_desc->byte_size(), tuple);
          ++num_to_commit;
        }
      }
    }

    rows_read += row_idx;
    coll_value_builder->CommitTuples(num_to_commit);
    continue_execution &= !scan_node_->ReachedLimitShared() && !context_->cancelled();
  }
  coll_items_read_counter_ += rows_read;
  if (end_of_collection) {
    // All column readers should report the start of the same collection.
    for (int c = 1; c < column_readers.size(); ++c) {
      FILE_CHECK_EQ(column_readers[c]->rep_level(), column_readers[0]->rep_level());
    }
  }
  return continue_execution;
}

inline bool HdfsParquetScanner::ReadCollectionItem(
    const vector<ParquetColumnReader*>& column_readers,
    bool materialize_tuple, MemPool* pool, Tuple* tuple) const {
  DCHECK(!column_readers.empty());
  bool continue_execution = true;
  int size = column_readers.size();
  for (int c = 0; c < size; ++c) {
    ParquetColumnReader* col_reader = column_readers[c];
    if (materialize_tuple) {
      // All column readers for this tuple should a value to materialize.
      FILE_CHECK_GE(col_reader->def_level(),
                    col_reader->def_level_of_immediate_repeated_ancestor());
      // Fill in position slot if applicable
      const SlotDescriptor* pos_slot_desc = col_reader->pos_slot_desc();
      if (pos_slot_desc != nullptr) {
        col_reader->ReadPositionNonBatched(
            tuple->GetBigIntSlot(pos_slot_desc->tuple_offset()));
      }
      continue_execution = col_reader->ReadValue(pool, tuple);
    } else {
      // A containing repeated field is empty or NULL
      FILE_CHECK_LT(col_reader->def_level(),
                    col_reader->def_level_of_immediate_repeated_ancestor());
      continue_execution = col_reader->NextLevels();
    }
    if (UNLIKELY(!continue_execution)) break;
  }
  return continue_execution;
}

Status HdfsParquetScanner::ProcessFooter() {
  const int64_t file_len = stream_->file_desc()->file_length;
  const int64_t scan_range_len = stream_->scan_range()->len();

  // We're processing the scan range issued in IssueInitialRanges(). The scan range should
  // be the last FOOTER_BYTES of the file. !success means the file is shorter than we
  // expect. Note we can't detect if the file is larger than we expect without attempting
  // to read past the end of the scan range, but in this case we'll fail below trying to
  // parse the footer.
  DCHECK_LE(scan_range_len, FOOTER_SIZE);
  uint8_t* buffer;
  bool success = stream_->ReadBytes(scan_range_len, &buffer, &parse_status_);
  if (!success) {
    DCHECK(!parse_status_.ok());
    if (parse_status_.code() == TErrorCode::SCANNER_INCOMPLETE_READ) {
      VLOG_QUERY << "Metadata for file '" << filename() << "' appears stale: "
                 << "metadata states file size to be "
                 << PrettyPrinter::Print(file_len, TUnit::BYTES)
                 << ", but could only read "
                 << PrettyPrinter::Print(stream_->total_bytes_returned(), TUnit::BYTES);
      return Status(TErrorCode::STALE_METADATA_FILE_TOO_SHORT, filename(),
          scan_node_->hdfs_table()->fully_qualified_name());
    }
    return parse_status_;
  }
  DCHECK(stream_->eosr());

  // Number of bytes in buffer after the fixed size footer is accounted for.
  int remaining_bytes_buffered = scan_range_len - sizeof(int32_t) -
      sizeof(PARQUET_VERSION_NUMBER);

  // Make sure footer has enough bytes to contain the required information.
  if (remaining_bytes_buffered < 0) {
    return Status(Substitute("File '$0' is invalid. Missing metadata.", filename()));
  }

  // Validate magic file bytes are correct.
  uint8_t* magic_number_ptr = buffer + scan_range_len - sizeof(PARQUET_VERSION_NUMBER);
  if (memcmp(magic_number_ptr, PARQUET_VERSION_NUMBER,
             sizeof(PARQUET_VERSION_NUMBER)) != 0) {
    return Status(TErrorCode::PARQUET_BAD_VERSION_NUMBER, filename(),
        string(reinterpret_cast<char*>(magic_number_ptr), sizeof(PARQUET_VERSION_NUMBER)),
        scan_node_->hdfs_table()->fully_qualified_name());
  }

  // The size of the metadata is encoded as a 4 byte little endian value before
  // the magic number
  uint8_t* metadata_size_ptr = magic_number_ptr - sizeof(int32_t);
  uint32_t metadata_size = *reinterpret_cast<uint32_t*>(metadata_size_ptr);
  // The start of the metadata is:
  // file_len - 4-byte footer length field - 4-byte version number field - metadata size
  int64_t metadata_start = file_len - sizeof(int32_t) - sizeof(PARQUET_VERSION_NUMBER) -
      metadata_size;
  if (UNLIKELY(metadata_start < 0)) {
    return Status(Substitute("File '$0' is invalid. Invalid metadata size in file "
                             "footer: $1 bytes. File size: $2 bytes.",
        filename(), metadata_size, file_len));
  }
  uint8_t* metadata_ptr = metadata_size_ptr - metadata_size;

  // If the metadata was too big, we need to read it into a contiguous buffer before
  // deserializing it.
  ScopedBuffer metadata_buffer(scan_node_->mem_tracker());

  DCHECK(metadata_range_ != nullptr);
  if (UNLIKELY(metadata_size > remaining_bytes_buffered)) {
    // In this case, the metadata is bigger than our guess meaning there are
    // not enough bytes in the footer range from IssueInitialRanges().
    // We'll just issue more ranges to the IoMgr that is the actual footer.
    int64_t partition_id = context_->partition_descriptor()->id();
    const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(partition_id, filename());
    DCHECK_EQ(file_desc, stream_->file_desc());

    if (!metadata_buffer.TryAllocate(metadata_size)) {
      string details = Substitute("Could not allocate buffer of $0 bytes for Parquet "
          "metadata for file '$1'.", metadata_size, filename());
      return scan_node_->mem_tracker()->MemLimitExceeded(state_, details, metadata_size);
    }
    metadata_ptr = metadata_buffer.buffer();

    // Read the footer into the metadata buffer. Skip HDFS caching in this case.
    int cache_options = metadata_range_->cache_options() & ~BufferOpts::USE_HDFS_CACHE;
    ScanRange* metadata_range = scan_node_->AllocateScanRange(
        metadata_range_->fs(), filename(), metadata_size, metadata_start, partition_id,
        metadata_range_->disk_id(), metadata_range_->expected_local(),
        metadata_range_->is_erasure_coded(), metadata_range_->mtime(),
        BufferOpts::ReadInto(metadata_buffer.buffer(), metadata_size, cache_options));

    unique_ptr<BufferDescriptor> io_buffer;
    bool needs_buffers;
    RETURN_IF_ERROR(
        scan_node_->reader_context()->StartScanRange(metadata_range, &needs_buffers));
    DCHECK(!needs_buffers) << "Already provided a buffer";
    RETURN_IF_ERROR(metadata_range->GetNext(&io_buffer));
    DCHECK_EQ(io_buffer->buffer(), metadata_buffer.buffer());
    DCHECK_EQ(io_buffer->len(), metadata_size);
    DCHECK(io_buffer->eosr());
    metadata_range->ReturnBuffer(move(io_buffer));
  }

  // Deserialize file footer
  // TODO: this takes ~7ms for a 1000-column table, figure out how to reduce this.
  Status status =
      DeserializeThriftMsg(metadata_ptr, &metadata_size, true, &file_metadata_);
  if (!status.ok()) {
    return Status(Substitute("File '$0' of length $1 bytes has invalid file metadata "
        "at file offset $2, Error = $3.", filename(), file_len, metadata_start,
        status.GetDetail()));
  }

  RETURN_IF_ERROR(ParquetMetadataUtils::ValidateFileVersion(file_metadata_, filename()));

  // IMPALA-3943: Do not throw an error for empty files for backwards compatibility.
  if (file_metadata_.num_rows == 0) {
    // Warn if the num_rows is inconsistent with the row group metadata.
    if (!file_metadata_.row_groups.empty()) {
      bool has_non_empty_row_group = false;
      for (const parquet::RowGroup& row_group : file_metadata_.row_groups) {
        if (row_group.num_rows > 0) {
          has_non_empty_row_group = true;
          break;
        }
      }
      // Warn if there is at least one non-empty row group.
      if (has_non_empty_row_group) {
        ErrorMsg msg(TErrorCode::PARQUET_ZERO_ROWS_IN_NON_EMPTY_FILE, filename());
        state_->LogError(msg);
      }
    }
    return Status::OK();
  }

  // Parse out the created by application version string
  if (file_metadata_.__isset.created_by) {
    file_version_ = ParquetFileVersion(file_metadata_.created_by);
  }
  if (file_metadata_.row_groups.empty()) {
    return Status(
        Substitute("Invalid file. This file: $0 has no row groups", filename()));
  }
  if (file_metadata_.num_rows < 0) {
    return Status(Substitute("Corrupt Parquet file '$0': negative row count $1 in "
        "file metadata", filename(), file_metadata_.num_rows));
  }
  return Status::OK();
}

Status HdfsParquetScanner::CreateColumnReaders(const TupleDescriptor& tuple_desc,
    const ParquetSchemaResolver& schema_resolver,
    vector<ParquetColumnReader*>* column_readers) {
  DCHECK(column_readers != nullptr);
  DCHECK(column_readers->empty());

  if (scan_node_->optimize_parquet_count_star()) {
    // Column readers are not needed because we are not reading from any columns if this
    // optimization is enabled.
    return Status::OK();
  }

  // Each tuple can have at most one position slot. We'll process this slot desc last.
  SlotDescriptor* pos_slot_desc = nullptr;

  for (SlotDescriptor* slot_desc: tuple_desc.slots()) {
    // Skip partition columns
    if (&tuple_desc == scan_node_->tuple_desc() &&
        slot_desc->col_pos() < scan_node_->num_partition_keys()) continue;

    SchemaNode* node = nullptr;
    bool pos_field;
    bool missing_field;
    RETURN_IF_ERROR(schema_resolver.ResolvePath(
        slot_desc->col_path(), &node, &pos_field, &missing_field));

    if (missing_field) {
      // In this case, we are selecting a column that is not in the file.
      // Update the template tuple to put a NULL in this slot.
      Tuple** template_tuple = &template_tuple_map_[&tuple_desc];
      if (*template_tuple == nullptr) {
        *template_tuple =
            Tuple::Create(tuple_desc.byte_size(), template_tuple_pool_.get());
      }
      (*template_tuple)->SetNull(slot_desc->null_indicator_offset());
      continue;
    }

    if (pos_field) {
      DCHECK(pos_slot_desc == nullptr)
          << "There should only be one position slot per tuple";
      pos_slot_desc = slot_desc;
      continue;
    }

    RETURN_IF_ERROR(ParquetMetadataUtils::ValidateColumn(filename(), *node->element,
        slot_desc, state_));

    ParquetColumnReader* col_reader = ParquetColumnReader::Create(
        *node, slot_desc->type().IsCollectionType(), slot_desc, this);
    column_readers->push_back(col_reader);

    if (col_reader->IsCollectionReader()) {
      // Recursively populate col_reader's children
      DCHECK(slot_desc->collection_item_descriptor() != nullptr);
      const TupleDescriptor* item_tuple_desc = slot_desc->collection_item_descriptor();
      CollectionColumnReader* collection_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      RETURN_IF_ERROR(CreateColumnReaders(
          *item_tuple_desc, schema_resolver, collection_reader->children()));
    } else {
      scalar_reader_map_[node->col_idx] = static_cast<BaseScalarColumnReader*>(
          col_reader);
    }
  }

  if (column_readers->empty()) {
    // This is either a count(*) over a collection type (count(*) over the table is
    // handled in ProcessFooter()), or no materialized columns appear in this file
    // (e.g. due to schema evolution, or if there's only a position slot). Create a single
    // column reader that we will use to count the number of tuples we should output. We
    // will not read any values from this reader.
    ParquetColumnReader* reader;
    RETURN_IF_ERROR(CreateCountingReader(
        tuple_desc.tuple_path(), schema_resolver, &reader));
    column_readers->push_back(reader);
  }

  if (pos_slot_desc != nullptr) {
    // 'tuple_desc' has a position slot. Use an existing column reader to populate it.
    DCHECK(!column_readers->empty());
    (*column_readers)[0]->set_pos_slot_desc(pos_slot_desc);
  }

  return Status::OK();
}

Status HdfsParquetScanner::CreateCountingReader(const SchemaPath& parent_path,
    const ParquetSchemaResolver& schema_resolver, ParquetColumnReader** reader) {
  SchemaNode* parent_node;
  bool pos_field;
  bool missing_field;
  RETURN_IF_ERROR(schema_resolver.ResolvePath(
      parent_path, &parent_node, &pos_field, &missing_field));

  if (missing_field) {
    // TODO: can we do anything else here?
    return Status(Substitute("Could not find '$0' in file '$1'.",
        PrintPath(*scan_node_->hdfs_table(), parent_path), filename()));
  }
  DCHECK(!pos_field);
  DCHECK(parent_path.empty() || parent_node->is_repeated());

  if (!parent_node->children.empty()) {
    // Find a non-struct (i.e. collection or scalar) child of 'parent_node', which we will
    // use to create the item reader
    const SchemaNode* target_node = &parent_node->children[0];
    while (!target_node->children.empty() && !target_node->is_repeated()) {
      target_node = &target_node->children[0];
    }

    *reader = ParquetColumnReader::Create(
        *target_node, target_node->is_repeated(), nullptr, this);
    if (target_node->is_repeated()) {
      // Find the closest scalar descendant of 'target_node' via breadth-first search, and
      // create scalar reader to drive 'reader'. We find the closest (i.e. least-nested)
      // descendant as a heuristic for picking a descendant with fewer values, so it's
      // faster to scan.
      // TODO: use different heuristic than least-nested? Fewest values?
      const SchemaNode* node = nullptr;
      queue<const SchemaNode*> nodes;
      nodes.push(target_node);
      while (!nodes.empty()) {
        node = nodes.front();
        nodes.pop();
        if (node->children.size() > 0) {
          for (const SchemaNode& child: node->children) nodes.push(&child);
        } else {
          // node is the least-nested scalar descendant of 'target_node'
          break;
        }
      }
      DCHECK(node->children.empty()) << node->DebugString();
      CollectionColumnReader* parent_reader =
          static_cast<CollectionColumnReader*>(*reader);
      parent_reader->children()->push_back(
          ParquetColumnReader::Create(*node, false, nullptr, this));
    }
  } else {
    // Special case for a repeated scalar node. The repeated node represents both the
    // parent collection and the child item.
    *reader = ParquetColumnReader::Create(*parent_node, false, nullptr, this);
  }

  return Status::OK();
}

void HdfsParquetScanner::InitCollectionColumns() {
  for (CollectionColumnReader* col_reader: collection_readers_) {
    col_reader->Reset();
  }
}

Status HdfsParquetScanner::InitScalarColumns() {
  int64_t partition_id = context_->partition_descriptor()->id();
  const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(partition_id, filename());
  DCHECK(file_desc != nullptr);
  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx_];

  // Used to validate that the number of values in each reader in column_readers_ at the
  // same SchemaElement is the same.
  unordered_map<const parquet::SchemaElement*, int> num_values_map;
  for (BaseScalarColumnReader* scalar_reader : scalar_readers_) {
    const parquet::ColumnChunk& col_chunk = row_group.columns[scalar_reader->col_idx()];
    auto num_values_it = num_values_map.find(&scalar_reader->schema_element());
    int num_values = -1;
    if (num_values_it != num_values_map.end()) {
      num_values = num_values_it->second;
    } else {
      num_values_map[&scalar_reader->schema_element()] = col_chunk.meta_data.num_values;
    }
    if (num_values != -1 && col_chunk.meta_data.num_values != num_values) {
      // TODO: improve this error message by saying which columns are different,
      // and also specify column in other error messages as appropriate
      return Status(TErrorCode::PARQUET_NUM_COL_VALS_ERROR, scalar_reader->col_idx(),
          col_chunk.meta_data.num_values, num_values, filename());
    }
    RETURN_IF_ERROR(scalar_reader->Reset(*file_desc, col_chunk, row_group_idx_));
  }
  RETURN_IF_ERROR(DivideReservationBetweenColumns(scalar_readers_));
  return Status::OK();
}

Status HdfsParquetScanner::DivideReservationBetweenColumns(
    const vector<BaseScalarColumnReader*>& column_readers) {
  DiskIoMgr* io_mgr = ExecEnv::GetInstance()->disk_io_mgr();
  const int64_t min_buffer_size = io_mgr->min_buffer_size();
  const int64_t max_buffer_size = io_mgr->max_buffer_size();
  // The HdfsScanNode reservation calculation in the planner ensures that we have
  // reservation for at least one buffer per column.
  if (context_->total_reservation() < min_buffer_size * column_readers.size()) {
    return Status(TErrorCode::INTERNAL_ERROR,
        Substitute("Not enough reservation in Parquet scanner for file '$0'. Need at "
                   "least $1 bytes per column for $2 columns but had $3 bytes",
            filename(), min_buffer_size, column_readers.size(),
            context_->total_reservation()));
  }

  vector<int64_t> col_range_lengths(column_readers.size());
  for (int i = 0; i < column_readers.size(); ++i) {
    col_range_lengths[i] = column_readers[i]->scan_range()->bytes_to_read();
  }

  // The scanner-wide stream was used only to read the file footer.  Each column has added
  // its own stream. We can use the total reservation now that 'stream_''s resources have
  // been released. We may benefit from increasing reservation further, so let's compute
  // the ideal reservation to scan all the columns.
  int64_t ideal_reservation = ComputeIdealReservation(col_range_lengths);
  if (ideal_reservation > context_->total_reservation()) {
    context_->TryIncreaseReservation(ideal_reservation);
  }
  scan_node_->runtime_profile()->GetSummaryStatsCounter(ACTUAL_RESERVATION_COUNTER_NAME)->
      UpdateCounter(context_->total_reservation());
  scan_node_->runtime_profile()->GetSummaryStatsCounter(IDEAL_RESERVATION_COUNTER_NAME)->
      UpdateCounter(ideal_reservation);

  vector<pair<int, int64_t>> tmp_reservations = DivideReservationBetweenColumnsHelper(
      min_buffer_size, max_buffer_size, col_range_lengths, context_->total_reservation());
  for (auto& tmp_reservation : tmp_reservations) {
    column_readers[tmp_reservation.first]->set_io_reservation(tmp_reservation.second);
  }
  return Status::OK();
}

int64_t HdfsParquetScanner::ComputeIdealReservation(
    const vector<int64_t>& col_range_lengths) {
  DiskIoMgr* io_mgr = ExecEnv::GetInstance()->disk_io_mgr();
  int64_t ideal_reservation = 0;
  for (int64_t len : col_range_lengths) {
    ideal_reservation += io_mgr->ComputeIdealBufferReservation(len);
  }
  return ideal_reservation;
}

vector<pair<int, int64_t>> HdfsParquetScanner::DivideReservationBetweenColumnsHelper(
    int64_t min_buffer_size, int64_t max_buffer_size,
    const vector<int64_t>& col_range_lengths, int64_t reservation_to_distribute) {
  // Pair of (column index, reservation allocated).
  vector<pair<int, int64_t>> tmp_reservations;
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
        return left_len != right_len ? left_len > right_len : left.first < right.first;
      });

  // Set aside the minimum reservation per column.
  reservation_to_distribute -= min_buffer_size * col_range_lengths.size();

  // Allocate reservations to columns by repeatedly allocating either a max-sized buffer
  // or a large enough buffer to fit the remaining data for each column. Do this
  // round-robin up to the ideal number of I/O buffers.
  for (int i = 0; i < DiskIoMgr::IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE; ++i) {
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
      } else if (bytes_left_in_range > 0 &&
          reservation_to_distribute >= min_buffer_size) {
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

Status HdfsParquetScanner::InitDictionaries(
    const vector<BaseScalarColumnReader*>& column_readers) {
  for (BaseScalarColumnReader* scalar_reader : column_readers) {
    RETURN_IF_ERROR(scalar_reader->InitDictionary());
  }
  return Status::OK();
}

Status HdfsParquetScanner::ValidateEndOfRowGroup(
    const vector<ParquetColumnReader*>& column_readers, int row_group_idx,
    int64_t rows_read) {
  DCHECK(!column_readers.empty());
  DCHECK(parse_status_.ok()) << "Don't overwrite parse_status_"
      << parse_status_.GetDetail();

  if (column_readers[0]->max_rep_level() == 0) {
    if (candidate_ranges_.empty()) {
      // These column readers materialize table-level values (vs. collection values).
      // Test if the expected number of rows from the file metadata matches the actual
      // number of rows read from the file.
      int64_t expected_rows_in_group = file_metadata_.row_groups[row_group_idx].num_rows;
      if (rows_read != expected_rows_in_group) {
        return Status(TErrorCode::PARQUET_GROUP_ROW_COUNT_ERROR, filename(),
            row_group_idx, expected_rows_in_group, rows_read);
      }
    } else {
      // In this case we filter out row ranges. Validate that the number of rows read
      // matches the number of rows determined by the candidate row ranges.
      int64_t expected_rows_in_group = 0;
      for (auto& range : candidate_ranges_) {
        expected_rows_in_group += range.last - range.first + 1;
      }
      if (rows_read != expected_rows_in_group) {
        return Status(Substitute("Based on the page index of group $0($1) there are $2 ",
            "rows need to be scanned, but $3 rows were read.", filename(), row_group_idx,
            expected_rows_in_group, rows_read));
      }
    }
  }

  // Validate scalar column readers' state
  int num_values_read = -1;
  for (int c = 0; c < column_readers.size(); ++c) {
    if (column_readers[c]->IsCollectionReader()) continue;
    BaseScalarColumnReader* reader =
        static_cast<BaseScalarColumnReader*>(column_readers[c]);
    // All readers should have exhausted the final data page. This could fail if one
    // column has more values than stated in the metadata, meaning the final data page
    // will still have unread values.
    if (reader->num_buffered_values_ != 0) {
      return Status(Substitute("Corrupt Parquet metadata in file '$0': metadata reports "
          "'$1' more values in data page than actually present", filename(),
          reader->num_buffered_values_));
    }
    // Sanity check that the num_values_read_ value is the same for all readers. All
    // readers should have been advanced in lockstep (the above check is more likely to
    // fail if this not the case though, since num_values_read_ is only updated at the end
    // of a data page).
    if (num_values_read == -1) num_values_read = reader->num_values_read_;
    if (candidate_ranges_.empty()) DCHECK_EQ(reader->num_values_read_, num_values_read);
    // ReadDataPage() uses metadata_->num_values to determine when the column's done
    DCHECK(!candidate_ranges_.empty() ||
        (reader->num_values_read_ == reader->metadata_->num_values ||
        !state_->abort_on_error()));
  }
  return Status::OK();
}

ColumnStatsReader HdfsParquetScanner::CreateColumnStatsReader(
    const parquet::ColumnChunk& col_chunk, const ColumnType& col_type,
    const parquet::ColumnOrder* col_order, const parquet::SchemaElement& element) {
  ColumnStatsReader stats_reader(col_chunk, col_type, col_order, element);
  if (col_type.IsTimestampType()) {
    stats_reader.SetTimestampDecoder(CreateTimestampDecoder(element));
  }
  return stats_reader;
}

ParquetTimestampDecoder HdfsParquetScanner::CreateTimestampDecoder(
    const parquet::SchemaElement& element) {
  bool timestamp_conversion_needed_for_int96_timestamps =
      FLAGS_convert_legacy_hive_parquet_utc_timestamps &&
      file_version_.application == "parquet-mr" &&
      state_->local_time_zone() != UTCPTR;

  return ParquetTimestampDecoder(element, state_->local_time_zone(),
      timestamp_conversion_needed_for_int96_timestamps);
}

void HdfsParquetScanner::UpdateCompressedPageSizeCounter(int64_t compressed_page_size) {
  parquet_compressed_page_size_counter_->UpdateCounter(compressed_page_size);
}

void HdfsParquetScanner::UpdateUncompressedPageSizeCounter(
    int64_t uncompressed_page_size) {
  parquet_uncompressed_page_size_counter_->UpdateCounter(uncompressed_page_size);
}

}
