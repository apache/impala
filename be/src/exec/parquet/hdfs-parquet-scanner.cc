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
#include "exec/parquet/parquet-bloom-filter-util.h"
#include "exec/parquet/parquet-collection-column-reader.h"
#include "exec/parquet/parquet-column-readers.h"
#include "exec/parquet/parquet-struct-column-reader.h"
#include "exec/scanner-context.inline.h"
#include "exec/scratch-tuple-batch.h"
#include "exprs/literal.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-fn-call.h"
#include "exprs/slot-ref.h"
#include "rpc/thrift-util.h"
#include "runtime/collection-value-builder.h"
#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/request-context.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/scoped-buffer.h"
#include "service/hs2-util.h"
#include "util/dict-encoding.h"
#include "util/parquet-bloom-filter.h"
#include "util/pretty-printer.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

using std::move;
using std::sort;
using namespace impala;
using namespace impala::io;

namespace impala {

// Max entries in the dictionary before switching to PLAIN encoding. If a dictionary
// has fewer entries, then the entire column is dictionary encoded. This threshold
// is guaranteed to be true for Impala versions 2.9 or below.
// THIS RECORDS INFORMATION ABOUT PAST BEHAVIOR. DO NOT CHANGE THIS CONSTANT.
const int LEGACY_IMPALA_MAX_DICT_ENTRIES = 40000;

static const string PARQUET_MEM_LIMIT_EXCEEDED =
    "HdfsParquetScanner::$0() failed to allocate $1 bytes for $2.";


Status HdfsParquetScanner::IssueInitialRanges(HdfsScanNodeBase* scan_node,
    const vector<HdfsFileDesc*>& files) {
  DCHECK(!files.empty());
  for (HdfsFileDesc* file : files) {
    // If the file size is less than 12 bytes, it is an invalid Parquet file.
    if (file->file_length < 12) {
      return Status(Substitute("Parquet file $0 has an invalid file length: $1",
          file->filename, file->file_length));
    }
  }
  return IssueFooterRanges(
      scan_node, THdfsFileFormat::PARQUET, files, PARQUET_FOOTER_SIZE);
}

HdfsParquetScanner::HdfsParquetScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
  : HdfsColumnarScanner(scan_node, state),
    row_group_idx_(-1),
    row_group_rows_read_(0),
    advance_row_group_(true),
    min_max_tuple_(nullptr),
    row_batches_produced_(0),
    dictionary_pool_(new MemPool(scan_node->mem_tracker())),
    stats_batch_read_pool_(new MemPool(scan_node->mem_tracker())),
    assemble_rows_timer_(scan_node_->materialize_tuple_timer()),
    num_stats_filtered_row_groups_counter_(nullptr),
    num_minmax_filtered_row_groups_counter_(nullptr),
    num_bloom_filtered_row_groups_counter_(nullptr),
    num_rowgroups_skipped_by_unuseful_filters_counter_(nullptr),
    num_row_groups_counter_(nullptr),
    num_minmax_filtered_pages_counter_(nullptr),
    num_dict_filtered_row_groups_counter_(nullptr),
    parquet_compressed_page_size_counter_(nullptr),
    parquet_uncompressed_page_size_counter_(nullptr),
    coll_items_read_counter_(0),
    page_index_(this),
    late_materialization_threshold_(
      state->query_options().parquet_late_materialization_threshold) {
  assemble_rows_timer_.Stop();
  complete_micro_batch_ = {0, state_->batch_size() - 1, state_->batch_size()};
}

Status HdfsParquetScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsColumnarScanner::Open(context));
  metadata_range_ = stream_->scan_range();
  num_stats_filtered_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumStatsFilteredRowGroups",
          TUnit::UNIT);
  num_minmax_filtered_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumRuntimeFilteredRowGroups",
          TUnit::UNIT);
  num_bloom_filtered_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumBloomFilteredRowGroups",
          TUnit::UNIT);
  num_rowgroups_skipped_by_unuseful_filters_counter_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "NumRowGroupsSkippedByUnusefulFilters", TUnit::UNIT);
  num_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumRowGroups", TUnit::UNIT);
  num_row_groups_with_page_index_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumRowGroupsWithPageIndex",
          TUnit::UNIT);
  num_stats_filtered_pages_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumStatsFilteredPages", TUnit::UNIT);
  num_minmax_filtered_pages_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumRuntimeFilteredPages", TUnit::UNIT);
  num_pages_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumPages", TUnit::UNIT);
  num_pages_skipped_by_late_materialization_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumPagesSkippedByLateMaterialization",
          TUnit::UNIT);
  num_dict_filtered_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumDictFilteredRowGroups", TUnit::UNIT);
  parquet_compressed_page_size_counter_ = ADD_SUMMARY_STATS_COUNTER(
      scan_node_->runtime_profile(), "ParquetCompressedPageSize", TUnit::BYTES);
  parquet_uncompressed_page_size_counter_ = ADD_SUMMARY_STATS_COUNTER(
      scan_node_->runtime_profile(), "ParquetUncompressedPageSize", TUnit::BYTES);
  process_page_index_stats_ =
      ADD_SUMMARY_STATS_TIMER(scan_node_->runtime_profile(), "PageIndexProcessingTime");

  codegend_process_scratch_batch_fn_ = scan_node_->GetCodegenFn(THdfsFileFormat::PARQUET);
  if (codegend_process_scratch_batch_fn_ == nullptr) {
    scan_node_->IncNumScannersCodegenDisabled();
  } else {
    scan_node_->IncNumScannersCodegenEnabled();
  }

  perm_pool_.reset(new MemPool(scan_node_->mem_tracker()));

  // Allocate tuple buffer to evaluate conjuncts on parquet::Statistics.
  const TupleDescriptor* min_max_tuple_desc = scan_node_->stats_tuple_desc();
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
      scan_node_->stats_conjunct_evals(), &stats_conjunct_evals_));

  for (int i = 0; i < context->filter_ctxs().size(); ++i) {
    const FilterContext* ctx = &context->filter_ctxs()[i];
    DCHECK(ctx->filter != nullptr);
    filter_ctxs_.push_back(ctx);
    const auto& expr = ctx->expr_eval->root();
    vector<SlotId> slot_ids;
    if(expr.GetSlotIds(&slot_ids) == 1) {
      single_col_filter_ctxs_[slot_ids[0]].push_back(ctx);
    }
  }
  filter_stats_.resize(filter_ctxs_.size());
  for (int i = 0; i < filter_stats_.size(); ++i) {
    filter_stats_[i].enabled_for_rowgroup = true;
    filter_stats_[i].enabled_for_page = true;
    filter_stats_[i].enabled_for_row = true;
  }

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
  InitSlotIdsForConjuncts();
  COUNTER_SET(num_cols_counter_,
      static_cast<int64_t>(CountScalarColumns(column_readers_)));
  // Set top-level template tuple.
  template_tuple_ = template_tuple_map_[scan_node_->tuple_desc()];

  RETURN_IF_ERROR(InitDictFilterStructures());
  if (state_->query_options().parquet_bloom_filtering) {
    RETURN_IF_ERROR(CreateColIdx2EqConjunctMap());
  }
  DivideFilterAndNonFilterColumnReaders(column_readers_, &filter_readers_,
      &non_filter_readers_);
  return Status::OK();
}

// Currently, Collection Readers and scalar readers upon collection values
// are not supported for late materialization.
static bool DoesNotSupportLateMaterialization(ParquetColumnReader* column_reader) {
  return column_reader->IsCollectionReader() || column_reader->max_rep_level() > 0;
}

void HdfsParquetScanner::DivideFilterAndNonFilterColumnReaders(
    const vector<ParquetColumnReader*>& column_readers,
    vector<ParquetColumnReader*>* filter_readers,
    vector<ParquetColumnReader*>* non_filter_readers) const {
  filter_readers->clear();
  non_filter_readers->clear();
  for (auto column_reader : column_readers) {
    auto slot_desc = column_reader->slot_desc();
    if (DoesNotSupportLateMaterialization(column_reader) || (slot_desc != nullptr &&
        std::find(conjunct_slot_ids_.begin(), conjunct_slot_ids_.end(), slot_desc->id())
            != conjunct_slot_ids_.end())) {
      filter_readers->push_back(column_reader);
    } else {
      non_filter_readers->push_back(column_reader);
    }
  }
}

void HdfsParquetScanner::InitSlotIdsForConjuncts() {
  conjunct_slot_ids_.reserve(scan_node_->conjuncts().size() +
    scan_node_->filter_exprs().size());
  vector<ScalarExpr*> conjuncts;
  conjuncts.reserve(scan_node_->conjuncts().size() +
    scan_node_->filter_exprs().size());
  conjuncts.insert(std::end(conjuncts), std::begin(scan_node_->conjuncts()),
      std::end(scan_node_->conjuncts()));
  conjuncts.insert(std::end(conjuncts), std::begin(scan_node_->filter_exprs()),
      std::end(scan_node_->filter_exprs()));
  for (int conjunct_idx = 0; conjunct_idx < conjuncts.size(); ++conjunct_idx) {
    conjuncts[conjunct_idx]->GetSlotIds(&conjunct_slot_ids_);
  }
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
    if (reader->IsComplexReader()) {
      ComplexColumnReader* complex_reader = static_cast<ComplexColumnReader*>(reader);
      for (ParquetColumnReader* r: *complex_reader->children()) readers.push(r);
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

  ScalarExprEvaluator::Close(stats_conjunct_evals_, state_);

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
  DCHECK(!column_readers.empty() || scan_node_->optimize_count_star());
  int num_columns = 0;
  stack<ParquetColumnReader*> readers;
  for (ParquetColumnReader* r: column_readers_) readers.push(r);
  while (!readers.empty()) {
    ParquetColumnReader* col_reader = readers.top();
    readers.pop();
    if (col_reader->IsComplexReader()) {
      ComplexColumnReader* complex_reader =
          static_cast<ComplexColumnReader*>(col_reader);
      for (ParquetColumnReader* r: *complex_reader->children()) readers.push(r);
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
  if (scan_node_->optimize_count_star()) {
    // This is an optimized count(*) case.
    // Populate the single slot with the Parquet num rows statistic.
    DCHECK(is_footer_scanner_);
    int64_t tuple_buf_size;
    uint8_t* tuple_buf;
    int capacity = 1;
    RETURN_IF_ERROR(
        RowBatch::ResizeAndAllocateTupleBuffer(state_, row_batch->tuple_data_pool(),
            row_batch->row_desc()->GetRowSize(), &capacity, &tuple_buf_size, &tuple_buf));
    if (file_metadata_.num_rows > 0) {
      COUNTER_ADD(num_file_metadata_read_, 1);
      Tuple* dst_tuple = reinterpret_cast<Tuple*>(tuple_buf);
      TupleRow* dst_row = row_batch->GetRow(row_batch->AddRow());
      InitTuple(template_tuple_, dst_tuple);
      int64_t* dst_slot = dst_tuple->GetBigIntSlot(scan_node_->count_star_slot_offset());
      *dst_slot = 0;
      for (const auto &row_group : file_metadata_.row_groups) {
        *dst_slot += row_group.num_rows;
      }
      dst_row->SetTuple(0, dst_tuple);
      row_batch->CommitLastRow();
      row_group_rows_read_ += *dst_slot;
    }
    eos_ = true;
    return Status::OK();
  } else if (scan_node_->IsZeroSlotTableScan()) {
    DCHECK(is_footer_scanner_);
    // There are no materialized slots and we are not optimizing count(*), e.g.
    // "select 1 from alltypes". We can serve this query from just the file metadata.
    // We don't need to read the column data.
    if (row_group_rows_read_ == file_metadata_.num_rows) {
      eos_ = true;
      return Status::OK();
    }
    COUNTER_ADD(num_file_metadata_read_, 1);
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
  if (!scan_node_->hdfs_table()->IsIcebergTable()) {
    if (!scan_node_->PartitionPassesFilters(context_->partition_descriptor()->id(),
        FilterStats::ROW_GROUPS_KEY, context_->filter_ctxs())) {
      eos_ = true;
      DCHECK(parse_status_.ok());
      return Status::OK();
    }
  }
  assemble_rows_timer_.Start();
  Status status;
  if (filter_pages_) {
    status = AssembleRows<true>(row_batch, &advance_row_group_);
  } else {
    status = AssembleRows<false>(row_batch, &advance_row_group_);
  }
  assemble_rows_timer_.Stop();
  RETURN_IF_ERROR(status);
  if (!parse_status_.ok()) {
    RETURN_IF_ERROR(state_->LogOrReturnError(parse_status_.msg()));
    parse_status_ = Status::OK();
  }

  return Status::OK();
}

Status HdfsParquetScanner::ResolveSchemaForStatFiltering(SlotDescriptor* slot_desc,
    bool* missing_field, SchemaNode** schema_node_ptr) {
  DCHECK(missing_field);
  *missing_field = false;
  // Resolve column path.
  SchemaNode* node = nullptr;
  bool pos_field;
  RETURN_IF_ERROR(schema_resolver_->ResolvePath(
      slot_desc->col_path(), &node, &pos_field, missing_field));

  if (pos_field) {
    // The planner should not send predicates with 'pos' for stats filtering to the BE.
    // In case there is a bug, we return an error, which will abort the query.
    stringstream err;
    err << "Statistics not supported for pos fields: " << slot_desc->DebugString();
    DCHECK(false) << err.str();
    return Status(err.str());
  }

  if (schema_node_ptr) *schema_node_ptr = node;

  return Status::OK();
}

ColumnStatsReader HdfsParquetScanner::CreateStatsReader(
    const parquet::FileMetaData& file_metadata, const parquet::RowGroup& row_group,
    SchemaNode* node, const ColumnType& col_type) {
  DCHECK(node);

  int col_idx = node->col_idx;
  DCHECK_LT(col_idx, row_group.columns.size());

  const vector<parquet::ColumnOrder>& col_orders = file_metadata.column_orders;
  const parquet::ColumnOrder* col_order =
      col_idx < col_orders.size() ? &col_orders[col_idx] : nullptr;

  const parquet::ColumnChunk& col_chunk = row_group.columns[col_idx];

  DCHECK(node->element != nullptr);

  ColumnStatsReader stat_reader =
      CreateColumnStatsReader(col_chunk, col_type, col_order, *node->element);

  return stat_reader;
}

Status HdfsParquetScanner::EvaluateStatsConjuncts(
    const parquet::FileMetaData& file_metadata, const parquet::RowGroup& row_group,
    bool* skip_row_group) {
  *skip_row_group = false;

  if (!state_->query_options().parquet_read_statistics) return Status::OK();

  const TupleDescriptor* min_max_tuple_desc = scan_node_->stats_tuple_desc();
  if (!min_max_tuple_desc) return Status::OK();

  int64_t tuple_size = min_max_tuple_desc->byte_size();

  DCHECK(min_max_tuple_ != nullptr);
  min_max_tuple_->Init(tuple_size);

  DCHECK_GE(min_max_tuple_desc->slots().size(), stats_conjunct_evals_.size());
  for (int i = 0; i < stats_conjunct_evals_.size(); ++i) {

    SlotDescriptor* slot_desc = min_max_tuple_desc->slots()[i];
    ScalarExprEvaluator* eval = stats_conjunct_evals_[i];
    const string& fn_name = eval->root().function_name();
    if (!IsSupportedStatsConjunct(fn_name)) continue;
    bool missing_field = false;
    SchemaNode* node = nullptr;

    RETURN_IF_ERROR(ResolveSchemaForStatFiltering(slot_desc, &missing_field, &node));

    if (missing_field) {
      if (!file_metadata_utils_.NeedDataInFile(slot_desc)) continue;
      // We are selecting a column that is not in the file. We would set its slot to NULL
      // during the scan, so any predicate would evaluate to false. Return early. NULL
      // comparisons cannot happen here, since predicates with NULL literals are filtered
      // in the frontend.
      *skip_row_group = true;
      break;
    }

    ColumnStatsReader stats_reader =
        CreateStatsReader(file_metadata, row_group, node, slot_desc->type());

    bool all_nulls = false;
    if (stats_reader.AllNulls(&all_nulls) && all_nulls) {
      *skip_row_group = true;
      break;
    }

    ColumnStatsReader::StatsField stats_field;
    if (!ColumnStatsReader::GetRequiredStatsField(fn_name, &stats_field)) continue;

    void* slot = min_max_tuple_->GetSlot(slot_desc->tuple_offset());
    bool stats_read = stats_reader.ReadFromThrift(stats_field, slot);

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

bool HdfsParquetScanner::FilterAlreadyDisabledOrOverlapWithColumnStats(
    int filter_id, MinMaxFilter* minmax_filter, int idx, float threshold) {
  const TRuntimeFilterDesc& filter_desc = filter_ctxs_[idx]->filter->filter_desc();
  const TRuntimeFilterTargetDesc& target_desc = filter_desc.targets[0];

  /// If the filter is always true, not enabled for row group, or covers too much area
  /// with respect to column min and max stats, disable the use of the filter at all
  /// levels and proceed to next predicate.
  bool filterAlwaysTrue = false;
  bool columnStatsRejected = false;
  if ((filterAlwaysTrue = minmax_filter->AlwaysTrue())
      || !filter_stats_[idx].enabled_for_rowgroup
      || (columnStatsRejected = FilterContext::ShouldRejectFilterBasedOnColumnStats(
              target_desc, minmax_filter, threshold))) {
    filter_stats_[idx].enabled_for_rowgroup = false;
    filter_stats_[idx].enabled_for_row = false;
    filter_stats_[idx].enabled_for_page = false;
    filter_ctxs_[idx]->stats->IncrCounters(FilterStats::ROW_GROUPS_KEY, 1, 1, 0);
    VLOG(3) << "A filter is determined to be not useful:"
            << " fid=" << filter_id
            << ", enabled_for_rowgroup=" << (bool)filter_stats_[idx].enabled_for_rowgroup
            << ", enabled_for_page=" << (bool)filter_stats_[idx].enabled_for_page
            << ", enabled_for_row=" << (bool)filter_stats_[idx].enabled_for_row
            << ", content=" << minmax_filter->DebugString()
            << ", target column stats: low=" << PrintTColumnValue(target_desc.low_value)
            << ", high=" << PrintTColumnValue(target_desc.high_value)
            << ", threshold=" << threshold;
    return true;
  }
  return false;
}

Status HdfsParquetScanner::EvaluateOverlapForRowGroup(
    const parquet::FileMetaData& file_metadata, const parquet::RowGroup& row_group,
    bool* skip_row_group) {
  *skip_row_group = false;

  if (!state_->query_options().parquet_read_statistics) return Status::OK();

  const TupleDescriptor* stats_tuple_desc = scan_node_->stats_tuple_desc();
  if (GetOverlapPredicateDescs().size() > 0 && !stats_tuple_desc) {
    stringstream err;
    err << "stats_tuple_desc is null.";
    DCHECK(false) << err.str();
    return Status(err.str());
  }

  DCHECK(min_max_tuple_ != nullptr);
  min_max_tuple_->Init(stats_tuple_desc->byte_size());

  // The total number slots in min_max_tuple_ should be equal to or larger than
  // the number of min/max conjuncts. The extra number slots are for the overlap
  // predicates. # min_max_conjuncts + 2 * # overlap predicates = # min_max_slots.
  DCHECK_GE(stats_tuple_desc->slots().size(), stats_conjunct_evals_.size());

  TMinmaxFilteringLevel::type level = state_->query_options().minmax_filtering_level;
  float threshold = (float)(state_->query_options().minmax_filter_threshold);
  bool row_group_skipped_by_unuseful_filters = false;

  for (auto desc: GetOverlapPredicateDescs()) {
    int filter_id = desc.filter_id;
    int slot_idx = desc.slot_index;
    /// Find the index of the filter that is common in data structure
    /// filter_ctxs_ and filter_stats_.
    int idx = FindFilterIndex(filter_id);

    const RuntimeFilter* filter = GetFilter(idx);
    // We skip row group filtering if the column is not present in the data files.
    if (!filter->IsColumnInDataFile(GetScanNodeId())) continue;

    MinMaxFilter* minmax_filter = GetMinMaxFilter(filter);

    VLOG(3) << "Try to filter out a rowgroup via overlap predicate filter: "
            << " fid=" << filter_id
            << " threshold=" << threshold
            << ", Current time since reboot(ms)=" << MonotonicMillis();

    if (!minmax_filter) {
      // The filter is not available yet.
      filter_ctxs_[idx]->stats->IncrCounters(FilterStats::ROW_GROUPS_KEY, 1, 0, 0);
      continue;
    }

    if (HdfsParquetScanner::FilterAlreadyDisabledOrOverlapWithColumnStats(
            filter_id, minmax_filter, idx, threshold)) {
      // The filter is already disabled or too close to the column min/max stats, ignore
      // it.
      row_group_skipped_by_unuseful_filters = true;
      VLOG(3) << "The filter is available but either disabled or overlapping too much "
                 "with column stats: "
              << " fid=" << filter_id;
      continue;
    }

    SlotDescriptor* slot_desc = stats_tuple_desc->slots()[slot_idx];

    bool missing_field = false;
    SchemaNode* node = nullptr;

    RETURN_IF_ERROR(ResolveSchemaForStatFiltering(slot_desc, &missing_field, &node));

    if (missing_field) {
      if (!file_metadata_utils_.NeedDataInFile(slot_desc)) continue;
      // We are selecting a column that is not in the file. We would set its slot to NULL
      // during the scan, so any predicate would evaluate to false. Return early. NULL
      // comparisons cannot happen here, since predicates with NULL literals are filtered
      // in the frontend.
      *skip_row_group = true;
      break;
    }
    ColumnStatsReader stats_reader =
        CreateStatsReader(file_metadata, row_group, node, slot_desc->type());

    bool all_nulls = false;
    if (stats_reader.AllNulls(&all_nulls) && all_nulls) {
      *skip_row_group = true;
      break;
    }

    void* min_slot = nullptr;
    void* max_slot = nullptr;
    GetMinMaxSlotsForOverlapPred(slot_idx, &min_slot, &max_slot);

    bool value_read = stats_reader.ReadMinMaxFromThrift(min_slot, max_slot);
    if (!value_read) {
      continue;
    }

    const ColumnType& col_type = slot_desc->type();

    float overlap_ratio =
        minmax_filter->ComputeOverlapRatio(col_type, min_slot, max_slot);

    /// Compute the likelyhood of this filter being effective. Threshold 'threshold' is
    /// the upper bound for the overlapping ratio. Any filter with an overlap ratio
    /// (>0.0) less than 'threshold' will undertake overlap check at the page and
    /// the row level when the filtering level control allows.
    bool worthiness = !(minmax_filter->AlwaysTrue()) && (overlap_ratio < threshold);

    VLOG(3) << "The filter is available: "
            << "fid=" << filter_id
            << ", SchemaNode=" << node->DebugString()
            << ", columnType=" << col_type.DebugString()
            << ", overlap ratio=" << overlap_ratio
            << ", threshold=" << threshold
            << ", worthiness=" << worthiness
            << ", level=" << level
            << ", enabled for page=" <<
              ((level != TMinmaxFilteringLevel::ROW_GROUP) && worthiness)
            << ", enabled for row=" <<
              ((level == TMinmaxFilteringLevel::ROW) && worthiness)
            << ", data min="
            << RawValue::PrintValue(min_slot, col_type, col_type.scale)
            << ", data max="
            << RawValue::PrintValue(max_slot, col_type, col_type.scale)
            << ", content=" << minmax_filter->DebugString();

    /// If not overlapping with this particular filter, the row group can be filtered
    /// out safely.
    if (overlap_ratio == 0.0) {
      VLOG(3) << "The rowgroup was filtered out.";
      *skip_row_group = true;

      /// Update the row groups stats: rejected.
      filter_ctxs_[idx]->stats->IncrCounters(FilterStats::ROW_GROUPS_KEY, 1, 1, 1);
      break;
    }

    /// Update two fields in the entry at index 'idx' in local filter stats so that
    /// the info can be used when checking out the pages in the group in
    /// FindSkipRangesForPagesWithMinMaxFilters() and rows in these pages in
    /// HdfsScanner::EvalRuntimeFilter(). The worthiness flag is ANDed into the enabled
    /// field so that the state in enabled will never go back to 1 once set to 0.
    /// The filtering level control is considered too.
    filter_stats_[idx].enabled_for_row &=
        (level == TMinmaxFilteringLevel::ROW) && worthiness;
    filter_stats_[idx].enabled_for_page &=
        (level != TMinmaxFilteringLevel::ROW_GROUP) && worthiness;

    /// Update the row groups stats: no rejection.
    filter_ctxs_[idx]->stats->IncrCounters(FilterStats::ROW_GROUPS_KEY, 1, 1, 0);
  }

  if (row_group_skipped_by_unuseful_filters) {
    COUNTER_ADD(num_rowgroups_skipped_by_unuseful_filters_counter_, 1);
  }

  return Status::OK();
}

bool HdfsParquetScanner::ShouldProcessPageIndex() {
  if (!state_->query_options().parquet_read_page_index) return false;
  if (!stats_conjunct_evals_.empty()) return true;
  for (auto desc : GetOverlapPredicateDescs()) {
    if (IsFilterWorthyForOverlapCheck(FindFilterIndex(desc.filter_id))) {
      return true;
    }
  }
  return false;
}

Status HdfsParquetScanner::NextRowGroup() {
  const ScanRange* split_range = static_cast<ScanRangeMetadata*>(
      metadata_range_->meta_data())->original_split;
  int64_t split_offset = split_range->offset();
  int64_t split_length = split_range->len();

  const HdfsFileDesc* file_desc =
      scan_node_->GetFileDesc(context_->partition_descriptor()->id(), filename());

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

    // Let's find the index of the first row in this row group. It's needed to track the
    // file position of each row.
    int64_t row_group_first_row = 0;
    for (int i = 0; i < row_group_idx_; ++i) {
      const parquet::RowGroup& row_group = file_metadata_.row_groups[i];
      row_group_first_row += row_group.num_rows;
    }

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

    // Evaluate row group statistics with stats conjuncts.
    bool skip_row_group_on_stats;
    RETURN_IF_ERROR(
        EvaluateStatsConjuncts(file_metadata_, row_group, &skip_row_group_on_stats));
    if (skip_row_group_on_stats) {
      COUNTER_ADD(num_stats_filtered_row_groups_counter_, 1);
      continue;
    }

    // Evaluate row group statistics with min/max filters.
    bool skip_row_group_on_minmax;
    RETURN_IF_ERROR(
      EvaluateOverlapForRowGroup(file_metadata_, row_group, &skip_row_group_on_minmax));
    if (skip_row_group_on_minmax) {
      COUNTER_ADD(num_minmax_filtered_row_groups_counter_, 1);
      continue;
    }

    // Evaluate page index with min-max conjuncts and/or min/max overlap predicates.
    if (ShouldProcessPageIndex()) {
      Status page_index_status = ProcessPageIndex();
      if (!page_index_status.ok()) {
        RETURN_IF_ERROR(state_->LogOrReturnError(page_index_status.msg()));
      }
      if (filter_pages_ && candidate_ranges_.empty()) {
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

    if (state_->query_options().parquet_bloom_filtering) {
      bool skip_row_group_on_bloom_filters;
      Status bloom_filter_status = ProcessBloomFilter(row_group,
          &skip_row_group_on_bloom_filters);
      if (!bloom_filter_status.ok()) {
        RETURN_IF_ERROR(state_->LogOrReturnError(bloom_filter_status.msg()));
      }
      if (skip_row_group_on_bloom_filters) {
        COUNTER_ADD(num_bloom_filtered_row_groups_counter_, 1);
        continue;
      }
    }

    InitComplexColumns();
    RETURN_IF_ERROR(InitScalarColumns(row_group_first_row));

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

bool HdfsParquetScanner::ReadStatFromIndex(const ColumnStatsReader& stats_reader,
    const parquet::ColumnIndex& column_index, int page_idx, bool* is_null_page,
    void* min_slot, void* max_slot) {
  *is_null_page = column_index.null_pages[page_idx];
  if (*is_null_page) return false;
  return (
      stats_reader.ReadFromString(ColumnStatsReader::StatsField::MIN,
          column_index.min_values[page_idx], &column_index.max_values[page_idx], min_slot)
      && stats_reader.ReadFromString(ColumnStatsReader::StatsField::MAX,
          column_index.max_values[page_idx], &column_index.min_values[page_idx],
          max_slot));
}

Status HdfsParquetScanner::AddToSkipRanges(void* min_slot, void* max_slot,
    parquet::RowGroup& row_group, int page_idx, const ColumnType& col_type, int col_idx,
    const parquet::ColumnChunk& col_chunk, vector<RowRange>* skip_ranges,
    int* filtered_pages) {
  VLOG(3) << "Page " << page_idx << " was filtered out."
          << "data min=" << RawValue::PrintValue(min_slot, col_type, col_type.scale)
          << ", data max=" << RawValue::PrintValue(max_slot, col_type, col_type.scale);

  BaseScalarColumnReader* scalar_reader = scalar_reader_map_[col_idx];
  parquet::OffsetIndex& offset_index = scalar_reader->offset_index_;
  if (UNLIKELY(offset_index.page_locations.empty())) {
    RETURN_IF_ERROR(page_index_.DeserializeOffsetIndex(col_chunk, &offset_index));
  }

  RowRange row_range;
  GetRowRangeForPage(row_group, offset_index, page_idx, &row_range);
  skip_ranges->push_back(row_range);
  (*filtered_pages)++;

  return Status::OK();
}

/// Define a function pointer that compares less for two std::string objects.
typedef bool (*CompareFunc)(const std::string& v1, const std::string& v2);

/// Return a lambda expression that compares less for two string objects holding raw
/// values in Impala internal format. Examples of raw values include RawValue,
/// StringValue, TimestampValue or DateValue.
template <typename T>
inline CompareFunc GetCompareLessFunc() {
  return [](const string& v1, const string& v2) {
    DCHECK_EQ(v1.length(), sizeof(T));
    DCHECK_EQ(v2.length(), sizeof(T));
    return (*reinterpret_cast<const T*>(v1.data()))
        < (*reinterpret_cast<const T*>(v2.data()));
  };
}

/// Return a function pointer of type 'CompareFunc', based on type 'col_type'.
CompareFunc GetCompareLessFunc(const ColumnType& col_type) {
  switch (col_type.type) {
    case TYPE_NULL:
      return nullptr;
    case TYPE_BOOLEAN:
      return GetCompareLessFunc<bool>();
    case TYPE_TINYINT:
      return GetCompareLessFunc<int8_t>();
    case TYPE_SMALLINT:
      return GetCompareLessFunc<int16_t>();
    case TYPE_INT:
      return GetCompareLessFunc<int32_t>();
    case TYPE_BIGINT:
      return GetCompareLessFunc<int64_t>();
    case TYPE_FLOAT:
      return GetCompareLessFunc<float>();
    case TYPE_DOUBLE:
      return GetCompareLessFunc<double>();
    case TYPE_DATE:
      return GetCompareLessFunc<DateValue>();
    case TYPE_CHAR:
      return nullptr;
    case TYPE_STRING:
    case TYPE_VARCHAR:
      return GetCompareLessFunc<StringValue>();
    case TYPE_TIMESTAMP:
      return GetCompareLessFunc<TimestampValue>();
    case TYPE_DECIMAL:
      switch (col_type.GetByteSize()) {
        case 4:
          return GetCompareLessFunc<Decimal4Value>();
        case 8:
          return GetCompareLessFunc<Decimal8Value>();
        case 16:
          return GetCompareLessFunc<Decimal16Value>();
        default:
          DCHECK(false) << "Unknown decimal byte size: " << col_type.GetByteSize();
          return nullptr;
      }
    default:
      DCHECK(false) << "Invalid type: " << col_type.DebugString();
      break;
  }
  return nullptr;
}

Status HdfsParquetScanner::ConvertStatsIntoInternalValuesBatch(
    const ColumnStatsReader& stats_reader, const parquet::ColumnIndex& column_index,
    int start_page_idx, int end_page_idx, const ColumnType& col_type,
    uint8_t** min_values, uint8_t** max_values) {
  DCHECK_GE(end_page_idx, start_page_idx);
  int slot_size = col_type.GetSlotSize();
  int64_t total_size = (end_page_idx - start_page_idx + 1) * slot_size;

  uint8_t* min_slots = stats_batch_read_pool_->TryAllocate(total_size);
  if (!min_slots) {
    return stats_batch_read_pool_->mem_tracker()->MemLimitExceeded(state_,
        "Fail to allocate memory to hold a batch of min column stats", total_size);
  }
  if (!stats_reader.ReadFromStringsBatch(ColumnStatsReader::StatsField::MIN,
          column_index.min_values, start_page_idx, end_page_idx, col_type.len,
          min_slots)) {
    return Status("Fail to read from min stats in column index in batch");
  }

  uint8_t* max_slots = stats_batch_read_pool_->TryAllocate(total_size);
  if (!max_slots) {
    return stats_batch_read_pool_->mem_tracker()->MemLimitExceeded(state_,
        "Fail to allocate memory to hold a batch of max column stats", total_size);
  }
  if (!stats_reader.ReadFromStringsBatch(ColumnStatsReader::StatsField::MAX,
          column_index.max_values, start_page_idx, end_page_idx, col_type.len,
          max_slots)) {
    return Status("Fail to read from max stats in column index in batch");
  }

  *min_values = min_slots;
  *max_values = max_slots;
  return Status::OK();
}

Status HdfsParquetScanner::SkipPagesBatch(parquet::RowGroup& row_group,
    const ColumnStatsReader& stats_reader, const parquet::ColumnIndex& column_index,
    int start_page_idx, int end_page_idx, const ColumnType& col_type, int col_idx,
    const parquet::ColumnChunk& col_chunk, const MinMaxFilter* minmax_filter,
    vector<RowRange>* skip_ranges, int* filtered_pages) {
  BaseScalarColumnReader* scalar_reader = scalar_reader_map_[col_idx];
  parquet::OffsetIndex& offset_index = scalar_reader->offset_index_;
  if (UNLIKELY(offset_index.page_locations.empty())) {
    RETURN_IF_ERROR(page_index_.DeserializeOffsetIndex(col_chunk, &offset_index));
  }

  int slot_size = col_type.GetSlotSize();

  VLOG(3) << "SkiPagesBatch() called: "
          << " start_page_idx=" << start_page_idx
          << ", end_page_idx=" << end_page_idx
          << ", #pages=" << (end_page_idx - start_page_idx + 1)
          << ", slot_size=" << slot_size;

  bool validate_fast_code_path = false;

  // Go through a fast code path to collect the skipped pages via binary search, provided
  // that the fast code path mode is on, min/max values are sorted, and a compare less
  // function is defined for 'col_type'.
  TMinmaxFilterFastCodePathMode::type fast_code_path_mode =
      state_->query_options().minmax_filter_fast_code_path;
  // A vector of booleans used only for verification.
  std::vector<bool> pageIndicesFromFastSearch(end_page_idx + 1, false);
  if (fast_code_path_mode != TMinmaxFilterFastCodePathMode::OFF
      && column_index.boundary_order == parquet::BoundaryOrder::ASCENDING
      && col_type.type == minmax_filter->type() && GetCompareLessFunc(col_type)) {
    validate_fast_code_path =
        fast_code_path_mode == TMinmaxFilterFastCodePathMode::VERIFICATION;

    vector<PageRange> skipped_page_ranges;
    if (ParquetPlainEncoder::IsIdenticalToParquetStorageType(col_type)) {
      // For those Impala types that are identical to the Parquet storage type, do
      // the binary search directly on the min/max values.
      const vector<string>& min_vals = column_index.min_values;
      const vector<string>& max_vals = column_index.max_values;
      HdfsParquetScanner::CollectSkippedPageRangesForSortedColumn(minmax_filter, col_type,
          min_vals, max_vals, start_page_idx, end_page_idx, &skipped_page_ranges);
    } else {
      // Otherwise, convert the min/max values into internal format first which validate
      // the stats and then do the binary search.
      uint8_t* min_slots = nullptr;
      uint8_t* max_slots = nullptr;
      RETURN_IF_ERROR(ConvertStatsIntoInternalValuesBatch(stats_reader, column_index,
          start_page_idx, end_page_idx, col_type, &min_slots, &max_slots));

      // Prepare two string vectors that hold the converted min and max values.
      // First allocate a total of 'start_page_idx' strings in each vectors which do not
      // participate in the binary search. Then reserve a total of 'end_page_idx' + 1
      // strings. Lastly, copy a total of 'sz' converted stats objects into the two
      // vectors as strings.
      vector<string> min_vals(start_page_idx);
      vector<string> max_vals(start_page_idx);
      min_vals.reserve(end_page_idx+1);
      max_vals.reserve(end_page_idx+1);

      uint8_t* min_slot = min_slots;
      uint8_t* max_slot = max_slots;
      int64_t sz = end_page_idx - start_page_idx + 1;
      for (int i = 0; i < sz; i++) {
        min_vals.emplace_back(string(reinterpret_cast<char*>(min_slot), slot_size));
        max_vals.emplace_back(string(reinterpret_cast<char*>(max_slot), slot_size));
        min_slot += slot_size;
        max_slot += slot_size;
      }
      // The two string vectors are ready. Perform the binary search.
      HdfsParquetScanner::CollectSkippedPageRangesForSortedColumn(minmax_filter, col_type,
          min_vals, max_vals, start_page_idx, end_page_idx, &skipped_page_ranges);
      stats_batch_read_pool_->FreeAll();
    }
    if (UNLIKELY(validate_fast_code_path)) {
      for (const auto& page_range : skipped_page_ranges) {
        page_range.convertToIndices(&pageIndicesFromFastSearch);
      }
      skip_ranges->clear();
    } else {
      for (const auto& page_range : skipped_page_ranges) {
        RowRange row_range;
        GetRowRangeForPageRange(
            row_group, scalar_reader->offset_index_, page_range, &row_range);
        skip_ranges->push_back(row_range);
        *filtered_pages += page_range.last - page_range.first + 1;
        VLOG(3) << "Filtered out page range=[" << page_range.first << ", "
                << page_range.last << "]"
                << ", equivalent row range=[" << row_range.first << ", " << row_range.last
                << "]";
      }
      return Status::OK();
    }
  }

  // Go through the regular code path to collect the skipped pages. Linearly go over
  // each page in the entire page range ([start_page_idx, end_page_idx]) which are
  // collected with batch read.
  uint8_t* min_slots = nullptr;
  uint8_t* max_slots = nullptr;
  RETURN_IF_ERROR(ConvertStatsIntoInternalValuesBatch(stats_reader,
      column_index, start_page_idx, end_page_idx, col_type, &min_slots, &max_slots));

  uint8_t* min_slot = min_slots;
  uint8_t* max_slot = max_slots;
  for (int i = start_page_idx; i <= end_page_idx; i++) {
    VLOG(3) << "stats for page[" << i << "]: "
            << "min=" << RawValue::PrintValue(min_slot, col_type, col_type.scale)
            << ", max=" << RawValue::PrintValue(max_slot, col_type, col_type.scale);
    if (!minmax_filter->EvalOverlap(col_type, min_slot, max_slot)) {
      RETURN_IF_ERROR(AddToSkipRanges(min_slot, max_slot, row_group, i, col_type,
          col_idx, col_chunk, skip_ranges, filtered_pages));

      if (UNLIKELY(validate_fast_code_path)) {
        if (!pageIndicesFromFastSearch[i]) {
          return Status(
              Substitute("Fast code path does not find the skipped page $0 found in the "
                         "normal code path. The workaround is to turn it off via set "
                         "minmax_filter_fast_code_path=false", i));
        } else {
          pageIndicesFromFastSearch[i] = false;
        }
      }
    }

    min_slot += slot_size;
    max_slot += slot_size;
  }

  if (UNLIKELY(validate_fast_code_path)) {
    for (int i = 0; i < pageIndicesFromFastSearch.size(); i++) {
      if (pageIndicesFromFastSearch[i]) {
        return Status(
            Substitute("Fast code path finds the skipped page $0 not found in the "
                       "normal code path. The workaround is to turn it off via set "
                       "minmax_filter_fast_code_path=false", i));
      }
    }
  }

  stats_batch_read_pool_->FreeAll();
  return Status::OK();
}

void HdfsParquetScanner::ResetPageFiltering() {
  filter_pages_ = false;
  candidate_ranges_.clear();
  for (auto& scalar_reader : scalar_readers_) scalar_reader->ResetPageFiltering();
}

Status HdfsParquetScanner::ProcessPageIndex() {
  MonotonicStopWatch single_process_page_index_timer;
  single_process_page_index_timer.Start();
  ResetPageFiltering();
  RETURN_IF_ERROR(page_index_.ReadAll(row_group_idx_));
  if (page_index_.IsEmpty()) return Status::OK();
  // We can release the raw page index buffer when we exit this function.
  const auto scope_exit = MakeScopeExitTrigger([this](){page_index_.Release();});
  RETURN_IF_ERROR(EvaluatePageIndex());
  RETURN_IF_ERROR(ComputeCandidatePagesForColumns());
  single_process_page_index_timer.Stop();
  process_page_index_stats_->UpdateCounter(single_process_page_index_timer.ElapsedTime());
  return Status::OK();
}

const vector<TOverlapPredicateDesc>& HdfsParquetScanner::GetOverlapPredicateDescs() {
  const HdfsScanPlanNode& pnode =
      static_cast<const HdfsScanPlanNode&>(scan_node_->plan_node());
  return pnode.overlap_predicate_descs_;
}

void HdfsParquetScanner::GetMinMaxSlotsForOverlapPred(
    int overlap_slot_idx, void** min_slot, void** max_slot) {
  DCHECK(min_slot);
  DCHECK(max_slot);
  SlotDescriptor* min_slot_desc =
      scan_node_->stats_tuple_desc()->slots()[overlap_slot_idx];
  SlotDescriptor* max_slot_desc =
      scan_node_->stats_tuple_desc()->slots()[overlap_slot_idx + 1];
  *min_slot = min_max_tuple_->GetSlot(min_slot_desc->tuple_offset());
  *max_slot = min_max_tuple_->GetSlot(max_slot_desc->tuple_offset());
}

MinMaxFilter* HdfsParquetScanner::GetMinMaxFilter(const RuntimeFilter* filter) {
  if (filter && filter->is_min_max_filter()) {
    return filter->get_min_max();
  }
  return nullptr;
}

bool HdfsParquetScanner::IsBoundByPartitionColumn(int filter_idx) {
  DCHECK_LE(0, filter_idx);
  DCHECK_LT(filter_idx,filter_ctxs_.size());
  const RuntimeFilter* filter = filter_ctxs_[filter_idx]->filter;
  return filter->IsBoundByPartitionColumn(GetScanNodeId());
}

bool HdfsParquetScanner::IsFilterWorthyForOverlapCheck(int filter_idx) {
  if (filter_idx >= 0 && filter_idx < filter_stats_.size()) {
    LocalFilterStats* stats = &filter_stats_[filter_idx];
    return stats && stats->enabled_for_page;
  }
  DCHECK(false);
  return true;
}

void HdfsParquetScanner::CollectSkippedPageRangesForSortedColumn(
    const MinMaxFilter* minmax_filter, const ColumnType& col_type,
    const vector<string>& min_vals, const vector<string>& max_vals,
    int start_page_idx, int end_page_idx,
    vector<PageRange>* skipped_ranges) {
  DCHECK(minmax_filter && col_type.type == minmax_filter->type());
  DCHECK_EQ(min_vals.size(), max_vals.size());
  DCHECK(skipped_ranges && skipped_ranges->size() == 0);
  if (start_page_idx > end_page_idx) return;
  DCHECK_LE(0, start_page_idx);
  DCHECK_LT(end_page_idx, max_vals.size());
  VLOG(3) << "Use fast code path to filter on the leading sort by column."
          << " Filter=" << minmax_filter->DebugString();
  string filter_min = string(
      reinterpret_cast<const char*>(minmax_filter->GetMin()), col_type.GetSlotSize());
  string filter_max = string(
      reinterpret_cast<const char*>(minmax_filter->GetMax()), col_type.GetSlotSize());

  CompareFunc compare_less = DCHECK_NOTNULL(GetCompareLessFunc(col_type));

  // If the max of max values is less than the min in the filter, or the min of the min
  // values is greater than the max in the filter, all pages can be skipped.
  if ((*compare_less)(max_vals[end_page_idx], filter_min)
      || (*compare_less)(filter_max, min_vals[start_page_idx])) {
    skipped_ranges->emplace_back(PageRange(start_page_idx, end_page_idx));
    return;
  }

  // The std::lower_bound() call below returns the 1st element in max_vals that
  // max_vals[i] >= filter_min, or end() if no such element exists. In the former
  // case, all pages at itor 'it' - 1 and before satisfy max_vals[i] < filter_min and
  // can be skipped. In the latter case, no conclusion can be made.
  vector<string>::const_iterator begin = max_vals.begin() + start_page_idx;
  vector<string>::const_iterator end = max_vals.begin() + end_page_idx + 1;
  auto it = std::lower_bound(begin, end, filter_min, compare_less);

  int idx = start_page_idx;
  if (it != end && it != begin) {
    idx = it - begin + start_page_idx;
    // skip from start_page_idx to idx - 1
    skipped_ranges->emplace_back(PageRange(start_page_idx, idx - 1));
  }

  // The std::upper_bound() call below returns the 1st element in min_vals such that
  // min_vals[i] > filter_max or end() if no such element is found, starting at 'idx'.
  // In the former case, all pages at itor 'it' and after satisfy min_vals[i] > filter_max
  // and can be skipped. In the latter case, no conclusion can be made.
  begin = min_vals.begin() + idx;
  end = min_vals.begin() + end_page_idx + 1;
  it = std::upper_bound(begin, end, filter_max, compare_less);

  if (it != end) {
    idx += it - begin;
    // skip from idx to end_page_idx
    skipped_ranges->emplace_back(PageRange(idx, end_page_idx));
  }

  VLOG(3) << "skipped_ranges->size()=" << skipped_ranges->size();
}

Status HdfsParquetScanner::FindSkipRangesForPagesWithMinMaxFilters(
    vector<RowRange>* skip_ranges) {
  DCHECK(skip_ranges);
  const TupleDescriptor* min_max_tuple_desc = scan_node_->stats_tuple_desc();
  if (GetOverlapPredicateDescs().size() > 0 && !min_max_tuple_desc) {
    stringstream err;
    err << "stats_tuple_desc is null.";
    DCHECK(false) << err.str();
    return Status(err.str());
  }

  min_max_tuple_->Init(min_max_tuple_desc->byte_size());
  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx_];

  int filtered_pages = 0;

  for (auto desc: GetOverlapPredicateDescs()) {
    int filter_id = desc.filter_id;
    int slot_idx = desc.slot_index;

    int filter_idx = FindFilterIndex(filter_id);
    const RuntimeFilter* filter = GetFilter(filter_idx);
    MinMaxFilter* minmax_filter = GetMinMaxFilter(filter);
    if (!minmax_filter || !IsFilterWorthyForOverlapCheck(filter_idx)) {
      continue;
    }

    // This is the desc for the min slot, from which we find the stats_reader
    // through col_path.
    SlotDescriptor* slot_desc = min_max_tuple_desc->slots()[slot_idx];

    bool missing_field = false;
    SchemaNode* node = nullptr;

    RETURN_IF_ERROR(ResolveSchemaForStatFiltering(slot_desc, &missing_field, &node));

    if (missing_field) {
      continue;
    }

    int col_idx = node->col_idx;

    if (UNLIKELY(scalar_reader_map_.find(col_idx) == scalar_reader_map_.end())) {
      // IMPALA-11147: It's possible that 'node' is also a partitioning column. In that
      // case we don't have a scalar reader for it.
      DCHECK(scan_node_->hdfs_table()->IsIcebergTable() ||
             slot_desc->col_pos() < scan_node_->num_partition_keys());
      continue;
    }

    ColumnStatsReader stats_reader =
        CreateStatsReader(file_metadata_, row_group, node, slot_desc->type());

    DCHECK_LT(col_idx, row_group.columns.size());
    const parquet::ColumnChunk& col_chunk = row_group.columns[col_idx];
    if (col_chunk.column_index_length == 0) continue;

    parquet::ColumnIndex column_index;
    RETURN_IF_ERROR(page_index_.DeserializeColumnIndex(col_chunk, &column_index));

    const ColumnType& col_type = slot_desc->type();

    const int num_of_pages = column_index.null_pages.size();
    // If there is only one page in a row group, there is no need to apply the
    // filter since the page stats must be identical to that of the containing
    // row group that has already been filtered (passed).
    if (num_of_pages == 1) break;

    VLOG(3) << "Try to filter out pages via overlap predicate."
            << "  fid=" << filter_id << ", columnType=" << col_type.DebugString()
            << ", filter=" << minmax_filter->DebugString()
            << ", #pages=" << num_of_pages;
    int first_not_null_page_idx = -1;

    // Try to detect the longest span of non-null pages and batch read min/max stats for
    // it. If a null-page is found, skip it right away and continue.
    for (int page_idx = 0; page_idx < num_of_pages; ++page_idx) {
      bool is_null_page = column_index.null_pages[page_idx];
      if (UNLIKELY(is_null_page)) {
        RETURN_IF_ERROR(AddToSkipRanges(nullptr, nullptr, row_group, page_idx, col_type,
            col_idx, col_chunk, skip_ranges, &filtered_pages));

        // Process the previous span of non-null pages, if exist. The range of the
        // span is [first_not_null_page_idx, page_idx-1]
        if (LIKELY(first_not_null_page_idx != -1)) {
          RETURN_IF_ERROR(SkipPagesBatch(row_group, stats_reader, column_index,
              first_not_null_page_idx, page_idx - 1, col_type, col_idx, col_chunk,
              minmax_filter, skip_ranges, &filtered_pages));
        }

        first_not_null_page_idx = -1;
      } else {
        // Mark the first non-null page, if not done yet.
        if (UNLIKELY(first_not_null_page_idx == -1)) {
          first_not_null_page_idx = page_idx;
        }
      }
    }

    // Prorcess the last span of non-null pages, if exist. The range of the span
    // is [first_not_null_page_idx, num_of_pages-1]
    if (LIKELY(first_not_null_page_idx != -1)) {
      RETURN_IF_ERROR(SkipPagesBatch(row_group, stats_reader, column_index,
          first_not_null_page_idx, num_of_pages - 1, col_type, col_idx, col_chunk,
          minmax_filter, skip_ranges, &filtered_pages));
    }
  }

  if (filtered_pages > 0) {
    COUNTER_ADD(num_minmax_filtered_pages_counter_, filtered_pages);
  }
  return Status::OK();
}

Status HdfsParquetScanner::EvaluatePageIndex() {
  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx_];
  vector<RowRange> skip_ranges;

  for (int i = 0; i < stats_conjunct_evals_.size(); ++i) {
    ScalarExprEvaluator* eval = stats_conjunct_evals_[i];
    const string& fn_name = eval->root().function_name();
    if (!IsSupportedStatsConjunct(fn_name)) continue;
    SlotDescriptor* slot_desc = scan_node_->stats_tuple_desc()->slots()[i];

    bool missing_field = false;
    SchemaNode* node = nullptr;

    RETURN_IF_ERROR(ResolveSchemaForStatFiltering(slot_desc, &missing_field, &node));

    if (missing_field) {
      continue;
    }
    int col_idx = node->col_idx;;
    ColumnStatsReader stats_reader =
        CreateStatsReader(file_metadata_, row_group, node, slot_desc->type());

    DCHECK_LT(col_idx, row_group.columns.size());
    const parquet::ColumnChunk& col_chunk = row_group.columns[col_idx];
    if (col_chunk.column_index_length == 0) continue;

    parquet::ColumnIndex column_index;
    RETURN_IF_ERROR(page_index_.DeserializeColumnIndex(col_chunk, &column_index));

    min_max_tuple_->Init(scan_node_->stats_tuple_desc()->byte_size());
    void* slot = min_max_tuple_->GetSlot(slot_desc->tuple_offset());

    const int num_of_pages = column_index.null_pages.size();
    ColumnStatsReader::StatsField stats_field;
    if (!ColumnStatsReader::GetRequiredStatsField(fn_name, &stats_field)) continue;

    for (int page_idx = 0; page_idx < num_of_pages; ++page_idx) {
      bool value_read, is_null_page;
      value_read = ReadStatFromIndex(
          stats_reader, column_index, page_idx, stats_field, &is_null_page, slot);
      if (!is_null_page && !value_read) continue;
      TupleRow row;
      row.SetTuple(0, min_max_tuple_);
      // Accept NULL as the predicate can contain a CAST which may fail.
      if (is_null_page || !eval->EvalPredicateAcceptNull(&row)) {
        BaseScalarColumnReader* scalar_reader = scalar_reader_map_[col_idx];
        parquet::OffsetIndex& offset_index = scalar_reader->offset_index_;
        if (offset_index.page_locations.empty()) {
          RETURN_IF_ERROR(page_index_.DeserializeOffsetIndex(col_chunk, &offset_index));
        }
        RowRange row_range;
        GetRowRangeForPage(row_group, scalar_reader->offset_index_, page_idx, &row_range);
        skip_ranges.push_back(row_range);
      }
    }
  }

  // On top of min/max conjuncts, apply min/max filters to filter out pages.
  if (state_->query_options().minmax_filtering_level
      != TMinmaxFilteringLevel::ROW_GROUP) {
    RETURN_IF_ERROR(FindSkipRangesForPagesWithMinMaxFilters(&skip_ranges));
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
    ResetPageFiltering();
    return Status(Substitute(
        "Invalid offset index in Parquet file $0. Page index filtering is disabled.",
        filename()));
  }
  filter_pages_ = true;
  return Status::OK();
}

Status HdfsParquetScanner::ComputeCandidatePagesForColumns() {
  if (candidate_ranges_.empty()) return Status::OK();

  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx_];
  for (BaseScalarColumnReader* scalar_reader : scalar_readers_) {
    const auto& page_locations = scalar_reader->offset_index_.page_locations;
    if (!ComputeCandidatePages(page_locations, candidate_ranges_, row_group.num_rows,
        &scalar_reader->candidate_data_pages_)) {
      ResetPageFiltering();
      return Status(Substitute(
          "Invalid offset index in Parquet file $0. Page index filtering is disabled.",
          filename()));
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
  auto runtime_filter_it = single_col_filter_ctxs_.find(slot_desc->id());
  if (runtime_filter_it == single_col_filter_ctxs_.end()
      && dict_filter_it == dict_filter_map_.end()) return false;

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
    if (reader->IsComplexReader()) {
      ComplexColumnReader* col_reader = static_cast<ComplexColumnReader*>(reader);
        complex_readers_.push_back(col_reader);
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
      state_->query_options().parquet_dictionary_filtering &&
      (!dict_filter_map_.empty() || !single_col_filter_ctxs_.empty());

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
      if (enc_stat.page_type == parquet::PageType::DATA_PAGE
          && !IsDictionaryEncoding(enc_stat.encoding) && enc_stat.count > 0) {
        return false;
      }
    }
  } else {
    // Condition #2 above
    bool has_dict_encoding = false;
    bool has_nondict_encoding = false;
    for (const parquet::Encoding::type& encoding : col_metadata.encodings) {
      if (IsDictionaryEncoding(encoding)) has_dict_encoding = true;

      // RLE and BIT_PACKED are used for repetition/definition levels
      if (!IsDictionaryEncoding(encoding) &&
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
    const vector<ScalarExprEvaluator*>* dict_filter_conjunct_evals =
        dict_filter_it != dict_filter_map_.end()
        ? &(dict_filter_it->second) : nullptr;

    auto runtime_filter_it = single_col_filter_ctxs_.find(slot_desc->id());
    const vector<const FilterContext*>* runtime_filters =
        runtime_filter_it != single_col_filter_ctxs_.end()
        ? &runtime_filter_it->second : nullptr;
    DCHECK(runtime_filters != nullptr || dict_filter_conjunct_evals != nullptr);

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
    bool should_eval_runtime_filter = dictionary->num_entries() <=
        state_->query_options().parquet_dictionary_runtime_filter_entry_limit;
    int runtime_filters_processed = 0;
    for (int dict_idx = 0; dict_idx < dictionary->num_entries(); ++dict_idx) {
      if (dict_idx % 1024 == 0) {
        // Don't let expr result allocations accumulate too much for large dictionaries or
        // many row groups.
        context_->expr_results_pool()->Clear();
      }
      dictionary->GetValue(dict_idx, slot);

      // If any dictionary value passes the conjuncts and runtime filters, then move on to
      // the next column.
      TupleRow row;
      row.SetTuple(0, dict_filter_tuple);
      if (dict_filter_conjunct_evals != nullptr
          && !ExecNode::EvalConjuncts(dict_filter_conjunct_evals->data(),
              dict_filter_conjunct_evals->size(), &row)) {
        continue; // Failed the conjunct check, move on to the next entry.
      }
      column_has_match = true; // match caused by conjunct evaluation
      if (runtime_filters != nullptr && should_eval_runtime_filter) {
        for (int rf_idx = 0; rf_idx < runtime_filters->size(); rf_idx++) {
          if (runtime_filters_processed <= rf_idx) runtime_filters_processed++;
          if (!runtime_filters->at(rf_idx)->Eval(&row)) {
            column_has_match = false;
            break;
          }
        }
        if (column_has_match) {
          break; // Passed the conjunct and there were no runtime filter miss.
        }
      } else {
        break; // Passed the conjunct and runtime filter does not exist.
      }
    }
    // Free all expr result allocations now that we're done with the filter.
    context_->expr_results_pool()->Clear();

    // Although, the accepted and rejected runtime filter stats can not be updated
    // meaningfully, it is possible to update the processed stat.
    if (runtime_filters != nullptr) {
      for (int rf_idx = 0; rf_idx < runtime_filters_processed; rf_idx++) {
          runtime_filters->at(rf_idx)->stats->IncrCounters(
              FilterStats::ROW_GROUPS_KEY, 1, 1, 0);
      }
    }

    // This column contains no value that matches the conjunct or runtime filter,
    // therefore this row group can be eliminated.
    if (!column_has_match) {
      *row_group_eliminated = true;
      COUNTER_ADD(num_dict_filtered_row_groups_counter_, 1);
      return Status::OK();
    }
  }

  // Any columns that were not 100% dictionary encoded need to initialize
  // their dictionaries here.
  RETURN_IF_ERROR(BaseScalarColumnReader::InitDictionaries(deferred_dict_init_list));

  return Status::OK();
}

Status HdfsParquetScanner::ReadToBuffer(uint64_t offset, uint8_t* buffer, uint64_t size) {
  DCHECK(context_ != nullptr);
  DCHECK(metadata_range_ != nullptr);
  DCHECK(scan_node_ != nullptr);

  const int64_t partition_id = context_->partition_descriptor()->id();
  const int cache_options =
      metadata_range_->cache_options() & ~BufferOpts::USE_HDFS_CACHE;
  ScanRange* object_range = scan_node_->AllocateScanRange(
      metadata_range_->GetFileInfo(), size, offset, partition_id,
      metadata_range_->disk_id(), metadata_range_->expected_local(),
      BufferOpts::ReadInto(buffer, size, cache_options));
  unique_ptr<BufferDescriptor> io_buffer;
  bool needs_buffers;
  RETURN_IF_ERROR(
      scan_node_->reader_context()->StartScanRange(object_range,
          &needs_buffers));
  DCHECK(!needs_buffers) << "Already provided a buffer";
  RETURN_IF_ERROR(object_range->GetNext(&io_buffer));
  DCHECK_EQ(io_buffer->buffer(), buffer);
  DCHECK_EQ(io_buffer->len(), size);
  DCHECK(io_buffer->eosr());
  AddSyncReadBytesCounter(io_buffer->len());
  object_range->ReturnBuffer(move(io_buffer));
  return Status::OK();
}

void LogMissingFields(google::LogSeverity log_level, const std::string& text_before,
    const std::string& text_after, const std::unordered_set<std::string>& paths) {
  stringstream s;
  s << text_before;
  s << "[";
  size_t i = 0;
  for (const std::string& path : paths) {
    s << path;
    if (i + 1 < paths.size()) {
      s << ", ";
    }
    i++;
  }

  s << "]. ";
  s << text_after;
  VLOG(log_level) << s.str();
}

// Create a map from column index to EQ conjuncts for Bloom filtering.
Status HdfsParquetScanner::CreateColIdx2EqConjunctMap() {
  // EQ conjuncts are represented as a LE and a GE conjunct with the same
  // value. This map is used to pair them to form EQ conjuncts.
  // The value is a set because there may be multiple GE or LE conjuncts on a column.
  unordered_map<int, std::unordered_set<std::pair<std::string, const Literal*>>>
      conjunct_halves;

  // Slot paths for which no data is found in the file. It is expected for example if it
  // is a partition column and unexpected for example if the column was added to the table
  // schema after the current file was written and therefore the current file does
  // not have the column.
  std::unordered_set<std::string> unexpected_missing_fields;
  std::unordered_set<std::string> expected_missing_fields;

  for (ScalarExprEvaluator* eval : stats_conjunct_evals_) {
    const ScalarExpr& expr = eval->root();
    const string& function_name = expr.function_name();

    if (function_name == "le" || function_name == "ge") {
      // If this is a LE or GE conjunct, then 'expr' is a ScalarFnCall with 2 children:
      // the first child or one of the nodes in its subtree is a SlotRef containing
      // information about which column the conjunct refers to; the second child is a
      // literal value.
      DCHECK_EQ(expr.GetNumChildren(), 2);

      const ScalarExpr* child0 = expr.GetChild(0);
      const SlotRef* child_slot_ref = dynamic_cast<const SlotRef*>(child0);

      // Sometimes the left side of the ScalarFnCall in min/max conjuncts is not directly
      // a SlotRef but can also be an implicit cast wrapping a SlotRef. The FE ensures
      // that these are the only possibilities. An example for the implicit cast case is
      // when the expression is a ScalarFnCall with name "casttostring":
      //
      // create table chars (id int, c char(4)) stored as parquet;
      // select count(*) from chars where c <= "aaaa"
      //
      // At the moment we do not support Parquet Bloom filtering for types for which this
      // is relevant, so if we encounter an implicit cast here, we can discard the
      // conjunct.
      if (child_slot_ref == nullptr) continue;

      const ScalarExpr* child1 = expr.GetChild(1);
      const Literal* child_literal = dynamic_cast<const Literal*>(child1);
      // Expression rewrites should always simplify the right side to a Literal, but if
      // expr rewrites are disabled, we can see a cast there (IMPALA-10742).
      if (child_literal == nullptr) continue;

      // Convert the slot_id of 'child_slot_ref' to a column index.
      SlotDescriptor* slot_desc = scan_node_->runtime_state()->desc_tbl().
          GetSlotDescriptor(child_slot_ref->slot_id());
      SchemaNode* node = nullptr;
      bool pos_field;
      bool missing_field;
      RETURN_IF_ERROR(schema_resolver_->ResolvePath(slot_desc->col_path(),
            &node, &pos_field, &missing_field));

      if (pos_field) {
        stringstream err;
        err << "Bloom filtering not supported for pos fields: "
            << slot_desc->DebugString();
        DCHECK(false) << err.str();
        return Status(err.str());
      }

      if (missing_field) {
        if (file_metadata_utils_.NeedDataInFile(slot_desc)) {
          // If a column is added to the schema of an existing table, the schemas of the
          // old parquet data files do not contain the new column: see IMPALA-11345. This
          // is not an error, we simply disregard this column in Bloom filtering in this
          // scanner.
          unexpected_missing_fields.emplace(
              PrintPath(*scan_node_->hdfs_table(), slot_desc->col_path()));
        } else {
          // If the data is not expected to be in the file, we disregard the conjuncts for
          // the purposes of Bloom filtering.
          expected_missing_fields.emplace(
              PrintPath(*scan_node_->hdfs_table(), slot_desc->col_path()));
        }
        continue;
      }

      DCHECK(node != nullptr);

      if (!IsParquetBloomFilterSupported(node->element->type, child_slot_ref->type())) {
        continue;
      }

      const int col_idx = node->col_idx;

      // Check if the other half of the EQ conjunct is already in conjunct_halves.
      const std::unordered_set<std::pair<string, const Literal*>>& conj_halves_for_col
          = conjunct_halves[col_idx];
      auto it = conj_halves_for_col.begin();
      for (; it != conj_halves_for_col.end(); it++) {
        const std::pair<string, const Literal*>& pair = *it;
        const bool opposite_halves = (function_name == "le" && pair.first == "ge")
            || (function_name == "ge" && pair.first == "le");
        if (opposite_halves && (*pair.second) == (*child_literal)) {
          break;
        }
      }

      const bool match_found = (it != conj_halves_for_col.end());
      if (match_found) {
        vector<uint8_t> buffer;
        uint8_t* ptr = nullptr;
        size_t len = -1;
        RETURN_IF_ERROR(LiteralToParquetType(*child_literal, eval,
              node->element->type, &buffer, &ptr, &len));
        DCHECK(len != -1);
        DCHECK(ptr != nullptr || len == 0); // For an empty string, 'ptr' is NULL.
        const uint64_t hash = ParquetBloomFilter::Hash(ptr, len);

        eq_conjunct_info_.emplace(col_idx, hash);

        conjunct_halves[col_idx].erase(it);
      } else {
        conjunct_halves[col_idx].emplace(function_name, child_literal);
      }
    }
  }

  // Log expected and unexpected missing fields.
  if (!unexpected_missing_fields.empty()) {
    LogMissingFields(google::WARNING,
        Substitute(
          "Unable to find SchemaNode for the following paths in the schema of "
          "file '$0': ",
          filename()),
        "This may be because the column may have been added to the table schema after "
        "writing this file. Disregarding conjuncts on this path for the purpose of "
        "Parquet Bloom filtering in this file.",
        unexpected_missing_fields);
  }

  if (!expected_missing_fields.empty()) {
    LogMissingFields(google::INFO,
        Substitute(
          "Data for the following paths is not expected to be present in file '$0': ",
          filename()),
        "Disregarding conjuncts on this path for the purpose of Parquet Bloom filtering "
        "in this file.",
        expected_missing_fields);
  }

  return Status::OK();
}

Status HdfsParquetScanner::ReadBloomFilterHeader(uint64_t bloom_filter_offset,
    ScopedBuffer* buffer, uint32_t* header_size,
    parquet::BloomFilterHeader* bloom_filter_header) {
  DCHECK(buffer != nullptr);
  DCHECK(header_size != nullptr);

  // Should be a power of 2 for the size check and reporting to work correctly.
  constexpr uint32_t MAX_BLOOM_FILTER_HEADER_SIZE = 1024;
  uint32_t buffer_size = 32;

  Status status;
  do {
    buffer->Release();
    if (!buffer->TryAllocate(buffer_size)) {
      return Status(Substitute("Could not allocate buffer of $0 bytes for Parquet "
            "Bloom filter header for file '$1'.", buffer_size, filename()));
    }

    RETURN_IF_ERROR(ReadToBuffer(bloom_filter_offset, buffer->buffer(), buffer->Size()));

    *header_size = buffer_size;
    status = DeserializeThriftMsg(buffer->buffer(), header_size, true,
        bloom_filter_header);

    buffer_size *= 2;
    if (buffer_size > MAX_BLOOM_FILTER_HEADER_SIZE) {
      return Status(Substitute(
            "Bloom filter header size ($0) exceeded the limit of $1 bytes.",
            buffer_size, MAX_BLOOM_FILTER_HEADER_SIZE));
    }

  } while (!status.ok());

  return Status::OK();
}

Status HdfsParquetScanner::ProcessBloomFilter(const parquet::RowGroup&
    row_group, bool* skip_row_group) {
  *skip_row_group = false;

  for (const std::pair<const int, uint64_t>& col_idx_to_hash : eq_conjunct_info_) {
    const int col_idx = col_idx_to_hash.first;
    const parquet::ColumnChunk& col_chunk = row_group.columns[col_idx];
    if (col_chunk.__isset.meta_data
        && col_chunk.meta_data.__isset.bloom_filter_offset
        && !IsDictionaryEncoded(col_chunk.meta_data)) {
      int64_t bloom_filter_offset = col_chunk.meta_data.bloom_filter_offset;

      parquet::BloomFilterHeader bloom_filter_header;
      ScopedBuffer header_buffer(scan_node_->mem_tracker());
      uint32_t header_size;
      RETURN_IF_ERROR(ReadBloomFilterHeader(bloom_filter_offset, &header_buffer,
          &header_size, &bloom_filter_header));

      RETURN_IF_ERROR(ValidateBloomFilterHeader(bloom_filter_header));

      ScopedBuffer data_buffer(scan_node_->mem_tracker());

      if (LIKELY(bloom_filter_header.numBytes > 0)) {
        if (!data_buffer.TryAllocate(bloom_filter_header.numBytes)) {
          return Status(Substitute("Could not allocate buffer of $0 bytes for Parquet "
              "Bloom filter data for file '$1'.",
              bloom_filter_header.numBytes, filename()));
        }
      }

      // Read remaining bytes of Bloom filter data. It is possible that we
      // have already read some of the Bloom filter data when trying to read
      // the header.
      const uint32_t data_already_read = header_buffer.Size() - header_size;
      memcpy(data_buffer.buffer(), header_buffer.buffer() + header_size,
          data_already_read);

      const uint32_t data_to_read = bloom_filter_header.numBytes - data_already_read;
      RETURN_IF_ERROR(ReadToBuffer(bloom_filter_offset + header_buffer.Size(),
          data_buffer.buffer() + data_already_read, data_to_read));

      // Construct ParquetBloomFilter instance.
      ParquetBloomFilter bloom_filter;
      RETURN_IF_ERROR(bloom_filter.Init(data_buffer.buffer(), data_buffer.Size(), false));

      const uint64_t hash = col_idx_to_hash.second;
      if (!bloom_filter.Find(hash)) {
        *skip_row_group = true;
        VLOG(3) << Substitute("Row group with idx $0 filtered by Parquet Bloom filter on "
            "column with idx $1 in file $2.", row_group_idx_, col_idx, filename());
        return Status::OK();
      }
    }
  }

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
Status HdfsParquetScanner::AssembleRowsWithoutLateMaterialization(
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
              "had $2 remaining values but expected $3",filename(),
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

/// High-level steps of this function:
/// 1. If late materialization is disabled or not applicable, use
///    'AssembleRowsWithoutLateMaterialization'.
/// 2. Allocate 'scratch' memory for tuples able to hold a full batch.
/// 3. Populate the slots of all scratch tuples one column reader at a time only
///    for 'filter_readers_', using the ColumnReader::Read*ValueBatch() functions.
///    These column readers are based on columns used in conjuncts and runtime filters.
/// 3. Evaluate runtime filters and conjuncts against the scratch tuples and save
///    pointer of surviving tuples in the output batch. Note that it just saves the
///    pointer to surviving tuples which are still part of 'scratch' memory.
/// 4. If no tuples survive step 3, skip materializing tuples for 'non_filter_readers'.
///    Collect the rows to be skipped and skip them later at step 5 b.
/// 5. If surviving tuples are present then:
///    a. Get the micro batches of surviving rows to be read by 'non_filter_readers_'.
///    b. Skip rows collected at step 4, if needed.
///    c. Fill 'scratch' memory by reading micro batches using 'FillScratchMicroBatches'.
///       Only the rows in micro batches are filled in 'scratch' and other rows are
///       ignored. Note, we don't need to filter the rows again as output batch already
///       has pointer to surviving tuples.
/// 6. Transfer the ownership of scratch memory to the output batch once the scratch
///    memory is exhausted.
/// 7. Repeat steps above until we are done with the row group or an error
///    occurred.
/// TODO: Since the scratch batch is populated in a column-wise fashion, it is
/// difficult to maintain a maximum memory footprint without throwing away at least
/// some work. This point needs further experimentation and thought.
template <bool USES_PAGE_INDEX>
Status HdfsParquetScanner::AssembleRows(RowBatch* row_batch, bool* skip_row_group) {
  DCHECK(!column_readers_.empty());
  DCHECK(row_batch != nullptr);
  DCHECK_EQ(*skip_row_group, false);
  DCHECK(scratch_batch_ != nullptr);

  if (filter_readers_.empty() || non_filter_readers_.empty() ||
      late_materialization_threshold_ < 0 || filter_readers_[0]->max_rep_level() > 0 ||
      HasStructColumnReader(non_filter_readers_)) {
    // Late Materialization is either disabled or not applicable for assembling rows here.
    return AssembleRowsWithoutLateMaterialization(column_readers_, row_batch,
        skip_row_group);
  }

  int64_t num_rows_read = 0;
  int64_t num_rows_to_skip = 0;
  int64_t last_row_id_processed = -1;
  while (!column_readers_[0]->RowGroupAtEnd()) {
    // Start a new scratch batch.
    RETURN_IF_ERROR(scratch_batch_->Reset(state_));
    InitTupleBuffer(template_tuple_, scratch_batch_->tuple_mem, scratch_batch_->capacity);
    // Adjust complete_micro_batch_ length to new scratch_batch_->capacity after
    // ScratchTupleBatch::Reset
    complete_micro_batch_.AdjustLength(scratch_batch_->capacity);

    // Late Materialization
    // 1. Filter rows only materializing the columns in 'filter_readers_'
    // 2. Transfer the surviving rows
    // 3. Materialize rest of the columns only for surviving rows.

    RETURN_IF_ERROR(FillScratchMicroBatches(filter_readers_, row_batch,
        skip_row_group, &complete_micro_batch_, 1, scratch_batch_->capacity,
        &scratch_batch_->num_tuples));
    if (*skip_row_group) { return Status::OK(); }
    num_rows_read += scratch_batch_->num_tuples;
    bool row_group_end = filter_readers_[0]->RowGroupAtEnd();
    int num_row_to_commit = FilterScratchBatch(row_batch);
    if (num_row_to_commit == 0) {
      // Collect the rows to skip, so that we can skip them together to avoid
      // decompression and decoding. This ensures compressed pages that don't
      // have any rows of interest are skiped without decompression.
      num_rows_to_skip += scratch_batch_->num_tuples;
      last_row_id_processed = filter_readers_[0]->LastProcessedRow();
    } else {
      if (num_rows_to_skip > 0) {
        // skip reading for rest of the non-filter column readers now.
        RETURN_IF_ERROR(SkipRowsForColumns(
            non_filter_readers_, &num_rows_to_skip, &last_row_id_processed));
      }
      int num_tuples;
      if (USES_PAGE_INDEX || !scratch_batch_->AtEnd() ||
          num_row_to_commit == scratch_batch_->num_tuples) {
        // When using Page Index, materialize the entire batch. Currently, only avoiding
        // materializing only at the granularity of entire batch is supported for page
        // indexes. Other condition is when no filtering happened.
        RETURN_IF_ERROR(FillScratchMicroBatches(non_filter_readers_, row_batch,
            skip_row_group, &complete_micro_batch_,
            1 /*'complete_micro_batch' is a single batch */, scratch_batch_->capacity,
            &num_tuples));
      } else {
        ScratchMicroBatch micro_batches[scratch_batch_->capacity];
        int num_micro_batches = scratch_batch_->GetMicroBatches(
            late_materialization_threshold_, micro_batches);
        RETURN_IF_ERROR(FillScratchMicroBatches(non_filter_readers_, row_batch,
            skip_row_group, micro_batches, num_micro_batches,
            scratch_batch_->num_tuples, &num_tuples));
      }
      if (*skip_row_group) { return Status::OK(); }
    }
    // Finalize the Transfer
    if (scratch_batch_->tuple_byte_size != 0) {
      scratch_batch_->FinalizeTupleTransfer(row_batch, num_row_to_commit);
    }
    if (row_group_end) {
      // skip reading for rest of the non-filter column readers now.
      RETURN_IF_ERROR(SkipRowsForColumns(
          non_filter_readers_, &num_rows_to_skip, &last_row_id_processed));
      for (int c = 0; c < non_filter_readers_.size(); ++c) {
        ParquetColumnReader* col_reader = non_filter_readers_[c];
        if (UNLIKELY(!col_reader->SetRowGroupAtEnd())) {
          return Status(
              Substitute("Could not move to RowGroup end in file $0.", filename()));
        }
      }
    }
    if (num_row_to_commit != 0) {
      RETURN_IF_ERROR(CheckPageFiltering());
      RETURN_IF_ERROR(CommitRows(row_batch, num_row_to_commit));
    }
    if (row_batch->AtCapacity()) {
      // skip reading for rest of the non-filter column readers now.
      RETURN_IF_ERROR(SkipRowsForColumns(
          non_filter_readers_, &num_rows_to_skip, &last_row_id_processed));
      break;
    }
  }
  row_group_rows_read_ += num_rows_read;
  COUNTER_ADD(scan_node_->rows_read_counter(), num_rows_read);
  // Merge Scanner-local counter into HdfsScanNode counter and reset.
  COUNTER_ADD(scan_node_->collection_items_read_counter(), coll_items_read_counter_);
  coll_items_read_counter_ = 0;
  return Status::OK();
}

bool HdfsParquetScanner::HasStructColumnReader(
    const std::vector<ParquetColumnReader*>& column_readers) const {
  for (const ParquetColumnReader* col_reader : column_readers) {
    if (col_reader->HasStructReader()) return true;
  }
  return false;
}

Status HdfsParquetScanner::SkipRowsForColumns(
    const vector<ParquetColumnReader*>& column_readers, int64_t* num_rows_to_skip,
    int64_t* skip_to_row) {
  if (*num_rows_to_skip > 0) {
    for (int c = 0; c < column_readers.size(); ++c) {
      ParquetColumnReader* col_reader = column_readers[c];
      // Skipping may fail for corrupted Parquet file due to mismatch of rows
      // among columns.
      if (UNLIKELY(!col_reader->SkipRows(*num_rows_to_skip, *skip_to_row))) {
        return Status(Substitute(
            "Parquet file might be corrupted: Error in skipping $0 values to row $1 "
            "in column $2 of file $3.",
            *num_rows_to_skip, *skip_to_row, col_reader->schema_element().name,
            filename()));
      }
    }
    *num_rows_to_skip = 0;
    *skip_to_row = -1;
  }
  return Status::OK();
}

Status HdfsParquetScanner::FillScratchMicroBatches(
    const vector<ParquetColumnReader*>& column_readers, RowBatch* row_batch,
    bool* skip_row_group, const ScratchMicroBatch* micro_batches, int num_micro_batches,
    int max_num_tuples, int* num_tuples) {
  if (UNLIKELY(num_micro_batches < 1)) {
    return Status(Substitute("Number of batches is $0, less than 1", num_micro_batches));
  }

  // Materialize the top-level slots into the scratch batches column-by-column.
  int last_num_tuples[num_micro_batches];
  for (int c = 0; c < column_readers.size(); ++c) {
    ParquetColumnReader* col_reader = column_readers[c];
    bool continue_execution = false;
    int last = -1;
    for (int r = 0; r < num_micro_batches; r++) {
      if (r == 0) {
        if (micro_batches[0].start > 0) {
          if (UNLIKELY(!col_reader->SkipRows(micro_batches[0].start, -1))) {
            return Status(ErrorMsg(TErrorCode::PARQUET_ROWS_SKIPPING,
                col_reader->schema_element().name, filename()));
          }
        }
      } else {
        if (UNLIKELY(!col_reader->SkipRows(micro_batches[r].start - last - 1, -1))) {
          return Status(ErrorMsg(TErrorCode::PARQUET_ROWS_SKIPPING,
              col_reader->schema_element().name, filename()));
        }
      }
      // Ensure that the length of the micro_batch is less than
      // or equal to the capacity of scratch_batch_.
      DCHECK_LE(micro_batches[r].length, scratch_batch_->capacity);
      uint8_t* next_tuple_mem = scratch_batch_->tuple_mem
          + (scratch_batch_->tuple_byte_size * micro_batches[r].start);
      if (col_reader->max_rep_level() > 0) {
        continue_execution = col_reader->ReadValueBatch(&scratch_batch_->aux_mem_pool,
            micro_batches[r].length, tuple_byte_size_, next_tuple_mem, num_tuples);
      } else {
        continue_execution =
            col_reader->ReadNonRepeatedValueBatch(&scratch_batch_->aux_mem_pool,
                micro_batches[r].length, tuple_byte_size_, next_tuple_mem, num_tuples);
      }
      last = micro_batches[r].end;
      // Check that all column readers populated the same number of values.
      bool num_tuples_mismatch = c != 0 && last_num_tuples[r] != *num_tuples;
      if (UNLIKELY(!continue_execution || num_tuples_mismatch)) {
        // Skipping this row group. Free up all the resources with this row group.
        FlushRowGroupResources(row_batch);
        *num_tuples = 0;
        *skip_row_group = true;
        if (num_tuples_mismatch && continue_execution) {
          Status err(Substitute("Corrupt Parquet file '$0': column '$1' "
              "had $2 remaining values but expected $3",filename(),
              col_reader->schema_element().name, last_num_tuples[r], *num_tuples));
          parse_status_.MergeStatus(err);
        }
        return Status::OK();
      }
      last_num_tuples[r] = *num_tuples;
    }
    if (UNLIKELY(last < max_num_tuples - 1)) {
      if (UNLIKELY(!col_reader->SkipRows(max_num_tuples - 1 - last, -1))) {
        return Status(ErrorMsg(TErrorCode::PARQUET_ROWS_SKIPPING,
            col_reader->schema_element().name, filename()));
      }
    }
  }
  return Status::OK();
}

Status HdfsParquetScanner::CheckPageFiltering() {
  if (candidate_ranges_.empty() || scalar_readers_.empty()) return Status::OK();

  int64_t current_row = scalar_readers_[0]->LastProcessedRow();
  for (int i = 1; i < scalar_readers_.size(); ++i) {
    int64_t scalar_reader_row = scalar_readers_[i]->LastProcessedRow();
    if (current_row != scalar_reader_row) {
      // Column readers have two strategy to read a column:
      // 1: Initialize the reader with NextLevels(), then in a loop call ReadValue() then
      //    NextLevels(). NextLevels() increments 'current_row_' if the repetition level
      //    is zero. Because we invoke NextLevels() last, 'current_row_' might correspond
      //    to the row we are going to read next.
      // 2: Use the ReadValueBatch() to read a batch of values. In that case we
      //    simultaneously read the levels and values, so there is no readahead.
      //    'current_row_' always corresponds to the row that we have completely read.
      // Because in case 1 'current_row_' might correspond to the next row, or the row
      // currently being read, we might have a difference of one here.
      if (abs(current_row - scalar_reader_row ) > 1) {
        return Status(Substitute(
            "Top level rows aren't in sync during page filtering. "
            "Current row of $0: $1. Current row of $2: $3. Encountered it when "
            "processing file $4. For a workaround page filtering can be turned off by "
            "setting query option 'parquet_read_page_index' to FALSE.",
            scalar_readers_[0]->node_.element->name,
            current_row,
            scalar_readers_[i]->node_.element->name,
            scalar_reader_row,
            filename()));
      }
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
        col_reader->ReadItemPositionNonBatched(
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
  DCHECK_LE(scan_range_len, PARQUET_FOOTER_SIZE);
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
    // Report the ill-formatted Parquet version string in hex.
    return Status(TErrorCode::PARQUET_BAD_VERSION_NUMBER, filename(),
        ReadWriteUtil::HexDump(magic_number_ptr, sizeof(PARQUET_VERSION_NUMBER)),
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
    RETURN_IF_ERROR(ReadToBuffer(metadata_start, metadata_ptr, metadata_size));
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

  if (scan_node_->optimize_count_star()) {
    // Column readers are not needed because we are not reading from any columns if this
    // optimization is enabled.
    return Status::OK();
  }

  // Each tuple can have at most one position slot. We'll process this slot desc last.
  SlotDescriptor* pos_slot_desc = nullptr;
  SlotDescriptor* file_pos_slot_desc = nullptr;

  for (SlotDescriptor* slot_desc: tuple_desc.slots()) {
    // Skip partition columns
    if (!file_metadata_utils_.NeedDataInFile(slot_desc)) {
      if (UNLIKELY(slot_desc->virtual_column_type() ==
          TVirtualColumnType::FILE_POSITION)) {
        DCHECK(file_pos_slot_desc == nullptr)
            << "There should only be one position slot per tuple";
        file_pos_slot_desc = slot_desc;
      }
      continue;
    }

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

    if (col_reader->IsComplexReader()) {
      // Recursively populate col_reader's children
      DCHECK(slot_desc->children_tuple_descriptor() != nullptr);
      const TupleDescriptor* item_tuple_desc = slot_desc->children_tuple_descriptor();
      ComplexColumnReader* complex_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      RETURN_IF_ERROR(CreateColumnReaders(
          *item_tuple_desc, schema_resolver, complex_reader->children()));
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

  // We either have a file position slot or a position slot in a tuple, but not both.
  // Because file position is always at the table-level tuple, while position slot is
  // at collection item-level.
  DCHECK(file_pos_slot_desc == nullptr || pos_slot_desc == nullptr);
  if (file_pos_slot_desc != nullptr) {
    // 'tuple_desc' has a file position slot. Use an existing column reader to populate it
    DCHECK(!column_readers->empty());
    (*column_readers)[0]->set_file_pos_slot_desc(file_pos_slot_desc);
  } else if (pos_slot_desc != nullptr) {
    // 'tuple_desc' has a position slot. Use an existing column reader to populate it.
    DCHECK(!column_readers->empty());
    (*column_readers)[0]->set_pos_slot_desc(pos_slot_desc);
  }

  return Status::OK();
}

Status HdfsParquetScanner::CreateCountingReader(const SchemaPath& parent_path,
    const ParquetSchemaResolver& schema_resolver, ParquetColumnReader** reader) {
  SchemaNode* parent_node;
  bool missing_field;
  bool pos_field;
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

void HdfsParquetScanner::InitComplexColumns() {
  for (ComplexColumnReader* col_reader: complex_readers_) {
    col_reader->Reset();
  }
}

Status HdfsParquetScanner::InitScalarColumns(int64_t row_group_first_row) {
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
    RETURN_IF_ERROR(scalar_reader->Reset(*file_desc, col_chunk, row_group_idx_,
        row_group_first_row));
  }

  ColumnRangeLengths col_range_lengths(scalar_readers_.size());
  for (int i = 0; i < scalar_readers_.size(); ++i) {
    col_range_lengths[i] = scalar_readers_[i]->scan_range()->bytes_to_read();
  }

  ColumnReservations reservation_per_column;
  RETURN_IF_ERROR(
      DivideReservationBetweenColumns(col_range_lengths, reservation_per_column));
  for (auto& col_reservation : reservation_per_column) {
    scalar_readers_[col_reservation.first]->set_io_reservation(col_reservation.second);
  }
  return Status::OK();
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
    if (column_readers[c]->IsComplexReader()) continue;
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
      file_version_.application == "parquet-mr" &&
      state_->query_options().convert_legacy_hive_parquet_utc_timestamps &&
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
