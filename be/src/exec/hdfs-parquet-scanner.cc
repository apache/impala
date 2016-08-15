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

#include "exec/hdfs-parquet-scanner.h"

#include <limits> // for std::numeric_limits
#include <queue>

#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "exec/hdfs-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/parquet-column-readers.h"
#include "exec/scanner-context.inline.h"
#include "exprs/expr.h"
#include "runtime/collection-value-builder.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/scoped-buffer.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "rpc/thrift-util.h"

#include "common/names.h"

using strings::Substitute;
using namespace impala;

DEFINE_double(parquet_min_filter_reject_ratio, 0.1, "(Advanced) If the percentage of "
    "rows rejected by a runtime filter drops below this value, the filter is disabled.");
DECLARE_bool(enable_partitioned_aggregation);
DECLARE_bool(enable_partitioned_hash_join);

// The number of rows between checks to see if a filter is not effective, and should be
// disabled. Must be a power of two.
const int ROWS_PER_FILTER_SELECTIVITY_CHECK = 16 * 1024;
static_assert(
    !(ROWS_PER_FILTER_SELECTIVITY_CHECK & (ROWS_PER_FILTER_SELECTIVITY_CHECK - 1)),
    "ROWS_PER_FILTER_SELECTIVITY_CHECK must be a power of two");

// Max dictionary page header size in bytes. This is an estimate and only needs to be an
// upper bound.
const int MAX_DICT_HEADER_SIZE = 100;

const int64_t HdfsParquetScanner::FOOTER_SIZE;
const int16_t HdfsParquetScanner::ROW_GROUP_END;
const int16_t HdfsParquetScanner::INVALID_LEVEL;
const int16_t HdfsParquetScanner::INVALID_POS;

Status HdfsParquetScanner::IssueInitialRanges(HdfsScanNode* scan_node,
    const std::vector<HdfsFileDesc*>& files) {
  vector<DiskIoMgr::ScanRange*> footer_ranges;
  for (int i = 0; i < files.size(); ++i) {
    // If the file size is less than 12 bytes, it is an invalid Parquet file.
    if (files[i]->file_length < 12) {
      return Status(Substitute("Parquet file $0 has an invalid file length: $1",
          files[i]->filename, files[i]->file_length));
    }
    // Compute the offset of the file footer.
    int64_t footer_size = min(FOOTER_SIZE, files[i]->file_length);
    int64_t footer_start = files[i]->file_length - footer_size;
    DCHECK_GE(footer_start, 0);

    // Try to find the split with the footer.
    DiskIoMgr::ScanRange* footer_split = FindFooterSplit(files[i]);

    for (int j = 0; j < files[i]->splits.size(); ++j) {
      DiskIoMgr::ScanRange* split = files[i]->splits[j];

      DCHECK_LE(split->offset() + split->len(), files[i]->file_length);
      // If there are no materialized slots (such as count(*) over the table), we can
      // get the result with the file metadata alone and don't need to read any row
      // groups. We only want a single node to process the file footer in this case,
      // which is the node with the footer split.  If it's not a count(*), we create a
      // footer range for the split always.
      if (!scan_node->IsZeroSlotTableScan() || footer_split == split) {
        ScanRangeMetadata* split_metadata =
            reinterpret_cast<ScanRangeMetadata*>(split->meta_data());
        // Each split is processed by first issuing a scan range for the file footer, which
        // is done here, followed by scan ranges for the columns of each row group within
        // the actual split (in InitColumns()). The original split is stored in the
        // metadata associated with the footer range.
        DiskIoMgr::ScanRange* footer_range;
        if (footer_split != NULL) {
          footer_range = scan_node->AllocateScanRange(files[i]->fs,
              files[i]->filename.c_str(), footer_size, footer_start,
              split_metadata->partition_id, footer_split->disk_id(),
              footer_split->try_cache(), footer_split->expected_local(), files[i]->mtime,
              split);
        } else {
          // If we did not find the last split, we know it is going to be a remote read.
          footer_range = scan_node->AllocateScanRange(files[i]->fs,
              files[i]->filename.c_str(), footer_size, footer_start,
              split_metadata->partition_id, -1, false, false, files[i]->mtime, split);
        }

        footer_ranges.push_back(footer_range);
      } else {
        scan_node->RangeComplete(THdfsFileFormat::PARQUET, THdfsCompression::NONE);
      }
    }
  }
  // The threads that process the footer will also do the scan, so we mark all the files
  // as complete here.
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(footer_ranges, files.size()));
  return Status::OK();
}

DiskIoMgr::ScanRange* HdfsParquetScanner::FindFooterSplit(HdfsFileDesc* file) {
  DCHECK(file != NULL);
  for (int i = 0; i < file->splits.size(); ++i) {
    DiskIoMgr::ScanRange* split = file->splits[i];
    if (split->offset() + split->len() == file->file_length) return split;
  }
  return NULL;
}

namespace impala {

HdfsParquetScanner::HdfsParquetScanner(HdfsScanNode* scan_node, RuntimeState* state,
    bool add_batches_to_queue)
    : HdfsScanner(scan_node, state, add_batches_to_queue),
      row_group_idx_(-1),
      row_group_rows_read_(0),
      advance_row_group_(true),
      scratch_batch_(new ScratchTupleBatch(
          scan_node->row_desc(), state_->batch_size(), scan_node->mem_tracker())),
      metadata_range_(NULL),
      dictionary_pool_(new MemPool(scan_node->mem_tracker())),
      assemble_rows_timer_(scan_node_->materialize_tuple_timer()),
      num_cols_counter_(NULL),
      num_row_groups_counter_(NULL) {
  assemble_rows_timer_.Stop();
}

Status HdfsParquetScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Open(context));
  stream_->set_contains_tuple_data(false);
  metadata_range_ = stream_->scan_range();
  num_cols_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumColumns", TUnit::UNIT);
  num_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumRowGroups", TUnit::UNIT);

  scan_node_->IncNumScannersCodegenDisabled();

  level_cache_pool_.reset(new MemPool(scan_node_->mem_tracker()));

  for (int i = 0; i < context->filter_ctxs().size(); ++i) {
    const FilterContext* ctx = &context->filter_ctxs()[i];
    DCHECK(ctx->filter != NULL);
    if (!ctx->filter->AlwaysTrue()) filter_ctxs_.push_back(ctx);
  }
  filter_stats_.resize(filter_ctxs_.size());

  DCHECK(parse_status_.ok()) << "Invalid parse_status_" << parse_status_.GetDetail();

  // First process the file metadata in the footer.
  Status status = ProcessFooter();
  // Release I/O buffers immediately to make sure they are cleaned up
  // in case we return a non-OK status anywhere below.
  context_->ReleaseCompletedResources(NULL, true);
  RETURN_IF_ERROR(status);

  // Parse the file schema into an internal representation for schema resolution.
  schema_resolver_.reset(new ParquetSchemaResolver(*scan_node_->hdfs_table(),
      state_->query_options().parquet_fallback_schema_resolution));
  RETURN_IF_ERROR(schema_resolver_->Init(&file_metadata_, filename()));

  // We've processed the metadata and there are columns that need to be materialized.
  RETURN_IF_ERROR(CreateColumnReaders(
      *scan_node_->tuple_desc(), *schema_resolver_, &column_readers_));
  COUNTER_SET(num_cols_counter_,
      static_cast<int64_t>(CountScalarColumns(column_readers_)));
  // Set top-level template tuple.
  template_tuple_ = template_tuple_map_[scan_node_->tuple_desc()];

  // The scanner-wide stream was used only to read the file footer.  Each column has added
  // its own stream.
  stream_ = NULL;
  return Status::OK();
}

void HdfsParquetScanner::Close(RowBatch* row_batch) {
  if (row_batch != NULL) {
    FlushRowGroupResources(row_batch);
    if (add_batches_to_queue_) scan_node_->AddMaterializedRowBatch(row_batch);
  } else if (!FLAGS_enable_partitioned_hash_join ||
      !FLAGS_enable_partitioned_aggregation) {
    // With the legacy aggs/joins the tuple ptrs of the scratch batch are allocated
    // from the scratch batch's mem pool. We can get into this case if Open() fails.
    scratch_batch_->mem_pool()->FreeAll();
  }
  // Verify all resources (if any) have been transferred.
  DCHECK_EQ(dictionary_pool_.get()->total_allocated_bytes(), 0);
  DCHECK_EQ(scratch_batch_->mem_pool()->total_allocated_bytes(), 0);
  DCHECK_EQ(context_->num_completed_io_buffers(), 0);

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
  if (compression_types.empty()) compression_types.push_back(THdfsCompression::NONE);
  scan_node_->RangeComplete(THdfsFileFormat::PARQUET, compression_types);

  if (level_cache_pool_.get() != NULL) {
    level_cache_pool_->FreeAll();
    level_cache_pool_.reset();
  }

  if (schema_resolver_.get() != NULL) schema_resolver_.reset();

  for (int i = 0; i < filter_ctxs_.size(); ++i) {
    const FilterStats* stats = filter_ctxs_[i]->stats;
    const LocalFilterStats& local = filter_stats_[i];
    stats->IncrCounters(FilterStats::ROWS_KEY, local.total_possible,
        local.considered, local.rejected);
  }

  HdfsScanner::Close(row_batch);
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

int HdfsParquetScanner::CountScalarColumns(const vector<ParquetColumnReader*>& column_readers) {
  DCHECK(!column_readers.empty());
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
  DCHECK(add_batches_to_queue_);
  bool scanner_eos = false;
  do {
    RETURN_IF_ERROR(StartNewRowBatch());
    RETURN_IF_ERROR(GetNextInternal(batch_, &scanner_eos));
    scan_node_->AddMaterializedRowBatch(batch_);
  } while (!scanner_eos && !scan_node_->ReachedLimit());

  // Transfer the remaining resources to this new batch in Close().
  RETURN_IF_ERROR(StartNewRowBatch());
  return Status::OK();
}

Status HdfsParquetScanner::GetNextInternal(RowBatch* row_batch, bool* eos) {
  if (scan_node_->IsZeroSlotTableScan()) {
    // There are no materialized slots, e.g. count(*) over the table.  We can serve
    // this query from just the file metadata. We don't need to read the column data.
    if (row_group_rows_read_ == file_metadata_.num_rows) {
      *eos = true;
      return Status::OK();
    }
    assemble_rows_timer_.Start();
    DCHECK_LE(row_group_rows_read_, file_metadata_.num_rows);
    int rows_remaining = file_metadata_.num_rows - row_group_rows_read_;
    int max_tuples = min(row_batch->capacity(), rows_remaining);
    TupleRow* current_row = row_batch->GetRow(row_batch->AddRow());
    int num_to_commit = WriteEmptyTuples(context_, current_row, max_tuples);
    Status status = CommitRows(row_batch, num_to_commit);
    assemble_rows_timer_.Stop();
    RETURN_IF_ERROR(status);
    row_group_rows_read_ += num_to_commit;
    COUNTER_ADD(scan_node_->rows_read_counter(), row_group_rows_read_);
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
    context_->ClearStreams();
    if (!advance_row_group_) {
      Status status =
          ValidateEndOfRowGroup(column_readers_, row_group_idx_, row_group_rows_read_);
      if (!status.ok()) RETURN_IF_ERROR(state_->LogOrReturnError(status.msg()));
    }
    RETURN_IF_ERROR(NextRowGroup());
    if (row_group_idx_ >= file_metadata_.row_groups.size()) {
      *eos = true;
      DCHECK(parse_status_.ok());
      return Status::OK();
    }
  }

  // Apply any runtime filters to static tuples containing the partition keys for this
  // partition. If any filter fails, we return immediately and stop processing this
  // scan range.
  if (!scan_node_->PartitionPassesFilters(context_->partition_descriptor()->id(),
      FilterStats::ROW_GROUPS_KEY, context_->filter_ctxs())) {
    *eos = true;
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

Status HdfsParquetScanner::NextRowGroup() {
  advance_row_group_ = false;
  row_group_rows_read_ = 0;

  // Loop until we have found a non-empty row group, and successfully initialized and
  // seeded the column readers. Return a non-OK status from within loop only if the error
  // is non-recoverable, otherwise log the error and continue with the next row group.
  while (true) {
    // Reset the parse status for the next row group.
    parse_status_ = Status::OK();

    ++row_group_idx_;
    if (row_group_idx_ >= file_metadata_.row_groups.size()) break;
    const parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx_];
    if (row_group.num_rows == 0) continue;

    const DiskIoMgr::ScanRange* split_range = reinterpret_cast<ScanRangeMetadata*>(
        metadata_range_->meta_data())->original_split;
    HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename());
    RETURN_IF_ERROR(ParquetMetadataUtils::ValidateColumnOffsets(
        file_desc->filename, file_desc->file_length, row_group));

    int64_t row_group_mid_pos = GetRowGroupMidOffset(row_group);
    int64_t split_offset = split_range->offset();
    int64_t split_length = split_range->len();
    if (!(row_group_mid_pos >= split_offset &&
        row_group_mid_pos < split_offset + split_length)) {
      // A row group is processed by the scanner whose split overlaps with the row
      // group's mid point. This row group will be handled by a different scanner.
      continue;
    }
    COUNTER_ADD(num_row_groups_counter_, 1);

    // Prepare column readers for first read
    RETURN_IF_ERROR(InitColumns(row_group_idx_, column_readers_));
    bool seeding_ok = true;
    for (ParquetColumnReader* col_reader: column_readers_) {
      // Seed collection and boolean column readers with NextLevel().
      // The ScalarColumnReaders use an optimized ReadValueBatch() that
      // should not be seeded.
      // TODO: Refactor the column readers to look more like the optimized
      // ScalarColumnReader::ReadValueBatch() which does not need seeding. This
      // will allow better sharing of code between the row-wise and column-wise
      // materialization strategies.
      if (col_reader->NeedsSeedingForBatchedReading()) {
        if (!col_reader->NextLevels()) {
          seeding_ok = false;
          break;
        }
      }
      DCHECK(parse_status_.ok()) << "Invalid parse_status_" << parse_status_.GetDetail();
    }

    if (!parse_status_.ok()) {
      RETURN_IF_ERROR(state_->LogOrReturnError(parse_status_.msg()));
    } else if (seeding_ok) {
      // Found a non-empty row group and successfully initialized the column readers.
      break;
    }
  }

  DCHECK(parse_status_.ok());
  return Status::OK();
}

void HdfsParquetScanner::FlushRowGroupResources(RowBatch* row_batch) {
  DCHECK(row_batch != NULL);
  row_batch->tuple_data_pool()->AcquireData(dictionary_pool_.get(), false);
  row_batch->tuple_data_pool()->AcquireData(scratch_batch_->mem_pool(), false);
  context_->ReleaseCompletedResources(row_batch, true);
  for (ParquetColumnReader* col_reader: column_readers_) {
    col_reader->Close(row_batch);
  }
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
  DCHECK(row_batch != NULL);
  DCHECK_EQ(*skip_row_group, false);
  DCHECK(scratch_batch_ != NULL);

  while (!column_readers[0]->RowGroupAtEnd()) {
    // Start a new scratch batch.
    RETURN_IF_ERROR(scratch_batch_->Reset(state_));
    int scratch_capacity = scratch_batch_->capacity();

    // Initialize tuple memory.
    for (int i = 0; i < scratch_capacity; ++i) {
      InitTuple(template_tuple_, scratch_batch_->GetTuple(i));
    }

    // Materialize the top-level slots into the scratch batch column-by-column.
    int last_num_tuples = -1;
    int num_col_readers = column_readers.size();
    bool continue_execution = true;
    for (int c = 0; c < num_col_readers; ++c) {
      ParquetColumnReader* col_reader = column_readers[c];
      if (col_reader->max_rep_level() > 0) {
        continue_execution = col_reader->ReadValueBatch(
            scratch_batch_->mem_pool(), scratch_capacity, tuple_byte_size_,
            scratch_batch_->tuple_mem, &scratch_batch_->num_tuples);
      } else {
        continue_execution = col_reader->ReadNonRepeatedValueBatch(
            scratch_batch_->mem_pool(), scratch_capacity, tuple_byte_size_,
            scratch_batch_->tuple_mem, &scratch_batch_->num_tuples);
      }
      // Check that all column readers populated the same number of values.
      bool num_tuples_mismatch = c != 0 && last_num_tuples != scratch_batch_->num_tuples;
      if (UNLIKELY(!continue_execution || num_tuples_mismatch)) {
        // Skipping this row group. Free up all the resources with this row group.
        scratch_batch_->mem_pool()->FreeAll();
        scratch_batch_->num_tuples = 0;
        *skip_row_group = true;
        if (num_tuples_mismatch) {
          parse_status_.MergeStatus(Substitute("Corrupt Parquet file '$0': column '$1' "
              "had $2 remaining values but expected $3", filename(),
              col_reader->schema_element().name, last_num_tuples,
              scratch_batch_->num_tuples));
        }
        return Status::OK();
      }
      last_num_tuples = scratch_batch_->num_tuples;
    }
    row_group_rows_read_ += scratch_batch_->num_tuples;
    COUNTER_ADD(scan_node_->rows_read_counter(), scratch_batch_->num_tuples);

    int num_row_to_commit = TransferScratchTuples(row_batch);
    RETURN_IF_ERROR(CommitRows(row_batch, num_row_to_commit));
    if (row_batch->AtCapacity()) return Status::OK();
  }

  return Status::OK();
}

Status HdfsParquetScanner::CommitRows(RowBatch* dst_batch, int num_rows) {
  DCHECK(dst_batch != NULL);
  dst_batch->CommitRows(num_rows);

  // We need to pass the row batch to the scan node if there is too much memory attached,
  // which can happen if the query is very selective. We need to release memory even
  // if no rows passed predicates.
  if (dst_batch->AtCapacity() || context_->num_completed_io_buffers() > 0) {
    context_->ReleaseCompletedResources(dst_batch, /* done */ false);
  }
  if (context_->cancelled()) return Status::CANCELLED;
  // TODO: It's a really bad idea to propagate UDF error via the global RuntimeState.
  // Store UDF error in thread local storage or make UDF return status so it can merge
  // with parse_status_.
  RETURN_IF_ERROR(state_->GetQueryStatus());
  // Free local expr allocations for this thread
  for (const auto& kv: scanner_conjuncts_map_) {
    ExprContext::FreeLocalAllocations(kv.second);
  }
  return Status::OK();
}

int HdfsParquetScanner::TransferScratchTuples(RowBatch* dst_batch) {
  // This function must not be called when the output batch is already full. As long as
  // we always call CommitRows() after TransferScratchTuples(), the output batch can
  // never be empty.
  DCHECK_LT(dst_batch->num_rows(), dst_batch->capacity());

  const bool has_filters = !filter_ctxs_.empty();
  const bool has_conjuncts = !scanner_conjunct_ctxs_->empty();
  ExprContext* const* conjunct_ctxs = &(*scanner_conjunct_ctxs_)[0];
  const int num_conjuncts = scanner_conjunct_ctxs_->size();

  // Start/end/current iterators over the output rows.
  DCHECK_EQ(scan_node_->tuple_idx(), 0);
  DCHECK_EQ(dst_batch->row_desc().tuple_descriptors().size(), 1);
  Tuple** output_row_start =
      reinterpret_cast<Tuple**>(dst_batch->GetRow(dst_batch->num_rows()));
  Tuple** output_row_end =
      output_row_start + (dst_batch->capacity() - dst_batch->num_rows());
  Tuple** output_row = output_row_start;

  // Start/end/current iterators over the scratch tuples.
  uint8_t* scratch_tuple_start = scratch_batch_->CurrTuple();
  uint8_t* scratch_tuple_end = scratch_batch_->TupleEnd();
  uint8_t* scratch_tuple = scratch_tuple_start;
  const int tuple_size = scratch_batch_->tuple_byte_size;

  if (tuple_size == 0) {
    // We are materializing a collection with empty tuples. Add a NULL tuple to the
    // output batch per remaining scratch tuple and return. No need to evaluate
    // filters/conjuncts.
    DCHECK(!has_filters);
    DCHECK(!has_conjuncts);
    int num_tuples = min(dst_batch->capacity() - dst_batch->num_rows(),
        scratch_batch_->num_tuples - scratch_batch_->tuple_idx);
    memset(output_row, 0, num_tuples * sizeof(Tuple*));
    scratch_batch_->tuple_idx += num_tuples;
    // If compressed Parquet was read then there may be data left to free from the
    // scratch batch (originating from the decompressed data pool).
    if (scratch_batch_->AtEnd()) scratch_batch_->mem_pool()->FreeAll();
    return num_tuples;
  }

  // Loop until the scratch batch is exhausted or the output batch is full.
  // Do not use batch_->AtCapacity() in this loop because it is not necessary
  // to perform the memory capacity check.
  while (scratch_tuple != scratch_tuple_end) {
    *output_row = reinterpret_cast<Tuple*>(scratch_tuple);
    scratch_tuple += tuple_size;
    // Evaluate runtime filters and conjuncts. Short-circuit the evaluation if
    // the filters/conjuncts are empty to avoid function calls.
    if (has_filters && !EvalRuntimeFilters(reinterpret_cast<TupleRow*>(output_row))) {
      continue;
    }
    if (has_conjuncts && !ExecNode::EvalConjuncts(
        conjunct_ctxs, num_conjuncts, reinterpret_cast<TupleRow*>(output_row))) {
      continue;
    }
    // Row survived runtime filters and conjuncts.
    ++output_row;
    if (output_row == output_row_end) break;
  }
  scratch_batch_->tuple_idx += (scratch_tuple - scratch_tuple_start) / tuple_size;

  // TODO: Consider compacting the output row batch to better handle cases where
  // there are few surviving tuples per scratch batch. In such cases, we could
  // quickly accumulate memory in the output batch, hit the memory capacity limit,
  // and return an output batch with relatively few rows.
  if (scratch_batch_->AtEnd()) {
    dst_batch->tuple_data_pool()->AcquireData(scratch_batch_->mem_pool(), false);
  }
  return output_row - output_row_start;
}

bool HdfsParquetScanner::EvalRuntimeFilters(TupleRow* row) {
  int num_filters = filter_ctxs_.size();
  for (int i = 0; i < num_filters; ++i) {
    LocalFilterStats* stats = &filter_stats_[i];
    if (!stats->enabled) continue;
    const RuntimeFilter* filter = filter_ctxs_[i]->filter;
    // Check filter effectiveness every ROWS_PER_FILTER_SELECTIVITY_CHECK rows.
    // TODO: The stats updates and the filter effectiveness check are executed very
    // frequently. Consider hoisting it out of of this loop, and doing an equivalent
    // check less frequently, e.g., after producing an output batch.
    ++stats->total_possible;
    if (UNLIKELY(
        !(stats->total_possible & (ROWS_PER_FILTER_SELECTIVITY_CHECK - 1)))) {
      double reject_ratio = stats->rejected / static_cast<double>(stats->considered);
      if (filter->AlwaysTrue() ||
          reject_ratio < FLAGS_parquet_min_filter_reject_ratio) {
        stats->enabled = 0;
        continue;
      }
    }
    ++stats->considered;
    void* e = filter_ctxs_[i]->expr->GetValue(row);
    if (!filter->Eval<void>(e, filter_ctxs_[i]->expr->root()->type())) {
      ++stats->rejected;
      return false;
    }
  }
  return true;
}

bool HdfsParquetScanner::AssembleCollection(
    const vector<ParquetColumnReader*>& column_readers, int new_collection_rep_level,
    CollectionValueBuilder* coll_value_builder) {
  DCHECK(!column_readers.empty());
  DCHECK_GE(new_collection_rep_level, 0);
  DCHECK(coll_value_builder != NULL);

  const TupleDescriptor* tuple_desc = &coll_value_builder->tuple_desc();
  Tuple* template_tuple = template_tuple_map_[tuple_desc];
  const vector<ExprContext*> conjunct_ctxs = scanner_conjuncts_map_[tuple_desc->id()];

  int64_t rows_read = 0;
  bool continue_execution = !scan_node_->ReachedLimit() && !context_->cancelled();
  // Note that this will be set to true at the end of the row group or the end of the
  // current collection (if applicable).
  bool end_of_collection = column_readers[0]->rep_level() == -1;
  // We only initialize end_of_collection to true here if we're at the end of the row
  // group (otherwise it would always be true because we're on the "edge" of two
  // collections), and only ProcessSplit() should call AssembleRows() at the end of the
  // row group.
  if (coll_value_builder != NULL) DCHECK(!end_of_collection);

  while (!end_of_collection && continue_execution) {
    MemPool* pool;
    Tuple* tuple;
    TupleRow* row = NULL;

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
    num_rows = std::min(
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
        if (ExecNode::EvalConjuncts(&conjunct_ctxs[0], conjunct_ctxs.size(), row)) {
          tuple = next_tuple(tuple_desc->byte_size(), tuple);
          ++num_to_commit;
        }
      }
    }

    rows_read += row_idx;
    COUNTER_ADD(scan_node_->rows_read_counter(), row_idx);
    coll_value_builder->CommitTuples(num_to_commit);
    continue_execution &= !scan_node_->ReachedLimit() && !context_->cancelled();
  }

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
      if (col_reader->pos_slot_desc() != NULL) col_reader->ReadPosition(tuple);
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
  int64_t len = stream_->scan_range()->len();

  // We're processing the scan range issued in IssueInitialRanges(). The scan range should
  // be the last FOOTER_BYTES of the file. !success means the file is shorter than we
  // expect. Note we can't detect if the file is larger than we expect without attempting
  // to read past the end of the scan range, but in this case we'll fail below trying to
  // parse the footer.
  DCHECK_LE(len, FOOTER_SIZE);
  uint8_t* buffer;
  bool success = stream_->ReadBytes(len, &buffer, &parse_status_);
  if (!success) {
    DCHECK(!parse_status_.ok());
    if (parse_status_.code() == TErrorCode::SCANNER_INCOMPLETE_READ) {
      VLOG_QUERY << "Metadata for file '" << filename() << "' appears stale: "
                 << "metadata states file size to be "
                 << PrettyPrinter::Print(stream_->file_desc()->file_length, TUnit::BYTES)
                 << ", but could only read "
                 << PrettyPrinter::Print(stream_->total_bytes_returned(), TUnit::BYTES);
      return Status(TErrorCode::STALE_METADATA_FILE_TOO_SHORT, filename(),
          scan_node_->hdfs_table()->fully_qualified_name());
    }
    return parse_status_;
  }
  DCHECK(stream_->eosr());

  // Number of bytes in buffer after the fixed size footer is accounted for.
  int remaining_bytes_buffered = len - sizeof(int32_t) - sizeof(PARQUET_VERSION_NUMBER);

  // Make sure footer has enough bytes to contain the required information.
  if (remaining_bytes_buffered < 0) {
    return Status(Substitute("File '$0' is invalid. Missing metadata.", filename()));
  }

  // Validate magic file bytes are correct.
  uint8_t* magic_number_ptr = buffer + len - sizeof(PARQUET_VERSION_NUMBER);
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
  uint8_t* metadata_ptr = metadata_size_ptr - metadata_size;
  // If the metadata was too big, we need to stitch it before deserializing it.
  // In that case, we stitch the data in this buffer.
  ScopedBuffer metadata_buffer(scan_node_->mem_tracker());

  DCHECK(metadata_range_ != NULL);
  if (UNLIKELY(metadata_size > remaining_bytes_buffered)) {
    // In this case, the metadata is bigger than our guess meaning there are
    // not enough bytes in the footer range from IssueInitialRanges().
    // We'll just issue more ranges to the IoMgr that is the actual footer.
    const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename());
    DCHECK(file_desc != NULL);
    // The start of the metadata is:
    // file_length - 4-byte metadata size - footer-size - metadata size
    int64_t metadata_start = file_desc->file_length -
      sizeof(int32_t) - sizeof(PARQUET_VERSION_NUMBER) - metadata_size;
    int64_t metadata_bytes_to_read = metadata_size;
    if (metadata_start < 0) {
      return Status(Substitute("File '$0' is invalid. Invalid metadata size in file "
          "footer: $1 bytes. File size: $2 bytes.", filename(), metadata_size,
          file_desc->file_length));
    }
    // IoMgr can only do a fixed size Read(). The metadata could be larger
    // so we stitch it here.
    // TODO: consider moving this stitching into the scanner context. The scanner
    // context usually handles the stitching but no other scanner need this logic
    // now.

    if (!metadata_buffer.TryAllocate(metadata_size)) {
      return Status(Substitute("Could not allocate buffer of $0 bytes for Parquet "
          "metadata for file '$1'.", metadata_size, filename()));
    }
    metadata_ptr = metadata_buffer.buffer();
    int64_t copy_offset = 0;
    DiskIoMgr* io_mgr = scan_node_->runtime_state()->io_mgr();

    while (metadata_bytes_to_read > 0) {
      int64_t to_read = ::min<int64_t>(io_mgr->max_read_buffer_size(),
          metadata_bytes_to_read);
      DiskIoMgr::ScanRange* range = scan_node_->AllocateScanRange(
          metadata_range_->fs(), filename(), to_read, metadata_start + copy_offset, -1,
          metadata_range_->disk_id(), metadata_range_->try_cache(),
          metadata_range_->expected_local(), file_desc->mtime);

      DiskIoMgr::BufferDescriptor* io_buffer = NULL;
      RETURN_IF_ERROR(io_mgr->Read(scan_node_->reader_context(), range, &io_buffer));
      memcpy(metadata_ptr + copy_offset, io_buffer->buffer(), io_buffer->len());
      io_buffer->Return();

      metadata_bytes_to_read -= to_read;
      copy_offset += to_read;
    }
    DCHECK_EQ(metadata_bytes_to_read, 0);
  }

  // Deserialize file header
  // TODO: this takes ~7ms for a 1000-column table, figure out how to reduce this.
  Status status =
      DeserializeThriftMsg(metadata_ptr, &metadata_size, true, &file_metadata_);
  if (!status.ok()) {
    return Status(Substitute("File $0 has invalid file metadata at file offset $1. "
        "Error = $2.", filename(),
        metadata_size + sizeof(PARQUET_VERSION_NUMBER) + sizeof(uint32_t),
        status.GetDetail()));
  }

  RETURN_IF_ERROR(ParquetMetadataUtils::ValidateFileVersion(file_metadata_, filename()));
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
  DCHECK(column_readers != NULL);
  DCHECK(column_readers->empty());

  // Each tuple can have at most one position slot. We'll process this slot desc last.
  SlotDescriptor* pos_slot_desc = NULL;

  for (SlotDescriptor* slot_desc: tuple_desc.slots()) {
    // Skip partition columns
    if (&tuple_desc == scan_node_->tuple_desc() &&
        slot_desc->col_pos() < scan_node_->num_partition_keys()) continue;

    SchemaNode* node = NULL;
    bool pos_field;
    bool missing_field;
    RETURN_IF_ERROR(schema_resolver.ResolvePath(
        slot_desc->col_path(), &node, &pos_field, &missing_field));

    if (missing_field) {
      // In this case, we are selecting a column that is not in the file.
      // Update the template tuple to put a NULL in this slot.
      Tuple** template_tuple = &template_tuple_map_[&tuple_desc];
      if (*template_tuple == NULL) {
        *template_tuple = scan_node_->InitEmptyTemplateTuple(tuple_desc);
      }
      (*template_tuple)->SetNull(slot_desc->null_indicator_offset());
      continue;
    }

    if (pos_field) {
      DCHECK(pos_slot_desc == NULL) << "There should only be one position slot per tuple";
      pos_slot_desc = slot_desc;
      continue;
    }

    ParquetColumnReader* col_reader = ParquetColumnReader::Create(
        *node, slot_desc->type().IsCollectionType(), slot_desc, this);
    column_readers->push_back(col_reader);

    if (col_reader->IsCollectionReader()) {
      // Recursively populate col_reader's children
      DCHECK(slot_desc->collection_item_descriptor() != NULL);
      const TupleDescriptor* item_tuple_desc = slot_desc->collection_item_descriptor();
      CollectionColumnReader* collection_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      RETURN_IF_ERROR(CreateColumnReaders(
          *item_tuple_desc, schema_resolver, collection_reader->children()));
    }
  }

  if (column_readers->empty()) {
    // This is either a count(*) over a collection type (count(*) over the table is
    // handled in ProcessFooter()), or no materialized columns appear in this file
    // (e.g. due to schema evolution, or if there's only a position slot). Create a single
    // column reader that we will use to count the number of tuples we should output. We
    // will not read any values from this reader.
    ParquetColumnReader* reader;
    RETURN_IF_ERROR(CreateCountingReader(tuple_desc.tuple_path(), schema_resolver, &reader));
    column_readers->push_back(reader);
  }

  if (pos_slot_desc != NULL) {
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
        *target_node, target_node->is_repeated(), NULL, this);
    if (target_node->is_repeated()) {
      // Find the closest scalar descendent of 'target_node' via breadth-first search, and
      // create scalar reader to drive 'reader'. We find the closest (i.e. least-nested)
      // descendent as a heuristic for picking a descendent with fewer values, so it's
      // faster to scan.
      // TODO: use different heuristic than least-nested? Fewest values?
      const SchemaNode* node = NULL;
      queue<const SchemaNode*> nodes;
      nodes.push(target_node);
      while (!nodes.empty()) {
        node = nodes.front();
        nodes.pop();
        if (node->children.size() > 0) {
          for (const SchemaNode& child: node->children) nodes.push(&child);
        } else {
          // node is the least-nested scalar descendent of 'target_node'
          break;
        }
      }
      DCHECK(node->children.empty()) << node->DebugString();
      CollectionColumnReader* parent_reader =
          static_cast<CollectionColumnReader*>(*reader);
      parent_reader->children()->push_back(
          ParquetColumnReader::Create(*node, false, NULL, this));
    }
  } else {
    // Special case for a repeated scalar node. The repeated node represents both the
    // parent collection and the child item.
    *reader = ParquetColumnReader::Create(*parent_node, false, NULL, this);
  }

  return Status::OK();
}

Status HdfsParquetScanner::InitColumns(
    int row_group_idx, const vector<ParquetColumnReader*>& column_readers) {
  const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename());
  DCHECK(file_desc != NULL);
  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx];

  // All the scan ranges (one for each column).
  vector<DiskIoMgr::ScanRange*> col_ranges;
  // Used to validate that the number of values in each reader in column_readers_ is the
  // same.
  int num_values = -1;
  // Used to validate we issued the right number of scan ranges
  int num_scalar_readers = 0;

  for (ParquetColumnReader* col_reader: column_readers) {
    if (col_reader->IsCollectionReader()) {
      CollectionColumnReader* collection_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      collection_reader->Reset();
      // Recursively init child readers
      RETURN_IF_ERROR(InitColumns(row_group_idx, *collection_reader->children()));
      continue;
    }
    ++num_scalar_readers;

    BaseScalarColumnReader* scalar_reader =
        static_cast<BaseScalarColumnReader*>(col_reader);
    const parquet::ColumnChunk& col_chunk = row_group.columns[scalar_reader->col_idx()];
    int64_t col_start = col_chunk.meta_data.data_page_offset;

    if (num_values == -1) {
      num_values = col_chunk.meta_data.num_values;
    } else if (col_chunk.meta_data.num_values != num_values) {
      // TODO for 2.3: improve this error message by saying which columns are different,
      // and also specify column in other error messages as appropriate
      return Status(TErrorCode::PARQUET_NUM_COL_VALS_ERROR, scalar_reader->col_idx(),
          col_chunk.meta_data.num_values, num_values, filename());
    }

    RETURN_IF_ERROR(ParquetMetadataUtils::ValidateColumn(file_metadata_, filename(),
        row_group_idx, scalar_reader->col_idx(), scalar_reader->schema_element(),
        scalar_reader->slot_desc_, state_));

    if (col_chunk.meta_data.__isset.dictionary_page_offset) {
      // Already validated in ValidateColumnOffsets()
      DCHECK_LT(col_chunk.meta_data.dictionary_page_offset, col_start);
      col_start = col_chunk.meta_data.dictionary_page_offset;
    }
    int64_t col_len = col_chunk.meta_data.total_compressed_size;
    if (col_len <= 0) {
      return Status(Substitute("File '$0' contains invalid column chunk size: $1",
          filename(), col_len));
    }
    int64_t col_end = col_start + col_len;

    // Already validated in ValidateColumnOffsets()
    DCHECK(col_end > 0 && col_end < file_desc->file_length);
    if (file_version_.application == "parquet-mr" && file_version_.VersionLt(1, 2, 9)) {
      // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
      // dictionary page header size in total_compressed_size and total_uncompressed_size
      // (see IMPALA-694). We pad col_len to compensate.
      int64_t bytes_remaining = file_desc->file_length - col_end;
      int64_t pad = min<int64_t>(MAX_DICT_HEADER_SIZE, bytes_remaining);
      col_len += pad;
    }

    // TODO: this will need to change when we have co-located files and the columns
    // are different files.
    if (!col_chunk.file_path.empty() && col_chunk.file_path != filename()) {
      return Status(Substitute("Expected parquet column file path '$0' to match "
          "filename '$1'", col_chunk.file_path, filename()));
    }

    const DiskIoMgr::ScanRange* split_range =
        reinterpret_cast<ScanRangeMetadata*>(metadata_range_->meta_data())->original_split;

    // Determine if the column is completely contained within a local split.
    bool column_range_local = split_range->expected_local() &&
                              col_start >= split_range->offset() &&
                              col_end <= split_range->offset() + split_range->len();

    DiskIoMgr::ScanRange* col_range = scan_node_->AllocateScanRange(
        metadata_range_->fs(), filename(), col_len, col_start, scalar_reader->col_idx(),
        split_range->disk_id(), split_range->try_cache(), column_range_local,
        file_desc->mtime);
    col_ranges.push_back(col_range);

    // Get the stream that will be used for this column
    ScannerContext::Stream* stream = context_->AddStream(col_range);
    DCHECK(stream != NULL);

    RETURN_IF_ERROR(scalar_reader->Reset(&col_chunk.meta_data, stream));

    const SlotDescriptor* slot_desc = scalar_reader->slot_desc();
    if (slot_desc == NULL || !slot_desc->type().IsStringType() ||
        col_chunk.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED) {
      // Non-string types are always compact.  Compressed columns don't reference data in
      // the io buffers after tuple materialization.  In both cases, we can set compact to
      // true and recycle buffers more promptly.
      stream->set_contains_tuple_data(false);
    }
  }
  DCHECK_EQ(col_ranges.size(), num_scalar_readers);

  // Issue all the column chunks to the io mgr and have them scheduled immediately.
  // This means these ranges aren't returned via DiskIoMgr::GetNextRange and
  // instead are scheduled to be read immediately.
  RETURN_IF_ERROR(scan_node_->runtime_state()->io_mgr()->AddScanRanges(
      scan_node_->reader_context(), col_ranges, true));

  return Status::OK();
}

Status HdfsParquetScanner::ValidateEndOfRowGroup(
    const vector<ParquetColumnReader*>& column_readers, int row_group_idx, int64_t rows_read) {
  DCHECK(!column_readers.empty());
  DCHECK(parse_status_.ok()) << "Don't overwrite parse_status_"
      << parse_status_.GetDetail();

  if (column_readers[0]->max_rep_level() == 0) {
    // These column readers materialize table-level values (vs. collection values). Test
    // if the expected number of rows from the file metadata matches the actual number of
    // rows read from the file.
    int64_t expected_rows_in_group = file_metadata_.row_groups[row_group_idx].num_rows;
    if (rows_read != expected_rows_in_group) {
      return Status(TErrorCode::PARQUET_GROUP_ROW_COUNT_ERROR, filename(), row_group_idx,
          expected_rows_in_group, rows_read);
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
    DCHECK_EQ(reader->num_values_read_, num_values_read);
    // ReadDataPage() uses metadata_->num_values to determine when the column's done
    DCHECK(reader->num_values_read_ == reader->metadata_->num_values ||
        !state_->abort_on_error());
  }
  return Status::OK();
}

}
