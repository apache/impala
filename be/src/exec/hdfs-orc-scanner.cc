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

#include "exec/hdfs-orc-scanner.h"

#include <queue>

#include "exec/scanner-context.inline.h"
#include "exprs/expr.h"
#include "runtime/exec-env.h"
#include "runtime/io/request-context.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/decompress.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;

DEFINE_bool(enable_orc_scanner, true,
    "If false, reading from ORC format tables is not supported");

Status HdfsOrcScanner::IssueInitialRanges(HdfsScanNodeBase* scan_node,
    const vector<HdfsFileDesc*>& files) {
  DCHECK(!files.empty());
  for (HdfsFileDesc* file : files) {
    // If the file size is less than 10 bytes, it is an invalid ORC file.
    if (file->file_length < 10) {
      return Status(Substitute("ORC file $0 has an invalid file length: $1",
          file->filename, file->file_length));
    }
  }
  return IssueFooterRanges(scan_node, THdfsFileFormat::ORC, files);
}

namespace impala {

HdfsOrcScanner::OrcMemPool::OrcMemPool(HdfsOrcScanner* scanner)
    : scanner_(scanner), mem_tracker_(scanner_->scan_node_->mem_tracker()) {
}

HdfsOrcScanner::OrcMemPool::~OrcMemPool() {
  FreeAll();
}

void HdfsOrcScanner::OrcMemPool::FreeAll() {
  int64_t total_bytes_released = 0;
  for (auto it = chunk_sizes_.begin(); it != chunk_sizes_.end(); ++it) {
    std::free(it->first);
    total_bytes_released += it->second;
  }
  mem_tracker_->Release(total_bytes_released);
  chunk_sizes_.clear();
}

// orc-reader will not check the malloc result. We throw an exception if we can't
// malloc to stop the orc-reader.
char* HdfsOrcScanner::OrcMemPool::malloc(uint64_t size) {
  if (!mem_tracker_->TryConsume(size)) {
    throw ResourceError(mem_tracker_->MemLimitExceeded(
        scanner_->state_, "Failed to allocate memory required by ORC library", size));
  }
  char* addr = static_cast<char*>(std::malloc(size));
  if (addr == nullptr) {
    mem_tracker_->Release(size);
    throw ResourceError(Status(TErrorCode::MEM_ALLOC_FAILED, size));
  }
  chunk_sizes_[addr] = size;
  return addr;
}

void HdfsOrcScanner::OrcMemPool::free(char* p) {
  DCHECK(chunk_sizes_.find(p) != chunk_sizes_.end()) << "invalid free!" << endl
       << GetStackTrace();
  std::free(p);
  int64_t size = chunk_sizes_[p];
  mem_tracker_->Release(size);
  chunk_sizes_.erase(p);
}

// TODO: improve this to use async IO (IMPALA-6636).
void HdfsOrcScanner::ScanRangeInputStream::read(void* buf, uint64_t length,
    uint64_t offset) {
  const ScanRange* metadata_range = scanner_->metadata_range_;
  const ScanRange* split_range =
      reinterpret_cast<ScanRangeMetadata*>(metadata_range->meta_data())->original_split;
  int64_t partition_id = scanner_->context_->partition_descriptor()->id();

  // Set expected_local to false to avoid cache on stale data (IMPALA-6830)
  bool expected_local = false;
  ScanRange* range = scanner_->scan_node_->AllocateScanRange(
      metadata_range->fs(), scanner_->filename(), length, offset, partition_id,
      split_range->disk_id(), expected_local, split_range->is_erasure_coded(),
      BufferOpts::ReadInto(reinterpret_cast<uint8_t*>(buf), length));

  unique_ptr<BufferDescriptor> io_buffer;
  Status status;
  {
    SCOPED_TIMER2(scanner_->state_->total_storage_wait_timer(),
        scanner_->scan_node_->scanner_io_wait_time());
    bool needs_buffers;
    status =
        scanner_->scan_node_->reader_context()->StartScanRange(range, &needs_buffers);
    DCHECK(!status.ok() || !needs_buffers) << "Already provided a buffer";
    if (status.ok()) status = range->GetNext(&io_buffer);
  }
  if (io_buffer != nullptr) range->ReturnBuffer(move(io_buffer));
  if (!status.ok()) throw ResourceError(status);
}

HdfsOrcScanner::HdfsOrcScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
  : HdfsScanner(scan_node, state),
    assemble_rows_timer_(scan_node_->materialize_tuple_timer()) {
  assemble_rows_timer_.Stop();
}

HdfsOrcScanner::~HdfsOrcScanner() {
}

Status HdfsOrcScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Open(context));
  metadata_range_ = stream_->scan_range();
  num_cols_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumOrcColumns", TUnit::UNIT);
  num_stripes_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumOrcStripes", TUnit::UNIT);
  num_scanners_with_no_reads_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumScannersWithNoReads", TUnit::UNIT);
  process_footer_timer_stats_ =
      ADD_SUMMARY_STATS_TIMER(scan_node_->runtime_profile(), "OrcFooterProcessingTime");
  scan_node_->IncNumScannersCodegenDisabled();

  DCHECK(parse_status_.ok()) << "Invalid parse_status_" << parse_status_.GetDetail();
  for (const FilterContext& ctx : context->filter_ctxs()) {
    DCHECK(ctx.filter != nullptr);
    filter_ctxs_.push_back(&ctx);
  }
  filter_stats_.resize(filter_ctxs_.size());
  reader_mem_pool_.reset(new OrcMemPool(this));
  reader_options_.setMemoryPool(*reader_mem_pool_);

  // Each scan node can process multiple splits. Each split processes the footer once.
  // We use a timer to measure the time taken to ProcessFileTail() per split and add
  // this time to the averaged timer.
  MonotonicStopWatch single_footer_process_timer;
  single_footer_process_timer.Start();
  // First process the file metadata in the footer.
  Status footer_status = ProcessFileTail();
  single_footer_process_timer.Stop();
  process_footer_timer_stats_->UpdateCounter(single_footer_process_timer.ElapsedTime());

  // Release I/O buffers immediately to make sure they are cleaned up
  // in case we return a non-OK status anywhere below.
  context_->ReleaseCompletedResources(true);
  RETURN_IF_ERROR(footer_status);

  // Update orc reader options base on the tuple descriptor
  RETURN_IF_ERROR(SelectColumns(scan_node_->tuple_desc()));

  // Set top-level template tuple.
  template_tuple_ = template_tuple_map_[scan_node_->tuple_desc()];
  return Status::OK();
}

void HdfsOrcScanner::Close(RowBatch* row_batch) {
  DCHECK(!is_closed_);
  if (row_batch != nullptr) {
    context_->ReleaseCompletedResources(true);
    row_batch->tuple_data_pool()->AcquireData(template_tuple_pool_.get(), false);
    if (scan_node_->HasRowBatchQueue()) {
      static_cast<HdfsScanNode*>(scan_node_)->AddMaterializedRowBatch(
          unique_ptr<RowBatch>(row_batch));
    }
  } else {
    template_tuple_pool_->FreeAll();
    context_->ReleaseCompletedResources(true);
  }
  scratch_batch_.reset(nullptr);

  // Verify all resources (if any) have been transferred.
  DCHECK_EQ(template_tuple_pool_->total_allocated_bytes(), 0);

  assemble_rows_timer_.Stop();
  assemble_rows_timer_.ReleaseCounter();

  THdfsCompression::type compression_type = THdfsCompression::NONE;
  if (reader_ != nullptr) {
    compression_type = TranslateCompressionKind(reader_->getCompression());
  }
  scan_node_->RangeComplete(THdfsFileFormat::ORC, compression_type);

  for (int i = 0; i < filter_ctxs_.size(); ++i) {
    const FilterStats* stats = filter_ctxs_[i]->stats;
    const LocalFilterStats& local = filter_stats_[i];
    stats->IncrCounters(FilterStats::ROWS_KEY, local.total_possible,
        local.considered, local.rejected);
  }
  CloseInternal();
}

Status HdfsOrcScanner::ProcessFileTail() {
  unique_ptr<orc::InputStream> input_stream(new ScanRangeInputStream(this));
  VLOG_FILE << "Processing FileTail of ORC file: " << input_stream->getName()
      << ", length: " << input_stream->getLength();
  try {
    reader_ = orc::createReader(move(input_stream), reader_options_);
  } catch (ResourceError& e) {  // errors throw from the orc scanner
    parse_status_ = e.GetStatus();
    return parse_status_;
  } catch (std::exception& e) { // other errors throw from the orc library
    string msg = Substitute("Encountered parse error in tail of ORC file $0: $1",
        filename(), e.what());
    parse_status_ = Status(msg);
    return parse_status_;
  }

  if (reader_->getNumberOfRows() == 0)  return Status::OK();
  if (reader_->getNumberOfStripes() == 0) {
    return Status(Substitute("Invalid ORC file: $0. No stripes in this file but"
        " numberOfRows in footer is $1", filename(), reader_->getNumberOfRows()));
  }
  return Status::OK();
}

inline THdfsCompression::type HdfsOrcScanner::TranslateCompressionKind(
    orc::CompressionKind kind) {
  switch (kind) {
    case orc::CompressionKind::CompressionKind_NONE: return THdfsCompression::NONE;
    // zlib used in ORC is corresponding to Deflate in Impala
    case orc::CompressionKind::CompressionKind_ZLIB: return THdfsCompression::DEFLATE;
    case orc::CompressionKind::CompressionKind_SNAPPY: return THdfsCompression::SNAPPY;
    case orc::CompressionKind::CompressionKind_LZO: return THdfsCompression::LZO;
    case orc::CompressionKind::CompressionKind_LZ4: return THdfsCompression::LZ4;
    case orc::CompressionKind::CompressionKind_ZSTD: return THdfsCompression::ZSTD;
    default:
      VLOG_QUERY << "Unknown compression kind of orc::CompressionKind: " << kind;
  }
  return THdfsCompression::DEFAULT;
}

Status HdfsOrcScanner::SelectColumns(const TupleDescriptor* tuple_desc) {
  list<uint64_t> selected_indices;
  int num_columns = 0;
  const orc::Type& root_type = reader_->getType();
  // TODO validate columns. e.g. scale of decimal type
  for (SlotDescriptor* slot_desc: tuple_desc->slots()) {
    // Skip partition columns
    if (slot_desc->col_pos() < scan_node_->num_partition_keys()) continue;

    const SchemaPath &path = slot_desc->col_path();
    DCHECK_EQ(path.size(), 1);
    int col_idx = path[0];
    // The first index in a path includes the table's partition keys
    int col_idx_in_file = col_idx - scan_node_->num_partition_keys();
    if (col_idx_in_file >= root_type.getSubtypeCount()) {
      // In this case, we are selecting a column that is not in the file.
      // Update the template tuple to put a NULL in this slot.
      Tuple** template_tuple = &template_tuple_map_[tuple_desc];
      if (*template_tuple == nullptr) {
        *template_tuple =
            Tuple::Create(tuple_desc->byte_size(), template_tuple_pool_.get());
      }
      (*template_tuple)->SetNull(slot_desc->null_indicator_offset());
      continue;
    }
    selected_indices.push_back(col_idx_in_file);
    const orc::Type* orc_type = root_type.getSubtype(col_idx_in_file);
    const ColumnType& col_type = scan_node_->hdfs_table()->col_descs()[col_idx].type();
    // TODO(IMPALA-6503): Support reading complex types from ORC format files
    DCHECK(!col_type.IsComplexType()) << "Complex types are not supported yet";
    RETURN_IF_ERROR(ValidateType(col_type, *orc_type));
    col_id_slot_map_[orc_type->getColumnId()] = slot_desc;
    ++num_columns;
  }
  COUNTER_SET(num_cols_counter_, static_cast<int64_t>(num_columns));
  row_reader_options.include(selected_indices);
  return Status::OK();
}

Status HdfsOrcScanner::ValidateType(const ColumnType& type, const orc::Type& orc_type) {
  switch (orc_type.getKind()) {
    case orc::TypeKind::BOOLEAN:
      if (type.type == TYPE_BOOLEAN) return Status::OK();
      break;
    case orc::TypeKind::BYTE:
      if (type.type == TYPE_TINYINT || type.type == TYPE_SMALLINT
          || type.type == TYPE_INT || type.type == TYPE_BIGINT)
        return Status::OK();
      break;
    case orc::TypeKind::SHORT:
      if (type.type == TYPE_SMALLINT || type.type == TYPE_INT
          || type.type == TYPE_BIGINT)
        return Status::OK();
      break;
    case orc::TypeKind::INT:
      if (type.type == TYPE_INT || type.type == TYPE_BIGINT) return Status::OK();
      break;
    case orc::TypeKind::LONG:
      if (type.type == TYPE_BIGINT) return Status::OK();
      break;
    case orc::TypeKind::FLOAT:
    case orc::TypeKind::DOUBLE:
      if (type.type == TYPE_FLOAT || type.type == TYPE_DOUBLE) return Status::OK();
      break;
    case orc::TypeKind::STRING:
    case orc::TypeKind::VARCHAR:
    case orc::TypeKind::CHAR:
      if (type.type == TYPE_STRING || type.type == TYPE_VARCHAR
          || type.type == TYPE_CHAR)
        return Status::OK();
      break;
    case orc::TypeKind::TIMESTAMP:
      if (type.type == TYPE_TIMESTAMP) return Status::OK();
      break;
    case orc::TypeKind::DECIMAL: {
      if (type.type != TYPE_DECIMAL || type.scale != orc_type.getScale()) break;
      bool overflow = false;
      int orc_precision = orc_type.getPrecision();
      if (orc_precision == 0 || orc_precision > ColumnType::MAX_DECIMAL8_PRECISION) {
        // For ORC decimals whose precision is larger than 18, its value can't fit into
        // an int64 (10^19 > 2^63). So we should use int128 (16 bytes) for this case.
        // The possible byte sizes for Impala decimals are 4, 8, 16.
        // We mark it as overflow if the target byte size is not 16.
        overflow = (type.GetByteSize() != 16);
      } else if (orc_type.getPrecision() > ColumnType::MAX_DECIMAL4_PRECISION) {
        // For ORC decimals whose precision <= 18 and > 9, int64 and int128 can fit them.
        // We only mark it as overflow if the target byte size is 4.
        overflow = (type.GetByteSize() == 4);
      }
      if (!overflow) return Status::OK();
      return Status(Substitute(
          "It can't be truncated to table column $2 for column $0 in ORC file '$1'",
          orc_type.toString(), filename(), type.DebugString()));
    }
    default: break;
  }
  return Status(Substitute(
      "Type mismatch: table column $0 is map to column $1 in ORC file '$2'",
      type.DebugString(), orc_type.toString(), filename()));
}

Status HdfsOrcScanner::ProcessSplit() {
  DCHECK(scan_node_->HasRowBatchQueue());
  HdfsScanNode* scan_node = static_cast<HdfsScanNode*>(scan_node_);
  do {
    unique_ptr<RowBatch> batch = make_unique<RowBatch>(scan_node_->row_desc(),
        state_->batch_size(), scan_node_->mem_tracker());
    Status status = GetNextInternal(batch.get());
    // Always add batch to the queue because it may contain data referenced by previously
    // appended batches.
    scan_node->AddMaterializedRowBatch(move(batch));
    RETURN_IF_ERROR(status);
    ++row_batches_produced_;
    if ((row_batches_produced_ & (BATCHES_PER_FILTER_SELECTIVITY_CHECK - 1)) == 0) {
      CheckFiltersEffectiveness();
    }
  } while (!eos_ && !scan_node_->ReachedLimit());
  return Status::OK();
}

Status HdfsOrcScanner::GetNextInternal(RowBatch* row_batch) {
  if (scan_node_->IsZeroSlotTableScan()) {
    uint64_t file_rows = reader_->getNumberOfRows();
    // There are no materialized slots, e.g. count(*) over the table.  We can serve
    // this query from just the file metadata.  We don't need to read the column data.
    if (stripe_rows_read_ == file_rows) {
      eos_ = true;
      return Status::OK();
    }
    assemble_rows_timer_.Start();
    DCHECK_LT(stripe_rows_read_, file_rows);
    int64_t rows_remaining = file_rows - stripe_rows_read_;
    int max_tuples = min<int64_t>(row_batch->capacity(), rows_remaining);
    TupleRow* current_row = row_batch->GetRow(row_batch->AddRow());
    int num_to_commit = WriteTemplateTuples(current_row, max_tuples);
    Status status = CommitRows(num_to_commit, row_batch);
    assemble_rows_timer_.Stop();
    RETURN_IF_ERROR(status);
    stripe_rows_read_ += max_tuples;
    COUNTER_ADD(scan_node_->rows_read_counter(), num_to_commit);
    return Status::OK();
  }

  // reset tuple memory. We'll allocate it the first time we use it.
  tuple_mem_ = nullptr;
  tuple_ = nullptr;

  // Transfer remaining tuples from the scratch batch.
  if (ScratchBatchNotEmpty()) {
    assemble_rows_timer_.Start();
    RETURN_IF_ERROR(TransferScratchTuples(row_batch));
    assemble_rows_timer_.Stop();
    if (row_batch->AtCapacity()) return Status::OK();
    DCHECK_EQ(scratch_batch_tuple_idx_, scratch_batch_->numElements);
  }

  while (advance_stripe_ || end_of_stripe_) {
    context_->ReleaseCompletedResources(/* done */ true);
    // Commit the rows to flush the row batch from the previous stripe
    RETURN_IF_ERROR(CommitRows(0, row_batch));

    RETURN_IF_ERROR(NextStripe());
    DCHECK_LE(stripe_idx_, reader_->getNumberOfStripes());
    if (stripe_idx_ == reader_->getNumberOfStripes()) {
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
  Status status = AssembleRows(row_batch);
  assemble_rows_timer_.Stop();
  RETURN_IF_ERROR(status);
  if (!parse_status_.ok()) {
    RETURN_IF_ERROR(state_->LogOrReturnError(parse_status_.msg()));
    parse_status_ = Status::OK();
  }
  return Status::OK();
}

inline bool HdfsOrcScanner::ScratchBatchNotEmpty() {
  return scratch_batch_ != nullptr
      && scratch_batch_tuple_idx_ < scratch_batch_->numElements;
}

inline static bool CheckStripeOverlapsSplit(int64_t stripe_start, int64_t stripe_end,
    int64_t split_start, int64_t split_end) {
  return (split_start >= stripe_start && split_start < stripe_end) ||
      (split_end > stripe_start && split_end <= stripe_end) ||
      (split_start <= stripe_start && split_end >= stripe_end);
}

Status HdfsOrcScanner::NextStripe() {
  const ScanRange* split_range = static_cast<ScanRangeMetadata*>(
      metadata_range_->meta_data())->original_split;
  int64_t split_offset = split_range->offset();
  int64_t split_length = split_range->len();

  bool start_with_first_stripe = stripe_idx_ == -1;
  bool misaligned_stripe_skipped = false;

  advance_stripe_ = false;
  stripe_rows_read_ = 0;

  // Loop until we have found a non-empty stripe.
  while (true) {
    // Reset the parse status for the next stripe.
    parse_status_ = Status::OK();

    ++stripe_idx_;
    if (stripe_idx_ >= reader_->getNumberOfStripes()) {
      if (start_with_first_stripe && misaligned_stripe_skipped) {
        // We started with the first stripe and skipped all the stripes because they were
        // misaligned. The execution flow won't reach this point if there is at least one
        // non-empty stripe which this scanner can process.
        COUNTER_ADD(num_scanners_with_no_reads_counter_, 1);
      }
      break;
    }
    unique_ptr<orc::StripeInformation> stripe = reader_->getStripe(stripe_idx_);
    // Also check 'footer_.numberOfRows' to make sure 'select count(*)' and 'select *'
    // behave consistently for corrupt files that have 'footer_.numberOfRows == 0'
    // but some data in stripe.
    if (stripe->getNumberOfRows() == 0 || reader_->getNumberOfRows() == 0) continue;

    uint64_t stripe_offset = stripe->getOffset();
    uint64_t stripe_len = stripe->getIndexLength() + stripe->getDataLength() +
        stripe->getFooterLength();
    int64_t stripe_mid_pos = stripe_offset + stripe_len / 2;
    if (!(stripe_mid_pos >= split_offset &&
        stripe_mid_pos < split_offset + split_length)) {
      // Middle pos not in split, this stripe will be handled by a different scanner.
      // Mark if the stripe overlaps with the split.
      misaligned_stripe_skipped |= CheckStripeOverlapsSplit(stripe_offset,
          stripe_offset + stripe_len, split_offset, split_offset + split_length);
      continue;
    }

    // TODO: check if this stripe can be skipped by stats. e.g. IMPALA-6505

    COUNTER_ADD(num_stripes_counter_, 1);
    row_reader_options.range(stripe->getOffset(), stripe_len);
    try {
      row_reader_ = reader_->createRowReader(row_reader_options);
    } catch (ResourceError& e) {  // errors throw from the orc scanner
      parse_status_ = e.GetStatus();
      return parse_status_;
    } catch (std::exception& e) { // errors throw from the orc library
      VLOG_QUERY << "Error in creating ORC column readers: " << e.what();
      parse_status_ = Status(
          Substitute("Error in creating ORC column readers: $0.", e.what()));
      return parse_status_;
    }
    end_of_stripe_ = false;
    VLOG_ROW << Substitute("Created RowReader for stripe(offset=$0, len=$1) in file $2",
        stripe->getOffset(), stripe_len, filename());
    break;
  }

  DCHECK(parse_status_.ok());
  return Status::OK();
}

Status HdfsOrcScanner::AssembleRows(RowBatch* row_batch) {
  bool continue_execution = !scan_node_->ReachedLimit() && !context_->cancelled();
  if (!continue_execution) return Status::CancelledInternal("ORC scanner");

  scratch_batch_tuple_idx_ = 0;
  scratch_batch_ = row_reader_->createRowBatch(row_batch->capacity());
  DCHECK_EQ(scratch_batch_->numElements, 0);

  int64_t num_rows_read = 0;
  while (continue_execution) {  // one ORC scratch batch (ColumnVectorBatch) in a round
    if (scratch_batch_tuple_idx_ == scratch_batch_->numElements) {
      try {
        if (!row_reader_->next(*scratch_batch_)) {
          end_of_stripe_ = true;
          break; // no more data to process
        }
      } catch (ResourceError& e) {
        parse_status_ = e.GetStatus();
        return parse_status_;
      } catch (std::exception& e) {
        VLOG_QUERY << "Encounter parse error: " << e.what();
        parse_status_ = Status(Substitute("Encounter parse error: $0.", e.what()));
        eos_ = true;
        return parse_status_;
      }
      if (scratch_batch_->numElements == 0) {
        RETURN_IF_ERROR(CommitRows(0, row_batch));
        end_of_stripe_ = true;
        return Status::OK();
      }
      num_rows_read += scratch_batch_->numElements;
      scratch_batch_tuple_idx_ = 0;
    }

    RETURN_IF_ERROR(TransferScratchTuples(row_batch));
    if (row_batch->AtCapacity()) break;
    continue_execution &= !scan_node_->ReachedLimit() && !context_->cancelled();
  }
  stripe_rows_read_ += num_rows_read;
  COUNTER_ADD(scan_node_->rows_read_counter(), num_rows_read);
  return Status::OK();
}

Status HdfsOrcScanner::TransferScratchTuples(RowBatch* dst_batch) {
  const TupleDescriptor* tuple_desc = scan_node_->tuple_desc();

  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_->data();
  int num_conjuncts = conjunct_evals_->size();

  const orc::Type* root_type = &row_reader_->getSelectedType();
  DCHECK_EQ(root_type->getKind(), orc::TypeKind::STRUCT);

  DCHECK_LT(dst_batch->num_rows(), dst_batch->capacity());
  if (tuple_ == nullptr) RETURN_IF_ERROR(AllocateTupleMem(dst_batch));
  int row_id = dst_batch->num_rows();
  int capacity = dst_batch->capacity();
  int num_to_commit = 0;
  TupleRow* row = dst_batch->GetRow(row_id);
  Tuple* tuple = tuple_;  // tuple_ is updated in CommitRows

  // TODO(IMPALA-6506): codegen the runtime filter + conjunct evaluation loop
  // TODO: transfer the scratch_batch_ column-by-column for batch, and then evaluate
  // the predicates in later loop.
  while (row_id < capacity && ScratchBatchNotEmpty()) {
    DCHECK_LT((void*)tuple, (void*)tuple_mem_end_);
    InitTuple(tuple_desc, template_tuple_, tuple);
    RETURN_IF_ERROR(ReadRow(static_cast<const orc::StructVectorBatch&>(*scratch_batch_),
        scratch_batch_tuple_idx_++, root_type, tuple, dst_batch));
    row->SetTuple(scan_node_->tuple_idx(), tuple);
    if (!EvalRuntimeFilters(row)) continue;
    if (ExecNode::EvalConjuncts(conjunct_evals, num_conjuncts, row)) {
      row = next_row(row);
      tuple = next_tuple(tuple_desc->byte_size(), tuple);
      ++row_id;
      ++num_to_commit;
    }
  }
  VLOG_ROW << Substitute("Transfer $0 rows from scratch batch to dst_batch ($1 rows)",
      num_to_commit, dst_batch->num_rows());
  return CommitRows(num_to_commit, dst_batch);
}

Status HdfsOrcScanner::AllocateTupleMem(RowBatch* row_batch) {
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size, &tuple_mem_));
  tuple_mem_end_ = tuple_mem_ + tuple_buffer_size;
  tuple_ = reinterpret_cast<Tuple*>(tuple_mem_);
  DCHECK_GT(row_batch->capacity(), 0);
  return Status::OK();
}

inline Status HdfsOrcScanner::ReadRow(const orc::StructVectorBatch& batch, int row_idx,
    const orc::Type* orc_type, Tuple* tuple, RowBatch* dst_batch) {
  for (unsigned int c = 0; c < orc_type->getSubtypeCount(); ++c) {
    orc::ColumnVectorBatch* col_batch = batch.fields[c];
    const orc::Type* col_type = orc_type->getSubtype(c);
    const SlotDescriptor* slot_desc = DCHECK_NOTNULL(
        col_id_slot_map_[col_type->getColumnId()]);
    if (col_batch->hasNulls && !col_batch->notNull[row_idx]) {
      tuple->SetNull(slot_desc->null_indicator_offset());
      continue;
    }
    void* slot_val_ptr = tuple->GetSlot(slot_desc->tuple_offset());
    switch (col_type->getKind()) {
      case orc::TypeKind::BOOLEAN: {
        int64_t val = static_cast<const orc::LongVectorBatch*>(col_batch)->
            data.data()[row_idx];
        *(reinterpret_cast<bool*>(slot_val_ptr)) = (val != 0);
        break;
      }
      case orc::TypeKind::BYTE:
      case orc::TypeKind::SHORT:
      case orc::TypeKind::INT:
      case orc::TypeKind::LONG: {
        const orc::LongVectorBatch* long_batch =
            static_cast<const orc::LongVectorBatch*>(col_batch);
        int64_t val = long_batch->data.data()[row_idx];
        switch (slot_desc->type().type) {
          case TYPE_TINYINT:
            *(reinterpret_cast<int8_t*>(slot_val_ptr)) = val;
            break;
          case TYPE_SMALLINT:
            *(reinterpret_cast<int16_t*>(slot_val_ptr)) = val;
            break;
          case TYPE_INT:
            *(reinterpret_cast<int32_t*>(slot_val_ptr)) = val;
            break;
          case TYPE_BIGINT:
            *(reinterpret_cast<int64_t*>(slot_val_ptr)) = val;
            break;
          default:
            DCHECK(false) << "Illegal translation from impala type "
                << slot_desc->DebugString() << " to orc INT";
        }
        break;
      }
      case orc::TypeKind::FLOAT:
      case orc::TypeKind::DOUBLE: {
        double val =
            static_cast<const orc::DoubleVectorBatch*>(col_batch)->data.data()[row_idx];
        if (slot_desc->type().type == TYPE_FLOAT) {
          *(reinterpret_cast<float*>(slot_val_ptr)) = val;
        } else {
          DCHECK_EQ(slot_desc->type().type, TYPE_DOUBLE);
          *(reinterpret_cast<double*>(slot_val_ptr)) = val;
        }
        break;
      }
      case orc::TypeKind::STRING:
      case orc::TypeKind::VARCHAR:
      case orc::TypeKind::CHAR: {
        auto str_batch = static_cast<const orc::StringVectorBatch*>(col_batch);
        const char* src_ptr = str_batch->data.data()[row_idx];
        int64_t src_len = str_batch->length.data()[row_idx];
        int dst_len = slot_desc->type().len;
        if (slot_desc->type().type == TYPE_CHAR) {
          int unpadded_len = min(dst_len, static_cast<int>(src_len));
          char* dst_char = reinterpret_cast<char*>(slot_val_ptr);
          memcpy(dst_char, src_ptr, unpadded_len);
          StringValue::PadWithSpaces(dst_char, dst_len, unpadded_len);
          break;
        }
        StringValue* dst = reinterpret_cast<StringValue*>(slot_val_ptr);
        if (slot_desc->type().type == TYPE_VARCHAR && src_len > dst_len) {
          dst->len = dst_len;
        } else {
          dst->len = src_len;
        }
        // Space in the StringVectorBatch is allocated by reader_mem_pool_. It will be
        // reused at next batch, so we allocate a new space for this string.
        uint8_t* buffer = dst_batch->tuple_data_pool()->TryAllocate(dst->len);
        if (buffer == nullptr) {
          string details = Substitute("Could not allocate string buffer of $0 bytes "
              "for ORC file '$1'.", dst->len, filename());
          return scan_node_->mem_tracker()->MemLimitExceeded(
              state_, details, dst->len);
        }
        dst->ptr = reinterpret_cast<char*>(buffer);
        memcpy(dst->ptr, src_ptr, dst->len);
        break;
      }
      case orc::TypeKind::TIMESTAMP: {
        const orc::TimestampVectorBatch* ts_batch =
            static_cast<const orc::TimestampVectorBatch*>(col_batch);
        int64_t secs = ts_batch->data.data()[row_idx];
        int64_t nanos = ts_batch->nanoseconds.data()[row_idx];
        *reinterpret_cast<TimestampValue*>(slot_val_ptr) =
            TimestampValue::FromUnixTimeNanos(secs, nanos, state_->local_time_zone());
        break;
      }
      case orc::TypeKind::DECIMAL: {
        // For decimals whose precision is larger than 18, its value can't fit into
        // an int64 (10^19 > 2^63). So we should use int128 for this case.
        if (col_type->getPrecision() == 0 || col_type->getPrecision() > 18) {
          auto int128_batch = static_cast<const orc::Decimal128VectorBatch*>(col_batch);
          orc::Int128 orc_val = int128_batch->values.data()[row_idx];

          DCHECK_EQ(slot_desc->type().GetByteSize(), 16);
          int128_t val = orc_val.getHighBits();
          val <<= 64;
          val |= orc_val.getLowBits();
          // Use memcpy to avoid gcc generating unaligned instructions like movaps
          // for int128_t. They will raise SegmentFault when addresses are not
          // aligned to 16 bytes.
          memcpy(slot_val_ptr, &val, sizeof(int128_t));
        } else {
          // Reminder: even decimal(1,1) is stored in int64 batch
          auto int64_batch = static_cast<const orc::Decimal64VectorBatch*>(col_batch);
          int64_t val = int64_batch->values.data()[row_idx];

          switch (slot_desc->type().GetByteSize()) {
            case 4:
              reinterpret_cast<Decimal4Value*>(slot_val_ptr)->value() = val;
              break;
            case 8:
              reinterpret_cast<Decimal8Value*>(slot_val_ptr)->value() = val;
              break;
            case 16:
              reinterpret_cast<Decimal16Value*>(slot_val_ptr)->value() = val;
              break;
            default: DCHECK(false) << "invalidate byte size";
          }
        }
        break;
      }
      case orc::TypeKind::LIST:
      case orc::TypeKind::MAP:
      case orc::TypeKind::STRUCT:
      case orc::TypeKind::UNION:
      default:
        DCHECK(false) << slot_desc->type().DebugString() << " map to ORC column "
            << col_type->toString();
    }
  }
  return Status::OK();
}

}
