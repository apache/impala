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

#include "exec/exec-node.inline.h"
#include "exec/orc-column-readers.h"
#include "exec/scanner-context.inline.h"
#include "exec/scratch-tuple-batch.h"
#include "exprs/expr.h"
#include "exprs/scalar-expr.h"
#include "runtime/collection-value-builder.h"
#include "runtime/exec-env.h"
#include "runtime/io/request-context.h"
#include "runtime/mem-tracker.h"
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
  if (!chunk_sizes_.empty()) {
    scanner_->state_->LogError(ErrorMsg(TErrorCode::INTERNAL_ERROR,
        "Impala had to free memory leaked by ORC library."));
  }
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
  int cache_options = split_range->cache_options() & ~BufferOpts::USE_HDFS_CACHE;
  ScanRange* range = scanner_->scan_node_->AllocateScanRange(
      metadata_range->fs(), scanner_->filename(), length, offset, partition_id,
      split_range->disk_id(), expected_local, split_range->mtime(),
      BufferOpts::ReadInto(reinterpret_cast<uint8_t*>(buf), length, cache_options));

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
  : HdfsColumnarScanner(scan_node, state),
    dictionary_pool_(new MemPool(scan_node->mem_tracker())),
    data_batch_pool_(new MemPool(scan_node->mem_tracker())),
    search_args_pool_(new MemPool(scan_node->mem_tracker())),
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

  codegend_process_scratch_batch_fn_ = scan_node_->GetCodegenFn(THdfsFileFormat::ORC);
  if (codegend_process_scratch_batch_fn_ == nullptr) {
    scan_node_->IncNumScannersCodegenDisabled();
  } else {
    scan_node_->IncNumScannersCodegenEnabled();
  }

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

  bool is_table_full_acid = scan_node_->hdfs_table()->IsTableFullAcid();
  schema_resolver_.reset(new OrcSchemaResolver(*scan_node_->hdfs_table(),
      &reader_->getType(), filename(), is_table_full_acid));
  bool is_file_full_acid = schema_resolver_->HasFullAcidV2Schema();
  acid_original_file_ = is_table_full_acid && !is_file_full_acid;
  if (is_table_full_acid) {
    acid_write_id_range_ = valid_write_ids_.GetWriteIdRange(filename());
    if (acid_original_file_ &&
        acid_write_id_range_.first != acid_write_id_range_.second) {
      return Status(Substitute("Found non-ACID file in directory that can only contain "
          "files with full ACID schema: $0", filename()));
    }
  }
  if (acid_original_file_) {
    int32_t filename_len = strlen(filename());
    if (filename_len >= 2 && strcmp(filename() + filename_len - 2, "_0") != 0) {
      // It's an original file that should be included in the result.
      // If it doesn't end with "_0" it means that it belongs to a bucket with other
      // files. Impala rejects such files and tables.
      // These files should only exist at table/partition root directory level.
      // Original files in delta directories are created via the LOAD DATA
      // statement. LOAD DATA assigns virtual bucket ids to files in non-bucketed
      // tables, so we will have one file per (virtual) bucket (all of them having "_0"
      // ending). For bucketed tables LOAD DATA will write ACID files. So after the first
      // major compaction the table should never get into this state ever again.
      return Status(Substitute("Found original file with unexpected name: $0 "
          "Please run a major compaction on the partition/table to overcome this.",
          filename()));
    }
  }

  // Hive Streaming Ingestion allocates multiple write ids, hence create delta directories
  // like delta_5_10. Then it continuously appends new stripes (and footers) to the
  // ORC files of the delte dir. So it's possible that the file has rows that Impala is
  // not allowed to see based on its valid write id list. In such cases we need to
  // validate the write ids of the row batches.
  if (is_table_full_acid && !ValidWriteIdList::IsCompacted(filename())) {
    valid_write_ids_.InitFrom(scan_node_->hdfs_table()->ValidWriteIdList());
    ValidWriteIdList::RangeResponse rows_valid = valid_write_ids_.IsWriteIdRangeValid(
        acid_write_id_range_.first, acid_write_id_range_.second);
    DCHECK_NE(rows_valid, ValidWriteIdList::NONE);
    row_batches_need_validation_ = rows_valid == ValidWriteIdList::SOME;
  }

  // Update 'row_reader_options_' based on the tuple descriptor so the ORC lib can skip
  // columns we don't need.
  RETURN_IF_ERROR(SelectColumns(*scan_node_->tuple_desc()));
  // By enabling lazy decoding, String stripes with DICTIONARY_ENCODING[_V2] can be
  // stored in an EncodedStringVectorBatch, where the data is stored in a dictionary
  // blob more efficiently.
  row_reader_options_.setEnableLazyDecoding(true);

  RETURN_IF_ERROR(PrepareSearchArguments());

  // To create OrcColumnReaders, we need the selected orc schema. It's a subset of the
  // file schema: a tree of selected orc types and can only be got from an orc::RowReader
  // (by orc::RowReader::getSelectedType).
  // Selected nodes are still connected as a tree since if a node is selected, all its
  // ancestors and children will be selected too.
  // Here we haven't read stripe data yet so no orc::RowReaders are created. To get the
  // selected types we create a temp orc::RowReader (but won't read rows from it).
  try {
    unique_ptr<orc::RowReader> tmp_row_reader =
        reader_->createRowReader(row_reader_options_);
    const orc::Type* root_type = &tmp_row_reader->getSelectedType();
    if (root_type->getKind() != orc::TypeKind::STRUCT) {
      parse_status_ = Status(TErrorCode::ORC_TYPE_NOT_ROOT_AT_STRUCT, "selected",
          root_type->toString(), filename());
      return parse_status_;
    }
    orc_root_reader_ = this->obj_pool_.Add(
        new OrcStructReader(root_type, scan_node_->tuple_desc(), this));
  } catch (std::exception& e) {
    string msg = Substitute("Encountered parse error during schema selection in "
        "ORC file $0: $1", filename(), e.what());
    parse_status_ = Status(msg);
    return parse_status_;
  }

  // Set top-level template tuple.
  template_tuple_ = template_tuple_map_[scan_node_->tuple_desc()];
  return Status::OK();
}

void HdfsOrcScanner::Close(RowBatch* row_batch) {
  DCHECK(!is_closed_);
  if (row_batch != nullptr) {
    context_->ReleaseCompletedResources(true);
    row_batch->tuple_data_pool()->AcquireData(template_tuple_pool_.get(), false);
    row_batch->tuple_data_pool()->AcquireData(dictionary_pool_.get(), false);
    row_batch->tuple_data_pool()->AcquireData(data_batch_pool_.get(), false);
    scratch_batch_->ReleaseResources(row_batch->tuple_data_pool());
    if (scan_node_->HasRowBatchQueue()) {
      static_cast<HdfsScanNode*>(scan_node_)->AddMaterializedRowBatch(
          unique_ptr<RowBatch>(row_batch));
    }
  } else {
    template_tuple_pool_->FreeAll();
    dictionary_pool_->FreeAll();
    data_batch_pool_->FreeAll();
    context_->ReleaseCompletedResources(true);
    scratch_batch_->ReleaseResources(nullptr);
  }
  orc_root_batch_.reset(nullptr);
  search_args_pool_->FreeAll();

  ScalarExprEvaluator::Close(min_max_conjunct_evals_, state_);

  // Verify all resources (if any) have been transferred.
  DCHECK_EQ(template_tuple_pool_->total_allocated_bytes(), 0);
  DCHECK_EQ(dictionary_pool_->total_allocated_bytes(), 0);
  DCHECK_EQ(data_batch_pool_->total_allocated_bytes(), 0);
  DCHECK_EQ(scratch_batch_->total_allocated_bytes(), 0);

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

bool HdfsOrcScanner::IsPartitionKeySlot(const SlotDescriptor* slot) {
  return slot->parent() == scan_node_->tuple_desc() &&
      slot->col_pos() < scan_node_->num_partition_keys();
}

bool HdfsOrcScanner::IsMissingField(const SlotDescriptor* slot) {
  return missing_field_slots_.find(slot) != missing_field_slots_.end();
}

// Fetch fully qualified name for 'col_path' by converting it into non-canonical
// table path.
string PrintColPath(const HdfsTableDescriptor& hdfs_table, const SchemaPath& col_path,
  const unique_ptr<OrcSchemaResolver>& schema_resolver) {
  SchemaPath table_col_path, file_col_path;
  if (col_path.size() > 0) {
    DCHECK(schema_resolver != nullptr);
    // Convert 'col_path' to non-canonical table path 'table_col_path'.
    schema_resolver->TranslateColPaths(col_path, &table_col_path, &file_col_path);
    auto it = table_col_path.begin();
    // remove initial -1s from the table_col_path
    // -1 is present to represent some of the constructs in ACID table which are not
    // present in table schema
    while (it != table_col_path.end()) {
      if (*it == -1) {
        it = table_col_path.erase(it);
      } else {
        break;
      }
    }
  }

  return PrintPath(hdfs_table, table_col_path);
}

Status HdfsOrcScanner::ResolveColumns(const TupleDescriptor& tuple_desc,
    list<const orc::Type*>* selected_nodes, stack<const SlotDescriptor*>* pos_slots) {
  const orc::Type* node = nullptr;
  bool pos_field = false;
  bool missing_field = false;
  // 1. Deal with the tuple descriptor. It should map to an ORC type.
  RETURN_IF_ERROR(schema_resolver_->ResolveColumn(tuple_desc.tuple_path(), &node,
      &pos_field, &missing_field));
  if (missing_field) {
    return Status(Substitute("Could not find nested column '$0' in file '$1'.",
        PrintColPath(*scan_node_->hdfs_table(), tuple_desc.tuple_path(),
        schema_resolver_), filename()));
  }
  tuple_to_col_id_.insert({&tuple_desc, node->getColumnId()});
  if (tuple_desc.byte_size() == 0) {
    // Don't need to materialize any slots but just generate an empty tuple for each
    // (collection) row. (E.g. count(*) or 'exists' on results of subquery).
    // Due to ORC-450 we can't get the number of tuples inside a collection without
    // reading its items (or subcolumn of its items). So we select the most inner
    // subcolumn of the collection (get by orc::Type::getMaximumColumnId()). E.g.
    // if 'node' is array<struct<c1:int,c2:int,c3:int>> and we just need the array
    // lengths, we still need to read at least one subcolumn otherwise the ORC lib
    // will skip the whole array column. So we select 'c3' for this case.
    selected_type_ids_.push_back(node->getMaximumColumnId());
    VLOG(3) << "Add ORC column " << node->getMaximumColumnId() << " for empty tuple "
        << PrintColPath(*scan_node_->hdfs_table(), tuple_desc.tuple_path(),
           schema_resolver_);
    return Status::OK();
  }

  // 2. Deal with slots of the tuple descriptor. Each slot should map to an ORC type,
  // otherwise it should be the position slot. Each tuple can have at most one position
  // slot.
  SlotDescriptor* pos_slot_desc = nullptr;
  for (SlotDescriptor* slot_desc : tuple_desc.slots()) {
    // Skip partition columns
    if (IsPartitionKeySlot(slot_desc)) continue;

    node = nullptr;
    pos_field = false;
    missing_field = false;
    // Reminder: slot_desc->col_path() can be much deeper than tuple_desc.tuple_path()
    // to reference to a deep subcolumn.
    RETURN_IF_ERROR(schema_resolver_->ResolveColumn(
        slot_desc->col_path(), &node, &pos_field, &missing_field));

    if (missing_field) {
      if (slot_desc->type().IsCollectionType()) {
        // If the collection column is missing, the whole scan range should return 0 rows
        // since we're selecting children column(s) of the collection.
        return Status(Substitute("Could not find nested column '$0' in file '$1'.",
            PrintColPath(*scan_node_->hdfs_table(), slot_desc->col_path(),
            schema_resolver_), filename()));
      }
      // In this case, we are selecting a column/subcolumn that is not in the file.
      // Update the template tuple to put a NULL in this slot.
      Tuple** template_tuple = &template_tuple_map_[&tuple_desc];
      if (*template_tuple == nullptr) {
        *template_tuple =
            Tuple::Create(tuple_desc.byte_size(), template_tuple_pool_.get());
      }
      if (acid_original_file_ && schema_resolver_->IsAcidColumn(slot_desc->col_path())) {
        SetSyntheticAcidFieldForOriginalFile(slot_desc, *template_tuple);
      } else {
        (*template_tuple)->SetNull(slot_desc->null_indicator_offset());
      }
      missing_field_slots_.insert(slot_desc);
      continue;
    }
    slot_to_col_id_.insert({slot_desc, node->getColumnId()});
    if (pos_field) {
      DCHECK(pos_slot_desc == nullptr)
          << "There should only be one position slot per tuple";
      pos_slot_desc = slot_desc;
      pos_slots->push(pos_slot_desc);
      DCHECK_EQ(node->getKind(), orc::TypeKind::LIST);
      continue;
    }

    if (slot_desc->type().IsComplexType()) {
      // Recursively resolve nested columns
      DCHECK(slot_desc->children_tuple_descriptor() != nullptr);
      const TupleDescriptor* item_tuple_desc = slot_desc->children_tuple_descriptor();
      RETURN_IF_ERROR(ResolveColumns(*item_tuple_desc, selected_nodes, pos_slots));
    } else {
      VLOG(3) << "Add ORC column " << node->getColumnId() << " for "
          << PrintColPath(*scan_node_->hdfs_table(), slot_desc->col_path(),
             schema_resolver_);
      selected_nodes->push_back(node);
    }
  }
  return Status::OK();
}

void HdfsOrcScanner::SetSyntheticAcidFieldForOriginalFile(const SlotDescriptor* slot_desc,
    Tuple* template_tuple) {
  DCHECK_EQ(1, slot_desc->col_path().size());
  int field_idx = slot_desc->col_path().front() - scan_node_->num_partition_keys();
  switch(field_idx) {
    case ACID_FIELD_OPERATION_INDEX:
      *template_tuple->GetIntSlot(slot_desc->tuple_offset()) = 0;
      break;
    case ACID_FIELD_ORIGINAL_TRANSACTION_INDEX:
    case ACID_FIELD_CURRENT_TRANSACTION_INDEX:
      DCHECK_EQ(acid_write_id_range_.first, acid_write_id_range_.second);
      *template_tuple->GetBigIntSlot(slot_desc->tuple_offset()) =
          acid_write_id_range_.first;
      break;
    case ACID_FIELD_BUCKET_INDEX:
      *template_tuple->GetBigIntSlot(slot_desc->tuple_offset()) =
          ValidWriteIdList::GetBucketProperty(filename());
      break;
    case ACID_FIELD_ROWID_INDEX:
      acid_synthetic_rowid_ = slot_desc;
    default:
      break;
  }
}

/// Whether 'selected_type_ids' contains the id of any children of 'node'
bool HasChildrenSelected(const orc::Type& node,
    const list<uint64_t>& selected_type_ids) {
  for (uint64_t id : selected_type_ids) {
    if (id >= node.getColumnId() && id <= node.getMaximumColumnId()) return true;
  }
  return false;
}

Status HdfsOrcScanner::SelectColumns(const TupleDescriptor& tuple_desc) {
  list<const orc::Type*> selected_nodes;
  stack<const SlotDescriptor*> pos_slots;
  // Select columns for all non-position slots.
  RETURN_IF_ERROR(ResolveColumns(tuple_desc, &selected_nodes, &pos_slots));

  for (auto t : selected_nodes) selected_type_ids_.push_back(t->getColumnId());

  // Select columns for array positions. Due to ORC-450 we can't materialize array
  // offsets without materializing its items, so we should still select the item or any
  // sub column of the item. To be simple, we choose the max column id in the subtree
  // of the ARRAY node.
  // We process the deeper position slots first since it may introduce an item column
  // that can also serve the position slot of upper arrays. E.g. for 'array_col' as
  // array<struct<c1:int,c2:int,c3:array<int>>>, if both 'array_col.pos' and
  // 'array_col.item.c3.pos' are needed, we just need to select 'array_col.item.c3.item'
  // in the ORC lib, then we get offsets(indices) of both the inner and outer arrays.
  while (!pos_slots.empty()) {
    const SlotDescriptor* pos_slot_desc = pos_slots.top();
    pos_slots.pop();
    const orc::Type* array_node = nullptr;
    bool pos_field = false;
    bool missing_field = false;
    RETURN_IF_ERROR(schema_resolver_->ResolveColumn(pos_slot_desc->col_path(),
        &array_node, &pos_field, &missing_field));
    if (HasChildrenSelected(*array_node, selected_type_ids_)) continue;
    selected_type_ids_.push_back(array_node->getMaximumColumnId());
    VLOG(3) << "Add ORC column " << array_node->getMaximumColumnId() << " for "
        << PrintColPath(*scan_node_->hdfs_table(), pos_slot_desc->col_path(),
           schema_resolver_);
    selected_nodes.push_back(array_node);
  }

  // Select "CurrentTransaction" when we need to validate rows.
  if (row_batches_need_validation_) {
    // In case of zero-slot scans (e.g. count(*) over the table) we only select the
    // 'currentTransaction' column.
    if (scan_node_->IsZeroSlotTableScan()) selected_type_ids_.clear();
    if (std::find(selected_type_ids_.begin(), selected_type_ids_.end(),
        CURRENT_TRANSCACTION_TYPE_ID) == selected_type_ids_.end()) {
      selected_type_ids_.push_back(CURRENT_TRANSCACTION_TYPE_ID);
    }
  }

  COUNTER_SET(num_cols_counter_, static_cast<int64_t>(selected_type_ids_.size()));
  row_reader_options_.includeTypes(selected_type_ids_);
  return Status::OK();
}

Status HdfsOrcScanner::ProcessSplit() {
  DCHECK(scan_node_->HasRowBatchQueue());
  HdfsScanNode* scan_node = static_cast<HdfsScanNode*>(scan_node_);
  do {
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

Status HdfsOrcScanner::GetNextInternal(RowBatch* row_batch) {
  // In case 'row_batches_need_validation_' is true, we need to look at the row
  // batches and check their validity. In that case 'currentTransaction' is the only
  // selected field from the file (in case of zero slot scans).
  if (scan_node_->IsZeroSlotTableScan() && !row_batches_need_validation_) {
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

  if (!scratch_batch_->AtEnd()) {
    assemble_rows_timer_.Start();
    int num_row_to_commit = TransferScratchTuples(row_batch);
    assemble_rows_timer_.Stop();
    RETURN_IF_ERROR(CommitRows(num_row_to_commit, row_batch));
    if (row_batch->AtCapacity()) return Status::OK();
  }

  // reset tuple memory. We'll allocate it the first time we use it.
  tuple_mem_ = nullptr;
  tuple_ = nullptr;

  // Transfer remaining values in current orc batch. They are left in the previous call
  // of 'TransferTuples' inside 'AssembleRows'. Since the orc batch has the same capacity
  // as RowBatch's, the remaining values should be drained by one more round of calling
  // 'TransferTuples' here.
  if (!orc_root_reader_->EndOfBatch()) {
    assemble_rows_timer_.Start();
    RETURN_IF_ERROR(TransferTuples(row_batch));
    assemble_rows_timer_.Stop();
    if (row_batch->AtCapacity()) return Status::OK();
    DCHECK(orc_root_reader_->EndOfBatch());
  }

  // Process next stripe if current stripe is drained. Each stripe will generate several
  // orc batches. We only advance the stripe after processing the last batch.
  // 'advance_stripe_' is updated in 'NextStripe', meaning the current stripe we advance
  // to can be skip. 'end_of_stripe_' marks whether current stripe is drained. It's only
  // set to true in 'AssembleRows'.
  while (advance_stripe_ || end_of_stripe_) {
    // The next stripe will use a new dictionary blob so transfer the memory to row_batch.
    row_batch->tuple_data_pool()->AcquireData(dictionary_pool_.get(), false);
    context_->ReleaseCompletedResources(/* done */ true);
    // Commit the rows to flush the row batch from the previous stripe.
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

  bool first_invocation = stripe_idx_ == -1;
  int64_t skipped_rows = 0;

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
      if (first_invocation) skipped_rows += stripe->getNumberOfRows();
      misaligned_stripe_skipped |= CheckStripeOverlapsSplit(stripe_offset,
          stripe_offset + stripe_len, split_offset, split_offset + split_length);
      continue;
    }

    // Set the file row index in 'orc_root_reader_' accordingly.
    if (first_invocation && acid_synthetic_rowid_ != nullptr) {
      orc_root_reader_->SetFileRowIndex(skipped_rows);
    }

    COUNTER_ADD(num_stripes_counter_, 1);
    row_reader_options_.range(stripe->getOffset(), stripe_len);
    try {
      row_reader_ = reader_->createRowReader(row_reader_options_);
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
  bool continue_execution = !scan_node_->ReachedLimitShared() && !context_->cancelled();
  if (!continue_execution) return Status::CancelledInternal("ORC scanner");

  // We're going to free the previous batch. Clear the reference first.
  RETURN_IF_ERROR(orc_root_reader_->UpdateInputBatch(nullptr));

  orc_root_batch_ = row_reader_->createRowBatch(row_batch->capacity());
  DCHECK_EQ(orc_root_batch_->numElements, 0);

  int64_t num_rows_read = 0;
  while (continue_execution) {  // one ORC batch (ColumnVectorBatch) in a round
    if (orc_root_reader_->EndOfBatch()) {
      row_batch->tuple_data_pool()->AcquireData(data_batch_pool_.get(), false);
      try {
        end_of_stripe_ |= !row_reader_->next(*orc_root_batch_);
        RETURN_IF_ERROR(orc_root_reader_->UpdateInputBatch(orc_root_batch_.get()));
        if (end_of_stripe_) break; // no more data to process
      } catch (ResourceError& e) {
        parse_status_ = e.GetStatus();
        return parse_status_;
      } catch (std::exception& e) {
        VLOG_QUERY << "Encounter parse error: " << e.what();
        parse_status_ = Status(Substitute("Encounter parse error: $0.", e.what()));
        eos_ = true;
        return parse_status_;
      }
      if (orc_root_batch_->numElements == 0) {
        RETURN_IF_ERROR(CommitRows(0, row_batch));
        end_of_stripe_ = true;
        return Status::OK();
      }
      num_rows_read += orc_root_batch_->numElements;
    }

    RETURN_IF_ERROR(TransferTuples(row_batch));
    if (row_batch->AtCapacity()) break;
    continue_execution &= !scan_node_->ReachedLimitShared() && !context_->cancelled();
  }
  stripe_rows_read_ += num_rows_read;
  COUNTER_ADD(scan_node_->rows_read_counter(), num_rows_read);
  // Merge Scanner-local counter into HdfsScanNode counter and reset.
  COUNTER_ADD(scan_node_->collection_items_read_counter(), coll_items_read_counter_);
  coll_items_read_counter_ = 0;
  return Status::OK();
}

Status HdfsOrcScanner::TransferTuples(RowBatch* dst_batch) {
  DCHECK_LT(dst_batch->num_rows(), dst_batch->capacity());
  if (tuple_ == nullptr) RETURN_IF_ERROR(AllocateTupleMem(dst_batch));
  int row_id = dst_batch->num_rows();
  int capacity = dst_batch->capacity();

  while (row_id < capacity && !orc_root_reader_->EndOfBatch()) {
    DCHECK(scratch_batch_ != nullptr);
    DCHECK(scratch_batch_->AtEnd());
    RETURN_IF_ERROR(scratch_batch_->Reset(state_));
    InitTupleBuffer(template_tuple_, scratch_batch_->tuple_mem, scratch_batch_->capacity);
    RETURN_IF_ERROR(orc_root_reader_->TopLevelReadValueBatch(scratch_batch_.get(),
        &scratch_batch_->aux_mem_pool));
    int num_tuples_transferred = TransferScratchTuples(dst_batch);
    row_id += num_tuples_transferred;
    VLOG_ROW << Substitute("Transfer $0 rows from scratch batch to dst_batch ($1 rows)",
        num_tuples_transferred, dst_batch->num_rows());
    RETURN_IF_ERROR(CommitRows(num_tuples_transferred, dst_batch));
  }
  return Status::OK();
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

Status HdfsOrcScanner::AssembleCollection(
    const OrcComplexColumnReader& complex_col_reader, int row_idx,
    CollectionValueBuilder* coll_value_builder) {
  int total_tuples = complex_col_reader.GetNumChildValues(row_idx);
  if (!complex_col_reader.MaterializeTuple()) {
    // 'complex_col_reader' maps to a STRUCT or collection column of STRUCTs/collections
    // and there're no need to materialize current level tuples. Delegate the
    // materialization to the unique child reader.
    DCHECK_EQ(complex_col_reader.children().size(), 1);
    DCHECK(complex_col_reader.children()[0]->IsComplexColumnReader());
    auto child_reader = reinterpret_cast<OrcComplexColumnReader*>(
        complex_col_reader.children()[0]);
    // We should give the child reader the boundary (offset and total tuples) of current
    // collection
    int child_batch_offset = complex_col_reader.GetChildBatchOffset(row_idx);
    for (int i = 0; i < total_tuples; ++i) {
      RETURN_IF_ERROR(AssembleCollection(*child_reader, child_batch_offset + i,
          coll_value_builder));
    }
    return Status::OK();
  }
  DCHECK(complex_col_reader.IsCollectionReader());
  auto coll_reader = reinterpret_cast<const OrcCollectionReader*>(&complex_col_reader);

  const TupleDescriptor* tuple_desc = &coll_value_builder->tuple_desc();
  Tuple* template_tuple = template_tuple_map_[tuple_desc];
  const vector<ScalarExprEvaluator*>& evals =
      conjunct_evals_map_[tuple_desc->id()];

  int tuple_idx = 0;
  while (!scan_node_->ReachedLimitShared() && !context_->cancelled()
      && tuple_idx < total_tuples) {
    MemPool* pool;
    Tuple* tuple;
    TupleRow* row = nullptr;

    int64_t num_rows;
    // We're assembling item tuples into an CollectionValue
    RETURN_IF_ERROR(
        GetCollectionMemory(coll_value_builder, &pool, &tuple, &row, &num_rows));
    // 'num_rows' can be very high if we're writing to a large CollectionValue. Limit
    // the number of rows we read at one time so we don't spend too long in the
    // 'num_rows' loop below before checking for cancellation or limit reached.
    num_rows = min(
        num_rows, static_cast<int64_t>(scan_node_->runtime_state()->batch_size()));
    int num_to_commit = 0;
    while (num_to_commit < num_rows && tuple_idx < total_tuples) {
      InitTuple(tuple_desc, template_tuple, tuple);
      RETURN_IF_ERROR(coll_reader->ReadChildrenValue(row_idx, tuple_idx++, tuple, pool));
      if (ExecNode::EvalConjuncts(evals.data(), evals.size(), row)) {
        tuple = next_tuple(tuple_desc->byte_size(), tuple);
        ++num_to_commit;
      }
    }
    coll_value_builder->CommitTuples(num_to_commit);
  }
  coll_items_read_counter_ += tuple_idx;
  return Status::OK();
}

/// T is the intended ORC primitive type and U is Impala internal primitive type.
template<typename T, typename U>
orc::Literal HdfsOrcScanner::GetOrcPrimitiveLiteral(
    orc::PredicateDataType predicate_type, void* val) {
  if (UNLIKELY(!val)) return orc::Literal(predicate_type);
  T* val_dst = reinterpret_cast<T*>(val);
  return orc::Literal(static_cast<U>(*val_dst));
}

orc::PredicateDataType HdfsOrcScanner::GetOrcPredicateDataType(const ColumnType& type) {
  switch (type.type) {
    case TYPE_BOOLEAN:
      return orc::PredicateDataType::BOOLEAN;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
      return orc::PredicateDataType::LONG;
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      return orc::PredicateDataType::FLOAT;
    case TYPE_TIMESTAMP:
      return orc::PredicateDataType::TIMESTAMP;
    case TYPE_DATE:
      return orc::PredicateDataType::DATE;
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE:
      return orc::PredicateDataType::STRING;
    case TYPE_DECIMAL:
      return orc::PredicateDataType::DECIMAL;
    default:
      DCHECK(false) << "Unsupported type: " << type.DebugString();
      return orc::PredicateDataType::LONG;
  }
}

orc::Literal HdfsOrcScanner::GetSearchArgumentLiteral(ScalarExprEvaluator* eval,
    const ColumnType& dst_type, orc::PredicateDataType* predicate_type) {
  DCHECK(eval->root().GetNumChildren() == 2);
  ScalarExpr* literal_expr = eval->root().GetChild(1);
  const ColumnType& type = literal_expr->type();
  DCHECK(literal_expr->IsLiteral());
  *predicate_type = GetOrcPredicateDataType(type);
  // Since we want to get a literal value, the second parameter below is not used.
  void* val = eval->GetValue(*literal_expr, nullptr);

  switch (type.type) {
    case TYPE_BOOLEAN:
      return GetOrcPrimitiveLiteral<bool, bool>(*predicate_type, val);
    case TYPE_TINYINT:
      return GetOrcPrimitiveLiteral<int8_t, int64_t>(*predicate_type, val);
    case TYPE_SMALLINT:
      return GetOrcPrimitiveLiteral<int16_t, int64_t>(*predicate_type, val);
    case TYPE_INT:
      return GetOrcPrimitiveLiteral<int32_t, int64_t>(*predicate_type, val);
    case TYPE_BIGINT:
      return GetOrcPrimitiveLiteral<int64_t, int64_t>(*predicate_type, val);
    case TYPE_FLOAT:
      return GetOrcPrimitiveLiteral<float, double>(*predicate_type, val);
    case TYPE_DOUBLE:
      return GetOrcPrimitiveLiteral<double, double>(*predicate_type, val);
    // Predicates on Timestamp are currently skipped in FE. We will focus on them in
    // IMPALA-10915.
    case TYPE_TIMESTAMP: {
      DCHECK(false) << "Timestamp predicate is not supported: IMPALA-10915";
      return orc::Literal(predicate_type);
    }
    case TYPE_DATE: {
      if (UNLIKELY(!val)) return orc::Literal(predicate_type);
      const DateValue* dv = reinterpret_cast<const DateValue*>(val);
      int32_t value = 0;
      // The date should be valid at this point.
      DCHECK(dv->ToDaysSinceEpoch(&value));
      return orc::Literal(*predicate_type, value);
    }
    case TYPE_STRING: {
      if (UNLIKELY(!val)) return orc::Literal(predicate_type);
      const StringValue* sv = reinterpret_cast<StringValue*>(val);
      return orc::Literal(sv->ptr, sv->len);
    }
    // Predicates on CHAR/VARCHAR are currently skipped in FE. We will focus on them in
    // IMPALA-10882.
    case TYPE_VARCHAR: {
      DCHECK(false) << "Varchar predicate is not supported: IMPALA-10882";
      return orc::Literal(predicate_type);
    }
    case TYPE_CHAR: {
      DCHECK(false) << "Char predicate is not supported: IMPALA-10882";
      if (UNLIKELY(!val)) return orc::Literal(predicate_type);
      const StringValue* sv = reinterpret_cast<StringValue*>(val);
      char* dst_ptr;
      if (dst_type.len > sv->len) {
        dst_ptr = reinterpret_cast<char*>(search_args_pool_->TryAllocate(dst_type.len));
        if (dst_ptr == nullptr) return orc::Literal(predicate_type);
        memcpy(dst_ptr, sv->ptr, sv->len);
        StringValue::PadWithSpaces(dst_ptr, dst_type.len, sv->len);
      } else {
        dst_ptr = sv->ptr;
      }
      return orc::Literal(dst_ptr, sv->len);
    }
    case TYPE_DECIMAL: {
      if (!val) return orc::Literal(predicate_type);
      orc::Int128 value;
      switch (type.GetByteSize()) {
        case 4: {
          Decimal4Value* dv4 = reinterpret_cast<Decimal4Value*>(val);
          value = orc::Int128(dv4->value());
          break;
        }
        case 8: {
          Decimal8Value* dv8 = reinterpret_cast<Decimal8Value*>(val);
          value = orc::Int128(dv8->value());
          break;
        }
        case 16: {
          Decimal16Value* dv16 = reinterpret_cast<Decimal16Value*>(val);
          value = orc::Int128(static_cast<int64_t>(dv16->value() >> 64), // higher bits
              static_cast<uint64_t>(dv16->value())); // lower bits
          break;
        }
        default:
          DCHECK(false) << "Invalid byte size for decimal type: " << type.GetByteSize();
      }
      return orc::Literal(value, type.precision, type.scale);
    }
    default:
      DCHECK(false) << "Invalid type";
      return orc::Literal(orc::PredicateDataType::BOOLEAN);
  }
}

Status HdfsOrcScanner::PrepareSearchArguments() {
  if (!state_->query_options().orc_read_statistics) return Status::OK();

  const TupleDescriptor* min_max_tuple_desc = scan_node_->min_max_tuple_desc();
  if (!min_max_tuple_desc) return Status::OK();
  // TODO(IMPALA-10894): pushing down predicates into the ORC reader will mess up the
  //  synthetic(fake) row id, because the row index in the returned batch might differ
  //  from the index in file (due to some rows are skipped).
  if (acid_synthetic_rowid_ != nullptr) {
    VLOG_FILE << "Skip pushing down predicates on non-ACID ORC files under an ACID "
                 "table: " << filename();
    return Status::OK();
  }

  // Clone the min/max statistics conjuncts.
  RETURN_IF_ERROR(ScalarExprEvaluator::Clone(&obj_pool_, state_,
      expr_perm_pool_.get(), context_->expr_results_pool(),
      scan_node_->min_max_conjunct_evals(), &min_max_conjunct_evals_));

  std::unique_ptr<orc::SearchArgumentBuilder> sarg =
      orc::SearchArgumentFactory::newBuilder();
  bool sargs_supported = false;

  DCHECK_EQ(min_max_tuple_desc->slots().size(), min_max_conjunct_evals_.size());
  for (int i = 0; i < min_max_conjunct_evals_.size(); ++i) {
    SlotDescriptor* slot_desc = min_max_tuple_desc->slots()[i];
    ScalarExprEvaluator* eval = min_max_conjunct_evals_[i];
    ScalarExpr* const_expr = eval->root().GetChild(1);
    // ORC reader only supports pushing down predicates that constant parts are literal.
    // We could get non-literal expr if expr rewrites are disabled.
    if (!const_expr->IsLiteral()) continue;
    // TODO(IMPALA-10882): push down min-max predicates on CHAR/VARCHAR.
    if (const_expr->type().type == TYPE_CHAR || const_expr->type().type == TYPE_VARCHAR
        || slot_desc->type().type == TYPE_CHAR
        || slot_desc->type().type == TYPE_VARCHAR) {
      continue;
    }
    // TODO(IMPALA-10916): dealing with lhs that is a simple cast expr.
    if (GetOrcPredicateDataType(slot_desc->type()) !=
        GetOrcPredicateDataType(const_expr->type())) {
      continue;
    }

    //Resolve column path to determine col idx.
    const orc::Type* node = nullptr;
    bool pos_field;
    bool missing_field;
    RETURN_IF_ERROR(schema_resolver_->ResolveColumn(slot_desc->col_path(),
       &node, &pos_field, &missing_field));

    if (pos_field || missing_field) continue;
    // TODO(IMPALA-10882): push down min-max predicates on CHAR/VARCHAR.
    if (node->getKind() == orc::CHAR || node->getKind() == orc::VARCHAR) continue;

    const string& fn_name = eval->root().function_name();
    orc::PredicateDataType predicate_type;
    orc::Literal literal =
        GetSearchArgumentLiteral(eval, slot_desc->type(), &predicate_type);
    if (literal.isNull()) {
      VLOG_QUERY << "Failed to push down predicate " << eval->root().DebugString();
      continue;
    }

    if (fn_name == "lt") {
      sarg->lessThan(node->getColumnId(), predicate_type, literal);
    } else if (fn_name == "gt") {
      sarg->startNot()
          .lessThanEquals(node->getColumnId(), predicate_type, literal)
          .end();
    } else if (fn_name == "le") {
      sarg->lessThanEquals(node->getColumnId(), predicate_type, literal);
    } else if (fn_name == "ge") {
      sarg->startNot()
          .lessThan(node->getColumnId(), predicate_type, literal)
          .end();
    } else {
      DCHECK(false) << "Invalid predicate: " << fn_name;
      continue;
    }
    // If we have reached this far, we have a valid search arg that we can build later.
    sargs_supported = true;
  }
  if (sargs_supported) {
    try {
      std::unique_ptr<orc::SearchArgument> final_sarg = sarg->build();
      VLOG_FILE << "Built search arguments for ORC file: " << filename() << ": "
          << final_sarg->toString() << ". File schema: " << reader_->getType().toString();
      row_reader_options_.searchArgument(std::move(final_sarg));
    } catch (std::exception& e) {
      string msg = Substitute("Encountered parse error during building search arguments "
          "in ORC file $0: $1", filename(), e.what());
      parse_status_ = Status(msg);
      return parse_status_;
    }
  }
  // Free any expr result allocations accumulated during conjunct evaluation.
  context_->expr_results_pool()->Clear();
  return Status::OK();
}

}
