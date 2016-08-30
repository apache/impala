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

#include "exec/hdfs-scan-node.h"
#include "exec/base-sequence-scanner.h"
#include "exec/hdfs-text-scanner.h"
#include "exec/hdfs-lzo-text-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-avro-scanner.h"
#include "exec/hdfs-parquet-scanner.h"

#include <sstream>
#include <avro/errors.h>
#include <avro/schema.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include <hdfs.h>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exprs/expr-context.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/string-buffer.h"
#include "scheduling/query-resource-mgr.h"
#include "util/bit-util.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/error-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

DEFINE_int32(max_row_batches, 0, "the maximum size of materialized_row_batches_");
DEFINE_bool(suppress_unknown_disk_id_warnings, false,
    "Suppress unknown disk id warnings generated when the HDFS implementation does not"
    " provide volume/disk information.");
DEFINE_int32(runtime_filter_wait_time_ms, 1000, "(Advanced) the maximum time, in ms, "
    "that a scan node will wait for expected runtime filters to arrive.");

#ifndef NDEBUG
DECLARE_bool(skip_file_runtime_filtering);
#endif

namespace filesystem = boost::filesystem;
using namespace impala;
using namespace llvm;
using namespace strings;
using boost::algorithm::join;

const string HdfsScanNode::HDFS_SPLIT_STATS_DESC =
    "Hdfs split stats (<volume id>:<# splits>/<split lengths>)";

// Amount of memory that we approximate a scanner thread will use not including IoBuffers.
// The memory used does not vary considerably between file formats (just a couple of MBs).
// This value is conservative and taken from running against the tpch lineitem table.
// TODO: revisit how we do this.
const int SCANNER_THREAD_MEM_USAGE = 32 * 1024 * 1024;

// Estimated upper bound on the compression ratio of compressed text files. Used to
// estimate scanner thread memory usage.
const int COMPRESSED_TEXT_COMPRESSION_RATIO = 11;

// Determines how many unexpected remote bytes trigger an error in the runtime state
const int UNEXPECTED_REMOTE_BYTES_WARN_THRESHOLD = 64 * 1024 * 1024;

// Amount of time to block waiting for GetNext() to release scanner threads between
// checking if a scanner thread should yield itself back to the global thread pool.
const int SCANNER_THREAD_WAIT_TIME_MS = 20;

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      runtime_state_(NULL),
      skip_header_line_count_(tnode.hdfs_scan_node.__isset.skip_header_line_count ?
          tnode.hdfs_scan_node.skip_header_line_count : 0),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      reader_context_(NULL),
      tuple_desc_(NULL),
      hdfs_table_(NULL),
      unknown_disk_id_warned_(false),
      initial_ranges_issued_(false),
      ranges_issued_barrier_(1),
      scanner_thread_bytes_required_(0),
      max_compressed_text_file_length_(NULL),
      disks_accessed_bitmap_(TUnit::UNIT, 0),
      bytes_read_local_(NULL),
      bytes_read_short_circuit_(NULL),
      bytes_read_dn_cache_(NULL),
      num_remote_ranges_(NULL),
      unexpected_remote_bytes_(NULL),
      done_(false),
      all_ranges_started_(false),
      counters_running_(false),
      thread_avail_cb_id_(-1),
      rm_callback_id_(-1),
      max_num_scanner_threads_(CpuInfo::num_cores()) {
  max_materialized_row_batches_ = FLAGS_max_row_batches;
  if (max_materialized_row_batches_ <= 0) {
    // TODO: This parameter has an U-shaped effect on performance: increasing the value
    // would first improve performance, but further increasing would degrade performance.
    // Investigate and tune this.
    max_materialized_row_batches_ =
        10 * (DiskInfo::num_disks() + DiskIoMgr::REMOTE_NUM_DISKS);
  }
  materialized_row_batches_.reset(new RowBatchQueue(max_materialized_row_batches_));
}

HdfsScanNode::~HdfsScanNode() {
}

Status HdfsScanNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));

  // Add collection item conjuncts
  const map<TTupleId, vector<TExpr>>& collection_conjuncts =
      tnode.hdfs_scan_node.collection_conjuncts;
  map<TTupleId, vector<TExpr>>::const_iterator iter = collection_conjuncts.begin();
  for (; iter != collection_conjuncts.end(); ++iter) {
    DCHECK(conjuncts_map_[iter->first].empty());
    RETURN_IF_ERROR(
        Expr::CreateExprTrees(pool_, iter->second, &conjuncts_map_[iter->first]));
  }

  const TQueryOptions& query_options = state->query_options();
  for (const TRuntimeFilterDesc& filter: tnode.runtime_filters) {
    auto it = filter.planid_to_target_ndx.find(tnode.node_id);
    DCHECK(it != filter.planid_to_target_ndx.end());
    const TRuntimeFilterTargetDesc& target = filter.targets[it->second];
    if (state->query_options().runtime_filter_mode == TRuntimeFilterMode::LOCAL &&
        !target.is_local_target) {
      continue;
    }
    if (query_options.disable_row_runtime_filtering &&
        !target.is_bound_by_partition_columns) {
      continue;
    }

    FilterContext filter_ctx;
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, target.target_expr, &filter_ctx.expr));
    filter_ctx.filter = state->filter_bank()->RegisterFilter(filter, false);

    string filter_profile_title = Substitute("Filter $0 ($1)", filter.filter_id,
        PrettyPrinter::Print(filter_ctx.filter->filter_size(), TUnit::BYTES));
    RuntimeProfile* profile = state->obj_pool()->Add(
        new RuntimeProfile(state->obj_pool(), filter_profile_title));
    runtime_profile_->AddChild(profile);
    filter_ctx.stats = state->obj_pool()->Add(new FilterStats(profile,
        target.is_bound_by_partition_columns));

    filter_ctxs_.push_back(filter_ctx);
  }

  // Add row batch conjuncts
  DCHECK(conjuncts_map_[tuple_id_].empty());
  conjuncts_map_[tuple_id_] = conjunct_ctxs_;

  return Status::OK();
}

bool HdfsScanNode::FilePassesFilterPredicates(const vector<FilterContext>& filter_ctxs,
    const THdfsFileFormat::type& format, HdfsFileDesc* file) {
#ifndef NDEBUG
  if (FLAGS_skip_file_runtime_filtering) return true;
#endif
  if (filter_ctxs_.size() == 0) return true;
  ScanRangeMetadata* metadata =
      reinterpret_cast<ScanRangeMetadata*>(file->splits[0]->meta_data());
  if (!PartitionPassesFilters(metadata->partition_id, FilterStats::FILES_KEY,
          filter_ctxs)) {
    for (int j = 0; j < file->splits.size(); ++j) {
      // Mark range as complete to ensure progress.
      RangeComplete(format, file->file_compression);
    }
    return false;
  }
  return true;
}

bool HdfsScanNode::WaitForRuntimeFilters(int32_t time_ms) {
  vector<string> arrived_filter_ids;
  int32_t start = MonotonicMillis();
  for (auto& ctx: filter_ctxs_) {
    if (ctx.filter->WaitForArrival(time_ms)) {
      arrived_filter_ids.push_back(Substitute("$0", ctx.filter->id()));
    }
  }
  int32_t end = MonotonicMillis();
  const string& wait_time = PrettyPrinter::Print(end - start, TUnit::TIME_MS);

  if (arrived_filter_ids.size() == filter_ctxs_.size()) {
    runtime_profile()->AddInfoString("Runtime filters",
        Substitute("All filters arrived. Waited $0", wait_time));
    VLOG_QUERY << "Filters arrived. Waited " << wait_time;
    return true;
  }

  const string& filter_str = Substitute("Only following filters arrived: $0, waited $1",
      join(arrived_filter_ids, ", "), wait_time);
  runtime_profile()->AddInfoString("Runtime filters", filter_str);
  VLOG_QUERY << filter_str;
  return false;
}

Status HdfsScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  if (!initial_ranges_issued_) {
    // We do this in GetNext() to maximise the amount of work we can do while waiting for
    // runtime filters to show up. The scanner threads have already started (in Open()),
    // so we need to tell them there is work to do.
    // TODO: This is probably not worth splitting the organisational cost of splitting
    // initialisation across two places. Move to before the scanner threads start.
    initial_ranges_issued_ = true;

    int32 wait_time_ms = FLAGS_runtime_filter_wait_time_ms;
    if (state->query_options().runtime_filter_wait_time_ms > 0) {
      wait_time_ms = state->query_options().runtime_filter_wait_time_ms;
    }
    if (filter_ctxs_.size() > 0) WaitForRuntimeFilters(wait_time_ms);
    // Apply dynamic partition-pruning per-file.
    FileFormatsMap matching_per_type_files;
    for (const FileFormatsMap::value_type& v: per_type_files_) {
      vector<HdfsFileDesc*>* matching_files = &matching_per_type_files[v.first];
      for (HdfsFileDesc* file: v.second) {
        if (FilePassesFilterPredicates(filter_ctxs_, v.first, file)) {
          matching_files->push_back(file);
        }
      }
    }

    // Issue initial ranges for all file types.
    RETURN_IF_ERROR(HdfsParquetScanner::IssueInitialRanges(this,
        matching_per_type_files[THdfsFileFormat::PARQUET]));
    RETURN_IF_ERROR(HdfsTextScanner::IssueInitialRanges(this,
        matching_per_type_files[THdfsFileFormat::TEXT]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        matching_per_type_files[THdfsFileFormat::SEQUENCE_FILE]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        matching_per_type_files[THdfsFileFormat::RC_FILE]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        matching_per_type_files[THdfsFileFormat::AVRO]));

    // Release the scanner threads
    ranges_issued_barrier_.Notify();

    if (progress_.done()) SetDone();
  }

  Status status = GetNextInternal(state, row_batch, eos);
  if (!status.ok() || *eos) StopAndFinalizeCounters();
  return status;
}

Status HdfsScanNode::GetNextInternal(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimit()) {
    // LIMIT 0 case.  Other limit values handled below.
    DCHECK_EQ(limit_, 0);
    *eos = true;
    return Status::OK();
  }
  *eos = false;
  RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    num_owned_io_buffers_.Add(-materialized_batch->num_io_buffers());
    row_batch->AcquireState(materialized_batch);
    // Update the number of materialized rows now instead of when they are materialized.
    // This means that scanners might process and queue up more rows than are necessary
    // for the limit case but we want to avoid the synchronized writes to
    // num_rows_returned_.
    num_rows_returned_ += row_batch->num_rows();
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit()) {
      int num_rows_over = num_rows_returned_ - limit_;
      row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
      num_rows_returned_ -= num_rows_over;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);

      *eos = true;
      SetDone();
    }
    DCHECK_EQ(materialized_batch->num_io_buffers(), 0);
    delete materialized_batch;
    return Status::OK();
  }
  // The RowBatchQueue was shutdown either because all scan ranges are complete or a
  // scanner thread encountered an error.  Check status_ to distinguish those cases.
  *eos = true;
  unique_lock<mutex> l(lock_);
  return status_;
}

DiskIoMgr::ScanRange* HdfsScanNode::AllocateScanRange(
    hdfsFS fs, const char* file, int64_t len, int64_t offset, int64_t partition_id,
    int disk_id, bool try_cache, bool expected_local, int64_t mtime,
    const DiskIoMgr::ScanRange* original_split) {
  DCHECK_GE(disk_id, -1);
  // Require that the scan range is within [0, file_length). While this cannot be used
  // to guarantee safety (file_length metadata may be stale), it avoids different
  // behavior between Hadoop FileSystems (e.g. s3n hdfsSeek() returns error when seeking
  // beyond the end of the file).
  DCHECK_GE(offset, 0);
  DCHECK_GE(len, 0);
  DCHECK_LE(offset + len, GetFileDesc(file)->file_length)
      << "Scan range beyond end of file (offset=" << offset << ", len=" << len << ")";
  disk_id = runtime_state_->io_mgr()->AssignQueue(file, disk_id, expected_local);

  ScanRangeMetadata* metadata = runtime_state_->obj_pool()->Add(
        new ScanRangeMetadata(partition_id, original_split));
  DiskIoMgr::ScanRange* range =
      runtime_state_->obj_pool()->Add(new DiskIoMgr::ScanRange());
  range->Reset(fs, file, len, offset, disk_id, try_cache, expected_local,
      mtime, metadata);
  return range;
}

HdfsFileDesc* HdfsScanNode::GetFileDesc(const string& filename) {
  DCHECK(file_descs_.find(filename) != file_descs_.end());
  return file_descs_[filename];
}

void HdfsScanNode::SetFileMetadata(const string& filename, void* metadata) {
  unique_lock<mutex> l(metadata_lock_);
  DCHECK(per_file_metadata_.find(filename) == per_file_metadata_.end());
  per_file_metadata_[filename] = metadata;
}

void* HdfsScanNode::GetFileMetadata(const string& filename) {
  unique_lock<mutex> l(metadata_lock_);
  map<string, void*>::iterator it = per_file_metadata_.find(filename);
  if (it == per_file_metadata_.end()) return NULL;
  return it->second;
}

void* HdfsScanNode::GetCodegenFn(THdfsFileFormat::type type) {
  CodegendFnMap::iterator it = codegend_fn_map_.find(type);
  if (it == codegend_fn_map_.end()) return NULL;
  return it->second;
}

Status HdfsScanNode::CreateAndOpenScanner(HdfsPartitionDescriptor* partition,
    ScannerContext* context, scoped_ptr<HdfsScanner>* scanner) {
  DCHECK(context != NULL);
  THdfsCompression::type compression =
      context->GetStream()->file_desc()->file_compression;

  // Create a new scanner for this file format and compression.
  switch (partition->file_format()) {
    case THdfsFileFormat::TEXT:
      // Lzo-compressed text files are scanned by a scanner that it is implemented as a
      // dynamic library, so that Impala does not include GPL code.
      if (compression == THdfsCompression::LZO) {
        scanner->reset(HdfsLzoTextScanner::GetHdfsLzoTextScanner(this, runtime_state_));
      } else {
        scanner->reset(new HdfsTextScanner(this, runtime_state_, true));
      }
      break;
    case THdfsFileFormat::SEQUENCE_FILE:
      scanner->reset(new HdfsSequenceScanner(this, runtime_state_, true));
      break;
    case THdfsFileFormat::RC_FILE:
      scanner->reset(new HdfsRCFileScanner(this, runtime_state_, true));
      break;
    case THdfsFileFormat::AVRO:
      scanner->reset(new HdfsAvroScanner(this, runtime_state_, true));
      break;
    case THdfsFileFormat::PARQUET:
      scanner->reset(new HdfsParquetScanner(this, runtime_state_, true));
      break;
    default:
      return Status(Substitute("Unknown Hdfs file format type: $0",
          partition->file_format()));
  }
  DCHECK(scanner->get() != NULL);
  Status status = ExecDebugAction(TExecNodePhase::PREPARE_SCANNER, runtime_state_);
  if (status.ok()) {
    status = scanner->get()->Open(context);
    if (!status.ok()) scanner->get()->Close(scanner->get()->batch());
  } else {
    context->ClearStreams();
  }
  return status;
}

Tuple* HdfsScanNode::InitTemplateTuple(RuntimeState* state,
    const vector<ExprContext*>& value_ctxs) {
  if (partition_key_slots_.empty()) return NULL;

  // Lock to protect access to partition_key_pool_ and value_ctxs
  // TODO: we can push the lock to the mempool and exprs_values should not
  // use internal memory.
  Tuple* template_tuple = InitEmptyTemplateTuple(*tuple_desc_);

  unique_lock<mutex> l(lock_);
  for (int i = 0; i < partition_key_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = partition_key_slots_[i];
    // Exprs guaranteed to be literals, so can safely be evaluated without a row context
    void* value = value_ctxs[slot_desc->col_pos()]->GetValue(NULL);
    RawValue::Write(value, template_tuple, slot_desc, NULL);
  }
  return template_tuple;
}

Tuple* HdfsScanNode::InitEmptyTemplateTuple(const TupleDescriptor& tuple_desc) {
  Tuple* template_tuple = NULL;
  {
    unique_lock<mutex> l(lock_);
    template_tuple = Tuple::Create(tuple_desc.byte_size(), scan_node_pool_.get());
  }
  memset(template_tuple, 0, tuple_desc.byte_size());
  return template_tuple;
}

void HdfsScanNode::TransferToScanNodePool(MemPool* pool) {
  unique_lock<mutex> l(lock_);
  scan_node_pool_->AcquireData(pool, false);
}

Status HdfsScanNode::TriggerDebugAction() {
  return ExecDebugAction(TExecNodePhase::GETNEXT, runtime_state_);
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  runtime_state_ = state;
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);

  // Prepare collection conjuncts
  ConjunctsMap::const_iterator iter = conjuncts_map_.begin();
  for (; iter != conjuncts_map_.end(); ++iter) {
    TupleDescriptor* tuple_desc = state->desc_tbl().GetTupleDescriptor(iter->first);

    // conjuncts_ are already prepared in ExecNode::Prepare(), don't try to prepare again
    if (tuple_desc == tuple_desc_) continue;

    RowDescriptor* collection_row_desc =
        state->obj_pool()->Add(new RowDescriptor(tuple_desc, /* is_nullable */ false));
    RETURN_IF_ERROR(
        Expr::Prepare(iter->second, state, *collection_row_desc, expr_mem_tracker()));
  }

  if (!state->cgroup().empty()) {
    scanner_threads_.SetCgroupsMgr(state->exec_env()->cgroups_mgr());
    scanner_threads_.SetCgroup(state->cgroup());
  }

  // One-time initialisation of state that is constant across scan ranges
  DCHECK(tuple_desc_->table_desc() != NULL);
  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
  scan_node_pool_.reset(new MemPool(mem_tracker()));

  for (int i = 0; i < filter_ctxs_.size(); ++i) {
    RETURN_IF_ERROR(
        filter_ctxs_[i].expr->Prepare(state, row_desc(), expr_mem_tracker()));
    AddExprCtxToFree(filter_ctxs_[i].expr);
  }

  // Parse Avro table schema if applicable
  const string& avro_schema_str = hdfs_table_->avro_schema();
  if (!avro_schema_str.empty()) {
    avro_schema_t avro_schema;
    int error = avro_schema_from_json_length(
        avro_schema_str.c_str(), avro_schema_str.size(), &avro_schema);
    if (error != 0) {
      return Status(Substitute("Failed to parse table schema: $0", avro_strerror()));
    }
    RETURN_IF_ERROR(AvroSchemaElement::ConvertSchema(avro_schema, avro_schema_.get()));
  }

  // Gather materialized partition-key slots and non-partition slots.
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (size_t i = 0; i < slots.size(); ++i) {
    if (hdfs_table_->IsClusteringCol(slots[i])) {
      partition_key_slots_.push_back(slots[i]);
    } else {
      materialized_slots_.push_back(slots[i]);
    }
  }

  // Order the materialized slots such that for schemaless file formats (e.g. text) the
  // order corresponds to the physical order in files. For formats where the file schema
  // is independent of the table schema (e.g. Avro, Parquet), this step is not necessary.
  sort(materialized_slots_.begin(), materialized_slots_.end(),
      SlotDescriptor::ColPathLessThan);

  // Populate mapping from slot path to index into materialized_slots_.
  for (int i = 0; i < materialized_slots_.size(); ++i) {
    path_to_materialized_slot_idx_[materialized_slots_[i]->col_path()] = i;
  }

  // Initialize is_materialized_col_
  is_materialized_col_.resize(hdfs_table_->num_cols());
  for (int i = 0; i < hdfs_table_->num_cols(); ++i) {
    is_materialized_col_[i] = GetMaterializedSlotIdx(vector<int>(1, i)) != SKIP_COLUMN;
  }

  HdfsFsCache::HdfsFsMap fs_cache;
  // Convert the TScanRangeParams into per-file DiskIO::ScanRange objects and populate
  // partition_ids_, file_descs_, and per_type_files_.
  DCHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";
  int num_ranges_missing_volume_id = 0;
  for (int i = 0; i < scan_range_params_->size(); ++i) {
    DCHECK((*scan_range_params_)[i].scan_range.__isset.hdfs_file_split);
    const THdfsFileSplit& split = (*scan_range_params_)[i].scan_range.hdfs_file_split;
    partition_ids_.insert(split.partition_id);
    HdfsPartitionDescriptor* partition_desc =
        hdfs_table_->GetPartition(split.partition_id);
    if (partition_desc == NULL) {
      // TODO: this should be a DCHECK but we sometimes hit it. It's likely IMPALA-1702.
      LOG(ERROR) << "Bad table descriptor! table_id=" << hdfs_table_->id()
                 << " partition_id=" << split.partition_id
                 << "\n" << PrintThrift(state->fragment_params());
      return Status("Query encountered invalid metadata, likely due to IMPALA-1702."
                    " Try rerunning the query.");
    }

    if (partition_template_tuple_map_.find(split.partition_id) ==
        partition_template_tuple_map_.end()) {
      partition_template_tuple_map_[split.partition_id] =
          InitTemplateTuple(state, partition_desc->partition_key_value_ctxs());;
    }

    filesystem::path file_path(partition_desc->location());
    file_path.append(split.file_name, filesystem::path::codecvt());
    const string& native_file_path = file_path.native();

    HdfsFileDesc* file_desc = NULL;
    FileDescMap::iterator file_desc_it = file_descs_.find(native_file_path);
    if (file_desc_it == file_descs_.end()) {
      // Add new file_desc to file_descs_ and per_type_files_
      file_desc = runtime_state_->obj_pool()->Add(new HdfsFileDesc(native_file_path));
      file_descs_[native_file_path] = file_desc;
      file_desc->file_length = split.file_length;
      file_desc->mtime = split.mtime;
      file_desc->file_compression = split.file_compression;
      RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
          native_file_path, &file_desc->fs, &fs_cache));
      num_unqueued_files_.Add(1);
      per_type_files_[partition_desc->file_format()].push_back(file_desc);
    } else {
      // File already processed
      file_desc = file_desc_it->second;
    }

    bool expected_local = (*scan_range_params_)[i].__isset.is_remote &&
        !(*scan_range_params_)[i].is_remote;
    if (expected_local && (*scan_range_params_)[i].volume_id == -1) {
      if (!FLAGS_suppress_unknown_disk_id_warnings && !unknown_disk_id_warned_) {
        AddRuntimeExecOption("Missing Volume Id");
        runtime_state()->LogError(ErrorMsg(TErrorCode::HDFS_SCAN_NODE_UNKNOWN_DISK));
        unknown_disk_id_warned_ = true;
      }
      ++num_ranges_missing_volume_id;
    }

    bool try_cache = (*scan_range_params_)[i].is_cached;
    if (runtime_state_->query_options().disable_cached_reads) {
      DCHECK(!try_cache) << "Params should not have had this set.";
    }
    file_desc->splits.push_back(
        AllocateScanRange(file_desc->fs, file_desc->filename.c_str(), split.length,
            split.offset, split.partition_id, (*scan_range_params_)[i].volume_id,
            try_cache, expected_local, file_desc->mtime));
  }

  // Compute the minimum bytes required to start a new thread. This is based on the
  // file format.
  // The higher the estimate, the less likely it is the query will fail but more likely
  // the query will be throttled when it does not need to be.
  // TODO: how many buffers should we estimate per range. The IoMgr will throttle down to
  // one but if there are already buffers queued before memory pressure was hit, we can't
  // reclaim that memory.
  if (per_type_files_[THdfsFileFormat::PARQUET].size() > 0) {
    // Parquet files require buffers per column
    scanner_thread_bytes_required_ =
        materialized_slots_.size() * 3 * runtime_state_->io_mgr()->max_read_buffer_size();
  } else {
    scanner_thread_bytes_required_ =
        3 * runtime_state_->io_mgr()->max_read_buffer_size();
  }
  // scanner_thread_bytes_required_ now contains the IoBuffer requirement.
  // Next we add in the other memory the scanner thread will use.
  // e.g. decompression buffers, tuple buffers, etc.
  // For compressed text, we estimate this based on the file size (since the whole file
  // will need to be decompressed at once). For all other formats, we use a constant.
  // TODO: can we do something better?
  int64_t scanner_thread_mem_usage = SCANNER_THREAD_MEM_USAGE;
  for (HdfsFileDesc* file: per_type_files_[THdfsFileFormat::TEXT]) {
    if (file->file_compression != THdfsCompression::NONE) {
      int64_t bytes_required = file->file_length * COMPRESSED_TEXT_COMPRESSION_RATIO;
      scanner_thread_mem_usage = ::max(bytes_required, scanner_thread_mem_usage);
    }
  }
  scanner_thread_bytes_required_ += scanner_thread_mem_usage;

  // Prepare all the partitions scanned by the scan node
  for (int64_t partition_id: partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    // This is IMPALA-1702, but will have been caught earlier in this method.
    DCHECK(partition_desc != NULL) << "table_id=" << hdfs_table_->id()
                                   << " partition_id=" << partition_id
                                   << "\n" << PrintThrift(state->fragment_params());
    RETURN_IF_ERROR(partition_desc->PrepareExprs(state));
  }

  // Update server wide metrics for number of scan ranges and ranges that have
  // incomplete metadata.
  ImpaladMetrics::NUM_RANGES_PROCESSED->Increment(scan_range_params_->size());
  ImpaladMetrics::NUM_RANGES_MISSING_VOLUME_ID->Increment(num_ranges_missing_volume_id);

  // Add per volume stats to the runtime profile
  PerVolumnStats per_volume_stats;
  stringstream str;
  UpdateHdfsSplitStats(*scan_range_params_, &per_volume_stats);
  PrintHdfsSplitStats(per_volume_stats, &str);
  runtime_profile()->AddInfoString(HDFS_SPLIT_STATS_DESC, str.str());

  // Create codegen'd functions
  for (int format = THdfsFileFormat::TEXT;
       format <= THdfsFileFormat::PARQUET; ++format) {
    vector<HdfsFileDesc*>& file_descs =
        per_type_files_[static_cast<THdfsFileFormat::type>(format)];

    if (file_descs.empty()) continue;

    // Randomize the order this node processes the files. We want to do this to avoid
    // issuing remote reads to the same DN from different impalads. In file formats such
    // as avro/seq/rc (i.e. splittable with a header), every node first reads the header.
    // If every node goes through the files in the same order, all the remote reads are
    // for the same file meaning a few DN serves a lot of remote reads at the same time.
    random_shuffle(file_descs.begin(), file_descs.end());

    // Create reusable codegen'd functions for each file type type needed
    // TODO: do this for conjuncts_map_
    Function* fn;
    Status status;
    switch (format) {
      case THdfsFileFormat::TEXT:
        status = HdfsTextScanner::Codegen(this, conjunct_ctxs_, &fn);
        break;
      case THdfsFileFormat::SEQUENCE_FILE:
        status = HdfsSequenceScanner::Codegen(this, conjunct_ctxs_, &fn);
        break;
      case THdfsFileFormat::AVRO:
        status = HdfsAvroScanner::Codegen(this, conjunct_ctxs_, &fn);
        break;
      case THdfsFileFormat::PARQUET:
        status = HdfsParquetScanner::Codegen(this, conjunct_ctxs_, &fn);
        break;
      default:
        // No codegen for this format
        fn = NULL;
        status = Status("Not implemented for this format.");
    }
    DCHECK(fn != NULL || !status.ok());

    const char* format_name = _THdfsFileFormat_VALUES_TO_NAMES.find(format)->second;
    if (!status.ok()) {
      AddCodegenExecOption(false, status, format_name);
    } else {
      AddCodegenExecOption(true, status, format_name);
      LlvmCodeGen* codegen;
      RETURN_IF_ERROR(runtime_state_->GetCodegen(&codegen));
      codegen->AddFunctionToJit(
          fn, &codegend_fn_map_[static_cast<THdfsFileFormat::type>(format)]);
    }
  }

  return Status::OK();
}

// This function initiates the connection to hdfs and starts up the initial scanner
// threads. The scanner subclasses are passed the initial splits. Scanners are expected to
// queue up a non-zero number of those splits to the io mgr (via the ScanNode). Scan
// ranges are not issued until the first GetNext() call; scanner threads will block on
// ranges_issued_barrier_ until ranges are issued.
Status HdfsScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));

  // Open collection conjuncts
  ConjunctsMap::const_iterator iter = conjuncts_map_.begin();
  for (; iter != conjuncts_map_.end(); ++iter) {
    // conjuncts_ are already opened in ExecNode::Open()
    if (iter->first == tuple_id_) continue;
    RETURN_IF_ERROR(Expr::Open(iter->second, state));
  }

  for (auto& filter_ctx: filter_ctxs_) RETURN_IF_ERROR(filter_ctx.expr->Open(state));

  // We need at least one scanner thread to make progress. We need to make this
  // reservation before any ranges are issued.
  runtime_state_->resource_pool()->ReserveOptionalTokens(1);
  if (runtime_state_->query_options().num_scanner_threads > 0) {
    max_num_scanner_threads_ = runtime_state_->query_options().num_scanner_threads;
  }
  DCHECK_GT(max_num_scanner_threads_, 0);

  thread_avail_cb_id_ = runtime_state_->resource_pool()->AddThreadAvailableCb(
      bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this, _1));

  if (runtime_state_->query_resource_mgr() != NULL) {
    rm_callback_id_ = runtime_state_->query_resource_mgr()->AddVcoreAvailableCb(
        bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this,
            runtime_state_->resource_pool()));
  }

  if (file_descs_.empty()) {
    SetDone();
    return Status::OK();
  }

  // Open all the partition exprs used by the scan node
  for (int64_t partition_id: partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    DCHECK(partition_desc != NULL) << "table_id=" << hdfs_table_->id()
                                   << " partition_id=" << partition_id
                                   << "\n" << PrintThrift(state->fragment_params());
    RETURN_IF_ERROR(partition_desc->OpenExprs(state));
  }

  RETURN_IF_ERROR(runtime_state_->io_mgr()->RegisterContext(
      &reader_context_, mem_tracker()));

  // Initialize HdfsScanNode specific counters
  read_timer_ = ADD_TIMER(runtime_profile(), TOTAL_HDFS_READ_TIMER);
  per_read_thread_throughput_counter_ = runtime_profile()->AddDerivedCounter(
      PER_READ_THREAD_THROUGHPUT_COUNTER, TUnit::BYTES_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_read_counter_, read_timer_));
  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TUnit::UNIT);
  if (DiskInfo::num_disks() < 64) {
    num_disks_accessed_counter_ =
        ADD_COUNTER(runtime_profile(), NUM_DISKS_ACCESSED_COUNTER, TUnit::UNIT);
  } else {
    num_disks_accessed_counter_ = NULL;
  }
  num_scanner_threads_started_counter_ =
      ADD_COUNTER(runtime_profile(), NUM_SCANNER_THREADS_STARTED, TUnit::UNIT);

  runtime_state_->io_mgr()->set_bytes_read_counter(reader_context_, bytes_read_counter());
  runtime_state_->io_mgr()->set_read_timer(reader_context_, read_timer());
  runtime_state_->io_mgr()->set_active_read_thread_counter(reader_context_,
      &active_hdfs_read_thread_counter_);
  runtime_state_->io_mgr()->set_disks_access_bitmap(reader_context_,
      &disks_accessed_bitmap_);

  average_scanner_thread_concurrency_ = runtime_profile()->AddSamplingCounter(
      AVERAGE_SCANNER_THREAD_CONCURRENCY, &active_scanner_thread_counter_);
  average_hdfs_read_thread_concurrency_ = runtime_profile()->AddSamplingCounter(
      AVERAGE_HDFS_READ_THREAD_CONCURRENCY, &active_hdfs_read_thread_counter_);

  bytes_read_local_ = ADD_COUNTER(runtime_profile(), "BytesReadLocal",
      TUnit::BYTES);
  bytes_read_short_circuit_ = ADD_COUNTER(runtime_profile(), "BytesReadShortCircuit",
      TUnit::BYTES);
  bytes_read_dn_cache_ = ADD_COUNTER(runtime_profile(), "BytesReadDataNodeCache",
      TUnit::BYTES);
  num_remote_ranges_ = ADD_COUNTER(runtime_profile(), "RemoteScanRanges",
      TUnit::UNIT);
  unexpected_remote_bytes_ = ADD_COUNTER(runtime_profile(), "BytesReadRemoteUnexpected",
      TUnit::BYTES);

  max_compressed_text_file_length_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxCompressedTextFileLength", TUnit::BYTES);

  for (int i = 0; i < state->io_mgr()->num_total_disks() + 1; ++i) {
    hdfs_read_thread_concurrency_bucket_.push_back(
        pool_->Add(new RuntimeProfile::Counter(TUnit::DOUBLE_VALUE, 0)));
  }
  runtime_profile()->RegisterBucketingCounters(&active_hdfs_read_thread_counter_,
      &hdfs_read_thread_concurrency_bucket_);

  counters_running_ = true;

  int total_splits = 0;
  for (FileDescMap::iterator it = file_descs_.begin(); it != file_descs_.end(); ++it) {
    total_splits += it->second->splits.size();
  }

  if (total_splits == 0) {
    SetDone();
    return Status::OK();
  }

  stringstream ss;
  ss << "Splits complete (node=" << id() << "):";
  progress_.Init(ss.str(), total_splits);
  return Status::OK();
}

Status HdfsScanNode::Reset(RuntimeState* state) {
  DCHECK(false) << "Internal error: Scan nodes should not appear in subplans.";
  return Status("Internal error: Scan nodes should not appear in subplans.");
}

void HdfsScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SetDone();

  if (thread_avail_cb_id_ != -1) {
    state->resource_pool()->RemoveThreadAvailableCb(thread_avail_cb_id_);
  }
  if (state->query_resource_mgr() != NULL && rm_callback_id_ != -1) {
    state->query_resource_mgr()->RemoveVcoreAvailableCb(rm_callback_id_);
  }

  scanner_threads_.JoinAll();

  num_owned_io_buffers_.Add(-materialized_row_batches_->Cleanup());
  DCHECK_EQ(num_owned_io_buffers_.Load(), 0) << "ScanNode has leaked io buffers";

  if (reader_context_ != NULL) {
    // There may still be io buffers used by parent nodes so we can't unregister the
    // reader context yet. The runtime state keeps a list of all the reader contexts and
    // they are unregistered when the fragment is closed.
    state->reader_contexts()->push_back(reader_context_);
    // Need to wait for all the active scanner threads to finish to ensure there is no
    // more memory tracked by this scan node's mem tracker.
    state->io_mgr()->CancelContext(reader_context_, true);
  }

  StopAndFinalizeCounters();

  // There should be no active scanner threads and hdfs read threads.
  DCHECK_EQ(active_scanner_thread_counter_.value(), 0);
  DCHECK_EQ(active_hdfs_read_thread_counter_.value(), 0);

  if (scan_node_pool_.get() != NULL) scan_node_pool_->FreeAll();

  // Close all the partitions scanned by the scan node
  for (int64_t partition_id: partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    if (partition_desc == NULL) {
      // TODO: Revert when IMPALA-1702 is fixed.
      LOG(ERROR) << "Bad table descriptor! table_id=" << hdfs_table_->id()
                 << " partition_id=" << partition_id
                 << "\n" << PrintThrift(state->fragment_params());
      continue;
    }
    partition_desc->CloseExprs(state);
  }

  // Close collection conjuncts
  ConjunctsMap::const_iterator iter = conjuncts_map_.begin();
  for (; iter != conjuncts_map_.end(); ++iter) {
    // conjuncts_ are already closed in ExecNode::Close()
    if (iter->first == tuple_id_) continue;
    Expr::Close(iter->second, state);
  }

  for (auto& filter_ctx: filter_ctxs_) filter_ctx.expr->Close(state);
  ScanNode::Close(state);
}

Status HdfsScanNode::AddDiskIoRanges(const vector<DiskIoMgr::ScanRange*>& ranges,
    int num_files_queued) {
  RETURN_IF_ERROR(
      runtime_state_->io_mgr()->AddScanRanges(reader_context_, ranges));
  num_unqueued_files_.Add(-num_files_queued);
  DCHECK_GE(num_unqueued_files_.Load(), 0);
  if (!ranges.empty()) ThreadTokenAvailableCb(runtime_state_->resource_pool());
  return Status::OK();
}

void HdfsScanNode::AddMaterializedRowBatch(RowBatch* row_batch) {
  InitNullCollectionValues(row_batch);
  materialized_row_batches_->AddBatch(row_batch);
}

void HdfsScanNode::InitNullCollectionValues(const TupleDescriptor* tuple_desc,
    Tuple* tuple) const {
  for (const SlotDescriptor* slot_desc: tuple_desc->collection_slots()) {
    CollectionValue* slot = reinterpret_cast<CollectionValue*>(
        tuple->GetSlot(slot_desc->tuple_offset()));
    if (tuple->IsNull(slot_desc->null_indicator_offset())) {
      *slot = CollectionValue();
      continue;
    }
    // Recursively traverse collection items.
    const TupleDescriptor* item_desc = slot_desc->collection_item_descriptor();
    if (item_desc->collection_slots().empty()) continue;
    for (int i = 0; i < slot->num_tuples; ++i) {
      int item_offset = i * item_desc->byte_size();
      Tuple* collection_item = reinterpret_cast<Tuple*>(slot->ptr + item_offset);
      InitNullCollectionValues(item_desc, collection_item);
    }
  }
}

void HdfsScanNode::InitNullCollectionValues(RowBatch* row_batch) const {
  DCHECK_EQ(row_batch->row_desc().tuple_descriptors().size(), 1);
  const TupleDescriptor& tuple_desc =
      *row_batch->row_desc().tuple_descriptors()[tuple_idx()];
  if (tuple_desc.collection_slots().empty()) return;
  for (int i = 0; i < row_batch->num_rows(); ++i) {
    Tuple* tuple = row_batch->GetRow(i)->GetTuple(tuple_idx());
    DCHECK(tuple != NULL);
    InitNullCollectionValues(&tuple_desc, tuple);
  }
}

// For controlling the amount of memory used for scanners, we approximate the
// scanner mem usage based on scanner_thread_bytes_required_, rather than the
// consumption in the scan node's mem tracker. The problem with the scan node
// trackers is that it does not account for the memory the scanner will use.
// For example, if there is 110 MB of memory left (based on the mem tracker)
// and we estimate that a scanner will use 100MB, we want to make sure to only
// start up one additional thread. However, after starting the first thread, the
// mem tracker value will not change immediately (it takes some time before the
// scanner is running and starts using memory). Therefore we just use the estimate
// based on the number of running scanner threads.
bool HdfsScanNode::EnoughMemoryForScannerThread(bool new_thread) {
  int64_t committed_scanner_mem =
      active_scanner_thread_counter_.value() * scanner_thread_bytes_required_;
  int64_t tracker_consumption = mem_tracker()->consumption();
  int64_t est_additional_scanner_mem = committed_scanner_mem - tracker_consumption;
  if (est_additional_scanner_mem < 0) {
    // This is the case where our estimate was too low. Expand the estimate based
    // on the usage.
    int64_t avg_consumption =
        tracker_consumption / active_scanner_thread_counter_.value();
    // Take the average and expand it by 50%. Some scanners will not have hit their
    // steady state mem usage yet.
    // TODO: how can we scale down if we've overestimated.
    // TODO: better heuristic?
    scanner_thread_bytes_required_ = static_cast<int64_t>(avg_consumption * 1.5);
    est_additional_scanner_mem = 0;
  }

  // If we are starting a new thread, take that into account now.
  if (new_thread) est_additional_scanner_mem += scanner_thread_bytes_required_;
  return est_additional_scanner_mem < mem_tracker()->SpareCapacity();
}

void HdfsScanNode::ThreadTokenAvailableCb(ThreadResourceMgr::ResourcePool* pool) {
  // This is called to start up new scanner threads. It's not a big deal if we
  // spin up more than strictly necessary since they will go through and terminate
  // promptly. However, we want to minimize that by checking a conditions.
  //  1. Don't start up if the ScanNode is done
  //  2. Don't start up if all the ranges have been taken by another thread.
  //  3. Don't start up if the number of ranges left is less than the number of
  //     active scanner threads.
  //  4. Don't start up if no initial ranges have been issued (see IMPALA-1722).
  //  5. Don't start up a ScannerThread if materialized_row_batches_ is full since
  //     we are not scanner bound.
  //  6. Don't start up a thread if there isn't enough memory left to run it.
  //  7. Don't start up more than maximum number of scanner threads configured.
  //  8. Don't start up if there are no thread tokens.
  //  9. Don't start up if we are running too many threads for our vcore allocation
  //  (unless the thread is reserved, in which case it has to run).

  // Case 4. We have not issued the initial ranges so don't start a scanner thread.
  // Issuing ranges will call this function and we'll start the scanner threads then.
  // TODO: It would be good to have a test case for that.
  if (!initial_ranges_issued_) return;

  while (true) {
    // The lock must be given up between loops in order to give writers to done_,
    // all_ranges_started_ etc. a chance to grab the lock.
    // TODO: This still leans heavily on starvation-free locks, come up with a more
    // correct way to communicate between this method and ScannerThreadHelper
    unique_lock<mutex> lock(lock_);
    // Cases 1, 2, 3.
    if (done_ || all_ranges_started_ ||
        active_scanner_thread_counter_.value() >= progress_.remaining()) {
      break;
    }

    // Cases 5 and 6.
    if (active_scanner_thread_counter_.value() > 0 &&
        (materialized_row_batches_->GetSize() >= max_materialized_row_batches_ ||
         !EnoughMemoryForScannerThread(true))) {
      break;
    }

    // Case 7 and 8.
    bool is_reserved = false;
    if (active_scanner_thread_counter_.value() >= max_num_scanner_threads_ ||
        !pool->TryAcquireThreadToken(&is_reserved)) {
      break;
    }

    // Case 9.
    if (!is_reserved) {
      if (runtime_state_->query_resource_mgr() != NULL &&
          runtime_state_->query_resource_mgr()->IsVcoreOverSubscribed()) {
        pool->ReleaseThreadToken(false);
        break;
      }
    }

    COUNTER_ADD(&active_scanner_thread_counter_, 1);
    COUNTER_ADD(num_scanner_threads_started_counter_, 1);
    stringstream ss;
    ss << "scanner-thread(" << num_scanner_threads_started_counter_->value() << ")";
    scanner_threads_.AddThread(
        new Thread("hdfs-scan-node", ss.str(), &HdfsScanNode::ScannerThread, this));

    if (runtime_state_->query_resource_mgr() != NULL) {
      runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(1);
    }
  }
}

void HdfsScanNode::ScannerThread() {
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  SCOPED_TIMER(runtime_state_->total_cpu_timer());

  // Make thread-local copy of filter contexts to prune scan ranges, and to pass to the
  // scanner for finer-grained filtering.
  vector<FilterContext> filter_ctxs;
  Status filter_status = Status::OK();
  for (auto& filter_ctx: filter_ctxs_) {
    FilterContext filter;
    filter_status = filter.CloneFrom(filter_ctx, runtime_state_);
    if (!filter_status.ok()) break;
    filter_ctxs.push_back(filter);
  }

  while (!done_) {
    {
      // Check if we have enough resources (thread token and memory) to keep using
      // this thread.
      unique_lock<mutex> l(lock_);
      if (active_scanner_thread_counter_.value() > 1) {
        if (runtime_state_->resource_pool()->optional_exceeded() ||
            !EnoughMemoryForScannerThread(false)) {
          // We can't break here. We need to update the counter with the lock held or else
          // all threads might see active_scanner_thread_counter_.value > 1
          COUNTER_ADD(&active_scanner_thread_counter_, -1);
          // Unlock before releasing the thread token to avoid deadlock in
          // ThreadTokenAvailableCb().
          l.unlock();
          if (runtime_state_->query_resource_mgr() != NULL) {
            runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(-1);
          }
          runtime_state_->resource_pool()->ReleaseThreadToken(false);
          if (filter_status.ok()) {
            for (auto& ctx: filter_ctxs) {
              ctx.expr->FreeLocalAllocations();
              ctx.expr->Close(runtime_state_);
            }
          }
          return;
        }
      } else {
        // If this is the only scanner thread, it should keep running regardless
        // of resource constraints.
      }
    }

    bool unused = false;
    // Wake up every SCANNER_THREAD_COUNTERS to yield scanner threads back if unused, or
    // to return if there's an error.
    ranges_issued_barrier_.Wait(SCANNER_THREAD_WAIT_TIME_MS, &unused);

    DiskIoMgr::ScanRange* scan_range;
    // Take a snapshot of num_unqueued_files_ before calling GetNextRange().
    // We don't want num_unqueued_files_ to go to zero between the return from
    // GetNextRange() and the check for when all ranges are complete.
    int num_unqueued_files = num_unqueued_files_.Load();
    // TODO: the Load() acts as an acquire barrier.  Is this needed? (i.e. any earlier
    // stores that need to complete?)
    AtomicUtil::MemoryBarrier();
    Status status = runtime_state_->io_mgr()->GetNextRange(reader_context_, &scan_range);

    if (status.ok() && scan_range != NULL) {
      // Got a scan range. Process the range end to end (in this thread).
      status = ProcessSplit(filter_status.ok() ? filter_ctxs : vector<FilterContext>(),
          scan_range);
    }

    if (!status.ok()) {
      {
        unique_lock<mutex> l(lock_);
        // If there was already an error, the main thread will do the cleanup
        if (!status_.ok()) break;

        if (status.IsCancelled() && done_) {
          // Scan node initiated scanner thread cancellation.  No need to do anything.
          break;
        }
        // Set status_ before calling SetDone() (which shuts down the RowBatchQueue),
        // to ensure that GetNextInternal() notices the error status.
        status_ = status;
      }

      SetDone();
      break;
    }

    // Done with range and it completed successfully
    if (progress_.done()) {
      // All ranges are finished.  Indicate we are done.
      SetDone();
      break;
    }

    if (scan_range == NULL && num_unqueued_files == 0) {
      // TODO: Based on the usage pattern of all_ranges_started_, it looks like it is not
      // needed to acquire the lock in x86.
      unique_lock<mutex> l(lock_);
      // All ranges have been queued and GetNextRange() returned NULL. This means that
      // every range is either done or being processed by another thread.
      all_ranges_started_ = true;
      break;
    }
  }

  if (filter_status.ok()) {
    for (auto& ctx: filter_ctxs) {
      ctx.expr->FreeLocalAllocations();
      ctx.expr->Close(runtime_state_);
    }
  }

  COUNTER_ADD(&active_scanner_thread_counter_, -1);
  if (runtime_state_->query_resource_mgr() != NULL) {
    runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(-1);
  }
  runtime_state_->resource_pool()->ReleaseThreadToken(false);
}

bool HdfsScanNode::PartitionPassesFilters(int32_t partition_id,
    const string& stats_name, const vector<FilterContext>& filter_ctxs) {
  if (filter_ctxs.size() == 0) return true;
  DCHECK_EQ(filter_ctxs.size(), filter_ctxs_.size())
      << "Mismatched number of filter contexts";
  Tuple* template_tuple = partition_template_tuple_map_[partition_id];
  // Defensive - if template_tuple is NULL, there can be no filters on partition columns.
  if (template_tuple == NULL) return true;
  TupleRow* tuple_row_mem = reinterpret_cast<TupleRow*>(&template_tuple);
  for (const FilterContext& ctx: filter_ctxs) {
    int target_ndx = ctx.filter->filter_desc().planid_to_target_ndx.at(id_);
    if (!ctx.filter->filter_desc().targets[target_ndx].is_bound_by_partition_columns) {
      continue;
    }
    void* e = ctx.expr->GetValue(tuple_row_mem);

    // Not quite right because bitmap could arrive after Eval(), but we're ok with
    // off-by-one errors.
    bool processed = ctx.filter->HasBloomFilter();
    bool passed_filter = ctx.filter->Eval<void>(e, ctx.expr->root()->type());
    ctx.stats->IncrCounters(stats_name, 1, processed, !passed_filter);
    if (!passed_filter) return false;
  }

  return true;
}

namespace {

// Returns true if 'format' uses a scanner derived from BaseSequenceScanner. Used to
// workaround IMPALA-3798.
bool FileFormatIsSequenceBased(THdfsFileFormat::type format) {
  return format == THdfsFileFormat::SEQUENCE_FILE ||
      format == THdfsFileFormat::RC_FILE ||
      format == THdfsFileFormat::AVRO;
}

}

Status HdfsScanNode::ProcessSplit(const vector<FilterContext>& filter_ctxs,
    DiskIoMgr::ScanRange* scan_range) {

  DCHECK(scan_range != NULL);

  ScanRangeMetadata* metadata =
      reinterpret_cast<ScanRangeMetadata*>(scan_range->meta_data());
  int64_t partition_id = metadata->partition_id;
  HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
  DCHECK(partition != NULL) << "table_id=" << hdfs_table_->id()
                            << " partition_id=" << partition_id
                            << "\n" << PrintThrift(runtime_state_->fragment_params());

  // IMPALA-3798: Filtering before the scanner is created can cause hangs if a header
  // split is filtered out, for sequence-based file formats. If the scanner does not
  // process the header split, the remaining scan ranges in the file will not be marked as
  // done. See FilePassesFilterPredicates() for the correct logic to mark all splits in a
  // file as done; the correct fix here is to do that for every file in a thread-safe way.
  if (!FileFormatIsSequenceBased(partition->file_format())) {
    if (!PartitionPassesFilters(partition_id, FilterStats::SPLITS_KEY, filter_ctxs)) {
      // Avoid leaking unread buffers in scan_range.
      scan_range->Cancel(Status::CANCELLED);
      // Mark scan range as done.
      scan_ranges_complete_counter()->Add(1);
      progress_.Update(1);
      return Status::OK();
    }
  }

  ScannerContext context(runtime_state_, this, partition, scan_range, filter_ctxs);
  scoped_ptr<HdfsScanner> scanner;
  Status status = CreateAndOpenScanner(partition, &context, &scanner);
  if (!status.ok()) {
    // If preparation fails, avoid leaking unread buffers in the scan_range.
    scan_range->Cancel(status);

    if (VLOG_QUERY_IS_ON) {
      stringstream ss;
      ss << "Error preparing scanner for scan range " << scan_range->file() <<
          "(" << scan_range->offset() << ":" << scan_range->len() << ").";
      ss << endl << runtime_state_->ErrorLog();
      VLOG_QUERY << ss.str();
    }
    return status;
  }

  status = scanner->ProcessSplit();
  if (VLOG_QUERY_IS_ON && !status.ok() && !status.IsCancelled()) {
    // This thread hit an error, record it and bail
    stringstream ss;
    ss << "Scan node (id=" << id() << ") ran into a parse error for scan range "
       << scan_range->file() << "(" << scan_range->offset() << ":"
       << scan_range->len() << ").";
    // Parquet doesn't read the range end to end so the current offset isn't useful.
    // TODO: make sure the parquet reader is outputting as much diagnostic
    // information as possible.
    if (partition->file_format() != THdfsFileFormat::PARQUET) {
      ScannerContext::Stream* stream = context.GetStream();
      ss << " Processed " << stream->total_bytes_returned() << " bytes.";
    }
    VLOG_QUERY << ss.str();
  }

  // Transfer the remaining resources to the final row batch (if any) and add it to
  // the row batch queue.
  scanner->Close(scanner->batch());
  return status;
}

void HdfsScanNode::RangeComplete(const THdfsFileFormat::type& file_type,
    const THdfsCompression::type& compression_type) {
  vector<THdfsCompression::type> types;
  types.push_back(compression_type);
  RangeComplete(file_type, types);
}

void HdfsScanNode::RangeComplete(const THdfsFileFormat::type& file_type,
    const vector<THdfsCompression::type>& compression_types) {
  scan_ranges_complete_counter()->Add(1);
  progress_.Update(1);

  {
    lock_guard<SpinLock> l(file_type_counts_lock_);
    for (int i = 0; i < compression_types.size(); ++i) {
      ++file_type_counts_[make_pair(file_type, compression_types[i])];
    }
  }
}

void HdfsScanNode::SetDone() {
  {
    unique_lock<mutex> l(lock_);
    if (done_) return;
    done_ = true;
  }
  if (reader_context_ != NULL) {
    runtime_state_->io_mgr()->CancelContext(reader_context_);
  }
  materialized_row_batches_->Shutdown();
}

void HdfsScanNode::ComputeSlotMaterializationOrder(vector<int>* order) const {
  const vector<ExprContext*>& conjuncts = ExecNode::conjunct_ctxs();
  // Initialize all order to be conjuncts.size() (after the last conjunct)
  order->insert(order->begin(), materialized_slots().size(), conjuncts.size());

  const DescriptorTbl& desc_tbl = runtime_state_->desc_tbl();

  vector<SlotId> slot_ids;
  for (int conjunct_idx = 0; conjunct_idx < conjuncts.size(); ++conjunct_idx) {
    slot_ids.clear();
    int num_slots = conjuncts[conjunct_idx]->root()->GetSlotIds(&slot_ids);
    for (int j = 0; j < num_slots; ++j) {
      SlotDescriptor* slot_desc = desc_tbl.GetSlotDescriptor(slot_ids[j]);
      int slot_idx = GetMaterializedSlotIdx(slot_desc->col_path());
      // slot_idx == -1 means this was a partition key slot which is always
      // materialized before any slots.
      if (slot_idx == -1) continue;
      // If this slot hasn't been assigned an order, assign it be materialized
      // before evaluating conjuncts[i]
      if ((*order)[slot_idx] == conjuncts.size()) {
        (*order)[slot_idx] = conjunct_idx;
      }
    }
  }
}

void HdfsScanNode::StopAndFinalizeCounters() {
  unique_lock<mutex> l(lock_);
  if (!counters_running_) return;
  counters_running_ = false;

  PeriodicCounterUpdater::StopTimeSeriesCounter(bytes_read_timeseries_counter_);
  PeriodicCounterUpdater::StopRateCounter(total_throughput_counter());
  PeriodicCounterUpdater::StopSamplingCounter(average_scanner_thread_concurrency_);
  PeriodicCounterUpdater::StopSamplingCounter(average_hdfs_read_thread_concurrency_);
  PeriodicCounterUpdater::StopBucketingCounters(&hdfs_read_thread_concurrency_bucket_,
      true);

  // Output hdfs read thread concurrency into info string
  stringstream ss;
  for (int i = 0; i < hdfs_read_thread_concurrency_bucket_.size(); ++i) {
    ss << i << ":" << setprecision(4)
       << hdfs_read_thread_concurrency_bucket_[i]->double_value() << "% ";
  }
  runtime_profile_->AddInfoString("Hdfs Read Thread Concurrency Bucket", ss.str());

  // Convert disk access bitmap to num of disk accessed
  uint64_t num_disk_bitmap = disks_accessed_bitmap_.value();
  int64_t num_disk_accessed = BitUtil::Popcount(num_disk_bitmap);
  if (num_disks_accessed_counter_ != NULL) {
    num_disks_accessed_counter_->Set(num_disk_accessed);
  }

  // output completed file types and counts to info string
  if (!file_type_counts_.empty()) {
    stringstream ss;
    {
      lock_guard<SpinLock> l2(file_type_counts_lock_);
      for (FileTypeCountsMap::const_iterator it = file_type_counts_.begin();
          it != file_type_counts_.end(); ++it) {
        ss << it->first.first << "/" << it->first.second << ":" << it->second << " ";
      }
    }
    runtime_profile_->AddInfoString("File Formats", ss.str());
  }

  // Output fraction of scanners with codegen enabled
  int num_enabled = num_scanners_codegen_enabled_.Load();
  int total = num_enabled + num_scanners_codegen_disabled_.Load();
  AddRuntimeExecOption(Substitute("Codegen enabled: $0 out of $1", num_enabled, total));

  if (reader_context_ != NULL) {
    bytes_read_local_->Set(runtime_state_->io_mgr()->bytes_read_local(reader_context_));
    bytes_read_short_circuit_->Set(
        runtime_state_->io_mgr()->bytes_read_short_circuit(reader_context_));
    bytes_read_dn_cache_->Set(
        runtime_state_->io_mgr()->bytes_read_dn_cache(reader_context_));
    num_remote_ranges_->Set(static_cast<int64_t>(
        runtime_state_->io_mgr()->num_remote_ranges(reader_context_)));
    unexpected_remote_bytes_->Set(
        runtime_state_->io_mgr()->unexpected_remote_bytes(reader_context_));

    if (unexpected_remote_bytes_->value() >= UNEXPECTED_REMOTE_BYTES_WARN_THRESHOLD) {
      runtime_state_->LogError(ErrorMsg(TErrorCode::GENERAL, Substitute(
          "Read $0 of data across network that was expected to be local. "
          "Block locality metadata for table '$1.$2' may be stale. Consider running "
          "\"INVALIDATE METADATA `$1`.`$2`\".",
          PrettyPrinter::Print(unexpected_remote_bytes_->value(), TUnit::BYTES),
          hdfs_table_->database(), hdfs_table_->name())));
    }

    ImpaladMetrics::IO_MGR_BYTES_READ->Increment(bytes_read_counter()->value());
    ImpaladMetrics::IO_MGR_LOCAL_BYTES_READ->Increment(
        bytes_read_local_->value());
    ImpaladMetrics::IO_MGR_SHORT_CIRCUIT_BYTES_READ->Increment(
        bytes_read_short_circuit_->value());
    ImpaladMetrics::IO_MGR_CACHED_BYTES_READ->Increment(
        bytes_read_dn_cache_->value());
  }
}

void HdfsScanNode::UpdateHdfsSplitStats(
    const vector<TScanRangeParams>& scan_range_params_list,
    PerVolumnStats* per_volume_stats) {
  pair<int, int64_t> init_value(0, 0);
  for (const TScanRangeParams& scan_range_params: scan_range_params_list) {
    const TScanRange& scan_range = scan_range_params.scan_range;
    if (!scan_range.__isset.hdfs_file_split) continue;
    const THdfsFileSplit& split = scan_range.hdfs_file_split;
    pair<int, int64_t>* stats =
        FindOrInsert(per_volume_stats, scan_range_params.volume_id, init_value);
    ++(stats->first);
    stats->second += split.length;
  }
}

void HdfsScanNode::PrintHdfsSplitStats(const PerVolumnStats& per_volume_stats,
    stringstream* ss) {
  for (PerVolumnStats::const_iterator i = per_volume_stats.begin();
       i != per_volume_stats.end(); ++i) {
     (*ss) << i->first << ":" << i->second.first << "/"
         << PrettyPrinter::Print(i->second.second, TUnit::BYTES) << " ";
  }
}
