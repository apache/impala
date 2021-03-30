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

#include "exec/hdfs-scan-node-base.h"

#include "exec/base-sequence-scanner.h"
#include "exec/hdfs-avro-scanner.h"
#include "exec/hdfs-columnar-scanner.h"
#include "exec/hdfs-orc-scanner.h"
#include "exec/hdfs-plugin-text-scanner.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-scan-node-mt.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-text-scanner.h"
#include "exec/parquet/hdfs-parquet-scanner.h"

#include <avro/errors.h>
#include <avro/schema.h>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/request-context.h"
#include "runtime/query-state.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "util/disk-info.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/periodic-counter-updater.h"
#include "util/pretty-printer.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

#ifndef NDEBUG
DECLARE_bool(skip_file_runtime_filtering);
#endif

DEFINE_bool(always_use_data_cache, false, "(Advanced) Always uses the IO data cache "
    "for all reads, regardless of whether the read is local or remote. By default, the "
    "IO data cache is only used if the data is expected to be remote. Used by tests.");

namespace filesystem = boost::filesystem;
using namespace impala;
using namespace impala::io;
using namespace strings;

PROFILE_DEFINE_TIMER(TotalRawHdfsReadTime, STABLE_LOW, "Aggregate wall clock time"
    " across all Disk I/O threads in HDFS read operations.");
PROFILE_DEFINE_TIMER(TotalRawHdfsOpenFileTime, STABLE_LOW, "Aggregate wall clock time"
    " spent across all Disk I/O threads in HDFS open operations.");
PROFILE_DEFINE_DERIVED_COUNTER(PerReadThreadRawHdfsThroughput, STABLE_LOW,
    TUnit::BYTES_PER_SECOND, "The read throughput in bytes/sec for each HDFS read thread"
    " while it is executing I/O operations on behalf of a scan.");
PROFILE_DEFINE_COUNTER(ScanRangesComplete, STABLE_LOW, TUnit::UNIT,
    "Number of scan ranges that have been completed by a scan node.");
PROFILE_DEFINE_COUNTER(CollectionItemsRead, STABLE_LOW, TUnit::UNIT,
    "Total number of nested collection items read by the scan. Only created for scans "
    "(e.g. Parquet) that support nested types.");
PROFILE_DEFINE_COUNTER(NumDisksAccessed, STABLE_LOW, TUnit::UNIT, "Number of distinct "
     "disks accessed by HDFS scan. Each local disk is counted as a disk and each type of"
     " remote filesystem (e.g. HDFS remote reads, S3) is counted as a distinct disk.");
PROFILE_DEFINE_SAMPLING_COUNTER(AverageHdfsReadThreadConcurrency, STABLE_LOW, "The"
     " average number of HDFS read threads executing read operations on behalf of this "
     "scan. Higher values (i.e. close to the aggregate number of I/O threads across "
     "all disks accessed) show that this scan is using a larger proportion of the I/O "
     "capacity of the system. Lower values show that either this scan is not I/O bound"
     " or that it is getting a small share of the I/O capacity of the system.");
PROFILE_DEFINE_SUMMARY_STATS_COUNTER(InitialRangeIdealReservation, DEBUG, TUnit::BYTES,
     "Tracks stats about the ideal reservation for initial scan ranges. Use this to "
     "determine if the scan got all of the reservation it wanted. Does not include "
     "subsequent reservation increases done by scanner implementation (e.g. for Parquet "
     "columns).");
PROFILE_DEFINE_SUMMARY_STATS_COUNTER(InitialRangeActualReservation, DEBUG,
    TUnit::BYTES, "Tracks stats about the actual reservation for initial scan ranges. "
    "Use this to determine if the scan got all of the reservation it wanted. Does not "
    "include subsequent reservation increases done by scanner implementation "
    "(e.g. for Parquet columns).");
PROFILE_DEFINE_COUNTER(BytesReadLocal, STABLE_LOW, TUnit::BYTES,
    "The total number of bytes read locally");
PROFILE_DEFINE_COUNTER(BytesReadShortCircuit, STABLE_LOW, TUnit::BYTES,
    "The total number of bytes read via short circuit read");
PROFILE_DEFINE_COUNTER(BytesReadDataNodeCache, STABLE_HIGH, TUnit::BYTES,
    "The total number of bytes read from data node cache");
PROFILE_DEFINE_COUNTER(RemoteScanRanges, STABLE_HIGH, TUnit::UNIT,
    "The total number of remote scan ranges");
PROFILE_DEFINE_COUNTER(BytesReadRemoteUnexpected, STABLE_LOW, TUnit::BYTES,
    "The total number of bytes read remotely that were expected to be local");
PROFILE_DEFINE_COUNTER(CachedFileHandlesHitCount, STABLE_LOW, TUnit::UNIT,
    "Total number of file handle opens where the file handle was present in the cache");
PROFILE_DEFINE_COUNTER(CachedFileHandlesMissCount, STABLE_LOW, TUnit::UNIT,
    "Total number of file handle opens where the file handle was not in the cache");
PROFILE_DEFINE_HIGH_WATER_MARK_COUNTER(MaxCompressedTextFileLength, STABLE_LOW,
    TUnit::BYTES, "The size of the largest compressed text file to be scanned. "
    "This is used to estimate scanner thread memory usage.");
PROFILE_DEFINE_TIMER(ScannerIoWaitTime, STABLE_LOW, "Total amount of time scanner "
    "threads spent waiting for I/O. This value can be compared to the value of "
    "ScannerThreadsTotalWallClockTime of MT_DOP = 0 scan nodes or otherwise compared "
    "to the total time reported for MT_DOP > 0 scan nodes. High values show that "
    "scanner threads are spending significant time waiting for I/O instead of "
    "processing data. Note that this includes the time when the thread is runnable "
    "but not scheduled.");
PROFILE_DEFINE_SUMMARY_STATS_COUNTER(ParquetUncompressedBytesReadPerColumn, STABLE_LOW,
    TUnit::BYTES, "Stats about the number of uncompressed bytes read per column. "
    "Each sample in the counter is the size of a single column that is scanned by the "
    "scan node.");
PROFILE_DEFINE_SUMMARY_STATS_COUNTER(ParquetCompressedBytesReadPerColumn, STABLE_LOW,
    TUnit::BYTES, "Stats about the number of compressed bytes read per column. "
    "Each sample in the counter is the size of a single column that is scanned by the "
    "scan node.");
PROFILE_DEFINE_COUNTER(DataCacheHitCount, STABLE_HIGH, TUnit::UNIT,
    "Total count of data cache hit");
PROFILE_DEFINE_COUNTER(DataCachePartialHitCount, STABLE_HIGH, TUnit::UNIT,
    "Total count of data cache partially hit");
PROFILE_DEFINE_COUNTER(DataCacheMissCount, STABLE_HIGH, TUnit::UNIT,
    "Total count of data cache miss");
PROFILE_DEFINE_COUNTER(DataCacheHitBytes, STABLE_HIGH, TUnit::BYTES,
    "Total bytes of data cache hit");
PROFILE_DEFINE_COUNTER(DataCacheMissBytes, STABLE_HIGH, TUnit::BYTES,
    "Total bytes of data cache miss");

const string HdfsScanNodeBase::HDFS_SPLIT_STATS_DESC =
    "Hdfs split stats (<volume id>:<# splits>/<split lengths>)";

// Determines how many unexpected remote bytes trigger an error in the runtime state
const int UNEXPECTED_REMOTE_BYTES_WARN_THRESHOLD = 64 * 1024 * 1024;

Status HdfsScanPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(ScanPlanNode::Init(tnode, state));

  tuple_id_ = tnode.hdfs_scan_node.tuple_id;
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_->table_desc() != NULL);
  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());

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
    is_materialized_col_[i] =
        GetMaterializedSlotIdx(vector<int>(1, i)) != HdfsScanNodeBase::SKIP_COLUMN;
  }

  // Add collection item conjuncts
  for (const auto& entry : tnode.hdfs_scan_node.collection_conjuncts) {
    TupleDescriptor* tuple_desc = state->desc_tbl().GetTupleDescriptor(entry.first);
    RowDescriptor* collection_row_desc =
        state->obj_pool()->Add(new RowDescriptor(tuple_desc, /* is_nullable */ false));
    DCHECK(conjuncts_map_.find(entry.first) == conjuncts_map_.end());
    RETURN_IF_ERROR(ScalarExpr::Create(
        entry.second, *collection_row_desc, state, &conjuncts_map_[entry.first]));
  }
  const TTupleId& tuple_id = tnode.hdfs_scan_node.tuple_id;
  DCHECK(conjuncts_map_[tuple_id].empty());
  conjuncts_map_[tuple_id] = conjuncts_;

  // Add min max conjuncts
  if (tnode.hdfs_scan_node.__isset.min_max_tuple_id) {
    TupleDescriptor* min_max_tuple_desc =
        state->desc_tbl().GetTupleDescriptor(tnode.hdfs_scan_node.min_max_tuple_id);
    DCHECK(min_max_tuple_desc != nullptr);
    RowDescriptor* min_max_row_desc = state->obj_pool()->Add(
        new RowDescriptor(min_max_tuple_desc, /* is_nullable */ false));
    RETURN_IF_ERROR(ScalarExpr::Create(tnode.hdfs_scan_node.min_max_conjuncts,
        *min_max_row_desc, state, &min_max_conjuncts_));
  }

  const vector<const TPlanFragmentInstanceCtx*>& instance_ctxs = state->instance_ctxs();
  for (auto ctx : instance_ctxs) {
    auto ranges = ctx->per_node_scan_ranges.find(tnode.node_id);
    if (ranges == ctx->per_node_scan_ranges.end()) continue;
    for (const TScanRangeParams& scan_range_param : ranges->second) {
      const THdfsFileSplit& split = scan_range_param.scan_range.hdfs_file_split;
      HdfsPartitionDescriptor* partition_desc =
          hdfs_table_->GetPartition(split.partition_id);
      scanned_file_formats_.insert(partition_desc->file_format());
    }
  }
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

int HdfsScanPlanNode::GetMaterializedSlotIdx(const std::vector<int>& path) const {
  auto result = path_to_materialized_slot_idx_.find(path);
  if (result == path_to_materialized_slot_idx_.end()) {
    return HdfsScanNodeBase::SKIP_COLUMN;
  }
  return result->second;
}

void HdfsScanPlanNode::Close() {
  TTupleId tuple_id = tnode_->hdfs_scan_node.tuple_id;
  for (auto& tid_conjunct : conjuncts_map_) {
    // PlanNode::conjuncts_ are already closed in PlanNode::Close()
    if (tid_conjunct.first == tuple_id) continue;
    ScalarExpr::Close(tid_conjunct.second);
  }
  ScalarExpr::Close(min_max_conjuncts_);
  PlanNode::Close();
}

Status HdfsScanPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(tnode_->hdfs_scan_node.use_mt_scan_node ?
          static_cast<HdfsScanNodeBase*>(
              new HdfsScanNodeMt(pool, *this, state->desc_tbl())) :
          static_cast<HdfsScanNodeBase*>(
              new HdfsScanNode(pool, *this, state->desc_tbl())));
  return Status::OK();
}

HdfsScanNodeBase::HdfsScanNodeBase(ObjectPool* pool, const HdfsScanPlanNode& pnode,
    const THdfsScanNode& hdfs_scan_node, const DescriptorTbl& descs)
  : ScanNode(pool, pnode, descs),
    min_max_tuple_id_(
        hdfs_scan_node.__isset.min_max_tuple_id ? hdfs_scan_node.min_max_tuple_id : -1),
    min_max_conjuncts_(pnode.min_max_conjuncts_),
    min_max_tuple_desc_(
        min_max_tuple_id_ == -1 ? nullptr : descs.GetTupleDescriptor(min_max_tuple_id_)),
    skip_header_line_count_(hdfs_scan_node.__isset.skip_header_line_count ?
            hdfs_scan_node.skip_header_line_count :
            0),
    tuple_id_(pnode.tuple_id_),
    parquet_count_star_slot_offset_(
        hdfs_scan_node.__isset.parquet_count_star_slot_offset ?
            hdfs_scan_node.parquet_count_star_slot_offset :
            -1),
    is_partition_key_scan_(hdfs_scan_node.is_partition_key_scan),
    tuple_desc_(pnode.tuple_desc_),
    hdfs_table_(pnode.hdfs_table_),
    avro_schema_(*pnode.avro_schema_.get()),
    conjuncts_map_(pnode.conjuncts_map_),
    thrift_dict_filter_conjuncts_map_(hdfs_scan_node.__isset.dictionary_filter_conjuncts ?
            &hdfs_scan_node.dictionary_filter_conjuncts :
            nullptr),
    codegend_fn_map_(pnode.codegend_fn_map_),
    is_materialized_col_(pnode.is_materialized_col_),
    materialized_slots_(pnode.materialized_slots_),
    partition_key_slots_(pnode.partition_key_slots_),
    disks_accessed_bitmap_(TUnit::UNIT, 0),
    active_hdfs_read_thread_counter_(TUnit::UNIT, 0) {}

HdfsScanNodeBase::~HdfsScanNodeBase() {}

Status HdfsScanNodeBase::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  AddBytesReadCounters();

  // Prepare collection conjuncts
  for (const auto& entry: conjuncts_map_) {
    TupleDescriptor* tuple_desc = state->desc_tbl().GetTupleDescriptor(entry.first);
    // conjuncts_ are already prepared in ExecNode::Prepare(), don't try to prepare again
    if (tuple_desc == tuple_desc_) {
      conjunct_evals_map_[entry.first] = conjunct_evals();
    } else {
      DCHECK(conjunct_evals_map_[entry.first].empty());
      RETURN_IF_ERROR(ScalarExprEvaluator::Create(entry.second, state, pool_,
          expr_perm_pool(), expr_results_pool(), &conjunct_evals_map_[entry.first]));
    }
  }

  // Prepare min max statistics conjuncts.
  if (min_max_tuple_id_ != -1) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(min_max_conjuncts_, state, pool_,
        expr_perm_pool(), expr_results_pool(), &min_max_conjunct_evals_));
  }

  // One-time initialization of state that is constant across scan ranges
  scan_node_pool_.reset(new MemPool(mem_tracker()));

  HdfsFsCache::HdfsFsMap fs_cache;
  // Convert the TScanRangeParams into per-file DiskIO::ScanRange objects and populate
  // partition_ids_, file_descs_, and per_type_files_.
  DCHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";
  int num_ranges_missing_volume_id = 0;
  for (const TScanRangeParams& params: *scan_range_params_) {
    DCHECK(params.scan_range.__isset.hdfs_file_split);
    const THdfsFileSplit& split = params.scan_range.hdfs_file_split;
    partition_ids_.insert(split.partition_id);
    HdfsPartitionDescriptor* partition_desc =
        hdfs_table_->GetPartition(split.partition_id);
    if (partition_desc == NULL) {
      // TODO: this should be a DCHECK but we sometimes hit it. It's likely IMPALA-1702.
      LOG(ERROR) << "Bad table descriptor! table_id=" << hdfs_table_->id()
                 << " partition_id=" << split.partition_id
                 << "\n" << PrintThrift(state->instance_ctx());
      return Status("Query encountered invalid metadata, likely due to IMPALA-1702."
                    " Try rerunning the query.");
    }

    filesystem::path file_path(partition_desc->location());
    file_path.append(split.relative_path, filesystem::path::codecvt());
    const string& native_file_path = file_path.native();

    auto file_desc_map_key = make_pair(partition_desc->id(), native_file_path);
    HdfsFileDesc* file_desc = NULL;
    FileDescMap::iterator file_desc_it = file_descs_.find(file_desc_map_key);
    if (file_desc_it == file_descs_.end()) {
      // Add new file_desc to file_descs_ and per_type_files_
      file_desc = runtime_state_->obj_pool()->Add(new HdfsFileDesc(native_file_path));
      file_descs_[file_desc_map_key] = file_desc;
      file_desc->file_length = split.file_length;
      file_desc->mtime = split.mtime;
      file_desc->file_compression = split.file_compression;
      file_desc->is_erasure_coded = split.is_erasure_coded;
      RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
          native_file_path, &file_desc->fs, &fs_cache));
      per_type_files_[partition_desc->file_format()].push_back(file_desc);
    } else {
      // File already processed
      file_desc = file_desc_it->second;
    }

    bool expected_local = params.__isset.is_remote && !params.is_remote;
    if (expected_local && params.volume_id == -1) ++num_ranges_missing_volume_id;

    int cache_options = BufferOpts::NO_CACHING;
    if (params.__isset.try_hdfs_cache && params.try_hdfs_cache) {
      cache_options |= BufferOpts::USE_HDFS_CACHE;
    }
    if ((!expected_local || FLAGS_always_use_data_cache) && !IsDataCacheDisabled()) {
      cache_options |= BufferOpts::USE_DATA_CACHE;
    }
    file_desc->splits.push_back(
        AllocateScanRange(file_desc->fs, file_desc->filename.c_str(), split.length,
            split.offset, split.partition_id, params.volume_id, expected_local,
            file_desc->is_erasure_coded, file_desc->mtime, BufferOpts(cache_options)));
  }

  // Update server wide metrics for number of scan ranges and ranges that have
  // incomplete metadata.
  ImpaladMetrics::NUM_RANGES_PROCESSED->Increment(scan_range_params_->size());
  ImpaladMetrics::NUM_RANGES_MISSING_VOLUME_ID->Increment(num_ranges_missing_volume_id);

  // Check if reservation was enough to allocate at least one buffer. The
  // reservation calculation in HdfsScanNode.java should guarantee this.
  // Hitting this error indicates a misconfiguration or bug.
  int64_t min_buffer_size = ExecEnv::GetInstance()->disk_io_mgr()->min_buffer_size();
  if (scan_range_params_->size() > 0
      && resource_profile_.min_reservation < min_buffer_size) {
    return Status(TErrorCode::INTERNAL_ERROR,
      Substitute("HDFS scan min reservation $0 must be >= min buffer size $1",
       resource_profile_.min_reservation, min_buffer_size));
  }
  // Add per volume stats to the runtime profile
  PerVolumeStats per_volume_stats;
  stringstream str;
  UpdateHdfsSplitStats(*scan_range_params_, &per_volume_stats);
  PrintHdfsSplitStats(per_volume_stats, &str);
  runtime_profile()->AddInfoString("Table Name", hdfs_table_->fully_qualified_name());
  runtime_profile()->AddInfoString(HDFS_SPLIT_STATS_DESC, str.str());
  return Status::OK();
}

void HdfsScanPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  for (THdfsFileFormat::type format: scanned_file_formats_) {
    llvm::Function* fn;
    Status status;
    switch (format) {
      case THdfsFileFormat::TEXT:
        status = HdfsTextScanner::Codegen(this, state, &fn);
        break;
      case THdfsFileFormat::SEQUENCE_FILE:
        status = HdfsSequenceScanner::Codegen(this, state, &fn);
        break;
      case THdfsFileFormat::AVRO:
        status = HdfsAvroScanner::Codegen(this, state, &fn);
        break;
      case THdfsFileFormat::PARQUET:
      case THdfsFileFormat::ORC:
        status = HdfsColumnarScanner::Codegen(this, state, &fn);
        break;
      default:
        // No codegen for this format
        fn = NULL;
        status = Status::Expected("Not implemented for this format.");
    }
    DCHECK(fn != NULL || !status.ok());
    const char* format_name = _THdfsFileFormat_VALUES_TO_NAMES.find(format)->second;
    if (status.ok()) {
      LlvmCodeGen* codegen = state->codegen();
      DCHECK(codegen != NULL);
      codegen->AddFunctionToJit(
          fn, &codegend_fn_map_[static_cast<THdfsFileFormat::type>(format)]);
    }
    AddCodegenStatus(status, format_name);
  }
}

Status HdfsScanNodeBase::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Open(state));

  // Open collection conjuncts
  for (auto& entry: conjunct_evals_map_) {
    // conjuncts_ are already opened in ExecNode::Open()
    if (entry.first == tuple_id_) continue;
    RETURN_IF_ERROR(ScalarExprEvaluator::Open(entry.second, state));
  }

  // Open min max conjuncts
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(min_max_conjunct_evals_, state));

  // Create template tuples for all partitions.
  for (int64_t partition_id: partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    DCHECK(partition_desc != NULL) << "table_id=" << hdfs_table_->id()
                                   << " partition_id=" << partition_id
                                   << "\n" << PrintThrift(state->instance_ctx());
    partition_template_tuple_map_[partition_id] = InitTemplateTuple(
        partition_desc->partition_key_value_evals(), scan_node_pool_.get(), state);
  }

  RETURN_IF_ERROR(ClaimBufferReservation(state));
  reader_context_ = ExecEnv::GetInstance()->disk_io_mgr()->RegisterContext();

  // Initialize HdfsScanNode specific counters
  hdfs_read_timer_ = PROFILE_TotalRawHdfsReadTime.Instantiate(runtime_profile());
  hdfs_open_file_timer_ =
      PROFILE_TotalRawHdfsOpenFileTime.Instantiate(runtime_profile());
  per_read_thread_throughput_counter_ =
      PROFILE_PerReadThreadRawHdfsThroughput.Instantiate(runtime_profile(),
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_read_counter_,
      hdfs_read_timer_));
  scan_ranges_complete_counter_ =
      PROFILE_ScanRangesComplete.Instantiate(runtime_profile());
  collection_items_read_counter_ =
      PROFILE_CollectionItemsRead.Instantiate(runtime_profile());
  if (DiskInfo::num_disks() < 64) {
    num_disks_accessed_counter_ =
        PROFILE_NumDisksAccessed.Instantiate(runtime_profile());
  } else {
    num_disks_accessed_counter_ = NULL;
  }

  data_cache_hit_count_ = PROFILE_DataCacheHitCount.Instantiate(runtime_profile());
  data_cache_partial_hit_count_ =
      PROFILE_DataCachePartialHitCount.Instantiate(runtime_profile());
  data_cache_miss_count_ = PROFILE_DataCacheMissCount.Instantiate(runtime_profile());
  data_cache_hit_bytes_ = PROFILE_DataCacheHitBytes.Instantiate(runtime_profile());
  data_cache_miss_bytes_ = PROFILE_DataCacheMissBytes.Instantiate(runtime_profile());

  reader_context_->set_bytes_read_counter(bytes_read_counter());
  reader_context_->set_read_timer(hdfs_read_timer_);
  reader_context_->set_open_file_timer(hdfs_open_file_timer_);
  reader_context_->set_active_read_thread_counter(&active_hdfs_read_thread_counter_);
  reader_context_->set_disks_accessed_bitmap(&disks_accessed_bitmap_);
  reader_context_->set_data_cache_hit_counter(data_cache_hit_count_);
  reader_context_->set_data_cache_partial_hit_counter(data_cache_partial_hit_count_);
  reader_context_->set_data_cache_miss_counter(data_cache_miss_count_);
  reader_context_->set_data_cache_hit_bytes_counter(data_cache_hit_bytes_);
  reader_context_->set_data_cache_miss_bytes_counter(data_cache_miss_bytes_);

  average_hdfs_read_thread_concurrency_ =
      PROFILE_AverageHdfsReadThreadConcurrency.Instantiate(runtime_profile(),
      &active_hdfs_read_thread_counter_);

  initial_range_ideal_reservation_stats_ =
      PROFILE_InitialRangeIdealReservation.Instantiate(runtime_profile());
  initial_range_actual_reservation_stats_ =
      PROFILE_InitialRangeActualReservation.Instantiate(runtime_profile());

  uncompressed_bytes_read_per_column_counter_ =
      PROFILE_ParquetUncompressedBytesReadPerColumn.Instantiate(runtime_profile());
  compressed_bytes_read_per_column_counter_ =
      PROFILE_ParquetCompressedBytesReadPerColumn.Instantiate(runtime_profile());

  bytes_read_local_ = PROFILE_BytesReadLocal.Instantiate(runtime_profile());
  bytes_read_short_circuit_ =
      PROFILE_BytesReadShortCircuit.Instantiate(runtime_profile());
  bytes_read_dn_cache_ = PROFILE_BytesReadDataNodeCache.Instantiate(runtime_profile());
  num_remote_ranges_ = PROFILE_RemoteScanRanges.Instantiate(runtime_profile());
  unexpected_remote_bytes_ =
      PROFILE_BytesReadRemoteUnexpected.Instantiate(runtime_profile());
  cached_file_handles_hit_count_ =
      PROFILE_CachedFileHandlesHitCount.Instantiate(runtime_profile());
  cached_file_handles_miss_count_ =
      PROFILE_CachedFileHandlesMissCount.Instantiate(runtime_profile());

  max_compressed_text_file_length_ =
      PROFILE_MaxCompressedTextFileLength.Instantiate(runtime_profile());

  scanner_io_wait_time_ = PROFILE_ScannerIoWaitTime.Instantiate(runtime_profile());
  hdfs_read_thread_concurrency_bucket_ = runtime_profile()->AddBucketingCounters(
      &active_hdfs_read_thread_counter_,
      ExecEnv::GetInstance()->disk_io_mgr()->num_total_disks() + 1);

  counters_running_ = true;

  int64_t total_splits = 0;
  for (const auto& fd: file_descs_) total_splits += fd.second->splits.size();
  progress_.Init(Substitute("Splits complete (node=$0)", id_), total_splits);
  return Status::OK();
}

Status HdfsScanNodeBase::Reset(RuntimeState* state, RowBatch* row_batch) {
  DCHECK(false) << "Internal error: Scan nodes should not appear in subplans.";
  return Status("Internal error: Scan nodes should not appear in subplans.");
}

void HdfsScanNodeBase::Close(RuntimeState* state) {
  if (is_closed()) return;

  if (reader_context_ != nullptr) {
    // Need to wait for all the active scanner threads to finish to ensure there is no
    // more memory tracked by this scan node's mem tracker.
    ExecEnv::GetInstance()->disk_io_mgr()->UnregisterContext(reader_context_.get());
  }

  StopAndFinalizeCounters();

  // There should be no active hdfs read threads.
  DCHECK_EQ(active_hdfs_read_thread_counter_.value(), 0);

  if (scan_node_pool_.get() != NULL) scan_node_pool_->FreeAll();

  // Close collection conjuncts
  for (auto& tid_conjunct_eval : conjunct_evals_map_) {
    // ExecNode::conjunct_evals_ are already closed in ExecNode::Close()
    if (tid_conjunct_eval.first == tuple_id_) continue;
    ScalarExprEvaluator::Close(tid_conjunct_eval.second, state);
  }

  // Close min max conjunct
  ScalarExprEvaluator::Close(min_max_conjunct_evals_, state);
  ScanNode::Close(state);
}

Status HdfsScanNodeBase::IssueInitialScanRanges(RuntimeState* state) {
  DCHECK(!initial_ranges_issued_.Load());
  initial_ranges_issued_.Store(true);
  // We want to decrement this remaining_scan_range_submissions in all cases.
  auto remaining_scan_range_submissions_trigger =
    MakeScopeExitTrigger([&](){ UpdateRemainingScanRangeSubmissions(-1); });

  // No need to issue ranges with limit 0.
  if (ReachedLimitShared()) {
    DCHECK_EQ(limit_, 0);
    return Status::OK();
  }

  if (filter_ctxs_.size() > 0) WaitForRuntimeFilters();
  // Apply dynamic partition-pruning per-file.
  FileFormatsMap matching_per_type_files;
  for (const FileFormatsMap::value_type& v: per_type_files_) {
    vector<HdfsFileDesc*>* matching_files = &matching_per_type_files[v.first];
    for (HdfsFileDesc* file: v.second) {
      if (FilePassesFilterPredicates(filter_ctxs_, v.first, file)) {
        matching_files->push_back(file);
      } else {
        SkipFile(v.first, file);
      }
    }
    // Randomize the order this node processes the files. We want to do this to avoid
    // issuing remote reads to the same DN from different impalads. In file formats such
    // as avro/seq/rc (i.e. splittable with a header), every node first reads the header.
    // If every node goes through the files in the same order, all the remote reads are
    // for the same file meaning a few DN serves a lot of remote reads at the same time.
    random_shuffle(matching_files->begin(), matching_files->end());
  }

  // Issue initial ranges for all file types. Only call functions for file types that
  // actually exist - trying to add empty lists of ranges can result in spurious
  // CANCELLED errors - see IMPALA-6564.
  for (const auto& entry : matching_per_type_files) {
    if (entry.second.empty()) continue;
    switch (entry.first) {
      case THdfsFileFormat::PARQUET:
        RETURN_IF_ERROR(HdfsParquetScanner::IssueInitialRanges(this, entry.second));
        break;
      case THdfsFileFormat::TEXT:
        RETURN_IF_ERROR(HdfsTextScanner::IssueInitialRanges(this, entry.second));
        break;
      case THdfsFileFormat::SEQUENCE_FILE:
      case THdfsFileFormat::RC_FILE:
      case THdfsFileFormat::AVRO:
        RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this, entry.second));
        break;
      case THdfsFileFormat::ORC:
        RETURN_IF_ERROR(HdfsOrcScanner::IssueInitialRanges(this, entry.second));
        break;
      default:
        DCHECK(false) << "Unexpected file type " << entry.first;
    }
  }
  // Except for BaseSequenceScanner, IssueInitialRanges() takes care of
  // issuing all the ranges. For BaseSequenceScanner, IssueInitialRanges()
  // will have incremented the counter.
  return Status::OK();
}

bool HdfsScanNodeBase::FilePassesFilterPredicates(
    const vector<FilterContext>& filter_ctxs, const THdfsFileFormat::type& format,
    HdfsFileDesc* file) {
#ifndef NDEBUG
  if (FLAGS_skip_file_runtime_filtering) return true;
#endif
  if (filter_ctxs_.size() == 0) return true;
  ScanRangeMetadata* metadata =
      static_cast<ScanRangeMetadata*>(file->splits[0]->meta_data());
  if (!PartitionPassesFilters(metadata->partition_id, FilterStats::FILES_KEY,
          filter_ctxs)) {
    return false;
  }
  return true;
}

void HdfsScanNodeBase::SkipScanRange(io::ScanRange* scan_range) {
  // Avoid leaking unread buffers in scan_range.
  scan_range->Cancel(Status::CancelledInternal("HDFS partition pruning"));
  ScanRangeMetadata* metadata = static_cast<ScanRangeMetadata*>(scan_range->meta_data());
  int64_t partition_id = metadata->partition_id;
  HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
  DCHECK(partition != nullptr) << "table_id=" << hdfs_table_->id()
                               << " partition_id=" << partition_id << "\n"
                               << PrintThrift(runtime_state_->instance_ctx());
  HdfsFileDesc* desc = GetFileDesc(partition_id, *scan_range->file_string());
  if (metadata->is_sequence_header) {
    // File ranges haven't been issued yet, skip entire file.
    UpdateRemainingScanRangeSubmissions(-1);
    SkipFile(partition->file_format(), desc);
  } else {
    // Mark this scan range as done.
    HdfsScanNodeBase::RangeComplete(
        partition->file_format(), desc->file_compression, true);
  }
}

Status HdfsScanNodeBase::StartNextScanRange(const std::vector<FilterContext>& filter_ctxs,
    int64_t* reservation, ScanRange** scan_range) {
  DiskIoMgr* io_mgr = ExecEnv::GetInstance()->disk_io_mgr();
  bool needs_buffers;
  // Loop until we've got a scan range or run out of ranges.
  do {
    RETURN_IF_ERROR(reader_context_->GetNextUnstartedRange(scan_range, &needs_buffers));
    if (*scan_range == nullptr) return Status::OK();
    if (filter_ctxs.size() > 0) {
      int64_t partition_id =
          static_cast<ScanRangeMetadata*>((*scan_range)->meta_data())->partition_id;
      if (!PartitionPassesFilters(partition_id, FilterStats::SPLITS_KEY, filter_ctxs)) {
        SkipScanRange(*scan_range);
        *scan_range = nullptr;
      }
    }
  } while (*scan_range == nullptr);
  if (needs_buffers) {
    // Check if we should increase our reservation to read this range more efficiently.
    // E.g. if we are scanning a large text file, we might want extra I/O buffers to
    // improve throughput. Note that if this is a columnar format like Parquet,
    // '*scan_range' is the small footer range only so we won't request an increase.
    int64_t ideal_scan_range_reservation =
        io_mgr->ComputeIdealBufferReservation((*scan_range)->bytes_to_read());
    *reservation = IncreaseReservationIncrementally(*reservation, ideal_scan_range_reservation);
    initial_range_ideal_reservation_stats_->UpdateCounter(ideal_scan_range_reservation);
    initial_range_actual_reservation_stats_->UpdateCounter(*reservation);
    RETURN_IF_ERROR(
        io_mgr->AllocateBuffersForRange(buffer_pool_client(), *scan_range, *reservation));
  }
  return Status::OK();
}

int64_t HdfsScanNodeBase::IncreaseReservationIncrementally(int64_t curr_reservation,
      int64_t ideal_reservation) {
  DiskIoMgr* io_mgr = ExecEnv::GetInstance()->disk_io_mgr();
  // Check if we could use at least one more max-sized I/O buffer for this range. Don't
  // increase in smaller increments since we may not be able to use additional smaller
  // buffers.
  while (curr_reservation < ideal_reservation) {
    // Increase to the next I/O buffer multiple or to the ideal reservation.
    int64_t target = min(ideal_reservation,
        BitUtil::RoundUpToPowerOf2(curr_reservation + 1, io_mgr->max_buffer_size()));
    DCHECK_LT(curr_reservation, target);
    bool increased = buffer_pool_client()->IncreaseReservation(target - curr_reservation);
    VLOG_FILE << "Increasing reservation from "
              << PrettyPrinter::PrintBytes(curr_reservation) << " to "
              << PrettyPrinter::PrintBytes(target) << " "
              << (increased ? "succeeded" : "failed");
    if (!increased) break;
    curr_reservation = target;
  }
  return curr_reservation;
}

ScanRange* HdfsScanNodeBase::AllocateScanRange(hdfsFS fs, const char* file,
    int64_t len, int64_t offset, int64_t partition_id, int disk_id, bool expected_local,
    bool is_erasure_coded, int64_t mtime,  const BufferOpts& buffer_opts,
    const ScanRange* original_split) {
  ScanRangeMetadata* metadata = runtime_state_->obj_pool()->Add(
        new ScanRangeMetadata(partition_id, original_split));
  return AllocateScanRange(fs, file, len, offset, {}, metadata, disk_id, expected_local,
      is_erasure_coded, mtime, buffer_opts);
}

ScanRange* HdfsScanNodeBase::AllocateScanRange(hdfsFS fs, const char* file,
    int64_t len, int64_t offset, ScanRangeMetadata* metadata, int disk_id,
    bool expected_local, bool is_erasure_coded, int64_t mtime,
    const BufferOpts& buffer_opts) {
  return AllocateScanRange(fs, file, len, offset, {}, metadata, disk_id, expected_local,
      is_erasure_coded, mtime, buffer_opts);
}

ScanRange* HdfsScanNodeBase::AllocateScanRange(hdfsFS fs, const char* file,
    int64_t len, int64_t offset, vector<ScanRange::SubRange>&& sub_ranges,
    int64_t partition_id, int disk_id, bool expected_local, bool is_erasure_coded,
    int64_t mtime, const BufferOpts& buffer_opts, const ScanRange* original_split) {
  ScanRangeMetadata* metadata = runtime_state_->obj_pool()->Add(
      new ScanRangeMetadata(partition_id, original_split));
  return AllocateScanRange(fs, file, len, offset, move(sub_ranges), metadata,
      disk_id, expected_local, is_erasure_coded, mtime, buffer_opts);
}

ScanRange* HdfsScanNodeBase::AllocateScanRange(hdfsFS fs, const char* file,
    int64_t len, int64_t offset, vector<ScanRange::SubRange>&& sub_ranges,
    ScanRangeMetadata* metadata, int disk_id, bool expected_local, bool is_erasure_coded,
    int64_t mtime, const BufferOpts& buffer_opts) {
  DCHECK_GE(disk_id, -1);
  // Require that the scan range is within [0, file_length). While this cannot be used
  // to guarantee safety (file_length metadata may be stale), it avoids different
  // behavior between Hadoop FileSystems (e.g. s3n hdfsSeek() returns error when seeking
  // beyond the end of the file).
  DCHECK_GE(offset, 0);
  DCHECK_GE(len, 0);
  DCHECK_LE(offset + len, GetFileDesc(metadata->partition_id, file)->file_length)
      << "Scan range beyond end of file (offset=" << offset << ", len=" << len << ")";
  disk_id = ExecEnv::GetInstance()->disk_io_mgr()->AssignQueue(
      file, disk_id, expected_local);

  ScanRange* range = runtime_state_->obj_pool()->Add(new ScanRange);
  range->Reset(fs, file, len, offset, disk_id, expected_local, is_erasure_coded, mtime,
      buffer_opts, move(sub_ranges), metadata);
  return range;
}

ScanRange* HdfsScanNodeBase::AllocateScanRange(hdfsFS fs, const char* file,
    int64_t len, int64_t offset, int64_t partition_id, int disk_id,
    int cache_options, bool expected_local, int64_t mtime,
    bool is_erasure_coded, const ScanRange* original_split) {
  return AllocateScanRange(fs, file, len, offset, partition_id, disk_id, expected_local,
      is_erasure_coded, mtime, BufferOpts(cache_options), original_split);
}

Status HdfsScanNodeBase::AddDiskIoRanges(const vector<ScanRange*>& ranges,
    EnqueueLocation enqueue_location) {
  DCHECK(!progress_.done()) << "Don't call AddScanRanges() after all ranges finished.";
  DCHECK_GT(remaining_scan_range_submissions_.Load(), 0);
  DCHECK_GT(ranges.size(), 0);
  return reader_context_->AddScanRanges(ranges, enqueue_location);
}

HdfsFileDesc* HdfsScanNodeBase::GetFileDesc(int64_t partition_id, const string& filename) {
  auto file_desc_map_key = make_pair(partition_id, filename);
  DCHECK(file_descs_.find(file_desc_map_key) != file_descs_.end());
  return file_descs_[file_desc_map_key];
}

void HdfsScanNodeBase::SetFileMetadata(
    int64_t partition_id, const string& filename, void* metadata) {
  unique_lock<mutex> l(metadata_lock_);
  auto file_metadata_map_key = make_pair(partition_id, filename);
  DCHECK(per_file_metadata_.find(file_metadata_map_key) == per_file_metadata_.end());
  per_file_metadata_[file_metadata_map_key] = metadata;
}

void* HdfsScanNodeBase::GetFileMetadata(
    int64_t partition_id, const string& filename) {
  unique_lock<mutex> l(metadata_lock_);
  auto file_metadata_map_key = make_pair(partition_id, filename);
  auto it = per_file_metadata_.find(file_metadata_map_key);
  if (it == per_file_metadata_.end()) return NULL;
  return it->second;
}

void* HdfsScanNodeBase::GetCodegenFn(THdfsFileFormat::type type) {
  auto it = codegend_fn_map_.find(type);
  if (it == codegend_fn_map_.end()) return NULL;
  return it->second;
}

Status HdfsScanNodeBase::CreateAndOpenScannerHelper(HdfsPartitionDescriptor* partition,
    ScannerContext* context, scoped_ptr<HdfsScanner>* scanner) {
  DCHECK(context != nullptr);
  DCHECK(scanner->get() == nullptr);
  THdfsCompression::type compression =
      context->GetStream()->file_desc()->file_compression;

  // Create a new scanner for this file format and compression.
  switch (partition->file_format()) {
    case THdfsFileFormat::TEXT:
      if (HdfsTextScanner::HasBuiltinSupport(compression)) {
        scanner->reset(new HdfsTextScanner(this, runtime_state_));
      } else {
        // No builtin support - we must have loaded the plugin in IssueInitialRanges().
        auto it = _THdfsCompression_VALUES_TO_NAMES.find(compression);
        DCHECK(it != _THdfsCompression_VALUES_TO_NAMES.end())
            << "Already issued ranges for this compression type.";
        scanner->reset(HdfsPluginTextScanner::GetHdfsPluginTextScanner(
            this, runtime_state_, it->second));
      }
      break;
    case THdfsFileFormat::SEQUENCE_FILE:
      scanner->reset(new HdfsSequenceScanner(this, runtime_state_));
      break;
    case THdfsFileFormat::RC_FILE:
      scanner->reset(new HdfsRCFileScanner(this, runtime_state_));
      break;
    case THdfsFileFormat::AVRO:
      scanner->reset(new HdfsAvroScanner(this, runtime_state_));
      break;
    case THdfsFileFormat::PARQUET:
      scanner->reset(new HdfsParquetScanner(this, runtime_state_));
      break;
    case THdfsFileFormat::ORC:
      scanner->reset(new HdfsOrcScanner(this, runtime_state_));
      break;
    default:
      return Status(Substitute("Unknown Hdfs file format type: $0",
          partition->file_format()));
  }
  DCHECK(scanner->get() != nullptr);
  RETURN_IF_ERROR(scanner->get()->Open(context));
  // Inject the error after the scanner is opened, to test the scanner close path.
  return ScanNodeDebugAction(TExecNodePhase::PREPARE_SCANNER);
}

Tuple* HdfsScanNodeBase::InitTemplateTuple(const vector<ScalarExprEvaluator*>& evals,
    MemPool* pool, RuntimeState* state) const {
  if (partition_key_slots_.empty()) return NULL;
  Tuple* template_tuple = Tuple::Create(tuple_desc_->byte_size(), pool);
  for (int i = 0; i < partition_key_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = partition_key_slots_[i];
    ScalarExprEvaluator* eval = evals[slot_desc->col_pos()];
    // Exprs guaranteed to be literals, so can safely be evaluated without a row.
    RawValue::Write(eval->GetValue(NULL), template_tuple, slot_desc, NULL);
  }
  return template_tuple;
}

void HdfsScanNodeBase::InitNullCollectionValues(const TupleDescriptor* tuple_desc,
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

void HdfsScanNodeBase::InitNullCollectionValues(RowBatch* row_batch) const {
  DCHECK_EQ(row_batch->row_desc()->tuple_descriptors().size(), 1);
  const TupleDescriptor& tuple_desc =
      *row_batch->row_desc()->tuple_descriptors()[0];
  if (tuple_desc.collection_slots().empty()) return;
  for (int i = 0; i < row_batch->num_rows(); ++i) {
    Tuple* tuple = row_batch->GetRow(i)->GetTuple(0);
    DCHECK(tuple != NULL);
    InitNullCollectionValues(&tuple_desc, tuple);
  }
}

bool HdfsScanNodeBase::PartitionPassesFilters(int32_t partition_id,
    const string& stats_name, const vector<FilterContext>& filter_ctxs) {
  if (filter_ctxs.empty()) return true;
  if (FilterContext::CheckForAlwaysFalse(stats_name, filter_ctxs)) return false;
  DCHECK_EQ(filter_ctxs.size(), filter_ctxs_.size())
      << "Mismatched number of filter contexts";
  Tuple* template_tuple = partition_template_tuple_map_[partition_id];
  // Defensive - if template_tuple is NULL, there can be no filters on partition columns.
  if (template_tuple == nullptr) return true;
  TupleRow* tuple_row_mem = reinterpret_cast<TupleRow*>(&template_tuple);
  for (const FilterContext& ctx: filter_ctxs) {
    int target_ndx = ctx.filter->filter_desc().planid_to_target_ndx.at(id_);
    if (!ctx.filter->filter_desc().targets[target_ndx].is_bound_by_partition_columns) {
      continue;
    }

    bool has_filter = ctx.filter->HasFilter();
    bool passed_filter = !has_filter || ctx.Eval(tuple_row_mem);
    ctx.stats->IncrCounters(stats_name, 1, has_filter, !passed_filter);
    if (!passed_filter) return false;
  }

  return true;
}

void HdfsScanNodeBase::RangeComplete(const THdfsFileFormat::type& file_type,
    const THdfsCompression::type& compression_type, bool skipped) {
  vector<THdfsCompression::type> types;
  types.push_back(compression_type);
  RangeComplete(file_type, types, skipped);
}

void HdfsScanNodeBase::RangeComplete(const THdfsFileFormat::type& file_type,
    const vector<THdfsCompression::type>& compression_types, bool skipped) {
  scan_ranges_complete_counter_->Add(1);
  progress_.Update(1);
  HdfsCompressionTypesSet compression_set;
  for (int i = 0; i < compression_types.size(); ++i) {
    compression_set.AddType(compression_types[i]);
  }
  ++file_type_counts_[std::make_tuple(file_type, skipped, compression_set)];
}

void HdfsScanNodeBase::SkipFile(const THdfsFileFormat::type& file_type,
    HdfsFileDesc* file) {
  for (int i = 0; i < file->splits.size(); ++i) {
    RangeComplete(file_type, file->file_compression, true);
  }
}

void HdfsScanPlanNode::ComputeSlotMaterializationOrder(
    const DescriptorTbl& desc_tbl, vector<int>* order) const {
  const vector<ScalarExpr*>& conjuncts = conjuncts_;
  // Initialize all order to be conjuncts.size() (after the last conjunct)
  order->insert(order->begin(), materialized_slots_.size(), conjuncts.size());

  vector<SlotId> slot_ids;
  for (int conjunct_idx = 0; conjunct_idx < conjuncts.size(); ++conjunct_idx) {
    slot_ids.clear();
    int num_slots = conjuncts[conjunct_idx]->GetSlotIds(&slot_ids);
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

void HdfsScanNodeBase::TransferToScanNodePool(MemPool* pool) {
  scan_node_pool_->AcquireData(pool, false);
}

void HdfsScanNodeBase::UpdateHdfsSplitStats(
    const vector<TScanRangeParams>& scan_range_params_list,
    PerVolumeStats* per_volume_stats) {
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

void HdfsScanNodeBase::PrintHdfsSplitStats(const PerVolumeStats& per_volume_stats,
    stringstream* ss) {
  for (PerVolumeStats::const_iterator i = per_volume_stats.begin();
       i != per_volume_stats.end(); ++i) {
     (*ss) << i->first << ":" << i->second.first << "/"
         << PrettyPrinter::Print(i->second.second, TUnit::BYTES) << " ";
  }
}

void HdfsScanNodeBase::StopAndFinalizeCounters() {
  if (!counters_running_) return;
  counters_running_ = false;

  runtime_profile_->StopPeriodicCounters();

  // Output hdfs read thread concurrency into info string
  stringstream ss;
  for (int i = 0; i < hdfs_read_thread_concurrency_bucket_->size(); ++i) {
    ss << i << ":" << setprecision(4)
       << (*hdfs_read_thread_concurrency_bucket_)[i]->double_value() << "% ";
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
      for (FileTypeCountsMap::const_iterator it = file_type_counts_.begin();
          it != file_type_counts_.end(); ++it) {

        THdfsFileFormat::type file_format = std::get<0>(it->first);
        bool skipped = std::get<1>(it->first);
        HdfsCompressionTypesSet compressions_set = std::get<2>(it->first);
        int file_cnt = it->second;

        if (skipped) {
          if (file_format == THdfsFileFormat::PARQUET) {
            // If a scan range stored as parquet is skipped, its compression type
            // cannot be figured out without reading the data.
            ss << PrintThriftEnum(file_format) << "/" << "Unknown" << "(Skipped):"
               << file_cnt << " ";
          } else {
            ss << PrintThriftEnum(file_format) << "/"
               << PrintThriftEnum(compressions_set.GetFirstType()) << "(Skipped):"
               << file_cnt << " ";
          }
        } else if (compressions_set.Size() == 1) {
          ss << PrintThriftEnum(file_format) << "/"
             << PrintThriftEnum(compressions_set.GetFirstType()) << ":" << file_cnt
             << " ";
        } else {
          ss << PrintThriftEnum(file_format) << "/" << "(";
          bool first = true;
          for (auto& elem : _THdfsCompression_VALUES_TO_NAMES) {
            THdfsCompression::type type = static_cast<THdfsCompression::type>(
                elem.first);
            if (!compressions_set.HasType(type)) continue;
            if (!first) ss << ",";
            ss << PrintThriftEnum(type);
            first = false;
          }
          ss << "):" << file_cnt << " ";
        }
      }
    }
    runtime_profile_->AddInfoString("File Formats", ss.str());
  }

  // Output fraction of scanners with codegen enabled
  int num_enabled = num_scanners_codegen_enabled_.Load();
  int total = num_enabled + num_scanners_codegen_disabled_.Load();
  runtime_profile()->AppendExecOption(
      Substitute("Codegen enabled: $0 out of $1", num_enabled, total));

  // Locking here should not be necessary since bytes_read_per_col_ is only updated inside
  // column readers, and all column readers should have completed at this point; however,
  // we acquire a read lock in case the update semantics of bytes_read_per_col_ change
  {
    shared_lock<shared_mutex> bytes_read_per_col_guard_read_lock(
        bytes_read_per_col_lock_);
    for (const auto& bytes_read : bytes_read_per_col_) {
      int64_t uncompressed_bytes_read = bytes_read.second.uncompressed_bytes_read.Load();
      if (uncompressed_bytes_read > 0) {
        uncompressed_bytes_read_per_column_counter_->UpdateCounter(
            uncompressed_bytes_read);
      }
      int64_t compressed_bytes_read = bytes_read.second.compressed_bytes_read.Load();
      if (compressed_bytes_read > 0) {
        compressed_bytes_read_per_column_counter_->UpdateCounter(compressed_bytes_read);
      }
    }
  }

  if (reader_context_ != nullptr) {
    bytes_read_local_->Set(reader_context_->bytes_read_local());
    bytes_read_short_circuit_->Set(reader_context_->bytes_read_short_circuit());
    bytes_read_dn_cache_->Set(reader_context_->bytes_read_dn_cache());
    num_remote_ranges_->Set(reader_context_->num_remote_ranges());
    unexpected_remote_bytes_->Set(reader_context_->unexpected_remote_bytes());
    cached_file_handles_hit_count_->Set(reader_context_->cached_file_handles_hit_count());
    cached_file_handles_miss_count_->Set(
        reader_context_->cached_file_handles_miss_count());

    if (unexpected_remote_bytes_->value() >= UNEXPECTED_REMOTE_BYTES_WARN_THRESHOLD) {
      runtime_state_->LogError(ErrorMsg(TErrorCode::GENERAL, Substitute(
          "Read $0 of data across network that was expected to be local. Block locality "
          "metadata for table '$1.$2' may be stale. This only affects query performance "
          "and not result correctness. One of the common causes for this warning is HDFS "
          "rebalancer moving some of the file's blocks. If the issue persists, consider "
          "running \"INVALIDATE METADATA `$1`.`$2`\".",
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

Status HdfsScanNodeBase::ScanNodeDebugAction(TExecNodePhase::type phase) {
  return ExecDebugAction(phase, runtime_state_);
}

void HdfsScanNodeBase::UpdateBytesRead(
    SlotId slot_id, int64_t uncompressed_bytes_read, int64_t compressed_bytes_read) {
  // Acquire a read lock first and check if the slot_id is in bytes_read_per_col_, if it
  // is then update the value and release the read lock; if not then release the read
  // lock, acquire the write lock, and then initialize the slot_id with the give value for
  // bytes_read
  shared_lock<shared_mutex> bytes_read_per_col_guard_read_lock(
      bytes_read_per_col_lock_);
  auto bytes_read_itr = bytes_read_per_col_.find(slot_id);
  if (bytes_read_itr != bytes_read_per_col_.end()) {
    bytes_read_itr->second.uncompressed_bytes_read.Add(uncompressed_bytes_read);
    bytes_read_itr->second.compressed_bytes_read.Add(compressed_bytes_read);
  } else {
    bytes_read_per_col_guard_read_lock.unlock();
    lock_guard<shared_mutex> bytes_read_per_col_guard_write_lock(
        bytes_read_per_col_lock_);
    bytes_read_per_col_[slot_id].uncompressed_bytes_read.Add(uncompressed_bytes_read);
    bytes_read_per_col_[slot_id].compressed_bytes_read.Add(compressed_bytes_read);
  }
}
