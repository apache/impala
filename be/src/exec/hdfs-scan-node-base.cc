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
#include "exec/hdfs-text-scanner.h"
#include "exec/hdfs-lzo-text-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-avro-scanner.h"
#include "exec/hdfs-parquet-scanner.h"

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
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/request-context.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "util/disk-info.h"
#include "util/hdfs-util.h"
#include "util/periodic-counter-updater.h"

#include "common/names.h"

#ifndef NDEBUG
DECLARE_bool(skip_file_runtime_filtering);
#endif

namespace filesystem = boost::filesystem;
using namespace impala;
using namespace impala::io;
using namespace strings;

const string HdfsScanNodeBase::HDFS_SPLIT_STATS_DESC =
    "Hdfs split stats (<volume id>:<# splits>/<split lengths>)";

// Determines how many unexpected remote bytes trigger an error in the runtime state
const int UNEXPECTED_REMOTE_BYTES_WARN_THRESHOLD = 64 * 1024 * 1024;

HdfsScanNodeBase::HdfsScanNodeBase(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      min_max_tuple_id_(tnode.hdfs_scan_node.__isset.min_max_tuple_id ?
          tnode.hdfs_scan_node.min_max_tuple_id : -1),
      skip_header_line_count_(tnode.hdfs_scan_node.__isset.skip_header_line_count ?
          tnode.hdfs_scan_node.skip_header_line_count : 0),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      optimize_parquet_count_star_(
          tnode.hdfs_scan_node.__isset.parquet_count_star_slot_offset),
      parquet_count_star_slot_offset_(
          tnode.hdfs_scan_node.__isset.parquet_count_star_slot_offset ?
          tnode.hdfs_scan_node.parquet_count_star_slot_offset : -1),
      tuple_desc_(descs.GetTupleDescriptor(tuple_id_)),
      thrift_dict_filter_conjuncts_map_(
          tnode.hdfs_scan_node.__isset.dictionary_filter_conjuncts ?
          &tnode.hdfs_scan_node.dictionary_filter_conjuncts : nullptr),
      disks_accessed_bitmap_(TUnit::UNIT, 0),
      active_hdfs_read_thread_counter_(TUnit::UNIT, 0) {
}

HdfsScanNodeBase::~HdfsScanNodeBase() {
}

Status HdfsScanNodeBase::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Init(tnode, state));

  // Add collection item conjuncts
  for (const auto& entry: tnode.hdfs_scan_node.collection_conjuncts) {
    TupleDescriptor* tuple_desc = state->desc_tbl().GetTupleDescriptor(entry.first);
    RowDescriptor* collection_row_desc = state->obj_pool()->Add(
        new RowDescriptor(tuple_desc, /* is_nullable */ false));
    DCHECK(conjuncts_map_.find(entry.first) == conjuncts_map_.end());
    RETURN_IF_ERROR(ScalarExpr::Create(entry.second, *collection_row_desc, state,
        &conjuncts_map_[entry.first]));
  }
  DCHECK(conjuncts_map_[tuple_id_].empty());
  conjuncts_map_[tuple_id_] = conjuncts_;

  // Add min max conjuncts
  if (min_max_tuple_id_ != -1) {
    min_max_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(min_max_tuple_id_);
    DCHECK(min_max_tuple_desc_ != nullptr);
    RowDescriptor* min_max_row_desc = state->obj_pool()->Add(
        new RowDescriptor(min_max_tuple_desc_, /* is_nullable */ false));
    RETURN_IF_ERROR(ScalarExpr::Create(tnode.hdfs_scan_node.min_max_conjuncts,
        *min_max_row_desc, state, &min_max_conjuncts_));
  }

  return Status::OK();
}

/// TODO: Break up this very long function.
Status HdfsScanNodeBase::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ScanNode::Prepare(state));

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
  DCHECK(tuple_desc_->table_desc() != NULL);
  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
  scan_node_pool_.reset(new MemPool(mem_tracker()));

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
    file_path.append(split.file_name, filesystem::path::codecvt());
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
      RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
          native_file_path, &file_desc->fs, &fs_cache));
      num_unqueued_files_.Add(1);
      per_type_files_[partition_desc->file_format()].push_back(file_desc);
    } else {
      // File already processed
      file_desc = file_desc_it->second;
    }

    bool expected_local = params.__isset.is_remote && !params.is_remote;
    if (expected_local && params.volume_id == -1) ++num_ranges_missing_volume_id;

    bool try_cache = params.is_cached;
    file_desc->splits.push_back(
        AllocateScanRange(file_desc->fs, file_desc->filename.c_str(), split.length,
            split.offset, split.partition_id, params.volume_id, expected_local,
            BufferOpts(try_cache, file_desc->mtime)));
  }

  // Update server wide metrics for number of scan ranges and ranges that have
  // incomplete metadata.
  ImpaladMetrics::NUM_RANGES_PROCESSED->Increment(scan_range_params_->size());
  ImpaladMetrics::NUM_RANGES_MISSING_VOLUME_ID->Increment(num_ranges_missing_volume_id);

  // Add per volume stats to the runtime profile
  PerVolumeStats per_volume_stats;
  stringstream str;
  UpdateHdfsSplitStats(*scan_range_params_, &per_volume_stats);
  PrintHdfsSplitStats(per_volume_stats, &str);
  runtime_profile()->AddInfoString(HDFS_SPLIT_STATS_DESC, str.str());
  AddCodegenDisabledMessage(state);
  return Status::OK();
}

void HdfsScanNodeBase::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  // Create codegen'd functions
  for (int format = THdfsFileFormat::TEXT; format <= THdfsFileFormat::PARQUET; ++format) {
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
    llvm::Function* fn;
    Status status;
    switch (format) {
      case THdfsFileFormat::TEXT:
        status = HdfsTextScanner::Codegen(this, conjuncts_, &fn);
        break;
      case THdfsFileFormat::SEQUENCE_FILE:
        status = HdfsSequenceScanner::Codegen(this, conjuncts_, &fn);
        break;
      case THdfsFileFormat::AVRO:
        status = HdfsAvroScanner::Codegen(this, conjuncts_, &fn);
        break;
      case THdfsFileFormat::PARQUET:
        status = HdfsParquetScanner::Codegen(this, conjuncts_, &fn);
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
      codegen->AddFunctionToJit(fn,
          &codegend_fn_map_[static_cast<THdfsFileFormat::type>(format)]);
    }
    runtime_profile()->AddCodegenMsg(status.ok(), status, format_name);
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

  reader_context_ = runtime_state_->io_mgr()->RegisterContext(mem_tracker());

  // Initialize HdfsScanNode specific counters
  // TODO: Revisit counters and move the counters specific to multi-threaded scans
  // into HdfsScanNode.
  read_timer_ = ADD_TIMER(runtime_profile(), TOTAL_HDFS_READ_TIMER);
  open_file_timer_ = ADD_TIMER(runtime_profile(), TOTAL_HDFS_OPEN_FILE_TIMER);
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

  runtime_state_->io_mgr()->set_bytes_read_counter(
      reader_context_.get(), bytes_read_counter());
  runtime_state_->io_mgr()->set_read_timer(reader_context_.get(), read_timer());
  runtime_state_->io_mgr()->set_open_file_timer(reader_context_.get(), open_file_timer());
  runtime_state_->io_mgr()->set_active_read_thread_counter(
      reader_context_.get(), &active_hdfs_read_thread_counter_);
  runtime_state_->io_mgr()->set_disks_access_bitmap(
      reader_context_.get(), &disks_accessed_bitmap_);

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
  cached_file_handles_hit_count_ = ADD_COUNTER(runtime_profile(),
      "CachedFileHandlesHitCount", TUnit::UNIT);
  cached_file_handles_miss_count_ = ADD_COUNTER(runtime_profile(),
      "CachedFileHandlesMissCount", TUnit::UNIT);

  max_compressed_text_file_length_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxCompressedTextFileLength", TUnit::BYTES);

  hdfs_read_thread_concurrency_bucket_ = runtime_profile()->AddBucketingCounters(
      &active_hdfs_read_thread_counter_, state->io_mgr()->num_total_disks() + 1);

  counters_running_ = true;

  int64_t total_splits = 0;
  for (const auto& fd: file_descs_) total_splits += fd.second->splits.size();
  progress_.Init(Substitute("Splits complete (node=$0)", total_splits), total_splits);
  return Status::OK();
}

Status HdfsScanNodeBase::Reset(RuntimeState* state) {
  DCHECK(false) << "Internal error: Scan nodes should not appear in subplans.";
  return Status("Internal error: Scan nodes should not appear in subplans.");
}

void HdfsScanNodeBase::Close(RuntimeState* state) {
  if (is_closed()) return;

  if (reader_context_ != nullptr) {
    // Need to wait for all the active scanner threads to finish to ensure there is no
    // more memory tracked by this scan node's mem tracker.
    state->io_mgr()->UnregisterContext(reader_context_.get());
  }

  StopAndFinalizeCounters();

  // There should be no active scanner threads and hdfs read threads.
  DCHECK_EQ(active_scanner_thread_counter_.value(), 0);
  DCHECK_EQ(active_hdfs_read_thread_counter_.value(), 0);

  if (scan_node_pool_.get() != NULL) scan_node_pool_->FreeAll();

  // Close collection conjuncts
  for (auto& tid_conjunct: conjuncts_map_) {
    // conjuncts_ are already closed in ExecNode::Close()
    if (tid_conjunct.first == tuple_id_) continue;
    ScalarExprEvaluator::Close(conjunct_evals_map_[tid_conjunct.first], state);
    ScalarExpr::Close(tid_conjunct.second);
  }

  // Close min max conjunct
  ScalarExprEvaluator::Close(min_max_conjunct_evals_, state);
  ScalarExpr::Close(min_max_conjuncts_);
  ScanNode::Close(state);
}

Status HdfsScanNodeBase::IssueInitialScanRanges(RuntimeState* state) {
  DCHECK(!initial_ranges_issued_);
  initial_ranges_issued_ = true;

  // No need to issue ranges with limit 0.
  if (ReachedLimit()) {
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
    SkipFile(format, file);
    return false;
  }
  return true;
}

ScanRange* HdfsScanNodeBase::AllocateScanRange(hdfsFS fs, const char* file,
    int64_t len, int64_t offset, int64_t partition_id, int disk_id, bool expected_local,
    const BufferOpts& buffer_opts,
    const ScanRange* original_split) {
  ScanRangeMetadata* metadata = runtime_state_->obj_pool()->Add(
        new ScanRangeMetadata(partition_id, original_split));
  return AllocateScanRange(fs, file, len, offset, metadata, disk_id, expected_local,
      buffer_opts);
}

ScanRange* HdfsScanNodeBase::AllocateScanRange(hdfsFS fs, const char* file,
    int64_t len, int64_t offset, ScanRangeMetadata* metadata, int disk_id, bool expected_local,
    const BufferOpts& buffer_opts) {
  DCHECK_GE(disk_id, -1);
  // Require that the scan range is within [0, file_length). While this cannot be used
  // to guarantee safety (file_length metadata may be stale), it avoids different
  // behavior between Hadoop FileSystems (e.g. s3n hdfsSeek() returns error when seeking
  // beyond the end of the file).
  DCHECK_GE(offset, 0);
  DCHECK_GE(len, 0);
  DCHECK_LE(offset + len, GetFileDesc(metadata->partition_id, file)->file_length)
      << "Scan range beyond end of file (offset=" << offset << ", len=" << len << ")";
  disk_id = runtime_state_->io_mgr()->AssignQueue(file, disk_id, expected_local);

  ScanRange* range = runtime_state_->obj_pool()->Add(new ScanRange);
  range->Reset(fs, file, len, offset, disk_id, expected_local, buffer_opts, metadata);
  return range;
}

ScanRange* HdfsScanNodeBase::AllocateScanRange(hdfsFS fs, const char* file,
    int64_t len, int64_t offset, int64_t partition_id, int disk_id, bool try_cache,
    bool expected_local, int mtime, const ScanRange* original_split) {
  return AllocateScanRange(fs, file, len, offset, partition_id, disk_id, expected_local,
      BufferOpts(try_cache, mtime), original_split);
}

Status HdfsScanNodeBase::AddDiskIoRanges(
    const vector<ScanRange*>& ranges, int num_files_queued) {
  RETURN_IF_ERROR(runtime_state_->io_mgr()->AddScanRanges(reader_context_.get(), ranges));
  num_unqueued_files_.Add(-num_files_queued);
  DCHECK_GE(num_unqueued_files_.Load(), 0);
  return Status::OK();
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
  CodegendFnMap::iterator it = codegend_fn_map_.find(type);
  if (it == codegend_fn_map_.end()) return NULL;
  return it->second;
}

Status HdfsScanNodeBase::CreateAndOpenScanner(HdfsPartitionDescriptor* partition,
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
        scanner->reset(new HdfsTextScanner(this, runtime_state_));
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
    default:
      return Status(Substitute("Unknown Hdfs file format type: $0",
          partition->file_format()));
  }
  DCHECK(scanner->get() != NULL);
  Status status = ScanNodeDebugAction(TExecNodePhase::PREPARE_SCANNER);
  if (status.ok()) {
    status = scanner->get()->Open(context);
    if (!status.ok()) {
      scanner->get()->Close(nullptr);
      scanner->reset();
    }
  } else {
    context->ClearStreams();
    scanner->reset();
  }
  return status;
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
      *row_batch->row_desc()->tuple_descriptors()[tuple_idx()];
  if (tuple_desc.collection_slots().empty()) return;
  for (int i = 0; i < row_batch->num_rows(); ++i) {
    Tuple* tuple = row_batch->GetRow(i)->GetTuple(tuple_idx());
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
  scan_ranges_complete_counter()->Add(1);
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

void HdfsScanNodeBase::ComputeSlotMaterializationOrder(vector<int>* order) const {
  const vector<ScalarExpr*>& conjuncts = ExecNode::conjuncts();
  // Initialize all order to be conjuncts.size() (after the last conjunct)
  order->insert(order->begin(), materialized_slots().size(), conjuncts.size());

  const DescriptorTbl& desc_tbl = runtime_state_->desc_tbl();

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
            ss << file_format << "/" << "Unknown" << "(Skipped):" << file_cnt << " ";
          } else {
            ss << file_format << "/" << compressions_set.GetFirstType() << "(Skipped):"
               << file_cnt << " ";
          }
        } else if (compressions_set.Size() == 1) {
          ss << file_format << "/" << compressions_set.GetFirstType() << ":" << file_cnt
             << " ";
        } else {
          ss << file_format << "/" << "(";
          bool first = true;
          for (auto& elem : _THdfsCompression_VALUES_TO_NAMES) {
            THdfsCompression::type type = static_cast<THdfsCompression::type>(
                elem.first);
            if (!compressions_set.HasType(type)) continue;
            if (!first) ss << ",";
            ss << type;
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

  if (reader_context_ != nullptr) {
    bytes_read_local_->Set(
        runtime_state_->io_mgr()->bytes_read_local(reader_context_.get()));
    bytes_read_short_circuit_->Set(
        runtime_state_->io_mgr()->bytes_read_short_circuit(reader_context_.get()));
    bytes_read_dn_cache_->Set(
        runtime_state_->io_mgr()->bytes_read_dn_cache(reader_context_.get()));
    num_remote_ranges_->Set(static_cast<int64_t>(
        runtime_state_->io_mgr()->num_remote_ranges(reader_context_.get())));
    unexpected_remote_bytes_->Set(
        runtime_state_->io_mgr()->unexpected_remote_bytes(reader_context_.get()));
    cached_file_handles_hit_count_->Set(
        runtime_state_->io_mgr()->cached_file_handles_hit_count(reader_context_.get()));
    cached_file_handles_miss_count_->Set(
        runtime_state_->io_mgr()->cached_file_handles_miss_count(reader_context_.get()));

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

Status HdfsScanNodeBase::ScanNodeDebugAction(TExecNodePhase::type phase) {
  return ExecDebugAction(phase, runtime_state_);
}
