// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/hdfs-scan-node.h"
#include "exec/base-sequence-scanner.h"
#include "exec/hdfs-text-scanner.h"
#include "exec/hdfs-lzo-text-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-avro-scanner.h"
#include "exec/hdfs-parquet-scanner.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include <hdfs.h>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exprs/expr-context.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "util/bit-util.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/error-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

DEFINE_int32(max_row_batches, 0, "the maximum size of materialized_row_batches_");
DECLARE_string(cgroup_hierarchy_path);
DECLARE_bool(enable_rm);

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;
using namespace strings;

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

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      thrift_plan_node_(new TPlanNode(tnode)),
      runtime_state_(NULL),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      requires_compaction_(tnode.compact_data),
      reader_context_(NULL),
      tuple_desc_(NULL),
      unknown_disk_id_warned_(false),
      initial_ranges_issued_(false),
      scanner_thread_bytes_required_(0),
      num_partition_keys_(0),
      disks_accessed_bitmap_(TCounterType::UNIT, 0),
      done_(false),
      all_ranges_started_(false),
      counters_running_(false),
      rm_callback_id_(-1) {
  max_materialized_row_batches_ = FLAGS_max_row_batches;
  if (max_materialized_row_batches_ <= 0) {
    // TODO: This parameter has an U-shaped effect on performance: increasing the value
    // would first improves performance, but further increasing would degrade performance.
    // Investigate and tune this.
    max_materialized_row_batches_ = 10 * DiskInfo::num_disks();
  }
  materialized_row_batches_.reset(new RowBatchQueue(max_materialized_row_batches_));
}

HdfsScanNode::~HdfsScanNode() {
}

Status HdfsScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  if (!initial_ranges_issued_) {
    // We do this in GetNext() to ensure that all execution time predicates have
    // been generated (e.g. probe side bitmap filters).
    // TODO: we could do dynamic partition pruning here as well.
    initial_ranges_issued_ = true;
    // Issue initial ranges for all file types.
    RETURN_IF_ERROR(HdfsTextScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::TEXT]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::SEQUENCE_FILE]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::RC_FILE]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::AVRO]));
    RETURN_IF_ERROR(HdfsParquetScanner::IssueInitialRanges(this,
          per_type_files_[THdfsFileFormat::PARQUET]));
    if (progress_.done()) SetDone();
  }

  Status status = GetNextInternal(state, row_batch, eos);
  if (status.IsMemLimitExceeded()) state->SetMemLimitExceeded();
  if (!status.ok() || *eos) StopAndFinalizeCounters();
  return status;
}

Status HdfsScanNode::GetNextInternal(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  {
    unique_lock<mutex> l(lock_);
    if (ReachedLimit() || !status_.ok()) {
      *eos = true;
      return status_;
    }
  }
  *eos = false;

  RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    num_owned_io_buffers_ -= materialized_batch->num_io_buffers();
    row_batch->AcquireState(materialized_batch);
    // Update the number of materialized rows instead of when they are materialized.
    // This means that scanners might process and queue up more rows than are necessary
    // for the limit case but we want to avoid the synchronized writes to
    // num_rows_returned_
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
    return Status::OK;
  }

  *eos = true;
  return Status::OK;
}

DiskIoMgr::ScanRange* HdfsScanNode::AllocateScanRange(const char* file, int64_t len,
    int64_t offset, int64_t partition_id, int disk_id, bool try_cache,
    bool expected_local) {
  DCHECK_GE(disk_id, -1);
  if (disk_id == -1) {
    // disk id is unknown, assign it a random one.
    static int next_disk_id = 0;
    disk_id = next_disk_id++;
  }

  // TODO: we need to parse the config for the number of dirs configured for this
  // data node.
  disk_id %= runtime_state_->io_mgr()->num_disks();

  ScanRangeMetadata* metadata =
      runtime_state_->obj_pool()->Add(new ScanRangeMetadata(partition_id));
  DiskIoMgr::ScanRange* range =
      runtime_state_->obj_pool()->Add(new DiskIoMgr::ScanRange());
  range->Reset(file, len, offset, disk_id, try_cache, expected_local, metadata);
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

HdfsScanner* HdfsScanNode::CreateAndPrepareScanner(HdfsPartitionDescriptor* partition,
    ScannerContext* context, Status* status) {
  DCHECK(context != NULL);
  HdfsScanner* scanner = NULL;
  THdfsCompression::type compression =
      context->GetStream()->file_desc()->file_compression;

  // Create a new scanner for this file format and compression.
  switch (partition->file_format()) {
    case THdfsFileFormat::TEXT:
      // Lzo-compressed text files are scanned by a scanner that it is implemented as a
      // dynamic library, so that Impala does not include GPL code.
      if (compression == THdfsCompression::LZO) {
        scanner = HdfsLzoTextScanner::GetHdfsLzoTextScanner(this, runtime_state_);
      } else {
        scanner = new HdfsTextScanner(this, runtime_state_);
      }
      break;
    case THdfsFileFormat::SEQUENCE_FILE:
      scanner = new HdfsSequenceScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::RC_FILE:
      scanner = new HdfsRCFileScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::AVRO:
      scanner = new HdfsAvroScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::PARQUET:
      scanner = new HdfsParquetScanner(this, runtime_state_);
      break;
    default:
      DCHECK(false) << "Unknown Hdfs file format type:" << partition->file_format();
      return NULL;
  }
  DCHECK(scanner != NULL);
  runtime_state_->obj_pool()->Add(scanner);
  *status = scanner->Prepare(context);
  return scanner;
}

Tuple* HdfsScanNode::InitTemplateTuple(RuntimeState* state,
                                       const vector<ExprContext*>& value_ctxs) {
  if (partition_key_slots_.empty()) return NULL;

  // Look to protect access to partition_key_pool_ and value_ctxs
  // TODO: we can push the lock to the mempool and exprs_values should not
  // use internal memory.
  Tuple* template_tuple = InitEmptyTemplateTuple();

  unique_lock<mutex> l(lock_);
  for (int i = 0; i < partition_key_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = partition_key_slots_[i];
    // Exprs guaranteed to be literals, so can safely be evaluated without a row context
    void* value = value_ctxs[slot_desc->col_pos()]->GetValue(NULL);
    RawValue::Write(value, template_tuple, slot_desc, NULL);
  }
  return template_tuple;
}

Tuple* HdfsScanNode::InitEmptyTemplateTuple() {
  Tuple* template_tuple = NULL;
  {
    unique_lock<mutex> l(lock_);
    template_tuple = Tuple::Create(tuple_desc_->byte_size(), scan_node_pool_.get());
  }
  memset(template_tuple, 0, tuple_desc_->byte_size());
  return template_tuple;
}

void HdfsScanNode::TransferToScanNodePool(MemPool* pool) {
  unique_lock<mutex> l(lock_);
  scan_node_pool_->AcquireData(pool, false);
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  runtime_state_ = state;
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);

  if (!state->cgroup().empty()) {
    scanner_threads_.SetCgroupsMgr(state->exec_env()->cgroups_mgr());
    scanner_threads_.SetCgroup(state->cgroup());
  }

  // One-time initialisation of state that is constant across scan ranges
  DCHECK(tuple_desc_->table_desc() != NULL);
  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
  scan_node_pool_.reset(new MemPool(mem_tracker()));

  // If there are no materialized string cols, we never need to compact data
  // (it is already compact).
  requires_compaction_ &= tuple_desc_->string_slots().size() > 0;

  // Create mapping from column index in table to slot index in output tuple.
  // First, initialize all columns to SKIP_COLUMN.
  int num_cols = hdfs_table_->num_cols();
  column_idx_to_materialized_slot_idx_.resize(num_cols);
  for (int i = 0; i < num_cols; ++i) {
    column_idx_to_materialized_slot_idx_[i] = SKIP_COLUMN;
  }

  num_partition_keys_ = hdfs_table_->num_clustering_cols();

  // Next, collect all materialized (partition key and not) slots
  vector<SlotDescriptor*> all_materialized_slots;
  all_materialized_slots.resize(num_cols);
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (size_t i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    int col_idx = slots[i]->col_pos();
    DCHECK_LT(col_idx, column_idx_to_materialized_slot_idx_.size());
    DCHECK_EQ(column_idx_to_materialized_slot_idx_[col_idx], SKIP_COLUMN);
    all_materialized_slots[col_idx] = slots[i];
  }

  // Finally, populate materialized_slots_ and partition_key_slots_ in the order that
  // the slots appear in the file.
  for (int i = 0; i < num_cols; ++i) {
    SlotDescriptor* slot_desc = all_materialized_slots[i];
    if (slot_desc == NULL) continue;
    if (hdfs_table_->IsClusteringCol(slot_desc)) {
      partition_key_slots_.push_back(slot_desc);
    } else {
      DCHECK_GE(i, num_partition_keys_);
      column_idx_to_materialized_slot_idx_[i] = materialized_slots_.size();
      materialized_slots_.push_back(slot_desc);
    }
  }

  // Initialize is_materialized_col_
  is_materialized_col_.resize(column_idx_to_materialized_slot_idx_.size());
  for (int i = 0; i < is_materialized_col_.size(); ++i) {
    is_materialized_col_[i] = column_idx_to_materialized_slot_idx_[i] != SKIP_COLUMN;
  }
  // TODO: don't assume all partitions are on the same filesystem.
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
      hdfs_table_->hdfs_base_dir(), &hdfs_connection_));
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
      file_desc->file_compression = split.file_compression;

      if (partition_desc == NULL) {
        stringstream ss;
        ss << "Could not find partition with id: " << split.partition_id;
        return Status(ss.str());
      }
      ++num_unqueued_files_;
      per_type_files_[partition_desc->file_format()].push_back(file_desc);
    } else {
      // File already processed
      file_desc = file_desc_it->second;
    }

    if ((*scan_range_params_)[i].volume_id == -1) {
      if (!unknown_disk_id_warned_) {
        AddRuntimeExecOption("Missing Volume Id");
        runtime_state()->LogError(
          "Unknown disk id.  This will negatively affect performance. "
          "Check your hdfs settings to enable block location metadata.");
        unknown_disk_id_warned_ = true;
      }
      ++num_ranges_missing_volume_id;
    }

    bool try_cache = (*scan_range_params_)[i].is_cached;
    bool expected_local = (*scan_range_params_)[i].__isset.is_remote &&
                          !(*scan_range_params_)[i].is_remote;
    if (runtime_state_->query_options().disable_cached_reads) {
      DCHECK(!try_cache) << "Params should not have had this set.";
    }
    file_desc->splits.push_back(
        AllocateScanRange(file_desc->filename.c_str(), split.length, split.offset,
                          split.partition_id, (*scan_range_params_)[i].volume_id,
                          try_cache, expected_local));
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
  BOOST_FOREACH(HdfsFileDesc* file, per_type_files_[THdfsFileFormat::TEXT]) {
    if (file->file_compression != THdfsCompression::NONE) {
      int64_t bytes_required = file->file_length * COMPRESSED_TEXT_COMPRESSION_RATIO;
      scanner_thread_mem_usage = ::max(bytes_required, scanner_thread_mem_usage);
    }
  }
  scanner_thread_bytes_required_ += scanner_thread_mem_usage;

  // Prepare all the partitions scanned by the scan node
  BOOST_FOREACH(const int64_t& partition_id, partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    DCHECK(partition_desc != NULL);
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

  // Initialize conjunct exprs
  RETURN_IF_ERROR(Expr::CreateExprTrees(
      runtime_state_->obj_pool(), thrift_plan_node_->conjuncts, &conjunct_ctxs_));
  RETURN_IF_ERROR(
      Expr::Prepare(conjunct_ctxs_, runtime_state_, row_desc(), expr_mem_tracker()));
  AddExprCtxsToFree(conjunct_ctxs_);

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
    Function* fn;
    switch (format) {
      case THdfsFileFormat::TEXT:
        fn = HdfsTextScanner::Codegen(this, conjunct_ctxs_);
        break;
      case THdfsFileFormat::SEQUENCE_FILE:
        fn = HdfsSequenceScanner::Codegen(this, conjunct_ctxs_);
        break;
      case THdfsFileFormat::AVRO:
        fn = HdfsAvroScanner::Codegen(this, conjunct_ctxs_);
        break;
      default:
        // No codegen for this format
        fn = NULL;
    }
    if (fn != NULL) {
      LlvmCodeGen* codegen;
      RETURN_IF_ERROR(runtime_state_->GetCodegen(&codegen));
      codegen->AddFunctionToJit(
          fn, &codegend_fn_map_[static_cast<THdfsFileFormat::type>(format)]);
    }
  }

  return Status::OK;
}

// This function initiates the connection to hdfs and starts up the initial scanner
// threads. The scanner subclasses are passed the initial splits.  Scanners are expected
// to queue up a non-zero number of those splits to the io mgr (via the ScanNode).
Status HdfsScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));

  // We need at least one scanner thread to make progress. We need to make this
  // reservation before any ranges are issued.
  runtime_state_->resource_pool()->ReserveOptionalTokens(1);
  if (runtime_state_->query_options().num_scanner_threads > 0) {
    runtime_state_->resource_pool()->set_max_quota(
        runtime_state_->query_options().num_scanner_threads);
  }

  runtime_state_->resource_pool()->SetThreadAvailableCb(
      bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this, _1));

  if (runtime_state_->query_resource_mgr() != NULL) {
    rm_callback_id_ = runtime_state_->query_resource_mgr()->AddVcoreAvailableCb(
        bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this,
            runtime_state_->resource_pool()));
  }

  if (file_descs_.empty()) {
    SetDone();
    return Status::OK;
  }

  // Open all the partition exprs used by the scan node
  BOOST_FOREACH(const int64_t& partition_id, partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    DCHECK(partition_desc != NULL);
    RETURN_IF_ERROR(partition_desc->OpenExprs(state));
  }

  // Open all conjuncts
  Expr::Open(conjunct_ctxs_, state);

  RETURN_IF_ERROR(runtime_state_->io_mgr()->RegisterContext(
      hdfs_connection_, &reader_context_, mem_tracker()));

  // Initialize HdfsScanNode specific counters
  read_timer_ = ADD_TIMER(runtime_profile(), TOTAL_HDFS_READ_TIMER);
  per_read_thread_throughput_counter_ = runtime_profile()->AddDerivedCounter(
      PER_READ_THREAD_THROUGHPUT_COUNTER, TCounterType::BYTES_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_read_counter_, read_timer_));
  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TCounterType::UNIT);
  if (DiskInfo::num_disks() < 64) {
    num_disks_accessed_counter_ =
        ADD_COUNTER(runtime_profile(), NUM_DISKS_ACCESSED_COUNTER, TCounterType::UNIT);
  } else {
    num_disks_accessed_counter_ = NULL;
  }
  num_scanner_threads_started_counter_ =
      ADD_COUNTER(runtime_profile(), NUM_SCANNER_THREADS_STARTED, TCounterType::UNIT);

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
      TCounterType::BYTES);
  bytes_read_short_circuit_ = ADD_COUNTER(runtime_profile(), "BytesReadShortCircuit",
      TCounterType::BYTES);
  bytes_read_dn_cache_ = ADD_COUNTER(runtime_profile(), "BytesReadDataNodeCache",
      TCounterType::BYTES);
  num_remote_ranges_ = ADD_COUNTER(runtime_profile(), "RemoteScanRanges",
      TCounterType::UNIT);
  unexpected_remote_bytes_ = ADD_COUNTER(runtime_profile(), "BytesReadRemoteUnexpected",
      TCounterType::BYTES);

  max_compressed_text_file_length_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxCompressedTextFileLength", TCounterType::BYTES);

  // Create num_disks+1 bucket counters
  for (int i = 0; i < state->io_mgr()->num_disks() + 1; ++i) {
    hdfs_read_thread_concurrency_bucket_.push_back(
        pool_->Add(new RuntimeProfile::Counter(TCounterType::DOUBLE_VALUE, 0)));
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
    return Status::OK;
  }

  stringstream ss;
  ss << "Splits complete (node=" << id() << "):";
  progress_ = ProgressUpdater(ss.str(), total_splits);

  return Status::OK;
}

void HdfsScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SetDone();

  state->resource_pool()->SetThreadAvailableCb(NULL);
  if (state->query_resource_mgr() != NULL && rm_callback_id_ != -1) {
    state->query_resource_mgr()->RemoveVcoreAvailableCb(rm_callback_id_);
  }

  scanner_threads_.JoinAll();

  num_owned_io_buffers_ -= materialized_row_batches_->Cleanup();
  DCHECK_EQ(num_owned_io_buffers_, 0) << "ScanNode has leaked io buffers";

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

  // Close all conjuncts
  Expr::Close(conjunct_ctxs_, state);

  // Close all the partitions scanned by the scan node
  BOOST_FOREACH(const int64_t& partition_id, partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    DCHECK(partition_desc != NULL);
    partition_desc->CloseExprs(state);
  }

  ScanNode::Close(state);
}

Status HdfsScanNode::AddDiskIoRanges(const vector<DiskIoMgr::ScanRange*>& ranges) {
  RETURN_IF_ERROR(
      runtime_state_->io_mgr()->AddScanRanges(reader_context_, ranges));
  ThreadTokenAvailableCb(runtime_state_->resource_pool());
  return Status::OK;
}

Status HdfsScanNode::AddDiskIoRanges(const HdfsFileDesc* desc) {
  const vector<DiskIoMgr::ScanRange*>& ranges = desc->splits;
  RETURN_IF_ERROR(
      runtime_state_->io_mgr()->AddScanRanges(reader_context_, ranges));
  MarkFileDescIssued(desc);
  ThreadTokenAvailableCb(runtime_state_->resource_pool());
  return Status::OK;
}

void HdfsScanNode::MarkFileDescIssued(const HdfsFileDesc* desc) {
  DCHECK_GT(num_unqueued_files_, 0);
  --num_unqueued_files_;
}

void HdfsScanNode::AddMaterializedRowBatch(RowBatch* row_batch) {
  materialized_row_batches_->AddBatch(row_batch);
}

Status HdfsScanNode::GetConjunctCtxs(vector<ExprContext*>* ctxs) {
  return Expr::Clone(conjunct_ctxs_, runtime_state_, ctxs);
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
  //  4. Don't start up a ScannerThread if materialized_row_batches_ is full since
  //     we are not scanner bound.
  //  5. Don't start up a thread if there isn't enough memory left to run it.
  //  6. Don't start up if there are no thread tokens.
  //  7. Don't start up if we are running too many threads for our vcore allocation
  //  (unless the thread is reserved, in which case it has to run).
  bool started_scanner = false;
  while (true) {
    // The lock must be given up between loops in order to give writers to done_,
    // all_ranges_started_ etc. a chance to grab the lock.
    // TODO: This still leans heavily on starvation-free locks, come up with a more
    // correct way to communicate between this method and ScannerThreadHelper
    unique_lock<mutex> lock(lock_);
    // Cases 1, 2, 3
    if (done_ || all_ranges_started_ ||
      active_scanner_thread_counter_.value() >= progress_.remaining()) {
      break;
    }

    // Cases 4 and 5
    if (active_scanner_thread_counter_.value() > 0 &&
        (materialized_row_batches_->GetSize() >= max_materialized_row_batches_ ||
         !EnoughMemoryForScannerThread(true))) {
      break;
    }

    // Case 6
    bool is_reserved = false;
    if (!pool->TryAcquireThreadToken(&is_reserved)) break;

    // Case 7
    if (!is_reserved) {
      if (runtime_state_->query_resource_mgr() != NULL &&
          runtime_state_->query_resource_mgr()->IsVcoreOverSubscribed()) {
        break;
      }
    }

    COUNTER_ADD(&active_scanner_thread_counter_, 1);
    COUNTER_ADD(num_scanner_threads_started_counter_, 1);
    stringstream ss;
    ss << "scanner-thread(" << num_scanner_threads_started_counter_->value() << ")";
    scanner_threads_.AddThread(
        new Thread("hdfs-scan-node", ss.str(), &HdfsScanNode::ScannerThread, this));
    started_scanner = true;

    if (runtime_state_->query_resource_mgr() != NULL) {
      runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(1);
    }
  }
  if (!started_scanner) ++num_skipped_tokens_;
}

void HdfsScanNode::ScannerThread() {
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  SCOPED_TIMER(runtime_state_->total_cpu_timer());

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
          return;
        }
      } else {
        // If this is the only scanner thread, it should keep running regardless
        // of resource constraints.
      }
    }

    DiskIoMgr::ScanRange* scan_range;
    // Take a snapshot of num_unqueued_files_ before calling GetNextRange().
    // We don't want num_unqueued_files_ to go to zero between the return from
    // GetNextRange() and the check for when all ranges are complete.
    int num_unqueued_files = num_unqueued_files_;
    AtomicUtil::MemoryBarrier();
    Status status = runtime_state_->io_mgr()->GetNextRange(reader_context_, &scan_range);

    if (status.ok() && scan_range != NULL) {
      // Got a scan range. Create a new scanner object and process the range
      // end to end (in this thread).
      ScanRangeMetadata* metadata =
          reinterpret_cast<ScanRangeMetadata*>(scan_range->meta_data());
      int64_t partition_id = metadata->partition_id;
      HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
      DCHECK(partition != NULL);

      ScannerContext* context = runtime_state_->obj_pool()->Add(
          new ScannerContext(runtime_state_, this, partition, scan_range));
      Status scanner_status;
      HdfsScanner* scanner = CreateAndPrepareScanner(partition, context, &scanner_status);
      if (!scanner_status.ok() || scanner == NULL) {
        stringstream ss;
        ss << "Error preparing text scanner for scan range " << scan_range->file() <<
            "(" << scan_range->offset() << ":" << scan_range->len() << ").";
        ss << endl << runtime_state_->ErrorLog();
        VLOG_QUERY << ss.str();
      }

      status = scanner->ProcessSplit();
      if (!status.ok()) {
        // This thread hit an error, record it and bail
        // TODO: better way to report errors?  Maybe via the thrift interface?
        if (VLOG_QUERY_IS_ON && !runtime_state_->error_log().empty()) {
          stringstream ss;
          ss << "Scan node (id=" << id() << ") ran into a parse error for scan range "
             << scan_range->file() << "(" << scan_range->offset() << ":"
             << scan_range->len() << ").";
          if (partition->file_format() != THdfsFileFormat::PARQUET) {
            // Parquet doesn't read the range end to end so the current offset
            // isn't useful.
            // TODO: make sure the parquet reader is outputting as much diagnostic
            // information as possible.
            ScannerContext::Stream* stream = context->GetStream();
            ss << "  Processed " << stream->total_bytes_returned() << " bytes.";
          }
          ss << endl << runtime_state_->ErrorLog();
          VLOG_QUERY << ss.str();
        }
      }
      scanner->Close();
    }

    if (!status.ok()) {
      {
        unique_lock<mutex> l(lock_);
        // If there was already an error, the main thread will do the cleanup
        if (!status_.ok()) break;

        if (status.IsCancelled()) {
          // Scan node should be the only thing that initiated scanner threads to see
          // cancelled (i.e. limit reached).  No need to do anything here.
          DCHECK(done_);
          break;
        }
        status_ = status;
      }

      if (status.IsMemLimitExceeded()) runtime_state_->SetMemLimitExceeded();
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
      unique_lock<mutex> l(lock_);
      // All ranges have been queued and GetNextRange() returned NULL. This
      // means that every range is either done or being processed by
      // another thread.
      all_ranges_started_ = true;
      break;
    }
  }

  COUNTER_ADD(&active_scanner_thread_counter_, -1);
  if (runtime_state_->query_resource_mgr() != NULL) {
    runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(-1);
  }
  runtime_state_->resource_pool()->ReleaseThreadToken(false);
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
    ScopedSpinLock l(&file_type_counts_lock_);
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
      int slot_idx = GetMaterializedSlotIdx(slot_desc->col_pos());
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
      ScopedSpinLock l2(&file_type_counts_lock_);
      for (FileTypeCountsMap::const_iterator it = file_type_counts_.begin();
          it != file_type_counts_.end(); ++it) {
        ss << it->first.first << "/" << it->first.second << ":" << it->second << " ";
      }
    }
    runtime_profile_->AddInfoString("File Formats", ss.str());
  }

  // Output fraction of scanners with codegen enabled
  ss.str(std::string());
  ss << "Codegen enabled: " << num_scanners_codegen_enabled_ << " out of "
     << (num_scanners_codegen_enabled_ + num_scanners_codegen_disabled_);
  AddRuntimeExecOption(ss.str());

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

    if (unexpected_remote_bytes_->value() > 0) {
      runtime_state_->LogError(Substitute(
          "Block locality metadata for table '$0' may be stale. Consider running "
          "\"INVALIDATE METADATA $0\".", hdfs_table_->name()));
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
  BOOST_FOREACH(const TScanRangeParams& scan_range_params, scan_range_params_list) {
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
         << PrettyPrinter::Print(i->second.second, TCounterType::BYTES) << " ";
  }
}
