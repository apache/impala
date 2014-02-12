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

#include <hdfs.h>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exprs/expr.h"
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

const string HdfsScanNode::HDFS_SPLIT_STATS_DESC =
    "Hdfs split stats (<volume id>:<# splits>/<split lengths>)";

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      thrift_plan_node_(new TPlanNode(tnode)),
      runtime_state_(NULL),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      compact_data_(tnode.compact_data),
      reader_context_(NULL),
      tuple_desc_(NULL),
      unknown_disk_id_warned_(false),
      num_interpreted_conjuncts_copies_(0),
      num_partition_keys_(0),
      disks_accessed_bitmap_(TCounterType::UNIT, 0),
      done_(false),
      all_ranges_started_(false),
      counters_running_(false) {
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
  Status status = GetNextInternal(state, row_batch, eos);
  if (status.IsMemLimitExceeded()) state->SetMemLimitExceeded();
  if (!status.ok() || *eos) StopAndFinalizeCounters();
  return status;
}

Status HdfsScanNode::GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  {
    unique_lock<mutex> l(lock_);
    if (ReachedLimit() || !status_.ok()) {
      *eos = true;
      return status_;
    }
  }

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
    *eos = false;
    return Status::OK;
  }

  *eos = true;
  return Status::OK;
}

Status HdfsScanNode::CreateConjuncts(vector<Expr*>* expr, bool disable_codegen) {
  // This is only used when codegen is not possible for this node.  We don't want to
  // codegen the copy of the expr.
  // TODO: we really need to stop having to create copies of exprs
  RETURN_IF_ERROR(Expr::CreateExprTrees(runtime_state_->obj_pool(),
      thrift_plan_node_->conjuncts, expr));
  all_conjuncts_copies_.push_back(expr);
  RETURN_IF_ERROR(Expr::Prepare(*expr, runtime_state_, row_desc(), disable_codegen));
  return Status::OK;
}

DiskIoMgr::ScanRange* HdfsScanNode::AllocateScanRange(const char* file, int64_t len,
    int64_t offset, int64_t partition_id, int disk_id, bool try_cache) {
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
  range->Reset(file, len, offset, disk_id, try_cache, metadata);
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

Function* HdfsScanNode::GetCodegenFn(THdfsFileFormat::type type) {
  CodegendFnMap::iterator it = codegend_fn_map_.find(type);
  if (it == codegend_fn_map_.end()) return NULL;
  if (codegend_conjuncts_thread_safe_) {
    DCHECK_EQ(it->second.size(), 1);
    return it->second.front();
  } else {
    unique_lock<mutex> l(codgend_fn_map_lock_);
    // If all the codegen'd fn's are used, return NULL.  This disables codegen for
    // this scanner.
    if (it->second.empty()) return NULL;
    Function* fn = it->second.front();
    it->second.pop_front();
    DCHECK(fn != NULL);
    return fn;
  }
}

void HdfsScanNode::ReleaseCodegenFn(THdfsFileFormat::type type, Function* fn) {
  if (fn == NULL) return;
  if (codegend_conjuncts_thread_safe_) return;

  CodegendFnMap::iterator it = codegend_fn_map_.find(type);
  DCHECK(it != codegend_fn_map_.end());
  unique_lock<mutex> l(codgend_fn_map_lock_);
  it->second.push_back(fn);
}

vector<Expr*>* HdfsScanNode::GetConjuncts() {
  ScopedSpinLock l(&interpreted_conjuncts_copies_lock_);
  DCHECK(!interpreted_conjuncts_copies_.empty());
  vector<Expr*>* conjuncts = interpreted_conjuncts_copies_.back();
  interpreted_conjuncts_copies_.pop_back();
  return conjuncts;
}

void HdfsScanNode::ReleaseConjuncts(vector<Expr*>* conjuncts) {
  DCHECK(conjuncts != NULL);
  ScopedSpinLock l(&interpreted_conjuncts_copies_lock_);
  interpreted_conjuncts_copies_.push_back(conjuncts);
  DCHECK_LE(interpreted_conjuncts_copies_.size(), num_interpreted_conjuncts_copies_);
}

HdfsScanner* HdfsScanNode::CreateScanner(HdfsPartitionDescriptor* partition) {
  HdfsScanner* scanner = NULL;

  // Create a new scanner for this file format
  switch (partition->file_format()) {
    case THdfsFileFormat::TEXT:
      scanner = new HdfsTextScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::LZO_TEXT:
      scanner = HdfsLzoTextScanner::GetHdfsLzoTextScanner(this, runtime_state_);
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
  return scanner;
}

Tuple* HdfsScanNode::InitTemplateTuple(RuntimeState* state,
    const vector<Expr*>& expr_values) {
  if (partition_key_slots_.empty()) return NULL;

  // Look to protect access to partition_key_pool_ and expr_values
  // TODO: we can push the lock to the mempool and exprs_values should not
  // use internal memory.
  Tuple* template_tuple = InitEmptyTemplateTuple();

  unique_lock<mutex> l(lock_);
  for (int i = 0; i < partition_key_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = partition_key_slots_[i];
    // Exprs guaranteed to be literals, so can safely be evaluated without a row context
    void* value = expr_values[slot_desc->col_pos()]->GetValue(NULL);
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
  compact_data_ |= tuple_desc_->string_slots().empty();

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
    if (slots[i]->type() == TYPE_DECIMAL) {
      return Status("Decimal is not yet implemented.");
    }
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

  hdfs_connection_ = HdfsFsCache::instance()->GetDefaultConnection();
  if (hdfs_connection_ == NULL) {
    string error_msg = GetStrErrMsg();
    stringstream ss;
    ss << "Failed to connect to HDFS." << "\n" << error_msg;
    return Status(ss.str());
  }

  // Convert the TScanRangeParams into per-file DiskIO::ScanRange objects and populate
  // file_descs_ and per_type_files_.
  DCHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";

  unordered_set<int64_t> partition_id_set;
  int num_ranges_missing_volume_id = 0;
  for (int i = 0; i < scan_range_params_->size(); ++i) {
    DCHECK((*scan_range_params_)[i].scan_range.__isset.hdfs_file_split);
    const THdfsFileSplit& split = (*scan_range_params_)[i].scan_range.hdfs_file_split;
    const string& path = split.path;
    partition_id_set.insert(split.partition_id);

    HdfsFileDesc* file_desc = NULL;
    FileDescMap::iterator file_desc_it = file_descs_.find(path);
    if (file_desc_it == file_descs_.end()) {
      // Add new file_desc to file_descs_ and per_type_files_
      file_desc = runtime_state_->obj_pool()->Add(new HdfsFileDesc(path));
      file_descs_[path] = file_desc;
      file_desc->file_length = split.file_length;

      HdfsPartitionDescriptor* partition_desc =
          hdfs_table_->GetPartition(split.partition_id);
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
    if (runtime_state_->query_options().disable_cached_reads) {
      DCHECK(!try_cache) << "Params should not have had this set.";
    }
    file_desc->splits.push_back(
        AllocateScanRange(file_desc->filename.c_str(), split.length, split.offset,
                          split.partition_id, (*scan_range_params_)[i].volume_id,
                          try_cache));
  }

  // Prepare all the partitions scanned by the scan node
  BOOST_FOREACH(const int64_t& partition_id, partition_id_set) {
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

  RETURN_IF_ERROR(CreateConjunctsCopies(THdfsFileFormat::TEXT));
  RETURN_IF_ERROR(CreateConjunctsCopies(THdfsFileFormat::LZO_TEXT));
  RETURN_IF_ERROR(CreateConjunctsCopies(THdfsFileFormat::RC_FILE));
  RETURN_IF_ERROR(CreateConjunctsCopies(THdfsFileFormat::SEQUENCE_FILE));
  RETURN_IF_ERROR(CreateConjunctsCopies(THdfsFileFormat::AVRO));
  RETURN_IF_ERROR(CreateConjunctsCopies(THdfsFileFormat::PARQUET));

  num_interpreted_conjuncts_copies_ = interpreted_conjuncts_copies_.size();

  // We need at least one scanner thread to make progress. We need to make this
  // reservation before any ranges are issued.
  runtime_state_->resource_pool()->ReserveOptionalTokens(1);
  if (runtime_state_->query_options().num_scanner_threads > 0) {
    runtime_state_->resource_pool()->set_max_quota(
        runtime_state_->query_options().num_scanner_threads);
  }

  runtime_state_->resource_pool()->SetThreadAvailableCb(
      bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this, _1));

  if (FLAGS_enable_rm) {
    runtime_state_->query_resource_mgr()->AddVcoreAvailableCb(
        bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this,
            runtime_state_->resource_pool()));
  }

  return Status::OK;
}

// This function initiates the connection to hdfs and starts up the initial scanner
// threads. The scanner subclasses are passed the initial splits.  Scanners are expected
// to queue up a non-zero number of those splits to the io mgr (via the ScanNode).
Status HdfsScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));

  if (file_descs_.empty()) {
    SetDone();
    return Status::OK;
  }

  for (list<vector<Expr*>*>::iterator it = all_conjuncts_copies_.begin();
     it != all_conjuncts_copies_.end(); ++it) {
    RETURN_IF_ERROR(Expr::Open(**it, state));
  }

  RETURN_IF_ERROR(runtime_state_->io_mgr()->RegisterReader(
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
    num_disks_accessed_counter_ = 0;
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

  // Issue initial ranges for all file types.
  RETURN_IF_ERROR(
      HdfsTextScanner::IssueInitialRanges(this, per_type_files_[THdfsFileFormat::TEXT]));
  RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
      per_type_files_[THdfsFileFormat::SEQUENCE_FILE]));
  RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
      per_type_files_[THdfsFileFormat::RC_FILE]));
  RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
      per_type_files_[THdfsFileFormat::AVRO]));
  RETURN_IF_ERROR(HdfsParquetScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::PARQUET]));
  if (!per_type_files_[THdfsFileFormat::LZO_TEXT].empty()) {
    // This will dlopen the lzo binary and can fail if it is not present
    RETURN_IF_ERROR(HdfsLzoTextScanner::IssueInitialRanges(state,
        this, per_type_files_[THdfsFileFormat::LZO_TEXT]));
  }

  if (progress_.done()) SetDone();
  return Status::OK;
}

void HdfsScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SetDone();

  state->resource_pool()->SetThreadAvailableCb(NULL);
  scanner_threads_.JoinAll();

  // All conjuncts should have been released
  DCHECK_EQ(interpreted_conjuncts_copies_.size(), num_interpreted_conjuncts_copies_)
      << "interpreted_conjuncts_copies_ leak, check that ReleaseConjuncts() "
      << "is being called";

  num_owned_io_buffers_ -= materialized_row_batches_->Cleanup();
  DCHECK_EQ(num_owned_io_buffers_, 0) << "ScanNode has leaked io buffers";

  if (reader_context_ != NULL) {
    state->io_mgr()->UnregisterReader(reader_context_);
  }

  StopAndFinalizeCounters();

  // There should be no active scanner threads and hdfs read threads.
  DCHECK_EQ(active_scanner_thread_counter_.value(), 0);
  DCHECK_EQ(active_hdfs_read_thread_counter_.value(), 0);

  if (scan_node_pool_.get() != NULL) scan_node_pool_->FreeAll();

  for (list<vector<Expr*>*>::iterator it = all_conjuncts_copies_.begin();
     it != all_conjuncts_copies_.end(); ++it) {
    Expr::Close(**it, state);
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

void HdfsScanNode::ThreadTokenAvailableCb(ThreadResourceMgr::ResourcePool* pool) {
  // This is called to start up new scanner threads. It's not a big deal if we
  // spin up more than strictly necessary since they will go through and terminate
  // promptly. However, we want to minimize that by checking a conditions.
  //  1. Don't start up if the ScanNode is done
  //  2. Don't start up if all the ranges have been taken by another thread.
  //  3. Don't start up if the number of ranges left is less than the number of
  //     active scanner threads.
  //  4. Don't start up if there are no thread tokens.
  bool started_scanner = false;
  while (true) {
    if (FLAGS_enable_rm &&
        runtime_state_->query_resource_mgr()->IsVcoreOverSubscribed()) {
      break;
    }
    // The lock must be given up between loops in order to give writers to done_,
    // all_ranges_started_ etc. a chance to grab the lock.
    // TODO: This still leans heavily on starvation-free locks, come up with a more
    // correct way to communicate between this method and ScannerThreadHelper
    unique_lock<mutex> lock(lock_);
    if (done_ || all_ranges_started_ ||
      active_scanner_thread_counter_.value() >= progress_.remaining() ||
      !pool->TryAcquireThreadToken()) {
      break;
    }
    COUNTER_UPDATE(&active_scanner_thread_counter_, 1);
    COUNTER_UPDATE(num_scanner_threads_started_counter_, 1);
    stringstream ss;
    ss << "scanner-thread(" << num_scanner_threads_started_counter_->value() << ")";
    scanner_threads_.AddThread(
        new Thread("hdfs-scan-node", ss.str(), &HdfsScanNode::ScannerThread, this));
    started_scanner = true;

    if (FLAGS_enable_rm) runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(1);
  }
  if (!started_scanner) ++num_skipped_tokens_;
}

inline void HdfsScanNode::ScannerThreadHelper() {
  SCOPED_TIMER(runtime_state_->total_cpu_timer());
  while (!done_ && !runtime_state_->resource_pool()->optional_exceeded()) {
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
      HdfsScanner* scanner = CreateScanner(partition);
      status = scanner->Prepare(context);

      if (status.ok()) {
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
      }
      scanner->Close();
    }

    if (!status.ok()) {
      {
        unique_lock<mutex> l(lock_);
        // If there was already an error, the main thread will do the cleanup
        if (!status_.ok()) return;

        if (status.IsCancelled()) {
          // Scan node should be the only thing that initiated scanner threads to see
          // cancelled (i.e. limit reached).  No need to do anything here.
          DCHECK(done_);
          return;
        }
        status_ = status;
      }

      if (status.IsMemLimitExceeded()) runtime_state_->SetMemLimitExceeded();
      SetDone();
      return;
    }

    // Done with range and it completed successfully
    if (progress_.done()) {
      // All ranges are finished.  Indicate we are done.
      SetDone();
      return;
    }

    if (scan_range == NULL && num_unqueued_files == 0) {
      unique_lock<mutex> l(lock_);
      // All ranges have been queued and GetNextRange() returned NULL. This
      // means that every range is either done or being processed by
      // another thread.
      all_ranges_started_ = true;
      return;
    }
  }
}

void HdfsScanNode::ScannerThread() {
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  ScannerThreadHelper();
  COUNTER_UPDATE(&active_scanner_thread_counter_, -1);
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
  scan_ranges_complete_counter()->Update(1);
  progress_.Update(1);

  {
    unique_lock<mutex> l(file_type_counts_lock_);
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
    runtime_state_->io_mgr()->CancelReader(reader_context_);
  }
  materialized_row_batches_->Shutdown();
}

void HdfsScanNode::ComputeSlotMaterializationOrder(vector<int>* order) const {
  const vector<Expr*>& conjuncts = ExecNode::conjuncts();
  // Initialize all order to be conjuncts.size() (after the last conjunct)
  order->insert(order->begin(), materialized_slots().size(), conjuncts.size());

  const DescriptorTbl& desc_tbl = runtime_state_->desc_tbl();

  vector<SlotId> slot_ids;
  for (int conjunct_idx = 0; conjunct_idx < conjuncts.size(); ++conjunct_idx) {
    slot_ids.clear();
    int num_slots = conjuncts[conjunct_idx]->GetSlotIds(&slot_ids);
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
    unique_lock<mutex> l2(file_type_counts_lock_);
    stringstream ss;
    for (FileTypeCountsMap::const_iterator it = file_type_counts_.begin();
        it != file_type_counts_.end(); ++it) {
      ss << it->first.first << "/" << it->first.second
        << ":" << it->second << " ";
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

    ImpaladMetrics::IO_MGR_BYTES_READ->Increment(bytes_read_counter()->value());
    ImpaladMetrics::IO_MGR_LOCAL_BYTES_READ->Increment(
        bytes_read_local_->value());
    ImpaladMetrics::IO_MGR_SHORT_CIRCUIT_BYTES_READ->Increment(
        bytes_read_short_circuit_->value());
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

Status HdfsScanNode::CreateConjunctsCopies(THdfsFileFormat::type format) {
  // Nothing to do
  if (per_type_files_[format].empty()) return Status::OK;

  // This query's current thread quota
  int num_threads = runtime_state_->resource_pool()->num_available_threads();
  // The number of splits for this format
  int num_splits = 0;
  BOOST_FOREACH(HdfsFileDesc* desc, per_type_files_[format]) {
    num_splits += desc->splits.size();
  }

  int num_codegen_copies;
  if (!codegend_conjuncts_thread_safe_) {
    // If the codegen'd conjuncts are not thread safe, we need to make copies of the exprs
    // and codegen those as well.
    num_codegen_copies = min(num_threads, num_splits);
  } else {
    // Codegen function is thread safe, we can just use a single copy of the conjuncts
    num_codegen_copies = 1;
  }

  for (int i = 0; i < num_codegen_copies; ++i) {
    vector<Expr*>* conjuncts = pool_->Add(new vector<Expr*>());
    RETURN_IF_ERROR(CreateConjuncts(conjuncts, false));
    Function* fn;
    switch (format) {
      case THdfsFileFormat::TEXT:
      case THdfsFileFormat::LZO_TEXT:
        fn = HdfsTextScanner::Codegen(this, *conjuncts);
        break;
      case THdfsFileFormat::SEQUENCE_FILE:
        fn = HdfsSequenceScanner::Codegen(this, *conjuncts);
        break;
      case THdfsFileFormat::AVRO:
        fn = HdfsAvroScanner::Codegen(this, *conjuncts);
        break;
      default:
        // No codegen for this format
        fn = NULL;
    }
    if (fn != NULL) {
      codegend_fn_map_[format].push_back(fn);
    } else {
      break;
    }
  }

  // In addition to the codegen'd functions, create non-codegen'd copies of the conjuncts
  // for use with EvalConjuncts(), or as a fallback if our thread quota increases or one
  // of the Codegen() calls fails.

  // Since this is the fallback, create the maximum number of copies we'll possibly
  // need.
  int max_threads = runtime_state_->exec_env()->thread_mgr()->system_threads_quota();
  int num_noncodegen_copies = min(max_threads, num_splits);

  // No point making more copies than the maximum possible number of threads
  while (num_noncodegen_copies > 0 &&
      interpreted_conjuncts_copies_.size() < max_threads) {
    vector<Expr*>* conjuncts = pool_->Add(new vector<Expr*>());
    RETURN_IF_ERROR(CreateConjuncts(conjuncts, true));
    interpreted_conjuncts_copies_.push_back(conjuncts);
    --num_noncodegen_copies;
  }

  return Status::OK;
}
