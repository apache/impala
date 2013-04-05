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
#include <dlfcn.h>

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
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/runtime-profile.h"
#include "util/disk-info.h"

#include "gen-cpp/PlanNodes_types.h"

// TODO: temp change to validate we don't have an incast problem for joins with big tables
DEFINE_bool(randomize_splits, false, 
    "if true, randomizes the order of splits");
DEFINE_int32(max_row_batches, 0, "the maximum size of materialized_row_batches_");

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      thrift_plan_node_(new TPlanNode(tnode)),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      compact_data_(tnode.compact_data),
      reader_context_(NULL),
      tuple_desc_(NULL),
      unknown_disk_id_warned_(false),
      tuple_pool_(new MemPool()),
      num_unqueued_files_(0),
      scanner_pool_(new ObjectPool()),
      num_partition_keys_(0),
      num_owned_io_buffers_(0),
      done_(false),
      partition_key_pool_(new MemPool()),
      counters_reported_(false) {
  max_materialized_row_batches_ = FLAGS_max_row_batches;
  if (max_materialized_row_batches_ <= 0) {
    // TODO: This parameter has an U-shaped effect on performance: increasing the value
    // would first improves performance, but further increasing would degrade performance.
    // Investigate and tune this.
    max_materialized_row_batches_ = 10 * DiskInfo::num_disks();
  }
}

HdfsScanNode::~HdfsScanNode() {
}

Status HdfsScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT));
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  {
    unique_lock<recursive_mutex> l(lock_);
    if (per_file_splits_.size() == 0 || ReachedLimit() || !status_.ok()) {
      UpdateCounters();
      *eos = true;
      return status_;
    }
  }

  RowBatch* materialized_batch = NULL;
  {
    unique_lock<mutex> l(row_batches_lock_);
    while (materialized_row_batches_.empty() && !done_) {
      row_batch_added_cv_.wait(l);
    }

    // Return any errors
    if (!status_.ok()) return status_;

    if (!materialized_row_batches_.empty()) {
      materialized_batch = materialized_row_batches_.front();
      materialized_row_batches_.pop_front();
    }
  }

  if (materialized_batch != NULL) {
    __sync_fetch_and_add(
        &num_owned_io_buffers_, -1 * materialized_batch->num_io_buffers());
    row_batch_consumed_cv_.notify_one();
    row_batch->Swap(materialized_batch);
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
      UpdateCounters();
      // Wake up disk thread notifying it we are done.  This triggers tear down
      // of the scanner threads.
      {
        unique_lock<mutex> l(row_batches_lock_);
        done_ = true;
      }
      state->io_mgr()->CancelReader(reader_context_);
      row_batch_consumed_cv_.notify_all();
    }
    delete materialized_batch;
    *eos = false;
    return Status::OK;
  }

  *eos = true;
  UpdateCounters();

  return Status::OK;
}

Status HdfsScanNode::CreateConjuncts(vector<Expr*>* expr) {
  // This is only used when codegen is not possible for this node.  We don't want to
  // codegen the copy of the expr.  
  // TODO: we really need to stop having to create copies of exprs
  RETURN_IF_ERROR(Expr::CreateExprTrees(runtime_state_->obj_pool(), 
      thrift_plan_node_->conjuncts, expr));
  for (int i = 0; i < expr->size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare((*expr)[i], runtime_state_, row_desc(), true));
  }
  return Status::OK;
}

Status HdfsScanNode::SetScanRanges(const vector<TScanRangeParams>& scan_range_params) {
  // Convert the input ranges into per file DiskIO::ScanRange objects
  int num_ranges_missing_volume_id = 0;
  
  for (int i = 0; i < scan_range_params.size(); ++i) {
    DCHECK(scan_range_params[i].scan_range.__isset.hdfs_file_split);
    const THdfsFileSplit& split = scan_range_params[i].scan_range.hdfs_file_split;
    const string& path = split.path;

    HdfsFileDesc* desc = NULL;
    SplitsMap::iterator desc_it = per_file_splits_.find(path);
    if (desc_it == per_file_splits_.end()) {
      desc = runtime_state_->obj_pool()->Add(new HdfsFileDesc(path));
      per_file_splits_[path] = desc;
      desc->file_length = split.file_length;
    } else {
      desc = desc_it->second;
    }
      
    if (scan_range_params[i].volume_id == -1 && !unknown_disk_id_warned_) {
      LOG(WARNING) << "Unknown disk id.  This will negatively affect performance. "
                   << " Check your hdfs settings to enable block location metadata.";
      unknown_disk_id_warned_ = true;
      ++num_ranges_missing_volume_id;
    }

    desc->splits.push_back(AllocateScanRange(desc->filename.c_str(), 
       split.length, split.offset, split.partition_id, scan_range_params[i].volume_id));
  }

  // Update server wide metrics for number of scan ranges and ranges that have 
  // incomplete metadata.
  ImpaladMetrics::NUM_RANGES_PROCESSED->Increment(scan_range_params.size());
  ImpaladMetrics::NUM_RANGES_MISSING_VOLUME_ID->Increment(num_ranges_missing_volume_id);

  return Status::OK;
}

DiskIoMgr::ScanRange* HdfsScanNode::AllocateScanRange(const char* file, int64_t len,
    int64_t offset, int64_t partition_id, int disk_id, ScannerContext::Stream* stream) {
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
      runtime_state_->obj_pool()->Add(new ScanRangeMetadata(partition_id, stream));
  DiskIoMgr::ScanRange* range = 
      runtime_state_->obj_pool()->Add(new DiskIoMgr::ScanRange());
  range->Reset(file, len, offset, disk_id, metadata);

  return range;
}

HdfsFileDesc* HdfsScanNode::GetFileDesc(const string& filename) {
  DCHECK(per_file_splits_.find(filename) != per_file_splits_.end());
  return per_file_splits_[filename];
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
  return it->second;
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
  if (scanner != NULL) {
    scanner_pool_->Add(scanner);
    // TODO better error handling
    Status status = scanner->Prepare();
    DCHECK(status.ok());
  }
  return scanner;
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
  runtime_state_ = state;
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);

  // One-time initialisation of state that is constant across scan ranges
  DCHECK(tuple_desc_->table_desc() != NULL);
  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
  tuple_pool_->set_limits(*state->mem_limits());
  partition_key_pool_->set_limits(*state->mem_limits());
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
    int col_idx = slots[i]->col_pos();
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
  
  hdfs_connection_ = state->fs_cache()->GetDefaultConnection();
  if (hdfs_connection_ == NULL) {
    stringstream ss;
    ss << "Failed to connect to HDFS." << "\nError(" << errno << "):" << strerror(errno);
    return Status(ss.str());
  } 

  // Codegen scanner specific functions
  if (state->llvm_codegen() != NULL) {
    Function* text_fn = HdfsTextScanner::Codegen(this);
    Function* seq_fn = HdfsSequenceScanner::Codegen(this);
    if (text_fn != NULL) codegend_fn_map_[THdfsFileFormat::TEXT] = text_fn;
    if (seq_fn != NULL) codegend_fn_map_[THdfsFileFormat::SEQUENCE_FILE] = seq_fn;
  }
  
  return Status::OK;
}

Tuple* HdfsScanNode::InitTemplateTuple(RuntimeState* state,
    const vector<Expr*>& expr_values) {
  if (partition_key_slots_.empty()) return NULL;

  // Look to protect access to partition_key_pool_ and expr_values
  // TODO: we can push the lock to the mempool and exprs_values should not
  // use internal memory.
  unique_lock<recursive_mutex> l(lock_);
  Tuple* template_tuple = 
      reinterpret_cast<Tuple*>(partition_key_pool_->Allocate(tuple_desc_->byte_size()));
  memset(template_tuple, 0, tuple_desc_->byte_size());

  for (int i = 0; i < partition_key_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = partition_key_slots_[i];

    // Exprs guaranteed to be literals, so can safely be evaluated without a row context
    void* value = expr_values[slot_desc->col_pos()]->GetValue(NULL);
    RawValue::Write(value, template_tuple, slot_desc, NULL);
  }
  return template_tuple;
}

// This function initiates the connection to hdfs and starts up the disk thread.
// Splits are accumulated by file type and the scanner subclasses are passed
// the initial splits.  Scanners are expected to queue up a non-zero number of
// those splits to the io mgr (via the ScanNode).
Status HdfsScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::OPEN));

  if (per_file_splits_.empty()) {
    done_ = true;
    return Status::OK;
  }

  RETURN_IF_ERROR(runtime_state_->io_mgr()->RegisterReader(
      hdfs_connection_, state->max_io_buffers(), state->num_scanner_threads(), 
      &reader_context_));
  runtime_state_->io_mgr()->set_bytes_read_counter(reader_context_, bytes_read_counter());
  runtime_state_->io_mgr()->set_read_timer(reader_context_, read_timer());

  int total_splits = 0;
  // Walk all the files on this node and coalesce all the files with the same
  // format.
  // Also, initialize all the exprs for all the partition keys.
  map<THdfsFileFormat::type, vector<HdfsFileDesc*> > per_type_files;
  for (SplitsMap::iterator it = per_file_splits_.begin(); 
       it != per_file_splits_.end(); ++it) {
    vector<DiskIoMgr::ScanRange*>& splits = it->second->splits;
    DCHECK(!splits.empty());
    
    ScanRangeMetadata* metadata = 
        reinterpret_cast<ScanRangeMetadata*>(splits[0]->meta_data());
    int64_t partition_id = metadata->partition_id;
    HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
    if (partition == NULL) {
      stringstream ss;
      ss << "Could not find partition with id: " << partition_id;
      return Status(ss.str());
    }

    ++num_unqueued_files_;
    total_splits += splits.size();

    RETURN_IF_ERROR(partition->PrepareExprs(state));
    per_type_files[partition->file_format()].push_back(it->second);
  }

  if (total_splits == 0) {
    done_ = true;
    return Status::OK;
  }

  stringstream ss;
  ss << "Splits complete (node=" << id() << "):";
  progress_ = ProgressUpdater(ss.str(), total_splits);

  if (FLAGS_randomize_splits) {
    unsigned int seed = time(NULL);
    srand(seed);
    VLOG_QUERY << "Randomizing scan range order with seed=" << seed;
    map<THdfsFileFormat::type, vector<HdfsFileDesc*> >::iterator it;
    for (it = per_type_files.begin(); it != per_type_files.end(); ++it) {
      vector<HdfsFileDesc*>& file_descs = it->second;
      for (int i = 0; i < file_descs.size(); ++i) {
        random_shuffle(file_descs[i]->splits.begin(), file_descs[i]->splits.end());
      }
      random_shuffle(file_descs.begin(), file_descs.end());
    }
  }

  // Issue initial ranges for all file types.
  HdfsTextScanner::IssueInitialRanges(this, per_type_files[THdfsFileFormat::TEXT]);
  BaseSequenceScanner::IssueInitialRanges(this, 
      per_type_files[THdfsFileFormat::SEQUENCE_FILE]);
  BaseSequenceScanner::IssueInitialRanges(this,
      per_type_files[THdfsFileFormat::RC_FILE]);
  BaseSequenceScanner::IssueInitialRanges(this,
      per_type_files[THdfsFileFormat::AVRO]);
  HdfsParquetScanner::IssueInitialRanges(this, per_type_files[THdfsFileFormat::PARQUET]);
  if (!per_type_files[THdfsFileFormat::LZO_TEXT].empty()) {
    // This will dlopen the lzo binary and can fail if it is not present
    RETURN_IF_ERROR(HdfsLzoTextScanner::IssueInitialRanges(state,
        this, per_type_files[THdfsFileFormat::LZO_TEXT]));
  }

  if (progress_.done()) {
    // No scan ranges queued, nothing to do
    DCHECK_EQ(queued_ranges_.size(), 0);
    done_ = true;
    return Status::OK;
  }

  // Start up disk thread which in turn drives the scanner threads.
  disk_read_thread_.reset(new thread(&HdfsScanNode::DiskThread, this));

  // scanners have added their initial ranges, issue the first batch to the io mgr.
  // TODO: gdb seems to cause a SIGSEGV when the java call this triggers in the
  // I/O threads when the thread created above appears during that operation. 
  // We create it first and then wake up the I/O threads. Need to investigate.
  IssueQueuedRanges();

  return Status::OK;
}

Status HdfsScanNode::Close(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::CLOSE));
  {
    unique_lock<mutex> l(row_batches_lock_);
    done_ = true;
  }
  if (reader_context_ != NULL) {
    runtime_state_->io_mgr()->CancelReader(reader_context_);
    row_batch_consumed_cv_.notify_all();
  }

  if (disk_read_thread_ != NULL) disk_read_thread_->join();

  // There are materialized batches that have not been returned to the parent node.
  // Clean those up now.
  for (list<RowBatch*>::iterator it = materialized_row_batches_.begin();
       it != materialized_row_batches_.end(); ++it) {
    __sync_fetch_and_add(&num_owned_io_buffers_, -1 * (*it)->num_io_buffers());
    delete *it;
  }
  materialized_row_batches_.clear();
  
  DCHECK_EQ(num_owned_io_buffers_, 0) << "ScanNode has leaked io buffers";
  if (reader_context_ != NULL) {
    runtime_state_->io_mgr()->UnregisterReader(reader_context_);
  }

  scanner_pool_.reset(NULL);

  if (memory_used_counter_ != NULL) {
    COUNTER_UPDATE(memory_used_counter_, tuple_pool_->peak_allocated_bytes());
  }

  return ExecNode::Close(state);
}

void HdfsScanNode::AddDiskIoRange(DiskIoMgr::ScanRange* range) {
  unique_lock<recursive_mutex> lock(lock_);
  queued_ranges_.push_back(range);
}

void HdfsScanNode::AddDiskIoRange(const HdfsFileDesc* desc) {
  const vector<DiskIoMgr::ScanRange*>& splits = desc->splits;
  unique_lock<recursive_mutex> lock(lock_);
  for (int j = 0; j < splits.size(); ++j) {
    queued_ranges_.push_back(splits[j]);
  }
  FileQueued(desc->filename.c_str());
}

void HdfsScanNode::FileQueued(const char* filename) {
  unique_lock<recursive_mutex> lock(lock_);
  DCHECK_GT(num_unqueued_files_, 0);
  if (--num_unqueued_files_ == 0) IssueQueuedRanges();
}

// TODO: this is not in the final state.  Currently, each scanner thread only
// works on one scan range and cannot switch back and forth between them.  This
// puts the limitation that we can't have more ranges in flight than the number of
// scanner threads.  There are two ways we can fix this:
// 1. Add the logic to the io mgr to cap the number of in flight contexts.  This is
// yet another resource for the io mgr to deal with.  It's a bit more bookkeeping:
// max 3 contexts across 5 disks, the io mgr needs to make sure 3 of the disks
// are busy for this reader.  Also, as in this example, it is not possible to spin
// up all the disks with 1 scanner thread.
// 2. Update the scanner threads to be able to switch between scan ranges.  This is
// more flexible and can be done with getcontext()/setcontext() (or libtask).  
// ScannerContext provides this abstraction already (when GetBytes() blocks, switch
// to another scan range).
//
// Currently, we have implemented #1.
Status HdfsScanNode::IssueQueuedRanges() {
  unique_lock<recursive_mutex> lock(lock_);
  RETURN_IF_ERROR(
      runtime_state_->io_mgr()->AddScanRanges(reader_context_, queued_ranges_));
  queued_ranges_.clear();
  return Status::OK;
}

// lock_ should be taken before calling this.
void HdfsScanNode::StartNewScannerThread(DiskIoMgr::BufferDescriptor* buffer) {
  ScanRangeMetadata* metadata = 
      reinterpret_cast<ScanRangeMetadata*>(buffer->scan_range()->meta_data());
  int64_t partition_id = metadata->partition_id;
  HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);

  ScannerContext* context = runtime_state_->obj_pool()->Add(
      new ScannerContext(runtime_state_, this, partition, buffer));
  metadata->stream = context->GetStream();

  // Track this context as active
  active_contexts_.insert(context);

  HdfsScanner* scanner = CreateScanner(partition);
  scanner_threads_.add_thread(new thread(&HdfsScanNode::ScannerThread, this,
        scanner, context)); 
}

// The disk thread continuously reads from the io mgr, queuing buffers to the 
// correct ScannerContext::Stream.  If the buffer is from a new stream (i.e. first
// buffer for the stream), then a new ScannerContext (and Stream) is created and
// a new thread is created for the ScannerContext.
void HdfsScanNode::DiskThread() {
  while (true) {
    bool eos = false;
    DiskIoMgr::BufferDescriptor* buffer_desc = NULL;
    Status status = 
        runtime_state_->io_mgr()->GetNext(reader_context_, &buffer_desc, &eos);

    ScannerContext::Stream* stream = NULL;
    {
      unique_lock<recursive_mutex> lock(lock_);

      // done_ will trigger the io mgr to return CANCELLED, we can ignore that error
      // since the scan node triggered it itself.
      if (done_) {
        if (buffer_desc != NULL) buffer_desc->Return();
        break;
      }

      // The disk io mgr is done or error occurred. Tear everything down.
      if (!status.ok()) {
        {
          unique_lock<mutex> l(row_batches_lock_);
          done_ = true;
        }
        if (buffer_desc != NULL) buffer_desc->Return();
        status_.AddError(status);
        break;
      }

      DCHECK(buffer_desc != NULL);
      __sync_fetch_and_add(&num_owned_io_buffers_, 1);

      ScanRangeMetadata* metadata = 
          reinterpret_cast<ScanRangeMetadata*>(buffer_desc->scan_range()->meta_data());
      if (metadata->stream == NULL) {
        // This buffer is not part of an existing stream, create a new scanner,
        // context and stream to process it.
        StartNewScannerThread(buffer_desc);
      } else {
        stream = metadata->stream;
      }
    }

    if (stream != NULL) {
      // Do not hold lock_ when calling AddBuffer.
      stream->AddBuffer(buffer_desc);
    }
  }
  // At this point the disk thread is starting cleanup and will no longer read
  // from the io mgr.  This can happen in one of these conditions:
  //   1. All ranges were returned.  (common case).
  //   2. Limit was reached (done_ and status_ is ok)
  //   3. Error occurred (done_ and status_ not ok).
  DCHECK(done_);

  VLOG_FILE << "Disk thread done (node=" << id() << ")";
  // Wake up all contexts that are still waiting.  done_ indicates that the
  // node is complete (e.g. parse error or limit reached()) and the scanner should
  // terminate immediately.
  {
    unique_lock<recursive_mutex> lock(lock_);
    for (unordered_set<ScannerContext*>::iterator it = active_contexts_.begin(); 
        it != active_contexts_.end(); ++it) {
      (*it)->Cancel();
    }
    active_contexts_.clear();
  }

  scanner_threads_.join_all();
   
  // Wake up thread in GetNext	
  row_batch_added_cv_.notify_one();
}

void HdfsScanNode::AddMaterializedRowBatch(RowBatch* row_batch) {
  {
    unique_lock<mutex> l(row_batches_lock_);
    while (UNLIKELY(materialized_row_batches_.size() >= max_materialized_row_batches_
        && !done_)) {
      row_batch_consumed_cv_.wait(l);
    }

    // We must enqueue row_batch even if we're done (rather than dropping it) in case
    // already queued batches depend on its attached resources.
    materialized_row_batches_.push_back(row_batch);
  }
  row_batch_added_cv_.notify_one();
}

void HdfsScanNode::ScannerThread(HdfsScanner* scanner, ScannerContext* context) {
  // Call into the scanner to process the range.  From the scanner's perspective,
  // everything is single threaded.
  Status status;
  {
    SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
    status = scanner->ProcessSplit(context);
    scanner->Close();
  }

  // Scanner thread completed. Take a look and update the status 
  unique_lock<recursive_mutex> l(lock_);
  active_contexts_.erase(context);
  ScannerContext::Stream* stream = context->GetStream();
  DCHECK(stream != NULL);
  
  // If there was already an error, the disk thread will do the cleanup.
  if (!status_.ok()) return;

  if (!status.ok()) {
    if (status.IsCancelled()) {
      // Scan node should be the only thing that initiated scanner threads to see
      // cancelled (i.e. limit reached).  No need to do anything here.
      DCHECK(done_);
      return;
    }
    
    // This thread hit an error, record it and bail
    // TODO: better way to report errors?  Maybe via the thrift interface?
    if (VLOG_QUERY_IS_ON && !runtime_state_->error_log().empty()) {
      stringstream ss;
      ss << "Scan node (id=" << id() << ") ran into a parse error for scan range "
         << stream->filename() << "(" << stream->scan_range()->offset() << ":" 
         << stream->scan_range()->len() 
         << ").  Processed " << stream->total_bytes_returned() << " bytes." << endl
         << runtime_state_->ErrorLog();
      VLOG_QUERY << ss.str();
    }
  
    status_ = status;
    {
      unique_lock<mutex> l(row_batches_lock_);
      done_ = true;
    }
    // Notify the disk which will trigger tear down of all threads.
    runtime_state_->io_mgr()->CancelReader(reader_context_);
    // Notify the main thread which reports the error
    row_batch_added_cv_.notify_one();
  }

  if (progress_.done()) {
    // All ranges are finished.  Indicate we are done.
    {
      unique_lock<mutex> l(row_batches_lock_);
      done_ = true;
    }
    row_batch_added_cv_.notify_one();
  } else {
    // The scanner could have queued more ranges.  Send them to the io mgr.
    IssueQueuedRanges();
  } 
}

void HdfsScanNode::RangeComplete(const THdfsFileFormat::type& file_type, 
    const THdfsCompression::type& compression_type) {
  scan_ranges_complete_counter()->Update(1);
  progress_.Update(1);

  {
    unique_lock<mutex> l(file_type_counts_lock_);
    ++file_type_counts_[make_pair(file_type, compression_type)];
  }
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

void HdfsScanNode::UpdateCounters() {
  unique_lock<recursive_mutex> l(lock_);
  if (counters_reported_) return;
  counters_reported_ = true;

  runtime_profile()->StopRateCounterUpdates(total_throughput_counter());
  
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
}

