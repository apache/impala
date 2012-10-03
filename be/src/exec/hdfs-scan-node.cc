// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-scan-node.h"
#include "exec/hdfs-text-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-trevni-scanner.h"
#include "exec/hdfs-byte-stream.h"

#include <sstream>
#include <boost/algorithm/string.hpp>

#include "common/logging.h"
#include "common/object-pool.h"
#include "exec/scan-range-context.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"

// TODO: temp change to validate we don't have an incast problem for joins with big tables
DEFINE_bool(randomize_scan_ranges, false, 
    "if true, randomizes the order of scan ranges");

using namespace std;
using namespace boost;
using namespace impala;

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
      current_scanner_(NULL),
      current_byte_stream_(NULL),
      num_partition_keys_(0),
      total_scan_ranges_(0),
      num_ranges_finished_(0),
      done_(false),
      partition_key_pool_(new MemPool()),
      next_range_to_issue_idx_(0),
      all_ranges_in_queue_(false),
      ranges_in_flight_(0),
      all_ranges_issued_(false) {
}

HdfsScanNode::~HdfsScanNode() {
}

Status HdfsScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  {
    unique_lock<recursive_mutex> l(lock_);
    if (per_file_scan_ranges_.size() == 0 || ReachedLimit() || !status_.ok()) {
      *eos = true;
      return status_;
    }
  }

  *eos = false;
  while (true) {
    unique_lock<mutex> l(row_batches_lock_);
    while (materialized_row_batches_.empty() && !done_) {
      row_batch_added_cv_.wait(l);
    }

    // Return any errors
    if (!status_.ok()) return status_;

    if (!materialized_row_batches_.empty()) {
      RowBatch* materialized_batch = materialized_row_batches_.front();
      materialized_row_batches_.pop_front();

      row_batch->Swap(materialized_batch);
      // Update the number of materialized rows instead of when they are materialized.
      // This means that scanners might process and queue up more rows that are necessary
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
        // Wake up disk thread notifying it we are done.  This triggers tear down
        // of the scanner threads.
        done_ = true;
        state->io_mgr()->CancelReader(reader_context_);
      }
      delete materialized_batch;
      return Status::OK;
    } else {
      break;
    }
  } 

  // TODO: remove when all scanners are updated to use io mgr.
  if (current_scanner_ == NULL) {
    RETURN_IF_ERROR(InitNextScanRange(state, eos));
    if (*eos) return Status::OK;
  }

  // Loops until all the scan ranges are complete or batch is full
  *eos = false;
  do {
    bool eosr = false;
    RETURN_IF_ERROR(current_scanner_->GetNext(row_batch, &eosr));
    RETURN_IF_CANCELLED(state);

    num_rows_returned_ += row_batch->num_rows();
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit()) {
      *eos = true;
      return Status::OK;
    }

    if (eosr) { // Current scan range is finished
      ++current_range_idx_;

      // Will update file and current_file_scan_ranges_ so that subsequent check passes
      RETURN_IF_ERROR(InitNextScanRange(state, eos));
      // Done with all files
      if (*eos) break;
    }
  } while (!row_batch->IsFull());

  return Status::OK;
}

Status HdfsScanNode::CreateConjuncts(vector<Expr*>* expr) {
  RETURN_IF_ERROR(Expr::CreateExprTrees(runtime_state_->obj_pool(), 
      thrift_plan_node_->conjuncts, expr));
  for (int i = 0; i < expr->size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare((*expr)[i], runtime_state_, row_desc()));
  }
  return Status::OK;
}

Status HdfsScanNode::SetScanRange(const TScanRange& scan_range) {
  DCHECK(scan_range.__isset.hdfsFileSplits);
  // Convert the input ranges into per file DiskIO::ScanRange objects
  for (int i = 0; i < scan_range.hdfsFileSplits.size(); ++i) {
    const THdfsFileSplit& split = scan_range.hdfsFileSplits[i];
    const string& path = split.path;

    HdfsFileDesc* desc = NULL;
    ScanRangeMap::iterator desc_it = per_file_scan_ranges_.find(path);
    if (desc_it == per_file_scan_ranges_.end()) {
      desc = runtime_state_->obj_pool()->Add(new HdfsFileDesc(path));
      per_file_scan_ranges_[path] = desc;
    } else {
      desc = desc_it->second;
    }
      
    if (split.volumeId == -1 && !unknown_disk_id_warned_) {
      LOG(WARNING) << "Unknown disk id.  This will negatively affect performance. "
                   << " Check your hdfs settings to enable block location metadata.";
      unknown_disk_id_warned_ = true;
    }

    desc->ranges.push_back(AllocateScanRange(desc->filename.c_str(), 
       split.length, split.offset, split.partitionId, split.volumeId));
  }
  return Status::OK;
}

DiskIoMgr::ScanRange* HdfsScanNode::AllocateScanRange(const char* file, int64_t len,
    int64_t offset, int64_t partition_id, int disk_id) {
  DCHECK_GE(disk_id, -1);
  if (disk_id == -1) {
    // disk id is unknown, assign it a random one.
    static int next_disk_id = 0;
    disk_id = next_disk_id++;

  }
  // TODO: we need to parse the config for the number of dirs configured for this
  // data node.
  disk_id %= runtime_state_->io_mgr()->num_disks();

  DiskIoMgr::ScanRange* range = 
      runtime_state_->obj_pool()->Add(new DiskIoMgr::ScanRange());
  range->Reset(file, len, offset, disk_id, reinterpret_cast<void*>(partition_id));
  return range;
}

Status HdfsScanNode::InitNextScanRange(RuntimeState* state, bool* scan_ranges_finished) {
  *scan_ranges_finished = false;
  while (true) {
    if (current_file_scan_ranges_ == per_file_scan_ranges_.end()) {
      // Defensively return if this gets called after all scan ranges are exhausted.
      *scan_ranges_finished = true;
      return Status::OK;
    } else if (current_range_idx_ == current_file_scan_ranges_->second->ranges.size()) {
      RETURN_IF_ERROR(current_byte_stream_->Close());
      current_byte_stream_.reset(NULL);
      ++current_file_scan_ranges_;
      current_range_idx_ = 0;
      if (current_file_scan_ranges_ == per_file_scan_ranges_.end()) {
        *scan_ranges_finished = true;
        return Status::OK; // We're done; caller will check for this condition and exit
      }
    }

    DiskIoMgr::ScanRange* range = 
        current_file_scan_ranges_->second->ranges[current_range_idx_];
    int64_t partition_id = reinterpret_cast<int64_t>(range->meta_data());
    HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);

    if (partition == NULL) {
      stringstream ss;
      ss << "Could not find partition with id: " << partition_id;
      return Status(ss.str());
    }

    // TODO: HACK.  Skip this file if it uses the io mgr, it is handled very differently
    if (partition->file_format() == THdfsFileFormat::TEXT ||
        partition->file_format() == THdfsFileFormat::SEQUENCE_FILE) {
      ++current_file_scan_ranges_;
      continue;
    }

    Tuple* template_tuple = NULL;
    // Only allocate template_tuple_ if there are partition keys.  The scanners
    // use template_tuple == NULL to determine if partition keys are necessary
    if (!partition_key_slots_.empty()) {
      DCHECK(!partition->partition_key_values().empty());
      template_tuple = InitTemplateTuple(state, partition->partition_key_values());
    } 

    if (current_byte_stream_ == NULL) {
      current_byte_stream_.reset(new HdfsByteStream(hdfs_connection_, this));
      RETURN_IF_ERROR(current_byte_stream_->Open(current_file_scan_ranges_->first));
      current_scanner_ = GetScanner(partition);
      DCHECK(current_scanner_ != NULL);
    }

    RETURN_IF_ERROR(current_scanner_->InitCurrentScanRange(partition, range, 
        template_tuple, current_byte_stream_.get()));
  }
  return Status::OK;
}

HdfsFileDesc* HdfsScanNode::GetFileDesc(const string& filename) {
  DCHECK(per_file_scan_ranges_.find(filename) != per_file_scan_ranges_.end());
  return per_file_scan_ranges_[filename];
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

HdfsScanner* HdfsScanNode::GetScanner(HdfsPartitionDescriptor* partition) {
  ScannerMap::iterator scanner_it = scanner_map_.find(partition->file_format());
  if (scanner_it != scanner_map_.end()) {
    return scanner_it->second;
  }
  HdfsScanner* scanner = CreateScanner(partition);
  scanner_map_[partition->file_format()] = scanner;
  return scanner;
}

HdfsScanner* HdfsScanNode::CreateScanner(HdfsPartitionDescriptor* partition) {
  HdfsScanner* scanner = NULL;

  // Create a new scanner for this file format
  switch (partition->file_format()) {
    case THdfsFileFormat::TEXT:
      scanner = new HdfsTextScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::SEQUENCE_FILE:
      scanner = new HdfsSequenceScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::RC_FILE:
      scanner = new HdfsRCFileScanner(this, runtime_state_, tuple_pool_.get());
      break;
    case THdfsFileFormat::TREVNI:
      scanner = new HdfsTrevniScanner(this, runtime_state_, tuple_pool_.get());
      break;
    default:
      DCHECK(false) << "Unknown Hdfs file format type:" << partition->file_format();
      return NULL;
  }
  scanner_pool_->Add(scanner);
  // TODO better error handling
  Status status = scanner->Prepare();
  DCHECK(status.ok());
  return scanner;
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
  runtime_state_ = state;
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);
  current_range_idx_ = 0;

  // One-time initialisation of state that is constant across scan ranges
  DCHECK(tuple_desc_->table_desc() != NULL);

  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());

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
// Scan ranges are accumulated by file type and the scanner subclasses are passed
// the initial ranges.  Scanners are expected to queue up a non-zero number of
// those ranges to the io mgr (via the ScanNode).
Status HdfsScanNode::Open(RuntimeState* state) {
  if (per_file_scan_ranges_.empty()) {
    done_ = true;
    return Status::OK;
  }

  hdfs_connection_ = state->fs_cache()->GetDefaultConnection();
  if (hdfs_connection_ == NULL) {
    stringstream ss;
    ss << "Failed to connect to HDFS." << "\nError(" << errno << "):" << strerror(errno);
    return Status(ss.str());
  } 

  RETURN_IF_ERROR(runtime_state_->io_mgr()->RegisterReader(
      hdfs_connection_, state->max_io_buffers(), &reader_context_));
  runtime_state_->io_mgr()->set_bytes_read_counter(reader_context_, bytes_read_counter());
  runtime_state_->io_mgr()->set_read_timer(reader_context_, read_timer());

  current_file_scan_ranges_ = per_file_scan_ranges_.begin();

  // Walk all the files on this node and coalesce all the files with the same
  // format.
  // Also, initialize all the exprs for all the partition keys.
  map<THdfsFileFormat::type, vector<HdfsFileDesc*> > per_type_files;
  for (ScanRangeMap::iterator it = per_file_scan_ranges_.begin(); 
       it != per_file_scan_ranges_.end(); ++it) {
    vector<DiskIoMgr::ScanRange*>& ranges = it->second->ranges;
    DCHECK(!ranges.empty());
    
    int64_t partition_id = reinterpret_cast<int64_t>(ranges[0]->meta_data());
    HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
    if (partition == NULL) {
      stringstream ss;
      ss << "Could not find partition with id: " << partition_id;
      return Status(ss.str());
    }

    // TODO: remove when all scanners are updated
    if (partition->file_format() == THdfsFileFormat::TEXT ||
        partition->file_format() == THdfsFileFormat::SEQUENCE_FILE) {
      ++num_unqueued_files_;
      total_scan_ranges_ += ranges.size();
    } 

    RETURN_IF_ERROR(partition->PrepareExprs(state));
    per_type_files[partition->file_format()].push_back(it->second);
  }

  if (total_scan_ranges_ == 0) {
    done_ = true;
    return Status::OK;
  }

  VLOG_QUERY << "Scan ranges assigned to node=" << id() << ": " << total_scan_ranges_;

  num_scanner_threads_ = state->num_scanner_threads();
  if (num_scanner_threads_ == 0) {
    num_scanner_threads_ = total_scan_ranges_;
  } else {
    num_scanner_threads_ = min(state->num_scanner_threads(), total_scan_ranges_);
  }
  VLOG_QUERY << "Using " << num_scanner_threads_ << " simultaneous scanner threads.";
  DCHECK_GT(num_scanner_threads_, 0);

  if (FLAGS_randomize_scan_ranges) {
    unsigned int seed = time(NULL);
    srand(seed);
    VLOG_QUERY << "Randomizing scan range order with seed=" << seed;
    map<THdfsFileFormat::type, vector<HdfsFileDesc*> >::iterator it;
    for (it = per_type_files.begin(); it != per_type_files.end(); ++it) {
      vector<HdfsFileDesc*>& file_descs = it->second;
      for (int i = 0; i < file_descs.size(); ++i) {
        random_shuffle(file_descs[i]->ranges.begin(), file_descs[i]->ranges.end());
      }
      random_shuffle(file_descs.begin(), file_descs.end());
    }
  }

  // Issue initial ranges for all file types.
  HdfsTextScanner::IssueInitialRanges(this, per_type_files[THdfsFileFormat::TEXT]);
  HdfsSequenceScanner::IssueInitialRanges(this, 
      per_type_files[THdfsFileFormat::SEQUENCE_FILE]);
  
  // scanners have added their initial ranges, issue the first batch to the io mgr.
  IssueMoreRanges();

  if (ranges_in_flight_ == 0) {
    // This is a temporary hack for scanners not using the io mgr.
    done_ = true;
    return Status::OK;
  }
  
  // Start up disk thread which in turn drives the scanner threads.
  disk_read_thread_.reset(new thread(&HdfsScanNode::DiskThread, this));
  
  return Status::OK;
}

Status HdfsScanNode::Close(RuntimeState* state) {
  done_ = true;
  if (reader_context_ != NULL) {
    runtime_state_->io_mgr()->CancelReader(reader_context_);
  }

  if (disk_read_thread_ != NULL) disk_read_thread_->join();

  // There are materialized batches that have not been returned to the parent node.
  // Clean those up now.
  for (list<RowBatch*>::iterator it = materialized_row_batches_.begin();
       it != materialized_row_batches_.end(); ++it) {
    delete *it;
  }
  materialized_row_batches_.clear();
  
  if (reader_context_ != NULL) {
    runtime_state_->io_mgr()->UnregisterReader(reader_context_);
  }

  scanner_pool_.reset(NULL);

  COUNTER_UPDATE(memory_used_counter_, tuple_pool_->peak_allocated_bytes());
  return ExecNode::Close(state);
}

void HdfsScanNode::AddDiskIoRange(DiskIoMgr::ScanRange* range) {
  unique_lock<recursive_mutex> lock(lock_);
  all_ranges_.push_back(range);
}

void HdfsScanNode::AddDiskIoRange(const HdfsFileDesc* desc) {
  const vector<DiskIoMgr::ScanRange*>& ranges = desc->ranges;
  unique_lock<recursive_mutex> lock(lock_);
  for (int j = 0; j < ranges.size(); ++j) {
    all_ranges_.push_back(ranges[j]);
  }
  FileQueued(desc->filename.c_str());
}

void HdfsScanNode::FileQueued(const char* filename) {
  unique_lock<recursive_mutex> lock(lock_);
  // all_ranges_issued_ is only set to true after all_ranges_in_queue_ is set to
  // true.
  DCHECK(!all_ranges_issued_);
  DCHECK_GT(num_unqueued_files_, 0);
  if (--num_unqueued_files_ == 0) {
    all_ranges_in_queue_ = true;
  }
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
// ScanRangeContext provides this abstraction already (when GetBytes() blocks, switch
// to another scan range).
//
// Currently, we have implemented a poor man's version of #1 above in the scan node.
// An alternative stop gap would be remove num_scanner_threads and always spin up
// threads = num_buffers_per_disk * num_disks.  This will let us use the disks
// better and closely mimics the final state with solution #2.
Status HdfsScanNode::IssueMoreRanges() {
  unique_lock<recursive_mutex> lock(lock_);

  int num_remaining = all_ranges_.size() - next_range_to_issue_idx_;
  int threads_remaining = runtime_state_->num_scanner_threads() - ranges_in_flight_;
  int ranges_to_issue = min(threads_remaining, num_remaining);

  vector<DiskIoMgr::ScanRange*> ranges;
  if (ranges_to_issue > 0) {
    for (int i = 0; i < ranges_to_issue; ++i, ++next_range_to_issue_idx_) {
      ranges.push_back(all_ranges_[next_range_to_issue_idx_]);
    }
  }
  ranges_in_flight_ += ranges.size();
  
  if (next_range_to_issue_idx_ == all_ranges_.size() && all_ranges_in_queue_) {
    all_ranges_issued_ = true;
  }
    
  if (!ranges.empty()) {
    RETURN_IF_ERROR(runtime_state_->io_mgr()->AddScanRanges(reader_context_, ranges));
  }

  return Status::OK;
}

// lock_ should be taken before calling this.
void HdfsScanNode::StartNewScannerThread(DiskIoMgr::BufferDescriptor* buffer) {
  DiskIoMgr::ScanRange* range = buffer->scan_range();
  int64_t partition_id = reinterpret_cast<int64_t>(range->meta_data());
  HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);

  // TODO: no reason these have to be reallocated.  Reuse them.
  ScanRangeContext* context = runtime_state_->obj_pool()->Add(
      new ScanRangeContext(runtime_state_, this, partition, buffer));
  contexts_[range] = context;
  HdfsScanner* scanner = CreateScanner(partition);

  scanner_threads_.add_thread(new thread(&HdfsScanNode::ScannerThread, this,
        scanner, context)); 
}

// The disk thread continuously reads from the io mgr queuing buffers to the 
// correct scan range context.  Each scan range context maps to a single scan range.
// Each buffer the io mgr returns can be mapped to the scan range it is for.  The
// disk thread is responsible for figuring out the mapping and then queueing the
// new buffer onto the corresponding context.  If no context exists (i.e. it is the
// first buffer for this scan range), a new context object is created as well as a
// new scanner thread to process it.
void HdfsScanNode::DiskThread() {
  while (true) {
    bool eos = false;
    DiskIoMgr::BufferDescriptor* buffer_desc = NULL;
    Status status = 
        runtime_state_->io_mgr()->GetNext(reader_context_, &buffer_desc, &eos);

    unique_lock<recursive_mutex> lock(lock_);

    // done_ will trigger the io mgr to return CANCELLED, we can ignore that error
    // since the scan node triggered it itself.
    if (done_) {
      if (buffer_desc != NULL) buffer_desc->Return();
      break;
    }

    // The disk io mgr is done or error occurred.  Tear everything down.
    if (!status.ok()) {
      done_ = true;
      if (buffer_desc != NULL) buffer_desc->Return();
      status_.AddError(status);
      break;
    }
    
    DCHECK(buffer_desc != NULL);
    {
      ContextMap::iterator context_it = contexts_.find(buffer_desc->scan_range());
      if (context_it == contexts_.end()) {
        // New scan range.  Create a new scanner, context and thread for processing it.
        StartNewScannerThread(buffer_desc);
      } else {
        context_it->second->AddBuffer(buffer_desc);
      }
    }
  }
  runtime_profile()->StopRateCounterUpdates(total_throughput_counter());

  // At this point the disk thread is starting cleanup and will no longer read
  // from the io mgr.  This can happen in one of these conditions:
  //   1. All ranges were returned.  (common case).
  //   2. Limit was reached (done_ and status_ is ok)
  //   3. Error occurred (done_ and status_ not ok).
  DCHECK(done_);

  VLOG_QUERY << "Disk thread done (node=" << id() << ")";
  // Wake up all contexts that are still waiting.  done_ indicates that the
  // node is complete (e.g. parse error or limit reached()) and the scanner should
  // terminate immediately.
  for (ContextMap::iterator it = contexts_.begin(); it != contexts_.end(); ++it) {
    it->second->Cancel();
  }

  scanner_threads_.join_all();
  contexts_.clear();
  
  // Wake up thread in GetNext
  {
    unique_lock<mutex> l(row_batches_lock_);
    done_ = true;
  }
  row_batch_added_cv_.notify_one();
}

void HdfsScanNode::AddMaterializedRowBatch(RowBatch* row_batch) {
  {
    unique_lock<mutex> l(row_batches_lock_);
    materialized_row_batches_.push_back(row_batch);
  }
  row_batch_added_cv_.notify_one();
}

void HdfsScanNode::ScannerThread(HdfsScanner* scanner, ScanRangeContext* context) {
  // Call into the scanner to process the range.  From the scanner's perspective,
  // everything is single threaded.
  Status status = scanner->ProcessScanRange(context);
  scanner->Close();

  // Scanner thread completed. Take a look and update the status 
  unique_lock<recursive_mutex> l(lock_);
  
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
         << context->filename() << "(" << context->scan_range()->offset() << ":" 
         << context->scan_range()->len() 
         << ").  Processed " << context->total_bytes_returned() << " bytes." << endl
         << runtime_state_->ErrorLog();
      VLOG_QUERY << ss.str();
    }
  
    status_ = status;
    done_ = true;
    // Notify the disk which will trigger tear down of all threads.
    runtime_state_->io_mgr()->CancelReader(reader_context_);
    // Notify the main thread which reports the error
    row_batch_added_cv_.notify_one();
  }

  --ranges_in_flight_;
  if (num_ranges_finished_ == total_scan_ranges_) {
    // All ranges are finished.  Indicate we are done.
    {
      unique_lock<mutex> l(row_batches_lock_);
      done_ = true;
    }
    row_batch_added_cv_.notify_one();
  } else {
    IssueMoreRanges();
  }
}

void HdfsScanNode::RangeComplete() {
  int num_ranges = __sync_fetch_and_add(&num_ranges_finished_, 1) + 1;
  
  // Print out every 50 ranges when we have more than 50 to parse,
  // every 10 when we are between [10, 100] and every range after.
  int num_left = total_scan_ranges_ - num_ranges;
  if ((num_left >= 50 && (num_ranges % 50 == 0)) ||
      (num_left >= 10 && (num_ranges % 10 == 0)) ||
      (num_left < 10)) {
    VLOG_QUERY << "Scan Ranges finished (node=" << id() << ") = " << num_ranges 
               << " out of " << total_scan_ranges_;
  }
}
