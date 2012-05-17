// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-scan-node.h"
#include "exec/hdfs-text-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-byte-stream.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "exec/text-converter.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"
#include "util/sse-util.h"
#include "util/string-parser.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"


using namespace std;
using namespace boost;
using namespace impala;

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      compact_data_(tnode.compact_data),
      tuple_desc_(NULL),
      tuple_pool_(new MemPool()),
      scanner_pool_(new ObjectPool()),
      current_scanner_(NULL),
      current_byte_stream_(NULL),
      template_tuple_(NULL),
      partition_key_pool_(new MemPool()),
      num_partition_keys_(0) {
}

Status HdfsScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  COUNTER_SCOPED_TIMER(runtime_profile_->total_time_counter());

  // Guard against trying to read an empty set of scan ranges
  if (per_file_scan_ranges_.size() == 0) {
    *eos = true;
    return Status::OK;
  }

  if (current_scanner_ == NULL) {
    RETURN_IF_ERROR(InitNextScanRange(state, eos));
    if (*eos) return Status::OK;
  }

  // Loops until all the scan ranges are complete or batch is full
  *eos = false;
  do {
    bool eosr = false;
    RETURN_IF_ERROR(current_scanner_->GetNext(row_batch, &eosr));

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

Status HdfsScanNode::SetScanRange(const TScanRange& scan_range) {
  DCHECK(scan_range.__isset.hdfsFileSplits);
  for (int i = 0; i < scan_range.hdfsFileSplits.size(); ++i) {
    const THdfsFileSplit& split = scan_range.hdfsFileSplits[i];
    const string& path = split.path;
    if (per_file_scan_ranges_.find(path) == per_file_scan_ranges_.end()) {
      per_file_scan_ranges_[path] = vector<HdfsScanRange>();
    }

    per_file_scan_ranges_[path].push_back(HdfsScanRange(split));
  }

  return Status::OK;
}

Status HdfsScanNode::InitNextScanRange(RuntimeState* state, bool* scan_ranges_finished) {
  *scan_ranges_finished = false;
  if (current_file_scan_ranges_ == per_file_scan_ranges_.end()) {
    // Defensively return if this gets called after all scan ranges are exhausted.
    *scan_ranges_finished = true;
    return Status::OK;
  } else if (current_range_idx_ == current_file_scan_ranges_->second.size()) {
    RETURN_IF_ERROR(current_byte_stream_->Close());
    current_byte_stream_.reset(NULL);
    ++current_file_scan_ranges_;
    current_range_idx_ = 0;
    if (current_file_scan_ranges_ == per_file_scan_ranges_.end()) {
      *scan_ranges_finished = true;
      return Status::OK; // We're done; caller will check for this condition and exit
    }
  }

  HdfsScanRange& range = current_file_scan_ranges_->second[current_range_idx_];
  HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(range.partition_id_);

  if (partition == NULL) {
    stringstream ss;
    ss << "Could not find partition with id: " << range.partition_id_;
    return Status(ss.str());
  }

  RETURN_IF_ERROR(partition->PrepareExprs(state));
  // Only allocate template_tuple_ if there are partition keys.  The scanners
  // use template_tuple == NULL to determine if partition keys are necessary
  if (!partition->partition_key_values().empty()) {
    InitTemplateTuple(state, partition->partition_key_values());
  } 

  if (current_byte_stream_ == NULL) {
    current_byte_stream_.reset(new HdfsByteStream(hdfs_connection_, this));
    RETURN_IF_ERROR(current_byte_stream_->Open(current_file_scan_ranges_->first));

    ScannerMap::const_iterator iter = scanner_map_.find(partition->file_format());
    if (iter == scanner_map_.end()) {
      switch (partition->file_format()) {
        case THdfsFileFormat::TEXT:
          current_scanner_ = new HdfsTextScanner(this, state, template_tuple_, 
              tuple_pool_.get());
          break;
        case THdfsFileFormat::SEQUENCE_FILE:
          current_scanner_ = new HdfsSequenceScanner(this, state, template_tuple_, 
              tuple_pool_.get());
          break;
        case THdfsFileFormat::RC_FILE:
          current_scanner_ = new HdfsRCFileScanner(this, state, template_tuple_, 
              tuple_pool_.get());
          break;
      default:
        return Status("Unknown Hdfs file format type");
     }
      scanner_pool_->Add(current_scanner_);
      RETURN_IF_ERROR(current_scanner_->Prepare());
      scanner_map_[partition->file_format()] = current_scanner_;
    } else {
      current_scanner_ = iter->second;
    }
    DCHECK(current_scanner_ != NULL);
  }

  RETURN_IF_ERROR(current_scanner_->InitCurrentScanRange(partition, &range, 
      template_tuple_, current_byte_stream_.get()));
  return Status::OK;
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);
  current_range_idx_ = 0;
  current_file_scan_ranges_ = per_file_scan_ranges_.begin();

  // One-time initialisation of state that is constant across scan ranges
  DCHECK(tuple_desc_->table_desc() != NULL);

  // set up Hdfs specific counters
  parse_time_counter_ =
      ADD_COUNTER(runtime_profile_, "ParseTime", TCounterType::CPU_TICKS);
  memory_used_counter_ =
      ADD_COUNTER(runtime_profile_, "MemoryUsed", TCounterType::BYTES);

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

void HdfsScanNode::InitTemplateTuple(RuntimeState* state,
    const vector<Expr*>& expr_values) {
  if (materialized_slots().empty() || template_tuple_ == NULL) {
    template_tuple_ =
      reinterpret_cast<Tuple*>(partition_key_pool_->Allocate(tuple_desc_->byte_size()));
    memset(template_tuple_, 0, tuple_desc_->byte_size());
  }

  for (int i = 0; i < partition_key_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = partition_key_slots_[i];

    // Exprs guaranteed to be literals, so can safely be evaluated without a row context
    void* value = expr_values[slot_desc->col_pos()]->GetValue(NULL);
    RawValue::Write(value, template_tuple_, slot_desc, NULL);
  }
}

Status HdfsScanNode::Open(RuntimeState* state) {
  hdfs_connection_ = state->fs_cache()->GetDefaultConnection();
  if (hdfs_connection_ == NULL) {
    stringstream ss;
    ss << "Failed to connect to HDFS." << "\nError(" << errno << "):" << strerror(errno);
    return Status(ss.str());
  } else {
    return Status::OK;
  }
}

Status HdfsScanNode::Close(RuntimeState* state) {
  if (current_byte_stream_ != NULL) {
    RETURN_IF_ERROR(current_byte_stream_->Close());
  }
  scanner_pool_.reset(NULL);
  COUNTER_UPDATE(memory_used_counter_, tuple_pool_->peak_allocated_bytes());
  RETURN_IF_ERROR(ExecNode::Close(state));
  return Status::OK;
}
