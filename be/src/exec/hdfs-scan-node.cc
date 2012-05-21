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
      tuple_desc_(NULL),
      tuple_pool_(new MemPool()),
      current_scanner_(NULL),
      current_byte_stream_(NULL),
      template_tuple_(NULL),
      partition_key_pool_(new MemPool()),
      num_partition_keys_(0),
      plan_node_type_(tnode.node_type) {
  Status status = InitRegex(pool, tnode);
  DCHECK(status.ok()) << "HdfsScanNode c'tor:Init failed: \n" << status.GetErrorMsg();
}

Status HdfsScanNode::InitRegex(ObjectPool* pool, const TPlanNode& tnode) {
  try {
    partition_key_regex_ = regex(tnode.hdfs_scan_node.partition_key_regex);
  } catch(bad_expression&) {
    stringstream ss;
    ss << "HdfsScanNode::Init(): "
       << "Invalid regex: " << tnode.hdfs_scan_node.partition_key_regex;
    return Status(ss.str());
  }
  return Status::OK;
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
    RETURN_IF_ERROR(current_scanner_->GetNext(state, row_batch, &eosr));
    if (eosr) { // Current scan range is finished
      ++current_range_idx_;

      // Will update file and current_file_scan_ranges_ so that subsequent check passes
      RETURN_IF_ERROR(InitNextScanRange(state, eos));
      // Done with all files
      if (*eos) break;
    }
  } while (!row_batch->IsFull() && !ReachedLimit());

  if (ReachedLimit()) {
    *eos = true;
  }

  return Status::OK;
}

void HdfsScanNode::SetScanRange(const TScanRange& scan_range) {
  DCHECK(scan_range.__isset.hdfsFileSplits);
  for (int i = 0; i < scan_range.hdfsFileSplits.size(); ++i) {
    const THdfsFileSplit& split = scan_range.hdfsFileSplits[i];
    const string& path = split.path;
    if (per_file_scan_ranges_.find(path) == per_file_scan_ranges_.end()) {
      per_file_scan_ranges_[path] = vector<HdfsScanRange>();
    }
    per_file_scan_ranges_[path].push_back(HdfsScanRange(split.offset, split.length));
  }
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
  RETURN_IF_ERROR(ExtractPartitionKeyValues(state, key_idx_to_slot_idx_));

  if (current_byte_stream_ == NULL) {
    current_byte_stream_.reset(new HdfsByteStream(hdfs_connection_));
    RETURN_IF_ERROR(current_byte_stream_->Open(current_file_scan_ranges_->first));

    switch (plan_node_type_) {
      case TPlanNodeType::HDFS_TEXT_SCAN_NODE:
        current_scanner_.reset(new HdfsTextScanner(this, tuple_desc_, template_tuple_,
                                                   tuple_pool_.get()));
        break;
      case TPlanNodeType::HDFS_SEQFILE_SCAN_NODE:
        current_scanner_.reset(new HdfsSequenceScanner(this, tuple_desc_, template_tuple_,
                                                       tuple_pool_.get())); break;
      case TPlanNodeType::HDFS_RCFILE_SCAN_NODE:
        current_scanner_.reset(new HdfsRCFileScanner(this, tuple_desc_, template_tuple_,
                                                     tuple_pool_.get()));
        break;
      default:
        return Status("Unknown plan node type");
    }
    RETURN_IF_ERROR(current_scanner_->Prepare(state, current_byte_stream_.get()));
  }

  HdfsScanRange& range = current_file_scan_ranges_->second[current_range_idx_];
  RETURN_IF_ERROR(current_scanner_->InitCurrentScanRange(state,
                                                         &range,
                                                         current_byte_stream_.get()));
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

  // setup Hdfs specific counter
  parse_time_counter_ =
      ADD_COUNTER(runtime_profile(), "ParseTime", TCounterType::CPU_TICKS);

  const HdfsTableDescriptor* hdfs_table =
    static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());

  // Create mapping from column index in table to slot index in output tuple.
  // First, initialize all columns to SKIP_COLUMN.
  int num_cols = hdfs_table->num_cols();
  column_idx_to_slot_idx_.reserve(num_cols);
  column_idx_to_slot_idx_.resize(num_cols);
  for (int i = 0; i < num_cols; i++) {
    column_idx_to_slot_idx_[i] = SKIP_COLUMN;
  }

  num_partition_keys_ = hdfs_table->num_clustering_cols();

  // Next, set mapping from column index to slot index for all slots in the query.
  // We also set the key_idx_to_slot_idx__ to mapping for materializing partition keys.
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (size_t i = 0; i < slots.size(); i++) {
    if (!slots[i]->is_materialized()) continue;
    if (hdfs_table->IsClusteringCol(slots[i])) {
      // Set partition-key index to slot mapping.
      key_idx_to_slot_idx_.push_back(make_pair(slots[i]->col_pos(), i));
    } else {
      // Set column to slot mapping.
      // assert(slots[i]->col_pos() - num_partition_keys_ >= 0);
      column_idx_to_slot_idx_[slots[i]->col_pos()] = i;
    }
  }

  // Find all the materialized non-partition key slots.
  for (int i = 0; i < num_cols; ++i) {
    if (column_idx_to_slot_idx_[i] != SKIP_COLUMN) {
      // Each entry contains the index of table column i, ignoring
      // partition keys (for scanners to index their internal data
      // structures), and the slot descriptor for that column.
      pair<int, SlotDescriptor*> entry =
        make_pair(i - num_partition_keys_, slots[column_idx_to_slot_idx_[i]]);
      materialized_slots_.push_back(entry);
    }
  }

  return Status::OK;
}

Status HdfsScanNode::ExtractPartitionKeyValues(RuntimeState* state,
    const vector<pair<int, int> >& key_idx_to_slot_idx) {
  if (key_idx_to_slot_idx.size() == 0) return Status::OK;

  const string& file = current_file_scan_ranges_->first;

  template_tuple_ =
      reinterpret_cast<Tuple*>(partition_key_pool_->Allocate(tuple_desc_->byte_size()));
  memset(template_tuple_, 0, tuple_desc_->byte_size());

  smatch match;
  if (boost::regex_search(file, match, partition_key_regex_)) {
    for (int i = 0; i < key_idx_to_slot_idx.size(); ++i) {
      int regex_idx = key_idx_to_slot_idx[i].first + 1; //match[0] is input string
      int slot_idx = key_idx_to_slot_idx[i].second;
      const SlotDescriptor* slot_desc = tuple_desc_->slots()[slot_idx];
      PrimitiveType type = slot_desc->type();
      const string& string_value = match[regex_idx];

      // Populate a template tuple with just the partition key values. Separate handling
      // of the string and other types saves an allocation and copy.
      const void* value = NULL;
      ExprValue expr_value;
      if (type == TYPE_STRING) {
        expr_value.string_val.ptr = const_cast<char*>(string_value.c_str());
        expr_value.string_val.len = string_value.size();
        value = reinterpret_cast<void*>(&expr_value.string_val);
      } else {
        // TODO: Check if string_value == hive null partition key
        // Waiting on completion of NullLiteral to do this.
        value = expr_value.TryParse(string_value, type);
      }
      if (value == NULL) {
        stringstream ss;
        ss << "file name'" << file << "' does not have the correct format. "
           << "Partition key: " << string_value << " is not of type: "
           << TypeToString(type);
        return Status(ss.str());
      }

      RawValue::Write(value, template_tuple_, slot_desc, partition_key_pool_.get());
    }
    return Status::OK;
  }

  stringstream ss;
  ss << "file name '" << file << "' "
     << "does not match partition key regex (" << partition_key_regex_ << ")";
  return Status(ss.str());
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
  RETURN_IF_ERROR(ExecNode::Close(state));
  return Status::OK;
}
