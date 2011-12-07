// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-rcfile-scan-node.h"

#include <boost/algorithm/string.hpp>
#include "text-converter.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;
using namespace boost;
using namespace impala;

HdfsRCFileScanNode::HdfsRCFileScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                   const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      file_idx_(-1),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      tuple_desc_(NULL),
      tuple_buf_pool_(new MemPool()),
      obj_pool_(new ObjectPool()),
      hdfs_connection_(NULL),
      tuple_buf_(NULL),
      tuple_(NULL),
      num_cols_(0),
      num_partition_keys_(0),
      text_converter_(NULL),
      rc_reader_(NULL),
      row_group_(NULL) {
  // Initialize partition key regex
  Status status = Init(pool, tnode);
  DCHECK(status.ok()) << "HdfsRCFileScanNode c'tor:Init failed: \n" << status.GetErrorMsg();
}

Status HdfsRCFileScanNode::Init(ObjectPool* pool, const TPlanNode& tnode) {
  try {
    partition_key_regex_ = regex(tnode.hdfs_scan_node.partition_key_regex);
  } catch(bad_expression&) {
    std::stringstream ss;
    ss << "HdfsRCFileScanNode::Init(): "
       << "Invalid regex: " << tnode.hdfs_scan_node.partition_key_regex;
    return Status(ss.str());
  }  
  return Status::OK;
}

Status HdfsRCFileScanNode::Prepare(RuntimeState* state) {
  PrepareConjuncts(state);

  tuple_desc_ = state->descs().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to get tuple descriptor.");
  }

  // Set delimiters from table descriptor
  if (tuple_desc_->table_desc() == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Tuple descriptor does not have table descriptor set.");
  }
  const HdfsTableDescriptor* hdfs_table =
      static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
  text_converter_.reset(new TextConverter(false, 0, tuple_buf_pool_.get()));

  // Create mapping from column index in table to slot index in output tuple.
  // First, initialize all columns to SKIP_COLUMN.
  num_cols_ = hdfs_table->num_cols();
  column_idx_to_slot_idx_.reserve(num_cols_);
  column_idx_to_slot_idx_.resize(num_cols_);
  for (int i = 0; i < num_cols_; ++i) {
    column_idx_to_slot_idx_[i] = SKIP_COLUMN;
  }
  num_partition_keys_ = hdfs_table->num_clustering_cols();
  
  // Initialize the column read mask
  column_idx_read_mask_.reserve(num_cols_ - num_partition_keys_);
  column_idx_read_mask_.resize(num_cols_ - num_partition_keys_);
  for (int i = 0; i < column_idx_read_mask_.size(); ++i) {
    column_idx_read_mask_[i] = false;
  }
  
  // Next, set mapping from column index to slot index for all slots in the query.
  // We also set the key_idx_to_slot_idx_ to mapping for materializing partition keys.
  const std::vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (size_t i = 0; i < slots.size(); ++i) {
    cout << slots[i]->DebugString() << endl;
    if (!slots[i]->is_materialized()) continue;
    if (hdfs_table->IsClusteringCol(slots[i])) {
      key_idx_to_slot_idx_.push_back(make_pair(slots[i]->col_pos(), i));
    } else {
      // Set column to slot mapping.
      // assert(slots[i]->col_pos() - num_partition_keys_ >= 0);
      column_idx_to_slot_idx_[slots[i]->col_pos() - num_partition_keys_] = i;
      column_idx_read_mask_[slots[i]->col_pos() - num_partition_keys_] = true;
    }
  }
  partition_key_values_.resize(key_idx_to_slot_idx_.size());
 
  // Allocate tuple buffer.
  tuple_buf_size_ = state->batch_size() * tuple_desc_->byte_size();
  tuple_buf_ = static_cast<char*>(tuple_buf_pool_->Allocate(tuple_buf_size_));

  return Status::OK;
}

Status HdfsRCFileScanNode::Open(RuntimeState* state) {
  hdfs_connection_ = hdfsConnect("default", 0);
  if (hdfs_connection_ == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to connect to HDFS.");
  }

  rc_reader_.reset(new RCFileReader(hdfs_connection_, files_, column_idx_read_mask_));
  if (rc_reader_ == NULL) {
    return Status("Failed to create RCFileReader.");
  }

  row_group_.reset(rc_reader_->NewRCFileRowGroup());
  return Status::OK;
}

Status HdfsRCFileScanNode::Close(RuntimeState* state) {
  if (hdfsDisconnect(hdfs_connection_) == 0) {
    return Status::OK;
  } else {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to close HDFS connection.");
  }
}

Status HdfsRCFileScanNode::GetNext(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  // Indicates whether the current row has errors.
  bool error_in_row = false;
  
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }
  tuple_ = reinterpret_cast<Tuple*>(tuple_buf_);
  bzero(tuple_buf_, tuple_buf_size_);

  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;

  TupleRow* current_row = NULL;

  while (!ReachedLimit() && !row_batch->IsFull()) {
    if (row_group_->NumRowsRemaining() == 0) {
      RETURN_IF_ERROR(rc_reader_->ReadNextRowGroup(row_group_.get()));
      if (row_group_->num_rows() == 0) break;

      if (file_idx_ != rc_reader_->file_idx()) {
        file_idx_ = rc_reader_->file_idx();
        RETURN_IF_ERROR(ExtractPartitionKeyValues(state));
      }
    }

    // Copy rows out of the current row_group into the row_batch
    while (!ReachedLimit() && !row_batch->IsFull() && row_group_->NextRow()) {
      if (row_idx == RowBatch::INVALID_ROW_INDEX) {
        row_idx = row_batch->AddRow();
        current_row = row_batch->GetRow(row_idx);
        current_row->SetTuple(0, tuple_);
      }
        
      for (int col_idx = 0; col_idx < num_cols_ - num_partition_keys_; ++col_idx) {
        int slot_idx = column_idx_to_slot_idx_[col_idx];
        if (slot_idx == SKIP_COLUMN) continue;
        
        const char* col_start = row_group_->GetFieldPtr(col_idx);
        int field_len = row_group_->GetFieldLength(col_idx);
        const SlotDescriptor* slot_desc = tuple_desc_->slots()[slot_idx];
        bool parse_ok = true;
        
        switch (slot_desc->type()) {
          case TYPE_STRING:
            // TODO: Eliminate the unnecessary copy operation from the RCFileRowGroup
            // buffers to the tuple buffers by pushing the tuple buffer down into the
            // RowGroup class.
            parse_ok = text_converter_->ConvertAndWriteSlotBytes(col_start,
                col_start + field_len, tuple_, slot_desc, true, false);
            break;
          default:
            // RCFile stores all fields as strings regardless of type, but these
            // strings are not NULL terminated. The strto* functions that TextConverter
            // uses require NULL terminated strings, so we have to manually NULL terminate
            // the strings before passing them to ConvertAndWriteSlotBytes.
            // TODO: Devise a way to avoid this unecessary copy-and-terminate operation.
            string terminated_field(col_start, field_len);
            const char* c_str = terminated_field.c_str();
            parse_ok = text_converter_->ConvertAndWriteSlotBytes(c_str,
                 c_str + field_len, tuple_, slot_desc, false, false);
            break;
        }

        if (!parse_ok) {
          error_in_row = true;
          if (state->LogHasSpace()) {
            state->error_stream() << "Error converting column: " << col_idx <<
                " TO " << TypeToString(slot_desc->type()) << endl;
          }
        }
      }

      if (error_in_row) {
        error_in_row = false;
        if (state->LogHasSpace()) {
          state->error_stream() << "file: " << files_[rc_reader_->file_idx()] << endl;
          state->error_stream() << "row group: " << rc_reader_->row_group_idx() << endl;
          state->error_stream() << "row index: " << row_group_->row_idx();
          state->LogErrorStream();
        }
        if (state->abort_on_error()) {
          state->ReportFileErrors(files_[rc_reader_->file_idx()], 1);
          return Status(
              "Aborted HdfsRCFileScanNode due to parse errors. View error log for "
              "details.");
        }
      }
      
      // Materialize partition-key values (if any).
      for (int i = 0; i < key_idx_to_slot_idx_.size(); ++i) {
        int slot_idx = key_idx_to_slot_idx_[i].second;
        Expr* expr = partition_key_values_[i];
        SlotDescriptor* slot = tuple_desc_->slots()[slot_idx];
        RawValue::Write(expr->GetValue(NULL), tuple_, slot, tuple_buf_pool_.get());
      }
      
      if (EvalConjuncts(current_row)) {
        row_batch->CommitLastRow();
        ++num_rows_returned_;
        if (ReachedLimit()) break;
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_desc_->byte_size();
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
        row_idx = RowBatch::INVALID_ROW_INDEX;
        if (row_batch->IsFull()) break;
        // don't check until we know we'll use tuple_
        DCHECK_LE(new_tuple, tuple_buf_ + tuple_buf_size_ - tuple_desc_->byte_size());
      } else {
        // Make sure to reset null indicators since we're overwriting
        // the tuple assembled for the previous row;
        // this also wipes out materialized partition key values.
        tuple_->Init(tuple_desc_->byte_size());
      }
    }
  }

  if (ReachedLimit() || (row_group_ != NULL && row_group_->num_rows() == 0)) {
    // We either reached the limit or hit the end of the table.
    // No more work to be done. Clean up all pools with the last row batch.
    *eos = true;
    row_batch->tuple_data_pool()->AcquireData(tuple_buf_pool_.get(), false);
  } else {
    DCHECK(row_batch->IsFull());
    // The current row_batch is full, but we haven't yet reached our limit.
    // Hang on to the last chunks. We'll continue from there in the next
    // call to GetNext().
    *eos = false;
    row_batch->tuple_data_pool()->AcquireData(tuple_buf_pool_.get(), true);
  }
  
  return Status::OK;
}

void HdfsRCFileScanNode::DebugString(
    int indentation_level, std::stringstream* out) const {
  // TODO: Add more details of internal state.
  *out << string(indentation_level * 2, ' ');
  *out << "HdfsRCFileScanNode(tupleid=" << tuple_id_ << " files[" << join(files_, ",");
  ExecNode::DebugString(indentation_level, out);
  *out << "])" << endl;
}

void HdfsRCFileScanNode::SetScanRange(const TScanRange& scan_range) {
  DCHECK(scan_range.__isset.hdfsFileSplits);
  for (int i = 0; i < scan_range.hdfsFileSplits.size(); ++i) {
    files_.push_back(scan_range.hdfsFileSplits[i].path);
    // TODO: take into account offset and length 
  }
}

Status HdfsRCFileScanNode::ExtractPartitionKeyValues(RuntimeState* state) {
  DCHECK_LT(rc_reader_->file_idx(), files_.size());
  if (key_idx_to_slot_idx_.size() == 0) return Status::OK;

  smatch match;
  const string& file = files_[rc_reader_->file_idx()];
  if (boost::regex_search(file, match, partition_key_regex_)) {
    for (int i = 0; i < key_idx_to_slot_idx_.size(); ++i) {
      int regex_idx = key_idx_to_slot_idx_[i].first + 1; //match[0] is input string
      int slot_idx = key_idx_to_slot_idx_[i].second;
      const SlotDescriptor* slot_desc = tuple_desc_->slots()[slot_idx];
      PrimitiveType type = slot_desc->type();
      const string& value = match[regex_idx]; 
      Expr* expr = Expr::CreateLiteral(obj_pool_.get(), type, value);
      if (expr == NULL) {
        std::stringstream ss;
        ss << "file name'" << file << "' does not have the correct format. "
           << "Partition key: " << value << " is not of type: "
           << TypeToString(type);
        return Status(ss.str());
      }
      expr->Prepare(state, row_desc());
      partition_key_values_[i] = expr;
    }
    return Status::OK;
  }
  
  std::stringstream ss;
  ss << "file name '" << file << "' " 
     << "does not match partition key regex (" << partition_key_regex_ << ")";
  return Status(ss.str());
}
