// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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
#include "runtime/hdfs-fs-cache.h"
#include "runtime/string-value.h"
#include "util/runtime-profile.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-scan-node.h"

using namespace std;
using namespace boost;
using namespace impala;

HdfsRCFileScanner::HdfsRCFileScanner(HdfsScanNode* scan_node,
    const TupleDescriptor* tuple_desc, Tuple* template_tuple, MemPool* mem_pool)
    : HdfsScanner(scan_node, tuple_desc, template_tuple, mem_pool),
      scan_range_fully_buffered_(false) {
}

Status HdfsRCFileScanner::Prepare(RuntimeState* state, ByteStream* byte_stream) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(state, byte_stream));

  text_converter_.reset(new TextConverter(0, tuple_pool_));

  // Initialize the column read mask
  const vector<int>& column_idx_to_slot_idx = scan_node_->column_to_slot_index();

  int num_part_keys = scan_node_->GetNumPartitionKeys();
  int num_non_partition_slots =
    column_idx_to_slot_idx.size() - num_part_keys;
  column_idx_read_mask_.reserve(num_non_partition_slots);
  column_idx_read_mask_.resize(num_non_partition_slots);

  for (int i = 0; i < num_non_partition_slots; ++i) {
    column_idx_read_mask_[i] =
      column_idx_to_slot_idx[i + num_part_keys] != HdfsScanNode::SKIP_COLUMN;
  }

  tuple_ = NULL;

  return Status::OK;
}

Status HdfsRCFileScanner::InitCurrentScanRange(RuntimeState* state,
    HdfsScanRange* scan_range, ByteStream* byte_stream) {
  HdfsScanner::InitCurrentScanRange(state, scan_range, byte_stream);

  rc_reader_.reset(new RCFileReader(column_idx_read_mask_, byte_stream));
  if (rc_reader_ == NULL) {
    return Status("Failed to create RCFileReader.");
  }

  RETURN_IF_ERROR(rc_reader_.get()->InitCurrentScanRange(scan_range));
  row_group_.reset(rc_reader_->NewRCFileRowGroup());
  scan_range_fully_buffered_ = false;

  return Status::OK;
}

Status HdfsRCFileScanner::GetNext(
    RuntimeState* state, RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  // Indicates whether the current row has errors.
  bool error_in_row = false;

  if (scan_node_->ReachedLimit()) {
    tuple_ = NULL;
    *eosr = true;
    return Status::OK;
  }
  tuple_ = reinterpret_cast<Tuple*>(tuple_buffer_);
  bzero(tuple_buffer_, tuple_buffer_size_);

  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;

  TupleRow* current_row = NULL;

  // RCFileReader effectively does buffered IO, in that it reads the
  // rcfile into a RCRowGroup buffer, and sets an eof marker at that
  // point, before the buffer is drained.
  // In the following loop, eosr tracks the end of stream flag that
  // tells the scan node to move onto the next scan range.
  // scan_range_fully_buffered_ is set once the RC file has been fully
  // read into RCRowGroups.
  while (!scan_node_->ReachedLimit() && !row_batch->IsFull() && !(*eosr)) {
    if (row_group_->NumRowsRemaining() == 0) {
      // scan_range_fully_buffered_ is set iff the scan range has been
      // exhausted due to a previous read.
      if (scan_range_fully_buffered_) {
        *eosr = true;
        break;
      }
      RETURN_IF_ERROR(rc_reader_->ReadRowGroup(row_group_.get(),
        current_scan_range_, &scan_range_fully_buffered_));
      if (row_group_->num_rows() == 0) break;
    }

    // Copy rows out of the current row_group into the row_batch
    while (!scan_node_->ReachedLimit() && !row_batch->IsFull() && row_group_->NextRow()) {
      if (row_idx == RowBatch::INVALID_ROW_INDEX) {
        row_idx = row_batch->AddRow();
        current_row = row_batch->GetRow(row_idx);
        current_row->SetTuple(0, tuple_);
        // Initialize tuple_ from the partition key template tuple before writing the
        // slots
        if (template_tuple_ != NULL) {
          memcpy(tuple_, template_tuple_, tuple_byte_size_);
        }
      }

      const vector<pair<int, SlotDescriptor*> > materialized_slots
        = scan_node_->materialized_slots();
      vector<pair<int, SlotDescriptor*> >::const_iterator it;

      for (it = materialized_slots.begin(); it != materialized_slots.end(); ++it) {
        int rc_column_idx = it->first;
        const SlotDescriptor* slot_desc = it->second;

        const char* col_start = row_group_->GetFieldPtr(rc_column_idx);
        int field_len = row_group_->GetFieldLength(rc_column_idx);
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
            state->error_stream() << "Error converting column: " << rc_column_idx <<
                " TO " << TypeToString(slot_desc->type()) << endl;
          }
        }
      }

      if (error_in_row) {
        error_in_row = false;
        if (state->LogHasSpace()) {
          state->error_stream() << "file: " <<
            current_byte_stream_->GetLocation() << endl;
          state->error_stream() << "row group: " << rc_reader_->row_group_idx() << endl;
          state->error_stream() << "row index: " << row_group_->row_idx();
          state->LogErrorStream();
        }
        if (state->abort_on_error()) {
          state->ReportFileErrors(current_byte_stream_->GetLocation(), 1);
          return Status(
              "Aborted HdfsRCFileScanner due to parse errors. View error log for "
              "details.");
        }
      }

      if (scan_node_->EvalConjunctsForScanner(current_row)) {
        row_batch->CommitLastRow();
        scan_node_->IncrNumRowsReturned();
        ++num_rows_returned_;
        if (scan_node_->ReachedLimit()) break;
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_byte_size_;
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
        row_idx = RowBatch::INVALID_ROW_INDEX;
        if (row_batch->IsFull()) {
          tuple_ = NULL;
          break;
        }
        // don't check until we know we'll use tuple_
        DCHECK_LE(new_tuple, tuple_buffer_ + tuple_buffer_size_ - tuple_byte_size_);
      } else {
        // Make sure to reset null indicators since we're overwriting
        // the tuple assembled for the previous row;
        // this also wipes out materialized partition key values.
        tuple_->Init(tuple_byte_size_);
        if (template_tuple_ != NULL) {
          memcpy(tuple_, template_tuple_, tuple_byte_size_);
        }
      }
    }
  }

  // Note that NumRowsRemaining() == 0 -> scan_range_fully_buffered_
  // at this point, hence we don't need to explicitly check that
  // scan_range_fully_buffered_ == true.
  if (scan_node_->ReachedLimit() || (row_group_ != NULL && row_group_->num_rows() == 0)
      || row_group_->NumRowsRemaining() == 0) {
    // We reached the limit, drained the row group or hit the end of the table.
    // No more work to be done. Clean up all pools with the last row batch.
    *eosr = true;
    row_batch->tuple_data_pool()->AcquireData(tuple_pool_, false);
  } else {
    DCHECK(row_batch->IsFull());
    // The current row_batch is full, but we haven't yet reached our limit.
    // Hang on to the last chunks. We'll continue from there in the next
    // call to GetNext().
    *eosr = false;
    row_batch->tuple_data_pool()->AcquireData(tuple_pool_, true);
  }

  return Status::OK;
}

void HdfsRCFileScanner::DebugString(
    int indentation_level, std::stringstream* out) const {
  // TODO: Add more details of internal state.
  *out << string(indentation_level * 2, ' ');
  *out << "HdfsRCFileScanner(tupleid=" << tuple_idx_ <<
    " file=" << current_byte_stream_->GetLocation();
  // TODO: Scanner::DebugString
  //  ExecNode::DebugString(indentation_level, out);
  *out << "])" << endl;
}
