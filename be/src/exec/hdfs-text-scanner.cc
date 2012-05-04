// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/runtime-state.h"
#include "exec/hdfs-text-scanner.h"
#include "runtime/tuple.h"
#include "runtime/row-batch.h"
#include "exec/text-converter.h"
#include "util/cpu-info.h"
#include "exec/hdfs-scan-node.h"
#include "exec/delimited-text-parser.h"
#include "exec/text-converter.inline.h"

using namespace impala;
using namespace std;

HdfsTextScanner::HdfsTextScanner(HdfsScanNode* scan_node,
    const TupleDescriptor* tuple_desc, Tuple* template_tuple, MemPool* tuple_pool)
    : HdfsScanner(scan_node, tuple_desc, template_tuple, tuple_pool),
      boundary_mem_pool_(new MemPool()),
      boundary_row_(boundary_mem_pool_.get()),
      boundary_column_(boundary_mem_pool_.get()),
      slot_idx_(0),
      delimited_text_parser_(NULL),
      text_converter_(NULL),
      byte_buffer_pool_(new MemPool()),
      byte_buffer_ptr_(NULL),
      byte_buffer_(NULL),
      byte_buffer_end_(NULL),
      reuse_byte_buffer_(false),
      byte_buffer_read_size_(0),
      error_in_row_(false) {
  const HdfsTableDescriptor* hdfs_table =
    static_cast<const HdfsTableDescriptor*>(tuple_desc->table_desc());

  text_converter_.reset(new TextConverter(hdfs_table->escape_char(), tuple_pool_));

  char field_delim = hdfs_table->field_delim();
  char collection_delim = hdfs_table->collection_delim();
  if (scan_node_->materialized_slots().size() == 0) {
    field_delim = '\0';
    collection_delim = '\0';
  }
  delimited_text_parser_.reset(new DelimitedTextParser(scan_node->column_to_slot_index(),
      scan_node->GetNumPartitionKeys(), hdfs_table->line_delim(), 
      field_delim, collection_delim, hdfs_table->escape_char()));
}

Status HdfsTextScanner::InitCurrentScanRange(RuntimeState* state,
                                             HdfsScanRange* scan_range,
                                             ByteStream* byte_stream) {
  HdfsScanner::InitCurrentScanRange(state, scan_range, byte_stream);
  current_range_remaining_len_ = scan_range->length;
  error_in_row_ = false;

  // Note - this initialisation relies on the assumption that N partition keys will occupy
  // entries 0 through N-1 in column_idx_to_slot_idx. If this changes, we will need
  // another layer of indirection to map text-file column indexes onto the
  // column_idx_to_slot_idx table used below.
  slot_idx_ = 0;

  // Pre-load byte buffer with size of entire range, if possible
  RETURN_IF_ERROR(current_byte_stream_->Seek(scan_range->offset));
  byte_buffer_ptr_ = byte_buffer_end_;
  RETURN_IF_ERROR(FillByteBuffer(state, current_range_remaining_len_));

  boundary_column_.Clear();
  boundary_row_.Clear();
  delimited_text_parser_->ParserReset();

  // Offset may not point to tuple boundary
  if (scan_range->offset != 0) {
    COUNTER_SCOPED_TIMER(scan_node_->parse_time_counter());
    int first_tuple_offset = delimited_text_parser_->FindFirstTupleStart(
        byte_buffer_, byte_buffer_read_size_);
    DCHECK_LE(first_tuple_offset, min(state->file_buffer_size(),
         current_range_remaining_len_));
    byte_buffer_ptr_ += first_tuple_offset;
    current_range_remaining_len_ -= first_tuple_offset;
  }

  return Status::OK;
}

Status HdfsTextScanner::FillByteBuffer(RuntimeState* state, int64_t size) {
  DCHECK_GE(size, 0);
  DCHECK(current_byte_stream_ != NULL);
  // TODO: Promote state->file_buffer_size to int64_t
  int64_t file_buffer_size = state->file_buffer_size();
  int read_size = min(file_buffer_size, size);

  if (!reuse_byte_buffer_) {
    byte_buffer_ =
        reinterpret_cast<char*>(byte_buffer_pool_->Allocate(state->file_buffer_size()));
    reuse_byte_buffer_ = true;
  }
  {
    COUNTER_SCOPED_TIMER(scan_node_->scanner_timer());
    RETURN_IF_ERROR(current_byte_stream_->Read(reinterpret_cast<uint8_t*>(byte_buffer_),
       read_size, &byte_buffer_read_size_));
  }
  byte_buffer_end_ = byte_buffer_ + byte_buffer_read_size_;
  byte_buffer_ptr_ = byte_buffer_;
  COUNTER_UPDATE(scan_node_->bytes_read_counter(), byte_buffer_read_size_);
  return Status::OK;
}

Status HdfsTextScanner::Prepare(RuntimeState* state, ByteStream* byte_stream) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(state, byte_stream));

  current_range_remaining_len_ = 0;

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  field_locations_.resize(state->batch_size() * scan_node_->materialized_slots().size());

  return Status::OK;
}

Status HdfsTextScanner::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;
  char* col_start = NULL;

  // This loop contains a small state machine:
  //  1. byte_buffer_ptr_ != byte_buffer_end_: no need to read more, process what's in
  //     the read buffer.
  //  2. current_range_remaining_len_ > 0: keep reading from the current file location
  //  3. current_range_remaining_len_ == 0: done with current range but might need more
  //     to complete the last tuple.
  //  4. current_range_remaining_len_ < 0: Reading past this scan range to finish tuple
  while (true) {
    if (byte_buffer_ptr_ == byte_buffer_end_) {
      if (current_range_remaining_len_ < 0) {
        // We want to minimize how much we read next.  It is past the end of this block
        // and likely on another node.
        // TODO: Set it to be the average size of a row
        RETURN_IF_ERROR(FillByteBuffer(state, NEXT_BLOCK_READ_SIZE));
        if (byte_buffer_read_size_ == 0) {
          // TODO: log an error, we have an incomplete tuple at the end of the file
          current_range_remaining_len_ = 0;
          slot_idx_ = 0;
          delimited_text_parser_->ParserReset();
          boundary_column_.Clear();
          byte_buffer_ptr_ = NULL;
          continue;
        }
      } else {
        if (current_range_remaining_len_ == 0) {
          // Check if a tuple is straddling this block and the next:
          //  1. boundary_column_ is not empty
          //  2. if we are halfway through reading a tuple: !AtStart.
          //  3. We have are in the middle of the first column
          // We need to continue scanning until the end of the tuple.  Note that
          // boundary_column_ will be empty if we are on a column boundary,
          // but could still be inside a tuple. Similarly column_idx_ could be
          // first_materialised_col_idx if we are in the middle of reading the first
          // column. Therefore we need both checks.
          // TODO: test that hits this condition.
        if (!boundary_column_.Empty() || !delimited_text_parser_->AtTupleStart() ||
            (col_start != NULL && col_start != byte_buffer_ptr_)) {
            current_range_remaining_len_ = -1;
            continue;
          }
          *eosr = true;
          break;
        } else {
          // Continue reading from the current file location
          DCHECK_GE(current_range_remaining_len_, 0);

          int read_size = min(state->file_buffer_size(), current_range_remaining_len_);
          RETURN_IF_ERROR(FillByteBuffer(state, read_size));
        }
      }
    }

    // Parses (or resumes parsing) the current file buffer, appending tuples into
    // the tuple buffer until:
    // a) file buffer has been completely parsed or
    // b) tuple buffer is full
    // c) a parsing error was encountered and the user requested to abort on errors.
    int previous_num_rows = num_rows_returned_;
    // With two pass approach, we need to save some of the state before any of the file
    // was parsed
    char* line_start = byte_buffer_ptr_;
    int num_tuples = 0;
    int num_fields = 0;
    int max_tuples = row_batch->capacity() - row_batch->num_rows();
    // We are done with the current scan range, just parse for one more tuple to finish
    // it off
    if (current_range_remaining_len_ < 0) {
      max_tuples = 1;
    }
    char* previous_buffer_ptr = byte_buffer_ptr_;
    {
      COUNTER_SCOPED_TIMER(scan_node_->parse_time_counter());
      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(max_tuples,
          byte_buffer_end_ - byte_buffer_ptr_, &byte_buffer_ptr_,
          &field_locations_, &num_tuples, &num_fields, &col_start));
    }
    int bytes_processed = byte_buffer_ptr_ - line_start;
    current_range_remaining_len_ -= bytes_processed;

    DCHECK_GT(bytes_processed, 0);

    if (scan_node_->materialized_slots().size() != 0) {
      if (num_fields != 0) {
        // There can be one partial tuple which returned no more fields from this buffer.
        DCHECK_LE(num_tuples, num_fields + 1);
        if (!boundary_column_.Empty()) {
          CopyBoundaryField(&field_locations_[0]);
          boundary_column_.Clear();
        }
        RETURN_IF_ERROR(WriteFields(state, row_batch, num_fields, &row_idx, &line_start));
      } else if (col_start != previous_buffer_ptr) {
        // If we saw any delimiters col_start will move, clear the boundry_row_.
        boundary_row_.Clear();
      }
    } else if (num_tuples != 0) {
      // If we are doing count(*) then we return tuples only containing partition keys
      boundary_row_.Clear();
      line_start = byte_buffer_ptr_;
      RETURN_IF_ERROR(WriteTuples(state, row_batch, num_tuples, &row_idx));
    }

    // Cannot reuse file buffer if there are non-copied string slots materialized
    // TODO: If the tuple data contains very sparse string slots, we waste a lot of
    // memory.  Instead, we should consider copying the tuples to a compact new buffer
    // in this case.
    if (num_rows_returned_ > previous_num_rows && has_string_slots_) {
      reuse_byte_buffer_ = false;
    }

    if (scan_node_->ReachedLimit()) {
      break;
    }

    // Save contents that are split across buffers if we are going to return  this column
    if (col_start != byte_buffer_ptr_ && delimited_text_parser_->ReturnCurrentColumn()) {
      boundary_column_.Append(col_start, byte_buffer_ptr_ - col_start);
      boundary_row_.Append(line_start, byte_buffer_ptr_ - line_start);
    }

    if (current_range_remaining_len_ < 0) {
      DCHECK_LE(num_tuples, 1);
      // Just finished off this scan range
      if (num_tuples == 1) {
        DCHECK(boundary_column_.Empty());
        DCHECK_EQ(slot_idx_, 0);
        current_range_remaining_len_ = 0;
        byte_buffer_ptr_ = byte_buffer_end_;
      }
      *eosr = true;
      break;
    }

    // The row batch is full. We'll pick up where we left off in the next
    // GetNext() call.
    if (row_batch->IsFull()) {
      *eosr = false;

      DCHECK(delimited_text_parser_->AtTupleStart());
      return Status::OK;
    }
  }

  // This is the only non-error return path for this function.  There are
  // two return paths:
  // 1. EOS: limit is reached or scan range is complete
  // 2. row batch is full.
  // In either case, we must hand over ownership of all tuple data to the
  // row batch.  If it is not EOS, we'll need to keep the current buffers.
  if (!reuse_byte_buffer_) {
    row_batch->tuple_data_pool()->AcquireData(byte_buffer_pool_.get(), !*eosr);
  }
  row_batch->tuple_data_pool()->AcquireData(tuple_pool_, !*eosr);

  DCHECK(delimited_text_parser_->AtTupleStart());
  return Status::OK;
}

void HdfsTextScanner::ReportRowParseError(RuntimeState* state, char* line_start,
                                          int len) {
  ++num_errors_in_file_;
  if (state->LogHasSpace()) {
    state->error_stream() << "file: " << current_byte_stream_->GetLocation() << endl;
    state->error_stream() << "line: ";
    if (!boundary_row_.Empty()) {
      // Log the beginning of the line from the previous file buffer(s)
      state->error_stream() << boundary_row_.str();
    }
    // Log the erroneous line (or the suffix of a line if !boundary_line.empty()).
    state->error_stream() << string(line_start, len);
    state->LogErrorStream();
  }
}

// TODO: apply conjuncts as slots get materialized and skip to the end of the row
// if we determine it's not a match.
Status HdfsTextScanner::WriteFields(RuntimeState* state, RowBatch* row_batch,
                                    int num_fields, int* row_idx, char** line_start) {
  COUNTER_SCOPED_TIMER(scan_node_->tuple_write_timer());
  DCHECK_GT(num_fields, 0);

  // Keep track of where lines begin as we write out fields for error reporting
  int next_line_offset = 0;

  // Initialize tuple_ from the partition key template tuple before writing the slots
  if (template_tuple_ != NULL) {
    memcpy(tuple_, template_tuple_, tuple_byte_size_);
  }

  // Loop through all the parsed_data and parse out the values to slots
  for (int n = 0; n < num_fields; ++n) {
    int need_escape = false;
    int len = field_locations_[n].len;
    if (len < 0) {
      len = -len; 
      need_escape = true;
    }
    next_line_offset += (len + 1);

    boundary_row_.Clear();
    if (!text_converter_->WriteSlot(state,
        scan_node_->materialized_slots()[slot_idx_].second, tuple_,
        field_locations_[n].start, len, false, need_escape).ok()) {
      error_in_row_ = true;
    }

    // If slot_idx_ equals the number of materialized slots, we have completed
    // parsing the tuple.  At this point we can:
    //  - Report errors if there are any
    //  - Reset line_start, materialized_slot_idx to be ready to parse the next row
    //  - Materialize partition key values
    //  - Evaluate conjuncts and add if necessary
    if (++slot_idx_ == scan_node_->materialized_slots().size()) {
      if (error_in_row_) {
        ReportRowParseError(state, *line_start, next_line_offset - 1);
        error_in_row_ = false;
        if (state->abort_on_error()) {
          state->ReportFileErrors(current_byte_stream_->GetLocation(), 1);
          return Status(
              "Aborted HdfsScanner due to parse errors. View error log for details.");
        }
      }

      // Reset for next tuple
      *line_start += next_line_offset;
      next_line_offset = 0;
      slot_idx_ = 0;

      // We now have a complete row, with everything materialized
      DCHECK(!row_batch->IsFull());
      if (*row_idx == RowBatch::INVALID_ROW_INDEX) {
        *row_idx = row_batch->AddRow();
      }
      TupleRow* current_row = row_batch->GetRow(*row_idx);
      current_row->SetTuple(tuple_idx_, tuple_);

      // Evaluate the conjuncts and add the row to the batch
      bool conjuncts_true = scan_node_->EvalConjunctsForScanner(current_row);

      if (conjuncts_true) {
        row_batch->CommitLastRow();
        *row_idx = RowBatch::INVALID_ROW_INDEX;
        scan_node_->IncrNumRowsReturned();
        ++num_rows_returned_;
        if (scan_node_->ReachedLimit() || row_batch->IsFull()) {
          tuple_ = NULL;
          return Status::OK;
        }
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_byte_size_;
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
      }

      // Need to reset the tuple_ if
      //  1. eval failed (clear out null-indicator bits) OR
      //  2. there are partition keys that need to be copied
      // TODO: if the slots that need to be updated are very sparse (very few NULL slots
      // or very few partition keys), updating all the tuple memory is probably bad
      if (!conjuncts_true || template_tuple_ != NULL) {
        if (template_tuple_ != NULL) {
          memcpy(tuple_, template_tuple_, tuple_byte_size_);
        } else {
          tuple_->Init(tuple_byte_size_);
        }
      }
    }
  }
  return Status::OK;
}

void HdfsTextScanner::CopyBoundaryField(DelimitedTextParser::FieldLocation* data) {
  const int total_len = data->len + boundary_column_.Size();
  char* str_data = reinterpret_cast<char*>(tuple_pool_->Allocate(total_len));
  memcpy(str_data, boundary_column_.str().ptr, boundary_column_.Size());
  memcpy(str_data + boundary_column_.Size(), data->start, data->len);
  data->start = str_data;
  data->len = total_len;
}

