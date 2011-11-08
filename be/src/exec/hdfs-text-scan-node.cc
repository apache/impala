// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-text-scan-node.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "exec/text-converter.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/debug-util.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;
using namespace boost;
using namespace impala;

HdfsTextScanNode::HdfsTextScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                   const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      tuple_desc_(NULL),
      tuple_idx_(0),
      tuple_delim_(DELIM_INIT),
      field_delim_(DELIM_INIT),
      collection_item_delim_(DELIM_INIT),
      escape_char_(DELIM_INIT),
      strings_are_quoted_(false),
      string_quote_(DELIM_INIT),
      tuple_pool_(new MemPool()),
      file_buffer_pool_(new MemPool()),
      obj_pool_(new ObjectPool()),
      hdfs_connection_(NULL),
      hdfs_file_(NULL),
      file_buffer_read_size_(0),
      file_idx_(0),
      tuple_buffer_(NULL),
      tuple_(NULL),
      file_buffer_(NULL),
      file_buffer_ptr_(NULL),
      file_buffer_end_(NULL),
      num_errors_in_file_(0),
      num_partition_keys_(0),
      text_converter_(NULL) {
  // Initialize partition key regex
  Status status = Init(pool, tnode);
  DCHECK(status.ok()) << "HdfsTextScanNode c'tor:Init failed: \n" << status.GetErrorMsg();
}

Status HdfsTextScanNode::Init(ObjectPool* pool, const TPlanNode& tnode) {
  try {
    partition_key_regex_ = regex(tnode.hdfs_scan_node.partition_key_regex);
  } catch(bad_expression&) {
    std::stringstream ss;
    ss << "HdfsTextScanNode::Init(): "
       << "Invalid regex: " << tnode.hdfs_scan_node.partition_key_regex;
    return Status(ss.str());
  }  
  return Status::OK;
}

Status HdfsTextScanNode::Prepare(RuntimeState* state) {
  PrepareConjuncts(state);
  // Initialize ptr to end, to initiate reading the first file block
  file_buffer_ptr_ = file_buffer_end_;

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
  tuple_delim_ = hdfs_table->line_delim();
  field_delim_ = hdfs_table->field_delim();
  collection_item_delim_ = hdfs_table->collection_delim();
  escape_char_ = hdfs_table->escape_char();
  string_quote_ = hdfs_table->quote_char();
  strings_are_quoted_ = hdfs_table->strings_are_quoted();
  text_converter_.reset(
      new TextConverter(strings_are_quoted_, escape_char_, tuple_pool_.get()));

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
  // We also set the key_idx_to_slot_idx_ to mapping for materializing partition keys.
  const std::vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (size_t i = 0; i < slots.size(); i++) {
    if (!slots[i]->is_materialized()) continue;
    if (hdfs_table->IsClusteringCol(slots[i])) {
      // Set partition-key index to slot mapping.
      // assert(key_idx_to_slot_idx_.size() * num_partition_keys_ + slots[i]->col_pos()
      //        < key_values_.size());
      key_idx_to_slot_idx_.push_back(make_pair(slots[i]->col_pos(), i));
    } else {
      // Set column to slot mapping.
      // assert(slots[i]->col_pos() - num_partition_keys_ >= 0);
      column_idx_to_slot_idx_[slots[i]->col_pos() - num_partition_keys_] = i;
    }
  }
  partition_key_values_.resize(key_idx_to_slot_idx_.size());
  
  // TODO(marcel): add int tuple_idx_[] indexed by TupleId somewhere in runtime-state.h
  tuple_idx_ = 0;

  return Status::OK;
}

Status HdfsTextScanNode::Open(RuntimeState* state) {
  hdfs_connection_ = hdfsConnect("default", 0);
  if (hdfs_connection_ == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to connect to HDFS.");
  } else {
    return Status::OK;
  }
}

Status HdfsTextScanNode::Close(RuntimeState* state) {
  int hdfs_ret = hdfsDisconnect(hdfs_connection_);
  if (hdfs_ret == 0) {
    return Status::OK;
  } else {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to close HDFS connection.");
  }
}

Status HdfsTextScanNode::GetNext(RuntimeState* state, RowBatch* row_batch) {
  if (ReachedLimit()) return Status::OK;

  // create new tuple buffer for row_batch
  tuple_buffer_size_ = row_batch->capacity() * tuple_desc_->byte_size();
  tuple_buffer_ = tuple_pool_->Allocate(tuple_buffer_size_);
  bzero(tuple_buffer_, tuple_buffer_size_);
  tuple_ = reinterpret_cast<Tuple*>(tuple_buffer_);

  boundary_column_.clear();

  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;

  // Index into current column.
  int column_idx = 0;

  // Indicates whether the last character parsed was an escape.
  bool last_char_is_escape = false;

  // Indicates whether the current string contains an escape
  // If so, we must remove the escapes and copy the string data
  bool unescape_string = false;

  // Indicates whether we are parsing a quoted string.
  // If not, it is set to NOT_IN_STRING.
  // Else it is set to the quote char that started the quoted string.
  char quote_char = NOT_IN_STRING;

  // Indicates whether the current row has errors.
  bool error_in_row = false;

  while (file_idx_ < files_.size()) {
    // Did we finish reading the previous file? if so, open next file
    if (file_buffer_read_size_ == 0) {
      hdfs_file_ = hdfsOpenFile(hdfs_connection_, files_[file_idx_].c_str(), O_RDONLY, 0, 0, 0);
      if (hdfs_file_ == NULL) {
        // TODO: make sure we print all available diagnostic output to our error log
        return Status("Failed to open HDFS file.");
      }
      // Extract partition keys from this file path.
      RETURN_IF_ERROR(ExtractPartitionKeyValues(state));
      num_errors_in_file_ = 0;
      VLOG(1) << "HdfsTextScanNode: opened file " << files_[file_idx_];
    }

    do {
      // Did we finish parsing the last file buffer? if so, read the next one
      // Note: we set file_buffer_ptr_ == file_buffer_end_ in Prepare() to read the
      // first file block
      if (file_buffer_ptr_ == file_buffer_end_) {
        // TODO: re-use file_buffer_ if it didn't contain any data we're going to return
        file_buffer_ = file_buffer_pool_->Allocate(state->file_buffer_size());
        file_buffer_read_size_ =
            hdfsRead(hdfs_connection_, hdfs_file_, file_buffer_, state->file_buffer_size());
        VLOG(1) << "HdfsTextScanNode: read " << file_buffer_read_size_
                << " bytes in file " << files_[file_idx_];
        file_buffer_ptr_ = file_buffer_;
        file_buffer_end_ = file_buffer_ + file_buffer_read_size_;
      }

      // Parses (or resumes parsing) the current file buffer,
      // appending tuples into the tuple buffer until:
      // a) file buffer has been completely parsed or
      // b) tuple buffer is full
      // c) a parsing error was encountered and the user requested to abort on errors.
      RETURN_IF_ERROR(
          ParseFileBuffer(state, row_batch, &row_idx, &column_idx, &last_char_is_escape,
                          &unescape_string, &quote_char, &error_in_row));

      if (!ReachedLimit() && row_batch->IsFull()) {
        // If we've reached our limit, continue on to close the current file.
        // Otherwise simply return, the next GetNext() call will pick up where we
        // left off.
        // Hang on to the last chunks, we'll continue from there in the next
        // GetNext() call.
        row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), true);
        row_batch->tuple_data_pool()->AcquireData(file_buffer_pool_.get(), true);
        return Status::OK;
      }
    } while (!ReachedLimit() && file_buffer_read_size_ > 0);

    // Close current file.
    int hdfs_ret = hdfsCloseFile(hdfs_connection_, hdfs_file_);
    if (hdfs_ret != 0) {
      // TODO: make sure we print all available diagnostic output to our error log
      return Status("Failed to close HDFS file.");
    }

    // Report number of errors encountered in the current file, if any.
    if (num_errors_in_file_ > 0) {
      state->ReportFileErrors(files_[file_idx_], num_errors_in_file_);
    }

    // if we hit our limit, return now, after having closed the current file
    // and cleaned up the pools
    if (ReachedLimit()) {
      row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
      row_batch->tuple_data_pool()->AcquireData(file_buffer_pool_.get(), false);
      return Status::OK;
    }

    // Move on to next file.
    ++file_idx_;
  }

  // No more work to be done. Clean up all pools with the last row batch.
  row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
  row_batch->tuple_data_pool()->AcquireData(file_buffer_pool_.get(), false);

  // We have read all partitions (tuple buffer not necessarily full).
  return Status::OK;
}

// This first assembles the full tuple before applying the conjuncts.
// TODO: apply conjuncts as slots get materialized and skip to the end of the row
// if we determine it's not a match.
Status HdfsTextScanNode::ParseFileBuffer(RuntimeState* state, RowBatch* row_batch, int* row_idx,
    int* column_idx, bool* last_char_is_escape, bool* unescape_string, char* quote_char,
    bool* error_in_row) {

  // Points to beginning of current column data.
  char* column_start = file_buffer_ptr_;

  // Points to beginning of current line data.
  char* line_start = file_buffer_ptr_;

  // We go through the file buffer character-by-character and:
  // 0. Determine whether we need to add a new row.
  //    This could be due a new row batch or file buffer,
  //    or because we added a new tuple in the previous iteration of the current file buffer.
  //    Also materialize the virtual partition keys (if any) into the tuple.
  // 1. Determine action based on current character:
  // 1.1 Recognize field and collection delimiters (ignore delimiters if enclosed in quotes)
  // 1.2 Recognize tuple delimiter
  // 1.3 Recognize quote characters
  // 2. Take action if necessary:
  // 2.1 Write a new slot (if new_column==true and we don't ignore this column)
  // 2.2 Check if we finished a tuple, and whether the tuple buffer is full.
  TupleRow* current_row = NULL;
  if (*row_idx != RowBatch::INVALID_ROW_INDEX) {
    current_row = row_batch->GetRow(*row_idx);
  }

  while (file_buffer_ptr_ != file_buffer_end_) {

    // Indicates whether we have found a new complete slot.
    bool new_column = false;

    // new_tuple==true implies new_column==true
    bool new_tuple = false;

    // add new row
    if (!row_batch->IsFull() && *row_idx == RowBatch::INVALID_ROW_INDEX) {
      *row_idx = row_batch->AddRow();
      current_row = row_batch->GetRow(*row_idx);
      current_row->SetTuple(tuple_idx_, tuple_);
      // don't materialize partition key values at this point, we might
      // not find a matching tuple until we start reading a new file
    }

    // 1. Determine action based on current character:
    // All delimiters can be escaped, causing them to be ignored.
    // Unquoted strings must escape delimiters (and escape itself) for them to be
    // recognized as part of the string.
    char c = *file_buffer_ptr_;
    if (!*last_char_is_escape && (c == field_delim_ || c == collection_item_delim_)) {
      // 1.1 Recognize field and collection delimiters
      if (*quote_char == NOT_IN_STRING) {
        new_column = true;
      }
    } else if (!*last_char_is_escape && c == tuple_delim_) {
      // 1.2: Recognize tuple delimiter
      if (*quote_char == NOT_IN_STRING) {
        new_column = true;
        new_tuple = true;
      }
    } else if (strings_are_quoted_ && c == string_quote_) {
      // 1.3: Recognize quote characters
      // Broken input like 'text col''text col2',..., will be treated as:
      // text col''test col2 (without the enclosing quotes).
      // The problem is that column_idx will only be incremented once,
      // which will corrupt the output tuple or cause an
      // exception to be thrown in ConvertAndWriteSlotBytes due to a failed type conversion.
      if (*quote_char == NOT_IN_STRING) {
        *quote_char = c;
      } else {
        // Matches opening quote char
        if (*last_char_is_escape) {
          // Quote escaped as part of string
          *unescape_string = true;
        } else {
          // Quote not escaped, indicating end of string
          *quote_char = NOT_IN_STRING;
        }
      }
      *last_char_is_escape = false;
    } else if (c == escape_char_) {
      if (*last_char_is_escape) {
        *last_char_is_escape = false;
      } else {
        *last_char_is_escape = true;
      }
    } else {
      *last_char_is_escape = false;
    }

    // 2. Take action if necessary:
    // 2.1 Write a new slot
    if (new_column) {
      // Replace field delimiter with zero terminator.
      int slot_idx = column_idx_to_slot_idx_[*column_idx];
      if (slot_idx != SKIP_COLUMN) {
        char delim_char = *file_buffer_ptr_;
        *file_buffer_ptr_ = '\0';
        bool parsed_ok = true;
        if (boundary_column_.empty()) {
          parsed_ok = text_converter_->ConvertAndWriteSlotBytes(column_start, file_buffer_ptr_,
              tuple_, tuple_desc_->slots()[slot_idx], *unescape_string, *unescape_string);
        } else {
          boundary_column_.append(file_buffer_, file_buffer_ptr_ - file_buffer_);
          parsed_ok = text_converter_->ConvertAndWriteSlotBytes(boundary_column_.data(),
              boundary_column_.data() + boundary_column_.size(), tuple_,
              tuple_desc_->slots()[slot_idx], true, *unescape_string);
          boundary_column_.clear();
          *unescape_string = false;
        }
        // Error logging. Append string to error_msg.
        if (!parsed_ok) {
          *error_in_row = true;
          if (state->LogHasSpace()) {
            state->error_stream() << "Error converting column: " << *column_idx <<
                " TO " << TypeToString(tuple_desc_->slots()[slot_idx]->type()) << endl;
          }
        }
        *file_buffer_ptr_ = delim_char;
      }
      ++(*column_idx);
      column_start = file_buffer_ptr_ + 1;
    }

    ++file_buffer_ptr_;

    // 2.2 Check if we finished a tuple, and whether the tuple buffer is full.
    if (new_tuple) {
      // Error logging: Write the errors and the erroneous line to the log.
      if (*error_in_row) {
        ++num_errors_in_file_;
        *error_in_row = false;
        if (state->LogHasSpace()) {
          state->error_stream() << "file: " << files_[file_idx_] << endl;
          state->error_stream() << "line: ";
          if (!boundary_row_.empty()) {
            // Log the beginning of the line from the previous file buffer(s)
            state->error_stream() << boundary_row_;
          }
          // Log the erroneous line (or the suffix of a line if !boundary_line.empty()).
          state->error_stream() << string(line_start, file_buffer_ptr_ - line_start - 1);
          state->LogErrorStream();
        }
        if (state->abort_on_error()) {
          state->ReportFileErrors(files_[file_idx_], 1);
          return Status(
              "Aborted HdfsTextScanNode due to parse errors. View error "
              "log for details.");
        }
      }
      *column_idx = 0;
      line_start = file_buffer_ptr_;
      boundary_row_.clear();

      // Materialize partition-key values (if any).
      for (int i = 0; i < key_idx_to_slot_idx_.size(); ++i) {
        int slot_idx = key_idx_to_slot_idx_[i].second;
        Expr* expr = partition_key_values_[i];
        SlotDescriptor* slot = tuple_desc_->slots()[slot_idx];
        RawValue::Write(expr->GetValue(NULL), tuple_, slot, tuple_pool_.get());
      }

      if (EvalConjuncts(current_row)) {
        row_batch->CommitLastRow();
        VLOG(1) << "scanned row " << PrintRow(current_row, row_desc());
        ++num_rows_returned_;
        if (ReachedLimit()) return Status::OK;
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_desc_->byte_size();
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
        *row_idx = RowBatch::INVALID_ROW_INDEX;
        if (row_batch->IsFull()) {
          // Tuple buffer is full.
          return Status::OK;
        }
        // don't check until we know we'll use tuple_
        DCHECK_LE(new_tuple, tuple_buffer_ + tuple_buffer_size_ - tuple_desc_->byte_size());
      } else {
        // Make sure to reset null indicators since we're overwriting
        // the tuple assembled for the previous row;
        // this also wipes out materialized partition key values.
        tuple_->Init(tuple_desc_->byte_size());
      }
    }
  }

  // Deal with beginning of boundary column/line.
  // Also handles case where entire new file block belongs to previous boundary column.
  if (column_start != file_buffer_ptr_) {
    boundary_column_.append(column_start, file_buffer_ptr_ - column_start);
    boundary_row_.append(line_start, file_buffer_ptr_ - line_start);
  }

  return Status::OK;
}

void HdfsTextScanNode::DebugString(int indentation_level, std::stringstream* out) const {
  *out << string(indentation_level * 2, ' ')
       << "HdfsTextScanNode(tupleid=" << tuple_id_ << " files[" << join(files_, ",")
       << " regex=" << partition_key_regex_ << " ";
  ExecNode::DebugString(indentation_level, out);
  *out << "])" << endl;
}

void HdfsTextScanNode::SetScanRange(const TScanRange& scan_range) {
  DCHECK(scan_range.__isset.hdfsFileSplits);
  for (int i = 0; i < scan_range.hdfsFileSplits.size(); ++i) {
    files_.push_back(scan_range.hdfsFileSplits[i].path);
    // TODO: take into account offset and length 
  }
}

Status HdfsTextScanNode::ExtractPartitionKeyValues(RuntimeState* state) {
  DCHECK_LT(file_idx_, files_.size());
  if (key_idx_to_slot_idx_.size() == 0) return Status::OK;

  smatch match;
  const string& file = files_[file_idx_];
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

