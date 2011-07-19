// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "text-scan-node.h"
#include <cstring>
#include <cstdlib>
#include <boost/algorithm/string/predicate.hpp>
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;
using namespace boost;
using namespace impala;

TextScanNode::TextScanNode(const TScanNode& tscan_node)
    : files_(tscan_node.file_paths),
      tuple_id_(tscan_node.tuple_id),
      tuple_desc_(NULL),
      tuple_delim_(DELIM_INIT),
      field_delim_(DELIM_INIT),
      collection_item_delim_(DELIM_INIT),
      escape_char_(DELIM_INIT),
      string_quote_(DELIM_INIT),
      strings_are_quoted_(false),
      tuple_buf_pool_(new MemPool(POOL_INIT_SIZE)),
      file_buf_pool_(new MemPool(POOL_INIT_SIZE)),
      var_len_pool_(new MemPool(POOL_INIT_SIZE)),
      hdfs_connection_(NULL),
      hdfs_file_(NULL),
      file_buf_actual_size_(0),
      file_idx_(0),
      tuple_buf_(NULL),
      tuple_idx_(0),
      tuple_(NULL),
      file_buf_(NULL),
      file_buf_ptr_(NULL),
      file_buf_end_(NULL) {
}

Status TextScanNode::Prepare(RuntimeState* state) {
  // Initialize ptr to end, to initiate reading the first file block
  file_buf_ptr_ = file_buf_end_;

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
  tuple_delim_ = tuple_desc_->table_desc()->line_delim();
  field_delim_ = tuple_desc_->table_desc()->field_delim();
  collection_item_delim_ = tuple_desc_->table_desc()->collection_delim();
  escape_char_ = tuple_desc_->table_desc()->escape_char();
  string_quote_ = tuple_desc_->table_desc()->quote_char();
  strings_are_quoted_ = tuple_desc_->table_desc()->strings_are_quoted();

  // Create mapping from column index in table to slot index in output tuple.
  // First, initialize all columns to SKIP_COLUMN.
  int num_cols = tuple_desc_->table_desc()->num_cols();
  column_idx_to_slot_idx.reserve(num_cols);
  column_idx_to_slot_idx.resize(num_cols);
  for (int i = 0; i < num_cols; i++) {
    column_idx_to_slot_idx[i] = SKIP_COLUMN;
  }

  // Next, set mapping from column index to slot index for all slots in the query.
  const std::vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (int i = 0; i < slots.size(); i++) {
    // TODO: this seems like the wrong place to do this subtraction.
    int file_col_pos = slots[i]->col_pos() - tuple_desc_->table_desc()->num_partition_keys();
    if (file_col_pos >= 0) {
      column_idx_to_slot_idx[file_col_pos] = i;
    }
  }

  // Allocate tuple buffer.
  tuple_buf_size_ = state->batch_size() * tuple_desc_->byte_size();
  tuple_buf_ = tuple_buf_pool_->Allocate(tuple_buf_size_);

  // TODO(marcel): add int tuple_idx_[] indexed by TupleId somewhere in runtime-state.h
  tuple_idx_ = 0;

  return Status::OK;
}

Status TextScanNode::Open(RuntimeState* state) {
  hdfs_connection_ = hdfsConnect("default", 0);
  if (hdfs_connection_ == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to connect to HDFS.");
  } else {
    return Status::OK;
  }
}

Status TextScanNode::Close(RuntimeState* state) {
  int hdfs_ret = hdfsDisconnect(hdfs_connection_);
  if (hdfs_ret == 0) {
    return Status::OK;
  } else {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to close HDFS connection.");
  }
}

void* TextScanNode::GetFileBuffer(RuntimeState* state, int* file_buf_idx) {
  // Never return file buffer 0, because it could contain valid data from a previous GetNext().
  ++*file_buf_idx;
  while (*file_buf_idx >= file_bufs_.size()) {
    file_bufs_.push_back(file_buf_pool_->Allocate(state->file_buf_size()));
  }
  return file_bufs_[*file_buf_idx];
}

Status TextScanNode::GetNext(RuntimeState* state, RowBatch* row_batch) {
  tuple_ = reinterpret_cast<Tuple*>(tuple_buf_);
  bzero(tuple_buf_, tuple_buf_size_);
  boundary_column_.clear();

  // Index into file buffer list.
  int file_buf_idx = 0;

  // Reset pool to reuse its already allocated memory.
  var_len_pool_->Clear();

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

  while (file_idx_ < files_.size()) {
    // Did we finish reading the previous file? if so, open next file
    if (file_buf_actual_size_ == 0) {
      hdfs_file_ = hdfsOpenFile(hdfs_connection_, files_[file_idx_].c_str(), O_RDONLY, 0, 0, 0);
      if (hdfs_file_ == NULL) {
        // TODO: make sure we print all available diagnostic output to our error log
        return Status("Failed to open HDFS file.");
      }
    }

    do {
      // Did we finish parsing the last file buffer? if so, read the next one
      // Note: we set file_buf_ptr_ == file_buf_end_ in Prepare() to read the first file block
      if (file_buf_ptr_ == file_buf_end_) {
        file_buf_ = reinterpret_cast<char*>(GetFileBuffer(state, &file_buf_idx));
        file_buf_actual_size_ =
            hdfsRead(hdfs_connection_, hdfs_file_, file_buf_, state->file_buf_size());
        file_buf_ptr_ = file_buf_;
        file_buf_end_ = file_buf_ + file_buf_actual_size_;
      }

      // Parses (or resumes parsing) the current file buffer,
      // appending tuples into the tuple buffer until:
      // a) file buffer has been completely parsed or
      // b) tuple buffer is full
      RETURN_IF_ERROR(ParseFileBuffer(row_batch, &row_idx, &column_idx,
          &last_char_is_escape, &unescape_string, &quote_char));

      if (row_batch->IsFull()) {
        // Swap file buffers, so previous buffer is always found at index 0,
        // and will never be overwritten in the following GetNext() call.
        void* tmp = file_bufs_[0];
        file_bufs_[0] = file_buf_;
        file_bufs_[file_buf_idx] = tmp;
        return Status::OK;
      }
    } while (file_buf_actual_size_ > 0);

    // Move on to next file
    int hdfs_ret = hdfsCloseFile(hdfs_connection_, hdfs_file_);
    if (hdfs_ret != 0) {
      // TODO: make sure we print all available diagnostic output to our error log
      return Status("Failed to close HDFS file.");
    }
    ++file_idx_;
  }

  // No more work to be done. Clean up all pools with the last row batch.
  row_batch->AddMemPool(tuple_buf_pool_.get());
  row_batch->AddMemPool(file_buf_pool_.get());
  row_batch->AddMemPool(var_len_pool_.get());

  // We have read all partitions (tuple buffer not necessarily full)
  return Status::OK;
}

Status TextScanNode::ParseFileBuffer(RowBatch* row_batch, int* row_idx, int* column_idx,
    bool* last_char_is_escape, bool* unescape_string, char* quote_char) {

  // Points to beginning of current column data.
  char* column_start = file_buf_ptr_;

  // We go through the file buffer character-by-character and:
  // 0. Determine whether we need to add a new row.
  //    This could be due a new row batch or file buffer,
  //    or because we added a new tuple in the previous iteration of the current file buffer.
  // 1. Determine action based on current character:
  // 1.1 Recognize field and collection delimiters (ignore delimiters if enclosed in quotes)
  // 1.2 Recognize tuple delimiter
  // 1.3 Recognize quote characters
  // 2. Take action if necessary:
  // 2.1 Write a new slot (if new_column==true and we don't ignore this column)
  // 2.2 Check if we finished a tuple, and whether the tuple buffer is full.
  while (file_buf_ptr_ != file_buf_end_) {

    // Indicates whether we have found a new complete slot.
    bool new_column = false;

    // new_tuple==true implies new_column==true
    bool new_tuple = false;

    // 0. Determine whether we need to add a new row.
    if (!row_batch->IsFull() && *row_idx == RowBatch::INVALID_ROW_INDEX) {
      *row_idx = row_batch->AddRow();
      TupleRow* tuple_row = row_batch->GetRow(*row_idx);
      tuple_row->SetTuple(tuple_idx_, tuple_);
    }

    // 1. Determine action based on current character:
    // All delimiters can be escaped, causing them to be ignored.
    // Unquoted strings must escape delimiters (and escape itself) for them to be
    // recognized as part of the string.
    char c = *file_buf_ptr_;
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
      // TODO: log the offending lines for broken input.
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
      int slot_idx = column_idx_to_slot_idx[*column_idx];
      if (slot_idx != SKIP_COLUMN) {
        *file_buf_ptr_ = '\0';
        if (boundary_column_.empty()) {
          ConvertAndWriteSlotBytes(column_start, file_buf_ptr_, tuple_,
              tuple_desc_->slots()[slot_idx], *unescape_string,
              *unescape_string);
        } else {
          boundary_column_.append(file_buf_, file_buf_ptr_ - file_buf_);
          ConvertAndWriteSlotBytes(boundary_column_.data(),
              boundary_column_.data() + boundary_column_.size(), tuple_,
              tuple_desc_->slots()[slot_idx], true, *unescape_string);
          boundary_column_.clear();
          *unescape_string = false;
        }
      }
      ++(*column_idx);
      column_start = file_buf_ptr_ + 1;
    }

    ++file_buf_ptr_;

    // 2.2 Check if we finished a tuple, and whether the tuple buffer is full.
    if (new_tuple) {
      *column_idx = 0;
      char* new_tuple = reinterpret_cast<char*> (tuple_);
      new_tuple += tuple_desc_->byte_size();
      // assert(new_tuple < tuple_buf_ + state->batch_size() * tuple_desc_->byte_size());
      tuple_ = reinterpret_cast<Tuple*> (new_tuple);
      // Will be set to a valid row_idx in the next iteration,
      // possibly after returning from this function and then re-entering with a new file buffer.
      *row_idx = RowBatch::INVALID_ROW_INDEX;
      if (row_batch->IsFull()) {
        // Tuple buffer is full.
        return Status::OK;
      }
    }
  }

  // Deal with beginning of boundary column.
  // Also handles case where entire new file block belongs to previous boundary column.
  if (column_start != file_buf_ptr_) {
    boundary_column_.append(column_start, file_buf_ptr_ - column_start);
  }

  return Status::OK;
}

void TextScanNode::ConvertAndWriteSlotBytes(const char* begin, const char* end, Tuple* tuple,
    const SlotDescriptor* slot_desc, bool copy_string, bool unescape_string) {

  // Check for null columns.
  // The below code implies that unquoted empty strings
  // such as "...,,..." become NULLs, and not empty strings.
  if (begin == end) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return;
  }
  // TODO: Handle out-of-range and other error conditions.
  switch (slot_desc->type()) {
    case TYPE_BOOLEAN: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      if (iequals(begin, "true")) {
        *reinterpret_cast<char*>(slot) = true;
      } else {
        *reinterpret_cast<char*>(slot) = false;
      }
      break;
    }
    case TYPE_TINYINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<char*>(slot) =
          static_cast<char>(strtol(begin, const_cast<char**>(&end), 0));
      break;
    }
    case TYPE_SMALLINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<short*>(slot) =
          static_cast<short>(strtol(begin, const_cast<char**>(&end), 0));
      break;
    }
    case TYPE_INT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<int*>(slot) =
          static_cast<int>(strtol(begin, const_cast<char**>(&end), 0));
      // Set NULL if inconvertible.
      if (*end != '\0') tuple->SetNull(slot_desc->null_indicator_offset());
      break;
    }
    case TYPE_BIGINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<long*>(slot) = strtol(begin, const_cast<char**>(&end), 0);
      break;
    }
    case TYPE_FLOAT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<float*>(slot) =
          static_cast<float>(strtod(begin, const_cast<char**>(&end)));
      break;
    }
    case TYPE_DOUBLE: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<double*>(slot) = strtod(begin, const_cast<char**>(&end));
      break;
    }
    case TYPE_STRING: {
      StringValue* slot = tuple->GetStringSlot(slot_desc->tuple_offset());
      const char* data_start = NULL;
      if (strings_are_quoted_) {
        // take out 2 characters for the quotes
        slot->len = end - begin - 2;
        // skip the quote char at the beginning
        data_start = begin + 1;
      } else {
        slot->len = end - begin;
        data_start = begin;
      }
      if (!copy_string) {
        slot->ptr = const_cast<char*>(data_start);
      } else {
        char* slot_data = reinterpret_cast<char*>(var_len_pool_->Allocate(slot->len));
        if (unescape_string) {
          UnescapeString(data_start, slot_data, &slot->len);
        } else {
          memcpy(slot_data, data_start, slot->len);
        }
        slot->ptr = slot_data;
      }
      break;
    }
    case INVALID_TYPE: {
      break;
    }
  }
  // Set NULL if inconvertible.
  if (*end != '\0') {
    tuple->SetNull(slot_desc->null_indicator_offset());
  }
}

void TextScanNode::UnescapeString(const char* src, char* dest, int* len) {
  char* dest_ptr = dest;
  const char* end = src + *len;
  while (src < end) {
    if (*src == escape_char_) {
      ++src;
    } else {
      *dest_ptr++ = *src++;
    }
  }
  char* dest_start = reinterpret_cast<char*>(dest);
  *len = dest_ptr - dest_start;
}
