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
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/parse-util.h"
#include "util/sse-util.h"
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
      reuse_file_buffer_(false),
      num_errors_in_file_(0),
      num_partition_keys_(0),
      num_materialized_slots_(0),
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
      ++num_materialized_slots_;
    }
  }
  // Find all the materialized non-partition key slots.  These slots are in the
  // order they will show up in the text file.
  for (int i = 0; i < num_cols; ++i) {
    if (column_idx_to_slot_idx_[i] != SKIP_COLUMN) {
      materialized_slots_.push_back(slots[column_idx_to_slot_idx_[i]]);
    }
  }
  partition_key_values_.resize(key_idx_to_slot_idx_.size());
  
  // TODO(marcel): add int tuple_idx_[] indexed by TupleId somewhere in runtime-state.h
  tuple_idx_ = 0;

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple.
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  parse_data_.resize(state->batch_size() * num_materialized_slots_);

  // Initialize the sse search registers.
  // TODO: is this safe to do in prepare?  Not sure if the compiler/system
  // will manage these registers for us.  
  char tmp[SSEUtil::CHARS_PER_128_BIT_REGISTER];
  memset(tmp, 0, sizeof(tmp));
  tmp[0] = tuple_delim_;
  xmm_tuple_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(tmp));
  tmp[0] = escape_char_;
  xmm_escape_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(tmp));
  tmp[0] = field_delim_;
  tmp[1] = collection_item_delim_;
  xmm_field_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(tmp));
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

Status HdfsTextScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  // create new tuple buffer for row_batch
  tuple_buffer_size_ = row_batch->capacity() * tuple_desc_->byte_size();
  tuple_buffer_ = tuple_pool_->Allocate(tuple_buffer_size_);
  bzero(tuple_buffer_, tuple_buffer_size_);
  tuple_ = reinterpret_cast<Tuple*>(tuple_buffer_);

  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;

  // Indicates whether the current string contains an escape
  // If so, we must remove the escapes and copy the string data
  bool unescape_string = false;

  // Indicates whether we are parsing a quoted string.
  // If not, it is set to NOT_IN_STRING.
  // Else it is set to the quote char that started the quoted string.
  char quote_char = NOT_IN_STRING;

  while (file_idx_ < files_.size()) {
    // Did we finish reading the previous file? if so, open next file
    if (file_buffer_read_size_ == 0) {
      hdfs_file_ =
          hdfsOpenFile(hdfs_connection_, files_[file_idx_].c_str(), O_RDONLY, 0, 0, 0);
      if (hdfs_file_ == NULL) {
        // TODO: make sure we print all available diagnostic output to our error log
        return Status("Failed to open HDFS file " + files_[file_idx_]);
      }
      // Extract partition keys from this file path.
      RETURN_IF_ERROR(ExtractPartitionKeyValues(state));
      num_errors_in_file_ = 0;
      VLOG(1) << "HdfsTextScanNode: opened file " << files_[file_idx_];
  
      // These state fields cannot be split across files
      column_idx_ = 0;
      slot_idx_ = 0;
      last_char_is_escape_ = false;
      current_column_has_escape_ = false;
      error_in_row_ = false;
    }

    do {
      // Did we finish parsing the last file buffer? if so, read the next one
      // Note: we set file_buffer_ptr_ == file_buffer_end_ in Prepare() to read the
      // first file block
      if (file_buffer_ptr_ == file_buffer_end_) {
        if (!reuse_file_buffer_) {
          file_buffer_ = file_buffer_pool_->Allocate(state->file_buffer_size());
          reuse_file_buffer_ = true;
        } 
        file_buffer_read_size_ =
            hdfsRead(hdfs_connection_, hdfs_file_, file_buffer_,
                     state->file_buffer_size());
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
      int previous_num_rows = num_rows_returned_;
      if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2) && !strings_are_quoted_) {
        // With two pass approach, we need to save some of the state before any of the file
        // was parsed
        char* col_start = NULL;
        char* line_start = file_buffer_ptr_;
        int num_tuples = 0;
        int num_fields = 0;
        int max_tuples = row_batch->capacity() - row_batch->num_rows();
        RETURN_IF_ERROR(ParseFileBufferSSE(max_tuples, &num_tuples, &num_fields, &col_start));
        if (num_fields != 0) {
          RETURN_IF_ERROR(WriteFields(state, row_batch, num_fields, &row_idx, &line_start));
        } else {
          RETURN_IF_ERROR(WriteTuples(state, row_batch, num_tuples, &row_idx, &line_start));
        }
        // Save contents that are split across files
        if (col_start != file_buffer_ptr_) {
          boundary_column_.append(col_start, file_buffer_ptr_ - col_start);
          boundary_row_.append(line_start, file_buffer_ptr_ - line_start);
        }
      } else {
        RETURN_IF_ERROR(
          ParseFileBuffer(state, row_batch, &row_idx, &unescape_string, &quote_char));
      }
      // TODO: does it ever make sense to deep copy some of the tuples (string data) if the
      // number of tuples in this batch is very small?
      // Cannot reuse file buffer if there are non-copied string slots materialized
      if (num_rows_returned_ > previous_num_rows &&
          !tuple_desc_->string_slots().empty() &&
          !strings_are_quoted_) {
        reuse_file_buffer_ = false;
      } 
      
      if (!ReachedLimit() && row_batch->IsFull()) {
        // If we've reached our limit, continue on to close the current file.
        // Otherwise simply return, the next GetNext() call will pick up where we
        // left off.
        // Hang on to the last chunks, we'll continue from there in the next
        // GetNext() call.
        row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), true);
        if (!reuse_file_buffer_) {
          row_batch->tuple_data_pool()->AcquireData(file_buffer_pool_.get(), true);
          file_buffer_ = NULL;
        }
        *eos = false;
        return Status::OK;
      }
    } while (!ReachedLimit() && file_buffer_read_size_ > 0);
    // Close current file.
    int hdfs_ret = hdfsCloseFile(hdfs_connection_, hdfs_file_);
    if (hdfs_ret != 0) {
      // TODO: make sure we print all available diagnostic output to our error log
      return Status("Failed to close HDFS file " + files_[file_idx_]);
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
      *eos = true;
      return Status::OK;
    }

    // Move on to next file.
    ++file_idx_;
  }

  // No more work to be done. Clean up all pools with the last row batch.
  row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
  row_batch->tuple_data_pool()->AcquireData(file_buffer_pool_.get(), false);

  // We have read all partitions (tuple buffer not necessarily full).
  *eos = true;
  return Status::OK;
}

// This first assembles the full tuple before applying the conjuncts.
// TODO: apply conjuncts as slots get materialized and skip to the end of the row
// if we determine it's not a match.
Status HdfsTextScanNode::ParseFileBuffer(
    RuntimeState* state, RowBatch* row_batch, int* row_idx, bool* unescape_string,
    char* quote_char) {

  // Points to beginning of current column data.
  char* column_start = file_buffer_ptr_;

  // Points to beginning of current line data.
  char* line_start = file_buffer_ptr_;

  // We go through the file buffer character-by-character and:
  // 0. Determine whether we need to add a new row.
  //    This could be due to a new row batch or file buffer,
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
    if (!last_char_is_escape_ && (c == field_delim_ || c == collection_item_delim_)) {
      // 1.1 Recognize field and collection delimiters
      if (*quote_char == NOT_IN_STRING) {
        new_column = true;
      }
    } else if (!last_char_is_escape_ && c == tuple_delim_) {
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
        if (last_char_is_escape_) {
          // Quote escaped as part of string
          *unescape_string = true;
        } else {
          // Quote not escaped, indicating end of string
          *quote_char = NOT_IN_STRING;
        }
      }
      last_char_is_escape_ = false;
    } else if (c == escape_char_) {
      if (last_char_is_escape_) {
        last_char_is_escape_ = false;
      } else {
        last_char_is_escape_ = true;
      }
    } else {
      last_char_is_escape_ = false;
    }

    // 2. Take action if necessary:
    // 2.1 Write a new slot
    if (new_column) {
      DCHECK_LT(column_idx_, column_idx_to_slot_idx_.size());
      // Replace field delimiter with zero terminator.
      int slot_idx = column_idx_to_slot_idx_[column_idx_];
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
        }
        // Error logging. Append string to error_msg.
        if (!parsed_ok) {
          error_in_row_ = true;
          if (state->LogHasSpace()) {
            state->error_stream() << "Error converting column: " << column_idx_ << " TO "
              << TypeToString(tuple_desc_->slots()[slot_idx]->type()) << endl;
          }
        }
        *file_buffer_ptr_ = delim_char;
      }
      *unescape_string = false;
      boundary_column_.clear();
      ++column_idx_;
      column_start = file_buffer_ptr_ + 1;
    }

    ++file_buffer_ptr_;

    // 2.2 Check if we finished a tuple, and whether the tuple buffer is full.
    if (new_tuple) {
      // Error logging: Write the errors and the erroneous line to the log.
      if (error_in_row_) {
        ReportRowParseError(state, line_start, file_buffer_ptr_ - line_start - 1);
        error_in_row_ = false;
        if (state->abort_on_error()) {
          state->ReportFileErrors(files_[file_idx_], 1);
          return Status(
              "Aborted HdfsTextScanNode due to parse errors. View error log for details.");
        }
      }
      column_idx_ = 0;
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

// Updates the values in the field and tuple masks, escaping them if necessary.
// If the character at n is an escape character, then delimiters(tuple/field/escape characters)
// at n+1 don't count.
inline void ProcessEscapeMask(int escape_mask, bool* last_char_is_escape, int* field_mask, 
    int* tuple_mask) {
  // Escape characters can escape escape characters.  
  bool first_char_is_escape = *last_char_is_escape;
  bool escape_next = first_char_is_escape;
  for (int i = 0; i < SSEUtil::CHARS_PER_128_BIT_REGISTER; ++i) {
    if (escape_next) {
      escape_mask &= ~SSEUtil::SSE_BITMASK[i];
    }
    escape_next = escape_mask & SSEUtil::SSE_BITMASK[i];
  }

  // Remember last character for the next iteration
  *last_char_is_escape = escape_mask & 
    SSEUtil::SSE_BITMASK[SSEUtil::CHARS_PER_128_BIT_REGISTER - 1];

  // Shift escape mask up one so they match at the same bit index as the tuple and field mask
  // (instead of being the character before) and set the correct first bit
  escape_mask = escape_mask << 1 | first_char_is_escape;

  // If escape_mask[n] is true, then tuple/field_mask[n] is escaped
  *tuple_mask &= ~escape_mask;
  *field_mask &= ~escape_mask;
}

// SSE optimized raw text file parsing.  SSE4_2 added an instruction (with 3 modes) for text
// processing.  The modes mimick strchr, strstr and strcmp.  For text parsing, we can leverage
// the strchr functionality.
//
// The instruction operates on two sse registers:
//  - the needle (what you are searching for)
//  - the haystack (where you are searching in)
// Both registers can contain up to 16 characters.  The result is a 16-bit mask with a bit
// set for each character in the haystack that matched any character in the needle.
// For example:
//  Needle   = 'abcd000000000000' (we're searching for any a's, b's, c's d's)
//  Haystack = 'asdfghjklhjbdwwc' (the raw string)
//  Result   = '101000000001101'
Status HdfsTextScanNode::ParseFileBufferSSE(int max_tuples, int* num_tuples, int* num_fields, 
    char** column_start) {
  DCHECK(CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2));
  DCHECK(!strings_are_quoted_);

  // Start of this batch.
  *column_start = file_buffer_ptr_;
  
  // To parse using SSE, we:
  //  1. Load into different sse registers the different characters we need to search for
  //      - tuple breaks, field breaks, escape characters
  //  2. Load 16 characters at a time into the sse register
  //  3. Use the SSE instruction to do strchr on those 16 chars, the result is a bitmask
  //  4. Compute the bitmask for tuple breaks, field breaks and escape characters.
  //  5. If there are escape characters, fix up the matching masked bits in the field/tuple mask
  //  6. Go through the mask bit by bit and write the parsed data.

  // xmm registers:
  //  - xmm_buffer: the register holding the current (16 chars) we're working on from the file
  //  - xmm_tuple_search_: the tuple search register.  Only contains the tuple_delim char.
  //  - xmm_field_search_: the field search register.  Contains field delim and 
  //        collection_item delim_char
  //  - xmm_escape_search_: the escape search register. Only contains escape char
  //  - xmm_tuple_mask: the result of doing strchr for the tuple delim
  //  - xmm_field_mask: the reuslt of doing strchr for the field delim
  //  - xmm_escape_mask: the reuslt of doing strchr for the escape char
  __m128i xmm_buffer, xmm_tuple_mask, xmm_field_mask, xmm_escape_mask;
  
  // Length remaining of buffer to process
  int remaining_len = file_buffer_end_ - file_buffer_ptr_;
  while (remaining_len >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
    // Load the next 16 bytes into the xmm register
    xmm_buffer = _mm_loadu_si128(reinterpret_cast<__m128i*>(file_buffer_ptr_));

    // Do the strchr for tuple and field breaks
    // TODO: can we parallelize this as well?  Are there multiple sse execution units?
    xmm_tuple_mask = _mm_cmpistrm(xmm_tuple_search_, xmm_buffer, SSEUtil::STRCHR_MODE);
    xmm_field_mask = _mm_cmpistrm(xmm_field_search_, xmm_buffer, SSEUtil::STRCHR_MODE);

    // The strchr sse instruction returns the result in the lower bits of the sse register.
    // Since we only process 16 characters at a time, only the lower 16 bits can contain
    // non-zero values.
    int tuple_mask = reinterpret_cast<int*>(&xmm_tuple_mask)[0];
    int field_mask = reinterpret_cast<int*>(&xmm_field_mask)[0];
    int escape_mask = 0;

    // If the table does not use escape characters, skip processing for it.
    if (escape_char_ != '\0') {
      xmm_escape_mask = _mm_cmpistrm(xmm_escape_search_, xmm_buffer, SSEUtil::STRCHR_MODE);
      escape_mask = reinterpret_cast<int*>(&xmm_escape_mask)[0];
      ProcessEscapeMask(escape_mask, &last_char_is_escape_, &field_mask, &tuple_mask);
    }

    // Tuple delims are automatically field delims
    field_mask |= tuple_mask;

    if (field_mask != 0) {
      // Loop through the mask and find the tuple/column offsets
      for (int n = 0; n < SSEUtil::CHARS_PER_128_BIT_REGISTER; ++n) {
        if (escape_mask != 0) {
          current_column_has_escape_ =  
              current_column_has_escape_ || (escape_mask & SSEUtil::SSE_BITMASK[n]);
        }

        if (field_mask & SSEUtil::SSE_BITMASK[n]) {
          char* column_end = file_buffer_ptr_ + n;
          // TODO: apparently there can be columns not in the schema which should be ignored.
          // This does not handle that.
          if (column_idx_to_slot_idx_[column_idx_] != SKIP_COLUMN) {
            DCHECK_LT(*num_fields, parse_data_.size());
            // Found a column that needs to be parsed, write the start/len to 'parsed_data_'
            const int len = column_end - *column_start;
            parse_data_[*num_fields].start = *column_start;
            if (!current_column_has_escape_) {
              parse_data_[*num_fields].len = len;
            } else {
              parse_data_[*num_fields].len = -len;
            }
            if (!boundary_column_.empty()) {
              CopyBoundaryField(&parse_data_[*num_fields]);
            }
            ++(*num_fields);
          }
          current_column_has_escape_ = false;
          boundary_column_.clear();
          *column_start = column_end + 1;
          ++column_idx_;
        }

        if (tuple_mask & SSEUtil::SSE_BITMASK[n]) {
          column_idx_ = 0;
          ++(*num_tuples);
          if (*num_tuples == max_tuples) {
            file_buffer_ptr_ += (n + 1);
            last_char_is_escape_ = false;
            return Status::OK;
          }
        }
      }
    } else {
      current_column_has_escape_ = (current_column_has_escape_ || escape_mask);
    }

    remaining_len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
    file_buffer_ptr_ += SSEUtil::CHARS_PER_128_BIT_REGISTER;
  }

  // Handle the remaining characters
  // TODO: run this code if sse4 is not there (and remove ParseFileBuffers)
  while (remaining_len > 0) {
    bool new_tuple = false;
    bool new_col = false;
    
    if (!last_char_is_escape_) {
      if (*file_buffer_ptr_ == tuple_delim_) {
        new_tuple = true;
        new_col = true;
      } else if (*file_buffer_ptr_ == field_delim_ || *file_buffer_ptr_ == collection_item_delim_) {
        new_col = true;
      }
    } 
    if (*file_buffer_ptr_ == escape_char_) {
      current_column_has_escape_ = true;
      last_char_is_escape_ = !last_char_is_escape_;
    } else {
      last_char_is_escape_ = false;
    }

    if (new_col) {
      if (column_idx_to_slot_idx_[column_idx_] != SKIP_COLUMN) {
        DCHECK_LT(*num_fields, parse_data_.size());
        // Found a column that needs to be parsed, write the start/len to 'parsed_data_'
        parse_data_[*num_fields].start = *column_start;
        parse_data_[*num_fields].len = file_buffer_ptr_ - *column_start;
        if (current_column_has_escape_) parse_data_[*num_fields].len *= -1;
        if (!boundary_column_.empty()) {
          CopyBoundaryField(&parse_data_[*num_fields]);
        }
        ++(*num_fields);
      }
      boundary_column_.clear();
      current_column_has_escape_ = false;
      *column_start = file_buffer_ptr_ + 1;
      ++column_idx_;
    }

    if (new_tuple) {
      column_idx_ = 0;
      ++(*num_tuples);
    }

    --remaining_len;
    ++file_buffer_ptr_;

    if (*num_tuples == max_tuples) return Status::OK;
  }
  
  return Status::OK;
}

// TODO: This function can be *WAY* simpler
Status HdfsTextScanNode::WriteTuples(RuntimeState* state, RowBatch* row_batch, int num_tuples,
    int* row_idx, char** line_start) {
  *line_start = file_buffer_ptr_;
  boundary_row_.clear();
  
  for (int n = 0; n < num_tuples; ++n) {
    // Materialize partition-key values (if any).
    for (int i = 0; i < key_idx_to_slot_idx_.size(); ++i) {
      int slot_idx = key_idx_to_slot_idx_[i].second;
      Expr* expr = partition_key_values_[i];
      SlotDescriptor* slot = tuple_desc_->slots()[slot_idx];
      RawValue::Write(expr->GetValue(NULL), tuple_, slot, tuple_pool_.get());
    }

    // Add a new row
    if (*row_idx == RowBatch::INVALID_ROW_INDEX) {
      *row_idx = row_batch->AddRow();
    }
    TupleRow* current_row = row_batch->GetRow(*row_idx);
    current_row->SetTuple(tuple_idx_, tuple_);
    
    // This eval is only for the partition-key values.
    // TODO: this should be unnecessary - we can evaluate these once when the file
    // is opened and skip the entire file if these don't match
    if (EvalConjuncts(current_row)) {
      row_batch->CommitLastRow();
      *row_idx = RowBatch::INVALID_ROW_INDEX;
      ++num_rows_returned_;
      if (ReachedLimit()) return Status::OK;
      char* new_tuple = reinterpret_cast<char*>(tuple_);
      new_tuple += tuple_desc_->byte_size();
      tuple_ = reinterpret_cast<Tuple*>(new_tuple);
    } 
  }
  return Status::OK;
}

Status HdfsTextScanNode::WriteFields(RuntimeState* state, RowBatch* row_batch, int num_fields, 
    int* row_idx, char** line_start) {

  // Keep track of where lines begin as we write out fields for error reporting
  int next_line_offset = 0;

  // Loop through all the parsed_data and parse out the values to slots
  for (int n = 0; n < num_fields; ++n) {
    boundary_row_.clear();
    SlotDescriptor* slot_desc = materialized_slots_[slot_idx_];
    char* data = parse_data_[n].start;
    int len = parse_data_[n].len;
    bool need_escape = false;
    if (len < 0) {
      len = -len;
      need_escape = true;
    }
    next_line_offset += (len + 1);
    if (len == 0) {
      tuple_->SetNull(slot_desc->null_indicator_offset());
    } else {
      bool parse_error = false;
      void* slot = tuple_->GetSlot(slot_desc->tuple_offset());
      
      // Parse the raw-text data.  At this point:
      switch (slot_desc->type()) {
        case TYPE_STRING: 
          reinterpret_cast<StringValue*>(slot)->ptr = data;
          reinterpret_cast<StringValue*>(slot)->len = len;
          if (need_escape) {
            text_converter_->UnescapeString(reinterpret_cast<StringValue*>(slot));
          }
          break;
        case TYPE_BOOLEAN:
          *reinterpret_cast<bool*>(slot) = StringToBool(data, len, &parse_error);
          break;
        case TYPE_TINYINT: 
          *reinterpret_cast<int8_t*>(slot) = StringToInt<int8_t>(data, len, &parse_error);
          break;
        case TYPE_SMALLINT: 
          *reinterpret_cast<int16_t*>(slot) = StringToInt<int16_t>(data, len, &parse_error);
          break;
        case TYPE_INT: 
          *reinterpret_cast<int32_t*>(slot) = StringToInt<int32_t>(data, len, &parse_error);
          break;
        case TYPE_BIGINT: 
          *reinterpret_cast<int64_t*>(slot) = StringToInt<int64_t>(data, len, &parse_error);
          break;
        case TYPE_FLOAT: 
          *reinterpret_cast<float*>(slot) = StringToFloat<float>(data, len, &parse_error);
          break;
        case TYPE_DOUBLE:
          *reinterpret_cast<double*>(slot) = StringToFloat<double>(data, len, &parse_error);
          break;
        default:
          DCHECK(false) << "bad slot type: " << TypeToString(slot_desc->type());
          break;
      }
      if (parse_error) {
        error_in_row_ = true;
        tuple_->SetNull(slot_desc->null_indicator_offset());
        if (state->LogHasSpace()) {
          state->error_stream() << "Error converting column: " 
            << slot_desc->col_pos() - num_partition_keys_ << " TO "
            << TypeToString(slot_desc->type()) << endl;
        }
      }
    }

    // If slot_idx_ equals the number of materialized slots, we have completed 
    // parsing the tuple.  At this point we can:
    //  - Report errors if there are any
    //  - Reset line_start, materialized_slot_idx to be ready to parse the next row
    //  - Matreialize partition key values
    //  - Evaluate conjuncts and add if necessary
    if (++slot_idx_ == num_materialized_slots_) {
      if (error_in_row_) {
        ReportRowParseError(state, *line_start, next_line_offset - 1);
        error_in_row_ = false;
        if (state->abort_on_error()) {
          state->ReportFileErrors(files_[file_idx_], 1);
          return Status("Aborted HdfsScanNode due to parse errors.  View error log for details.");
        }
      }

      // Reset for next tuple
      *line_start += next_line_offset;
      next_line_offset = 0;
      slot_idx_ = 0;
      
      // Materialize partition-key values (if any).
      // TODO: populate a template tuple with only the partition keys 
      // when we switch partitions, then create a copy of that when we create result tuples.
      for (int i = 0; i < key_idx_to_slot_idx_.size(); ++i) {
        int slot_idx = key_idx_to_slot_idx_[i].second;
        Expr* expr = partition_key_values_[i];
        SlotDescriptor* slot = tuple_desc_->slots()[slot_idx];
        RawValue::Write(expr->GetValue(NULL), tuple_, slot, tuple_pool_.get());
      }

      // We now have a complete row, with everything materialized
      DCHECK(!row_batch->IsFull());
      if (*row_idx == RowBatch::INVALID_ROW_INDEX) {
        *row_idx = row_batch->AddRow();
      }
      TupleRow* current_row = row_batch->GetRow(*row_idx);
      current_row->SetTuple(tuple_idx_, tuple_);
      
      // Evaluate the conjuncts and add the row to the batch
      if (EvalConjuncts(current_row)) {
        row_batch->CommitLastRow();
        *row_idx = RowBatch::INVALID_ROW_INDEX;
        ++num_rows_returned_;
        if (ReachedLimit()) return Status::OK;
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_desc_->byte_size();
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
      } else {
        // Clear out any null-indicator bits that might have been set
        tuple_->Init(tuple_desc_->byte_size());
      } 
    }
  }
  return Status::OK;
}

void HdfsTextScanNode::CopyBoundaryField(ParseData* data) {
  const int total_len = data->len + boundary_column_.size();
  char* str_data = reinterpret_cast<char*>(tuple_pool_->Allocate(total_len));
  memcpy(str_data, boundary_column_.c_str(), boundary_column_.size());
  memcpy(str_data + boundary_column_.size(), data->start, data->len);
  data->start = str_data;
  data->len = total_len;
}

void HdfsTextScanNode::DebugString(int indentation_level, std::stringstream* out) const {
  *out << string(indentation_level * 2, ' ')
       << "HdfsTextScanNode(tupleid=" << tuple_id_ << " files[" << join(files_, ",")
       << " regex=" << partition_key_regex_ << " ";
  ExecNode::DebugString(indentation_level, out);
  *out << "])" << endl;
}

void HdfsTextScanNode::ReportRowParseError(RuntimeState* state, char* line_start, int len) {
  ++num_errors_in_file_;
  if (state->LogHasSpace()) {
    state->error_stream() << "file: " << files_[file_idx_] << endl;
    state->error_stream() << "line: ";
    if (!boundary_row_.empty()) {
      // Log the beginning of the line from the previous file buffer(s)
      state->error_stream() << boundary_row_;
    }
    // Log the erroneous line (or the suffix of a line if !boundary_line.empty()).
    state->error_stream() << string(line_start, len);
    state->LogErrorStream();
  }
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

