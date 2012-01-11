// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-text-scan-node.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

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
#include "util/sse-util.h"
#include "util/string-parser.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"

DEFINE_string(nn, "default", "");
DEFINE_int32(nn_port, 0, "");

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
      tuple_pool_(new MemPool()),
      file_buffer_pool_(new MemPool()),
      obj_pool_(new ObjectPool()),
      partition_key_pool_(new MemPool()),
      hdfs_connection_(NULL),
      hdfs_file_(NULL),
      file_buffer_read_size_(0),
      file_idx_(0),
      partition_key_template_tuple_(NULL),
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

// HDFS will set errno on error.  Append this to message for better diagnostic messages.
static string AppendHdfsErrorMessage(const string& message) {
  std::stringstream ss;
  ss << message << " Error(" << errno << "): " << strerror(errno);
  return ss.str();
}

Status HdfsTextScanNode::Prepare(RuntimeState* state) {
  PrepareConjuncts(state);
  // Initialize ptr to end, to initiate reading the first file block
  file_buffer_ptr_ = file_buffer_end_;

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
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
  text_converter_.reset(new TextConverter(escape_char_, tuple_pool_.get()));

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
  hdfs_connection_ = hdfsConnect(FLAGS_nn.c_str(), FLAGS_nn_port);
  if (hdfs_connection_ == NULL) {
    return Status(AppendHdfsErrorMessage("Failed to connect to HDFS."));
  } else {
    return Status::OK;
  }
}

Status HdfsTextScanNode::Close(RuntimeState* state) {
  int hdfs_ret = hdfsDisconnect(hdfs_connection_);
  if (hdfs_ret == 0) {
    return Status::OK;
  } else {
    return Status(AppendHdfsErrorMessage("Failed to close HDFS connection."));
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

  while (file_idx_ < files_.size()) {
    // Did we finish reading the previous file? if so, open next file
    if (file_buffer_read_size_ == 0) {
      hdfs_file_ =
          hdfsOpenFile(hdfs_connection_, files_[file_idx_].c_str(), O_RDONLY, 0, 0, 0);
      if (hdfs_file_ == NULL) {
        return Status(AppendHdfsErrorMessage("Failed to open HDFS file " + files_[file_idx_]));
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
      // With two pass approach, we need to save some of the state before any of the file
      // was parsed
      char* col_start = NULL;
      char* line_start = file_buffer_ptr_;
      int num_tuples = 0;
      int num_fields = 0;
      int max_tuples = row_batch->capacity() - row_batch->num_rows();
      RETURN_IF_ERROR(ParseFileBuffer(max_tuples, &num_tuples, &num_fields, &col_start));
      if (num_fields != 0) {
        RETURN_IF_ERROR(WriteFields(state, row_batch, num_fields, &row_idx, &line_start));
      } else if (num_tuples != 0) {
        RETURN_IF_ERROR(WriteTuples(state, row_batch, num_tuples, &row_idx, &line_start));
      }
      // Save contents that are split across files
      if (col_start != file_buffer_ptr_) {
        boundary_column_.append(col_start, file_buffer_ptr_ - col_start);
        boundary_row_.append(line_start, file_buffer_ptr_ - line_start);
      }
      
      // TODO: does it ever make sense to deep copy some of the tuples (string data) if the
      // number of tuples in this batch is very small?
      // Cannot reuse file buffer if there are non-copied string slots materialized
      if (num_rows_returned_ > previous_num_rows &&
          !tuple_desc_->string_slots().empty()) {
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
      return Status(AppendHdfsErrorMessage("Failed to close HDFS file " + files_[file_idx_]));
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
// processing.  The modes mimic strchr, strstr and strcmp.  For text parsing, we can leverage
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
Status HdfsTextScanNode::ParseFileBuffer(int max_tuples, int* num_tuples, int* num_fields, 
    char** column_start) {

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
  //  - xmm_field_mask: the result of doing strchr for the field delim
  //  - xmm_escape_mask: the result of doing strchr for the escape char
  __m128i xmm_buffer, xmm_tuple_mask, xmm_field_mask, xmm_escape_mask;
  
  // Length remaining of buffer to process
  int remaining_len = file_buffer_end_ - file_buffer_ptr_;

  if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2)) {
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
      // _mm_extract_epi16 will extract 16 bits out of the xmm register.  The second parameter
      // specifies which 16 bits to extract (0 for the lowest 16 bits).
      int tuple_mask = _mm_extract_epi16(xmm_tuple_mask, 0);
      int field_mask = _mm_extract_epi16(xmm_field_mask, 0);
      int escape_mask = 0;

      // If the table does not use escape characters, skip processing for it.
      if (escape_char_ != '\0') {
        xmm_escape_mask = _mm_cmpistrm(xmm_escape_search_, xmm_buffer, SSEUtil::STRCHR_MODE);
        escape_mask = _mm_extract_epi16(xmm_escape_mask, 0);
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
  }

  // Handle the remaining characters
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

// In this code path, no slots were materialized from the input files.  The only
// slots are from partition keys.  This lets us simplify writing out the batches.
//   1. partition_key_template_tuple_ is the complete tuple.  
//   2. Eval conjuncts against the tuple.
//   3. If it passes, stamp out 'num_tuples' copies of it into the row_batch.
Status HdfsTextScanNode::WriteTuples(RuntimeState* state, RowBatch* row_batch, int num_tuples,
    int* row_idx, char** line_start) {
  DCHECK_GT(num_tuples, 0);

  *line_start = file_buffer_ptr_;
  boundary_row_.clear();
  
  // Make a row and evaluate the row
  if (*row_idx == RowBatch::INVALID_ROW_INDEX) {
    *row_idx = row_batch->AddRow();
  }
  TupleRow* current_row = row_batch->GetRow(*row_idx);
  current_row->SetTuple(tuple_idx_, partition_key_template_tuple_);
  if (!EvalConjuncts(current_row)) {
    return Status::OK;
  }
  // Add first tuple
  row_batch->CommitLastRow();
  *row_idx = RowBatch::INVALID_ROW_INDEX;
  ++num_rows_returned_;
  --num_tuples;

  // All the tuples in this batch are identical and can be stamped out 
  // from partition_key_template_tuple_.  Write out tuples up to the limit_. 
  if (limit_ != -1) {
    num_tuples = min(num_tuples, static_cast<int>(limit_ - num_rows_returned_));
  }
  DCHECK_LE(num_tuples, row_batch->capacity() - row_batch->num_rows());
  for (int n = 0; n < num_tuples; ++n) {
    DCHECK(!row_batch->IsFull());
    *row_idx = row_batch->AddRow();
    DCHECK(*row_idx != RowBatch::INVALID_ROW_INDEX);
    TupleRow* current_row = row_batch->GetRow(*row_idx);
    current_row->SetTuple(tuple_idx_, partition_key_template_tuple_);
    row_batch->CommitLastRow();
  }
  num_rows_returned_ += num_tuples;
  *row_idx = RowBatch::INVALID_ROW_INDEX;
  return Status::OK;
}

// TODO: apply conjuncts as slots get materialized and skip to the end of the row
// if we determine it's not a match.
Status HdfsTextScanNode::WriteFields(RuntimeState* state, RowBatch* row_batch, int num_fields, 
    int* row_idx, char** line_start) {
  DCHECK_GT(num_fields, 0);

  // Keep track of where lines begin as we write out fields for error reporting
  int next_line_offset = 0;

  // Initialize tuple_ from the partition key template tuple before writing the slots
  if (partition_key_template_tuple_ != NULL) {
    memcpy(tuple_, partition_key_template_tuple_, tuple_desc_->byte_size());
  }

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
      StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
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
          *reinterpret_cast<bool*>(slot) = StringParser::StringToBool(data, len, &parse_result);
          break;
        case TYPE_TINYINT: 
          *reinterpret_cast<int8_t*>(slot) = 
              StringParser::StringToInt<int8_t>(data, len, &parse_result);
          break;
        case TYPE_SMALLINT: 
          *reinterpret_cast<int16_t*>(slot) = 
              StringParser::StringToInt<int16_t>(data, len, &parse_result);
          break;
        case TYPE_INT: 
          *reinterpret_cast<int32_t*>(slot) = 
              StringParser::StringToInt<int32_t>(data, len, &parse_result);
          break;
        case TYPE_BIGINT: 
          *reinterpret_cast<int64_t*>(slot) = 
              StringParser::StringToInt<int64_t>(data, len, &parse_result);
          break;
        case TYPE_FLOAT: 
          *reinterpret_cast<float*>(slot) = 
              StringParser::StringToFloat<float>(data, len, &parse_result);
          break;
        case TYPE_DOUBLE:
          *reinterpret_cast<double*>(slot) = 
              StringParser::StringToFloat<double>(data, len, &parse_result);
          break;
        default:
          DCHECK(false) << "bad slot type: " << TypeToString(slot_desc->type());
          break;
      }
      
      // TODO: add warning for overflow case
      if (parse_result == StringParser::PARSE_FAILURE) {
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
    //  - Materialize partition key values
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
      
      // We now have a complete row, with everything materialized
      DCHECK(!row_batch->IsFull());
      if (*row_idx == RowBatch::INVALID_ROW_INDEX) {
        *row_idx = row_batch->AddRow();
      }
      TupleRow* current_row = row_batch->GetRow(*row_idx);
      current_row->SetTuple(tuple_idx_, tuple_);
      
      // Evaluate the conjuncts and add the row to the batch
      bool conjuncts_true = EvalConjuncts(current_row);
      if (conjuncts_true) {
        row_batch->CommitLastRow();
        *row_idx = RowBatch::INVALID_ROW_INDEX;
        ++num_rows_returned_;
        if (ReachedLimit() || row_batch->IsFull()) return Status::OK;
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_desc_->byte_size();
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
      }  
      
      // Need to reset the tuple_ if
      //  1. eval failed (clear out null-indicator bits) OR
      //  2. there are partition keys that need to be copied
      // TODO: if the slots that need to be updated are very sparse (very few NULL slots
      // or very few partition keys), updating all the tuple memory is probably bad
      if (!conjuncts_true || partition_key_template_tuple_ != NULL) {
        if (partition_key_template_tuple_ != NULL) {
          memcpy(tuple_, partition_key_template_tuple_, tuple_desc_->byte_size());
        } else {
          tuple_->Init(tuple_desc_->byte_size());
        }
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

  partition_key_template_tuple_ = 
      reinterpret_cast<Tuple*>(partition_key_pool_->Allocate(tuple_desc_->byte_size()));
  memset(partition_key_template_tuple_, 0, tuple_desc_->byte_size());

  smatch match;
  const string& file = files_[file_idx_];
  if (boost::regex_search(file, match, partition_key_regex_)) {
    for (int i = 0; i < key_idx_to_slot_idx_.size(); ++i) {
      int regex_idx = key_idx_to_slot_idx_[i].first + 1; //match[0] is input string
      int slot_idx = key_idx_to_slot_idx_[i].second;
      const SlotDescriptor* slot_desc = tuple_desc_->slots()[slot_idx];
      PrimitiveType type = slot_desc->type();
      const string& string_value = match[regex_idx]; 

      // Populate a template tuple with just the partition key values.  Separate handling of
      // the string and other types saves an allocation and copy.
      const void* value = NULL;
      ExprValue expr_value;
      if (type == TYPE_STRING) {
        expr_value.string_val.ptr = const_cast<char*>(string_value.c_str());
        expr_value.string_val.len = string_value.size();
        value = reinterpret_cast<void*>(&expr_value.string_val);
      } else {
        value = expr_value.TryParse(string_value, type);
      }
      if (value == NULL) {
        std::stringstream ss;
        ss << "file name'" << file << "' does not have the correct format. "
           << "Partition key: " << string_value << " is not of type: "
           << TypeToString(type);
        return Status(ss.str());
      }

      RawValue::Write(value, partition_key_template_tuple_, 
          slot_desc, partition_key_pool_.get());
    }
    return Status::OK;
  }
  
  std::stringstream ss;
  ss << "file name '" << file << "' " 
     << "does not match partition key regex (" << partition_key_regex_ << ")";
  return Status(ss.str());
}

