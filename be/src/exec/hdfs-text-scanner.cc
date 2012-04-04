// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/runtime-state.h"
#include "exec/hdfs-text-scanner.h"
#include "util/string-parser.h"
#include "runtime/tuple.h"
#include "runtime/row-batch.h"
#include "runtime/timestamp-value.h"
#include "exec/text-converter.h"
#include "util/cpu-info.h"
#include "exec/hdfs-scan-node.h"

using namespace impala;
using namespace std;

HdfsTextScanner::HdfsTextScanner(HdfsScanNode* scan_node,
    const TupleDescriptor* tuple_desc, Tuple* template_tuple, MemPool* tuple_pool)
    : HdfsScanner(scan_node, tuple_desc, template_tuple, tuple_pool),
      boundary_mem_pool_(new MemPool()),
      boundary_row_(boundary_mem_pool_.get()),
      boundary_column_(boundary_mem_pool_.get()),
      column_idx_(0),
      slot_idx_(0),
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

  tuple_delim_ = hdfs_table->line_delim();
  field_delim_ = hdfs_table->field_delim();
  collection_item_delim_ = hdfs_table->collection_delim();
  escape_char_ = hdfs_table->escape_char();
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
  column_idx_ = scan_node_->GetNumPartitionKeys();
  slot_idx_ = 0;

  // Pre-load byte buffer with size of entire range, if possible
  RETURN_IF_ERROR(current_byte_stream_->Seek(scan_range->offset));
  byte_buffer_ptr_ = byte_buffer_end_;
  RETURN_IF_ERROR(FillByteBuffer(state, current_range_remaining_len_));

  boundary_column_.Clear();
  boundary_row_.Clear();

  // Offset may not point to tuple boundary
  if (scan_range->offset != 0) {
    int first_tuple_offset = FindFirstTupleStart(byte_buffer_, byte_buffer_read_size_);
    DCHECK_LE(first_tuple_offset, min(state->file_buffer_size(),
                                      current_range_remaining_len_));
    byte_buffer_ptr_ += first_tuple_offset;
    current_range_remaining_len_ -= first_tuple_offset;
  }

  last_char_is_escape_ = false;
  current_column_has_escape_ = false;
  return Status::OK;
}

Status HdfsTextScanner::FillByteBuffer(RuntimeState* state, int64_t size) {
  DCHECK_GE(size, 0);
  DCHECK(current_byte_stream_ != NULL);
  // TODO: Promote state->file_buffer_size to int64_t
  int64_t file_buffer_size = state->file_buffer_size();
  int read_size = min(file_buffer_size, size);

  if (!reuse_byte_buffer_) {
    byte_buffer_ = byte_buffer_pool_->Allocate(state->file_buffer_size());
    reuse_byte_buffer_ = true;
  }
  {
    COUNTER_SCOPED_TIMER(scan_node_->hdfs_time_counter());
    RETURN_IF_ERROR(current_byte_stream_->Read(byte_buffer_, read_size,
                                               &byte_buffer_read_size_));
  }
  byte_buffer_end_ = byte_buffer_ + byte_buffer_read_size_;
  byte_buffer_ptr_ = byte_buffer_;
  COUNTER_UPDATE(scan_node_->bytes_read_counter(), byte_buffer_read_size_);
  return Status::OK;
}

Status HdfsTextScanner::Prepare(RuntimeState* state, ByteStream* byte_stream) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(state, byte_stream));

  current_range_remaining_len_ = 0;

  text_converter_.reset(new TextConverter(escape_char_, tuple_pool_));

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  field_locations_.resize(state->batch_size() * scan_node_->materialized_slots().size());

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

// Find the start of the first full tuple in buffer by looking for the end of
// the previous tuple.
// TODO: most of this is not tested.  We need some tailored data to exercise the boundary
// cases
int HdfsTextScanner::FindFirstTupleStart(char* buffer, int len) {
  int tuple_start = 0;
  char* buffer_start = buffer;
  while (tuple_start < len) {
    if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2)) {
      __m128i xmm_buffer, xmm_tuple_mask;
      while (len - tuple_start >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        // TODO: can we parallelize this as well?  Are there multiple sse execution units?
        // Load the next 16 bytes into the xmm register and do strchr for the
        // tuple delimiter.
        xmm_buffer = _mm_loadu_si128(reinterpret_cast<__m128i*>(buffer));
        xmm_tuple_mask = _mm_cmpistrm(xmm_tuple_search_, xmm_buffer, SSEUtil::STRCHR_MODE);
        int tuple_mask = _mm_extract_epi16(xmm_tuple_mask, 0);
        if (tuple_mask != 0) {
          for (int i = 0; i < SSEUtil::CHARS_PER_128_BIT_REGISTER; ++i) {
            if ((tuple_mask & SSEUtil::SSE_BITMASK[i]) != 0) {
              tuple_start += i + 1;
              buffer += i + 1;
              goto end;
            }
          }
        }
        tuple_start += SSEUtil::CHARS_PER_128_BIT_REGISTER;
        buffer += SSEUtil::CHARS_PER_128_BIT_REGISTER;
      }
    } else {
      for (int i = tuple_start; i < len; ++i) {
        char c = *buffer++;
        if (c == tuple_delim_) {
          tuple_start = i + 1;
          goto end;
        }
      }
    }

  end:
    if (escape_char_ != '\0') {
      // Scan backwards for escape characters.  We do this after
      // finding the tuple break rather than during the (above)
      // forward scan to make the forward scan faster.  This will
      // perform worse if there are many characters right before the
      // tuple break that are all escape characters, but that is
      // unlikely.
      int num_escape_chars = 0;
      int before_tuple_end = tuple_start - 2;
      for (; before_tuple_end >= 0; --before_tuple_end) {
        if (buffer_start[before_tuple_end] == escape_char_) {
          ++num_escape_chars;
        } else {
          break;
        }
      }
      // TODO: This sucks.  All the preceding characters before the tuple delim were
      // escape characters.  We need to read from the previous block to see what to do.
      DCHECK_GT(before_tuple_end, 0);

      // An even number of escape characters means they cancel out and this tuple break
      // is *not* escaped.
      if (num_escape_chars % 2 == 0) {
        return tuple_start;
      }
    } else {
      return tuple_start;
    }
  }
  return tuple_start;
}

Status HdfsTextScanner::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;
  int first_materialised_col_idx = scan_node_->GetNumPartitionKeys();

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
          column_idx_ = first_materialised_col_idx;
          boundary_column_.Clear();
          continue;
        }
      } else {
        if (current_range_remaining_len_ == 0) {
          // Check if a tuple is straddling this block and the next:
          //  1. boundary_column_ is not empty
          //  2. column_idx_ != first_materialised_col_idx if we are halfway through
          //  reading a tuple
          // We need to continue scanning until the end of the tuple.  Note that
          // boundary_column_ will be empty if we are on a column boundary, but could still
          // be inside a tuple. Similarly column_idx_ could be first_materialised_col_idx
          // if we are in the middle of reading the first column. Therefore we need both
          // checks.
          // We cannot use slot_idx, since that is incremented only if we are
          // materialising slots, which is not true for e.g. count(*)
          // TODO: test that hits this condition.
          if (!boundary_column_.Empty() || column_idx_ != first_materialised_col_idx) {
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
    char* col_start = NULL;
    char* line_start = byte_buffer_ptr_;
    int num_tuples = 0;
    int num_fields = 0;
    int max_tuples = row_batch->capacity() - row_batch->num_rows();
    // We are done with the current scan range, just parse for one more tuple to finish
    // it off
    if (current_range_remaining_len_ < 0) {
      max_tuples = 1;
    }
    RETURN_IF_ERROR(ParseFileBuffer(max_tuples, &num_tuples, &num_fields, &col_start));

    int bytes_processed = byte_buffer_ptr_ - line_start;
    current_range_remaining_len_ -= bytes_processed;

    DCHECK_GT(bytes_processed, 0);

    COUNTER_UPDATE(scan_node_->rows_read_counter(), num_tuples);
    if (num_fields != 0) {
      RETURN_IF_ERROR(WriteFields(state, row_batch, num_fields, &row_idx, &line_start));
    } else if (num_tuples != 0) {
      boundary_row_.Clear();
      line_start = byte_buffer_ptr_;
      RETURN_IF_ERROR(WriteTuples(state, row_batch, num_tuples, &row_idx));
    }

    // Cannot reuse file buffer if there are non-copied string slots materialized
    // TODO: If the tuple data contains very sparse string slots, we waste a lot of memory.
    // Instead, we should consider copying the tuples to a compact new buffer in this
    // case.
    if (num_rows_returned_ > previous_num_rows && has_string_slots_) {
      reuse_byte_buffer_ = false;
    }

    if (scan_node_->ReachedLimit()) {
      break;
    }

    // Save contents that are split across files
    if (col_start != byte_buffer_ptr_) {
      boundary_column_.Append(col_start, byte_buffer_ptr_ - col_start);
      boundary_row_.Append(line_start, byte_buffer_ptr_ - line_start);
    }

    if (current_range_remaining_len_ < 0) {
      DCHECK_LE(num_tuples, 1);
      // Just finished off this scan range
      if (num_tuples == 1) {
        DCHECK(boundary_column_.Empty());
        DCHECK(slot_idx_ == 0);
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
      break;
    }
  }

  DCHECK_EQ(column_idx_, first_materialised_col_idx);

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
  COUNTER_SCOPED_TIMER(scan_node_->tuple_write_time_counter());
  DCHECK_GT(num_fields, 0);

  // Keep track of where lines begin as we write out fields for error reporting
  int next_line_offset = 0;

  // Initialize tuple_ from the partition key template tuple before writing the slots
  if (template_tuple_ != NULL) {
    memcpy(tuple_, template_tuple_, tuple_byte_size_);
  }

  // Loop through all the parsed_data and parse out the values to slots
  for (int n = 0; n < num_fields; ++n) {

    WriteSlots(state, n, &next_line_offset);

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


Status HdfsTextScanner::WriteSlots(RuntimeState* state, int tuple_idx,
    int* next_line_offset) {
  boundary_row_.Clear();
  SlotDescriptor* slot_desc = scan_node_->materialized_slots()[slot_idx_].second;
  char* data = field_locations_[tuple_idx].start;
  int len = field_locations_[tuple_idx].len;
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
      *reinterpret_cast<bool*>(slot) =
        StringParser::StringToBool(data, len, &parse_result);
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
    case TYPE_TIMESTAMP: {
      string strbuf(data, len);
      *reinterpret_cast<TimestampValue*>(slot) = TimestampValue(strbuf);
      break;
    }
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
                              << slot_desc->col_pos() << " TO "
                              // TODO: num_partition_keys_ no longer visible to scanner.
                              // << slot_desc->col_pos() - num_partition_keys_ << " TO "
                              << TypeToString(slot_desc->type()) << endl;
      }
    }
  }

  return Status::OK;
}

void HdfsTextScanner::CopyBoundaryField(FieldLocations* data) {
  const int total_len = data->len + boundary_column_.Size();
  char* str_data = reinterpret_cast<char*>(tuple_pool_->Allocate(total_len));
  memcpy(str_data, boundary_column_.str().ptr, boundary_column_.Size());
  memcpy(str_data + boundary_column_.Size(), data->start, data->len);
  data->start = str_data;
  data->len = total_len;
}

// Updates the values in the field and tuple masks, escaping them if necessary.
// If the character at n is an escape character, then delimiters(tuple/field/escape
// characters) at n+1 don't count.
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

// SSE optimized raw text file parsing.  SSE4_2 added an instruction (with 3 modes) for
// text processing.  The modes mimic strchr, strstr and strcmp.  For text parsing, we can
// leverage the strchr functionality.
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
Status HdfsTextScanner::ParseFileBuffer(int max_tuples, int* num_tuples, int* num_fields,
    char** column_start) {
  COUNTER_SCOPED_TIMER(scan_node_->parse_time_counter());

  // Start of this batch.
  *column_start = byte_buffer_ptr_;

  // To parse using SSE, we:
  //  1. Load into different sse registers the different characters we need to search for
  //      - tuple breaks, field breaks, escape characters
  //  2. Load 16 characters at a time into the sse register
  //  3. Use the SSE instruction to do strchr on those 16 chars, the result is a bitmask
  //  4. Compute the bitmask for tuple breaks, field breaks and escape characters.
  //  5. If there are escape characters, fix up the matching masked bits in the field/tuple mask
  //  6. Go through the mask bit by bit and write the parsed data.

  // xmm registers:
  //  - xmm_buffer: the register holding the current (16 chars) we're working on from the
  //  - file
  //  - xmm_tuple_search_: the tuple search register.  Only contains the tuple_delim char.
  //  - xmm_field_search_: the field search register.  Contains field delim and
  //        collection_item delim_char
  //  - xmm_escape_search_: the escape search register. Only contains escape char
  //  - xmm_tuple_mask: the result of doing strchr for the tuple delim
  //  - xmm_field_mask: the result of doing strchr for the field delim
  //  - xmm_escape_mask: the result of doing strchr for the escape char
  __m128i xmm_buffer, xmm_tuple_mask, xmm_field_mask, xmm_escape_mask;

  // Length remaining of buffer to process
  int remaining_len = byte_buffer_end_ - byte_buffer_ptr_;

  const vector<int>& column_idx_to_slot_idx_ = scan_node_->column_to_slot_index();

  if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2)) {
    while (remaining_len >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
      // Load the next 16 bytes into the xmm register
      xmm_buffer = _mm_loadu_si128(reinterpret_cast<__m128i*>(byte_buffer_ptr_));

      // Do the strchr for tuple and field breaks
      // TODO: can we parallelize this as well?  Are there multiple sse execution units?
      xmm_tuple_mask = _mm_cmpistrm(xmm_tuple_search_, xmm_buffer, SSEUtil::STRCHR_MODE);
      xmm_field_mask = _mm_cmpistrm(xmm_field_search_, xmm_buffer, SSEUtil::STRCHR_MODE);

      // The strchr sse instruction returns the result in the lower bits of the sse
      // register.  Since we only process 16 characters at a time, only the lower 16 bits
      // can contain non-zero values.
      // _mm_extract_epi16 will extract 16 bits out of the xmm register.  The second
      // parameter specifies which 16 bits to extract (0 for the lowest 16 bits).
      int tuple_mask = _mm_extract_epi16(xmm_tuple_mask, 0);
      int field_mask = _mm_extract_epi16(xmm_field_mask, 0);
      int escape_mask = 0;

      // If the table does not use escape characters, skip processing for it.
      if (escape_char_ != '\0') {
        xmm_escape_mask = _mm_cmpistrm(xmm_escape_search_, xmm_buffer,
                                       SSEUtil::STRCHR_MODE);
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
            char* column_end = byte_buffer_ptr_ + n;
            // TODO: apparently there can be columns not in the schema which should be
            // ignored.  This does not handle that.
            if (column_idx_to_slot_idx_[column_idx_] != HdfsScanNode::SKIP_COLUMN) {
              DCHECK_LT(*num_fields, field_locations_.size());
              // Found a column that needs to be parsed, write the start/len to
              // 'parsed_data_'
              const int len = column_end - *column_start;
              field_locations_[*num_fields].start = *column_start;
              if (!current_column_has_escape_) {
                field_locations_[*num_fields].len = len;
              } else {
                field_locations_[*num_fields].len = -len;
              }
              if (!boundary_column_.Empty()) {
                CopyBoundaryField(&field_locations_[*num_fields]);
              }
              ++(*num_fields);
            }
            current_column_has_escape_ = false;
            boundary_column_.Clear();
            *column_start = column_end + 1;
            ++column_idx_;
          }

          if (tuple_mask & SSEUtil::SSE_BITMASK[n]) {
            column_idx_ = scan_node_->GetNumPartitionKeys();
            ++(*num_tuples);
            if (*num_tuples == max_tuples) {
              byte_buffer_ptr_ += (n + 1);
              last_char_is_escape_ = false;
              return Status::OK;
            }
          }
        }
      } else {
        current_column_has_escape_ = (current_column_has_escape_ || escape_mask);
      }

      remaining_len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      byte_buffer_ptr_ += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
  }

  // Handle the remaining characters
  while (remaining_len > 0) {
    bool new_tuple = false;
    bool new_col = false;

    if (!last_char_is_escape_) {
      if (*byte_buffer_ptr_ == tuple_delim_) {
        new_tuple = true;
        new_col = true;
      } else if (*byte_buffer_ptr_ == field_delim_
                 || *byte_buffer_ptr_ == collection_item_delim_) {
        new_col = true;
      }
    }
    if (*byte_buffer_ptr_ == escape_char_) {
      current_column_has_escape_ = true;
      last_char_is_escape_ = !last_char_is_escape_;
    } else {
      last_char_is_escape_ = false;
    }

    if (new_col) {
      if (column_idx_to_slot_idx_[column_idx_] != HdfsScanNode::SKIP_COLUMN) {
        DCHECK_LT(*num_fields, field_locations_.size());
        // Found a column that needs to be parsed, write the start/len to 'parsed_data_'
        field_locations_[*num_fields].start = *column_start;
        field_locations_[*num_fields].len = byte_buffer_ptr_ - *column_start;
        if (current_column_has_escape_) field_locations_[*num_fields].len *= -1;
        if (!boundary_column_.Empty()) {
          CopyBoundaryField(&field_locations_[*num_fields]);
        }
        ++(*num_fields);
      }
      boundary_column_.Clear();
      current_column_has_escape_ = false;
      *column_start = byte_buffer_ptr_ + 1;
      ++column_idx_;
    }

    if (new_tuple) {
      column_idx_ = scan_node_->GetNumPartitionKeys();
      ++(*num_tuples);
    }

    --remaining_len;
    ++byte_buffer_ptr_;

    if (*num_tuples == max_tuples) return Status::OK;
  }

  return Status::OK;
}
