// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "codegen/llvm-codegen.h"
#include "exec/delimited-text-parser.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hdfs-text-scanner.h"
#include "exec/text-converter.h"
#include "exec/text-converter.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"

using namespace impala;
using namespace llvm;
using namespace std;

const char* HdfsTextScanner::LLVM_CLASS_NAME = "class.impala::HdfsTextScanner";

HdfsTextScanner::HdfsTextScanner(HdfsScanNode* scan_node, RuntimeState* state,
    Tuple* template_tuple, MemPool* tuple_pool)
    : HdfsScanner(scan_node, state, template_tuple, tuple_pool),
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
      error_in_row_(false),
      write_tuples_fn_(NULL),
      escape_char_('\0'){
}

HdfsTextScanner::~HdfsTextScanner() {
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      byte_buffer_pool_->peak_allocated_bytes());
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      boundary_mem_pool_->peak_allocated_bytes());
}

Status HdfsTextScanner::InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition, 
    HdfsScanRange* scan_range, Tuple* template_tuple, ByteStream* byte_stream) {
  RETURN_IF_ERROR(
      HdfsScanner::InitCurrentScanRange(hdfs_partition, scan_range, template_tuple,
      byte_stream));

  // Cache escape char to detect whether or not to use the generated tuple writer for this
  // scan range
  escape_char_ = hdfs_partition->escape_char();

  text_converter_.reset(new TextConverter(escape_char_, tuple_pool_));

  char field_delim = hdfs_partition->field_delim();
  char collection_delim = hdfs_partition->collection_delim();
  if (scan_node_->materialized_slots().size() == 0) {
    field_delim = '\0';
    collection_delim = '\0';
  }
  delimited_text_parser_.reset(new DelimitedTextParser(scan_node_,
      hdfs_partition->line_delim(), field_delim, collection_delim,
      hdfs_partition->escape_char()));

  current_range_remaining_len_ = scan_range->length_;
  error_in_row_ = false;

  // Note - this initialisation relies on the assumption that N partition keys will occupy
  // entries 0 through N-1 in column_idx_to_slot_idx. If this changes, we will need
  // another layer of indirection to map text-file column indexes onto the
  // column_idx_to_slot_idx table used below.
  slot_idx_ = 0;

  // Pre-load byte buffer with size of entire range, if possible
  RETURN_IF_ERROR(current_byte_stream_->Seek(scan_range->offset_));
  byte_buffer_ptr_ = byte_buffer_end_;
  RETURN_IF_ERROR(FillByteBuffer(current_range_remaining_len_));

  boundary_column_.Clear();
  boundary_row_.Clear();
  delimited_text_parser_->ParserReset();

  // Offset may not point to tuple boundary
  if (scan_range->offset_ != 0) {
    COUNTER_SCOPED_TIMER(parse_delimiter_timer_);
    int first_tuple_offset = delimited_text_parser_->FindFirstInstance(
        byte_buffer_, byte_buffer_read_size_);
    DCHECK_LE(first_tuple_offset, min(state_->file_buffer_size(),
         current_range_remaining_len_));
    byte_buffer_ptr_ += first_tuple_offset;
    current_range_remaining_len_ -= first_tuple_offset;
  }
  return Status::OK;
}

Status HdfsTextScanner::FillByteBuffer(int64_t size) {
  DCHECK_GE(size, 0);
  DCHECK(current_byte_stream_ != NULL);
  // TODO: Promote state->file_buffer_size to int64_t
  int64_t file_buffer_size = state_->file_buffer_size();
  int read_size = min(file_buffer_size, size);

  if (!reuse_byte_buffer_) {
    byte_buffer_ =
        reinterpret_cast<char*>(byte_buffer_pool_->Allocate(state_->file_buffer_size()));
    reuse_byte_buffer_ = true;
  }
  RETURN_IF_ERROR(current_byte_stream_->Read(reinterpret_cast<uint8_t*>(byte_buffer_),
      read_size, &byte_buffer_read_size_));
  byte_buffer_end_ = byte_buffer_ + byte_buffer_read_size_;
  byte_buffer_ptr_ = byte_buffer_;
  return Status::OK;
}

Status HdfsTextScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());
  
  parse_delimiter_timer_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "DelimiterParseTime", TCounterType::CPU_TICKS);

  current_range_remaining_len_ = 0;

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  field_locations_.resize(state_->batch_size() * scan_node_->materialized_slots().size());

  // Text converter will, in the future, rely on the current escape char. For now, we only
  // codegen for unescaped data, so it's ok to prepare the codegen once. Eventually, the
  // codegen will need to be cached per partition or per delimiter combination
  text_converter_.reset(new TextConverter('\0', tuple_pool_));

  // Codegen for materialized parsed data into tuples.  The WriteCompleteTuple is
  // codegen'd using the IRBuilder for the specific tuple description.  This function
  // is then injected into the cross-compiled driving function, WriteAlignedTuples().
  LlvmCodeGen* codegen = state_->llvm_codegen();
  if (codegen != NULL) {
    Function* write_complete_tuple_fn = CodegenWriteCompleteTuple(codegen);
    if (write_complete_tuple_fn != NULL) {
      Function* write_tuples_fn = CodegenWriteAlignedTuples(codegen,
          write_complete_tuple_fn);
      if (write_tuples_fn != NULL) {
        write_tuples_fn_ = reinterpret_cast<WriteTuplesFn>(
            codegen->JitFunction(write_tuples_fn));
        LOG(INFO) << "HdfsTextScanner(node_id=" << scan_node_->id()
                  << ") using llvm codegend functions.";
      }
    }
  }

  return Status::OK;
}

Status HdfsTextScanner::GetNext(RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  char* col_start = NULL;

  // The only case where this will be false if the row batch fills up.
  *eosr = true;

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
        RETURN_IF_ERROR(FillByteBuffer(NEXT_BLOCK_READ_SIZE));
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
          break;
        } else {
          // Continue reading from the current file location
          DCHECK_GE(current_range_remaining_len_, 0);

          int read_size = min(state_->file_buffer_size(), current_range_remaining_len_);
          RETURN_IF_ERROR(FillByteBuffer(read_size));
        }
      }
    }

    // Parses (or resumes parsing) the current file buffer, appending tuples into
    // the tuple buffer until:
    // a) file buffer has been completely parsed or
    // b) tuple buffer is full
    // c) a parsing error was encountered and the user requested to abort on errors.
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
      COUNTER_SCOPED_TIMER(parse_delimiter_timer_);
      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(max_tuples,
          byte_buffer_end_ - byte_buffer_ptr_, &byte_buffer_ptr_,
          &field_locations_, &num_tuples, &num_fields, &col_start));
    }
    int bytes_processed = byte_buffer_ptr_ - line_start;
    current_range_remaining_len_ -= bytes_processed;

    DCHECK_GT(bytes_processed, 0);

    int num_tuples_materialized = 0;
    if (scan_node_->materialized_slots().size() != 0) {
      if (num_fields != 0) {
        // There can be one partial tuple which returned no more fields from this buffer.
        DCHECK_LE(num_tuples, num_fields + 1);
        if (!boundary_column_.Empty()) {
          CopyBoundaryField(&field_locations_[0]);
          boundary_column_.Clear();
        }
        num_tuples_materialized = WriteFields(row_batch, num_fields, &line_start);
      } else if (col_start != previous_buffer_ptr) {
        // If we saw any delimiters col_start will move, clear the boundry_row_.
        boundary_row_.Clear();
      }
    } else if (num_tuples != 0) {
      COUNTER_SCOPED_TIMER(scan_node_->materialize_tuple_timer());
      // If we are doing count(*) then we return tuples only containing partition keys
      boundary_row_.Clear();
      line_start = byte_buffer_ptr_;
      num_tuples_materialized = WriteEmptyTuples(row_batch, num_tuples);
    }

    RETURN_IF_ERROR(parse_status_);

    // Cannot reuse file buffer if there are non-copied string slots materialized
    // TODO: If the tuple data contains very sparse string slots, we waste a lot of
    // memory.  Instead, we should consider copying the tuples to a compact new buffer
    // in this case.
    if (num_tuples_materialized > 0 && has_noncompact_strings_) {
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
      break;
    }

    // The row batch is full. We'll pick up where we left off in the next
    // GetNext() call.
    if (row_batch->IsFull()) {
      *eosr = false;
      break;
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
  tuple_ = NULL;

  DCHECK(delimited_text_parser_->AtTupleStart());
  return Status::OK;
}

bool HdfsTextScanner::ReportTupleParseError(FieldLocation* fields, uint8_t* errors,
    char* line, int len) {
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    if (errors[i]) {
      const SlotDescriptor* desc = scan_node_->materialized_slots()[i];
      ReportColumnParseError(desc, fields[i].start, fields[i].len);
      errors[i] = false;
    }
  }
  parse_status_ = ReportRowParseError(line, len);
  return parse_status_.ok();
}

Status HdfsTextScanner::ReportRowParseError(char* line_start, int len) {
  ++num_errors_in_file_;
  if (state_->LogHasSpace()) {
    state_->error_stream() << "file: " << current_byte_stream_->GetLocation() << endl;
    state_->error_stream() << "line: ";
    if (!boundary_row_.Empty()) {
      // Log the beginning of the line from the previous file buffer(s)
      state_->error_stream() << boundary_row_.str();
    }
    // Log the erroneous line (or the suffix of a line if !boundary_line.empty()).
    state_->error_stream() << string(line_start, len);
    state_->LogErrorStream();
  }

  if (state_->abort_on_error()) {
    state_->ReportFileErrors(current_byte_stream_->GetLocation(), 1);
    return Status(
        "Aborted HdfsTextScanner due to parse errors. View error log for details.");
  }
  return Status::OK;
}

int HdfsTextScanner::WriteFields(RowBatch* row_batch, int num_fields, char** line_start) {
  COUNTER_SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  DCHECK_GT(num_fields, 0);

  FieldLocation* fields = &field_locations_[0];

  int num_tuples_materialized = 0;
  // Write remaining fields, if any, from the previous partial tuple.
  if (slot_idx_ != 0) {
    DCHECK(tuple_ != NULL);
    int num_partial_fields = scan_node_->materialized_slots().size() - slot_idx_;
    // Corner case where there will be no materialized tuples but at least one col
    // worth of string data.  In this case, make a deep copy and reuse the byte buffer.
    bool copy_strings = num_partial_fields > num_fields;
    num_partial_fields = min(num_partial_fields, num_fields);
    int next_line_offset = WritePartialTuple(fields, num_partial_fields, copy_strings);

    if (slot_idx_ == scan_node_->materialized_slots().size()) {
      if (UNLIKELY(error_in_row_)) {
        parse_status_ = ReportRowParseError(*line_start, next_line_offset - 1);
        if (!parse_status_.ok()) return -1;

        error_in_row_ = false;
      }
      boundary_row_.Clear();

      int row_idx = row_batch->AddRow();
      DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
      TupleRow* tuple_row = row_batch->GetRow(row_idx);
      tuple_row->SetTuple(tuple_idx_, tuple_);

      if (scan_node_->EvalConjunctsForScanner(tuple_row)) {
        row_batch->CommitLastRow();
        scan_node_->IncrNumRowsReturned();
        ++num_tuples_materialized;

        if (scan_node_->ReachedLimit() || row_batch->IsFull()) {
          return num_tuples_materialized;
        }

        uint8_t* new_tuple = reinterpret_cast<uint8_t*>(tuple_) + tuple_byte_size_;
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
      }

      *line_start += next_line_offset;
      slot_idx_ = 0;
    }

    num_fields -= num_partial_fields;
    fields += num_partial_fields;
  }

  // Write complete tuples.  The current field, if any, is at the start of a tuple.
  int num_tuples = num_fields / scan_node_->materialized_slots().size();
  if (num_tuples > 0) {
    int max_added_tuples = (scan_node_->limit() == -1) ?
          num_tuples : scan_node_->limit() - scan_node_->rows_returned();
    int tuples_returned = 0;
    // Call jitted function if possible
    if (write_tuples_fn_ != NULL && escape_char_ == '\0') {
      tuples_returned = write_tuples_fn_(this, row_batch, fields, num_tuples,
            max_added_tuples, scan_node_->materialized_slots().size(), line_start);
    } else {
      tuples_returned = WriteAlignedTuples(row_batch, fields, num_tuples,
            max_added_tuples, scan_node_->materialized_slots().size(), line_start);
    }
    if (tuples_returned == -1) return -1;

    scan_node_->IncrNumRowsReturned(tuples_returned);
    DCHECK_EQ(slot_idx_, 0);

    num_tuples_materialized += tuples_returned;
    fields += num_tuples * scan_node_->materialized_slots().size();
    num_fields = num_fields % scan_node_->materialized_slots().size();
  }


  // Write out the remaining slots (resulting in a partially materialized tuple)
  if (num_fields != 0) {
    DCHECK(tuple_ != NULL);
    InitTuple(tuple_);
    // If there have been no materialized tuples at this point, copy string data
    // out of byte_buffer and reuse the byte_buffer.  The copied data can be at
    // most one tuple's worth.
    WritePartialTuple(fields, num_fields, num_tuples_materialized == 0);
  }
  DCHECK_LT(slot_idx_, scan_node_->materialized_slots().size());
  return num_tuples_materialized;
}

void HdfsTextScanner::CopyBoundaryField(FieldLocation* data) {
  const int total_len = data->len + boundary_column_.Size();
  char* str_data = reinterpret_cast<char*>(tuple_pool_->Allocate(total_len));
  memcpy(str_data, boundary_column_.str().ptr, boundary_column_.Size());
  memcpy(str_data + boundary_column_.Size(), data->start, data->len);
  data->start = str_data;
  data->len = total_len;
}

int HdfsTextScanner::WritePartialTuple(FieldLocation* fields, int num_fields,
    bool copy_strings) {
  copy_strings |= scan_node_->compact_data();
  int next_line_offset = 0;
  for (int i = 0; i < num_fields; ++i) {
    int need_escape = false;
    int len = fields[i].len;
    if (len < 0) {
      len = -len;
      need_escape = true;
    }
    next_line_offset += (len + 1);

    const SlotDescriptor* desc = scan_node_->materialized_slots()[slot_idx_];
    if (!text_converter_->WriteSlot(desc, tuple_,
        fields[i].start, len, copy_strings, need_escape)) {
      ReportColumnParseError(desc, fields[i].start, len);
      error_in_row_ = true;
    }
    ++slot_idx_;
  }
  return next_line_offset;
}

void HdfsTextScanner::ComputeSlotMaterializationOrder(vector<int>* order) {
  order->resize(scan_node_->materialized_slots().size());
  const vector<Expr*>& conjuncts = scan_node_->conjuncts();
  // Initialize all order to be conjuncts.size() (after the last conjunct)
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    (*order)[i] = conjuncts.size();
  }

  const DescriptorTbl& desc_tbl = state_->desc_tbl();

  vector<SlotId> slot_ids;
  for (int conjunct_idx = 0; conjunct_idx < conjuncts.size(); ++conjunct_idx) {
    slot_ids.clear();
    int num_slots = conjuncts[conjunct_idx]->GetSlotIds(&slot_ids);
    for (int j = 0; j < num_slots; ++j) {
      SlotDescriptor* slot_desc = desc_tbl.GetSlotDescriptor(slot_ids[j]);
      int slot_idx = scan_node_->GetMaterializedSlotIdx(slot_desc->col_pos());
      // slot_idx == -1 means this was a partition key slot which is always
      // materialized before any slots.
      if (slot_idx == -1) continue;
      // If this slot hasn't been assigned an order, assign it be materialized
      // before evaluating conjuncts[i]
      if ((*order)[slot_idx] == conjuncts.size()) {
        (*order)[slot_idx] = conjunct_idx;
      }
    }
  }
}

bool HdfsTextScanner::WriteCompleteTuple(FieldLocation* fields, Tuple* tuple,
    TupleRow* tuple_row, uint8_t* error_fields, uint8_t* error_in_row, int* bytes) {
  int bytes_parsed = 0;

  // Initialize tuple before materializing slots
  InitTuple(tuple);

  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    int need_escape = false;
    int len = fields[i].len;
    if (UNLIKELY(len < 0)) {
      len = -len;
      need_escape = true;
    }
    *bytes += len + 1;

    SlotDescriptor* desc = scan_node_->materialized_slots()[i];
    bool error = !text_converter_->WriteSlot(desc, tuple,
        fields[i].start, len, scan_node_->compact_data(), need_escape);
    error_fields[i] = error;
    *error_in_row |= error;
  }

  *bytes = bytes_parsed;
  tuple_row->SetTuple(tuple_idx_, tuple);
  return scan_node_->EvalConjunctsForScanner(tuple_row);
}

// Codegen for WriteTuple(above).  The signature matches WriteTuple (except for the
// this* first argument).  For writing out and evaluating a single string slot:
// define i1 @WriteCompleteTuple(%"class.impala::HdfsTextScanner"* %this_ptr,
//                               %"struct.impala::FieldLocation"* %fields,
//                               %"class.impala::Tuple"* %tuple,
//                               %"class.impala::TupleRow"* %tuple_row, i8*
//                               %error_fields, i8* %error_in_row, i32* %row_len) {
// entry:
//   %tuple_ptr = bitcast %"class.impala::Tuple"* %tuple to
//                                       { i8, %"struct.impala::StringValue" }*
//   %null_byte = getelementptr inbounds
//                                    { i8, %"struct.impala::StringValue" }* %tuple_ptr,
//                                      i32 0, i32 0
//   store i8 0, i8* %null_byte
//   br label %parse
//
// parse:                                            ; preds = %entry
//   %data_ptr = getelementptr %"struct.impala::FieldLocation"* %fields, i32 0, i32 0
//   %len_ptr = getelementptr %"struct.impala::FieldLocation"* %fields, i32 0, i32 1
//   %slot_error_ptr = getelementptr i8* %error_fields, i32 0
//   %data = load i8** %data_ptr
//   %len = load i32* %len_ptr
//   %bytes_parsed = add i32 0, %len
//   %0 = call i1 @WriteSlot({ i8, %"struct.impala::StringValue" }* %tuple_ptr,
//                           i8* %data, i32 %len)
//   %slot_parse_error = xor i1 %0, true
//   %error_in_row1 = or i1 false, %slot_parse_error
//   %1 = zext i1 %slot_parse_error to i8
//   store i8 %1, i8* %slot_error_ptr
//   br label %parse_done
//
// parse_done:                                       ; preds = %parse
//   store i32 %bytes_parsed, i32* %row_len
//   %2 = zext i1 %error_in_row1 to i8
//   store i8 %2, i8* %error_in_row
//   %3 = bitcast %"class.impala::TupleRow"* %tuple_row to
//                                        { i8, %"struct.impala::StringValue" }**
//   %4 = getelementptr { i8, %"struct.impala::StringValue" }** %3, i32 0
//   store { i8, %"struct.impala::StringValue" }* %tuple_ptr,
//                                        { i8, %"struct.impala::StringValue" }** %4
//   %eval = call i1 @EvalConjuncts(%"class.impala::TupleRow"* %tuple_row)
//   ret i1 %eval
// }
Function* HdfsTextScanner::CodegenWriteCompleteTuple(LlvmCodeGen* codegen) {
  COUNTER_SCOPED_TIMER(codegen->codegen_timer());
  // TODO: Timestamp is not yet supported
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = scan_node_->materialized_slots()[i];
    if (slot_desc->type() == TYPE_TIMESTAMP) {
      LOG(WARNING) << "Could not codegen for HdfsTextScanner because timestamp "
                   << "slots are not supported.";
      return NULL;
    }
  }

  // TODO: can't codegen yet if strings need to be copied
  if (has_noncompact_strings_) return NULL;

  // Codegen for eval conjuncts
  const vector<Expr*>& conjuncts = scan_node_->conjuncts();
  for (int i = 0; i < conjuncts.size(); ++i) {
    if (conjuncts[i]->codegen_fn() == NULL) return NULL;
    // TODO: handle cases with scratch buffer.
    DCHECK_EQ(conjuncts[i]->scratch_buffer_size(), 0);
  }

  // Cast away const-ness.  The codegen only sets the cached typed llvm struct.
  TupleDescriptor* tuple_desc = const_cast<TupleDescriptor*>(scan_node_->tuple_desc());
  vector<Function*> slot_fns;
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = scan_node_->materialized_slots()[i];
    Function* fn = text_converter_->CodegenWriteSlot(codegen, tuple_desc, slot_desc);
    if (fn == NULL) return NULL;
    slot_fns.push_back(fn);
  }

  // Compute order to materialize slots.  BE assumes that conjuncts should
  // be evaluated in the order specified (optimization is already done by FE)
  vector<int> materialize_order;
  ComputeSlotMaterializationOrder(&materialize_order);

  // Get types to construct matching function signature to WriteCompleteTuple
  PointerType* uint8_ptr_type = PointerType::get(codegen->GetType(TYPE_TINYINT), 0);
  PointerType* int_ptr_type = PointerType::get(codegen->GetType(TYPE_INT), 0);

  StructType* field_loc_type = reinterpret_cast<StructType*>(
      codegen->GetType(FieldLocation::LLVM_CLASS_NAME));
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  Type* tuple_opaque_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  Type* hdfs_text_scanner_type = codegen->GetType(HdfsTextScanner::LLVM_CLASS_NAME);

  DCHECK(tuple_opaque_type != NULL);
  DCHECK(tuple_row_type != NULL);
  DCHECK(field_loc_type != NULL);
  DCHECK(hdfs_text_scanner_type != NULL);

  PointerType* field_loc_ptr_type = PointerType::get(field_loc_type, 0);
  PointerType* tuple_opaque_ptr_type = PointerType::get(tuple_opaque_type, 0);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);
  PointerType* hdfs_text_scanner_ptr_type = PointerType::get(hdfs_text_scanner_type, 0);

  // Generate the typed llvm struct for the output tuple
  StructType* tuple_type = tuple_desc->GenerateLlvmStruct(codegen);
  if (tuple_type == NULL) return NULL;
  PointerType* tuple_ptr_type = PointerType::get(tuple_type, 0);

  // Initialize the function prototype.  This needs to match
  // HdfsTextScanner::WriteCompleteTuple's signature identically.
  LlvmCodeGen::FnPrototype prototype(
      codegen, "WriteCompleteTuple", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", hdfs_text_scanner_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("fields", field_loc_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_opaque_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("error_fields", uint8_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("error_in_row", uint8_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row_len", int_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[7];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  BasicBlock* parse_block = BasicBlock::Create(context, "parse", fn);
  BasicBlock* eval_fail_block = BasicBlock::Create(context, "eval_fail", fn);

  // Extract the input args
  Value* fields_arg = args[1];
  Value* tuple_arg = builder.CreateBitCast(args[2], tuple_ptr_type, "tuple_ptr");
  Value* tuple_row_arg = builder.CreateBitCast(args[3],
      PointerType::get(codegen->ptr_type(), 0), "tuple_row_ptr");
  Value* errors_arg = args[4];
  Value* error_in_row_arg = args[5];
  Value* bytes_parsed_arg = args[6];

  // Codegen for function body
  Value* bytes_parsed = codegen->GetIntConstant(TYPE_INT, 0);
  Value* error_in_row = codegen->false_value();
  // Initialize tuple
  if (template_tuple_ == NULL) {
    // No partition key slots, just zero the NULL bytes.
    for (int i = 0; i < tuple_desc->num_null_bytes(); ++i) {
      Value* null_byte = builder.CreateStructGEP(tuple_arg, i, "null_byte");
      builder.CreateStore(codegen->GetIntConstant(TYPE_TINYINT, 0), null_byte);
    }
  } else {
    // Copy template tuple.
    // TODO: only copy what's necessary from the template tuple.
    Value* template_tuple_ptr =
        codegen->CastPtrToLlvmPtr(codegen->ptr_type(), template_tuple_);
    codegen->CodegenMemcpy(&builder, tuple_arg, template_tuple_ptr,
        codegen->GetIntConstant(TYPE_INT, tuple_byte_size_));
  }

  // Put tuple in tuple_row
  Value* tuple_row_typed =
      builder.CreateBitCast(tuple_row_arg, PointerType::get(tuple_ptr_type, 0));
  Value* tuple_row_idxs[] = { codegen->GetIntConstant(TYPE_INT, tuple_idx_) };
  Value* tuple_in_row_addr = builder.CreateGEP(tuple_row_typed, tuple_row_idxs);
  builder.CreateStore(tuple_arg, tuple_in_row_addr);
  builder.CreateBr(parse_block);

  // Loop through all the conjuncts in order and materialize slots as necessary
  // to evaluate the conjuncts (e.g. conjuncts[0] will have the slots it references
  // first).
  // materialized_order[slot_idx] represents the first conjunct which needs that slot.
  // Slots are only materialized if its order matches the current conjunct being
  // processed.  This guarantees that each slot is materialized once when it is first
  // needed and that at the end of the materialize loop, the conjunct has everything
  // it needs (either from this iteration or previous iterations).
  builder.SetInsertPoint(parse_block);
  LlvmCodeGen::NamedVariable null_var("null_ptr", codegen->boolean_type());
  Value* is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);
  for (int conjunct_idx = 0; conjunct_idx <= conjuncts.size(); ++conjunct_idx) {
    for (int slot_idx = 0; slot_idx < materialize_order.size(); ++slot_idx) {
      // If they don't match, it means either the slot has already been
      // materialized for a previous conjunct or will be materialized later for
      // another conjunct.  Either case, the slot does not need to be materialized
      // yet.
      if (materialize_order[slot_idx] != conjunct_idx) continue;

      // Materialize slots[slot_idx] to evaluate conjuncts[conjunct_idx]
      // All slots[i] with materialized_order[i] < conjunct_idx have already been
      // materialized by prior iterations through the outer loop

      // Extract ptr/len from fields
      Value* data_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
        codegen->GetIntConstant(TYPE_INT, 0),
      };
      Value* len_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
        codegen->GetIntConstant(TYPE_INT, 1),
      };
      Value* error_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
      };
      Value* data_ptr = builder.CreateGEP(fields_arg, data_idxs, "data_ptr");
      Value* len_ptr = builder.CreateGEP(fields_arg, len_idxs, "len_ptr");
      Value* error_ptr = builder.CreateGEP(errors_arg, error_idxs, "slot_error_ptr");
      Value* data = builder.CreateLoad(data_ptr, "data");
      Value* len = builder.CreateLoad(len_ptr, "len");

      // Accumulate row length
      bytes_parsed = builder.CreateAdd(bytes_parsed, len, "bytes_parsed");

      // Call slot parse function
      Function* slot_fn = slot_fns[slot_idx];
      Value* slot_parsed = builder.CreateCall3(slot_fn, tuple_arg, data, len);
      Value* slot_error = builder.CreateNot(slot_parsed, "slot_parse_error");
      error_in_row = builder.CreateOr(error_in_row, slot_error, "error_in_row");
      slot_error = builder.CreateZExt(slot_error, codegen->GetType(TYPE_TINYINT));
      builder.CreateStore(slot_error, error_ptr);
    }

    if (conjunct_idx == conjuncts.size()) {
      // In this branch, we've just materialized slots not referenced by any conjunct.
      // This slots are the last to get materialized.  If we are in this branch, the
      // tuple passed all conjuncts and should be added to the row batch.
      builder.CreateStore(bytes_parsed, bytes_parsed_arg);
      Value* error_ret = builder.CreateZExt(error_in_row, codegen->GetType(TYPE_TINYINT));
      builder.CreateStore(error_ret, error_in_row_arg);
      builder.CreateRet(codegen->true_value());
    } else {
      // All slots for conjuncts[conjunct_idx] are materialized, evaluate the partial
      // tuple against that conjunct and start a new parse_block for the next conjunct
      parse_block = BasicBlock::Create(context, "parse", fn, eval_fail_block);
      Function* conjunct_fn = conjuncts[conjunct_idx]->codegen_fn();

      Value* conjunct_args[] = {
        tuple_row_arg,
        ConstantPointerNull::get(codegen->ptr_type()),
        is_null_ptr
      };
      Value* result = builder.CreateCall(conjunct_fn, conjunct_args, "conjunct_eval");

      builder.CreateCondBr(result, parse_block, eval_fail_block);
      builder.SetInsertPoint(parse_block);
    }
  }

  // Block if eval failed.  TODO: bytes parsed is not right here.  Probably
  // easiest to iterate through all the fields and just sum up the lengths.
  builder.SetInsertPoint(eval_fail_block);
  builder.CreateRet(codegen->false_value());

  return codegen->FinalizeFunction(fn);
}

Function* HdfsTextScanner::CodegenWriteAlignedTuples(LlvmCodeGen* codegen,
    Function* write_complete_tuple_fn) {
  COUNTER_SCOPED_TIMER(codegen->codegen_timer());
  DCHECK(write_complete_tuple_fn != NULL);

  Function* write_tuples_fn =
      codegen->GetFunction(IRFunction::HDFS_TEXT_SCANNER_WRITE_ALIGNED_TUPLES);
  DCHECK(write_tuples_fn != NULL);

  int replaced = 0;
  write_tuples_fn = codegen->ReplaceCallSites(write_tuples_fn, false,
      write_complete_tuple_fn, "WriteCompleteTuple", &replaced);
  DCHECK_EQ(replaced, 1) << "One call site should be replaced.";
  DCHECK(write_tuples_fn != NULL);

  return codegen->FinalizeFunction(write_tuples_fn);
}
