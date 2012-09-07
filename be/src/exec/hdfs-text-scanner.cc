// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "codegen/llvm-codegen.h"
#include "exec/byte-stream.h"
#include "exec/delimited-text-parser.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hdfs-text-scanner.h"
#include "exec/scan-range-context.h"
#include "exec/text-converter.h"
#include "exec/text-converter.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

const char* HdfsTextScanner::LLVM_CLASS_NAME = "class.impala::HdfsTextScanner";

HdfsTextScanner::HdfsTextScanner(HdfsScanNode* scan_node, RuntimeState* state) 
    : HdfsScanner(scan_node, state, NULL),
      context_(NULL),
      boundary_mem_pool_(new MemPool()),
      boundary_row_(boundary_mem_pool_.get()),
      boundary_column_(boundary_mem_pool_.get()),
      slot_idx_(0),
      byte_buffer_ptr_(NULL),
      byte_buffer_end_(NULL),
      byte_buffer_read_size_(0),
      error_in_row_(false),
      write_tuples_fn_(NULL),
      escape_char_('\0') {
}

HdfsTextScanner::~HdfsTextScanner() {
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      boundary_mem_pool_->peak_allocated_bytes());
}

void HdfsTextScanner::IssueInitialRanges(HdfsScanNode* scan_node, 
    const vector<HdfsFileDesc*>& files) {
  // Text just issues all ranges at once
  for (int i = 0; i < files.size(); ++i) {
    scan_node->AddDiskIoRange(files[i]);
  }
}

Status HdfsTextScanner::ProcessScanRange(ScanRangeContext* context) {
  // Reset state for new scan range
  InitNewRange(context);

  // Find the first tuple.  If eosr is true, it means we went through the entire
  // scan range without finding a single tuple.  The bytes will be picked up
  // by the scan range before.
  bool tuple_found;
  RETURN_IF_ERROR(FindFirstTuple(&tuple_found));

  if (tuple_found) {
    // Process the scan range
    int dummy_num_tuples;
    RETURN_IF_ERROR(ProcessRange(&dummy_num_tuples, false));

    // Finish up reading past the scan range
    RETURN_IF_ERROR(FinishScanRange());
  }

  // All done with this scan range.
  return Status::OK;
}

Status HdfsTextScanner::Close() {
  context_->AcquirePool(boundary_mem_pool_.get());
  scan_node_->RangeComplete();
  context_->Complete();
  return Status::OK;
}

void HdfsTextScanner::InitNewRange(ScanRangeContext* context) {
  context->set_read_past_buffer_size(NEXT_BLOCK_READ_SIZE);
  context_ = context;
  HdfsPartitionDescriptor* hdfs_partition = context_->partition_descriptor();
  
  char field_delim = hdfs_partition->field_delim();
  char collection_delim = hdfs_partition->collection_delim();
  if (scan_node_->materialized_slots().size() == 0) {
    field_delim = '\0';
    collection_delim = '\0';
  }
  escape_char_ = hdfs_partition->escape_char();
  
  delimited_text_parser_.reset(new DelimitedTextParser(scan_node_,
      hdfs_partition->line_delim(), field_delim, collection_delim,
      hdfs_partition->escape_char()));
  text_converter_.reset(new TextConverter(escape_char_));

  error_in_row_ = false;
  
  // Note - this initialisation relies on the assumption that N partition keys will occupy
  // entries 0 through N-1 in column_idx_to_slot_idx. If this changes, we will need
  // another layer of indirection to map text-file column indexes onto the
  // column_idx_to_slot_idx table used below.
  slot_idx_ = 0;
  
  boundary_column_.Clear();
  boundary_row_.Clear();
  delimited_text_parser_->ParserReset();

  template_tuple_ = context_->template_tuple();
  partial_tuple_empty_ = true;
  byte_buffer_ptr_ = byte_buffer_end_ = NULL;

  partial_tuple_ = reinterpret_cast<Tuple*>(
      boundary_mem_pool_->Allocate(scan_node_->tuple_desc()->byte_size()));
}

Status HdfsTextScanner::FinishScanRange() {
  // For text we always need to scan past the scan range to find the next delimiter
  while (true) {
    bool eosr;
    Status status = FillByteBuffer(&eosr, NEXT_BLOCK_READ_SIZE);
    
    if (!status.ok() || byte_buffer_read_size_ == 0) {
      stringstream ss;
      if (status.IsCancelled()) return status;
      
      if (!status.ok()) {
        ss << "Read failed while trying to finish scan range: " << context_->filename() 
           << ":" << context_->file_offset() << endl << status.GetErrorMsg();
      } else if (!partial_tuple_empty_ || !boundary_column_.Empty() || 
          !boundary_row_.Empty()) {
        ss << "Incomplete tuple at end of file: " << context_->filename() << ":" 
           << context_->file_offset();
      } else {
        // This is the case where this is eof and everything is done.  
        break;
      }
      
      if (state_->LogHasSpace()) state_->LogError(ss.str());
      if (state_->abort_on_error()) return Status(ss.str());
      break;
    }

    DCHECK(eosr);

    int num_tuples;
    RETURN_IF_ERROR(ProcessRange(&num_tuples, true));
    if (num_tuples == 1) break;
    DCHECK_EQ(num_tuples, 0);
  }

  return Status::OK;
}

Status HdfsTextScanner::ProcessRange(int* num_tuples, bool past_scan_range) {
  bool eosr = past_scan_range || context_->eosr();

  while (true) {
    if (!eosr && byte_buffer_ptr_ == byte_buffer_end_) {
      RETURN_IF_ERROR(FillByteBuffer(&eosr));
    }
  
    MemPool* pool;
    TupleRow* tuple_row_mem;
    int max_tuples = context_->GetMemory(&pool, &tuple_, &tuple_row_mem);
    
    if (past_scan_range) {
      // byte_buffer_ptr_ is already set from FinishScanRange()
      max_tuples = 1;
      eosr = true;
    } 

    *num_tuples = 0;
    int num_fields = 0;
    
    DCHECK_GT(max_tuples, 0);
    
    batch_start_ptr_ = byte_buffer_ptr_;
    char* col_start = byte_buffer_ptr_;
    {
      // Parse the bytes for delimiters and store their offsets in field_locations_
      SCOPED_TIMER(parse_delimiter_timer_);
      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(max_tuples,
          byte_buffer_end_ - byte_buffer_ptr_, &byte_buffer_ptr_, 
          &row_end_locations_[0],
          &field_locations_, num_tuples, &num_fields, &col_start));
    }

    // Materialize the tuples into the in memory format for this query
    int num_tuples_materialized = 0;
    if (scan_node_->materialized_slots().size() != 0 &&
        (num_fields > 0 || *num_tuples > 0)) {
      // There can be one partial tuple which returned no more fields from this buffer.
      DCHECK_LE(*num_tuples, num_fields + 1);
      if (!boundary_column_.Empty()) {
        CopyBoundaryField(&field_locations_[0], pool);
        boundary_column_.Clear();
      }
      num_tuples_materialized = WriteFields(pool, tuple_row_mem, num_fields, *num_tuples);
      DCHECK_GE(num_tuples_materialized, 0);
      RETURN_IF_ERROR(parse_status_);
      if (num_tuples > 0) {
        // If we saw any tuple delimiters, clear the boundary_row_.
        boundary_row_.Clear();
      }
    } else if (*num_tuples != 0) {
      SCOPED_TIMER(scan_node_->materialize_tuple_timer());
      // If we are doing count(*) then we return tuples only containing partition keys
      boundary_row_.Clear();
      num_tuples_materialized = WriteEmptyTuples(context_, tuple_row_mem, *num_tuples);
    }

    // Save contents that are split across buffers if we are going to return this column
    if (col_start != byte_buffer_ptr_ && delimited_text_parser_->ReturnCurrentColumn()) {
      DCHECK_EQ(byte_buffer_ptr_, byte_buffer_end_);
      boundary_column_.Append(col_start, byte_buffer_ptr_ - col_start);
      char* last_row = NULL;
      if (*num_tuples == 0) {
        last_row = batch_start_ptr_;
      } else {
        last_row = row_end_locations_[*num_tuples - 1] + 1;
      }
      boundary_row_.Append(last_row, byte_buffer_ptr_ - last_row);
    }
    
    // Commit the rows to the row batch and scan node
    if (num_tuples_materialized > 0) {
      context_->CommitRows(num_tuples_materialized);
    }

    // Done with this buffer and the scan range
    if ((byte_buffer_ptr_ == byte_buffer_end_ && eosr) || past_scan_range) {
      break;
    } 

    // Scanning was aborted by main thread
    if (context_->cancelled()) break;
  }

  return Status::OK;
}

Status HdfsTextScanner::FillByteBuffer(bool* eosr, int num_bytes) {
  *eosr = false;
  RETURN_IF_ERROR(context_->GetBytes(
      reinterpret_cast<uint8_t**>(&byte_buffer_ptr_), num_bytes, 
          &byte_buffer_read_size_, eosr));
  byte_buffer_end_ = byte_buffer_ptr_ + byte_buffer_read_size_;
  return Status::OK;
}

Status HdfsTextScanner::FindFirstTuple(bool* tuple_found) {
  *tuple_found = true;
  if (context_->file_offset() != 0) {
    *tuple_found = false;
    // Offset may not point to tuple boundary, skip ahead to the first full tuple
    // start.
    while (true) {
      bool eosr = false;
      RETURN_IF_ERROR(FillByteBuffer(&eosr));

      SCOPED_TIMER(parse_delimiter_timer_);
      int first_tuple_offset = delimited_text_parser_->FindFirstInstance(
          byte_buffer_ptr_, byte_buffer_read_size_);
      
      if (first_tuple_offset == -1) {
        // Didn't find tuple in this buffer, keep going with this scan range
        if (!eosr) continue;
      } else {
        byte_buffer_ptr_ += first_tuple_offset;
        *tuple_found = true;
      }
      break;
    }
  }
  DCHECK(delimited_text_parser_->AtTupleStart());
  return Status::OK;
}

Status HdfsTextScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());
  
  parse_delimiter_timer_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "DelimiterParseTime", TCounterType::CPU_TICKS);

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  field_locations_.resize(state_->batch_size() * scan_node_->materialized_slots().size());
  row_end_locations_.resize(state_->batch_size());

  // Codegen for materialized parsed data into tuples.  The WriteCompleteTuple is
  // codegen'd using the IRBuilder for the specific tuple description.  This function
  // is then injected into the cross-compiled driving function, WriteAlignedTuples().
#if 0
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
#endif

  return Status::OK;
}

// TODO: remove when all scanners are updated
Status HdfsTextScanner::GetNext(RowBatch* row_batch, bool* eosr) {
  DCHECK(false);
  return Status::OK;
}

bool HdfsTextScanner::ReportTupleParseError(FieldLocation* fields, uint8_t* errors,
    int row_idx) {
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    if (errors[i]) {
      const SlotDescriptor* desc = scan_node_->materialized_slots()[i];
      ReportColumnParseError(desc, fields[i].start, fields[i].len);
      errors[i] = false;
    }
  }
  parse_status_ = ReportRowParseError(row_idx);
  return parse_status_.ok();
}

Status HdfsTextScanner::ReportRowParseError(int row_idx) {
  ++num_errors_in_file_;
  if (state_->LogHasSpace()) {
    DCHECK_LT(row_idx, row_end_locations_.size());
    char* row_end = row_end_locations_[row_idx];
    char* row_start;
    if (row_idx == 0) {
      row_start = batch_start_ptr_;
    } else {
      // Row start at 1 past the row end (i.e. the row delimiter) for the previous row
      row_start = row_end_locations_[row_idx - 1] + 1; 
    }

    stringstream ss;
    ss << "file: " << context_->filename() << endl << "line: ";
    if (!boundary_row_.Empty()) {
      // Log the beginning of the line from the previous file buffer(s)
      ss << boundary_row_.str();
    }
    // Log the erroneous line (or the suffix of a line if !boundary_line.empty()).
    ss << string(row_start, row_end - row_start);
    state_->LogError(ss.str());
  }

  if (state_->abort_on_error()) {
    state_->ReportFileErrors(context_->filename(), 1);
    return Status(state_->ErrorLog());
  }
  return Status::OK;
}


// This function writes fields in 'field_locations_' to the row_batch.  This function
// deals with tuples that straddle batches.  There are two cases:
// 1. There is already a partial tuple in flight from the previous time around.
//   This tuple can either be fully materialized (all the materialized columns have
//   been processed but we haven't seen the tuple delimiter yet) or only partially
//   materialized.  In this case num_tuples can be greater than num_fields
// 2. There is a non-fully materialized tuple at the end.  The cols that have been
//   parsed so far are written to 'tuple_' and the remained will be picked up (case 1)
//   the next time around.
int HdfsTextScanner::WriteFields(MemPool* pool, TupleRow* tuple_row, 
    int num_fields, int num_tuples) {
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());

  FieldLocation* fields = &field_locations_[0];

  int num_tuples_processed = 0;
  int num_tuples_materialized = 0;
  // Write remaining fields, if any, from the previous partial tuple.
  if (slot_idx_ != 0) {
    DCHECK(tuple_ != NULL);
    int num_partial_fields = scan_node_->materialized_slots().size() - slot_idx_;
    // Corner case where there will be no materialized tuples but at least one col
    // worth of string data.  In this case, make a deep copy and reuse the byte buffer.
    bool copy_strings = num_partial_fields > num_fields;
    num_partial_fields = min(num_partial_fields, num_fields);
    WritePartialTuple(fields, num_partial_fields, copy_strings);

    // This handles case 1.  If the tuple is complete and we've found a tuple delimiter
    // this time around (i.e. num_tuples > 0), add it to the row batch.  Otherwise,
    // it will get picked up the next time around
    if (slot_idx_ == scan_node_->materialized_slots().size() && num_tuples > 0) {
      if (UNLIKELY(error_in_row_)) {
        parse_status_ = ReportRowParseError(0);
        if (!parse_status_.ok()) return 0;
        error_in_row_ = false;
      }
      boundary_row_.Clear();

      memcpy(tuple_, partial_tuple_, scan_node_->tuple_desc()->byte_size());
      partial_tuple_empty_ = true;
      tuple_row->SetTuple(tuple_idx_, tuple_);

      slot_idx_ = 0;
      ++num_tuples_processed;
      --num_tuples;

      if (ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, tuple_row)) {
        ++num_tuples_materialized;
        tuple_ = context_->next_tuple(tuple_);
        tuple_row = context_->next_row(tuple_row);
      }
    } 

    num_fields -= num_partial_fields;
    fields += num_partial_fields;
  }

  // Write complete tuples.  The current field, if any, is at the start of a tuple.
  if (num_tuples > 0) {
    int max_added_tuples = (scan_node_->limit() == -1) ?
          num_tuples : scan_node_->limit() - scan_node_->rows_returned();
    int tuples_returned = 0;
    // Call jitted function if possible
    if (write_tuples_fn_ != NULL && escape_char_ == '\0') {
      DCHECK(false);
#if 0
      // TODO: turn codegen back on
      tuples_returned = write_tuples_fn_(this, row_batch, fields, num_tuples,
            max_added_tuples, scan_node_->materialized_slots().size(), 
            num_tuples_processed);
#endif
    } else {
      tuples_returned = WriteAlignedTuples(pool, tuple_row, 
          context_->row_byte_size(), fields, num_tuples, max_added_tuples, 
          scan_node_->materialized_slots().size(), num_tuples_processed);
    }
    if (tuples_returned == -1) return 0;
    DCHECK_EQ(slot_idx_, 0);

    num_tuples_materialized += tuples_returned;
    num_fields -= num_tuples * scan_node_->materialized_slots().size();
    fields += num_tuples * scan_node_->materialized_slots().size();
  }

  DCHECK_GE(num_fields, 0);
  DCHECK_LE(num_fields, scan_node_->materialized_slots().size());

  // Write out the remaining slots (resulting in a partially materialized tuple)
  if (num_fields != 0) {
    DCHECK(tuple_ != NULL);
    InitTuple(context_->template_tuple(), partial_tuple_);
    // If there have been no materialized tuples at this point, copy string data
    // out of byte_buffer and reuse the byte_buffer.  The copied data can be at
    // most one tuple's worth.
    WritePartialTuple(fields, num_fields, num_tuples_materialized == 0);
    partial_tuple_empty_ = false;
  }
  DCHECK_LE(slot_idx_, scan_node_->materialized_slots().size());
  return num_tuples_materialized;
}

void HdfsTextScanner::CopyBoundaryField(FieldLocation* data, MemPool* pool) {
  const int total_len = data->len + boundary_column_.Size();
  char* str_data = reinterpret_cast<char*>(pool->Allocate(total_len));
  memcpy(str_data, boundary_column_.str().ptr, boundary_column_.Size());
  memcpy(str_data + boundary_column_.Size(), data->start, data->len);
  data->start = str_data;
  data->len = total_len;
}

int HdfsTextScanner::WritePartialTuple(FieldLocation* fields, 
    int num_fields, bool copy_strings) {
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
    if (!text_converter_->WriteSlot(desc, partial_tuple_,
        fields[i].start, len, true, need_escape, boundary_mem_pool_.get())) {
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

bool HdfsTextScanner::WriteCompleteTuple(MemPool* pool, FieldLocation* fields, 
    Tuple* tuple, TupleRow* tuple_row, uint8_t* error_fields, uint8_t* error_in_row) {
  // Initialize tuple before materializing slots
  InitTuple(template_tuple_, tuple);

  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    int need_escape = false;
    int len = fields[i].len;
    if (UNLIKELY(len < 0)) {
      len = -len;
      need_escape = true;
    }

    SlotDescriptor* desc = scan_node_->materialized_slots()[i];
    bool error = !text_converter_->WriteSlot(desc, tuple,
        fields[i].start, len, scan_node_->compact_data(), need_escape, pool);
    error_fields[i] = error;
    *error_in_row |= error;
  }

  tuple_row->SetTuple(tuple_idx_, tuple);
  return ExecNode::EvalConjuncts(&conjuncts_mem_[0], num_conjuncts_, tuple_row);
}

// Codegen for WriteTuple(above).  The signature matches WriteTuple (except for the
// this* first argument).  For writing out and evaluating a single string slot:
// define i1 @WriteCompleteTuple(%"class.impala::HdfsTextScanner"* %this, 
//                               %"struct.impala::FieldLocation"* %fields, 
//                               %"class.impala::Tuple"* %tuple, 
//                               %"class.impala::TupleRow"* %tuple_row, 
//                               i8* %error_fields, i8* %error_in_row) {
// entry:
//   %null_ptr = alloca i1
//   %tuple_ptr = bitcast %"class.impala::Tuple"* %tuple 
//                                              to { i8, %"struct.impala::StringValue" }*
//   %tuple_row_ptr = bitcast %"class.impala::TupleRow"* %tuple_row to i8**
//   %null_byte = getelementptr inbounds 
//                    { i8, %"struct.impala::StringValue" }* %tuple_ptr, i32 0, i32 0
//   store i8 0, i8* %null_byte
//   %0 = bitcast i8** %tuple_row_ptr to { i8, %"struct.impala::StringValue" }**
//   %1 = getelementptr { i8, %"struct.impala::StringValue" }** %0, i32 0
//   store { i8, %"struct.impala::StringValue" }* %tuple_ptr, 
//         { i8, %"struct.impala::StringValue" }** %1
//   br label %parse
// 
// parse:                                            ; preds = %entry
//   %data_ptr = getelementptr %"struct.impala::FieldLocation"* %fields, i32 0, i32 0
//   %len_ptr = getelementptr %"struct.impala::FieldLocation"* %fields, i32 0, i32 1
//   %slot_error_ptr = getelementptr i8* %error_fields, i32 0
//   %data = load i8** %data_ptr
//   %len = load i32* %len_ptr
//   %2 = call i1 @WriteSlot({ i8, %"struct.impala::StringValue" }* 
//                                 %tuple_ptr, i8* %data, i32 %len)
//   %slot_parse_error = xor i1 %2, true
//   %error_in_row1 = or i1 false, %slot_parse_error
//   %3 = zext i1 %slot_parse_error to i8
//   store i8 %3, i8* %slot_error_ptr
//   %conjunct_eval = call i1 @BinaryPredicate(i8** %tuple_row_ptr, 
//                                             i8* null, i1* %null_ptr)
//   br i1 %conjunct_eval, label %parse2, label %eval_fail
// 
// parse2:                                           ; preds = %parse
//   %4 = zext i1 %error_in_row1 to i8
//   store i8 %4, i8* %error_in_row
//   ret i1 true
// 
// eval_fail:                                        ; preds = %parse
//   ret i1 false
// }
Function* HdfsTextScanner::CodegenWriteCompleteTuple(LlvmCodeGen* codegen) {
  SCOPED_TIMER(codegen->codegen_timer());
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
  if (scan_node_->compact_data() && !scan_node_->tuple_desc()->string_slots().empty()) {
    return NULL;
  }

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

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[6];
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

  // Codegen for function body
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
    codegen->CodegenMemcpy(&builder, tuple_arg, template_tuple_ptr, tuple_byte_size_);
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

  // Block if eval failed.  
  builder.SetInsertPoint(eval_fail_block);
  builder.CreateRet(codegen->false_value());

  return codegen->FinalizeFunction(fn);
}

Function* HdfsTextScanner::CodegenWriteAlignedTuples(LlvmCodeGen* codegen,
    Function* write_complete_tuple_fn) {
  SCOPED_TIMER(codegen->codegen_timer());
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
