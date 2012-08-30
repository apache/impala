// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_TEXT_SCANNER_H
#define IMPALA_EXEC_HDFS_TEXT_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "runtime/string-buffer.h"

namespace impala {

class DelimitedTextParser;
class ScanRangeContext;
class HdfsFileDesc;

// HdfsScanner implementation that understands text-formatted
// records. Uses SSE instructions, if available, for performance.
class HdfsTextScanner : public HdfsScanner {
 public:
  HdfsTextScanner(HdfsScanNode* scan_node, RuntimeState* state);
  virtual ~HdfsTextScanner();

  // Implementation of HdfsScanner interface.
  virtual Status Prepare();
  virtual Status GetNext(RowBatch* row_batch, bool* eosr);
  virtual Status ProcessScanRange(ScanRangeContext* context);

  // Issue io manager byte ranges for 'files'
  static void IssueInitialRanges(HdfsScanNode*, const std::vector<HdfsFileDesc*>& files);

  static const char* LLVM_CLASS_NAME;

 private:
  const static int NEXT_BLOCK_READ_SIZE = 1024; //bytes

  // Initializes this scanner for this context.  The context maps to a single
  // scan range.
  void InitNewRange(ScanRangeContext* context);

  // Finds the start of the first tuple in this scan range and initializes 
  // byte_buffer_ptr to be the next character (the start of the first tuple).  If 
  // there are no tuples starts in the entire range, *tuple_found is set to false 
  // and no more processing neesd to be done in this range (i.e. there are really large
  // columns)
  Status FindFirstTuple(bool* tuple_found);

  // Process the entire scan range, reading bytes from context and appending
  // materialized row batches to the scan node.  *num_tuples returns the
  // number of tuples parsed.  past_scan_range is true if this is processing
  // beyond the end of the scan range and this function should stop after
  // finding one tuple.
  Status ProcessRange(int* num_tuples, bool past_scan_range);

  // Reads past the end of the scan range for the next tuple end.
  Status FinishScanRange();
  
  // Fills the next byte buffer from the context.  This will block if there are
  // no bytes ready.  Updates byte_buffer_ptr, byte_buffer_end_ and 
  // byte_buffer_read_size_.
  // If num_bytes is 0, the scanner will read whatever is the io mgr buffer size,
  // otherwise it will just read num_bytes.
  Status FillByteBuffer(bool* eosr, int num_bytes = 0);

  // Prepends field data that was from the previous file buffer (This field straddled two
  // file buffers).  'data' already contains the pointer/len from the current file buffer,
  // boundary_column_ contains the beginning of the data from the previous file
  // buffer. This function will allocate a new string from the tuple pool, concatenate the
  // two pieces and update 'data' to contain the new pointer/len.
  void CopyBoundaryField(FieldLocation* data, MemPool* pool);

  // Writes the intermediate parsed data into slots, outputting
  // tuples to row_batch as they complete.
  // Input Parameters:
  //  mempool: MemPool to allocate from for field data
  //  num_fields: Total number of fields contained in parsed_data_
  //  num_tuples: Number of tuples in parsed_data_. This includes the potential
  //    partial tuple at the beginning of 'field_locations_'. 
  // Returns the number of tuples added to the row batch.
  int WriteFields(MemPool*, TupleRow* tuple_row_mem, int num_fields, int num_tuples);

  // Writes out all slots for 'tuple' from 'fields'. 'fields' must be aligned
  // to the start of the tuple (e.g. fields[0] maps to slots[0]).
  // After writing the tuple, it will be evaluated against the conjuncts.
  //  - error_fields is an out array.  error_fields[i] will be set to true if the ith
  //    field had a parse error
  //  - error_in_row is an out bool.  It is set to true if any field had parse errors
  // Returns whether the resulting tuplerow passed the conjuncts.
  //
  // The parsing of the fields and evaluating against conjuncts is combined in this
  // function.  This is done so it can be possible to evaluate conjuncts as slots
  // are materialized (on partial tuples).  
  //
  // This function is replaced by a codegen'd function at runtime.  This is
  // the reason that the out error parameters are typed uint8_t instead of bool. We need
  // to be able to match this function's signature identically for the codegen'd function.
  // Bool's as out parameters can get converted to bytes by the compiler and rather than
  // implicitly depending on that to happen, we will explicitly type them to bytes.
  // TODO: revisit this
  bool WriteCompleteTuple(MemPool* pool, FieldLocation* fields, Tuple* tuple, 
      TupleRow* tuple_row, uint8_t* error_fields, uint8_t* error_in_row);

  // Codegen function to replace WriteCompleteTuple. Should behave identically
  // to WriteCompleteTuple.
  llvm::Function* CodegenWriteCompleteTuple(LlvmCodeGen* codegen);

  // Processes batches of fields and writes them out to tuple_row_mem.
  // - 'pool' mempool to allocate from for auxiliary tuple memory
  // - tuple_row_mem preallocated tuple_row memory this function must use.
  // - 'fields' must start at the beginning of a tuple.
  // - num_tuples: number of tuples to process
  // - max_added_tuples: the maximum number of tuples that should be added to the batch.
  // - row_start_index is the number of rows that have already been processed
  //   as part of WritePartialTuple.  
  // Returns the number of tuples added to the row batch.  This can be less than
  // num_tuples/tuples_till_limit because of failed conjuncts.
  // Returns -1 if parsing should be aborted due to parse errors.
  int WriteAlignedTuples(MemPool* pool, TupleRow* tuple_row_mem, int row_size, 
      FieldLocation* fields, int num_tuples,
      int max_added_tuples, int slots_per_tuple, int row_start_indx);

  // Codegen function to replace WriteAlignedTuples.  WriteAlignedTuples is cross compiled
  // to IR.  This function loads the precompiled IR function, modifies it and returns the
  // resulting function.
  llvm::Function* CodegenWriteAlignedTuples(LlvmCodeGen*, llvm::Function* write_tuple_fn);

  // Utility function to write out 'num_fields' to 'tuple_'.  This is used to parse
  // partial tuples.  Returns bytes processed.  If copy_strings is true, strings
  // from fields will be copied into the boundary pool.
  int WritePartialTuple(FieldLocation*, int num_fields, bool copy_strings);

  // Utility function to compute the order to materialize slots to allow  for
  // computing conjuncts as slots get materialized (on partial tuples).
  // 'order' will contain for each slot, the first conjunct it is associated with.
  // e.g. order[2] = 1 indicates materialized_slots[2] must be materialized before
  // evaluating conjuncts[1].  Slots that are not referenced by any conjuncts will have
  // order set to conjuncts.size()
  void ComputeSlotMaterializationOrder(std::vector<int>* order);

  // Utility function to report parse errors for each field.
  // If errors[i] is nonzero, fields[i] had a parse error.
  // row_idx is the idx of the row in the current batch that had the parse error
  // Returns false if parsing should be aborted.
  bool ReportTupleParseError(FieldLocation* fields, uint8_t* errors, int row_idx);

  // Appends the current file and line to the RuntimeState's error log (if there's space).
  // Also, increments num_errors_in_file_.
  // row_idx is 0-based (in current batch) where the parse error occured.
  Status ReportRowParseError(int row_idx);

  // Context for this scan range
  ScanRangeContext* context_;

  // Memory pool for allocations into the boundary row / column
  boost::scoped_ptr<MemPool> boundary_mem_pool_;

  // Helper string for dealing with input rows that span file blocks.
  // We keep track of a whole line that spans file blocks to be able to report
  // the line as erroneous in case of parsing errors.
  StringBuffer boundary_row_;

  // Helper string for dealing with columns that span file blocks.
  StringBuffer boundary_column_;

  // Index into materialized_slots_ for the next slot to output for the current tuple.
  int slot_idx_;

  // Helper class for picking fields and rows from delimited text.
  boost::scoped_ptr<DelimitedTextParser> delimited_text_parser_;

  // Return field locations from the Delimited Text Parser.
  std::vector<FieldLocation> field_locations_;

  // Pointers into 'byte_buffer_' for the end ptr locations for each row
  // processed in the current batch.  Used to report row errors.
  std::vector<char*> row_end_locations_;

  // Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  // Current position in byte buffer.
  char* byte_buffer_ptr_;

  // Pointer into byte_buffer that is the start of the current batch being
  // processed.
  char* batch_start_ptr_;

  // Ending position of HDFS buffer.
  char* byte_buffer_end_;

  // Actual bytes received from last file read.
  int byte_buffer_read_size_;

  // Contains current parse status to minimize the number of Status objects returned.
  // This significantly minimizes the cross compile dependencies for llvm since status
  // objects inline a bunch of string functions.  Also, status objects aren't extremely
  // cheap.
  Status parse_status_;

  // Whether or not there was a parse error in the current row. Used for counting the
  // number of errors per file.  Once the error log is full, error_in_row will still be
  // set, in order to be able to record the errors per file, even if the details are not
  // logged.
  bool error_in_row_;

  // Matching typedef for WriteAlignedTuples for codegen.  Refer to comments for
  // that function.
  typedef int (*WriteTuplesFn)(HdfsTextScanner*, RowBatch*, FieldLocation*, int, int,
      int, int);
  // Jitted write tuples function pointer.  Null if codegen is disabled.
  WriteTuplesFn write_tuples_fn_;

  // Keep track of the current escape char to decide whether or not to use codegen for
  // tuple writes.
  // TODO(henry / nong): Remove this once we support codegen for escape characters.
  char escape_char_;

  // Memory to store partial tuples split across buffers.  Memory comes from 
  // boundary_pool_.  There is only one tuple allocated for this object and reused
  // for boundary tuples.
  Tuple* partial_tuple_;

  // If false, there is a tuple that is partially materialized (i.e. partial_tuple_
  // contains data)
  bool partial_tuple_empty_;

  // Time parsing text files
  RuntimeProfile::Counter* parse_delimiter_timer_;
};

}

#endif
