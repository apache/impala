// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_TEXT_SCANNER_H
#define IMPALA_EXEC_HDFS_TEXT_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "runtime/string-buffer.h"

namespace impala {

class DelimitedTextParser;

// HdfsScanner implementation that understands text-formatted
// records. Uses SSE instructions, if available, for performance.
class HdfsTextScanner : public HdfsScanner {
 public:
  HdfsTextScanner(HdfsScanNode* scan_node, RuntimeState* state, Tuple* template_tuple,
      MemPool* mem_pool);
  virtual ~HdfsTextScanner();
  virtual Status Prepare();
  virtual Status GetNext(RowBatch* row_batch, bool* eosr);

  static const char* LLVM_CLASS_NAME;

 private:
  const static char NOT_IN_STRING = -1;
  const static int POOL_INIT_SIZE = 4096;
  const static char DELIM_INIT = -1;
  const static int NEXT_BLOCK_READ_SIZE = 1024; //bytes

  // Prepends field data that was from the previous file buffer (This field straddled two
  // file buffers).  'data' already contains the pointer/len from the current file buffer,
  // boundary_column_ contains the beginning of the data from the previous file
  // buffer. This function will allocate a new string from the tuple pool, concatenate the
  // two pieces and update 'data' to contain the new pointer/len.
  void CopyBoundaryField(FieldLocation* data);

  // Initialises any state required at the beginning of a new scan
  // range. Here this means resetting escaping state.
  virtual Status InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition, 
      HdfsScanRange* scan_range, Tuple* template_tuple, ByteStream* byte_stream);

  // Writes the intermediate parsed data in to slots, outputting
  // tuples to row_batch as they complete.
  // Input Parameters:
  //  row_batch: Row batch into which to write new tuples
  //  num_fields: Total number of fields contained in parsed_data_
  // Input/Output Parameters
  //  line_start: pointer to within byte_buffer where the current line starts.  This is
  //              used for better error reporting
  // Returns the number of tuples added to the row batch.
  int WriteFields(RowBatch* row_batch, int num_fields, char** line_start);

  // Writes out all slots for 'tuple' from 'fields'. 'fields' must be aligned
  // to the start of the tuple (e.g. fields[0] maps to slots[0]).
  // After writing the tuple, it will be evaluated against the conjuncts.
  //  - error_fields is an out array.  error_fields[i] will be set to true if the ith
  //    field had a parse error
  //  - error_in_row is an out bool.  It is set to true if any field had parse errors
  //  - bytes_processed returns the number of bytes for the entire tuple
  // Returns whether the resulting tuplerow passed the conjuncts.
  //
  // The parsing of the fields and evaluating against conjuncts is combined in this
  // function.  This is done so it can be possible to evaluate conjuncts as slots
  // are materialized (on partial tuples).  TODO: do this
  //
  // This function is replaced by a codegen'd function at runtime.  This is
  // the reason that the out error parameters are typed uint8_t instead of bool. We need
  // to be able to match this function's signature identically for the codegen'd function.
  // Bool's as out parameters can get converted to bytes by the compiler and rather than
  // implicitly depending on that to happen, we will explicitly type them to bytes.
  // TODO: revisit this
  bool WriteCompleteTuple(FieldLocation* fields,
      Tuple* tuple, TupleRow* tuple_row,
      uint8_t* error_fields, uint8_t* error_in_row, int* bytes_processed);

  // Codegen function to replace WriteCompleteTuple. Should behave identically
  // to WriteCompleteTuple.
  llvm::Function* CodegenWriteCompleteTuple(LlvmCodeGen* codegen);

  // Processes batches of fields and writes them out to the row batch.
  // - 'fields' must start at the beginning of a tuple.
  // - num_tuples: number of tuples to process
  // - max_added_tuples: the maximum number of tuples that should be added to the batch.
  // - line_start is in/out parameter.  Caller must pass the start of the first tuple.
  //   on return, will contain the start of the next tuple
  // Returns the number of tuples added to the row batch.  This can be less than
  // num_tuples/tuples_till_limit because of failed conjuncts.
  // Returns -1 if parsing should be aborted due to parse errors.
  int WriteAlignedTuples(RowBatch* row_batch, FieldLocation* fields, int num_tuples,
      int max_added_tuples, int slots_per_tuple, char** line_start);

  // Codegen function to replace WriteAlignedTuples.  WriteAlignedTuples is cross compiled
  // to IR.  This function loads the precompiled IR function, modifies it and returns the
  // resulting function.
  llvm::Function* CodegenWriteAlignedTuples(LlvmCodeGen*, llvm::Function* write_tuple_fn);

  // Utility function to write out 'num_fields' to 'tuple_'.  This is used to parse
  // partial tuples.  Returns bytes processed.  If copy_strings is true, strings
  // from fields will be copied into the tuple_pool_.
  int WritePartialTuple(FieldLocation* fields, int num_fields, bool copy_strings);

  // Utility function to compute the order to materialize slots to allow  for
  // computing conjuncts as slots get materialized (on partial tuples).
  // 'order' will contain for each slot, the first conjunct it is associated with.
  // e.g. order[2] = 1 indicates materialized_slots[2] must be materialized before
  // evaluating conjuncts[1].  Slots that are not referenced by any conjuncts will have
  // order set to conjuncts.size()
  void ComputeSlotMaterializationOrder(std::vector<int>* order);

  // Utility function to report parse errors for each field.
  // If errors[i] is nonzero, fields[i] had a parse error.
  // row_start/len should the complete row containing fields.
  // Returns false if parsing should be aborted.
  bool ReportTupleParseError(FieldLocation* fields, uint8_t* errors,
      char* row_start, int row_len);

  // Appends the current file and line to the RuntimeState's error log (if there's space).
  // Also, increments num_errors_in_file_.
  Status ReportRowParseError(char* line_start, int len);

  // Reads up to size bytes from byte_stream into byte_buffer_, and
  // updates byte_buffer_read_size_
  Status FillByteBuffer(int64_t size);

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

  // Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  // Pool for allocating byte buffers.
  // TODO: If other scanners use similar functionality, consider
  // moving to utility class.
  boost::scoped_ptr<MemPool> byte_buffer_pool_;

  // Current position in byte buffer.
  char* byte_buffer_ptr_;

  // Buffer for data read from HDFS
  char* byte_buffer_;

  // Ending position of HDFS buffer.
  char* byte_buffer_end_;

  // True if the current file buffer can be reused (not used to store any tuple data). The
  // file buffer cannot be reused if the resulting tuples contain non-copied string slots
  // as the file buffer contains the tuple data.
  bool reuse_byte_buffer_;

  // Actual bytes received from last file read.
  int64_t byte_buffer_read_size_;

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

  // Tracks the number of bytes left to read in the current scan
  // range. When <= 0, GetNext will prepare to exit.
  int current_range_remaining_len_;

  // Matching typedef for WriteAlignedTuples for codegen.  Refer to comments for
  // that function.
  typedef int (*WriteTuplesFn)(HdfsTextScanner*, RowBatch*, FieldLocation*, int, int,
      int, char**);
  // Jitted write tuples function pointer.  Null if codegen is disabled.
  WriteTuplesFn write_tuples_fn_;

  // Keep track of the current escape char to decide whether or not to use codegen for
  // tuple writes.
  // TODO(henry / nong): Remove this once we support codegen for escape characters.
  char escape_char_;
};

}

#endif
