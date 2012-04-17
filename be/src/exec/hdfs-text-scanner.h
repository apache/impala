// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_TEXT_SCANNER_H
#define IMPALA_EXEC_HDFS_TEXT_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "exec/delimited-text-parser.h"

namespace impala {

// HdfsScanner implementation that understands text-formatted
// records. Uses SSE instructions, if available, for performance.
class HdfsTextScanner : public HdfsScanner {
 public:
  HdfsTextScanner(HdfsScanNode* scan_node, const TupleDescriptor* tuple_desc,
                  Tuple* template_tuple, MemPool* mem_pool);
  virtual ~HdfsTextScanner();
  virtual Status Prepare(RuntimeState* state, ByteStream* byte_stream);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eosr);

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
  void CopyBoundaryField(DelimitedTextParser::FieldLocation* data);

  // Initialises any state required at the beginning of a new scan
  // range. Here this means resetting escaping state.
  virtual Status InitCurrentScanRange(RuntimeState* state, HdfsScanRange* scan_range,
                                      ByteStream* byte_stream);

  // Writes the intermediate parsed data in to slots, outputting
  // tuples to row_batch as they complete.
  // Input Parameters:
  //  state: Runtime state into which we log errors
  //  row_batch: Row batch into which to write new tuples
  //  first_column_idx: The col idx for the raw file associated with parsed_data_[0]
  //  num_fields: Total number of fields contained in parsed_data_
  // Input/Output Parameters
  //  row_idx: Index of current row in row_batch.
  //  line_start: pointer to within byte_buffer where the current line starts.  This is
  //              used for better error reporting
  Status WriteFields(RuntimeState* state, RowBatch* row_batch, int num_fields,
                     int* row_idx, char** line_start);

  // Appends the current file and line to the RuntimeState's error log (if there's space).
  // Also, increments num_errors_in_file_.
  void ReportRowParseError(RuntimeState* state, char* line_start, int len);

  // Reads up to size bytes from byte_stream into byte_buffer_, and
  // updates byte_buffer_read_size_
  Status FillByteBuffer(RuntimeState* state, int64_t size);

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
  std::vector<DelimitedTextParser::FieldLocation> field_locations_;

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

  // Whether or not there was a parse error in the current row. Used for counting the
  // number of errors per file.  Once the error log is full, error_in_row will still be
  // set, in order to be able to record the errors per file, even if the details are not
  // logged.
  bool error_in_row_;

  // Tracks the number of bytes left to read in the current scan
  // range. When <= 0, GetNext will prepare to exit.
  int current_range_remaining_len_;

};

}

#endif
