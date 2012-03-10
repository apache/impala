// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_HDFS_TEXT_SCANNER_H_
#define IMPALA_HDFS_TEXT_SCANNER_H_

#include "exec/hdfs-scanner.h"

namespace impala {

// HdfsScanner implementation that understands text-formatted
// records. Uses SSE instructions, if available, for performance.
class HdfsTextScanner : public HdfsScanner {
 public:
  HdfsTextScanner(HdfsScanNode* scan_node, const TupleDescriptor* tuple_desc,
                  Tuple* template_tuple, MemPool* mem_pool);
  virtual Status Prepare(RuntimeState* state, ByteStream* byte_stream);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eosr);

 private:
  const static char NOT_IN_STRING = -1;
  const static int POOL_INIT_SIZE = 4096;
  const static char DELIM_INIT = -1;
  const static int NEXT_BLOCK_READ_SIZE = 1024; //bytes

  // Memory pool for allocations into the boundary row / column
  boost::scoped_ptr<MemPool> boundary_mem_pool_;

  // Helper string for dealing with input rows that span file blocks.
  // We keep track of a whole line that spans file blocks to be able to report
  // the line as erroneous in case of parsing errors.
  StringBuffer boundary_row_;

  // Helper string for dealing with columns that span file blocks.
  StringBuffer boundary_column_;

  // Index to keep track of the current current column in the current file
  int column_idx_;

  // Index into materialized_slots_ for the next slot to output for the current tuple.
  int slot_idx_;

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

  // Intermediate structure used for two pass parsing approach. In the first pass,
  // FieldLocations structs are filled out and contain where all the fields start and
  // their lengths.  In the second pass, the FieldLocations is used to write out the
  // slots. We want to keep this struct as small as possible.
  struct FieldLocations {
    //start of field
    char* start;
    // Encodes the length and whether or not this fields needs to be unescaped.
    // If len < 0, then the field needs to be unescaped.
    int len;
  };
  std::vector<FieldLocations> field_locations_;

  // SSE(xmm) register containing the tuple search character.
  __m128i xmm_tuple_search_;

  // SSE(xmm) register containing the field search character.
  __m128i xmm_field_search_;

  // SSE(xmm) register containing the escape search character.
  __m128i xmm_escape_search_;

  // Character delimiting tuples.
  char tuple_delim_;

  // Character delimiting fields (to become slots).
  char field_delim_;

  // Character delimiting collection items (to become slots).
  char collection_item_delim_;

  // Escape character.
  char escape_char_;

  // Whether or not the previous character was the escape character
  bool last_char_is_escape_;

  // Whether or not the current column has an escape character in it
  // (and needs to be unescaped)
  bool current_column_has_escape_;

  // Tracks the number of bytes left to read in the current scan
  // range. When <= 0, GetNext will prepare to exit.
  int current_range_remaining_len_;

  // Prepends field data that was from the previous file buffer (This field straddled two
  // file buffers).  'data' already contains the pointer/len from the current file buffer,
  // boundary_column_ contains the beginning of the data from the previous file
  // buffer. This function will allocate a new string from the tuple pool, concatenate the
  // two pieces and update 'data' to contain the new pointer/len.
  void CopyBoundaryField(FieldLocations* data);

  // Parses the current file_buffer_ for the field and tuple breaks.
  // This function will write the field start & len to 'parsed_data_'
  // which can then be written out to tuples.
  // This function will use SSE ("Intel x86 instruction set extension
  // 'Streaming Simd Extension') if the hardware supports SSE4.2
  // instructions.  SSE4.2 added string processing instructions that
  // allow for processing 16 characters at a time.  Otherwise, this
  // function will walk the file_buffer_ character by character.
  // Input Parameters:
  //  max_tuples: The maximum number of tuples that should be parsed.
  //              This is used to control how the batching works.
  // Output Parameters:
  //  num_tuples: Number of tuples parsed
  //  num_fields: Number of materialized fields parsed
  //  col_start: pointer within file_buffer_ where the next field starts
  Status ParseFileBuffer(int max_tuples, int* num_tuples, int* num_fields,
                         char** column_start);

  // Initialises any state required at the beginning of a new scan
  // range. Here this means resetting escaping state.
  virtual Status InitCurrentScanRange(RuntimeState* state, HdfsScanRange* scan_range,
                                      ByteStream* byte_stream);

  // Searches for the offset of the first full tuple in the supplied buffer.
  int FindFirstTupleStart(char* buffer, int len);

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

  Status WriteSlots(RuntimeState* state,  int tuple_idx, int* next_line_offset);

  // Appends the current file and line to the RuntimeState's error log (if there's space).
  // Also, increments num_errors_in_file_.
  void ReportRowParseError(RuntimeState* state, char* line_start, int len);

  // Reads up to size bytes from byte_stream into byte_buffer_, and
  // updates byte_buffer_read_size_
  Status FillByteBuffer(RuntimeState* state, int64_t size);

};

}

#endif
