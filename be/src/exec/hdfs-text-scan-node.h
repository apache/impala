// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_TEXT_SCAN_NODE_H_
#define IMPALA_EXEC_HDFS_TEXT_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>
#include <hdfs.h>
#include <boost/regex.hpp>
#include <boost/scoped_ptr.hpp>

#include "exec/scan-node.h"
#include "runtime/descriptors.h"
#include "util/sse-util.h"

namespace impala {

class DescriptorTbl;
class TPlanNode;
class RowBatch;
class Status;
class TupleDescriptor;
class MemPool;
class Tuple;
class SlotDescriptor;
class stringstream;
class Expr;
class TextConverter;
class TScanRange;

// This execution node parses delimited text files from HDFS,
// and writes their content as tuples in the
// Impala in-memory representation of data (tuples, rows, row batches).
// We read HDFS files one file buffer at a time,
// allocating new buffers with file_buffer_pool_ as needed.
// We parse each file buffer character-by-character and
// write tuples into a fixed-size tuple buffer that is allocated once in Prepare().
// For variable-length fields (e.g. strings), our tuples
// contain pointers to the variable-length data.
// The variable length data may be located in the following places:
// 1. In the original file buffer (we cannot overwrite the buffer until the next GetNext() call)
// 2. Memory allocated from the var_len_pool_
// The data of variable-length slots has to be copied into a
// separate memory location (allocated from var_len_pool_) in the following situations:
// 1. The originating column spans multiple file blocks
// 2. The slot is a string type that must be unescaped
//
// Columns could span multiple file blocks.
// In such scenarios we construct the complete column by
// appending the partial-column bytes to a boundary_column.
//
// TODO: separate file handling and parsing of text files; the latter should go into
// a separate helper class
class HdfsTextScanNode : public ScanNode {
 public:
  HdfsTextScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  // Allocates tuple buffer.
  // Sets tuple_idx_ and tuple_desc_ using RuntimeState.
  // Sets delimiters from table_desc_ in tuple_desc_.
  // Creates mapping from field index in table to slot index in output tuple.
  virtual Status Prepare(RuntimeState* state);

  // Connects to HDFS.
  virtual Status Open(RuntimeState* state);

  // Writes parsed tuples into tuple buffer,
  // and sets pointers in row_batch to point to them.
  // row_batch will be non-full when all blocks of all files have been read and parsed.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // Disconnects from HDFS.
  virtual Status Close(RuntimeState* state);

  virtual void SetScanRange(const TScanRange& scan_range);

 protected:
  // Write debug string of this into out.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  const static char NOT_IN_STRING = -1;
  const static int POOL_INIT_SIZE = 4096;
  const static char DELIM_INIT = -1;
  const static int SKIP_COLUMN = -1;

  // Parser configuration parameters:

  // List of HDFS paths to read.
  std::vector<std::string> files_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
  TupleId tuple_id_;

  // Regular expressions to evaluate over file paths to extract partition key values
  boost::regex partition_key_regex_;

  // Partition key values.  This is computed once for each file and valid for the
  // duration of that file.  The vector contains only literal exprs.
  std::vector<Expr*> partition_key_values_;

  // Descriptor of tuples in input files.
  const TupleDescriptor* tuple_desc_;

  // Tuple index in tuple row.
  int tuple_idx_;

  // Character delimiting tuples.
  char tuple_delim_;

  // Character delimiting fields (to become slots).
  char field_delim_;

  // Character delimiting collection items (to become slots).
  char collection_item_delim_;

  // Escape character. Ignored if strings are not quoted.
  char escape_char_;

  // Indicates whether strings are quoted. If set to false, string quotes will simply be copied.
  bool strings_are_quoted_;

  // Character in which quotes strings are enclosed in data files.
  // Ignored if strings_are_quoted_ is false.
  char string_quote_;

  // contains all memory for tuple data, including string data which we can't reference
  // in the file buffers (because it needs to be unescaped or straddles two file buffers)
  boost::scoped_ptr<MemPool> tuple_pool_;

  // Pool for allocating file buffers.
  boost::scoped_ptr<MemPool> file_buffer_pool_;

  // Pool for allocating objects.
  // Currently only used for creating LiteralExprs from TExpr for partition keys in Prepare().
  boost::scoped_ptr<ObjectPool> obj_pool_;

  // Parser internal state:

  // Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  // File handle of current partition being processed.
  hdfsFile hdfs_file_;

  // Actual bytes received from last file read.
  tSize file_buffer_read_size_;

  // Index of current file being processed.
  int file_idx_;

  // Contiguous block of memory into which tuples are written, allocated
  // from tuple_pool_. We don't allocate individual tuples from tuple_pool_ directly,
  // because MemPool::Allocate() always rounds up to the next 8 bytes
  // (ie, would be wasteful if we had 2-byte tuples).
  char* tuple_buffer_;

  // Size of tuple_buffer_.
  int tuple_buffer_size_;

  // Current tuple.
  Tuple* tuple_;

  // Buffer for data read from file.
  char* file_buffer_;

  // Current position in file buffer.
  char* file_buffer_ptr_;

  // Ending position of file buffer.
  char* file_buffer_end_;

  // True if the current file buffer can be reused (not used to store any tuple data).
  // The file buffer cannot be reused if the resulting tuples contain non-copied string slots
  // as the file buffer contains the tuple data.
  bool reuse_file_buffer_;

  // number of errors in current file
  int num_errors_in_file_;

  // Mapping from column index in table to slot index in output tuple.
  // Created in Prepare() from SlotDescriptors.
  std::vector<int> column_idx_to_slot_idx_;

  // Number of partition keys for the files_. Set in Prepare().
  int num_partition_keys_;

  // Number of materialized slots (not including partition-key slots)
  int num_materialized_slots_;

  // Mapping from partition key index to slot index
  // for materializing the virtual partition keys (if any) into Tuples.
  // pair.first refers to the partition key index.
  // pair.second refers to the target slot index.
  // Created in Prepare() from the TableDescriptor and SlotDescriptors.
  std::vector<std::pair<int, int> > key_idx_to_slot_idx_;

  // Helper string for dealing with input rows that span file blocks.
  // We keep track of a whole line that spans file blocks to be able to report
  // the line as erroneous in case of parsing errors.
  std::string boundary_row_;

  // Helper string for dealing with columns that span file blocks.
  std::string boundary_column_;

  // Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;
  
  // Vector containing all the materialized, non-partitioning slot descriptors
  std::vector<SlotDescriptor*> materialized_slots_;

  // Intermediate structure used for two pass parsing approach.  In the first pass,
  // ParseData structs are filled out and contain where all the fields start and
  // their lengths.  In the second pass, the ParseData is used to write out the slots.
  // We want to keep this struct as small as possible.
  struct ParseData {
    //start of field 
    char* start;  
    // Encodes the length and whether or not this fields needs to be unescaped.   
    // If len < 0, then the field needs to be unescaped.
    int len;
  };
  std::vector<ParseData> parse_data_;

  // SSE(xmm) register containing the tuple search character.
  __m128i xmm_tuple_search_;
  
  // SSE(xmm) register containing the field search character.
  __m128i xmm_field_search_;
  
  // SSE(xmm) register containing the escape search character.
  __m128i xmm_escape_search_;

  // State that can be split between calls to GetNext().  Tuple data can be split across
  // calls to GetNext().  Should be reset on file open since tuple data is not split across files.

  // Index to keep track of the current current column in the current file
  int column_idx_;

  // Index into materialized_slots_ for the next slot to output for the current tuple.
  int slot_idx_;

  // Whether or not the previous character was the escape character
  bool last_char_is_escape_;

  // Whether or not the current column has an escape character in it (and needs to be unescaped)
  bool current_column_has_escape_;
  
  // Whether or not there was a parse error in the current row.  Used for counting the
  // number of errors per file.  Once the error log is full, error_in_row will still be
  // set, in order to be able to record the errors per file, even if the details are not
  // logged.
  bool error_in_row_;
  
  // Parses the current file_buffer_ and writes tuples into the tuple buffer.
  // Input Parameters
  //   state: Runtime state into which we log errors.
  //   row_batch: Row batch into which to write new tuples.
  // Input/Output Parameters:
  //   The following parameters make up the state that must be maintained across file buffers.
  //   These parameters are changed within ParseFileBuffer.
  //   row_batch: Row batch into which to add tuples.
  //   row_idx: Index of current row. Possibly updated within ParseFileBuffer.
  //   unescape_string: Indicates whether the current string-slot contains an escape,
  //                    and must be copied unescaped into the the var_len_buffer.
  //   quote_char: Indicates the last quote character starting a quoted string.
  //               Set to NOT_IN_STRING if string_are_quoted==false,
  //               or we have not encountered a quote.
  // Returns Status::OK if no errors were found,
  // or if errors were found but state->abort_on_error() is false.
  // Returns error message if errors were found and state->abort_on_error() is true.
  Status ParseFileBuffer(RuntimeState* state, RowBatch* row_batch, int* row_idx,
      bool* unescape_string, char* quote_char);

  // Parses the current file_buffer_ using SSE ("Intel x86 instruction set extension 'Streaming
  // Simd Extension'). This should only be called if the hardware suports SSE4.2 instructions.  
  // SSE4.2 added string processing instructions that allow for proecessing 16 characters at a time.
  // This function will write field start/len to 'parsed_data_' which can then be written out
  // to tuples.
  // Input Parameters:
  //  max_tuples: The maximum number of tuples that should be parsed.  This is used to control
  //              how the batching works.
  // Output Parameters:
  //  num_tuples: Number of tuples parsed 
  //  num_fields: Number of materialized fields parsed
  //  col_start: pointer within file_buffer_ where the next field starts
  // TODO: Does not handle quoted strings.
  Status ParseFileBufferSSE(int max_tuples, int* num_tuples, int* num_fields, char** col_start);

  // Writes the intermediate data in parsed_data_ to slots, outputting tuples to row_batch as they
  // complete.
  // Input Paramters:
  //  state: Runtime state into which we log errors
  //  row_batch: Row batch into which to write new tuples
  //  first_column_idx: The col idx for the raw file associated with parsed_data_[0]
  //  num_fields: Total number of fields contained in parsed_data_
  // Input/Output Parameters
  //  row_idx: Index of current row in row_batch.
  //  line_start: pointer to within file_buffer where the current line starts.  This is used
  //              for better error reporting
  Status WriteFields(RuntimeState* state, RowBatch* row_batch, int num_fields, 
      int* row_idx, char** line_start);

  // Write out tuples when there are no materialized fields (e.g. select count(*)).  
  //  num_tuples: Total number of tuples to write out.
  Status WriteTuples(RuntimeState* state, RowBatch* row_batch, int num_tuples, 
      int* row_idx, char** line_start);

  // Appends the current file and line to the RuntimeState's error log (if there is space).  
  // Also, increments num_errors_in_file_.
  void ReportRowParseError(RuntimeState* state, char* line_start, int len);

  // Prepends field data that is was from the previous file buffer (This field straddled two file
  // buffers).  'data' already contains the pointer/len from the current file buffer, 
  // boundary_column_ contains the beginning of the data from the previous file buffer.
  // This function will allocate a new string from the tuple pool, concatenate the two pieces and
  // update 'data' to contain the new pointer/len.
  void CopyBoundaryField(ParseData* data);

  // Initializes the scan node.  
  //  - initialize partition key regex from fe input
  Status Init(ObjectPool* pool, const TPlanNode& tnode);

  // Updates 'partition_key_values_' by extracting the values from the current file path
  // using 'partition_key_regex_'
  Status ExtractPartitionKeyValues(RuntimeState* state);
};

}

#endif
