// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_TEXT_SCAN_NODE_H_
#define IMPALA_EXEC_TEXT_SCAN_NODE_H_

#include <vector>
#include <stdint.h>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include "runtime/descriptors.h"
#include "exec-node.h"

namespace impala {

class TPlanNode;
class RowBatch;
class Status;
class TupleDescriptor;
class MemPool;
class Tuple;
class SlotDescriptor;
class stringstream;
class Expr;

// This execution node parses delimited text files from HDFS,
// and writes their content as tuples in the
// Impala in-memory representation of data (tuples, rows, row batches).
// We read HDFS files one file buffer at a time,
// allocating new buffers with file_buf_pool_ as needed.
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
class TextScanNode : public ExecNode {
 public:
  TextScanNode(ObjectPool* pool, const TPlanNode& tnode);

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
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch);

  // Disconnects from HDFS.
  virtual Status Close(RuntimeState* state);

 protected:
  // Write debug string of this into out.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:

  // Parser configuration parameters:

  // List of HDFS paths to read.
  const std::vector<std::string> files_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
  TupleId tuple_id_;

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

  // Memory pools created in c'tor and destroyed in d'tor.

  // Pool for allocating tuple buffer.
  boost::scoped_ptr<MemPool> tuple_buf_pool_;

  // Pool for allocating file buffers;
  boost::scoped_ptr<MemPool> file_buf_pool_;

  // Pool for allocating memory for variable-length slots.
  boost::scoped_ptr<MemPool> var_len_pool_;

  // Pool for allocating objects.
  // Currently only used for creating LiteralExprs from TExpr for partition keys in Prepare().
  boost::scoped_ptr<ObjectPool> obj_pool_;

  // Pseudo ring of file buffers, to avoid reallocation.
  // file_bufs_[0] always points to the file buffer from a previous GetNext() call.
  // For reading HDFS files, we hand off buffers starting from index 1,
  // so we never overwrite the buffer from a previous GetNext() call.
  // Whenever a row batch fills up, we swap file_bufs_[file_buf_idx_] with file_bufs_[0].
  // Also see GetFileBuffer().
  std::vector<void*> file_bufs_;


  // Parser internal state:

  // Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  // File handle of current partition being processed.
  hdfsFile hdfs_file_;

  // Actual bytes received from last file read.
  tSize file_buf_actual_size_;

  // Index of current file being processed.
  int file_idx_;

  // Size of tuple buffer determined by size of tuples and capacity of row batches.
  int tuple_buf_size_;

  // Buffer where tuples are written into.
  // Must be valid until next GetNext().
  void* tuple_buf_;

  // Current tuple.
  Tuple* tuple_;

  // Buffer for data read from file.
  char* file_buf_;

  // Current position in file buffer.
  char* file_buf_ptr_;

  // Ending position of file buffer.
  char* file_buf_end_;

  // Mapping from column index in table to slot index in output tuple.
  // Created in Prepare() from SlotDescriptors.
  std::vector<int> column_idx_to_slot_idx_;

  // Number of partition keys for the files_. Set in Prepare().
  int num_partition_keys_;

  // Flattened list of LiteralExpr* representing the partition keys for each file.
  // key_values_[i*numPartitionKeys+j] is the j-th partition key of the i-th file.
  // Set in Prepare().
  std::vector<Expr*> key_values_;

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

  // Parses the current file_buf_ and writes tuples into the tuple buffer.
  // Input Parameters
  //   state: Runtime state into which we log errors.
  //   row_batch: Row batch into which to write new tuples.
  // Input/Output Parameters:
  //   The following parameters make up the state that must be maintained across file buffers.
  //   These parameters are changed within ParseFileBuffer.
  //   row_batch: Row batch into which to add tuples.
  //   row_idx: Index of current row. Possibly updated within ParseFileBuffer.
  //   column_idx: Index of current field in file.
  //   last_char_is_escape: Indicates whether the last character was an escape character.
  //   unescape_string: Indicates whether the current string-slot contains an escape,
  //                    and must be copied unescaped into the the var_len_buffer.
  //   quote_char: Indicates the last quote character starting a quoted string.
  //               Set to NOT_IN_STRING if string_are_quoted==false,
  //               or we have not encountered a quote.
  //   error_in_row: Indicates whether the current row being parsed has an error.
  //                 Used for counting the number of errors per file.
  //                 Once the error log is full, error_in_row will still be set,
  //                 in order to be able to record the errors per file,
  //                 even though not all details are logged.
  //   num_errors_: Counts the number of errors in the current file.
  //                Used for error reporting.
  // Returns Status::OK if no errors were found,
  // or if errors were found but state->abort_on_error() is false.
  // Returns error message if errors were found and state->abort_on_error() is true.
  Status ParseFileBuffer(RuntimeState* state, RowBatch* row_batch, int* row_idx,
      int* column_idx, bool* last_char_is_escape, bool* unescape_string,
      char* quote_char, bool* error_in_row, int* num_errors);

  // Converts slot data (begin, end) into type of slot_desc,
  // and writes the result into the tuples's slot.
  // copy_string indicates whether we need to make a separate copy of the string data:
  // For regular unescaped strings, we point to the original data in the file_buf_.
  // For regular escaped strings,
  // we copy an its unescaped string into a separate buffer and point to it.
  // For boundary string fields,
  // we create a contiguous copy of the string data into a separate buffer.
  // Unsuccessful conversions are turned into NULLs.
  // Returns true if value was converted and written successfully, false otherwise.
  bool ConvertAndWriteSlotBytes(const char* begin,
      const char* end, Tuple* tuple, const SlotDescriptor* slot_desc,
      bool copy_string, bool unescape_string);

  // Removes escape characters from len characters of the null-terminated string src,
  // and copies the unescaped string into dest, changing *len to the unescaped length.
  // No null-terminator is added to dest.
  void UnescapeString(const char* src, char* dest, int* len);

  // Returns the next valid file buffer for reading HDFS files.
  // We maintain a pseudo ring list of file buffers in file_bufs_.
  // We return file buffers starting from file_bufs_[1],
  // adding new buffers as necessary (allocated from file_buf_pool_).
  // file_bufs_[0] points to the file buffer from a previous call to GetNext().
  // input/output parameter:
  //   file_buf_idx: Index of current file buffer, will be incremented.
  void* GetFileBuffer(RuntimeState* state, int* file_buf_idx);

  const static char NOT_IN_STRING = -1;
  const static int POOL_INIT_SIZE = 4096;
  const static char DELIM_INIT = -1;
  const static int SKIP_COLUMN = -1;
};

}

#endif
