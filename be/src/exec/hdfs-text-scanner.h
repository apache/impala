// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXEC_HDFS_TEXT_SCANNER_H
#define IMPALA_EXEC_HDFS_TEXT_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "runtime/string-buffer.h"

namespace impala {

class DelimitedTextParser;
class ScannerContext;
struct HdfsFileDesc;

/// HdfsScanner implementation that understands text-formatted records.
/// Uses SSE instructions, if available, for performance.
class HdfsTextScanner : public HdfsScanner {
 public:
  HdfsTextScanner(HdfsScanNode* scan_node, RuntimeState* state);
  virtual ~HdfsTextScanner();

  /// Implementation of HdfsScanner interface.
  virtual Status Prepare(ScannerContext* context);
  virtual Status ProcessSplit();
  virtual void Close();

  /// Issue io manager byte ranges for 'files'.
  static Status IssueInitialRanges(HdfsScanNode* scan_node,
                                   const std::vector<HdfsFileDesc*>& files);

  /// Codegen writing tuples and evaluating predicates.
  static llvm::Function* Codegen(HdfsScanNode*,
                                 const std::vector<ExprContext*>& conjunct_ctxs);

  /// Suffix for lzo index files.
  const static std::string LZO_INDEX_SUFFIX;

  static const char* LLVM_CLASS_NAME;

 protected:
  /// Reset the scanner.  This clears any partial state that needs to
  /// be cleared when starting or when restarting after an error.
  Status ResetScanner();

  /// Current position in byte buffer.
  char* byte_buffer_ptr_;

  /// Ending position of HDFS buffer.
  char* byte_buffer_end_;

  /// Actual bytes received from last file read.
  int64_t byte_buffer_read_size_;

  /// True if we are parsing the header for this scanner.
  bool only_parsing_header_;

 private:
  const static int NEXT_BLOCK_READ_SIZE = 1024; //bytes

  /// Initializes this scanner for this context.  The context maps to a single
  /// scan range.
  virtual Status InitNewRange();

  /// Finds the start of the first tuple in this scan range and initializes
  /// byte_buffer_ptr to be the next character (the start of the first tuple).  If
  /// there are no tuples starts in the entire range, *tuple_found is set to false
  /// and no more processing neesd to be done in this range (i.e. there are really large
  /// columns)
  Status FindFirstTuple(bool* tuple_found);

  /// Process the entire scan range, reading bytes from context and appending
  /// materialized row batches to the scan node.  *num_tuples returns the
  /// number of tuples parsed.  past_scan_range is true if this is processing
  /// beyond the end of the scan range and this function should stop after
  /// finding one tuple.
  Status ProcessRange(int* num_tuples, bool past_scan_range);

  /// Reads past the end of the scan range for the next tuple end.
  Status FinishScanRange();

  /// Fills the next byte buffer from the context.  This will block if there are no bytes
  /// ready.  Updates byte_buffer_ptr_, byte_buffer_end_ and byte_buffer_read_size_.
  /// If num_bytes is 0, the scanner will read whatever is the io mgr buffer size,
  /// otherwise it will just read num_bytes. If we are reading compressed text, num_bytes
  /// must be 0.
  virtual Status FillByteBuffer(bool* eosr, int num_bytes = 0);

  /// Fills the next byte buffer from the compressed data in stream_ by reading the entire
  /// file, decompressing it, and setting the byte_buffer_ptr_ to the decompressed buffer.
  Status FillByteBufferCompressedFile(bool* eosr);

  /// Fills the next byte buffer from the compressed data in stream_. Unlike
  /// FillByteBufferCompressedFile(), the entire file does not need to be read at once.
  /// Buffers from stream_ are decompressed as they are read and byte_buffer_ptr_ is set
  /// to available decompressed data.
  Status FillByteBufferCompressedStream(bool* eosr);

  /// Used by FillByteBufferCompressedStream() to decompress data from 'stream_'.
  /// Returns COMPRESSED_FILE_DECOMPRESSOR_NO_PROGRESS if it needs more input.
  /// If bytes_to_read > 0, will read specified size.
  /// If bytes_to_read = -1, will call GetBuffer().
  Status DecompressBufferStream(int64_t bytes_to_read, uint8_t** decompressed_buffer,
      int64_t* decompressed_len, bool *eosr);

  /// Prepends field data that was from the previous file buffer (This field straddled two
  /// file buffers).  'data' already contains the pointer/len from the current file buffer,
  /// boundary_column_ contains the beginning of the data from the previous file
  /// buffer. This function will allocate a new string from the tuple pool, concatenate the
  /// two pieces and update 'data' to contain the new pointer/len.
  void CopyBoundaryField(FieldLocation* data, MemPool* pool);

  /// Writes the intermediate parsed data into slots, outputting
  /// tuples to row_batch as they complete.
  /// Input Parameters:
  ///  mempool: MemPool to allocate from for field data
  ///  num_fields: Total number of fields contained in parsed_data_
  ///  num_tuples: Number of tuples in parsed_data_. This includes the potential
  ///    partial tuple at the beginning of 'field_locations_'.
  /// Returns the number of tuples added to the row batch.
  int WriteFields(MemPool*, TupleRow* tuple_row_mem, int num_fields, int num_tuples);

  /// Utility function to write out 'num_fields' to 'tuple_'.  This is used to parse
  /// partial tuples.  Returns bytes processed.  If copy_strings is true, strings
  /// from fields will be copied into the boundary pool.
  int WritePartialTuple(FieldLocation*, int num_fields, bool copy_strings);

  /// Appends the current file and line to the RuntimeState's error log.
  /// row_idx is 0-based (in current batch) where the parse error occured.
  virtual void LogRowParseError(int row_idx, std::stringstream*);

  /// Mem pool for boundary_row_ and boundary_column_.
  boost::scoped_ptr<MemPool> boundary_pool_;

  /// Helper string for dealing with input rows that span file blocks.
  /// We keep track of a whole line that spans file blocks to be able to report
  /// the line as erroneous in case of parsing errors.
  StringBuffer boundary_row_;

  /// Helper string for dealing with columns that span file blocks.
  StringBuffer boundary_column_;

  /// Index into materialized_slots_ for the next slot to output for the current tuple.
  int slot_idx_;

  /// Helper class for picking fields and rows from delimited text.
  boost::scoped_ptr<DelimitedTextParser> delimited_text_parser_;

  /// Return field locations from the Delimited Text Parser.
  std::vector<FieldLocation> field_locations_;

  /// Pointers into 'byte_buffer_' for the end ptr locations for each row
  /// processed in the current batch.  Used to report row errors.
  std::vector<char*> row_end_locations_;

  /// Pointer into byte_buffer that is the start of the current batch being
  /// processed.
  char* batch_start_ptr_;

  /// Whether or not there was a parse error in the current row. Used for counting the
  /// number of errors per file.  Once the error log is full, error_in_row will still be
  /// set, in order to be able to record the errors per file, even if the details are not
  /// logged.
  bool error_in_row_;

  /// Memory to store partial tuples split across buffers.  Memory comes from
  /// boundary_pool_.  There is only one tuple allocated for this object and reused
  /// for boundary tuples.
  Tuple* partial_tuple_;

  /// If false, there is a tuple that is partially materialized (i.e. partial_tuple_
  /// contains data)
  bool partial_tuple_empty_;

  /// Time parsing text files
  RuntimeProfile::Counter* parse_delimiter_timer_;
};

}

#endif
