// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_EXEC_HDFS_TEXT_SCANNER_H
#define IMPALA_EXEC_HDFS_TEXT_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "runtime/string-buffer.h"
#include "util/runtime-profile-counters.h"

namespace impala {

template<bool>
class DelimitedTextParser;
class ScannerContext;
struct HdfsFileDesc;

/// HdfsScanner implementation that understands text-formatted records.
/// Uses SSE instructions, if available, for performance.
///
/// Splitting text files:
/// This scanner handles text files split across multiple blocks/scan ranges. Note that
/// the split can occur anywhere in the file, e.g. in the middle of a row. Each scanner
/// starts materializing tuples right after the first row delimiter found in the scan
/// range, and stops at the first row delimiter occurring past the end of the scan
/// range. If no delimiter is found in the scan range, the scanner doesn't materialize
/// anything. This scheme ensures that every row is materialized by exactly one scanner.
///
/// A special case is a "\r\n" row delimiter split across two scan ranges. (When the row
/// delimiter is '\n', we also consider '\r' and "\r\n" row delimiters.) In this case, the
/// delimiter is considered part of the second scan range, i.e., the first scan range's
/// scanner is responsible for the tuple directly before it, and the second scan range's
/// scanner for the tuple directly after it.
class HdfsTextScanner : public HdfsScanner {
 public:
  HdfsTextScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);
  virtual ~HdfsTextScanner();

  /// Implementation of HdfsScanner interface.
  virtual Status Open(ScannerContext* context) override WARN_UNUSED_RESULT;
  virtual void Close(RowBatch* row_batch) override;

  THdfsFileFormat::type file_format() const override {
    return THdfsFileFormat::TEXT;
  }

  /// Issue io manager byte ranges for 'files'.
  static Status IssueInitialRanges(HdfsScanNodeBase* scan_node,
      const std::vector<HdfsFileDesc*>& files) WARN_UNUSED_RESULT;

  /// Codegen WriteAlignedTuples(). Stores the resulting function in
  /// 'write_aligned_tuples_fn' if codegen was successful or nullptr otherwise.
  static Status Codegen(HdfsScanPlanNode* node, FragmentState* state,
      llvm::Function** write_aligned_tuples_fn);

  /// Return true if we have builtin support for scanning text files compressed with this
  /// codec.
  static bool HasBuiltinSupport(THdfsCompression::type compression) {
    switch (compression) {
      case THdfsCompression::NONE:
      case THdfsCompression::GZIP:
      case THdfsCompression::SNAPPY:
      case THdfsCompression::SNAPPY_BLOCKED:
      case THdfsCompression::ZSTD:
      case THdfsCompression::BZIP2:
      case THdfsCompression::DEFLATE:
        return true;
      default:
        return false;
    }
  }

  /// Suffix for lzo index files.
  const static std::string LZO_INDEX_SUFFIX;

  static const char* LLVM_CLASS_NAME;

 protected:
  virtual Status GetNextInternal(RowBatch* row_batch) override WARN_UNUSED_RESULT;

  /// Reset the scanner.  This clears any partial state that needs to
  /// be cleared when starting or when restarting after an error.
  Status ResetScanner() WARN_UNUSED_RESULT;

  /// Current position in byte buffer.
  char* byte_buffer_ptr_;

  /// Ending position of HDFS buffer.
  char* byte_buffer_end_;

  /// Actual bytes received from last file read.
  int64_t byte_buffer_read_size_;

  /// Last character of the last byte buffer filled, i.e. byte_buffer_end_[-1] when the
  /// buffer was last filled with data. Set in FillByteBufferWrapper() and used in
  /// CheckForSplitDelimiter(). Copied out of the byte buffer so that it can be
  /// referenced after the byte buffer is freed or transferred, e.g. in CommitRows().
  /// Valid if 'byte_buffer_filled_' is true.
  char byte_buffer_last_byte_;

  /// True if the byte buffer was filled with at least a byte of data since the last time
  /// the scanner was reset. Set in FillByteBufferWrapper().
  bool byte_buffer_filled_;

  /// True if we are parsing the header for this scanner.
  bool only_parsing_header_;

 private:
  const static int NEXT_BLOCK_READ_SIZE = 64 * 1024; //bytes

  /// The text scanner transitions through these states exactly in order.
  enum TextScanState {
    CONSTRUCTED,
    SCAN_RANGE_INITIALIZED,
    FIRST_TUPLE_FOUND,
    PAST_SCAN_RANGE,
    DONE
  };

  /// Initializes this scanner for this context.  The context maps to a single
  /// scan range. Advances the scan state to SCAN_RANGE_INITIALIZED.
  virtual Status InitNewRange() override WARN_UNUSED_RESULT;

  /// Finds the start of the first tuple in this scan range and initializes
  /// 'byte_buffer_ptr_' to point to the start of first tuple. Advances the scan state
  /// to FIRST_TUPLE_FOUND, if successful. Otherwise, consumes the whole scan range
  /// and does not update the scan state (e.g. if there are really large columns).
  /// Only valid to call in scan state SCAN_RANGE_INITIALIZED.
  Status FindFirstTuple(MemPool* pool) WARN_UNUSED_RESULT;

  /// When in scan state FIRST_TUPLE_FOUND, starts or continues processing the scan range
  /// by reading bytes from 'context_'. Adds materialized tuples that pass the conjuncts
  /// to 'row_batch', and returns when 'row_batch' is at capacity.
  /// When in scan state PAST_SCAN_RANGE, this function returns after parsing one tuple,
  /// regardless of whether it passed the conjuncts.
  /// *num_tuples returns the total number of tuples parsed, including tuples that did
  /// not pass conjuncts.
  /// Advances the scan state to PAST_SCAN_RANGE if all bytes in the scan range have been
  /// processed.
  /// Only valid to call in scan state FIRST_TUPLE_FOUND or PAST_SCAN_RANGE.
  Status ProcessRange(RowBatch* row_batch, int* num_tuples) WARN_UNUSED_RESULT;

  /// Reads past the end of the scan range for the next tuple end. If successful,
  /// advances the scan state to DONE. Only valid to call in state PAST_SCAN_RANGE.
  Status FinishScanRange(RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Wrapper around FillByteBuffer() that also updates 'byte_buffer_last_byte_'
  /// and 'byte_buffer_filled_'. Callers should call this instead of calling
  /// FillByteBuffer() directly.
  /// TODO: IMPALA-6146: this is a workaround that could be removed if FillByteBuffer()
  /// was a cleaner interface.
  Status FillByteBufferWrapper(MemPool* pool, bool* eosr, int num_bytes = 0)
      WARN_UNUSED_RESULT;

  /// Fills the next byte buffer from the context.  This will block if there are no bytes
  /// ready.  Updates byte_buffer_ptr_, byte_buffer_end_ and byte_buffer_read_size_.
  /// If num_bytes is 0, the scanner will read whatever is the io mgr buffer size,
  /// otherwise it will just read num_bytes. If we are reading compressed text, num_bytes
  /// must be 0. Internally, calls the appropriate streaming or non-streaming
  /// decompression functions DecompressFileToBuffer()/DecompressStreamToBuffer().
  /// If applicable, attaches decompression buffers from previous calls that might still
  /// be referenced by returned batches to 'pool'. If 'pool' is nullptr the buffers are
  /// freed instead.
  ///
  /// Subclasses can override this function to implement different behaviour.
  /// TODO: IMPALA-6146: rethink this interface - having subclasses modify member
  /// variables is brittle.
  virtual Status FillByteBuffer(MemPool* pool, bool* eosr, int num_bytes = 0)
      WARN_UNUSED_RESULT;

  /// Checks if the current buffer ends with a row delimiter spanning this and the next
  /// buffer (i.e. a "\r\n" delimiter). Does not modify byte_buffer_ptr_, etc. Always
  /// returns false if the table's row delimiter is not '\n'. This can only be called
  /// after the buffer has been fully parsed, i.e. when byte_buffer_ptr_ ==
  /// byte_buffer_end_.
  Status CheckForSplitDelimiter(bool* split_delimiter) WARN_UNUSED_RESULT;

  /// Prepends field data that was from the previous file buffer (This field straddled two
  /// file buffers). 'data' already contains the pointer/len from the current file buffer,
  /// boundary_column_ contains the beginning of the data from the previous file buffer.
  /// This function will allocate a new string from the tuple pool, concatenate the
  /// two pieces and update 'data' to contain the new pointer/len. Return error status if
  /// memory limit is exceeded when allocating a new string.
  Status CopyBoundaryField(FieldLocation* data, MemPool* pool) WARN_UNUSED_RESULT;

  /// Writes intermediate parsed data into 'tuple_', evaluates conjuncts, and appends
  /// surviving rows to 'row'. Advances 'tuple_' and 'row' as necessary.
  /// Input Parameters:
  ///  num_fields: Total number of fields contained in parsed_data_
  ///  num_tuples: Number of tuples in parsed_data_. This includes the potential
  ///    partial tuple at the beginning of 'field_locations_'.
  ///  pool: MemPool to allocate from for field data
  /// Returns the number of rows added to the row batch.
  int WriteFields(int num_fields, int num_tuples, MemPool* pool, TupleRow* row);

  /// Utility function to parse 'num_fields' and materialize the resulting slots into
  /// 'partial_tuple_'.  The data of var-len fields is copied into 'boundary_pool_'.
  void WritePartialTuple(FieldLocation*, int num_fields);

  /// Deep copies the partial tuple into 'tuple_'.  The deep copy is done to simplify
  /// memory ownership.  Also clears the boundary pool to prevent the accumulation of
  /// variable length data in it.
  void CopyAndClearPartialTuple(MemPool* pool);

  /// Current state of this scanner.  Advances through the states exactly in order.
  TextScanState scan_state_;

  /// Mem pool for boundary_row_, boundary_column_, partial_tuple_ and any variable length
  /// data that is pointed at by the partial tuple.  Does not hold any tuple data
  /// of returned batches, because the data is always deep-copied into the output batch.
  boost::scoped_ptr<MemPool> boundary_pool_;

  /// Helper string for dealing with input rows that span file blocks.  We keep track of
  /// a whole line that spans file blocks to be able to report the line as erroneous in
  /// case of parsing errors.  Does not hold any tuple data of returned batches.
  StringBuffer boundary_row_;

  /// Helper string for dealing with columns that span file blocks.  Does not hold any
  /// tuple data of returned batches, because the data is always deep-copied into the
  /// output batch.  Memory comes from boundary_pool_.
  StringBuffer boundary_column_;

  /// Index into materialized_slots_ for the next slot to output for the current tuple.
  int slot_idx_;

  /// Helper class for picking fields and rows from delimited text.
  boost::scoped_ptr<DelimitedTextParser<true>> delimited_text_parser_;

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

  /// Memory to store partial tuples split across buffers.  Does not hold any tuple data
  /// of returned batches, because the data is always deep-copied into the output batch.
  /// Memory comes from boundary_pool_.
  Tuple* partial_tuple_;

  /// Time parsing text files
  RuntimeProfile::Counter* parse_delimiter_timer_;
};

}

#endif
