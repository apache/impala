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

#ifndef IMPALA_EXEC_HDFS_JSON_SCANNER_H
#define IMPALA_EXEC_HDFS_JSON_SCANNER_H

#include <memory>

#include "common/status.h"
#include "exec/json/json-parser.h"
#include "exec/text/hdfs-text-scanner.h"
#include "runtime/mem-pool.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"


namespace impala {

class ScannerContext;
struct HdfsFileDesc;

/// HdfsScanner implementation that understands json-formatted records.
///
/// Splitting json files:
/// Similar to HdfsTextScanner, this scanner handles json files split across multiple
/// blocks/scan ranges. Note that the split can occur anywhere in the file, e.g. in the
/// middle of a row. Each scanner starts materializing tuples right after the first row
/// delimiter found in the scan range, and stops at the first row delimiter occurring past
/// the end of the scan range. If no delimiter is found in the scan range, the scanner
/// doesn't materialize anything. This scheme ensures that every row is materialized by
/// exactly one scanner.
///
/// Error handling:
/// During the process of scanning JSON, there are two types of errors may occur. The
/// first type is data conversion errors, such as attempting to convert a non-numeric
/// string to a number. These errors are detected and reported by TextConverter, and
/// handled by HdfsJsonScanner::HandleConvertError(), it will set the slot to NULL,
/// and record the error situation. If abort_on_error is true, it also returns false to
/// the event handling function of JsonParser, causing the JsonParser to interrupt parsing
/// and report the kParseErrorTermination error code. The second type of error occurs when
/// the JSON itself has formatting errors, such as missing colons or commas or including
/// invalid values. These errors are detected and reported as corresponding error codes by
/// JsonParser, and they are handled by HdfsJsonScanner::HandleError(). It will record the
/// error situation, and if abort_on_error is true, also returns an error status to
/// JsonParser, causing the query to be aborted.
class HdfsJsonScanner : public HdfsScanner {
 public:
  HdfsJsonScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);
  virtual ~HdfsJsonScanner();

  /// Implementation of HdfsScanner interface.
  virtual Status Open(ScannerContext* context) override WARN_UNUSED_RESULT;
  virtual void Close(RowBatch* row_batch) override;

  THdfsFileFormat::type file_format() const override {
    return THdfsFileFormat::JSON;
  }

  /// Issue io manager byte ranges for 'files'.
  static Status IssueInitialRanges(HdfsScanNodeBase* scan_node,
      const std::vector<HdfsFileDesc*>& files) WARN_UNUSED_RESULT {
    return HdfsTextScanner::IssueInitialRanges(scan_node, files);
  }

  /// Return true if we have builtin support for scanning text files compressed with this
  /// codec.
  static bool HasBuiltinSupport(THdfsCompression::type compression) {
    return HdfsTextScanner::HasBuiltinSupport(compression);
  }

 private:
  friend class JsonParser<HdfsJsonScanner>;

  virtual Status InitNewRange() override WARN_UNUSED_RESULT;

  virtual Status GetNextInternal(RowBatch* row_batch) override WARN_UNUSED_RESULT;

  /// Find the start of the first tuple in this scan range. If successful, advances the
  /// scanner state to SCANNING. Otherwise, consume the entire scan range without updating
  /// the scanner state (e.g. if there is a very large JSON object).
  Status FindFirstTuple() WARN_UNUSED_RESULT;

  /// A wrapper around JsonParser::Parse() that checks for buffer errors before returning
  /// its Status. If there are any buffer errors, they will be returned instead of the
  /// Status from JsonParser::Parse(). Because GetNextBuffer() is called as a callback,
  /// we need this approach to check for buffer errors.
  Status ParseWrapper(int max_tuples, int* num_tuples) WARN_UNUSED_RESULT;

  /// Called when adding a value to the tuple fails. Sets the target slot to null and
  /// reports the error message. Returns false if necessary to abort the scan.
  bool HandleConvertError(const SlotDescriptor* desc, const char* data, int len);

  /// Fills bytes to buffer from the context. If 'scanner_state_' is PAST_SCANNING, the
  /// scanner will read a small block data, otherwise it will just read whatever is the
  /// io mgr buffer size.
  Status FillBytesToBuffer(uint8_t** buffer, int64_t* bytes_read) WARN_UNUSED_RESULT;

  /// Scanner state, advances as the scanning process progresses.
  enum ScannerState {
    CREATED,

    /// Scanner is opened, ready to work.
    OPENED,

    /// Enter this state after finding the position of the first tuple in ScanRange,
    /// indicating that the scanner is scanning data normally.
    SCANNING,

    /// Indicates that the scan range has been scanned, but the first row of data past the
    /// end of the scan range still needs to be read for parsing.
    PAST_SCANNING,

    /// Scanning is finished, no more data to process.
    FINISHED
  } scanner_state_;

  /// The returned status when fetching data from stream. Set in GetNextBuffer() and
  /// checked in ParseWrapper().
  Status buffer_status_;

  /// TupleRow pointer of current row batch, set in GetNextInternal().
  TupleRow* tuple_row_;

  /// MemPool pointer of current row batch, set in GetNextInternal().
  MemPool* current_pool_;

  /// This is used to indicate whether an error has occurred in the currently parsed row.
  bool error_in_row_;

  /// A counter for the number of tuples materialized during a single ParseWrapper() call.
  int num_tuples_materialized_;

  /// Time JsonParser invoking, this roughly includes the time for parsing the JSON,
  /// materializing the tuple, and reading the data.
  RuntimeProfile::Counter* parse_json_timer_;

  /// Time get next buffer in GetNextBuffer().
  RuntimeProfile::Counter* get_buffer_timer_;

  const static int NEXT_BLOCK_READ_SIZE = 64 * 1024; //bytes

  /// JsonParser is a class template that implements parsing of JSON data stream. It is
  /// supplied with a data buffer by its template parameter Scanner, and some callback
  /// functions for assembling the parsing results into row format, see details in the
  /// JsonParse comment.
  std::unique_ptr<JsonParser<HdfsJsonScanner>> json_parser_;

  /// Invoke WriteSlot (CodeGen or Interpret version) to materialize the slot, and handle
  /// errors when conversion fails.
  inline bool InvokeWriteSlot(const SlotDescriptor* slot_desc, const char* data, int len);

  /// All functions below are callback functions provided for JsonParse, with their
  /// specific uses described in the JsonParse comment.
  Status HandleError(rapidjson::ParseErrorCode error, size_t offset);
  bool BreakParse() { return scanner_state_ == PAST_SCANNING; }
  void GetNextBuffer(const char** begin, const char** end);
  void InitRow();
  void SubmitRow();
  void AddNull(int index);
  bool AddBool(int index, bool value);
  bool AddString(int index, const char* str, uint32_t len);
  bool AddNumber(int index, const char* str, uint32_t len);
};

}

#endif
