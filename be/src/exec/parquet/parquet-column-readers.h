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

#ifndef IMPALA_PARQUET_COLUMN_READERS_H
#define IMPALA_PARQUET_COLUMN_READERS_H

#include <boost/scoped_ptr.hpp>

#include "exec/parquet/hdfs-parquet-scanner.h"
#include "exec/parquet/parquet-level-decoder.h"
#include "runtime/io/request-ranges.h"
#include "util/bit-stream-utils.h"
#include "util/codec.h"

namespace impala {

class DictDecoderBase;
class Tuple;
class MemPool;

/// Base class for reading a Parquet column. Reads a logical column, not necessarily a
/// column materialized in the file (e.g. collections). The two subclasses are
/// BaseScalarColumnReader and CollectionColumnReader. Column readers read one def and rep
/// level pair at a time. The current def and rep level are exposed to the user, and the
/// corresponding value (if defined) can optionally be copied into a slot via
/// ReadValue(). Can also write position slots.
///
/// The constructor adds the object to the obj_pool of the parent HdfsParquetScanner.
class ParquetColumnReader {
 public:
  /// Creates a column reader for 'node' and associates it with the given parent scanner.
  /// The constructor of column readers add the new object to the parent's object pool.
  /// 'slot_desc' may be NULL, in which case the returned column reader can only be used
  /// to read def/rep levels.
  /// 'is_collection_field' should be set to true if the returned reader is reading a
  /// collection. This cannot be determined purely by 'node' because a repeated scalar
  /// node represents both an array and the array's items (in this case
  /// 'is_collection_field' should be true if the reader reads one value per array, and
  /// false if it reads one value per item).  The reader is added to the runtime state's
  /// object pool. Does not create child readers for collection readers; these must be
  /// added by the caller.
  ///
  /// It supports the following primitive type widening that does not have any loss of
  /// precision.
  /// - tinyint (INT32) -> smallint (INT32), int (INT32), bigint (INT64), double (DOUBLE)
  /// - smallint (INT32) -> int (INT32), bigint (INT64), double (DOUBLE)
  /// - int (INT32) -> bigint (INT64), double (DOUBLE)
  /// - float (FLOAT) -> double (DOUBLE)
  static ParquetColumnReader* Create(const SchemaNode& node, bool is_collection_field,
      const SlotDescriptor* slot_desc, HdfsParquetScanner* parent);

  static ParquetColumnReader* CreateTimestampColumnReader(const SchemaNode& node,
      const SlotDescriptor* slot_desc, HdfsParquetScanner* parent);

  virtual ~ParquetColumnReader() { }

  int def_level() const { return def_level_; }
  int rep_level() const { return rep_level_; }

  const SlotDescriptor* slot_desc() const { return slot_desc_; }
  const parquet::SchemaElement& schema_element() const { return *node_.element; }
  int16_t max_def_level() const { return max_def_level_; }
  int16_t max_rep_level() const { return max_rep_level_; }
  int def_level_of_immediate_repeated_ancestor() const {
    return node_.def_level_of_immediate_repeated_ancestor;
  }
  const SlotDescriptor* pos_slot_desc() const { return pos_slot_desc_; }
  void set_pos_slot_desc(const SlotDescriptor* pos_slot_desc) {
    DCHECK(pos_slot_desc_ == NULL);
    pos_slot_desc_ = pos_slot_desc;
  }

  /// Returns true if this reader materializes collections (i.e. CollectionValues).
  virtual bool IsCollectionReader() const = 0;

  const char* filename() const { return parent_->filename(); }

  /// Read the current value (or null) into 'tuple' for this column. This should only be
  /// called when a value is defined, i.e., def_level() >=
  /// def_level_of_immediate_repeated_ancestor() (since empty or NULL collections produce
  /// no output values), otherwise NextLevels() should be called instead.
  ///
  /// Advances this column reader to the next value (i.e. NextLevels() doesn't need to be
  /// called after calling ReadValue()).
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  ///
  /// NextLevels() must be called on this reader before calling ReadValue() for the first
  /// time. This is to initialize the current value that ReadValue() will read.
  ///
  /// TODO: this is the function that needs to be codegen'd (e.g. CodegenReadValue())
  /// The codegened functions from all the materialized cols will then be combined
  /// into one function.
  /// TODO: another option is to materialize col by col for the entire row batch in
  /// one call.  e.g. MaterializeCol would write out 1024 values.  Our row batches
  /// are currently dense so we'll need to figure out something there.
  virtual bool ReadValue(MemPool* pool, Tuple* tuple) = 0;

  /// Same as ReadValue() but does not advance repetition level. Only valid for columns
  /// not in collections.
  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) = 0;

  /// Batched version of ReadValue() that reads up to max_values at once and materializes
  /// them into tuples in tuple_mem. Returns the number of values actually materialized
  /// in *num_values. The return value, error behavior and state changes are generally
  /// the same as in ReadValue(). For example, if an error occurs in the middle of
  /// materializing a batch then false is returned, and num_values, tuple_mem, as well as
  /// this column reader are left in an undefined state, assuming that the caller will
  /// immediately abort execution. NextLevels() does *not* need to be called before
  /// ReadValueBatch(), unlike ReadValue().
  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) = 0;

  /// Batched version of ReadNonRepeatedValue() that reads up to max_values at once and
  /// materializes them into tuples in tuple_mem.
  /// The return value and error behavior are the same as in ReadValueBatch().
  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) = 0;

  /// Advances this column reader's def and rep levels to the next logical value, i.e. to
  /// the next scalar value or the beginning of the next collection, without attempting to
  /// read the value. This is used to skip past def/rep levels that don't materialize a
  /// value, such as the def/rep levels corresponding to an empty containing collection.
  ///
  /// NextLevels() must be called on this reader before calling ReadValue() for the first
  /// time. This is to initialize the current value that ReadValue() will read.
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  virtual bool NextLevels() = 0;

  /// Writes pos_current_value_ (i.e. "reads" the synthetic position field of the
  /// parent collection) to 'pos' and increments pos_current_value_. Only valid to
  /// call when doing non-batched reading, i.e. NextLevels() must have been called
  /// before each call to this function to advance to the next element in the
  /// collection.
  inline void ReadPositionNonBatched(int64_t* pos);

  /// Returns true if this column reader has reached the end of the row group.
  inline bool RowGroupAtEnd() {
    DCHECK_EQ(rep_level_ == ParquetLevel::ROW_GROUP_END,
        def_level_ == ParquetLevel::ROW_GROUP_END);
    return rep_level_ == ParquetLevel::ROW_GROUP_END;
  }

  /// If 'row_batch' is non-NULL, transfers the remaining resources backing tuples to it,
  /// and frees up other resources. If 'row_batch' is NULL frees all resources instead.
  virtual void Close(RowBatch* row_batch) = 0;

 protected:
  HdfsParquetScanner* parent_;
  const SchemaNode& node_;
  const SlotDescriptor* const slot_desc_;

  /// The slot descriptor for the position field of the tuple, if there is one. NULL if
  /// there's not. Only one column reader for a given tuple desc will have this set.
  const SlotDescriptor* pos_slot_desc_;

  /// The next value to write into the position slot, if there is one. 64-bit int because
  /// the pos slot is always a BIGINT Set to ParquetLevel::INVALID_POS when this column
  /// reader does not have a current rep and def level (i.e. before the first NextLevels()
  /// call or after the last value in the column has been read).
  int64_t pos_current_value_;

  /// The current repetition and definition levels of this reader. Advanced via
  /// ReadValue() and NextLevels(). Set to ParquetLevel:: INVALID_LEVEL before the first
  /// NextLevels() call for a row group or if an error is encountered decoding a level.
  /// Set to ROW_GROUP_END after the last value in the column has been read). If this is
  /// not inside a collection, rep_level_ is always 0, ParquetLevel::INVALID_LEVEL or
  /// ParquetLevel::ROW_GROUP_END.
  /// int16_t is large enough to hold the valid levels 0-255 and negative sentinel values
  /// ParquetLevel::INVALID_LEVEL and ParquetLevel::ROW_GROUP_END. The maximum values are
  /// cached here because they are accessed in inner loops.
  int16_t rep_level_;
  const int16_t max_rep_level_;
  int16_t def_level_;
  const int16_t max_def_level_;

  // Cache frequently accessed members of slot_desc_ for perf.

  /// slot_desc_->tuple_offset(). -1 if slot_desc_ is NULL.
  const int tuple_offset_;

  /// slot_desc_->null_indicator_offset(). Invalid if slot_desc_ is NULL.
  const NullIndicatorOffset null_indicator_offset_;

  ParquetColumnReader(
      HdfsParquetScanner* parent, const SchemaNode& node, const SlotDescriptor* slot_desc)
    : parent_(parent),
      node_(node),
      slot_desc_(slot_desc),
      pos_slot_desc_(NULL),
      pos_current_value_(ParquetLevel::INVALID_POS),
      rep_level_(ParquetLevel::INVALID_LEVEL),
      max_rep_level_(node_.max_rep_level),
      def_level_(ParquetLevel::INVALID_LEVEL),
      max_def_level_(node_.max_def_level),
      tuple_offset_(slot_desc == NULL ? -1 : slot_desc->tuple_offset()),
      null_indicator_offset_(slot_desc == NULL ? NullIndicatorOffset() :
                                                 slot_desc->null_indicator_offset()) {
    DCHECK(parent != nullptr);
    parent->obj_pool_.Add(this);

    DCHECK_GE(node_.max_rep_level, 0);
    DCHECK_LE(node_.max_rep_level, std::numeric_limits<int16_t>::max());
    DCHECK_GE(node_.max_def_level, 0);
    DCHECK_LE(node_.max_def_level, std::numeric_limits<int16_t>::max());
    // rep_level_ is always valid and equal to 0 if col not in collection.
    if (max_rep_level() == 0) rep_level_ = 0;
  }

  /// Called in the middle of creating a scratch tuple batch to simulate failures
  /// such as exceeding memory limit or cancellation. Returns false if the debug
  /// action deems that the parquet column reader should halt execution. 'val_count'
  /// is the counter which the column reader uses to track the number of tuples
  /// produced so far. If the column reader should halt execution, 'parse_status_'
  /// is updated with the error status and 'val_count' is set to 0.
  inline bool ColReaderDebugAction(int* val_count);
};

/// Reader for a single column from the parquet file.  It's associated with a
/// ScannerContext::Stream and is responsible for decoding the data.  Super class for
/// per-type column readers. This contains most of the logic, the type specific functions
/// must be implemented in the subclass.
class BaseScalarColumnReader : public ParquetColumnReader {
 public:
  BaseScalarColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : ParquetColumnReader(parent, node, slot_desc),
      data_page_pool_(new MemPool(parent->scan_node_->mem_tracker())) {
    DCHECK_GE(node_.col_idx, 0) << node_.DebugString();
  }

  virtual ~BaseScalarColumnReader() { }

  virtual bool IsCollectionReader() const { return false; }

  /// Resets the reader for each row group in the file and creates the scan
  /// range for the column, but does not start it. To start scanning,
  /// set_io_reservation() must be called to assign reservation to this
  /// column, followed by StartScan().
  Status Reset(const HdfsFileDesc& file_desc, const parquet::ColumnChunk& col_chunk,
    int row_group_idx);

  /// Starts the column scan range. The reader must be Reset() and have a
  /// reservation assigned via set_io_reservation(). This must be called
  /// before any of the column data can be read (including dictionary and
  /// data pages). Returns an error status if there was an error starting the
  /// scan or allocating buffers for it.
  Status StartScan();

  /// Helper to start scans for multiple columns at once.
  static Status StartScans(const std::vector<BaseScalarColumnReader*> readers) {
    for (BaseScalarColumnReader* reader : readers) RETURN_IF_ERROR(reader->StartScan());
    return Status::OK();
  }

  virtual void Close(RowBatch* row_batch);

  io::ScanRange* scan_range() const { return scan_range_; }
  int64_t total_len() const { return metadata_->total_compressed_size; }
  int col_idx() const { return node_.col_idx; }
  THdfsCompression::type codec() const {
    if (metadata_ == NULL) return THdfsCompression::NONE;
    return ConvertParquetToImpalaCodec(metadata_->codec);
  }
  void set_io_reservation(int bytes) { io_reservation_ = bytes; }

  /// Reads the next definition and repetition levels for this column. Initializes the
  /// next data page if necessary.
  virtual bool NextLevels() { return NextLevels<true>(); }

  /// Check the data stream to see if there is a dictionary page. If there is,
  /// use that page to initialize dict_decoder_ and advance the data stream
  /// past the dictionary page.
  Status InitDictionary();

  /// Convenience function to initialize multiple dictionaries.
  static Status InitDictionaries(const std::vector<BaseScalarColumnReader*> readers);

  // Returns the dictionary or NULL if the dictionary doesn't exist
  virtual DictDecoderBase* GetDictionaryDecoder() { return nullptr; }

  // Returns whether the datatype for this column requires conversion from the on-disk
  // format for correctness. For example, timestamps can require an offset to be
  // applied.
  virtual bool NeedsConversion() { return false; }

  // Returns whether the datatype for this column requires validation. For example,
  // the timestamp format has certain bit combinations that are invalid, and these
  // need to be validated when read from disk.
  virtual bool NeedsValidation() { return false; }

  // TODO: Some encodings might benefit a lot from a SkipValues(int num_rows) if
  // we know this row can be skipped. This could be very useful with stats and big
  // sections can be skipped. Implement that when we can benefit from it.

 protected:
  // Friend parent scanner so it can perform validation (e.g. ValidateEndOfRowGroup())
  friend class HdfsParquetScanner;

  // Class members that are accessed for every column should be included up here so they
  // fit in as few cache lines as possible.

  /// Pointer to start of next value in data page
  uint8_t* data_ = nullptr;

  /// End of the data page.
  const uint8_t* data_end_ = nullptr;

  /// Decoder for definition levels.
  ParquetLevelDecoder def_levels_{true};

  /// Decoder for repetition levels.
  ParquetLevelDecoder rep_levels_{false};

  /// Page encoding for values of the current data page. Cached here for perf. Set in
  /// InitDataPage().
  parquet::Encoding::type page_encoding_ = parquet::Encoding::PLAIN_DICTIONARY;

  /// Num values remaining in the current data page
  int num_buffered_values_ = 0;

  // Less frequently used members that are not accessed in inner loop should go below
  // here so they do not occupy precious cache line space.

  /// The number of values seen so far. Updated per data page. It is only used for
  /// validation. It is not used when we filter rows based on the page index.
  int64_t num_values_read_ = 0;

  /// Metadata for the column for the current row group.
  const parquet::ColumnMetaData* metadata_ = nullptr;

  boost::scoped_ptr<Codec> decompressor_;

  /// The scan range for the column's data. Initialized for each row group by Reset().
  io::ScanRange* scan_range_ = nullptr;

  // Stream used to read data from 'scan_range_'. Initialized by StartScan().
  ScannerContext::Stream* stream_ = nullptr;

  /// Reservation in bytes to use for I/O buffers in 'scan_range_'/'stream_'. Must be set
  /// with set_io_reservation() before 'stream_' is initialized. Reset for each row group
  /// by Reset().
  int64_t io_reservation_ = 0;

  /// Pool to allocate storage for data pages from - either decompression buffers for
  /// compressed data pages or copies of the data page with var-len data to attach to
  /// batches.
  boost::scoped_ptr<MemPool> data_page_pool_;

  /// Header for current data page.
  parquet::PageHeader current_page_header_;

  /////////////////////////////////////////
  /// BEGIN: Members used for page filtering
  /// They are not set when we don't filter out pages at all.

  /// The parquet OffsetIndex of this column chunk. It stores information about the page
  /// locations and row indexes.
  parquet::OffsetIndex offset_index_;

  /// Collection of page indexes that we are going to read. When we use page filtering,
  /// we issue a scan-range with sub-ranges that belong to the candidate data pages, i.e.
  /// we will not even see the bytes of the filtered out pages.
  /// It is set in HdfsParquetScanner::CalculateCandidatePagesForColumns().
  std::vector<int> candidate_data_pages_;

  /// Stores an index to 'candidate_data_pages_'. It is the currently read data page when
  /// we have candidate pages.
  int candidate_page_idx_ = -1;

  /// Stores an index to 'parent_->candidate_ranges_'. When we have candidate pages, we
  /// are processing values in this range. When we leave this range, then we need to skip
  /// rows and increment this field.
  int current_row_range_ = 0;

  /// Index of the current top-level row. It is updated together with the rep/def levels.
  /// When updated, and its value is N, it means that we already processed the Nth row
  /// completely.
  int64_t current_row_ = -1;

  /// This flag is needed for the proper tracking of the last processed row.
  /// The batched and non-batched interfaces behave differently. E.g. when using the
  /// batched interface you don't need to invoke NextLevels() in advance, while you need
  /// to do that for the non-batched interface. In fact, the batched interface doesn't
  /// call NextLevels() at all. It directly reads the levels then the corresponding value
  /// in a loop. On the other hand, the non-batched interface (ReadValue()) expects that
  /// the levels for the next value are already read via NextLevels(). And after reading
  /// the value it calls NextLevels() to read the levels of the next value. Hence, the
  /// levels are always read ahead in this case.
  /// Returns true, if we read ahead def and rep levels. In this case 'current_row_'
  /// points to the row we'll process next, not to the row we already processed.
  bool levels_readahead_ = false;

  /// END: Members used for page filtering
  /////////////////////////////////////////

  /// Reads the next page header into next_page_header/next_header_size.
  /// If the stream reaches the end before reading a complete page header,
  /// eos is set to true. If peek is false, the stream position is advanced
  /// past the page header. If peek is true, the stream position is not moved.
  /// Returns an error status if the next page header could not be read.
  Status ReadPageHeader(bool peek, parquet::PageHeader* next_page_header,
      uint32_t* next_header_size, bool* eos);

  /// Read the next data page. If a dictionary page is encountered, that will be read and
  /// this function will continue reading the next data page.
  Status ReadDataPage();

  /// Try to move the the next page and buffer more values. Return false and
  /// sets rep_level_, def_level_ and pos_current_value_ to -1 if no more pages or an
  /// error encountered.
  bool NextPage();

  /// Implementation for NextLevels().
  template <bool ADVANCE_REP_LEVEL>
  bool NextLevels();

  /// Creates a dictionary decoder from values/size. 'decoder' is set to point to a
  /// dictionary decoder stored in this object. Subclass must implement this. Returns
  /// an error status if the dictionary values could not be decoded successfully.
  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) = 0;

  /// Return true if the column has a dictionary decoder. Subclass must implement this.
  virtual bool HasDictionaryDecoder() = 0;

  /// Clear the dictionary decoder so HasDictionaryDecoder() will return false. Subclass
  /// must implement this.
  virtual void ClearDictionaryDecoder() = 0;

  /// Initializes the reader with the data contents. This is the content for the entire
  /// decompressed data page. Decoders can initialize state from here. The caller must
  /// validate the input such that 'size' is non-negative and that 'data' has at least
  /// 'size' bytes remaining.
  virtual Status InitDataPage(uint8_t* data, int size) = 0;

  /// Allocate memory for the uncompressed contents of a data page of 'size' bytes from
  /// 'data_page_pool_'. 'err_ctx' provides context for error messages. On success,
  /// 'buffer' points to the allocated memory. Otherwise an error status is returned.
  Status AllocateUncompressedDataPage(
      int64_t size, const char* err_ctx, uint8_t** buffer);

  /// Returns true if a data page for this column with the specified 'encoding' may
  /// contain strings referenced by returned batches. Cases where this is not true are:
  /// * Dictionary-compressed pages, where any string data lives in 'dictionary_pool_'.
  /// * Fixed-length slots, where there is no string data.
  bool PageContainsTupleData(parquet::Encoding::type page_encoding) {
    return page_encoding != parquet::Encoding::PLAIN_DICTIONARY
        && slot_desc_ != nullptr && slot_desc_->type().IsVarLenStringType();
  }

  /// Resets structures related to page filtering.
  void ResetPageFiltering();

  /// Must be invoked when starting a new page. Updates the structures related to page
  /// filtering and skips the first rows if needed.
  Status StartPageFiltering();

  /// Returns the index of the row that was processed most recently.
  int64_t LastProcessedRow() const {
    if (def_level_ == ParquetLevel::ROW_GROUP_END) return current_row_;
    return levels_readahead_ ? current_row_ - 1 : current_row_;
  }

  /// Creates sub-ranges if page filtering is active.
  void CreateSubRanges(std::vector<io::ScanRange::SubRange>* sub_ranges);

  /// Calculates how many encoded values we need to skip in the page data, then
  /// invokes SkipEncodedValuesInPage(). The number of the encoded values depends on the
  /// nesting of the data, and also on the number of null values.
  /// E.g. if 'num_rows' is 10, and every row contains an array of 10 integers, then
  /// we need to skip 100 encoded values in the page data.
  /// And, if 'num_rows' is 10, and every second value is NULL, then we only need to skip
  /// 5 values in the page data since NULL values are not stored there.
  /// The number of primitive values can be calculated from the def and rep levels.
  /// Returns true on success, false otherwise.
  bool SkipTopLevelRows(int64_t num_rows);

  /// Skip values in the page data. Returns true on success, false otherwise.
  virtual bool SkipEncodedValuesInPage(int64_t num_values) = 0;

  /// Only valid to call this function when we filter out pages based on the Page index.
  /// Returns the RowGroup-level index of the starting row in the candidate page.
  int64_t FirstRowIdxInCurrentPage() const {
    DCHECK(!candidate_data_pages_.empty());
    return offset_index_.page_locations[
        candidate_data_pages_[candidate_page_idx_]].first_row_index;
  }

  /// The number of top-level rows until the end of the current candidate range.
  /// For simple columns it returns 0 if we have processed the last row in the current
  /// range. For nested columns, it returns 0 when we are processing values from the last
  /// row in the current row range.
  int RowsRemainingInCandidateRange() const {
    DCHECK(!candidate_data_pages_.empty());
    return parent_->candidate_ranges_[current_row_range_].last - current_row_;
  }

  /// Returns true if we are filtering pages.
  bool DoesPageFiltering() const {
    return !candidate_data_pages_.empty();
  }

  bool IsLastCandidateRange() const {
    return current_row_range_ == parent_->candidate_ranges_.size() - 1;
  }

  template <bool IN_COLLECTION>
  bool ConsumedCurrentCandidateRange() {
    return RowsRemainingInCandidateRange() == 0 &&
        (!IN_COLLECTION || max_rep_level() == 0 || num_buffered_values_ == 0 ||
            rep_levels_.PeekLevel() == 0);
  }

  /// This function fills the position slots up to 'max_values' or up to values belonging
  /// to the current candidate range or up to cached repetition levels. It returns the
  /// count of values until its limit. It can also be used without a position slot, in
  /// that case it returns the number of values until the first limit it reaches.
  /// It consumes cached repetition levels.
  int FillPositionsInCandidateRange(int rows_remaining, int max_values,
      uint8_t* RESTRICT tuple_mem, int tuple_size);

  /// Advance to the next candidate range that contains 'current_row_'.
  /// Cannot advance past the last candidate range.
  void AdvanceCandidateRange();

  /// Returns true if the current candidate row range has some rows in the current page.
  bool PageHasRemainingCandidateRows() const;

  /// Skip top level rows in current page until current candidate range is reached.
  bool SkipRowsInPage();

  /// Invoke this when there aren't any more interesting rows in the current page based
  /// on the page index. It starts reading the next page.
  bool JumpToNextPage();

  /// Slow-path status construction code for def/rep decoding errors. 'level_name' is
  /// either "rep" or "def", 'decoded_level' is the value returned from
  /// ParquetLevelDecoder::ReadLevel() and 'max_level' is the maximum allowed value.
  void __attribute__((noinline)) SetLevelDecodeError(const char* level_name,
      int decoded_level, int max_level);

  // Returns a detailed error message about unsupported encoding.
  Status GetUnsupportedDecodingError();
};

// Inline to allow inlining into collection and scalar column reader.
inline void ParquetColumnReader::ReadPositionNonBatched(int64_t* pos) {
  // NextLevels() should have already been called
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(pos_current_value_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";
  *pos = pos_current_value_++;
}

// Change 'val_count' to zero to exercise IMPALA-5197. This verifies the error handling
// path doesn't falsely report that the file is corrupted.
// Inlined to avoid overhead in release builds.
inline bool ParquetColumnReader::ColReaderDebugAction(int* val_count) {
#ifndef NDEBUG
  Status status = parent_->ScannerDebugAction();
  if (!status.ok()) {
    if (!status.IsCancelled()) parent_->parse_status_.MergeStatus(status);
    *val_count = 0;
    return false;
  }
#endif
  return true;
}

// Trigger debug action on every other call of Read*ValueBatch() once at least 128
// tuples have been produced to simulate failure such as exceeding memory limit.
// Triggering it every other call so as not to always fail on the first column reader
// when materializing multiple columns. Failing on non-empty row batch tests proper
// resources freeing by the Parquet scanner.
#ifndef NDEBUG
extern int parquet_column_reader_debug_count;
#define SHOULD_TRIGGER_COL_READER_DEBUG_ACTION(num_tuples) \
    ((parquet_column_reader_debug_count++ % 2) == 1 && num_tuples >= 128)
#else
#define SHOULD_TRIGGER_COL_READER_DEBUG_ACTION(x) (false)
#endif

} // namespace impala

#endif
