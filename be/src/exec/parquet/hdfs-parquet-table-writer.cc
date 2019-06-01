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

#include "exec/parquet/hdfs-parquet-table-writer.h"

#include <boost/unordered_set.hpp>

#include "common/version.h"
#include "exec/hdfs-table-sink.h"
#include "exec/parquet/parquet-column-stats.inline.h"
#include "exec/parquet/parquet-metadata-utils.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "rpc/thrift-util.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/bit-stream-utils.h"
#include "util/bit-util.h"
#include "util/buffer-builder.h"
#include "util/compress.h"
#include "util/debug-util.h"
#include "util/dict-encoding.h"
#include "util/hdfs-util.h"
#include "util/pretty-printer.h"
#include "util/rle-encoding.h"
#include "util/string-util.h"

#include <sstream>

#include "gen-cpp/ImpalaService_types.h"

#include "common/names.h"
using namespace impala;
using namespace apache::thrift;

// Managing file sizes: We need to estimate how big the files being buffered
// are in order to split them correctly in HDFS. Having a file that is too big
// will cause remote reads (parquet files are non-splittable).
// It's too expensive to compute the exact file sizes as the rows are buffered
// since the values in the current pages are only encoded/compressed when the page
// is full. Once the page is full, we encode and compress it, at which point we know
// the exact on file size.
// The current buffered pages (one for each column) can have a very poor estimate.
// To adjust for this, we aim for a slightly smaller file size than the ideal.
//
// Class that encapsulates all the state for writing a single column.  This contains
// all the buffered pages as well as the metadata (e.g. byte sizes, num values, etc).
// This is intended to be created once per writer per column and reused across
// row groups.
// We currently accumulate all the data pages for an entire row group per column
// before flushing them.  This can be pretty large (hundreds of MB) but we can't
// fix this without collocated files in HDFS.  With collocated files, the minimum
// we'd need to buffer is 1 page per column so on the order of 1MB (although we might
// decide to buffer a few pages for better HDFS write performance).
// Pages are reused between flushes.  They are created on demand as necessary and
// recycled after a flush.
// As rows come in, we accumulate the encoded values into the values_ and def_levels_
// buffers. When we've accumulated a page worth's of data, we combine values_ and
// def_levels_ into a single buffer that would be the exact bytes (with no gaps) in
// the file. The combined buffer is compressed if compression is enabled and we
// keep the combined/compressed buffer until we need to flush the file. The
// values_ and def_levels_ are then reused for the next page.
//
// TODO: For codegen, we would codegen the AppendRow() function for each column.
// This codegen is specific to the column expr (and type) and encoding.  The
// parent writer object would combine all the generated AppendRow from all
// the columns and run that function over row batches.
// TODO: we need to pass in the compression from the FE/metadata

namespace impala {

// Base class for column writers. This contains most of the logic except for
// the type specific functions which are implemented in the subclasses.
class HdfsParquetTableWriter::BaseColumnWriter {
 public:
  // expr - the expression to generate output values for this column.
  BaseColumnWriter(HdfsParquetTableWriter* parent, ScalarExprEvaluator* expr_eval,
      const THdfsCompression::type& codec)
    : parent_(parent),
      expr_eval_(expr_eval),
      codec_(codec),
      page_size_(DEFAULT_DATA_PAGE_SIZE),
      current_page_(nullptr),
      num_values_(0),
      total_compressed_byte_size_(0),
      total_uncompressed_byte_size_(0),
      dict_encoder_base_(nullptr),
      def_levels_(nullptr),
      values_buffer_len_(DEFAULT_DATA_PAGE_SIZE),
      page_stats_base_(nullptr),
      row_group_stats_base_(nullptr),
      table_sink_mem_tracker_(parent_->parent_->mem_tracker()) {
    static_assert(std::is_same<decltype(parent_->parent_), HdfsTableSink*>::value,
        "'table_sink_mem_tracker_' must point to the mem tracker of an HdfsTableSink");
    def_levels_ = parent_->state_->obj_pool()->Add(
        new RleEncoder(parent_->reusable_col_mem_pool_->Allocate(DEFAULT_DATA_PAGE_SIZE),
                       DEFAULT_DATA_PAGE_SIZE, 1));
    values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(values_buffer_len_);
  }

  virtual ~BaseColumnWriter() {}

  // Called after the constructor to initialize the column writer.
  Status Init() WARN_UNUSED_RESULT {
    Reset();
    RETURN_IF_ERROR(Codec::CreateCompressor(nullptr, false, codec_, &compressor_));
    return Status::OK();
  }

  // Appends the row to this column.  This buffers the value into a data page.  Returns
  // error if the space needed for the encoded value is larger than the data page size.
  // TODO: this needs to be batch based, instead of row based for better performance.
  // This is a bit trickier to handle the case where only a partial row batch can be
  // output to the current file because it reaches the max file size.  Enabling codegen
  // would also solve this problem.
  Status AppendRow(TupleRow* row) WARN_UNUSED_RESULT;

  // Flushes all buffered data pages to the file.
  // *file_pos is an output parameter and will be incremented by
  // the number of bytes needed to write all the data pages for this column.
  // first_data_page and first_dictionary_page are also out parameters and
  // will contain the byte offset for the data page and dictionary page.  They
  // will be set to -1 if the column does not contain that type of page.
  Status Flush(int64_t* file_pos, int64_t* first_data_page,
      int64_t* first_dictionary_page) WARN_UNUSED_RESULT;

  // Materializes the column statistics to the per-file MemPool so they are available
  // after their row batch buffer has been freed.
  Status MaterializeStatsValues() WARN_UNUSED_RESULT {
    RETURN_IF_ERROR(row_group_stats_base_->MaterializeStringValuesToInternalBuffers());
    RETURN_IF_ERROR(page_stats_base_->MaterializeStringValuesToInternalBuffers());
    return Status::OK();
  }

  // Encodes the row group statistics into a parquet::Statistics object and attaches it to
  // 'meta_data'.
  void EncodeRowGroupStats(parquet::ColumnMetaData* meta_data) {
    DCHECK(row_group_stats_base_ != nullptr);
    if (row_group_stats_base_->BytesNeeded() <= MAX_COLUMN_STATS_SIZE) {
      row_group_stats_base_->EncodeToThrift(&meta_data->statistics);
      meta_data->__isset.statistics = true;
    }
  }

  // Resets all the data accumulated for this column.  Memory can now be reused for
  // the next row group.
  // Any data for previous row groups must be reset (e.g. dictionaries).
  // Subclasses must call this if they override this function.
  virtual void Reset() {
    num_values_ = 0;
    total_compressed_byte_size_ = 0;
    current_encoding_ = parquet::Encoding::PLAIN;
    next_page_encoding_ = parquet::Encoding::PLAIN;
    pages_.clear();
    current_page_ = nullptr;
    column_encodings_.clear();
    dict_encoding_stats_.clear();
    data_encoding_stats_.clear();
    // Repetition/definition level encodings are constant. Incorporate them here.
    column_encodings_.insert(parquet::Encoding::RLE);
    offset_index_.page_locations.clear();
    column_index_.null_pages.clear();
    column_index_.min_values.clear();
    column_index_.max_values.clear();
    table_sink_mem_tracker_->Release(page_index_memory_consumption_);
    page_index_memory_consumption_ = 0;
    column_index_.null_counts.clear();
    valid_column_index_ = true;
    write_page_index_ = parent_->state_->query_options().parquet_write_page_index;
  }

  // Close this writer. This is only called after Flush() and no more rows will
  // be added.
  void Close() {
    if (compressor_.get() != nullptr) compressor_->Close();
    if (dict_encoder_base_ != nullptr) dict_encoder_base_->Close();
    // We must release the memory consumption of this column writer.
    table_sink_mem_tracker_->Release(page_index_memory_consumption_);
    page_index_memory_consumption_ = 0;
  }

  const ColumnType& type() const { return expr_eval_->root().type(); }
  uint64_t num_values() const { return num_values_; }
  uint64_t total_compressed_size() const { return total_compressed_byte_size_; }
  uint64_t total_uncompressed_size() const { return total_uncompressed_byte_size_; }
  parquet::CompressionCodec::type GetParquetCodec() const {
    return ConvertImpalaToParquetCodec(codec_);
  }

 protected:
  friend class HdfsParquetTableWriter;

  // Returns true if we should start writing a new page because of reaching some limits.
  bool ShouldStartNewPage() {
    int32_t num_values = current_page_->header.data_page_header.num_values;
    return def_levels_->buffer_full() || num_values >= parent_->page_row_count_limit();
  }

  Status AddMemoryConsumptionForPageIndex(int64_t new_memory_allocation) {
    if (UNLIKELY(!table_sink_mem_tracker_->TryConsume(new_memory_allocation))) {
      return table_sink_mem_tracker_->MemLimitExceeded(parent_->state_,
          "Failed to allocate memory for Parquet page index.", new_memory_allocation);
    }
    page_index_memory_consumption_ += new_memory_allocation;
    return Status::OK();
  }

  Status ReserveOffsetIndex(int64_t capacity) {
    if (!write_page_index_) return Status::OK();
    RETURN_IF_ERROR(
        AddMemoryConsumptionForPageIndex(capacity * sizeof(parquet::PageLocation)));
    offset_index_.page_locations.reserve(capacity);
    return Status::OK();
  }

  void AddLocationToOffsetIndex(const parquet::PageLocation& location) {
    if (!write_page_index_) return;
    offset_index_.page_locations.push_back(location);
  }

  Status AddPageStatsToColumnIndex() {
    if (!write_page_index_) return Status::OK();
    parquet::Statistics page_stats;
    page_stats_base_->EncodeToThrift(&page_stats);
    // If pages_stats contains min_value and max_value, then append them to min_values_
    // and max_values_ and also mark the page as not null. In case min and max values are
    // not set, push empty strings to maintain the consistency of the index and mark the
    // page as null. Always push the null_count.
    string min_val;
    string max_val;
    if ((page_stats.__isset.min_value) && (page_stats.__isset.max_value)) {
      Status s_min = TruncateDown(page_stats.min_value, PAGE_INDEX_MAX_STRING_LENGTH,
          &min_val);
      Status s_max = TruncateUp(page_stats.max_value, PAGE_INDEX_MAX_STRING_LENGTH,
          &max_val);
      if (!s_min.ok() || !s_max.ok()) valid_column_index_ = false;
      column_index_.null_pages.push_back(false);
    } else {
      DCHECK(!page_stats.__isset.min_value && !page_stats.__isset.max_value);
      column_index_.null_pages.push_back(true);
      DCHECK_EQ(page_stats.null_count, current_page_->header.data_page_header.num_values);
    }
    RETURN_IF_ERROR(
        AddMemoryConsumptionForPageIndex(min_val.capacity() + max_val.capacity()));
    column_index_.min_values.emplace_back(std::move(min_val));
    column_index_.max_values.emplace_back(std::move(max_val));
    column_index_.null_counts.push_back(page_stats.null_count);
    return Status::OK();
  }

  // Encodes value into the current page output buffer and updates the column statistics
  // aggregates. Returns true if the value was appended successfully to the current page.
  // Returns false if the value was not appended to the current page and the caller can
  // create a new page and try again with the same value. May change
  // 'next_page_encoding_' if the encoding for the next page should be different - e.g.
  // if a dictionary overflowed and dictionary encoding is no longer viable.
  // *bytes_needed will contain the (estimated) number of bytes needed to successfully
  // encode the value in the page.
  // Implemented in the subclass.
  virtual bool ProcessValue(void* value, int64_t* bytes_needed) WARN_UNUSED_RESULT = 0;


  // Subclasses can override this function to convert values after the expression was
  // evaluated. Used by int64 timestamp writers to change the TimestampValue returned by
  // the expression to an int64.
  virtual void* ConvertValue(void* value) { return value; }

  // Encodes out all data for the current page and updates the metadata.
  virtual Status FinalizeCurrentPage() WARN_UNUSED_RESULT;

  // Update current_page_ to a new page, reusing pages allocated if possible.
  void NewPage();

  // Writes out the dictionary encoded data buffered in dict_encoder_.
  void WriteDictDataPage();

  struct DataPage {
    // Page header.  This is a union of all page types.
    parquet::PageHeader header;

    // Number of bytes needed to store definition levels.
    int num_def_bytes;

    // This is the payload for the data page.  This includes the definition/repetition
    // levels data and the encoded values.  If compression is enabled, this is the
    // compressed data.
    uint8_t* data;

    // If true, this data page has been finalized.  All sizes are computed, header is
    // fully populated and any compression is done.
    bool finalized;

    // Number of non-null values
    int num_non_null;
  };

  HdfsParquetTableWriter* parent_;
  ScalarExprEvaluator* expr_eval_;

  THdfsCompression::type codec_;

  // Compression codec for this column.  If nullptr, this column is will not be
  // compressed.
  scoped_ptr<Codec> compressor_;

  // Size of newly created pages. Defaults to DEFAULT_DATA_PAGE_SIZE and is increased
  // when pages are not big enough. This only happens when there are enough unique values
  // such that we switch from PLAIN_DICTIONARY to PLAIN encoding and then have very
  // large values (i.e. greater than DEFAULT_DATA_PAGE_SIZE).
  // TODO: Consider removing and only creating a single large page as necessary.
  int64_t page_size_;

  // Pages belong to this column chunk. We need to keep them in memory in order to write
  // them together.
  vector<DataPage> pages_;

  // Pointer to the current page in 'pages_'. Not owned.
  DataPage* current_page_;

  // Total number of values across all pages, including NULL.
  int64_t num_values_;
  int64_t total_compressed_byte_size_;
  int64_t total_uncompressed_byte_size_;
  // Encoding of the current page.
  parquet::Encoding::type current_encoding_;
  // Encoding to use for the next page. By default, the same as 'current_encoding_'.
  // Used by the column writer to switch encoding while writing a column, e.g. if the
  // dictionary overflows.
  parquet::Encoding::type next_page_encoding_;

  // Set of all encodings used in the column chunk
  unordered_set<parquet::Encoding::type> column_encodings_;

  // Map from the encoding to the number of pages in the column chunk with this encoding
  // These are used to construct the PageEncodingStats, which provide information
  // about encoding usage for each different page type. Currently, only dictionary
  // and data pages are used.
  unordered_map<parquet::Encoding::type, int> dict_encoding_stats_;
  unordered_map<parquet::Encoding::type, int> data_encoding_stats_;

  // Created, owned, and set by the derived class.
  DictEncoderBase* dict_encoder_base_;

  // Rle encoder object for storing definition levels, owned by instances of this class.
  // For non-nested schemas, this always uses 1 bit per row. This is reused across pages
  // since the underlying buffer is copied out when the page is finalized.
  RleEncoder* def_levels_;

  // Data for buffered values. This is owned by instances of this class and gets reused
  // across pages.
  uint8_t* values_buffer_;
  // The size of values_buffer_.
  int values_buffer_len_;

  // Pointers to statistics, created, owned, and set by the derived class.
  ColumnStatsBase* page_stats_base_;
  ColumnStatsBase* row_group_stats_base_;

  // OffsetIndex stores the locations of the pages.
  parquet::OffsetIndex offset_index_;

  // ColumnIndex stores the statistics of the pages.
  parquet::ColumnIndex column_index_;

  // Pointer to the HdfsTableSink's MemTracker.
  MemTracker* table_sink_mem_tracker_;

  // Memory consumption of the min/max values in the page index.
  int64_t page_index_memory_consumption_ = 0;

  // Only write ColumnIndex when 'valid_column_index_' is true. We always need to write
  // the OffsetIndex though.
  bool valid_column_index_ = true;

  // True, if we should write the page index.
  bool write_page_index_;
};

// Per type column writer.
template<typename T>
class HdfsParquetTableWriter::ColumnWriter :
    public HdfsParquetTableWriter::BaseColumnWriter {
 public:
  ColumnWriter(HdfsParquetTableWriter* parent, ScalarExprEvaluator* eval,
      const THdfsCompression::type& codec)
    : BaseColumnWriter(parent, eval, codec),
      num_values_since_dict_size_check_(0),
      plain_encoded_value_size_(
          ParquetPlainEncoder::EncodedByteSize(eval->root().type())) {
    DCHECK_NE(eval->root().type().type, TYPE_BOOLEAN);
    // IMPALA-7304: Don't write column index for floating-point columns until
    // PARQUET-1222 is resolved.
    if (std::is_floating_point<T>::value) valid_column_index_ = false;
  }

  virtual void Reset() {
    BaseColumnWriter::Reset();
    // IMPALA-7304: Don't write column index for floating-point columns until
    // PARQUET-1222 is resolved.
    if (std::is_floating_point<T>::value) valid_column_index_ = false;
    // Default to dictionary encoding.  If the cardinality ends up being too high,
    // it will fall back to plain.
    current_encoding_ = parquet::Encoding::PLAIN_DICTIONARY;
    next_page_encoding_ = parquet::Encoding::PLAIN_DICTIONARY;
    dict_encoder_.reset(
        new DictEncoder<T>(parent_->per_file_mem_pool_.get(), plain_encoded_value_size_,
            parent_->parent_->mem_tracker()));
    dict_encoder_base_ = dict_encoder_.get();
    page_stats_.reset(
        new ColumnStats<T>(parent_->per_file_mem_pool_.get(), plain_encoded_value_size_));
    page_stats_base_ = page_stats_.get();
    row_group_stats_.reset(
        new ColumnStats<T>(parent_->per_file_mem_pool_.get(), plain_encoded_value_size_));
    row_group_stats_base_ = row_group_stats_.get();
  }

 protected:
  virtual bool ProcessValue(void* value, int64_t* bytes_needed) {
    if (current_encoding_ == parquet::Encoding::PLAIN_DICTIONARY) {
      if (UNLIKELY(num_values_since_dict_size_check_ >=
                   DICTIONARY_DATA_PAGE_SIZE_CHECK_PERIOD)) {
        num_values_since_dict_size_check_ = 0;
        if (dict_encoder_->EstimatedDataEncodedSize() >= page_size_) return false;
      }
      ++num_values_since_dict_size_check_;
      *bytes_needed = dict_encoder_->Put(*CastValue(value));
      // If the dictionary contains the maximum number of values, switch to plain
      // encoding for the next page. The current page is full and must be written out.
      if (UNLIKELY(*bytes_needed < 0)) {
        next_page_encoding_ = parquet::Encoding::PLAIN;
        return false;
      }
      parent_->file_size_estimate_ += *bytes_needed;
    } else if (current_encoding_ == parquet::Encoding::PLAIN) {
      T* v = CastValue(value);
      *bytes_needed = plain_encoded_value_size_ < 0 ?
          ParquetPlainEncoder::ByteSize<T>(*v) :
          plain_encoded_value_size_;
      if (current_page_->header.uncompressed_page_size + *bytes_needed > page_size_) {
        return false;
      }
      uint8_t* dst_ptr = values_buffer_ + current_page_->header.uncompressed_page_size;
      int64_t written_len =
          ParquetPlainEncoder::Encode(*v, plain_encoded_value_size_, dst_ptr);
      DCHECK_EQ(*bytes_needed, written_len);
      current_page_->header.uncompressed_page_size += written_len;
    } else {
      // TODO: support other encodings here
      DCHECK(false);
    }
    page_stats_->Update(*CastValue(value));
    return true;
  }

 private:
  // The period, in # of rows, to check the estimated dictionary page size against
  // the data page size. We want to start a new data page when the estimated size
  // is at least that big. The estimated size computation is not very cheap and
  // we can tolerate going over the data page size by some amount.
  // The expected byte size per dictionary value is < 1B and at most 2 bytes so the
  // error is pretty low.
  // TODO: is there a better way?
  static const int DICTIONARY_DATA_PAGE_SIZE_CHECK_PERIOD = 100;

  // Encoder for dictionary encoding for different columns. Only one is set.
  scoped_ptr<DictEncoder<T>> dict_encoder_;

  // The number of values added since we last checked the dictionary.
  int num_values_since_dict_size_check_;

  // Temporary string value to hold CHAR(N)
  StringValue temp_;

  // Tracks statistics per page. These are written out to the page index.
  scoped_ptr<ColumnStats<T>> page_stats_;

  // Tracks statistics per row group. This gets reset when starting a new row group.
  scoped_ptr<ColumnStats<T>> row_group_stats_;

  // Converts a slot pointer to a raw value suitable for encoding
  inline T* CastValue(void* value) {
    return reinterpret_cast<T*>(value);
  }
 protected:
  // Size of each encoded value in plain encoding. -1 if the type is variable-length.
  int64_t plain_encoded_value_size_;
};

template<>
inline StringValue* HdfsParquetTableWriter::ColumnWriter<StringValue>::CastValue(
    void* value) {
  if (type().type == TYPE_CHAR) {
    temp_.ptr = reinterpret_cast<char*>(value);
    temp_.len = StringValue::UnpaddedCharLength(temp_.ptr, type().len);
    return &temp_;
  }
  return reinterpret_cast<StringValue*>(value);
}

// Bools are encoded a bit differently so subclass it explicitly.
class HdfsParquetTableWriter::BoolColumnWriter :
    public HdfsParquetTableWriter::BaseColumnWriter {
 public:
  BoolColumnWriter(HdfsParquetTableWriter* parent, ScalarExprEvaluator* eval,
      const THdfsCompression::type& codec)
    : BaseColumnWriter(parent, eval, codec),
      page_stats_(parent_->reusable_col_mem_pool_.get(), -1),
      row_group_stats_(parent_->reusable_col_mem_pool_.get(), -1) {
    DCHECK_EQ(eval->root().type().type, TYPE_BOOLEAN);
    bool_values_ = parent_->state_->obj_pool()->Add(
        new BitWriter(values_buffer_, values_buffer_len_));
    // Dictionary encoding doesn't make sense for bools and is not allowed by
    // the format.
    current_encoding_ = parquet::Encoding::PLAIN;
    dict_encoder_base_ = nullptr;

    page_stats_base_ = &page_stats_;
    row_group_stats_base_ = &row_group_stats_;
  }

 protected:
  virtual bool ProcessValue(void* value, int64_t* bytes_needed) {
    bool v = *reinterpret_cast<bool*>(value);
    if (!bool_values_->PutValue(v, 1)) return false;
    page_stats_.Update(v);
    return true;
  }

  virtual Status FinalizeCurrentPage() {
    DCHECK(current_page_ != nullptr);
    if (current_page_->finalized) return Status::OK();
    bool_values_->Flush();
    int num_bytes = bool_values_->bytes_written();
    current_page_->header.uncompressed_page_size += num_bytes;
    // Call into superclass to handle the rest.
    RETURN_IF_ERROR(BaseColumnWriter::FinalizeCurrentPage());
    bool_values_->Clear();
    return Status::OK();
  }

 private:
  // Used to encode bools as single bit values. This is reused across pages.
  BitWriter* bool_values_;

  // Tracks statistics per page. These are written out to the page index.
  ColumnStats<bool> page_stats_;

  // Tracks statistics per row group. This gets reset when starting a new file.
  ColumnStats<bool> row_group_stats_;
};

}


/// Base class for int64 timestamp writers. 'eval' is expected to return a pointer to a
/// TimestampValue. The result of TimestampValue->int64 conversion is stored in 'result_'.
class HdfsParquetTableWriter::Int64TimestampColumnWriterBase :
    public HdfsParquetTableWriter::ColumnWriter<int64_t> {
public:
  Int64TimestampColumnWriterBase(HdfsParquetTableWriter* parent,
      ScalarExprEvaluator* eval, const THdfsCompression::type& codec)
    : HdfsParquetTableWriter::ColumnWriter<int64_t>(parent, eval, codec) {
    int64_t dummy;
    plain_encoded_value_size_ = ParquetPlainEncoder::ByteSize(dummy);
  }

protected:
  virtual void* ConvertValue(void* value) override {
    const TimestampValue* ts = reinterpret_cast<TimestampValue*>(value);
    return ConvertTimestamp(*ts, &result_) ? &result_ : nullptr;
  }

  /// Overrides of this function are expected to set 'result' if the conversion is
  /// successful and return true. If the timestamp is invalid or cannot
  /// be represented as int64 then false should be returned.
  virtual bool ConvertTimestamp(const TimestampValue& ts, int64_t* result) = 0;

private:
  int64_t result_;
};


/// Converts TimestampValues to INT64_MILLIS.
class HdfsParquetTableWriter::Int64MilliTimestampColumnWriter :
    public HdfsParquetTableWriter::Int64TimestampColumnWriterBase {
public:
  Int64MilliTimestampColumnWriter(HdfsParquetTableWriter* parent,
      ScalarExprEvaluator* eval, const THdfsCompression::type& codec)
    : HdfsParquetTableWriter::Int64TimestampColumnWriterBase(parent, eval, codec) {}

protected:
  virtual bool ConvertTimestamp(const TimestampValue& ts, int64_t* result) {
    return ts.FloorUtcToUnixTimeMillis(result);
  }
};

/// Converts TimestampValues to INT64_MICROS.
class HdfsParquetTableWriter::Int64MicroTimestampColumnWriter :
    public HdfsParquetTableWriter::Int64TimestampColumnWriterBase {
public:
  Int64MicroTimestampColumnWriter(HdfsParquetTableWriter* parent,
      ScalarExprEvaluator* eval, const THdfsCompression::type& codec)
    : HdfsParquetTableWriter::Int64TimestampColumnWriterBase(parent, eval, codec) {}

protected:
  virtual bool ConvertTimestamp(const TimestampValue& ts, int64_t* result) {
    return ts.FloorUtcToUnixTimeMicros(result);
  }
};

/// Converts TimestampValues to INT64_NANOS. Conversion is expected to fail for
/// timestamps outside [1677-09-21 00:12:43.145224192 .. 2262-04-11 23:47:16.854775807].
class HdfsParquetTableWriter::Int64NanoTimestampColumnWriter :
    public HdfsParquetTableWriter::Int64TimestampColumnWriterBase {
public:
  Int64NanoTimestampColumnWriter(HdfsParquetTableWriter* parent,
      ScalarExprEvaluator* eval, const THdfsCompression::type& codec)
    : HdfsParquetTableWriter::Int64TimestampColumnWriterBase(parent, eval, codec) {}

protected:
  virtual bool ConvertTimestamp(const TimestampValue& ts, int64_t* result) {
    return ts.UtcToUnixTimeLimitedRangeNanos(result);
  }
};

inline Status HdfsParquetTableWriter::BaseColumnWriter::AppendRow(TupleRow* row) {
  ++num_values_;
  void* value = ConvertValue(expr_eval_->GetValue(row));
  if (current_page_ == nullptr) NewPage();

  if (ShouldStartNewPage()) {
    RETURN_IF_ERROR(FinalizeCurrentPage());
    NewPage();
  }

  // Encoding may fail for several reasons - because the current page is not big enough,
  // because we've encoded the maximum number of unique dictionary values and need to
  // switch to plain encoding, etc. so we may need to try again more than once.
  // TODO: Have a clearer set of state transitions here, to make it easier to see that
  // this won't loop forever.
  while (true) {
    // Nulls don't get encoded. Increment the null count of the parquet statistics.
    if (value == nullptr) {
      DCHECK(page_stats_base_ != nullptr);
      page_stats_base_->IncrementNullCount(1);
      break;
    }

    int64_t bytes_needed = 0;
    if (ProcessValue(value, &bytes_needed)) {
      ++current_page_->num_non_null;
      break; // Succesfully appended, don't need to retry.
    }

    // Value didn't fit on page, try again on a new page.
    RETURN_IF_ERROR(FinalizeCurrentPage());

    // Check how much space is needed to write this value. If that is larger than the
    // page size then increase page size and try again.
    if (UNLIKELY(bytes_needed > page_size_)) {
      if (bytes_needed > MAX_DATA_PAGE_SIZE) {
        stringstream ss;
        ss << "Cannot write value of size "
           << PrettyPrinter::Print(bytes_needed, TUnit::BYTES) << " bytes to a Parquet "
           << "data page that exceeds the max page limit "
           << PrettyPrinter::Print(MAX_DATA_PAGE_SIZE , TUnit::BYTES) << ".";
        return Status(ss.str());
      }
      page_size_ = bytes_needed;
      values_buffer_len_ = page_size_;
      values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(values_buffer_len_);
    }
    NewPage();
  }

  // Now that the value has been successfully written, write the definition level.
  bool ret = def_levels_->Put(value != nullptr);
  // Writing the def level will succeed because we ensured there was enough space for it
  // above, and new pages will always have space for at least a single def level.
  DCHECK(ret);
  ++current_page_->header.data_page_header.num_values;

  return Status::OK();
}

inline void HdfsParquetTableWriter::BaseColumnWriter::WriteDictDataPage() {
  DCHECK(dict_encoder_base_ != nullptr);
  DCHECK_EQ(current_page_->header.uncompressed_page_size, 0);
  if (current_page_->num_non_null == 0) return;
  int len = dict_encoder_base_->WriteData(values_buffer_, values_buffer_len_);
  while (UNLIKELY(len < 0)) {
    // len < 0 indicates the data doesn't fit into a data page. Allocate a larger data
    // page.
    values_buffer_len_ *= 2;
    values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(values_buffer_len_);
    len = dict_encoder_base_->WriteData(values_buffer_, values_buffer_len_);
  }
  dict_encoder_base_->ClearIndices();
  current_page_->header.uncompressed_page_size = len;
}

Status HdfsParquetTableWriter::BaseColumnWriter::Flush(int64_t* file_pos,
   int64_t* first_data_page, int64_t* first_dictionary_page) {
  if (current_page_ == nullptr) {
    // This column/file is empty
    *first_data_page = *file_pos;
    *first_dictionary_page = -1;
    return Status::OK();
  }

  RETURN_IF_ERROR(FinalizeCurrentPage());

  *first_dictionary_page = -1;
  // First write the dictionary page before any of the data pages.
  if (dict_encoder_base_ != nullptr) {
    *first_dictionary_page = *file_pos;
    // Write dictionary page header
    parquet::DictionaryPageHeader dict_header;
    dict_header.num_values = dict_encoder_base_->num_entries();
    dict_header.encoding = parquet::Encoding::PLAIN_DICTIONARY;
    ++dict_encoding_stats_[dict_header.encoding];

    parquet::PageHeader header;
    header.type = parquet::PageType::DICTIONARY_PAGE;
    header.uncompressed_page_size = dict_encoder_base_->dict_encoded_size();
    header.__set_dictionary_page_header(dict_header);

    // Write the dictionary page data, compressing it if necessary.
    uint8_t* dict_buffer = parent_->per_file_mem_pool_->Allocate(
        header.uncompressed_page_size);
    dict_encoder_base_->WriteDict(dict_buffer);
    if (compressor_.get() != nullptr) {
      SCOPED_TIMER(parent_->parent_->compress_timer());
      int64_t max_compressed_size =
          compressor_->MaxOutputLen(header.uncompressed_page_size);
      DCHECK_GT(max_compressed_size, 0);
      uint8_t* compressed_data =
          parent_->per_file_mem_pool_->Allocate(max_compressed_size);
      header.compressed_page_size = max_compressed_size;
      RETURN_IF_ERROR(compressor_->ProcessBlock32(true, header.uncompressed_page_size,
          dict_buffer, &header.compressed_page_size, &compressed_data));
      dict_buffer = compressed_data;
      // We allocated the output based on the guessed size, return the extra allocated
      // bytes back to the mem pool.
      parent_->per_file_mem_pool_->ReturnPartialAllocation(
          max_compressed_size - header.compressed_page_size);
    } else {
      header.compressed_page_size = header.uncompressed_page_size;
    }

    uint8_t* header_buffer;
    uint32_t header_len;
    RETURN_IF_ERROR(parent_->thrift_serializer_->SerializeToBuffer(
        &header, &header_len, &header_buffer));
    RETURN_IF_ERROR(parent_->Write(header_buffer, header_len));
    *file_pos += header_len;
    total_compressed_byte_size_ += header_len;
    total_uncompressed_byte_size_ += header_len;

    RETURN_IF_ERROR(parent_->Write(dict_buffer, header.compressed_page_size));
    *file_pos += header.compressed_page_size;
    total_compressed_byte_size_ += header.compressed_page_size;
    total_uncompressed_byte_size_ += header.uncompressed_page_size;
  }

  *first_data_page = *file_pos;
  int64_t current_row_group_index = 0;
  RETURN_IF_ERROR(ReserveOffsetIndex(pages_.size()));

  // Write data pages
  for (const DataPage& page : pages_) {
    parquet::PageLocation location;

    if (page.header.data_page_header.num_values == 0) {
      // Skip empty pages
      location.offset = -1;
      location.compressed_page_size = 0;
      location.first_row_index = -1;
      AddLocationToOffsetIndex(location);
      continue;
    }

    location.offset = *file_pos;
    location.first_row_index = current_row_group_index;

    // Write data page header
    uint8_t* buffer = nullptr;
    uint32_t len = 0;
    RETURN_IF_ERROR(
        parent_->thrift_serializer_->SerializeToBuffer(&page.header, &len, &buffer));
    RETURN_IF_ERROR(parent_->Write(buffer, len));
    *file_pos += len;

    // Note that the namings are confusing here:
    // parquet::PageHeader::compressed_page_size is the compressed page size in bytes, as
    // its name suggests. On the other hand, parquet::PageLocation::compressed_page_size
    // also includes the size of the page header.
    location.compressed_page_size = page.header.compressed_page_size + len;
    AddLocationToOffsetIndex(location);

    // Write the page data
    RETURN_IF_ERROR(parent_->Write(page.data, page.header.compressed_page_size));
    *file_pos += page.header.compressed_page_size;
    current_row_group_index += page.header.data_page_header.num_values;
  }
  return Status::OK();
}

Status HdfsParquetTableWriter::BaseColumnWriter::FinalizeCurrentPage() {
  DCHECK(current_page_ != nullptr);
  if (current_page_->finalized) return Status::OK();

  // If the entire page was NULL, encode it as PLAIN since there is no
  // data anyway. We don't output a useless dictionary page and it works
  // around a parquet MR bug (see IMPALA-759 for more details).
  if (current_page_->num_non_null == 0) current_encoding_ = parquet::Encoding::PLAIN;

  if (current_encoding_ == parquet::Encoding::PLAIN_DICTIONARY) WriteDictDataPage();

  parquet::PageHeader& header = current_page_->header;
  header.data_page_header.encoding = current_encoding_;

  // Accumulate encoding statistics
  column_encodings_.insert(header.data_page_header.encoding);
  ++data_encoding_stats_[header.data_page_header.encoding];

  // Compute size of definition bits
  def_levels_->Flush();
  current_page_->num_def_bytes = sizeof(int32_t) + def_levels_->len();
  header.uncompressed_page_size += current_page_->num_def_bytes;

  // At this point we know all the data for the data page.  Combine them into one buffer.
  uint8_t* uncompressed_data = nullptr;
  if (compressor_.get() == nullptr) {
    uncompressed_data =
        parent_->per_file_mem_pool_->Allocate(header.uncompressed_page_size);
  } else {
    // We have compression.  Combine into the staging buffer.
    parent_->compression_staging_buffer_.resize(
        header.uncompressed_page_size);
    uncompressed_data = &parent_->compression_staging_buffer_[0];
  }

  BufferBuilder buffer(uncompressed_data, header.uncompressed_page_size);

  // Copy the definition (null) data
  int num_def_level_bytes = def_levels_->len();

  buffer.Append(num_def_level_bytes);
  buffer.Append(def_levels_->buffer(), num_def_level_bytes);
  // TODO: copy repetition data when we support nested types.
  buffer.Append(values_buffer_, buffer.capacity() - buffer.size());

  // Apply compression if necessary
  if (compressor_.get() == nullptr) {
    current_page_->data = uncompressed_data;
    header.compressed_page_size = header.uncompressed_page_size;
  } else {
    SCOPED_TIMER(parent_->parent_->compress_timer());
    int64_t max_compressed_size =
        compressor_->MaxOutputLen(header.uncompressed_page_size);
    DCHECK_GT(max_compressed_size, 0);
    uint8_t* compressed_data = parent_->per_file_mem_pool_->Allocate(max_compressed_size);
    header.compressed_page_size = max_compressed_size;
    RETURN_IF_ERROR(compressor_->ProcessBlock32(true, header.uncompressed_page_size,
        uncompressed_data, &header.compressed_page_size, &compressed_data));
    current_page_->data = compressed_data;

    // We allocated the output based on the guessed size, return the extra allocated
    // bytes back to the mem pool.
    parent_->per_file_mem_pool_->ReturnPartialAllocation(
        max_compressed_size - header.compressed_page_size);
  }

  DCHECK(page_stats_base_ != nullptr);
  RETURN_IF_ERROR(AddPageStatsToColumnIndex());

  // Update row group statistics from page statistics.
  DCHECK(row_group_stats_base_ != nullptr);
  row_group_stats_base_->Merge(*page_stats_base_);

  // Add the size of the data page header
  uint8_t* header_buffer;
  uint32_t header_len = 0;
  RETURN_IF_ERROR(parent_->thrift_serializer_->SerializeToBuffer(
      &current_page_->header, &header_len, &header_buffer));

  current_page_->finalized = true;
  total_compressed_byte_size_ += header_len + header.compressed_page_size;
  total_uncompressed_byte_size_ += header_len + header.uncompressed_page_size;
  parent_->file_size_estimate_ += header_len + header.compressed_page_size;
  def_levels_->Clear();
  return Status::OK();
}

void HdfsParquetTableWriter::BaseColumnWriter::NewPage() {
  pages_.push_back(DataPage());
  current_page_ = &pages_.back();

  parquet::DataPageHeader header;
  header.num_values = 0;
  // The code that populates the column chunk metadata's encodings field
  // relies on these specific values for the definition/repetition level
  // encodings.
  header.definition_level_encoding = parquet::Encoding::RLE;
  header.repetition_level_encoding = parquet::Encoding::RLE;
  current_page_->header.__set_data_page_header(header);
  current_encoding_ = next_page_encoding_;
  current_page_->finalized = false;
  current_page_->num_non_null = 0;
  page_stats_base_->Reset();
}

HdfsParquetTableWriter::HdfsParquetTableWriter(HdfsTableSink* parent, RuntimeState* state,
    OutputPartition* output, const HdfsPartitionDescriptor* part_desc,
    const HdfsTableDescriptor* table_desc)
  : HdfsTableWriter(parent, state, output, part_desc, table_desc),
    thrift_serializer_(new ThriftSerializer(true)),
    current_row_group_(nullptr),
    row_count_(0),
    file_size_limit_(0),
    reusable_col_mem_pool_(new MemPool(parent_->mem_tracker())),
    per_file_mem_pool_(new MemPool(parent_->mem_tracker())),
    row_idx_(0) {}

HdfsParquetTableWriter::~HdfsParquetTableWriter() {
}

Status HdfsParquetTableWriter::Init() {
  // Initialize file metadata
  file_metadata_.version = PARQUET_CURRENT_VERSION;

  stringstream created_by;
  created_by << "impala version " << GetDaemonBuildVersion()
             << " (build " << GetDaemonBuildHash() << ")";
  file_metadata_.__set_created_by(created_by.str());

  // Default to snappy compressed
  THdfsCompression::type codec = THdfsCompression::SNAPPY;

  const TQueryOptions& query_options = state_->query_options();
  if (query_options.__isset.compression_codec) {
    codec = query_options.compression_codec;
  }
  if (!(codec == THdfsCompression::NONE ||
        codec == THdfsCompression::GZIP ||
        codec == THdfsCompression::SNAPPY)) {
    stringstream ss;
    ss << "Invalid parquet compression codec " << Codec::GetCodecName(codec);
    return Status(ss.str());
  }
  VLOG_FILE << "Using compression codec: " << codec;

  if (query_options.__isset.parquet_page_row_count_limit) {
    page_row_count_limit_ = query_options.parquet_page_row_count_limit;
  }

  int num_cols = table_desc_->num_cols() - table_desc_->num_clustering_cols();
  // When opening files using the hdfsOpenFile() API, the maximum block size is limited to
  // 2GB.
  int64_t min_block_size = MinBlockSize(num_cols);
  if (min_block_size >= numeric_limits<int32_t>::max()) {
    stringstream ss;
    return Status(Substitute("Minimum required block size must be less than 2GB "
        "(currently $0), try reducing the number of non-partitioning columns in the "
        "target table (currently $1).",
        PrettyPrinter::Print(min_block_size, TUnit::BYTES), num_cols));
  }

  columns_.resize(num_cols);
  // Initialize each column structure.
  for (int i = 0; i < columns_.size(); ++i) {
    BaseColumnWriter* writer = nullptr;
    const ColumnType& type = output_expr_evals_[i]->root().type();
    switch (type.type) {
      case TYPE_BOOLEAN:
        writer = new BoolColumnWriter(this, output_expr_evals_[i], codec);
        break;
      case TYPE_TINYINT:
        writer = new ColumnWriter<int8_t>(this, output_expr_evals_[i], codec);
        break;
      case TYPE_SMALLINT:
        writer = new ColumnWriter<int16_t>(this, output_expr_evals_[i], codec);
        break;
      case TYPE_INT:
        writer = new ColumnWriter<int32_t>(this, output_expr_evals_[i], codec);
        break;
      case TYPE_BIGINT:
        writer = new ColumnWriter<int64_t>(this, output_expr_evals_[i], codec);
        break;
      case TYPE_FLOAT:
        writer = new ColumnWriter<float>(this, output_expr_evals_[i], codec);
        break;
      case TYPE_DOUBLE:
        writer = new ColumnWriter<double>(this, output_expr_evals_[i], codec);
        break;
      case TYPE_TIMESTAMP:
        switch (state_->query_options().parquet_timestamp_type) {
          case TParquetTimestampType::INT96_NANOS:
            writer =
                new ColumnWriter<TimestampValue>(this, output_expr_evals_[i], codec);
            break;
          case TParquetTimestampType::INT64_MILLIS:
            writer =
                new Int64MilliTimestampColumnWriter(this, output_expr_evals_[i], codec);
            break;
          case TParquetTimestampType::INT64_MICROS:
            writer =
                new Int64MicroTimestampColumnWriter(this, output_expr_evals_[i], codec);
            break;
          case TParquetTimestampType::INT64_NANOS:
            writer =
                new Int64NanoTimestampColumnWriter(this, output_expr_evals_[i], codec);
            break;
          default:
            DCHECK(false);
        }
        break;
      case TYPE_VARCHAR:
      case TYPE_STRING:
      case TYPE_CHAR:
        writer = new ColumnWriter<StringValue>(this, output_expr_evals_[i], codec);
        break;
      case TYPE_DECIMAL:
        switch (output_expr_evals_[i]->root().type().GetByteSize()) {
          case 4:
            writer = new ColumnWriter<Decimal4Value>(
                this, output_expr_evals_[i], codec);
            break;
          case 8:
            writer = new ColumnWriter<Decimal8Value>(
                this, output_expr_evals_[i], codec);
            break;
          case 16:
            writer = new ColumnWriter<Decimal16Value>(
                this, output_expr_evals_[i], codec);
            break;
          default:
            DCHECK(false);
        }
        break;
      case TYPE_DATE:
        writer = new ColumnWriter<DateValue>(this, output_expr_evals_[i], codec);
        break;
      default:
        DCHECK(false);
    }
    columns_[i].reset(writer);
    RETURN_IF_ERROR(columns_[i]->Init());
  }
  RETURN_IF_ERROR(CreateSchema());
  return Status::OK();
}

Status HdfsParquetTableWriter::CreateSchema() {
  int num_clustering_cols = table_desc_->num_clustering_cols();

  // Create flattened tree with a single root.
  file_metadata_.schema.resize(columns_.size() + 1);
  file_metadata_.schema[0].__set_num_children(columns_.size());
  file_metadata_.schema[0].name = "schema";

  for (int i = 0; i < columns_.size(); ++i) {
    parquet::SchemaElement& col_schema = file_metadata_.schema[i + 1];
    const ColumnType& col_type = output_expr_evals_[i]->root().type();
    col_schema.name = table_desc_->col_descs()[i + num_clustering_cols].name();
    ParquetMetadataUtils::FillSchemaElement(col_type, state_->query_options(),
                                            &col_schema);
  }

  return Status::OK();
}

Status HdfsParquetTableWriter::AddRowGroup() {
  if (current_row_group_ != nullptr) RETURN_IF_ERROR(FlushCurrentRowGroup());
  file_metadata_.row_groups.push_back(parquet::RowGroup());
  current_row_group_ = &file_metadata_.row_groups[file_metadata_.row_groups.size() - 1];

  // Initialize new row group metadata.
  int num_clustering_cols = table_desc_->num_clustering_cols();
  current_row_group_->columns.resize(columns_.size());
  for (int i = 0; i < columns_.size(); ++i) {
    parquet::ColumnMetaData metadata;
    metadata.type = ParquetMetadataUtils::ConvertInternalToParquetType(
        columns_[i]->type().type, state_->query_options());
    metadata.path_in_schema.push_back(
        table_desc_->col_descs()[i + num_clustering_cols].name());
    metadata.codec = columns_[i]->GetParquetCodec();
    current_row_group_->columns[i].__set_meta_data(metadata);
  }

  return Status::OK();
}

int64_t HdfsParquetTableWriter::MinBlockSize(int64_t num_file_cols) const {
  // See file_size_limit_ calculation in InitNewFile().
  return 3 * DEFAULT_DATA_PAGE_SIZE * num_file_cols;
}

uint64_t HdfsParquetTableWriter::default_block_size() const {
  int64_t block_size;
  if (state_->query_options().__isset.parquet_file_size &&
      state_->query_options().parquet_file_size > 0) {
    // If the user specified a value explicitly, use it. InitNewFile() will verify that
    // the actual file's block size is sufficient.
    block_size = state_->query_options().parquet_file_size;
  } else {
    block_size = HDFS_BLOCK_SIZE;
    // Blocks are usually HDFS_BLOCK_SIZE bytes, unless there are many columns, in
    // which case a per-column minimum kicks in.
    block_size = max(block_size, MinBlockSize(columns_.size()));
  }
  // HDFS does not like block sizes that are not aligned
  return BitUtil::RoundUp(block_size, HDFS_BLOCK_ALIGNMENT);
}

Status HdfsParquetTableWriter::InitNewFile() {
  DCHECK(current_row_group_ == nullptr);

  per_file_mem_pool_->Clear();

  // Get the file limit
  file_size_limit_ = output_->block_size;
  if (file_size_limit_ < HDFS_MIN_FILE_SIZE) {
    stringstream ss;
    ss << "Hdfs file size (" << file_size_limit_ << ") is too small.";
    return Status(ss.str());
  }

  // We want to output HDFS files that are no more than file_size_limit_.  If we
  // go over the limit, HDFS will split the file into multiple blocks which
  // is undesirable.  If we are under the limit, we potentially end up with more
  // files than necessary.  Either way, it is not going to generate a invalid
  // file.
  // With arbitrary encoding schemes, it is not possible to know if appending
  // a new row will push us over the limit until after encoding it.  Rolling back
  // a row can be tricky as well so instead we will stop the file when it is
  // 2 * DEFAULT_DATA_PAGE_SIZE * num_cols short of the limit. e.g. 50 cols with 8K data
  // pages, means we stop 800KB shy of the limit.
  // Data pages calculate their size precisely when they are complete so having
  // a two page buffer guarantees we will never go over (unless there are huge values
  // that require increasing the page size).
  // TODO: this should be made dynamic based on the size of rows seen so far.
  // This would for example, let us account for very long string columns.
  const int64_t num_cols = columns_.size();
  if (file_size_limit_ < MinBlockSize(num_cols)) {
    stringstream ss;
    ss << "Parquet file size " << file_size_limit_ << " bytes is too small for "
       << "a table with " << num_cols << " non-partitioning columns. Set query option "
       << "PARQUET_FILE_SIZE to at least " << MinBlockSize(num_cols) << ".";
    return Status(ss.str());
  }
  file_size_limit_ -= 2 * DEFAULT_DATA_PAGE_SIZE * columns_.size();
  DCHECK_GE(file_size_limit_,
      static_cast<int64_t>(DEFAULT_DATA_PAGE_SIZE * columns_.size()));
  file_pos_ = 0;
  row_count_ = 0;
  file_size_estimate_ = 0;

  file_metadata_.row_groups.clear();
  RETURN_IF_ERROR(AddRowGroup());
  RETURN_IF_ERROR(WriteFileHeader());

  return Status::OK();
}

Status HdfsParquetTableWriter::AppendRows(
    RowBatch* batch, const vector<int32_t>& row_group_indices, bool* new_file) {
  SCOPED_TIMER(parent_->encode_timer());
  *new_file = false;
  int limit;
  if (row_group_indices.empty()) {
    limit = batch->num_rows();
  } else {
    limit = row_group_indices.size();
  }

  bool all_rows = row_group_indices.empty();
  for (; row_idx_ < limit;) {
    TupleRow* current_row = all_rows ?
        batch->GetRow(row_idx_) : batch->GetRow(row_group_indices[row_idx_]);
    for (int j = 0; j < columns_.size(); ++j) {
      RETURN_IF_ERROR(columns_[j]->AppendRow(current_row));
    }
    ++row_idx_;
    ++row_count_;
    ++output_->num_rows;

    if (file_size_estimate_ > file_size_limit_) {
      // This file is full.  We need a new file.
      *new_file = true;
      return Status::OK();
    }
  }

  // We exhausted the batch, so we materialize the statistics before releasing the memory.
  for (unique_ptr<BaseColumnWriter>& column : columns_) {
    RETURN_IF_ERROR(column->MaterializeStatsValues());
  }

  // Reset the row_idx_ when we exhaust the batch.  We can exit before exhausting
  // the batch if we run out of file space and will continue from the last index.
  row_idx_ = 0;
  return Status::OK();
}

Status HdfsParquetTableWriter::Finalize() {
  SCOPED_TIMER(parent_->hdfs_write_timer());

  // At this point we write out the rest of the file.  We first update the file
  // metadata, now that all the values have been seen.
  file_metadata_.num_rows = row_count_;

  // Set the ordering used to write parquet statistics for columns in the file.
  parquet::ColumnOrder col_order = parquet::ColumnOrder();
  col_order.__set_TYPE_ORDER(parquet::TypeDefinedOrder());
  file_metadata_.column_orders.assign(columns_.size(), col_order);
  file_metadata_.__isset.column_orders = true;

  RETURN_IF_ERROR(FlushCurrentRowGroup());
  RETURN_IF_ERROR(WritePageIndex());
  for (auto& column : columns_) column->Reset();
  RETURN_IF_ERROR(WriteFileFooter());
  *stats_.mutable_parquet_stats() = parquet_dml_stats_;
  COUNTER_ADD(parent_->rows_inserted_counter(), row_count_);
  return Status::OK();
}

void HdfsParquetTableWriter::Close() {
  // Release all accumulated memory
  for (int i = 0; i < columns_.size(); ++i) {
    columns_[i]->Close();
  }
  reusable_col_mem_pool_->FreeAll();
  per_file_mem_pool_->FreeAll();
  compression_staging_buffer_.clear();
}

Status HdfsParquetTableWriter::WriteFileHeader() {
  DCHECK_EQ(file_pos_, 0);
  RETURN_IF_ERROR(Write(PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)));
  file_pos_ += sizeof(PARQUET_VERSION_NUMBER);
  file_size_estimate_ += sizeof(PARQUET_VERSION_NUMBER);
  return Status::OK();
}

Status HdfsParquetTableWriter::FlushCurrentRowGroup() {
  if (current_row_group_ == nullptr) return Status::OK();

  int num_clustering_cols = table_desc_->num_clustering_cols();
  for (int i = 0; i < columns_.size(); ++i) {
    int64_t data_page_offset, dict_page_offset;
    // Flush this column.  This updates the final metadata sizes for this column.
    RETURN_IF_ERROR(columns_[i]->Flush(&file_pos_, &data_page_offset, &dict_page_offset));
    DCHECK_GT(data_page_offset, 0);

    parquet::ColumnChunk& col_chunk = current_row_group_->columns[i];
    parquet::ColumnMetaData& col_metadata = col_chunk.meta_data;
    col_metadata.data_page_offset = data_page_offset;
    if (dict_page_offset >= 0) {
      col_metadata.__set_dictionary_page_offset(dict_page_offset);
    }

    BaseColumnWriter* col_writer = columns_[i].get();
    col_metadata.num_values = col_writer->num_values();
    col_metadata.total_uncompressed_size = col_writer->total_uncompressed_size();
    col_metadata.total_compressed_size = col_writer->total_compressed_size();
    current_row_group_->total_byte_size += col_writer->total_compressed_size();
    current_row_group_->num_rows = col_writer->num_values();
    current_row_group_->columns[i].file_offset = file_pos_;
    const string& col_name = table_desc_->col_descs()[i + num_clustering_cols].name();
    google::protobuf::Map<string,int64>* column_size_map =
        parquet_dml_stats_.mutable_per_column_size();
    (*column_size_map)[col_name] += col_writer->total_compressed_size();

    // Write encodings and encoding stats for this column
    col_metadata.encodings.clear();
    for (parquet::Encoding::type encoding : col_writer->column_encodings_) {
      col_metadata.encodings.push_back(encoding);
    }

    vector<parquet::PageEncodingStats> encoding_stats;
    // Add dictionary page encoding stats
    for (const auto& entry: col_writer->dict_encoding_stats_) {
      parquet::PageEncodingStats dict_enc_stat;
      dict_enc_stat.page_type = parquet::PageType::DICTIONARY_PAGE;
      dict_enc_stat.encoding = entry.first;
      dict_enc_stat.count = entry.second;
      encoding_stats.push_back(dict_enc_stat);
    }
    // Add data page encoding stats
    for (const auto& entry: col_writer->data_encoding_stats_) {
      parquet::PageEncodingStats data_enc_stat;
      data_enc_stat.page_type = parquet::PageType::DATA_PAGE;
      data_enc_stat.encoding = entry.first;
      data_enc_stat.count = entry.second;
      encoding_stats.push_back(data_enc_stat);
    }
    col_metadata.__set_encoding_stats(encoding_stats);

    // Build column statistics and add them to the header.
    col_writer->EncodeRowGroupStats(&current_row_group_->columns[i].meta_data);

    // Since we don't supported complex schemas, all columns should have the same
    // number of values.
    DCHECK_EQ(current_row_group_->columns[0].meta_data.num_values,
        col_writer->num_values());

    // Metadata for this column is complete, write it out to file.  The column metadata
    // goes at the end so that when we have collocated files, the column data can be
    // written without buffering.
    uint8_t* buffer = nullptr;
    uint32_t len = 0;
    RETURN_IF_ERROR(thrift_serializer_->SerializeToBuffer(
        &current_row_group_->columns[i], &len, &buffer));
    RETURN_IF_ERROR(Write(buffer, len));
    file_pos_ += len;
  }

  // Populate RowGroup::sorting_columns with all columns specified by the Frontend.
  for (int col_idx : parent_->sort_columns()) {
    current_row_group_->sorting_columns.push_back(parquet::SortingColumn());
    parquet::SortingColumn& sorting_column = current_row_group_->sorting_columns.back();
    sorting_column.column_idx = col_idx;
    sorting_column.descending = false;
    sorting_column.nulls_first = false;
  }
  current_row_group_->__isset.sorting_columns =
      !current_row_group_->sorting_columns.empty();

  current_row_group_ = nullptr;
  return Status::OK();
}

Status HdfsParquetTableWriter::WritePageIndex() {
  if (!state_->query_options().parquet_write_page_index) return Status::OK();

  // Currently Impala only write Parquet files with a single row group. The current
  // page index logic depends on this behavior as it only keeps one row group's
  // statistics in memory.
  DCHECK_EQ(file_metadata_.row_groups.size(), 1);

  parquet::RowGroup* row_group = &(file_metadata_.row_groups[0]);
  // Write out the column indexes.
  for (int i = 0; i < columns_.size(); ++i) {
    auto& column = *columns_[i];
    if (!column.valid_column_index_) continue;
    column.column_index_.__set_boundary_order(
        column.row_group_stats_base_->GetBoundaryOrder());
    // We always set null_counts.
    column.column_index_.__isset.null_counts = true;
    uint8_t* buffer = nullptr;
    uint32_t len = 0;
    RETURN_IF_ERROR(thrift_serializer_->SerializeToBuffer(
        &column.column_index_, &len, &buffer));
    RETURN_IF_ERROR(Write(buffer, len));
    // Update the column_index_offset and column_index_length of the ColumnChunk
    row_group->columns[i].__set_column_index_offset(file_pos_);
    row_group->columns[i].__set_column_index_length(len);
    file_pos_ += len;
  }
  // Write out the offset indexes.
  for (int i = 0; i < columns_.size(); ++i) {
    auto& column = *columns_[i];
    uint8_t* buffer = nullptr;
    uint32_t len = 0;
    RETURN_IF_ERROR(thrift_serializer_->SerializeToBuffer(
        &column.offset_index_, &len, &buffer));
    RETURN_IF_ERROR(Write(buffer, len));
    // Update the offset_index_offset and offset_index_length of the ColumnChunk
    row_group->columns[i].__set_offset_index_offset(file_pos_);
    row_group->columns[i].__set_offset_index_length(len);
    file_pos_ += len;
  }
  return Status::OK();
}

Status HdfsParquetTableWriter::WriteFileFooter() {
  // Write file_meta_data
  uint32_t file_metadata_len = 0;
  uint8_t* buffer = nullptr;
  RETURN_IF_ERROR(thrift_serializer_->SerializeToBuffer(
      &file_metadata_, &file_metadata_len, &buffer));
  RETURN_IF_ERROR(Write(buffer, file_metadata_len));

  // Write footer
  RETURN_IF_ERROR(Write<uint32_t>(file_metadata_len));
  RETURN_IF_ERROR(Write(PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)));
  return Status::OK();
}
