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

#include <boost/algorithm/string.hpp>
#include <boost/unordered_set.hpp>

#include "common/version.h"
#include "exec/hdfs-table-sink.h"
#include "exec/parquet/parquet-column-stats.inline.h"
#include "exec/parquet/parquet-metadata-utils.h"
#include "exec/parquet/parquet-bloom-filter-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "rpc/thrift-util.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/scoped-buffer.h"
#include "runtime/string-value.inline.h"
#include "util/bit-stream-utils.h"
#include "util/bit-util.h"
#include "util/buffer-builder.h"
#include "util/compress.h"
#include "util/debug-util.h"
#include "util/dict-encoding.h"
#include "util/hdfs-util.h"
#include "util/parquet-bloom-filter.h"
#include "util/pretty-printer.h"
#include "util/rle-encoding.h"
#include "util/string-util.h"

#include <sstream>
#include <string>

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

static const string PARQUET_MEM_LIMIT_EXCEEDED =
    "HdfsParquetTableWriter::$0() failed to allocate $1 bytes for $2.";

DEFINE_bool_hidden(write_new_parquet_dictionary_encodings, false,
    "(Experimental) Write parquet files with PLAIN/RLE_DICTIONARY encoding instead of "
    "PLAIN_DICTIONARY as recommended by Parquet 2.0 standard");

namespace impala {

// Returns the parquet::Encoding enum value to use for plain-encoded dictionary pages.
static parquet::Encoding::type DictPageEncoding() {
  return FLAGS_write_new_parquet_dictionary_encodings ?
          parquet::Encoding::PLAIN :
          parquet::Encoding::PLAIN_DICTIONARY;
}

// Returns the parquet::Encoding enum value to use for dictionary-encoded data pages.
static parquet::Encoding::type DataPageDictionaryEncoding() {
  return FLAGS_write_new_parquet_dictionary_encodings ?
          parquet::Encoding::RLE_DICTIONARY:
          parquet::Encoding::PLAIN_DICTIONARY;
}

// Base class for column writers. This contains most of the logic except for
// the type specific functions which are implemented in the subclasses.
class HdfsParquetTableWriter::BaseColumnWriter {
 public:
  // expr - the expression to generate output values for this column.
  BaseColumnWriter(HdfsParquetTableWriter* parent, ScalarExprEvaluator* expr_eval,
      const Codec::CodecInfo& codec_info, const string column_name)
    : parent_(parent),
      expr_eval_(expr_eval),
      codec_info_(codec_info),
      plain_page_size_(parent->default_plain_page_size()),
      current_page_(nullptr),
      num_values_(0),
      total_compressed_byte_size_(0),
      total_uncompressed_byte_size_(0),
      dict_encoder_base_(nullptr),
      def_levels_(nullptr),
      values_buffer_len_(DEFAULT_DATA_PAGE_SIZE),
      page_stats_base_(nullptr),
      row_group_stats_base_(nullptr),
      table_sink_mem_tracker_(parent_->parent_->mem_tracker()),
      column_name_(std::move(column_name)) {
    static_assert(std::is_base_of_v<TableSinkBase,
                                    std::remove_reference_t<decltype(*parent_->parent_)>>,
        "'table_sink_mem_tracker_' must point to the mem tracker of a TableSinkBase");
    def_levels_ = parent_->state_->obj_pool()->Add(
        new RleEncoder(parent_->reusable_col_mem_pool_->Allocate(DEFAULT_DATA_PAGE_SIZE),
                       DEFAULT_DATA_PAGE_SIZE, 1));
    values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(values_buffer_len_);
  }

  virtual ~BaseColumnWriter() {}

  // Called after the constructor to initialize the column writer.
  Status Init() WARN_UNUSED_RESULT {
    Reset();
    RETURN_IF_ERROR(Codec::CreateCompressor(nullptr, false, codec_info_, &compressor_));
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
    return ConvertImpalaToParquetCodec(codec_info_.format_);
  }
  const std::string& column_name() { return column_name_; }

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

  // Returns the bytes needed to represent the value in a PLAIN encoded page.
  virtual int64_t BytesNeededFor(void* value) = 0;

  // Increases the page size to be able to hold a value of size bytes_needed.
  Status GrowPageSize(int64_t bytes_needed) WARN_UNUSED_RESULT;

  // Subclasses can override this function to convert values after the expression was
  // evaluated. Used by int64 timestamp writers to change the TimestampValue returned by
  // the expression to an int64.
  virtual void* ConvertValue(void* value) { return value; }

  // Some subclasses may write a ParquetBloomFilter, in which case they should override
  // this method.
  virtual const ParquetBloomFilter* GetParquetBloomFilter() const {
    return nullptr;
  }

  // Some subclasses need to flush their dictionary to a bloom filter when full.
  virtual void FlushDictionaryToParquetBloomFilterIfNeeded() { }

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

  Codec::CodecInfo codec_info_;

  // Compression codec for this column.  If nullptr, this column is will not be
  // compressed.
  scoped_ptr<Codec> compressor_;

  // Size of newly created PLAIN encoded pages. Defaults to DEFAULT_DATA_PAGE_SIZE or to
  // the value of 'write.parquet.page-size-bytes' table property for Iceberg tables.
  // Its value is increased when pages are not big enough. This only happens when there
  // are enough unique values such that we switch from PLAIN_DICTIONARY/RLE_DICTIONARY to
  // PLAIN encoding and then have very large values (i.e. greater than
  // DEFAULT_DATA_PAGE_SIZE).
  // TODO: Consider removing and only creating a single large page as necessary.
  int64_t plain_page_size_;

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

  // Column name in the HdfsTableDescriptor.
  const string column_name_;
};

// Per type column writer.
template<typename T>
class HdfsParquetTableWriter::ColumnWriter :
    public HdfsParquetTableWriter::BaseColumnWriter {
 public:
  ColumnWriter(HdfsParquetTableWriter* parent, ScalarExprEvaluator* eval,
      const Codec::CodecInfo& codec_info, const std::string& col_name)
    : BaseColumnWriter(parent, eval, codec_info, col_name),
      num_values_since_dict_size_check_(0),
      parquet_bloom_filter_bytes_(0),
      parquet_bloom_filter_buffer_(table_sink_mem_tracker_),
      plain_encoded_value_size_(
          ParquetPlainEncoder::EncodedByteSize(eval->root().type())) {
    DCHECK_NE(eval->root().type().type, TYPE_BOOLEAN);

    const std::map<string, int64_t>& col_to_size =
      parent->parent_->GetParquetBloomFilterColumns();
    const auto it = col_to_size.find(column_name());

    if (GetBloomFilterWriteOption() == TParquetBloomFilterWrite::NEVER
        || it == col_to_size.end()) {
      // Parquet Bloom filtering is disabled altogether or is not turned on for this
      // column.
      parquet_bloom_filter_state_ = ParquetBloomFilterState::DISABLED;
    } else {
      // Parquet Bloom filtering is enabled for this column either immediately or if
      // falling back from dict encoding.
      parquet_bloom_filter_state_ = ParquetBloomFilterState::UNINITIALIZED;

      // It is the responsibility of the FE to enforce the below constraints.
      parquet_bloom_filter_bytes_ = it->second;
      DCHECK_LE(parquet_bloom_filter_bytes_, ParquetBloomFilter::MAX_BYTES);
      DCHECK_GE(parquet_bloom_filter_bytes_, ParquetBloomFilter::MIN_BYTES);
      DCHECK(BitUtil::IsPowerOf2(parquet_bloom_filter_bytes_));
    }
    parquet_type_ = ParquetMetadataUtils::ConvertInternalToParquetType(type().type,
        parent_->timestamp_type_);
  }

  void Reset() override {
    BaseColumnWriter::Reset();
    valid_column_index_ = true;
    // Default to dictionary encoding.  If the cardinality ends up being too high,
    // it will fall back to plain.
    current_encoding_ = DataPageDictionaryEncoding();
    next_page_encoding_ = DataPageDictionaryEncoding();
    dict_encoder_.reset(new DictEncoder<T>(parent_->per_file_mem_pool_.get(),
        plain_encoded_value_size_, parent_->parent_->mem_tracker()));
    dict_encoder_base_ = dict_encoder_.get();
    page_stats_.reset(
        new ColumnStats<T>(parent_->per_file_mem_pool_.get(), plain_encoded_value_size_));
    page_stats_base_ = page_stats_.get();
    row_group_stats_.reset(
        new ColumnStats<T>(parent_->per_file_mem_pool_.get(), plain_encoded_value_size_));
    if (parquet_bloom_filter_state_ != ParquetBloomFilterState::DISABLED) {
      Status status = InitParquetBloomFilter();
      if (!status.ok()) {
        VLOG(google::WARNING)
            << "Failed to initialise Parquet Bloom filter for column "
            << column_name() << "."
            << " Error message: " << status.msg().msg();
        ReleaseParquetBloomFilterResources();
      }
    }
    row_group_stats_base_ = row_group_stats_.get();
  }

 protected:
  bool ProcessValue(void* value, int64_t* bytes_needed) override {
    T* val = CastValue(value);
    if (IsDictionaryEncoding(current_encoding_)) {
      if (UNLIKELY(num_values_since_dict_size_check_ >=
                   DICTIONARY_DATA_PAGE_SIZE_CHECK_PERIOD)) {
        num_values_since_dict_size_check_ = 0;
        if (dict_encoder_->EstimatedDataEncodedSize() >= parent_->dict_page_size()) {
          return false;
        }
      }
      ++num_values_since_dict_size_check_;
      *bytes_needed = dict_encoder_->Put(*val);
      // If the dictionary contains the maximum number of values, the current page is
      // full and must be written out. FinalizeCurrentPage handles dict cleanup.
      if (UNLIKELY(*bytes_needed < 0)) {
        return false;
      }
      parent_->file_size_estimate_ += *bytes_needed;
    } else if (current_encoding_ == parquet::Encoding::PLAIN) {
      *bytes_needed = plain_encoded_value_size_ < 0 ?
          ParquetPlainEncoder::ByteSize<T>(*val) :
          plain_encoded_value_size_;
      if (current_page_->header.uncompressed_page_size + *bytes_needed >
          plain_page_size_) {
        // Shouldn't happen on an empty page as it should be sized for bytes_needed.
        DCHECK_GT(current_page_->header.uncompressed_page_size, 0);
        return false;
      }
      uint8_t* dst_ptr = values_buffer_ + current_page_->header.uncompressed_page_size;
      int64_t written_len =
          ParquetPlainEncoder::Encode(*val, plain_encoded_value_size_, dst_ptr);
      DCHECK_EQ(*bytes_needed, written_len);
      current_page_->header.uncompressed_page_size += written_len;
    } else {
      // TODO: support other encodings here
      DCHECK(false);
    }
    // IMPALA-8498: Write column index for floating types when NaN is not present
    if (std::is_same<float, std::remove_cv_t<T>>::value &&
        UNLIKELY(std::isnan(*static_cast<float*>(value)))) {
      valid_column_index_ = false;
    } else if (std::is_same<double, std::remove_cv_t<T>>::value &&
        UNLIKELY(std::isnan(*static_cast<double*>(value)))) {
      valid_column_index_ = false;
    }

    page_stats_->Update(*val);
    UpdateParquetBloomFilterIfNeeded(val);

    return true;
  }

  int64_t BytesNeededFor(void* value) override {
    if (plain_encoded_value_size_ >= 0) return plain_encoded_value_size_;
    T* val = CastValue(value);
    return ParquetPlainEncoder::ByteSize<T>(*val);
  }

  const ParquetBloomFilter* GetParquetBloomFilter() const override {
    return parquet_bloom_filter_.get();
  }

  void FlushDictionaryToParquetBloomFilterIfNeeded() override {
    if (parquet_bloom_filter_state_
        == ParquetBloomFilterState::WAIT_FOR_FALLBACK_FROM_DICT) {
      parquet_bloom_filter_state_ = ParquetBloomFilterState::ENABLED;

      // Write dictionary keys to Parquet Bloom filter if we haven't been filling it so
      // far (and Bloom filtering is enabled). If there are too many values for a
      // dictionary, a Bloom filter may still be useful.
      Status status = DictKeysToParquetBloomFilter();
      if (!status.ok()) {
        VLOG(google::WARNING)
            << "Failed to add dictionary keys to Parquet Bloom filter for column "
            << column_name()
            << " when falling back from dictionary encoding to plain encoding."
            << " Error message: " << status.msg().msg();
        parquet_bloom_filter_state_ = ParquetBloomFilterState::FAILED;
        ReleaseParquetBloomFilterResources();
      }
    }
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


  enum struct ParquetBloomFilterState {
    /// Parquet Bloom filtering is turned off either completely or for this column.
    DISABLED,

    /// The Parquet Bloom filter needs to be initialised before being used.
    UNINITIALIZED,

    /// The Bloom filter has been initialised but it is not being used as the dictionary
    /// can hold all elements. If the dictionary becomes full and there are still new
    /// elements, we fall back from dictionary encoding to plain encoding and start using
    /// the Bloom filter.
    WAIT_FOR_FALLBACK_FROM_DICT,

    /// The Parquet Bloom filter is being used.
    ENABLED,

    /// An error occured with the Parquet Bloom filter so we are not using it.
    FAILED
  };

  ParquetBloomFilterState parquet_bloom_filter_state_;
  uint64_t parquet_bloom_filter_bytes_;
  ScopedBuffer parquet_bloom_filter_buffer_;

  // The parquet type corresponding to this->type(). Needed by the Parquet Bloom filter.
  parquet::Type::type parquet_type_;

  // Buffer used when converting values to the form that is used for hashing and insertion
  // into the ParquetBloomFilter. The conversion function, 'BytesToParquetType' requires a
  // vector to be able to allocate space if necessary. However, by caching the allocated
  // buffer here we avoid the overhead of allocation for every conversion - when
  // 'BytesToParquetType' calls 'resize' on the vector it will already have at least the
  // desired length in most (or all) cases.
  //
  // We prellocate 16 bytes because that is the longest fixed size type (except for fixed
  // length arrays).
  std::vector<uint8_t> parquet_bloom_conversion_buffer{16, 0};

  // The ParquetBloomFilter object if one is being written. If
  // 'ShouldInitParquetBloomFilter()' is false, the combination of the impala type and the
  // parquet type is not supported or some error occurs during the initialisation of the
  // ParquetBloomFilter object, it is set to NULL.
  unique_ptr<ParquetBloomFilter> parquet_bloom_filter_;

  // Converts a slot pointer to a raw value suitable for encoding
  inline T* CastValue(void* value) {
    return reinterpret_cast<T*>(value);
  }

  Status InitParquetBloomFilter() WARN_UNUSED_RESULT {
    DCHECK(parquet_bloom_filter_state_ != ParquetBloomFilterState::DISABLED);
    const ColumnType& impala_type = type();
    if (!IsParquetBloomFilterSupported(parquet_type_, impala_type)) {
      stringstream ss;
      ss << "Parquet Bloom filtering not supported for parquet type " << parquet_type_
          << " and impala type " << impala_type << ".";
      return Status::Expected(ss.str());
    }

    parquet_bloom_filter_buffer_.Release();
    if (!parquet_bloom_filter_buffer_.TryAllocate(parquet_bloom_filter_bytes_)) {
      parquet_bloom_filter_state_ = ParquetBloomFilterState::FAILED;
      return Status(Substitute("Could not allocate buffer of $0 bytes for Parquet "
            "Bloom filter data when writing column '$1'.",
            parquet_bloom_filter_bytes_, column_name()));
    }
    std::memset(parquet_bloom_filter_buffer_.buffer(), 0,
        parquet_bloom_filter_buffer_.Size());

    parquet_bloom_filter_ = make_unique<ParquetBloomFilter>();
    Status status = parquet_bloom_filter_->Init(parquet_bloom_filter_buffer_.buffer(),
          parquet_bloom_filter_buffer_.Size(), true);

    if (!status.ok()) {
      parquet_bloom_filter_state_ = ParquetBloomFilterState::FAILED;
      return status;
    } else {
      const bool should_wait_for_fallback =
          (GetBloomFilterWriteOption() == TParquetBloomFilterWrite::IF_NO_DICT)
          && IsDictionaryEncoding(current_encoding_);
      parquet_bloom_filter_state_ = should_wait_for_fallback
          ? ParquetBloomFilterState::WAIT_FOR_FALLBACK_FROM_DICT
          : ParquetBloomFilterState::ENABLED;;
      return Status::OK();
    }
  }

  void UpdateParquetBloomFilterIfNeeded(const void* val) {
    if (parquet_bloom_filter_state_ == ParquetBloomFilterState::ENABLED) {
      Status status = UpdateParquetBloomFilter(val);
      if (!status.ok()) {
        // If an error happens, for example conversion to the form expected by the Bloom
        // filter fails, we stop writing the Bloom filter and release resources associated
        // with it.
        VLOG(google::WARNING)
            << "An error happened updating Parquet Bloom filter in column "
            << column_name() << " at row idx " << parent_->row_idx_ << "."
            << " Error message: " << status.msg().msg();
        parquet_bloom_filter_state_ = ParquetBloomFilterState::FAILED;
        ReleaseParquetBloomFilterResources();
      }
    }
  }

  Status UpdateParquetBloomFilter(const void* val) WARN_UNUSED_RESULT {
    DCHECK(parquet_bloom_filter_state_ == ParquetBloomFilterState::ENABLED);
    DCHECK(parquet_bloom_filter_ != nullptr);

    uint8_t* ptr = nullptr;
    size_t len = -1;
    const ColumnType& impala_type = type();
    RETURN_IF_ERROR(BytesToParquetType(val, impala_type, parquet_type_,
        &parquet_bloom_conversion_buffer, &ptr, &len));
    DCHECK(ptr != nullptr);
    DCHECK(len != -1);
    parquet_bloom_filter_->HashAndInsert(ptr, len);

    return Status::OK();
  }

  void ReleaseParquetBloomFilterResources() {
    parquet_bloom_filter_ = nullptr;
    parquet_bloom_filter_buffer_.Release();
  }

  Status DictKeysToParquetBloomFilter() {
    return dict_encoder_->ForEachDictKey([this](const T& value) {
        return UpdateParquetBloomFilter(&value);
        });
  }

  TParquetBloomFilterWrite::type GetBloomFilterWriteOption() {
    return parent_->state_->query_options().parquet_bloom_filter_write;
  }

 protected:
  // Size of each encoded value in plain encoding. -1 if the type is variable-length.
  int64_t plain_encoded_value_size_;
};

template<>
inline StringValue* HdfsParquetTableWriter::ColumnWriter<StringValue>::CastValue(
    void* value) {
  if (type().type == TYPE_CHAR) {
    char* s = reinterpret_cast<char*>(value);
    int len = StringValue::UnpaddedCharLength(s, type().len);
    temp_.Assign(reinterpret_cast<char*>(value), len);
    return &temp_;
  }
  return reinterpret_cast<StringValue*>(value);
}

// Bools are encoded a bit differently so subclass it explicitly.
class HdfsParquetTableWriter::BoolColumnWriter :
    public HdfsParquetTableWriter::BaseColumnWriter {
 public:
  BoolColumnWriter(HdfsParquetTableWriter* parent, ScalarExprEvaluator* eval,
      const Codec::CodecInfo& codec_info, const std::string& col_name)
    : BaseColumnWriter(parent, eval, codec_info, col_name),
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
  bool ProcessValue(void* value, int64_t* bytes_needed) override {
    bool v = *reinterpret_cast<bool*>(value);
    if (!bool_values_->PutValue(v, 1)) return false;
    page_stats_.Update(v);
    return true;
  }

  int64_t BytesNeededFor(void* value) override {
    return 1;
  }

  Status FinalizeCurrentPage() override {
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
 Int64TimestampColumnWriterBase(HdfsParquetTableWriter* parent, ScalarExprEvaluator* eval,
     const Codec::CodecInfo& codec_info, const std::string& col_name)
   : HdfsParquetTableWriter::ColumnWriter<int64_t>(parent, eval, codec_info, col_name) {
   int64_t dummy;
   plain_encoded_value_size_ = ParquetPlainEncoder::ByteSize(dummy);
  }

protected:
  virtual void* ConvertValue(void* value) override {
    if (UNLIKELY(value == nullptr)) return nullptr;
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
     ScalarExprEvaluator* eval, const Codec::CodecInfo& codec_info,
     const std::string& col_name)
   : HdfsParquetTableWriter::Int64TimestampColumnWriterBase(
       parent, eval, codec_info, col_name) {}

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
     ScalarExprEvaluator* eval, const Codec::CodecInfo& codec_info,
     const std::string& col_name)
   : HdfsParquetTableWriter::Int64TimestampColumnWriterBase(
       parent, eval, codec_info, col_name) {}

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
 Int64NanoTimestampColumnWriter(HdfsParquetTableWriter* parent, ScalarExprEvaluator* eval,
     const Codec::CodecInfo& codec_info, const std::string& col_name)
   : HdfsParquetTableWriter::Int64TimestampColumnWriterBase(
       parent, eval, codec_info, col_name) {}

protected:
  virtual bool ConvertTimestamp(const TimestampValue& ts, int64_t* result) {
    return ts.UtcToUnixTimeLimitedRangeNanos(result);
  }
};

inline Status HdfsParquetTableWriter::BaseColumnWriter::AppendRow(TupleRow* row) {
  ++num_values_;
  void* value = ConvertValue(expr_eval_->GetValue(row));
  if (current_page_ == nullptr) NewPage();

  int64_t bytes_needed = 0;
  if (ShouldStartNewPage()) {
    RETURN_IF_ERROR(FinalizeCurrentPage());

    if (value != nullptr) {
      // Ensure the new page can hold the value so we don't create an empty page.
      bytes_needed = BytesNeededFor(value);
      if (UNLIKELY(bytes_needed > plain_page_size_)) {
        RETURN_IF_ERROR(GrowPageSize(bytes_needed));
      }
    }
    NewPage();
  }

  // Encoding may fail for several reasons - because the current page is not big enough,
  // because we've encoded the maximum number of unique dictionary values and need to
  // switch to plain encoding, etc. In these events, we finalize and create a new page.
  // Nulls don't get encoded. Increment the null count of the parquet statistics.
  if (value == nullptr) {
    DCHECK(page_stats_base_ != nullptr);
    page_stats_base_->IncrementNullCount(1);
  } else if (ProcessValue(value, &bytes_needed)) {
    // Succesfully appended.
    ++current_page_->num_non_null;
  } else {
    // Value didn't fit on page, try again on a new page.
    RETURN_IF_ERROR(FinalizeCurrentPage());

    // Check how much space is needed to write this value. If that is larger than the
    // page size then increase page size and try again.
    if (UNLIKELY(bytes_needed > plain_page_size_)) {
      RETURN_IF_ERROR(GrowPageSize(bytes_needed));
    }
    NewPage();

    // Try again. This must succeed as we've created a new page for this value.
    bool ret = ProcessValue(value, &bytes_needed);
    DCHECK(ret);
    ++current_page_->num_non_null;
  }

  // Now that the value has been successfully written, write the definition level.
  bool ret = def_levels_->Put(value != nullptr);
  // Writing the def level will succeed because we ensured there was enough space for it
  // above, and new pages will always have space for at least a single def level.
  DCHECK(ret);
  ++current_page_->header.data_page_header.num_values;

  return Status::OK();
}

inline Status HdfsParquetTableWriter::BaseColumnWriter::GrowPageSize(
    int64_t bytes_needed) {
  if (bytes_needed > MAX_DATA_PAGE_SIZE) {
    return Status(Substitute("Cannot write value of size $0 to a Parquet data page that "
        "exceeds the max page limit $1.",
        PrettyPrinter::Print(bytes_needed, TUnit::BYTES),
        PrettyPrinter::Print(MAX_DATA_PAGE_SIZE , TUnit::BYTES)));
  }
  plain_page_size_ = bytes_needed;
  values_buffer_len_ = plain_page_size_;
  values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(values_buffer_len_);
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
    dict_header.encoding = DictPageEncoding();
    ++dict_encoding_stats_[dict_header.encoding];

    parquet::PageHeader header;
    header.type = parquet::PageType::DICTIONARY_PAGE;
    header.uncompressed_page_size = dict_encoder_base_->dict_encoded_size();
    header.__set_dictionary_page_header(dict_header);

    // Write the dictionary page data, compressing it if necessary.
    uint8_t* dict_buffer =
        parent_->per_file_mem_pool_->TryAllocate(header.uncompressed_page_size);
    if (UNLIKELY(dict_buffer == nullptr)) {
      string details = (Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "BaseColumnWriter::Flush",
          header.uncompressed_page_size, "dictionary page"));
      return parent_->per_file_mem_pool_->mem_tracker()->MemLimitExceeded(
          parent_->state_, details, header.uncompressed_page_size);
    }
    dict_encoder_base_->WriteDict(dict_buffer);
    if (compressor_.get() != nullptr) {
      SCOPED_TIMER(parent_->parent_->compress_timer());
      int64_t max_compressed_size =
          compressor_->MaxOutputLen(header.uncompressed_page_size);
      DCHECK_GT(max_compressed_size, 0);
      uint8_t* compressed_data =
          parent_->per_file_mem_pool_->TryAllocate(max_compressed_size);
      if (UNLIKELY(compressed_data == nullptr)) {
        string details =
            (Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "BaseColumnWriter::Flush",
                max_compressed_size, "compressed dictionary page"));
        return parent_->per_file_mem_pool_->mem_tracker()->MemLimitExceeded(
            parent_->state_, details, max_compressed_size);
      }
      header.compressed_page_size = max_compressed_size;
      const Status& status =
          compressor_->ProcessBlock32(true, header.uncompressed_page_size, dict_buffer,
              &header.compressed_page_size, &compressed_data);
      if (!status.ok()) {
        return Status(Substitute("Error writing parquet file '$0' column '$1': $2",
            parent_->output_->current_file_name, column_name(), status.GetDetail()));
      }
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
    // There should be no empty pages.
    DCHECK_NE(page.header.data_page_header.num_values, 0);

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
  DCHECK_NE(current_page_->header.data_page_header.num_values, 0);
  if (current_page_->finalized) return Status::OK();

  // If the entire page was NULL, encode it as PLAIN since there is no
  // data anyway. We don't output a useless dictionary page and it works
  // around a parquet MR bug (see IMPALA-759 for more details).
  if (current_page_->num_non_null == 0) current_encoding_ = parquet::Encoding::PLAIN;

  if (IsDictionaryEncoding(current_encoding_)) {
    // If the dictionary contains the maximum number of values, switch to plain
    // encoding for the next page and flush the dictionary as well.
    if (UNLIKELY(dict_encoder_base_->IsFull())) {
      FlushDictionaryToParquetBloomFilterIfNeeded();
      next_page_encoding_ = parquet::Encoding::PLAIN;
    }

    WriteDictDataPage();
  }

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
    const Status& status =
        compressor_->ProcessBlock32(true, header.uncompressed_page_size,
            uncompressed_data, &header.compressed_page_size, &compressed_data);
    if (!status.ok()) {
      return Status(Substitute("Error writing parquet file '$0' column '$1': $2",
          parent_->output_->current_file_name, column_name(), status.GetDetail()));
    }
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
  RETURN_IF_ERROR(row_group_stats_base_->MaterializeStringValuesToInternalBuffers());
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

HdfsParquetTableWriter::
HdfsParquetTableWriter(TableSinkBase* parent, RuntimeState* state,
    OutputPartition* output, const HdfsPartitionDescriptor* part_desc,
    const HdfsTableDescriptor* table_desc)
  : HdfsTableWriter(parent, state, output, part_desc, table_desc),
    thrift_serializer_(new ThriftSerializer(true)),
    current_row_group_(nullptr),
    row_count_(0),
    file_size_limit_(0),
    reusable_col_mem_pool_(new MemPool(parent_->mem_tracker())),
    per_file_mem_pool_(new MemPool(parent_->mem_tracker())),
    row_idx_(0),
    default_block_size_(0),
    default_plain_page_size_(0),
    dict_page_size_(0) {
  is_iceberg_file_ = table_desc->IsIcebergTable();
}

HdfsParquetTableWriter::~HdfsParquetTableWriter() {
}

void HdfsParquetTableWriter::Configure(int num_cols) {
  DCHECK(!is_iceberg_file_);

  timestamp_type_ = state_->query_options().parquet_timestamp_type;

  string_utf8_ = state_->query_options().parquet_annotate_strings_utf8;

  if (state_->query_options().__isset.parquet_file_size &&
      state_->query_options().parquet_file_size > 0) {
    // If the user specified a value explicitly, use it. InitNewFile() will verify that
    // the actual file's block size is sufficient.
    default_block_size_ = state_->query_options().parquet_file_size;
  } else {
    default_block_size_ = HDFS_BLOCK_SIZE;
    // Blocks are usually HDFS_BLOCK_SIZE bytes, unless there are many columns, in
    // which case a per-column minimum kicks in.
    default_block_size_ = max(default_block_size_, MinBlockSize(num_cols));
  }
  // HDFS does not like block sizes that are not aligned
  default_block_size_ = BitUtil::RoundUp(default_block_size_, HDFS_BLOCK_ALIGNMENT);

  default_plain_page_size_ = DEFAULT_DATA_PAGE_SIZE;
  dict_page_size_ = DEFAULT_DATA_PAGE_SIZE;
}

void HdfsParquetTableWriter::ConfigureForIceberg(int num_cols) {
  DCHECK(is_iceberg_file_);

  // The Iceberg spec states that timestamps are stored as INT64 micros.
  timestamp_type_ = TParquetTimestampType::INT64_MICROS;

  string_utf8_ = true;

  if (state_->query_options().__isset.parquet_file_size &&
      state_->query_options().parquet_file_size > 0) {
    // If the user specified a value explicitly, use it. InitNewFile() will verify that
    // the actual file's block size is sufficient.
    default_block_size_ = state_->query_options().parquet_file_size;
  } else if (table_desc_->IcebergParquetRowGroupSize() > 0) {
    // If the user specified a value explicitly, use it. InitNewFile() will verify that
    // the actual file's block size is sufficient.
    default_block_size_ = table_desc_->IcebergParquetRowGroupSize();
  } else {
    default_block_size_ = HDFS_BLOCK_SIZE;
    // Blocks are usually HDFS_BLOCK_SIZE bytes, unless there are many columns, in
    // which case a per-column minimum kicks in.
    default_block_size_ = max(default_block_size_, MinBlockSize(num_cols));
  }
  // HDFS does not like block sizes that are not aligned
  default_block_size_ = BitUtil::RoundUp(default_block_size_, HDFS_BLOCK_ALIGNMENT);

  default_plain_page_size_ = table_desc_->IcebergParquetPlainPageSize();
  if (default_plain_page_size_ <= 0) default_plain_page_size_ = DEFAULT_DATA_PAGE_SIZE;

  dict_page_size_ = table_desc_->IcebergParquetDictPageSize();
  if (dict_page_size_ <= 0) dict_page_size_ = DEFAULT_DATA_PAGE_SIZE;
}

Status HdfsParquetTableWriter::Init() {
  // Initialize file metadata
  file_metadata_.version = PARQUET_WRITER_VERSION;

  stringstream created_by;
  created_by << "impala version " << GetDaemonBuildVersion()
             << " (build " << GetDaemonBuildHash() << ")";
  file_metadata_.__set_created_by(created_by.str());

  // Default to snappy compressed
  THdfsCompression::type codec = THdfsCompression::SNAPPY;
  // Compression level only supported for zstd.
  int clevel = ZSTD_CLEVEL_DEFAULT;
  const TQueryOptions& query_options = state_->query_options();
  if (query_options.__isset.compression_codec) {
    codec = query_options.compression_codec.codec;
    clevel = query_options.compression_codec.compression_level;
  } else if (table_desc_->IsIcebergTable()) {
    TCompressionCodec compression_codec = table_desc_->IcebergParquetCompressionCodec();
    codec = compression_codec.codec;
    if (compression_codec.__isset.compression_level) {
      clevel = compression_codec.compression_level;
    }
  }

  if (!(codec == THdfsCompression::NONE ||
        codec == THdfsCompression::GZIP ||
        codec == THdfsCompression::SNAPPY ||
        codec == THdfsCompression::ZSTD ||
        codec == THdfsCompression::LZ4)) {
    stringstream ss;
    ss << "Invalid parquet compression codec " << Codec::GetCodecName(codec);
    return Status(ss.str());
  }

  // Map parquet codecs to Impala codecs. Its important to do the above check before
  // we do any mapping.
  // Parquet supports codecs enumerated in parquet::CompressionCodec. Impala supports
  // codecs enumerated in impala::THdfsCompression. In most cases, Impala codec and
  // Parquet codec refer to the same codec. The only exception is LZ4. For Hadoop
  // compatibility parquet::CompressionCodec::LZ4 refers to THdfsCompression::LZ4_BLOCKED
  // and not THdfsCompression::LZ4. Hence, the following mapping and re-mapping to ensure
  // that the input THdfsCompression::LZ4 codec gets mapped to
  // THdfsCompression::LZ4_BLOCKED for parquet.
  parquet::CompressionCodec::type parquet_codec = ConvertImpalaToParquetCodec(codec);
  codec = ConvertParquetToImpalaCodec(parquet_codec);

  VLOG_FILE << "Using compression codec: " << codec;
  if (codec == THdfsCompression::ZSTD) {
    VLOG_FILE << "Using compression level: " << clevel;
  }

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

  Codec::CodecInfo codec_info(codec, clevel);

  if (is_iceberg_file_) {
    ConfigureForIceberg(num_cols);
  } else {
    Configure(num_cols);
  }

  columns_.resize(num_cols);
  // Initialize each column structure.
  for (int i = 0; i < columns_.size(); ++i) {
    BaseColumnWriter* writer = nullptr;
    const ColumnType& type = output_expr_evals_[i]->root().type();
    const int num_clustering_cols = table_desc_->num_clustering_cols();
    const string& col_name = table_desc_->col_descs()[i + num_clustering_cols].name();
    switch (type.type) {
      case TYPE_BOOLEAN:
        writer =
          new BoolColumnWriter(this, output_expr_evals_[i], codec_info, col_name);
        break;
      case TYPE_TINYINT:
        writer =
          new ColumnWriter<int8_t>(this, output_expr_evals_[i], codec_info, col_name);
        break;
      case TYPE_SMALLINT:
        writer =
          new ColumnWriter<int16_t>(this, output_expr_evals_[i], codec_info, col_name);
        break;
      case TYPE_INT:
        writer =
          new ColumnWriter<int32_t>(this, output_expr_evals_[i], codec_info, col_name);
        break;
      case TYPE_BIGINT:
        writer =
          new ColumnWriter<int64_t>(this, output_expr_evals_[i], codec_info, col_name);
        break;
      case TYPE_FLOAT:
        writer =
          new ColumnWriter<float>(this, output_expr_evals_[i], codec_info, col_name);
        break;
      case TYPE_DOUBLE:
        writer =
          new ColumnWriter<double>(this, output_expr_evals_[i], codec_info, col_name);
        break;
      case TYPE_TIMESTAMP:
        switch (timestamp_type_) {
          case TParquetTimestampType::INT96_NANOS:
            writer =
                new ColumnWriter<TimestampValue>(
                    this, output_expr_evals_[i], codec_info, col_name);
            break;
          case TParquetTimestampType::INT64_MILLIS:
            writer = new Int64MilliTimestampColumnWriter(
                this, output_expr_evals_[i], codec_info, col_name);
            break;
          case TParquetTimestampType::INT64_MICROS:
            writer = new Int64MicroTimestampColumnWriter(
                this, output_expr_evals_[i], codec_info, col_name);
            break;
          case TParquetTimestampType::INT64_NANOS:
            writer = new Int64NanoTimestampColumnWriter(
                this, output_expr_evals_[i], codec_info, col_name);
            break;
          default:
            DCHECK(false);
        }
        break;
      case TYPE_VARCHAR:
      case TYPE_STRING:
      case TYPE_CHAR:
        writer = new ColumnWriter<StringValue>(
            this, output_expr_evals_[i], codec_info, col_name);
        break;
      case TYPE_DECIMAL:
        switch (output_expr_evals_[i]->root().type().GetByteSize()) {
          case 4:
            writer =
                new ColumnWriter<Decimal4Value>(
                    this, output_expr_evals_[i], codec_info, col_name);
            break;
          case 8:
            writer =
                new ColumnWriter<Decimal8Value>(
                    this, output_expr_evals_[i], codec_info, col_name);
            break;
          case 16:
            writer =
                new ColumnWriter<Decimal16Value>(
                    this, output_expr_evals_[i], codec_info, col_name);
            break;
          default:
            DCHECK(false);
        }
        break;
      case TYPE_DATE:
        writer = new ColumnWriter<DateValue>(
            this, output_expr_evals_[i], codec_info, col_name);
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
  // Create flattened tree with a single root.
  file_metadata_.schema.resize(columns_.size() + 1);
  file_metadata_.schema[0].__set_num_children(columns_.size());
  file_metadata_.schema[0].name = "schema";
  const int num_clustering_cols = table_desc_->num_clustering_cols();

  for (int i = 0; i < columns_.size(); ++i) {
    parquet::SchemaElement& col_schema = file_metadata_.schema[i + 1];
    const ColumnType& col_type = output_expr_evals_[i]->root().type();
    col_schema.name = columns_[i]->column_name();
    const ColumnDescriptor& col_desc = table_desc_->col_descs()[i + num_clustering_cols];
    DCHECK_EQ(col_desc.name(), columns_[i]->column_name());
    const int field_id = col_desc.field_id();
    if (field_id != -1) col_schema.__set_field_id(field_id);
    ParquetMetadataUtils::FillSchemaElement(col_type, string_utf8_, timestamp_type_,
        &col_schema);
  }

  return Status::OK();
}

Status HdfsParquetTableWriter::AddRowGroup() {
  if (current_row_group_ != nullptr) RETURN_IF_ERROR(FlushCurrentRowGroup());
  file_metadata_.row_groups.push_back(parquet::RowGroup());
  current_row_group_ = &file_metadata_.row_groups[file_metadata_.row_groups.size() - 1];

  // Initialize new row group metadata.
  current_row_group_->columns.resize(columns_.size());
  for (int i = 0; i < columns_.size(); ++i) {
    parquet::ColumnMetaData metadata;
    metadata.type = ParquetMetadataUtils::ConvertInternalToParquetType(
        columns_[i]->type().type, timestamp_type_);
    metadata.path_in_schema.push_back(columns_[i]->column_name());
    metadata.codec = columns_[i]->GetParquetCodec();
    current_row_group_->columns[i].__set_meta_data(metadata);
  }

  return Status::OK();
}

int64_t HdfsParquetTableWriter::MinBlockSize(int64_t num_file_cols) const {
  // See file_size_limit_ calculation in InitNewFile().
  return 3 * DEFAULT_DATA_PAGE_SIZE * num_file_cols;
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
    ++output_->current_file_rows;

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
  RETURN_IF_ERROR(state_->CheckQueryState());
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
  FinalizePartitionInfo();
  return Status::OK();
}

void HdfsParquetTableWriter::FinalizePartitionInfo() {
  output_->current_file_bytes = file_pos_;
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

Status HdfsParquetTableWriter::WriteParquetBloomFilter(BaseColumnWriter* col_writer,
    parquet::ColumnMetaData* meta_data) {
  DCHECK(col_writer != nullptr);
  DCHECK(meta_data != nullptr);

  const ParquetBloomFilter* bloom_filter = col_writer->GetParquetBloomFilter();
  if (bloom_filter == nullptr || bloom_filter->AlwaysFalse()) {
    // If there is no Bloom filter for this column or if it is empty we don't need to do
    // anything.
    // If bloom_filter->AlwaysFalse() is true, it means the Bloom filter was initialised
    // but no element was inserted, probably because we have not fallen back to plain
    // encoding from dictionary encoding.
    return Status::OK();
  }

  // Update metadata.
  meta_data->__set_bloom_filter_offset(file_pos_);

  // Write the header to the file.
  parquet::BloomFilterHeader header = CreateBloomFilterHeader(*bloom_filter);
  uint8_t* buffer = nullptr;
  uint32_t len = 0;
  RETURN_IF_ERROR(thrift_serializer_->SerializeToBuffer(&header, &len, &buffer));
  DCHECK(buffer != nullptr);
  DCHECK_GT(len, 0);
  RETURN_IF_ERROR(Write(buffer, len));
  file_pos_ += len;

  // Write the Bloom filter directory (bitset) to the file.
  const uint8_t* directory = bloom_filter->directory();
  const int64_t directory_size = bloom_filter->directory_size();
  RETURN_IF_ERROR(Write(directory, directory_size));
  file_pos_ += directory_size;

  return Status::OK();
}

void HdfsParquetTableWriter::CollectIcebergDmlFileColumnStats(int field_id,
    const BaseColumnWriter* col_writer) {
  // Each data file consists of a single row group, so row group null_count / min / max
  // stats can be used as data file stats.
  // Get column_size from column writer.
  col_writer->row_group_stats_base_->GetIcebergStats(
      col_writer->total_compressed_size(), col_writer->num_values(),
      &iceberg_file_stats_[field_id]);
}

Status HdfsParquetTableWriter::FlushCurrentRowGroup() {
  if (current_row_group_ == nullptr) return Status::OK();

  const int num_clustering_cols = table_desc_->num_clustering_cols();
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
    const string& col_name = col_writer->column_name();
    google::protobuf::Map<string,int64>* column_size_map =
        parquet_dml_stats_.mutable_per_column_size();
    (*column_size_map)[col_name] += col_writer->total_compressed_size();

    if (is_iceberg_file_ ) {
      const ColumnDescriptor& col_desc =
          table_desc_->col_descs()[i + num_clustering_cols];
      DCHECK_EQ(col_desc.name(), col_name);
      const int field_id = col_desc.field_id();
      if (field_id != -1) CollectIcebergDmlFileColumnStats(field_id, col_writer);
    }

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

    // Write Bloom filter and update metadata.
    RETURN_IF_ERROR(WriteParquetBloomFilter(col_writer,
        &current_row_group_->columns[i].meta_data));

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
  // Do that only, if the sorting type is lexical.
  if (parent_->sorting_order() == TSortingOrder::LEXICAL){
    for (int col_idx : parent_->sort_columns()) {
      current_row_group_->sorting_columns.push_back(parquet::SortingColumn());
      parquet::SortingColumn& sorting_column = current_row_group_->sorting_columns.back();
      sorting_column.column_idx = col_idx;
      sorting_column.descending = false;
      sorting_column.nulls_first = false;
    }
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
  file_pos_ += file_metadata_len;

  // Write footer
  RETURN_IF_ERROR(Write<uint32_t>(file_metadata_len));
  file_pos_ += sizeof(uint32_t);
  RETURN_IF_ERROR(Write(PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)));
  file_pos_ += sizeof(PARQUET_VERSION_NUMBER);
  return Status::OK();
}
