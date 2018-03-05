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

#include "exec/hdfs-parquet-table-writer.h"

#include "common/version.h"
#include "exec/hdfs-table-sink.h"
#include "exec/parquet-column-stats.inline.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "rpc/thrift-util.h"
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
#include "util/rle-encoding.h"

#include <sstream>

#include "gen-cpp/ImpalaService_types.h"

#include "common/names.h"
using namespace impala;
using namespace parquet;
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
      row_group_stats_base_(nullptr) {
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
  void EncodeRowGroupStats(ColumnMetaData* meta_data) {
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
    num_data_pages_ = 0;
    current_page_ = nullptr;
    num_values_ = 0;
    total_compressed_byte_size_ = 0;
    current_encoding_ = Encoding::PLAIN;
    next_page_encoding_ = Encoding::PLAIN;
    column_encodings_.clear();
    dict_encoding_stats_.clear();
    data_encoding_stats_.clear();
    // Repetition/definition level encodings are constant. Incorporate them here.
    column_encodings_.insert(Encoding::RLE);
  }

  // Close this writer. This is only called after Flush() and no more rows will
  // be added.
  void Close() {
    if (compressor_.get() != nullptr) compressor_->Close();
    if (dict_encoder_base_ != nullptr) dict_encoder_base_->Close();
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

  // Encodes out all data for the current page and updates the metadata.
  virtual Status FinalizeCurrentPage() WARN_UNUSED_RESULT;

  // Update current_page_ to a new page, reusing pages allocated if possible.
  void NewPage();

  // Writes out the dictionary encoded data buffered in dict_encoder_.
  void WriteDictDataPage();

  struct DataPage {
    // Page header.  This is a union of all page types.
    PageHeader header;

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

  vector<DataPage> pages_;

  // Number of pages in 'pages_' that are used.  'pages_' is reused between flushes
  // so this number can be less than pages_.size()
  int num_data_pages_;

  // Size of newly created pages. Defaults to DEFAULT_DATA_PAGE_SIZE and is increased
  // when pages are not big enough. This only happens when there are enough unique values
  // such that we switch from PLAIN_DICTIONARY to PLAIN encoding and then have very
  // large values (i.e. greater than DEFAULT_DATA_PAGE_SIZE).
  // TODO: Consider removing and only creating a single large page as necessary.
  int64_t page_size_;

  // Pointer to the current page in 'pages_'. Not owned.
  DataPage* current_page_;

  // Total number of values across all pages, including NULL.
  int64_t num_values_;
  int64_t total_compressed_byte_size_;
  int64_t total_uncompressed_byte_size_;
  // Encoding of the current page.
  Encoding::type current_encoding_;
  // Encoding to use for the next page. By default, the same as 'current_encoding_'.
  // Used by the column writer to switch encoding while writing a column, e.g. if the
  // dictionary overflows.
  Encoding::type next_page_encoding_;

  // Set of all encodings used in the column chunk
  unordered_set<Encoding::type> column_encodings_;

  // Map from the encoding to the number of pages in the column chunk with this encoding
  // These are used to construct the PageEncodingStats, which provide information
  // about encoding usage for each different page type. Currently, only dictionary
  // and data pages are used.
  unordered_map<Encoding::type, int> dict_encoding_stats_;
  unordered_map<Encoding::type, int> data_encoding_stats_;

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
  }

  virtual void Reset() {
    BaseColumnWriter::Reset();
    // Default to dictionary encoding.  If the cardinality ends up being too high,
    // it will fall back to plain.
    current_encoding_ = Encoding::PLAIN_DICTIONARY;
    next_page_encoding_ = Encoding::PLAIN_DICTIONARY;
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
    if (current_encoding_ == Encoding::PLAIN_DICTIONARY) {
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
        next_page_encoding_ = Encoding::PLAIN;
        return false;
      }
      parent_->file_size_estimate_ += *bytes_needed;
    } else if (current_encoding_ == Encoding::PLAIN) {
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

  // Size of each encoded value in plain encoding. -1 if the type is variable-length.
  int64_t plain_encoded_value_size_;

  // Temporary string value to hold CHAR(N)
  StringValue temp_;

  // Tracks statistics per page. These are not written out currently but are merged into
  // the row group stats. TODO(IMPALA-5841): Write these to the page index.
  scoped_ptr<ColumnStats<T>> page_stats_;

  // Tracks statistics per row group. This gets reset when starting a new row group.
  scoped_ptr<ColumnStats<T>> row_group_stats_;

  // Converts a slot pointer to a raw value suitable for encoding
  inline T* CastValue(void* value) {
    return reinterpret_cast<T*>(value);
  }
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
    current_encoding_ = Encoding::PLAIN;
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

  // Tracks statistics per page. These are not written out currently but are merged into
  // the row group stats. TODO(IMPALA-5841): Write these to the page index.
  ColumnStats<bool> page_stats_;

  // Tracks statistics per row group. This gets reset when starting a new file.
  ColumnStats<bool> row_group_stats_;
};

}

inline Status HdfsParquetTableWriter::BaseColumnWriter::AppendRow(TupleRow* row) {
  ++num_values_;
  void* value = expr_eval_->GetValue(row);
  if (current_page_ == nullptr) NewPage();

  // Ensure that we have enough space for the definition level, but don't write it yet in
  // case we don't have enough space for the value.
  if (def_levels_->buffer_full()) {
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
    DictionaryPageHeader dict_header;
    dict_header.num_values = dict_encoder_base_->num_entries();
    dict_header.encoding = Encoding::PLAIN_DICTIONARY;
    ++dict_encoding_stats_[dict_header.encoding];

    PageHeader header;
    header.type = PageType::DICTIONARY_PAGE;
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
    RETURN_IF_ERROR(parent_->thrift_serializer_->Serialize(
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
  // Write data pages
  for (int i = 0; i < num_data_pages_; ++i) {
    DataPage& page = pages_[i];

    if (page.header.data_page_header.num_values == 0) {
      // Skip empty pages
      continue;
    }

    // Write data page header
    uint8_t* buffer = nullptr;
    uint32_t len = 0;
    RETURN_IF_ERROR(
        parent_->thrift_serializer_->Serialize(&page.header, &len, &buffer));
    RETURN_IF_ERROR(parent_->Write(buffer, len));
    *file_pos += len;

    // Write the page data
    RETURN_IF_ERROR(parent_->Write(page.data, page.header.compressed_page_size));
    *file_pos += page.header.compressed_page_size;
  }
  return Status::OK();
}

Status HdfsParquetTableWriter::BaseColumnWriter::FinalizeCurrentPage() {
  DCHECK(current_page_ != nullptr);
  if (current_page_->finalized) return Status::OK();

  // If the entire page was NULL, encode it as PLAIN since there is no
  // data anyway. We don't output a useless dictionary page and it works
  // around a parquet MR bug (see IMPALA-759 for more details).
  if (current_page_->num_non_null == 0) current_encoding_ = Encoding::PLAIN;

  if (current_encoding_ == Encoding::PLAIN_DICTIONARY) WriteDictDataPage();

  PageHeader& header = current_page_->header;
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

  // Update row group statistics from page statistics.
  DCHECK(row_group_stats_base_ != nullptr);
  DCHECK(page_stats_base_ != nullptr);
  row_group_stats_base_->Merge(*page_stats_base_);

  // Add the size of the data page header
  uint8_t* header_buffer;
  uint32_t header_len = 0;
  RETURN_IF_ERROR(parent_->thrift_serializer_->Serialize(
      &current_page_->header, &header_len, &header_buffer));

  current_page_->finalized = true;
  total_compressed_byte_size_ += header_len + header.compressed_page_size;
  total_uncompressed_byte_size_ += header_len + header.uncompressed_page_size;
  parent_->file_size_estimate_ += header_len + header.compressed_page_size;
  def_levels_->Clear();
  return Status::OK();
}

void HdfsParquetTableWriter::BaseColumnWriter::NewPage() {
  if (num_data_pages_ < pages_.size()) {
    // Reuse an existing page
    current_page_ = &pages_[num_data_pages_++];
    current_page_->header.data_page_header.num_values = 0;
    current_page_->header.compressed_page_size = 0;
    current_page_->header.uncompressed_page_size = 0;
  } else {
    pages_.push_back(DataPage());
    current_page_ = &pages_[num_data_pages_++];

    DataPageHeader header;
    header.num_values = 0;
    // The code that populates the column chunk metadata's encodings field
    // relies on these specific values for the definition/repetition level
    // encodings.
    header.definition_level_encoding = Encoding::RLE;
    header.repetition_level_encoding = Encoding::RLE;
    current_page_->header.__set_data_page_header(header);
  }
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

  columns_.resize(table_desc_->num_cols() - table_desc_->num_clustering_cols());
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
        writer = new ColumnWriter<TimestampValue>(
            this, output_expr_evals_[i], codec);
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
    parquet::SchemaElement& node = file_metadata_.schema[i + 1];
    const ColumnType& type = output_expr_evals_[i]->root().type();
    node.name = table_desc_->col_descs()[i + num_clustering_cols].name();
    node.__set_type(ConvertInternalToParquetType(type.type));
    node.__set_repetition_type(FieldRepetitionType::OPTIONAL);
    if (type.type == TYPE_DECIMAL) {
      // This column is type decimal. Update the file metadata to include the
      // additional fields:
      //  1) converted_type: indicate this is really a decimal column.
      //  2) type_length: the number of bytes used per decimal value in the data
      //  3) precision/scale
      node.__set_converted_type(ConvertedType::DECIMAL);
      node.__set_type_length(
          ParquetPlainEncoder::DecimalSize(output_expr_evals_[i]->root().type()));
      node.__set_scale(output_expr_evals_[i]->root().type().scale);
      node.__set_precision(output_expr_evals_[i]->root().type().precision);
    } else if (type.type == TYPE_VARCHAR || type.type == TYPE_CHAR ||
        (type.type == TYPE_STRING &&
         state_->query_options().parquet_annotate_strings_utf8)) {
      node.__set_converted_type(ConvertedType::UTF8);
    } else if (type.type == TYPE_TINYINT) {
      node.__set_converted_type(ConvertedType::INT_8);
    } else if (type.type == TYPE_SMALLINT) {
      node.__set_converted_type(ConvertedType::INT_16);
    } else if (type.type == TYPE_INT) {
      node.__set_converted_type(ConvertedType::INT_32);
    } else if (type.type == TYPE_BIGINT) {
      node.__set_converted_type(ConvertedType::INT_64);
    }
  }

  return Status::OK();
}

Status HdfsParquetTableWriter::AddRowGroup() {
  if (current_row_group_ != nullptr) RETURN_IF_ERROR(FlushCurrentRowGroup());
  file_metadata_.row_groups.push_back(RowGroup());
  current_row_group_ = &file_metadata_.row_groups[file_metadata_.row_groups.size() - 1];

  // Initialize new row group metadata.
  int num_clustering_cols = table_desc_->num_clustering_cols();
  current_row_group_->columns.resize(columns_.size());
  for (int i = 0; i < columns_.size(); ++i) {
    ColumnMetaData metadata;
    metadata.type = ConvertInternalToParquetType(columns_[i]->type().type);
    metadata.path_in_schema.push_back(
        table_desc_->col_descs()[i + num_clustering_cols].name());
    metadata.codec = columns_[i]->GetParquetCodec();
    current_row_group_->columns[i].__set_meta_data(metadata);
  }

  return Status::OK();
}

int64_t HdfsParquetTableWriter::MinBlockSize() const {
  // See file_size_limit_ calculation in InitNewFile().
  return 3 * DEFAULT_DATA_PAGE_SIZE * columns_.size();
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
    block_size = max(block_size, MinBlockSize());
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
  // is undesirable.  We are under the limit, we potentially end up with more
  // files than necessary.  Either way, it is not going to generate a invalid
  // file.
  // With arbitrary encoding schemes, it is  not possible to know if appending
  // a new row will push us over the limit until after encoding it.  Rolling back
  // a row can be tricky as well so instead we will stop the file when it is
  // 2 * DEFAULT_DATA_PAGE_SIZE * num_cols short of the limit. e.g. 50 cols with 8K data
  // pages, means we stop 800KB shy of the limit.
  // Data pages calculate their size precisely when they are complete so having
  // a two page buffer guarantees we will never go over (unless there are huge values
  // that require increasing the page size).
  // TODO: this should be made dynamic based on the size of rows seen so far.
  // This would for example, let us account for very long string columns.
  if (file_size_limit_ < MinBlockSize()) {
    stringstream ss;
    ss << "Parquet file size " << file_size_limit_ << " bytes is too small for "
       << "a table with " << columns_.size() << " columns. Set query option "
       << "PARQUET_FILE_SIZE to at least " << MinBlockSize() << ".";
    return Status(ss.str());
  }
  file_size_limit_ -= 2 * DEFAULT_DATA_PAGE_SIZE * columns_.size();
  DCHECK_GE(file_size_limit_, DEFAULT_DATA_PAGE_SIZE * columns_.size());
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
  ColumnOrder col_order = ColumnOrder();
  col_order.__set_TYPE_ORDER(TypeDefinedOrder());
  file_metadata_.column_orders.assign(columns_.size(), col_order);
  file_metadata_.__isset.column_orders = true;

  RETURN_IF_ERROR(FlushCurrentRowGroup());
  RETURN_IF_ERROR(WriteFileFooter());
  stats_.__set_parquet_stats(parquet_insert_stats_);
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

    ColumnChunk& col_chunk = current_row_group_->columns[i];
    ColumnMetaData& col_metadata = col_chunk.meta_data;
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
    parquet_insert_stats_.per_column_size[col_name] +=
        col_writer->total_compressed_size();

    // Write encodings and encoding stats for this column
    col_metadata.encodings.clear();
    for (Encoding::type encoding : col_writer->column_encodings_) {
      col_metadata.encodings.push_back(encoding);
    }

    vector<PageEncodingStats> encoding_stats;
    // Add dictionary page encoding stats
    for (const auto& entry: col_writer->dict_encoding_stats_) {
      PageEncodingStats dict_enc_stat;
      dict_enc_stat.page_type = PageType::DICTIONARY_PAGE;
      dict_enc_stat.encoding = entry.first;
      dict_enc_stat.count = entry.second;
      encoding_stats.push_back(dict_enc_stat);
    }
    // Add data page encoding stats
    for (const auto& entry: col_writer->data_encoding_stats_) {
      PageEncodingStats data_enc_stat;
      data_enc_stat.page_type = PageType::DATA_PAGE;
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
    RETURN_IF_ERROR(
        thrift_serializer_->Serialize(&current_row_group_->columns[i], &len, &buffer));
    RETURN_IF_ERROR(Write(buffer, len));
    file_pos_ += len;

    col_writer->Reset();
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

Status HdfsParquetTableWriter::WriteFileFooter() {
  // Write file_meta_data
  uint32_t file_metadata_len = 0;
  uint8_t* buffer = nullptr;
  RETURN_IF_ERROR(
      thrift_serializer_->Serialize(&file_metadata_, &file_metadata_len, &buffer));
  RETURN_IF_ERROR(Write(buffer, file_metadata_len));

  // Write footer
  RETURN_IF_ERROR(Write<uint32_t>(file_metadata_len));
  RETURN_IF_ERROR(Write(PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)));
  return Status::OK();
}
