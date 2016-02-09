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

#include "exec/hdfs-parquet-table-writer.h"

#include "common/version.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/decimal-value.h"
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
#include "rpc/thrift-util.h"

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
  BaseColumnWriter(HdfsParquetTableWriter* parent, ExprContext* expr_ctx,
      const THdfsCompression::type& codec)
    : parent_(parent), expr_ctx_(expr_ctx), codec_(codec),
      page_size_(DEFAULT_DATA_PAGE_SIZE), current_page_(NULL), num_values_(0),
      total_compressed_byte_size_(0),
      total_uncompressed_byte_size_(0),
      dict_encoder_base_(NULL),
      def_levels_(NULL),
      values_buffer_len_(DEFAULT_DATA_PAGE_SIZE) {
    Codec::CreateCompressor(NULL, false, codec, &compressor_);

    def_levels_ = parent_->state_->obj_pool()->Add(
        new RleEncoder(parent_->reusable_col_mem_pool_->Allocate(DEFAULT_DATA_PAGE_SIZE),
                       DEFAULT_DATA_PAGE_SIZE, 1));
    values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(values_buffer_len_);
  }

  virtual ~BaseColumnWriter() {}

  // Appends the row to this column.  This buffers the value into a data page.  Returns
  // error if the space needed for the encoded value is larger than the data page size.
  // TODO: this needs to be batch based, instead of row based for better performance.
  // This is a bit trickier to handle the case where only a partial row batch can be
  // output to the current file because it reaches the max file size.  Enabling codegen
  // would also solve this problem.
  Status AppendRow(TupleRow* row);

  // Flushes all buffered data pages to the file.
  // *file_pos is an output parameter and will be incremented by
  // the number of bytes needed to write all the data pages for this column.
  // first_data_page and first_dictionary_page are also out parameters and
  // will contain the byte offset for the data page and dictionary page.  They
  // will be set to -1 if the column does not contain that type of page.
  Status Flush(int64_t* file_pos, int64_t* first_data_page,
      int64_t* first_dictionary_page);

  // Resets all the data accumulated for this column.  Memory can now be reused for
  // the next row group
  // Any data for previous row groups must be reset (e.g. dictionaries).
  // Subclasses must call this if they override this function.
  virtual void Reset() {
    num_data_pages_ = 0;
    current_page_ = NULL;
    num_values_ = 0;
    total_compressed_byte_size_ = 0;
    current_encoding_ = Encoding::PLAIN;
  }

  // Close this writer. This is only called after Flush() and no more rows will
  // be added.
  void Close() {
    if (compressor_.get() != NULL) compressor_->Close();
    if (dict_encoder_base_ != NULL) dict_encoder_base_->ClearIndices();
  }

  const ColumnType& type() const { return expr_ctx_->root()->type(); }
  uint64_t num_values() const { return num_values_; }
  uint64_t total_compressed_size() const { return total_compressed_byte_size_; }
  uint64_t total_uncompressed_size() const { return total_uncompressed_byte_size_; }
  parquet::CompressionCodec::type codec() const {
    return IMPALA_TO_PARQUET_CODEC[codec_];
  }

 protected:
  friend class HdfsParquetTableWriter;

  // Encode value into the current page output buffer. Returns true if the value fits
  // on the current page. If this function returned false, the caller should create a
  // new page and try again with the same value.
  // *bytes_needed will contain the (estimated) number of bytes needed to successfully
  // encode the value in the page.
  // Implemented in the subclass.
  virtual bool EncodeValue(void* value, int64_t* bytes_needed) = 0;

  // Encodes out all data for the current page and updates the metadata.
  virtual void FinalizeCurrentPage();

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
  ExprContext* expr_ctx_;

  THdfsCompression::type codec_;

  // Compression codec for this column.  If NULL, this column is will not be compressed.
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

  DataPage* current_page_;
  int64_t num_values_; // Total number of values across all pages, including NULLs.
  int64_t total_compressed_byte_size_;
  int64_t total_uncompressed_byte_size_;
  Encoding::type current_encoding_;

  // Created and set by the base class.
  DictEncoderBase* dict_encoder_base_;

  // Rle encoder object for storing definition levels. For non-nested schemas,
  // this always uses 1 bit per row.
  // This is reused across pages since the underlying buffer is copied out when
  // the page is finalized.
  RleEncoder* def_levels_;

  // Data for buffered values. This is reused across pages.
  uint8_t* values_buffer_;
  // The size of values_buffer_.
  int values_buffer_len_;
};

// Per type column writer.
template<typename T>
class HdfsParquetTableWriter::ColumnWriter :
    public HdfsParquetTableWriter::BaseColumnWriter {
 public:
  ColumnWriter(HdfsParquetTableWriter* parent, ExprContext* ctx,
      const THdfsCompression::type& codec) : BaseColumnWriter(parent, ctx, codec),
      num_values_since_dict_size_check_(0) {
    DCHECK_NE(ctx->root()->type().type, TYPE_BOOLEAN);
    encoded_value_size_ = ParquetPlainEncoder::ByteSize(ctx->root()->type());
  }

  virtual void Reset() {
    BaseColumnWriter::Reset();
    // Default to dictionary encoding.  If the cardinality ends up being too high,
    // it will fall back to plain.
    current_encoding_ = Encoding::PLAIN_DICTIONARY;
    dict_encoder_.reset(
        new DictEncoder<T>(parent_->per_file_mem_pool_.get(), encoded_value_size_));
    dict_encoder_base_ = dict_encoder_.get();
  }

 protected:
  virtual bool EncodeValue(void* value, int64_t* bytes_needed) {
    if (current_encoding_ == Encoding::PLAIN_DICTIONARY) {
      if (UNLIKELY(num_values_since_dict_size_check_ >=
                   DICTIONARY_DATA_PAGE_SIZE_CHECK_PERIOD)) {
        num_values_since_dict_size_check_ = 0;
        if (dict_encoder_->EstimatedDataEncodedSize() >= page_size_) return false;
      }
      ++num_values_since_dict_size_check_;
      *bytes_needed = dict_encoder_->Put(*CastValue(value));
      // If the dictionary contains the maximum number of values, switch to plain
      // encoding.  The current dictionary encoded page is written out.
      if (UNLIKELY(*bytes_needed < 0)) {
        FinalizeCurrentPage();
        current_encoding_ = Encoding::PLAIN;
        return false;
      }
      parent_->file_size_estimate_ += *bytes_needed;
    } else if (current_encoding_ == Encoding::PLAIN) {
      T* v = CastValue(value);
      *bytes_needed = encoded_value_size_ < 0 ?
          ParquetPlainEncoder::ByteSize<T>(*v) : encoded_value_size_;
      if (current_page_->header.uncompressed_page_size + *bytes_needed > page_size_) {
        return false;
      }
      uint8_t* dst_ptr = values_buffer_ + current_page_->header.uncompressed_page_size;
      int64_t written_len =
          ParquetPlainEncoder::Encode(dst_ptr, encoded_value_size_, *v);
      DCHECK_EQ(*bytes_needed, written_len);
      current_page_->header.uncompressed_page_size += written_len;
    } else {
      // TODO: support other encodings here
      DCHECK(false);
    }
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
  scoped_ptr<DictEncoder<T> > dict_encoder_;

  // The number of values added since we last checked the dictionary.
  int num_values_since_dict_size_check_;

  // Size of each encoded value. -1 if the size is type is variable-length.
  int64_t encoded_value_size_;

  // Temporary string value to hold CHAR(N)
  StringValue temp_;

  // Converts a slot pointer to a raw value suitable for encoding
  inline T* CastValue(void* value) {
    return reinterpret_cast<T*>(value);
  }
};

template<>
inline StringValue* HdfsParquetTableWriter::ColumnWriter<StringValue>::CastValue(
    void* value) {
  if (type().type == TYPE_CHAR) {
    temp_.ptr = StringValue::CharSlotToPtr(value, type());
    temp_.len = StringValue::UnpaddedCharLength(temp_.ptr, type().len);
    return &temp_;
  }
  return reinterpret_cast<StringValue*>(value);
}

// Bools are encoded a bit differently so subclass it explicitly.
class HdfsParquetTableWriter::BoolColumnWriter :
    public HdfsParquetTableWriter::BaseColumnWriter {
 public:
  BoolColumnWriter(HdfsParquetTableWriter* parent, ExprContext* ctx,
      const THdfsCompression::type& codec) : BaseColumnWriter(parent, ctx, codec) {
    DCHECK_EQ(ctx->root()->type().type, TYPE_BOOLEAN);
    bool_values_ = parent_->state_->obj_pool()->Add(
        new BitWriter(values_buffer_, values_buffer_len_));
    // Dictionary encoding doesn't make sense for bools and is not allowed by
    // the format.
    current_encoding_ = Encoding::PLAIN;
    dict_encoder_base_ = NULL;
  }

 protected:
  virtual bool EncodeValue(void* value, int64_t* bytes_needed) {
    return bool_values_->PutValue(*reinterpret_cast<bool*>(value), 1);
  }

  virtual void FinalizeCurrentPage() {
    DCHECK(current_page_ != NULL);
    if (current_page_->finalized) return;
    bool_values_->Flush();
    int num_bytes = bool_values_->bytes_written();
    current_page_->header.uncompressed_page_size += num_bytes;
    // Call into superclass to handle the rest.
    BaseColumnWriter::FinalizeCurrentPage();
    bool_values_->Clear();
  }

 private:
  // Used to encode bools as single bit values. This is reused across pages.
  BitWriter* bool_values_;
};

}

inline Status HdfsParquetTableWriter::BaseColumnWriter::AppendRow(TupleRow* row) {
  ++num_values_;
  void* value = expr_ctx_->GetValue(row);
  if (current_page_ == NULL) NewPage();

  // We might need to try again if this current page is not big enough
  while (true) {
    if (!def_levels_->Put(value != NULL)) {
      FinalizeCurrentPage();
      NewPage();
      bool ret = def_levels_->Put(value != NULL);
      DCHECK(ret);
    }

    // Nulls don't get encoded.
    if (value == NULL) break;
    ++current_page_->num_non_null;

    int64_t bytes_needed = 0;
    if (EncodeValue(value, &bytes_needed)) break;

    // Value didn't fit on page, try again on a new page.
    FinalizeCurrentPage();

    // Check how much space it is needed to write this value. If that is larger than the
    // page size then increase page size and try again.
    if (UNLIKELY(bytes_needed > page_size_)) {
      page_size_ = bytes_needed;
      if (page_size_ > MAX_DATA_PAGE_SIZE) {
        stringstream ss;
        ss << "Cannot write value of size "
           << PrettyPrinter::Print(bytes_needed, TUnit::BYTES) << " bytes to a Parquet "
           << "data page that exceeds the max page limit "
           << PrettyPrinter::Print(MAX_DATA_PAGE_SIZE , TUnit::BYTES) << ".";
        return Status(ss.str());
      }
      values_buffer_len_ = page_size_;
      values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(values_buffer_len_);
    }
    NewPage();
  }
  ++current_page_->header.data_page_header.num_values;
  return Status::OK();
}

inline void HdfsParquetTableWriter::BaseColumnWriter::WriteDictDataPage() {
  DCHECK(dict_encoder_base_ != NULL);
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
  if (current_page_ == NULL) {
    // This column/file is empty
    *first_data_page = *file_pos;
    *first_dictionary_page = -1;
    return Status::OK();
  }

  FinalizeCurrentPage();

  *first_dictionary_page = -1;
  // First write the dictionary page before any of the data pages.
  if (dict_encoder_base_ != NULL) {
    *first_dictionary_page = *file_pos;
    // Write dictionary page header
    DictionaryPageHeader dict_header;
    dict_header.num_values = dict_encoder_base_->num_entries();
    dict_header.encoding = Encoding::PLAIN_DICTIONARY;

    PageHeader header;
    header.type = PageType::DICTIONARY_PAGE;
    header.uncompressed_page_size = dict_encoder_base_->dict_encoded_size();
    header.__set_dictionary_page_header(dict_header);

    // Write the dictionary page data, compressing it if necessary.
    uint8_t* dict_buffer = parent_->per_file_mem_pool_->Allocate(
        header.uncompressed_page_size);
    dict_encoder_base_->WriteDict(dict_buffer);
    if (compressor_.get() != NULL) {
      SCOPED_TIMER(parent_->parent_->compress_timer());
      int64_t max_compressed_size =
          compressor_->MaxOutputLen(header.uncompressed_page_size);
      DCHECK_GT(max_compressed_size, 0);
      uint8_t* compressed_data =
          parent_->per_file_mem_pool_->Allocate(max_compressed_size);
      header.compressed_page_size = max_compressed_size;
      compressor_->ProcessBlock32(true, header.uncompressed_page_size, dict_buffer,
          &header.compressed_page_size, &compressed_data);
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

    // Last page might be empty
    if (page.header.data_page_header.num_values == 0) {
      DCHECK_EQ(page.header.compressed_page_size, 0);
      DCHECK_EQ(i, num_data_pages_ - 1);
      continue;
    }

    // Write data page header
    uint8_t* buffer = NULL;
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

void HdfsParquetTableWriter::BaseColumnWriter::FinalizeCurrentPage() {
  DCHECK(current_page_ != NULL);
  if (current_page_->finalized) return;

  // If the entire page was NULL, encode it as PLAIN since there is no
  // data anyway. We don't output a useless dictionary page and it works
  // around a parquet MR bug (see IMPALA-759 for more details).
  if (current_page_->num_non_null == 0) current_encoding_ = Encoding::PLAIN;

  if (current_encoding_ == Encoding::PLAIN_DICTIONARY) WriteDictDataPage();

  PageHeader& header = current_page_->header;
  header.data_page_header.encoding = current_encoding_;

  // Compute size of definition bits
  def_levels_->Flush();
  current_page_->num_def_bytes = sizeof(int32_t) + def_levels_->len();
  header.uncompressed_page_size += current_page_->num_def_bytes;

  // At this point we know all the data for the data page.  Combine them into one buffer.
  uint8_t* uncompressed_data = NULL;
  if (compressor_.get() == NULL) {
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
  if (compressor_.get() == NULL) {
    current_page_->data = reinterpret_cast<uint8_t*>(uncompressed_data);
    header.compressed_page_size = header.uncompressed_page_size;
  } else {
    SCOPED_TIMER(parent_->parent_->compress_timer());
    int64_t max_compressed_size =
        compressor_->MaxOutputLen(header.uncompressed_page_size);
    DCHECK_GT(max_compressed_size, 0);
    uint8_t* compressed_data = parent_->per_file_mem_pool_->Allocate(max_compressed_size);
    header.compressed_page_size = max_compressed_size;
    compressor_->ProcessBlock32(true, header.uncompressed_page_size, uncompressed_data,
        &header.compressed_page_size, &compressed_data);
    current_page_->data = compressed_data;

    // We allocated the output based on the guessed size, return the extra allocated
    // bytes back to the mem pool.
    parent_->per_file_mem_pool_->ReturnPartialAllocation(
        max_compressed_size - header.compressed_page_size);
  }

  // Add the size of the data page header
  uint8_t* header_buffer;
  uint32_t header_len = 0;
  parent_->thrift_serializer_->Serialize(
      &current_page_->header, &header_len, &header_buffer);

  current_page_->finalized = true;
  total_compressed_byte_size_ += header_len + header.compressed_page_size;
  total_uncompressed_byte_size_ += header_len + header.uncompressed_page_size;
  parent_->file_size_estimate_ += header_len + header.compressed_page_size;
  def_levels_->Clear();
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
    header.definition_level_encoding = Encoding::RLE;
    header.repetition_level_encoding = Encoding::BIT_PACKED;
    current_page_->header.__set_data_page_header(header);
  }
  current_page_->finalized = false;
  current_page_->num_non_null = 0;
}

HdfsParquetTableWriter::HdfsParquetTableWriter(HdfsTableSink* parent, RuntimeState* state,
    OutputPartition* output, const HdfsPartitionDescriptor* part_desc,
    const HdfsTableDescriptor* table_desc, const vector<ExprContext*>& output_expr_ctxs)
    : HdfsTableWriter(
        parent, state, output, part_desc, table_desc, output_expr_ctxs),
      thrift_serializer_(new ThriftSerializer(true)),
      current_row_group_(NULL),
      row_count_(0),
      file_size_limit_(0),
      reusable_col_mem_pool_(new MemPool(parent_->mem_tracker())),
      per_file_mem_pool_(new MemPool(parent_->mem_tracker())),
      row_idx_(0) {
}

HdfsParquetTableWriter::~HdfsParquetTableWriter() {
}

Status HdfsParquetTableWriter::Init() {
  // Initialize file metadata
  file_metadata_.version = PARQUET_CURRENT_VERSION;

  stringstream created_by;
  created_by << "impala version " << IMPALA_BUILD_VERSION
             << " (build " << IMPALA_BUILD_HASH << ")";
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
    BaseColumnWriter* writer = NULL;
    const ColumnType& type = output_expr_ctxs_[i]->root()->type();
    switch (type.type) {
      case TYPE_BOOLEAN:
        writer = new BoolColumnWriter(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_TINYINT:
        writer = new ColumnWriter<int8_t>(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_SMALLINT:
        writer = new ColumnWriter<int16_t>(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_INT:
        writer = new ColumnWriter<int32_t>(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_BIGINT:
        writer = new ColumnWriter<int64_t>(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_FLOAT:
        writer = new ColumnWriter<float>(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_DOUBLE:
        writer = new ColumnWriter<double>(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_TIMESTAMP:
        writer = new ColumnWriter<TimestampValue>(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_VARCHAR:
      case TYPE_STRING:
      case TYPE_CHAR:
        writer = new ColumnWriter<StringValue>(
            this, output_expr_ctxs_[i], codec);
        break;
      case TYPE_DECIMAL:
        switch (output_expr_ctxs_[i]->root()->type().GetByteSize()) {
          case 4:
            writer = new ColumnWriter<Decimal4Value>(
                this, output_expr_ctxs_[i], codec);
            break;
          case 8:
            writer = new ColumnWriter<Decimal8Value>(
                this, output_expr_ctxs_[i], codec);
            break;
          case 16:
            writer = new ColumnWriter<Decimal16Value>(
                this, output_expr_ctxs_[i], codec);
            break;
          default:
            DCHECK(false);
        }
        break;
      default:
        DCHECK(false);
    }
    columns_[i] = state_->obj_pool()->Add(writer);
    columns_[i]->Reset();
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
    node.name = table_desc_->col_descs()[i + num_clustering_cols].name();
    node.__set_type(IMPALA_TO_PARQUET_TYPES[output_expr_ctxs_[i]->root()->type().type]);
    node.__set_repetition_type(FieldRepetitionType::OPTIONAL);
    const ColumnType& type = output_expr_ctxs_[i]->root()->type();
    if (type.type == TYPE_DECIMAL) {
      // This column is type decimal. Update the file metadata to include the
      // additional fields:
      //  1) converted_type: indicate this is really a decimal column.
      //  2) type_length: the number of bytes used per decimal value in the data
      //  3) precision/scale
      node.__set_converted_type(ConvertedType::DECIMAL);
      node.__set_type_length(
          ParquetPlainEncoder::DecimalSize(output_expr_ctxs_[i]->root()->type()));
      node.__set_scale(output_expr_ctxs_[i]->root()->type().scale);
      node.__set_precision(output_expr_ctxs_[i]->root()->type().precision);
    } else if (type.type == TYPE_VARCHAR || type.type == TYPE_CHAR) {
      node.__set_converted_type(ConvertedType::UTF8);
    }
  }

  return Status::OK();
}

Status HdfsParquetTableWriter::AddRowGroup() {
  if (current_row_group_ != NULL) RETURN_IF_ERROR(FlushCurrentRowGroup());
  file_metadata_.row_groups.push_back(RowGroup());
  current_row_group_ = &file_metadata_.row_groups[file_metadata_.row_groups.size() - 1];

  // Initialize new row group metadata.
  int num_clustering_cols = table_desc_->num_clustering_cols();
  current_row_group_->columns.resize(columns_.size());
  for (int i = 0; i < columns_.size(); ++i) {
    ColumnMetaData metadata;
    metadata.type = IMPALA_TO_PARQUET_TYPES[columns_[i]->expr_ctx_->root()->type().type];
    // Add all encodings that were used in this file.  Currently we use PLAIN and
    // PLAIN_DICTIONARY for data values and RLE for the definition levels.
    metadata.encodings.push_back(Encoding::RLE);
    // Columns are initially dictionary encoded
    // TODO: we might not have PLAIN encoding in this case
    metadata.encodings.push_back(Encoding::PLAIN_DICTIONARY);
    metadata.encodings.push_back(Encoding::PLAIN);
    metadata.path_in_schema.push_back(
        table_desc_->col_descs()[i + num_clustering_cols].name());
    metadata.codec = columns_[i]->codec();
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
  DCHECK(current_row_group_ == NULL);

  per_file_mem_pool_->Clear();

  // Get the file limit
  RETURN_IF_ERROR(HdfsTableSink::GetFileBlockSize(output_, &file_size_limit_));
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

Status HdfsParquetTableWriter::AppendRowBatch(RowBatch* batch,
    const vector<int32_t>& row_group_indices, bool* new_file) {
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
  RETURN_IF_ERROR(FlushCurrentRowGroup());
  RETURN_IF_ERROR(WriteFileFooter());
  stats_.__set_parquet_stats(parquet_stats_);
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
  if (current_row_group_ == NULL) return Status::OK();

  int num_clustering_cols = table_desc_->num_clustering_cols();
  for (int i = 0; i < columns_.size(); ++i) {
    int64_t data_page_offset, dict_page_offset;
    // Flush this column.  This updates the final metadata sizes for this column.
    RETURN_IF_ERROR(columns_[i]->Flush(&file_pos_, &data_page_offset, &dict_page_offset));
    DCHECK_GT(data_page_offset, 0);

    current_row_group_->columns[i].meta_data.data_page_offset = data_page_offset;
    if (dict_page_offset >= 0) {
      current_row_group_->columns[i].meta_data.__set_dictionary_page_offset(
          dict_page_offset);
    }

    current_row_group_->columns[i].meta_data.num_values = columns_[i]->num_values();
    current_row_group_->columns[i].meta_data.total_uncompressed_size =
        columns_[i]->total_uncompressed_size();
    current_row_group_->columns[i].meta_data.total_compressed_size =
        columns_[i]->total_compressed_size();
    current_row_group_->total_byte_size += columns_[i]->total_compressed_size();
    current_row_group_->num_rows = columns_[i]->num_values();
    current_row_group_->columns[i].file_offset = file_pos_;
    const string& col_name = table_desc_->col_descs()[i + num_clustering_cols].name();
    parquet_stats_.per_column_size[col_name] += columns_[i]->total_compressed_size();

    // Since we don't supported complex schemas, all columns should have the same
    // number of values.
    DCHECK_EQ(current_row_group_->columns[0].meta_data.num_values,
        columns_[i]->num_values());

    // Metadata for this column is complete, write it out to file.  The column metadata
    // goes at the end so that when we have collocated files, the column data can be
    // written without buffering.
    uint8_t* buffer = NULL;
    uint32_t len = 0;
    RETURN_IF_ERROR(
        thrift_serializer_->Serialize(&current_row_group_->columns[i], &len, &buffer));
    RETURN_IF_ERROR(Write(buffer, len));
    file_pos_ += len;

    columns_[i]->Reset();
  }

  current_row_group_ = NULL;
  return Status::OK();
}

Status HdfsParquetTableWriter::WriteFileFooter() {
  // Write file_meta_data
  uint32_t file_metadata_len = 0;
  uint8_t* buffer = NULL;
  RETURN_IF_ERROR(
      thrift_serializer_->Serialize(&file_metadata_, &file_metadata_len, &buffer));
  RETURN_IF_ERROR(Write(buffer, file_metadata_len));

  // Write footer
  RETURN_IF_ERROR(Write<uint32_t>(file_metadata_len));
  RETURN_IF_ERROR(Write(PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)));
  return Status::OK();
}

