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
#include "runtime/primitive-type.h"
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
#include "util/thrift-util.h"

#include <sstream>

#include "gen-cpp/ImpalaService_types.h"

using namespace std;
using namespace boost;
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

// The maximum entries in the dictionary before giving up and switching to
// plain encoding.
// TODO: more complicated heuristic?
static const int MAX_DICTIONARY_ENTRIES = (1 << 16) - 1;

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
class HdfsParquetTableWriter::ColumnWriter {
 public:
  // expr - the expression to generate output values for this column.
  ColumnWriter(HdfsParquetTableWriter* parent, Expr* expr,
      const THdfsCompression::type& codec)
    : parent_(parent), expr_(expr),
      codec_(codec), current_page_(NULL), num_values_(0),
      total_compressed_byte_size_(0),
      total_uncompressed_byte_size_(0),
      def_levels_(NULL), bool_values_(NULL), values_buffer_(NULL) {
    Codec::CreateCompressor(parent_->state_, NULL, false, codec, &compressor_);

    if (expr_->type() == TYPE_STRING) {
      // Strings default to dictionary encoding.  If the cardinality ends up
      // being too high, it will fall back to plain.
      current_encoding_ = Encoding::PLAIN_DICTIONARY;
      dict_encoder_.reset(new DictEncoder<StringValue>(parent_->state_->mem_limits()));
    } else {
      current_encoding_ = Encoding::PLAIN;
    }

    def_levels_ = parent_->state_->obj_pool()->Add(
        new RleEncoder(parent_->reusable_col_mem_pool_->Allocate(DATA_PAGE_SIZE),
                       DATA_PAGE_SIZE, 1));

    if (expr_->type() == TYPE_BOOLEAN) {
      values_buffer_ = parent_->reusable_col_mem_pool_.get()->Allocate(DATA_PAGE_SIZE);
      bool_values_ = parent_->state_->obj_pool()->Add(
          new BitWriter(values_buffer_, DATA_PAGE_SIZE));
    } else {
      bool_values_ = NULL;
      values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(DATA_PAGE_SIZE);
    }
    Reset();
  }

  // Append the row to this column.  This buffers the value into a data page.
  // Returns the (estimated) delta in file size in bytes.
  // TODO: this needs to be batch based, instead of row based for better
  // performance.  This is a bit trickier to handle the case where only a
  // partial row batch can be output to the current file because it reaches
  // the max file size.  Enabling codegen would also solve this problem.
  int AppendRow(TupleRow* row);

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
  void Reset() {
    num_data_pages_ = 0;
    current_page_ = NULL;
    num_values_ = 0;
    total_compressed_byte_size_ = 0;
  }

  uint64_t num_values() const { return num_values_; }
  uint64_t total_compressed_size() const { return total_compressed_byte_size_; }
  uint64_t total_uncompressed_size() const { return total_uncompressed_byte_size_; }
  parquet::CompressionCodec::type codec() const {
    return IMPALA_TO_PARQUET_CODEC[codec_];
  }

 private:
  friend class HdfsParquetTableWriter;

  // Encodes out all data for the current page and updates the metadata. Returns
  // the number of bytes added to the current page (e.g. definition/repetition bits,
  // header byte size).
  int64_t FinalizeCurrentPage();

  // Update current_page_ to a new page, reusing pages allocated if possible.
  void NewPage();

  // Encode value (of type expr_->type()) in the plain encoding.  The plain encoding
  // is little-endian for plain types and the raw bytes for strings.
  // Returns the number of (uncompressed) bytes added.  If the current page does not
  // have enough bytes left to encode the value, returns -1.
  // TODO: this would benefit quite a bit from codegen.
  int EncodePlain(void* value) const;

  // Writes out the dictionary encoded data buffered in dict_encoder_.  This also
  // finalizes current_page_ (which must not contain any data already).
  // Does not call NewPage().
  // Returns the number of bytes of the encoded data.
  int WriteDictDataPage();

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
  Expr* expr_;

  THdfsCompression::type codec_;
  // Encoder for dictionary encoding string columns.
  scoped_ptr<DictEncoder<StringValue> > dict_encoder_;

  // Compression codec for this column.  If NULL, this column is will not be compressed.
  scoped_ptr<Codec> compressor_;

  vector<DataPage> pages_;

  // Number of pages in 'pages_' that are used.  'pages_' is reused between flushes
  // so this number can be less than pages_.size()
  int num_data_pages_;

  DataPage* current_page_;
  int64_t num_values_; // Total number of values across all pages, including NULLs.
  int64_t total_compressed_byte_size_;
  int64_t total_uncompressed_byte_size_;
  Encoding::type current_encoding_;

  // Rle encoder object for storing definition levels. For non-nested schemas,
  // this always uses 1 bit per row.
  // This is reused across pages since the underlying buffer is copied out when
  // the page is finalized.
  RleEncoder* def_levels_;

  // Used to encode bools as single bit values.  This is only allocated if
  // the column type is boolean.
  // This is reused across pages.
  BitWriter* bool_values_;

  // Data for buffered values.  For non-bool columns, this is where the output
  // is accumulated.  For bool columns, this is a ptr into bool_values (no
  // memory is allocated for it).
  // This is reused across pages.
  uint8_t* values_buffer_;
};

inline int HdfsParquetTableWriter::ColumnWriter::AppendRow(TupleRow* row) {
  int bytes_added = 0;
  ++num_values_;
  void* value = expr_->GetValue(row);
  if (current_page_ == NULL) NewPage();
  int encoded_len = 0;

  // We might need to try again if this current page is not big enough
  while (true) {
    if (!def_levels_->Put(value != NULL)) {
      bytes_added += FinalizeCurrentPage();
      NewPage();
      bool ret = def_levels_->Put(value != NULL);
      DCHECK(ret);
    }

    // Nulls don't get encoded.
    if (value == NULL) break;

    // TODO: support group var int encoding
    if (current_encoding_ == Encoding::PLAIN_DICTIONARY) {
      DCHECK_EQ(expr_->type(), TYPE_STRING);
      bytes_added += dict_encoder_->Put(*reinterpret_cast<StringValue*>(value));

      // If the dictionary contains the maximum number of values, switch to plain
      // encoding.  The current dictionary encoded page is written out.
      if (dict_encoder_->num_entries() == MAX_DICTIONARY_ENTRIES) {
        bytes_added += WriteDictDataPage();
        current_encoding_ = Encoding::PLAIN;
        NewPage();
        continue;
      } else if (dict_encoder_->EstimatedDataEncodedSize() >= DATA_PAGE_SIZE) {
        bytes_added += WriteDictDataPage();
        NewPage();
        continue;
      }
    } else if (current_encoding_ == Encoding::PLAIN) {
      encoded_len = EncodePlain(value);
      // len < 0 indicates the data does not fit in the current data page, make
      // a new page and try again.
      if (encoded_len < 0) {
        bytes_added += FinalizeCurrentPage();
        NewPage();
        // Try writing the value on the next page.  Note that the NULL value added for
        // the previous page does not need to be undone since the number of values in
        // the data page was not updated.
        continue;
      }
    } else {
      DCHECK(false);
    }
    ++current_page_->num_non_null;
    break;
  }

  DCHECK_GE(encoded_len, 0);
  ++current_page_->header.data_page_header.num_values;
  current_page_->header.uncompressed_page_size += encoded_len;
  return bytes_added;
}

inline int HdfsParquetTableWriter::ColumnWriter::EncodePlain(void* value) const {
  int32_t int_val;
  int len = 4;
  void* ptr = &int_val;
  uint8_t* dst_ptr = values_buffer_ + current_page_->header.uncompressed_page_size;

  // Special case bool and string
  switch (expr_->type()) {
    case TYPE_BOOLEAN:
      if (!bool_values_->PutValue(*reinterpret_cast<bool*>(value), 1)) {
        return -1;
      }
      return 0;
    case TYPE_STRING: {
      StringValue* sv = reinterpret_cast<StringValue*>(value);
      int bytes_added = ParquetPlainEncoder::ByteSize<StringValue>(*sv);
      if (current_page_->header.uncompressed_page_size + bytes_added > DATA_PAGE_SIZE) {
        return -1;
      }
      ParquetPlainEncoder::Encode<StringValue>(dst_ptr, *sv);
      return bytes_added;
    }
    case TYPE_TIMESTAMP:
      ptr = value;
      len = 12;
      break;
    case TYPE_TINYINT:
      int_val = *reinterpret_cast<uint8_t*>(value);
      break;
    case TYPE_SMALLINT:
      int_val = *reinterpret_cast<uint16_t*>(value);
      break;
    case TYPE_FLOAT:
    case TYPE_INT:
      ptr = value;
      break;
    case TYPE_DOUBLE:
    case TYPE_BIGINT:
      ptr = value;
      len = 8;
      break;
    default:
      DCHECK(0);
  }

  if (current_page_->header.uncompressed_page_size + len > DATA_PAGE_SIZE) return -1;
  memcpy(dst_ptr, ptr, len);
  return len;
}

inline int HdfsParquetTableWriter::ColumnWriter::WriteDictDataPage() {
  DCHECK(dict_encoder_.get() != NULL);
  DCHECK_EQ(current_page_->header.uncompressed_page_size, 0);
  int buffer_len = DATA_PAGE_SIZE;
  int len = dict_encoder_->WriteData(values_buffer_, buffer_len);
  while (UNLIKELY(len < 0)) {
    // len < 0 indicates the data doesn't fit into a data page. Allocate a larger data
    // page.
    buffer_len *= 2;
    values_buffer_ = parent_->reusable_col_mem_pool_->Allocate(buffer_len);
    len = dict_encoder_->WriteData(values_buffer_, buffer_len);
  }
  current_page_->header.uncompressed_page_size = len;
  len += FinalizeCurrentPage();
  return len;
}

Status HdfsParquetTableWriter::ColumnWriter::Flush(int64_t* file_pos,
   int64_t* first_data_page, int64_t* first_dictionary_page) {
  if (current_encoding_ == Encoding::PLAIN_DICTIONARY &&
      current_page_->header.data_page_header.num_values > 0) {
    WriteDictDataPage();
  } else if (current_encoding_ == Encoding::PLAIN) {
    FinalizeCurrentPage();
  }

  *first_dictionary_page = -1;
  // First write the dictionary page before any of the data pages.
  if (dict_encoder_.get() != NULL) {
    *first_dictionary_page = *file_pos;
    // Write dictionary page header
    DictionaryPageHeader dict_header;
    dict_header.num_values = dict_encoder_->num_entries();
    dict_header.encoding = Encoding::PLAIN_DICTIONARY;

    PageHeader header;
    header.type = PageType::DICTIONARY_PAGE;
    header.uncompressed_page_size = dict_encoder_->dict_encoded_size();
    header.__set_dictionary_page_header(dict_header);

    // Write the dictionary page data, compressing it if necessary.
    uint8_t* dict_buffer = parent_->per_file_mem_pool_->Allocate(
        header.uncompressed_page_size);
    dict_encoder_->WriteDict(dict_buffer);
    if (compressor_.get() != NULL) {
      int max_compressed_size =
          compressor_->MaxOutputLen(header.uncompressed_page_size);
      DCHECK_GT(max_compressed_size, 0);
      uint8_t* compressed_data =
          parent_->per_file_mem_pool_->Allocate(max_compressed_size);
      header.compressed_page_size = max_compressed_size;
      compressor_->ProcessBlock(true, header.uncompressed_page_size, dict_buffer,
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
    uint8_t* buffer;
    uint32_t len;
    RETURN_IF_ERROR(
        parent_->thrift_serializer_->Serialize(&page.header, &len, &buffer));
    RETURN_IF_ERROR(parent_->Write(buffer, len));
    *file_pos += len;

    // Write the page data
    RETURN_IF_ERROR(parent_->Write(page.data, page.header.compressed_page_size));
    *file_pos += page.header.compressed_page_size;
  }
  return Status::OK;
}

int64_t HdfsParquetTableWriter::ColumnWriter::FinalizeCurrentPage() {
  DCHECK(current_page_ != NULL);
  if (current_page_->finalized) return 0;

  PageHeader& header = current_page_->header;
  int64_t bytes_added = 0;
  if (expr_->type() == TYPE_BOOLEAN) {
    bool_values_->Flush();
    // Compute size of bool bits, they are encoded differently.
    int num_bytes = BitUtil::Ceil(current_page_->num_non_null, 8);
    header.uncompressed_page_size += num_bytes;
    bytes_added += num_bytes;
  }

  // Compute size of definition bits
  def_levels_->Flush();
  current_page_->num_def_bytes = sizeof(int32_t) + def_levels_->len();
  header.uncompressed_page_size += current_page_->num_def_bytes;
  bytes_added += current_page_->num_def_bytes;

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
    int max_compressed_size = compressor_->MaxOutputLen(header.uncompressed_page_size);
    DCHECK_GT(max_compressed_size, 0);
    uint8_t* compressed_data = parent_->per_file_mem_pool_->Allocate(max_compressed_size);
    header.compressed_page_size = max_compressed_size;
    compressor_->ProcessBlock(true, header.uncompressed_page_size, uncompressed_data,
        &header.compressed_page_size, &compressed_data);
    current_page_->data = compressed_data;

    // We allocated the output based on the guessed size, return the extra allocated
    // bytes back to the mem pool.
    parent_->per_file_mem_pool_->ReturnPartialAllocation(
        max_compressed_size - header.compressed_page_size);
  }
  bytes_added += header.compressed_page_size;

  // Add the size of the data page header
  uint8_t* header_buffer;
  uint32_t header_len = 0;
  parent_->thrift_serializer_->Serialize(
      &current_page_->header, &header_len, &header_buffer);
  bytes_added += header_len;

  current_page_->finalized = true;
  total_compressed_byte_size_ += header_len + header.compressed_page_size;
  total_uncompressed_byte_size_ += header_len + header.uncompressed_page_size;
  def_levels_->Clear();
  if (expr_->type() == TYPE_BOOLEAN) bool_values_->Clear();
  return bytes_added;
}

void HdfsParquetTableWriter::ColumnWriter::NewPage() {
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
  current_page_->header.data_page_header.encoding = current_encoding_;
  current_page_->finalized = false;
  current_page_->num_non_null = 0;
}

HdfsParquetTableWriter::HdfsParquetTableWriter(HdfsTableSink* parent, RuntimeState* state,
    OutputPartition* output, const HdfsPartitionDescriptor* part_desc,
    const HdfsTableDescriptor* table_desc, const vector<Expr*>& output_exprs)
    : HdfsTableWriter(parent, state, output, part_desc, table_desc, output_exprs),
      thrift_serializer_(new ThriftSerializer(true)),
      current_row_group_(NULL),
      row_count_(0),
      file_size_limit_(0),
      reusable_col_mem_pool_(new MemPool(state->mem_limits())),
      row_idx_(0) {
}

HdfsParquetTableWriter::~HdfsParquetTableWriter() {
}

Status HdfsParquetTableWriter::Init() {
  columns_.resize(table_desc_->num_cols() - table_desc_->num_clustering_cols());

  // Initialize file metadata
  file_metadata_.version = PARQUET_CURRENT_VERSION;

  stringstream created_by;
  created_by << "impala version " << IMPALA_BUILD_VERSION
             << " (build " << IMPALA_BUILD_HASH << ")";
  file_metadata_.__set_created_by(created_by.str());

  // Default to snappy compressed
  THdfsCompression::type codec = THdfsCompression::SNAPPY;

  const TQueryOptions& query_options = state_->query_options();
  if (query_options.__isset.parquet_compression_codec) {
    codec = query_options.parquet_compression_codec;
  }
  VLOG_FILE << "Using compression codec: " << codec;

  // Initialize each column structure.
  for (int i = 0; i < columns_.size(); ++i) {
    columns_[i] = state_->obj_pool()->Add(
        new ColumnWriter(this, output_exprs_[i], codec));
  }
  RETURN_IF_ERROR(CreateSchema());
  return Status::OK;
}

Status HdfsParquetTableWriter::CreateSchema() {
  int num_clustering_cols = table_desc_->num_clustering_cols();

  // Create flattened tree with a single root.
  file_metadata_.schema.resize(columns_.size() + 1);
  file_metadata_.schema[0].__set_num_children(columns_.size());
  file_metadata_.schema[0].name = "schema";

  for (int i = 0; i < columns_.size(); ++i) {
    parquet::SchemaElement& node = file_metadata_.schema[i + 1];
    node.name = table_desc_->col_names()[i + num_clustering_cols];
    node.__set_type(IMPALA_TO_PARQUET_TYPES[output_exprs_[i]->type()]);
    node.__set_repetition_type(FieldRepetitionType::OPTIONAL);
  }

  return Status::OK;
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
    metadata.type = IMPALA_TO_PARQUET_TYPES[columns_[i]->expr_->type()];
    // Add all encodings that were used in this file.  Currently we use PLAIN and
    // PLAIN_DICTIONARY for data values and RLE for the definition levels.
    metadata.encodings.push_back(Encoding::PLAIN);
    metadata.encodings.push_back(Encoding::RLE);
    if (metadata.type == Type::BYTE_ARRAY) {
      // String columns are initially dictionary encoded
      // TODO: we might not have PLAIN encoding in this case
      metadata.encodings.push_back(Encoding::PLAIN_DICTIONARY);
    }
    metadata.path_in_schema.push_back(table_desc_->col_names()[i + num_clustering_cols]);
    metadata.codec = columns_[i]->codec();
    current_row_group_->columns[i].__set_meta_data(metadata);
  }

  return Status::OK;
}

Status HdfsParquetTableWriter::InitNewFile() {
  DCHECK(current_row_group_ == NULL);

  if (per_file_mem_pool_.get() != NULL) {
    COUNTER_SET(parent_->memory_used_counter(),
        per_file_mem_pool_->peak_allocated_bytes());
  }
  per_file_mem_pool_.reset(new MemPool(state_->mem_limits()));

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
  // 2 * DATA_PAGE_SIZE * num_cols short of the limit. e.g. 50 cols with 8K data
  // pages, means we stop 800KB shy of the limit.
  // Data pages calculate their size precisely when they are complete so having
  // a two page buffer guarantees we will never go over.
  // TODO: this should be made dynamic based on the size of rows seen so far.
  // This would for example, let us account for very long string columns.
  file_size_limit_ -= 2 * DATA_PAGE_SIZE * columns_.size();
  DCHECK_GT(file_size_limit_, DATA_PAGE_SIZE * columns_.size());

  file_pos_ = 0;
  row_count_ = 0;
  file_size_estimate_ = 0;

  file_metadata_.row_groups.clear();
  RETURN_IF_ERROR(AddRowGroup());
  RETURN_IF_ERROR(WriteFileHeader());

  return Status::OK;
}

Status HdfsParquetTableWriter::AppendRowBatch(RowBatch* batch,
                                             const vector<int32_t>& row_group_indices,
                                             bool* new_file) {
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
      file_size_estimate_ += columns_[j]->AppendRow(current_row);
    }
    ++row_idx_;
    ++row_count_;
    ++output_->num_rows;

    if (file_size_estimate_ > file_size_limit_) {
      // This file is full.  We need a new file.
      *new_file = true;
      return Status::OK;
    }
  }

  // Reset the row_idx_ when we exhaust the batch.  We can exit before exhausting
  // the batch if we run out of file space and will continue from the last index.
  row_idx_ = 0;
  return Status::OK;
}

Status HdfsParquetTableWriter::Finalize() {
  SCOPED_TIMER(parent_->hdfs_write_timer());

  // At this point we write out the rest of the file.  We first update the file
  // metadata, now that all the values have been seen.
  file_metadata_.num_rows = row_count_;
  RETURN_IF_ERROR(FlushCurrentRowGroup());
  RETURN_IF_ERROR(WriteFileFooter());

  COUNTER_SET(parent_->memory_used_counter(),
      reusable_col_mem_pool_->peak_allocated_bytes());
  if (per_file_mem_pool_.get() != NULL) {
    COUNTER_SET(parent_->memory_used_counter(),
        per_file_mem_pool_->peak_allocated_bytes());
  }
  COUNTER_UPDATE(parent_->rows_inserted_counter(), row_count_);
  return Status::OK;
}

Status HdfsParquetTableWriter::WriteFileHeader() {
  DCHECK_EQ(file_pos_, 0);
  RETURN_IF_ERROR(Write(PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)));
  file_pos_ += sizeof(PARQUET_VERSION_NUMBER);
  file_size_estimate_ += sizeof(PARQUET_VERSION_NUMBER);
  return Status::OK;
}

Status HdfsParquetTableWriter::FlushCurrentRowGroup() {
  if (current_row_group_ == NULL) return Status::OK;

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

    // Since we don't supported complex schemas, all columns should have the same
    // number of values.
    DCHECK_EQ(current_row_group_->columns[0].meta_data.num_values,
        columns_[i]->num_values());

    // Metadata for this column is complete, write it out to file.  The column metadata
    // goes at the end so that when we have collocated files, the column data can be
    // written without buffering.
    uint32_t len;
    uint8_t* buffer;
    RETURN_IF_ERROR(
        thrift_serializer_->Serialize(&current_row_group_->columns[i], &len, &buffer));
    RETURN_IF_ERROR(Write(buffer, len));
    file_pos_ += len;

    columns_[i]->Reset();
  }

  current_row_group_ = NULL;
  return Status::OK;
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
  return Status::OK;
}

