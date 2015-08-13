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

#include "exec/hdfs-parquet-scanner.h"

#include <limits> // for std::numeric_limits

#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>

#include "common/object-pool.h"
#include "common/logging.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "exec/read-write-util.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/bitmap.h"
#include "util/bit-util.h"
#include "util/decompress.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/dict-encoding.h"
#include "util/rle-encoding.h"
#include "util/runtime-profile.h"
#include "rpc/thrift-util.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using namespace impala;
using namespace strings;

// Provide a workaround for IMPALA-1658.
DEFINE_bool(convert_legacy_hive_parquet_utc_timestamps, false,
    "When true, TIMESTAMPs read from files written by Parquet-MR (used by Hive) will "
    "be converted from UTC to local time. Writes are unaffected.");

// Max data page header size in bytes. This is an estimate and only needs to be an upper
// bound. It is theoretically possible to have a page header of any size due to string
// value statistics, but in practice we'll have trouble reading string values this large.
const int MAX_PAGE_HEADER_SIZE = 8 * 1024 * 1024;

// Max dictionary page header size in bytes. This is an estimate and only needs to be an
// upper bound.
const int MAX_DICT_HEADER_SIZE = 100;

#define LOG_OR_ABORT(error_msg, runtime_state)                          \
  if (runtime_state->abort_on_error()) {                                \
    return Status(error_msg);                                           \
  } else {                                                              \
    runtime_state->LogError(error_msg);                                 \
    return Status::OK();                                                  \
  }

#define LOG_OR_RETURN_ON_ERROR(error_msg, runtime_state)                \
  if (runtime_state->abort_on_error()) {                                \
    return Status(error_msg.msg());                                     \
  }                                                                     \
  runtime_state->LogError(error_msg);

Status HdfsParquetScanner::IssueInitialRanges(HdfsScanNode* scan_node,
    const std::vector<HdfsFileDesc*>& files) {
  vector<DiskIoMgr::ScanRange*> footer_ranges;
  for (int i = 0; i < files.size(); ++i) {
    for (int j = 0; j < files[i]->splits.size(); ++j) {
      DiskIoMgr::ScanRange* split = files[i]->splits[j];

      // Since Parquet scanners always read entire files, only read a file if we're
      // assigned the first split to avoid reading multi-block files with multiple
      // scanners.
      if (split->offset() != 0) {
        // We are expecting each file to be one hdfs block (so all the scan range
        // offsets should be 0).  Having multiple blocks is not incorrect but is
        // nonoptimal so issue a warning.  However, if there is no impalad co-located
        // with the datanode holding the block (i.e. split->expected_local() is false),
        // then this may indicate a pseudo-HDFS system like Isilon where HDFS blocks
        // aren't meaningful from a locality standpoint.  In that case, the warning is
        // spurious so suppress it.
        if (split->expected_local()) {
          scan_node->runtime_state()->LogError(
              ErrorMsg(TErrorCode::PARQUET_MULTIPLE_BLOCKS, files[i]->filename));
        }
        // We assign the entire file to one scan range, so mark all but one split
        // (i.e. the first split) as complete
        scan_node->RangeComplete(THdfsFileFormat::PARQUET, THdfsCompression::NONE);
        continue;
      }

      // Compute the offset of the file footer
      DCHECK_GT(files[i]->file_length, 0);
      int64_t footer_size = min(static_cast<int64_t>(FOOTER_SIZE), files[i]->file_length);
      int64_t footer_start = files[i]->file_length - footer_size;

      ScanRangeMetadata* metadata =
          reinterpret_cast<ScanRangeMetadata*>(split->meta_data());
      DiskIoMgr::ScanRange* footer_range = scan_node->AllocateScanRange(
          files[i]->fs, files[i]->filename.c_str(), footer_size, footer_start,
          metadata->partition_id, split->disk_id(), split->try_cache(),
          split->expected_local(), files[i]->mtime);
      footer_ranges.push_back(footer_range);
    }
  }
  // Issue the footer ranges for all files. The same thread that processes the footer
  // will assemble the rows for this file, so mark these files added completely.
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(footer_ranges, files.size()));
  return Status::OK();
}

namespace impala {

HdfsParquetScanner::HdfsParquetScanner(HdfsScanNode* scan_node, RuntimeState* state)
    : HdfsScanner(scan_node, state),
      metadata_range_(NULL),
      dictionary_pool_(new MemPool(scan_node->mem_tracker())),
      assemble_rows_timer_(scan_node_->materialize_tuple_timer()) {
  assemble_rows_timer_.Stop();
}

HdfsParquetScanner::~HdfsParquetScanner() {
}

// Reader for a single column from the parquet file.  It's associated with a
// ScannerContext::Stream and is responsible for decoding the data.
// Super class for per-type column readers. This contains most of the logic,
// the type specific functions must be implemented in the subclass.
class HdfsParquetScanner::BaseColumnReader {
 public:
  virtual ~BaseColumnReader() {}

  // This is called once for each row group in the file.
  Status Reset(const parquet::ColumnMetaData* metadata, ScannerContext::Stream* stream) {
    DCHECK(stream != NULL);
    DCHECK(metadata != NULL);

    num_buffered_values_ = 0;
    data_ = NULL;
    stream_ = stream;
    metadata_ = metadata;
    dict_decoder_base_ = NULL;
    num_values_read_ = 0;
    if (metadata_->codec != parquet::CompressionCodec::UNCOMPRESSED) {
      RETURN_IF_ERROR(Codec::CreateDecompressor(
          NULL, false, PARQUET_TO_IMPALA_CODEC[metadata_->codec], &decompressor_));
    }
    return Status::OK();
  }

  // Called once when the scanner is complete for final cleanup.
  void Close() {
    if (decompressor_.get() != NULL) decompressor_->Close();
  }

  int64_t total_len() const { return metadata_->total_compressed_size; }
  const SlotDescriptor* slot_desc() const { return node_.slot_desc; }
  const parquet::SchemaElement& schema_element() const { return *node_.element; }
  int col_idx() const { return node_.col_idx; }
  int max_def_level() const { return node_.max_def_level; }
  int max_rep_level() const { return node_.max_rep_level; }
  THdfsCompression::type codec() const {
    if (metadata_ == NULL) return THdfsCompression::NONE;
    return PARQUET_TO_IMPALA_CODEC[metadata_->codec];
  }

  // Read the next value into tuple for this column.  Returns false if there are no
  // more values in the file.
  // *conjuncts_failed is an in/out parameter. If false, it means this row has already
  // been filtered out (i.e. ReadValue is really a SkipValue()) and should be set to
  // true if ReadValue() can filter out this row.
  // TODO: this is the function that needs to be codegen'd (e.g. CodegenReadValue())
  // The codegened functions from all the materialized cols will then be combined
  // into one function.
  // TODO: another option is to materialize col by col for the entire row batch in
  // one call.  e.g. MaterializeCol would write out 1024 values.  Our row batches
  // are currently dense so we'll need to figure out something there.
  bool ReadValue(MemPool* pool, Tuple* tuple, bool* conjuncts_failed);

  // TODO: Some encodings might benefit a lot from a SkipValues(int num_rows) if
  // we know this row can be skipped. This could be very useful with stats and big
  // sections can be skipped. Implement that when we can benefit from it.

 protected:
  friend class HdfsParquetScanner;

  HdfsParquetScanner* parent_;
  const SchemaNode& node_;

  const parquet::ColumnMetaData* metadata_;
  scoped_ptr<Codec> decompressor_;
  ScannerContext::Stream* stream_;

  // Pool to allocate decompression buffers from.
  boost::scoped_ptr<MemPool> decompressed_data_pool_;

  // Header for current data page.
  parquet::PageHeader current_page_header_;

  // Num values remaining in the current data page
  int num_buffered_values_;

  // Pointer to start of next value in data page
  uint8_t* data_;

  // Decoder for definition levels. Only one of these is valid at a time, depending on the
  // data page metadata.
  RleDecoder rle_def_levels_;
  BitReader bit_packed_def_levels_;

  // Decoder for repetition levels. Only one of these is valid at a time, depending on the
  // data page metadata.
  RleDecoder rle_rep_levels_;
  BitReader bit_packed_rep_levels_;

  // Decoder for dictionary-encoded columns. Set by the subclass.
  DictDecoderBase* dict_decoder_base_;

  // The number of values seen so far. Updated per data page.
  int64_t num_values_read_;

  // Cache of the bitmap_filter_ (if any) for this slot.
  const Bitmap* bitmap_filter_;
  // Cache of hash_seed_ to use with bitmap_filter_.
  uint32_t hash_seed_;

  // Bitmap filters are optional (i.e. they can be ignored and the results will be
  // correct). Keep track of stats to determine if the filter is not effective. If
  // the number of rows filtered out is too low, this is not worth the cost.
  // TODO: this should be cost based taking into account how much we save when we
  // filter a row.
  int64_t rows_returned_;
  int64_t bitmap_filter_rows_rejected_;

  BaseColumnReader(HdfsParquetScanner* parent, const SchemaNode& node)
    : parent_(parent),
      node_(node),
      metadata_(NULL),
      stream_(NULL),
      decompressed_data_pool_(new MemPool(parent->scan_node_->mem_tracker())),
      num_buffered_values_(0),
      num_values_read_(0) {
    DCHECK(node.slot_desc != NULL);
    DCHECK_GE(node.col_idx, 0);
    DCHECK_GE(node.max_def_level, 0);

    RuntimeState* state = parent_->scan_node_->runtime_state();
    bitmap_filter_ = state->GetBitmapFilter(slot_desc()->id());
    hash_seed_ = state->fragment_hash_seed();
    rows_returned_ = 0;
    bitmap_filter_rows_rejected_ = 0;
  }

  // Read the next data page.  If a dictionary page is encountered, that will
  // be read and this function will continue reading for the next data page.
  Status ReadDataPage();

  // Returns the definition level for the next value
  // Returns -1 if there was a error parsing it.
  int ReadDefinitionLevel();

  // Returns the repetition level for the next value
  // Returns -1 if there was a error parsing it.
  int ReadRepetitionLevel();

  // Creates a dictionary decoder from values/size. Subclass must implement this
  // and set dict_decoder_base_.
  virtual void CreateDictionaryDecoder(uint8_t* values, int size) = 0;

  // Initializes the reader with the data contents. This is the content for
  // the entire decompressed data page. Decoders can initialize state from
  // here.
  virtual Status InitDataPage(uint8_t* data, int size) = 0;

  // Writes the next value into *slot using pool if necessary.
  // Returns false if there was an error.
  // Subclass must implement this.
  // TODO: we need to remove this with codegen.
  virtual bool ReadSlot(void* slot, MemPool* pool, bool* conjuncts_failed) = 0;

 private:
  // Helper method for creating definition and repetition level decoders
  Status InitLevelDecoders(parquet::Encoding::type encoding, int max_level,
      uint8_t** data, int* data_size, RleDecoder* rle_decoder, BitReader* bit_reader);

  // Helper method for ReadDefinitionLevel() and ReadRepetitionLevel().
  int ReadLevel(parquet::Encoding::type encoding, RleDecoder* rle_decoder,
      BitReader* bit_reader);
};

// Per column type reader.
template<typename T>
class HdfsParquetScanner::ColumnReader : public HdfsParquetScanner::BaseColumnReader {
 public:
  ColumnReader(HdfsParquetScanner* parent, const SchemaNode& node)
    : BaseColumnReader(parent, node) {
    DCHECK_NE(slot_desc()->type().type, TYPE_BOOLEAN);
    if (slot_desc()->type().type == TYPE_DECIMAL) {
      fixed_len_size_ = ParquetPlainEncoder::DecimalSize(slot_desc()->type());
    } else if (slot_desc()->type().type == TYPE_VARCHAR) {
      fixed_len_size_ = slot_desc()->type().len;
    } else {
      fixed_len_size_ = -1;
    }
    needs_conversion_ = slot_desc()->type().type == TYPE_CHAR ||
        // TODO: Add logic to detect file versions that have unconverted TIMESTAMP
        // values. Currently all versions have converted values.
        (FLAGS_convert_legacy_hive_parquet_utc_timestamps &&
        slot_desc()->type().type == TYPE_TIMESTAMP &&
        parent->file_version_.application == "parquet-mr");
  }

 protected:
  virtual void CreateDictionaryDecoder(uint8_t* values, int size) {
    dict_decoder_.reset(new DictDecoder<T>(values, size, fixed_len_size_));
    dict_decoder_base_ = dict_decoder_.get();
  }

  virtual Status InitDataPage(uint8_t* data, int size) {
    if (current_page_header_.data_page_header.encoding ==
          parquet::Encoding::PLAIN_DICTIONARY) {
      if (dict_decoder_.get() == NULL) {
        return Status("File corrupt. Missing dictionary page.");
      }
      dict_decoder_->SetData(data, size);
    }

    // Check if we should disable the bitmap filter. We'll do this if the filter
    // is not removing a lot of rows.
    // TODO: how to pick the selectivity?
    if (bitmap_filter_ != NULL && rows_returned_ > 10000 &&
        bitmap_filter_rows_rejected_ < rows_returned_ * .1) {
      bitmap_filter_ = NULL;
    }
    return Status::OK();
  }

  virtual bool ReadSlot(void* slot, MemPool* pool, bool* conjuncts_failed)  {
    parquet::Encoding::type page_encoding =
        current_page_header_.data_page_header.encoding;
    T val;
    T* val_ptr = needs_conversion_ ? &val : reinterpret_cast<T*>(slot);
    bool result = true;
    if (page_encoding == parquet::Encoding::PLAIN_DICTIONARY) {
      result = dict_decoder_->GetValue(val_ptr);
    } else {
      DCHECK(page_encoding == parquet::Encoding::PLAIN);
      data_ += ParquetPlainEncoder::Decode<T>(data_, fixed_len_size_, val_ptr);
    }
    if (needs_conversion_) ConvertSlot(&val, reinterpret_cast<T*>(slot), pool);
    ++rows_returned_;
    if (!*conjuncts_failed && bitmap_filter_ != NULL) {
      uint32_t h = RawValue::GetHashValue(slot, slot_desc()->type(), hash_seed_);
      *conjuncts_failed = !bitmap_filter_->Get<true>(h);
      ++bitmap_filter_rows_rejected_;
    }
    return result;
  }

 private:
  void CopySlot(T* slot, MemPool* pool) {
    // no-op for non-string columns.
  }

  // Converts and writes src into dst based on desc_->type()
  void ConvertSlot(const T* src, T* dst, MemPool* pool) {
    DCHECK(false);
  }

  scoped_ptr<DictDecoder<T> > dict_decoder_;

  // true decoded values must be converted before being written to an output tuple
  bool needs_conversion_;

  // The size of this column with plain encoding for FIXED_LEN_BYTE_ARRAY, or
  // the max length for VARCHAR columns. Unused otherwise.
  int fixed_len_size_;
};

template<>
void HdfsParquetScanner::ColumnReader<StringValue>::CopySlot(
    StringValue* slot, MemPool* pool) {
  if (slot->len == 0) return;
  uint8_t* buffer = pool->Allocate(slot->len);
  memcpy(buffer, slot->ptr, slot->len);
  slot->ptr = reinterpret_cast<char*>(buffer);
}

template<>
void HdfsParquetScanner::ColumnReader<StringValue>::ConvertSlot(
    const StringValue* src, StringValue* dst, MemPool* pool) {
  DCHECK(slot_desc()->type().type == TYPE_CHAR);
  int len = slot_desc()->type().len;
  StringValue sv;
  sv.len = len;
  if (slot_desc()->type().IsVarLenStringType()) {
    sv.ptr = reinterpret_cast<char*>(pool->Allocate(len));
  } else {
    sv.ptr = reinterpret_cast<char*>(dst);
  }
  int unpadded_len = min(len, src->len);
  memcpy(sv.ptr, src->ptr, unpadded_len);
  StringValue::PadWithSpaces(sv.ptr, len, unpadded_len);

  if (slot_desc()->type().IsVarLenStringType()) *dst = sv;
}

template<>
void HdfsParquetScanner::ColumnReader<TimestampValue>::ConvertSlot(
    const TimestampValue* src, TimestampValue* dst, MemPool* pool) {
  // Conversion should only happen when this flag is enabled.
  DCHECK(FLAGS_convert_legacy_hive_parquet_utc_timestamps);
  *dst = *src;
  if (dst->HasDateAndTime()) dst->UtcToLocal();
}

class HdfsParquetScanner::BoolColumnReader : public HdfsParquetScanner::BaseColumnReader {
 public:
  BoolColumnReader(HdfsParquetScanner* parent, const SchemaNode& node)
    : BaseColumnReader(parent, node) {
    DCHECK_EQ(slot_desc()->type().type, TYPE_BOOLEAN);
  }

 protected:
  virtual void CreateDictionaryDecoder(uint8_t* values, int size) {
    DCHECK(false) << "Dictionary encoding is not supported for bools. Should never "
                  << "have gotten this far.";
  }

  virtual Status InitDataPage(uint8_t* data, int size) {
    // Initialize bool decoder
    bool_values_ = BitReader(data, size);
    return Status::OK();
  }

  virtual bool ReadSlot(void* slot, MemPool* pool, bool* conjuncts_failed)  {
    bool valid = bool_values_.GetValue(1, reinterpret_cast<bool*>(slot));
    if (!valid) parent_->parse_status_ = Status("Invalid bool column.");
    return valid;
  }

 private:
  BitReader bool_values_;
};

}

Status HdfsParquetScanner::Prepare(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(context));
  num_cols_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumColumns", TUnit::UNIT);

  scan_node_->IncNumScannersCodegenDisabled();
  return Status::OK();
}

void HdfsParquetScanner::Close() {
  vector<THdfsCompression::type> compression_types;
  for (int i = 0; i < column_readers_.size(); ++i) {
    if (column_readers_[i]->decompressed_data_pool_.get() != NULL) {
      // No need to commit the row batches with the AttachPool() calls
      // since AddFinalRowBatch() already does below.
      AttachPool(column_readers_[i]->decompressed_data_pool_.get(), false);
    }
    column_readers_[i]->Close();
    compression_types.push_back(column_readers_[i]->codec());
  }
  AttachPool(dictionary_pool_.get(), false);
  AddFinalRowBatch();

  // If this was a metadata only read (i.e. count(*)), there are no columns.
  if (compression_types.empty()) compression_types.push_back(THdfsCompression::NONE);
  scan_node_->RangeComplete(THdfsFileFormat::PARQUET, compression_types);
  assemble_rows_timer_.Stop();
  assemble_rows_timer_.ReleaseCounter();

  HdfsScanner::Close();
}

HdfsParquetScanner::BaseColumnReader* HdfsParquetScanner::CreateReader(
    const SchemaNode& node) {
  BaseColumnReader* reader = NULL;
  switch (node.slot_desc->type().type) {
    case TYPE_BOOLEAN:
      reader = new BoolColumnReader(this, node);
      break;
    case TYPE_TINYINT:
      reader = new ColumnReader<int8_t>(this, node);
      break;
    case TYPE_SMALLINT:
      reader = new ColumnReader<int16_t>(this, node);
      break;
    case TYPE_INT:
      reader = new ColumnReader<int32_t>(this, node);
      break;
    case TYPE_BIGINT:
      reader = new ColumnReader<int64_t>(this, node);
      break;
    case TYPE_FLOAT:
      reader = new ColumnReader<float>(this, node);
      break;
    case TYPE_DOUBLE:
      reader = new ColumnReader<double>(this, node);
      break;
    case TYPE_TIMESTAMP:
      reader = new ColumnReader<TimestampValue>(this, node);
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
      reader = new ColumnReader<StringValue>(this, node);
      break;
    case TYPE_DECIMAL:
      switch (node.slot_desc->type().GetByteSize()) {
        case 4:
          reader = new ColumnReader<Decimal4Value>(this, node);
          break;
        case 8:
          reader = new ColumnReader<Decimal8Value>(this, node);
          break;
        case 16:
          reader = new ColumnReader<Decimal16Value>(this, node);
          break;
      }
      break;
    default:
      DCHECK(false);
  }
  return scan_node_->runtime_state()->obj_pool()->Add(reader);
}

// In 1.1, we had a bug where the dictionary page metadata was not set. Returns true
// if this matches those versions and compatibility workarounds need to be used.
static bool RequiresSkippedDictionaryHeaderCheck(
    const HdfsParquetScanner::FileVersion& v) {
  if (v.application != "impala") return false;
  return v.VersionEq(1,1,0) || (v.VersionEq(1,2,0) && v.is_impala_internal);
}

Status HdfsParquetScanner::BaseColumnReader::ReadDataPage() {
  Status status;
  uint8_t* buffer;

  // We're about to move to the next data page.  The previous data page is
  // now complete, pass along the memory allocated for it.
  parent_->AttachPool(decompressed_data_pool_.get(), false);

  // Read the next data page, skipping page types we don't care about.
  // We break out of this loop on the non-error case (a data page was found or we read all
  // the pages).
  while (true) {
    DCHECK_EQ(num_buffered_values_, 0);
    if (num_values_read_ >= metadata_->num_values) {
      // No more pages to read
      DCHECK_EQ(num_values_read_, metadata_->num_values);
      break;
    }

    int64_t buffer_size;
    RETURN_IF_ERROR(stream_->GetBuffer(true, &buffer, &buffer_size));
    if (buffer_size == 0) {
      DCHECK(stream_->eosr());
      ErrorMsg msg(TErrorCode::PARQUET_COLUMN_METADATA_INVALID,
         metadata_->num_values, num_values_read_,
         slot_desc()->col_pos() - parent_->scan_node_->num_partition_keys());
      LOG_OR_ABORT(msg, parent_->scan_node_->runtime_state());
    }

    // We don't know the actual header size until the thrift object is deserialized.  Loop
    // until we successfully deserialize the header or exceed the maximum header size.
    uint32_t header_size;
    while (true) {
      header_size = buffer_size;
      status = DeserializeThriftMsg(
          buffer, &header_size, true, &current_page_header_);
      if (status.ok()) break;

      if (buffer_size >= MAX_PAGE_HEADER_SIZE) {
        stringstream ss;
        ss << "ParquetScanner: could not read data page because page header exceeded "
           << "maximum size of "
           << PrettyPrinter::Print(MAX_PAGE_HEADER_SIZE, TUnit::BYTES);
        status.AddDetail(ss.str());
        return status;
      }

      // Didn't read entire header, increase buffer size and try again
      Status status;
      int64_t new_buffer_size = max(buffer_size * 2, 1024L);
      bool success = stream_->GetBytes(
          new_buffer_size, &buffer, &new_buffer_size, &status, /* peek */ true);
      if (!success) {
        DCHECK(!status.ok());
        return status;
      }
      DCHECK(status.ok());

      if (buffer_size == new_buffer_size) {
        DCHECK_NE(new_buffer_size, 0);
        ErrorMsg msg(TErrorCode::PARQUET_HEADER_EOF);
        LOG_OR_ABORT(msg, parent_->scan_node_->runtime_state());
      }
      DCHECK_GT(new_buffer_size, buffer_size);
      buffer_size = new_buffer_size;
    }

    // Successfully deserialized current_page_header_
    if (!stream_->SkipBytes(header_size, &status)) return status;

    int data_size = current_page_header_.compressed_page_size;
    int uncompressed_size = current_page_header_.uncompressed_page_size;

    if (current_page_header_.type == parquet::PageType::DICTIONARY_PAGE) {
      if (dict_decoder_base_ != NULL) {
        return Status("Column chunk should not contain two dictionary pages.");
      }
      if (slot_desc()->type().type == TYPE_BOOLEAN) {
        return Status("Unexpected dictionary page. Dictionary page is not"
            " supported for booleans.");
      }
      const parquet::DictionaryPageHeader* dict_header = NULL;
      if (current_page_header_.__isset.dictionary_page_header) {
        dict_header = &current_page_header_.dictionary_page_header;
      } else {
        if (!RequiresSkippedDictionaryHeaderCheck(parent_->file_version_)) {
          return Status("Dictionary page does not have dictionary header set.");
        }
      }
      if (dict_header != NULL &&
          dict_header->encoding != parquet::Encoding::PLAIN &&
          dict_header->encoding != parquet::Encoding::PLAIN_DICTIONARY) {
        return Status("Only PLAIN and PLAIN_DICTIONARY encodings are supported "
            "for dictionary pages.");
      }

      if (!stream_->ReadBytes(data_size, &data_, &status)) return status;

      uint8_t* dict_values = NULL;
      if (decompressor_.get() != NULL) {
        dict_values = parent_->dictionary_pool_->Allocate(uncompressed_size);
        RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, data_size, data_,
            &uncompressed_size, &dict_values));
        VLOG_FILE << "Decompressed " << data_size << " to " << uncompressed_size;
        data_size = uncompressed_size;
      } else {
        DCHECK_EQ(data_size, current_page_header_.uncompressed_page_size);
        // Copy dictionary from io buffer (which will be recycled as we read
        // more data) to a new buffer
        dict_values = parent_->dictionary_pool_->Allocate(data_size);
        memcpy(dict_values, data_, data_size);
      }

      CreateDictionaryDecoder(dict_values, data_size);
      if (dict_header != NULL &&
          dict_header->num_values != dict_decoder_base_->num_entries()) {
        return Status(Substitute(
            "Invalid dictionary. Expected $0 entries but data contained $1 entries",
            dict_header->num_values, dict_decoder_base_->num_entries()));
      }
      // Done with dictionary page, read next page
      continue;
    }

    if (current_page_header_.type != parquet::PageType::DATA_PAGE) {
      // We can safely skip non-data pages
      if (!stream_->SkipBytes(data_size, &status)) return status;
      continue;
    }

    // Read Data Page
    if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
    num_buffered_values_ = current_page_header_.data_page_header.num_values;
    num_values_read_ += num_buffered_values_;

    if (decompressor_.get() != NULL) {
      SCOPED_TIMER(parent_->decompress_timer_);
      uint8_t* decompressed_buffer = decompressed_data_pool_->Allocate(uncompressed_size);
      RETURN_IF_ERROR(decompressor_->ProcessBlock32(true,
          current_page_header_.compressed_page_size, data_, &uncompressed_size,
          &decompressed_buffer));
      VLOG_FILE << "Decompressed " << current_page_header_.compressed_page_size
                << " to " << uncompressed_size;
      DCHECK_EQ(current_page_header_.uncompressed_page_size, uncompressed_size);
      data_ = decompressed_buffer;
      data_size = current_page_header_.uncompressed_page_size;
    } else {
      DCHECK_EQ(metadata_->codec, parquet::CompressionCodec::UNCOMPRESSED);
      DCHECK_EQ(current_page_header_.compressed_page_size, uncompressed_size);
    }

    if (max_rep_level() > 0) {
      // Initialize the repetition level data
      InitLevelDecoders(current_page_header_.data_page_header.repetition_level_encoding,
          max_rep_level(), &data_, &data_size, &rle_rep_levels_, &bit_packed_rep_levels_);
    }

    if (max_def_level() > 0) {
      // Initialize the definition level data
      InitLevelDecoders(current_page_header_.data_page_header.definition_level_encoding,
          max_def_level(), &data_, &data_size, &rle_def_levels_, &bit_packed_def_levels_);
    }

    // Data can be empty if the column contains all NULLs
    if (data_size != 0) RETURN_IF_ERROR(InitDataPage(data_, data_size));
    break;
  }

  return Status::OK();
}

Status HdfsParquetScanner::BaseColumnReader::InitLevelDecoders(
    parquet::Encoding::type encoding, int max_level, uint8_t** data, int* data_size,
    RleDecoder* rle_decoder, BitReader* bit_reader) {
  int32_t num_bytes = 0;
  switch (encoding) {
    case parquet::Encoding::RLE: {
      Status status;
      if (!ReadWriteUtil::Read(data, data_size, &num_bytes, &status)) {
        return status;
      }
      if (num_bytes < 0) {
        return Status(TErrorCode::PARQUET_CORRUPT_VALUE,
            Substitute("RLE level data bytes = $0", num_bytes));
      }
      int bit_width = BitUtil::Log2(max_level + 1);
      *rle_decoder = RleDecoder(*data, num_bytes, bit_width);
      break;
    }
    case parquet::Encoding::BIT_PACKED:
      num_bytes = BitUtil::Ceil(num_buffered_values_, 8);
      *bit_reader = BitReader(*data, num_bytes);
      break;
    default: {
      stringstream ss;
      ss << "Unsupported encoding: " << encoding;
      return Status(ss.str());
    }
  }
  DCHECK_GT(num_bytes, 0);
  *data += num_bytes;
  *data_size -= num_bytes;
  return Status::OK();
}

// TODO More codegen here as well.
inline int HdfsParquetScanner::BaseColumnReader::ReadLevel(
    parquet::Encoding::type encoding, RleDecoder* rle_decoder, BitReader* bit_reader) {
  uint8_t level;
  bool valid = false;
  switch (encoding) {
    case parquet::Encoding::RLE:
      valid = rle_decoder->Get(&level);
      break;
    case parquet::Encoding::BIT_PACKED: {
      valid = bit_reader->GetValue(1, &level);
      break;
    }
    default:
      DCHECK(false);
  }
  if (!valid) return -1;
  return level;
}

inline int HdfsParquetScanner::BaseColumnReader::ReadDefinitionLevel() {
  if (max_def_level() == 0) {
    // This column and any containing structs are required so there is nothing encoded for
    // the definition levels.
    return 0;
  }
  return ReadLevel(current_page_header_.data_page_header.definition_level_encoding,
      &rle_def_levels_, &bit_packed_def_levels_);
}

inline int HdfsParquetScanner::BaseColumnReader::ReadRepetitionLevel() {
  if (max_rep_level() == 0) {
    // This column is not nested in any collection types so there is nothing encoded for
    // the repetition levels.
    return 0;
  }
  return ReadLevel(current_page_header_.data_page_header.repetition_level_encoding,
      &rle_rep_levels_, &bit_packed_rep_levels_);
}

inline bool HdfsParquetScanner::BaseColumnReader::ReadValue(
    MemPool* pool, Tuple* tuple, bool* conjuncts_failed) {
  if (num_buffered_values_ == 0) {
    parent_->assemble_rows_timer_.Stop();
    parent_->parse_status_ = ReadDataPage();
    // We don't return Status objects as parameters because they are too
    // expensive for per row/per col calls.  If ReadDataPage failed,
    // return false to indicate this column reader is done.
    if (num_buffered_values_ == 0 || !parent_->parse_status_.ok()) return false;
    parent_->assemble_rows_timer_.Start();
  }

  --num_buffered_values_;
  int definition_level = ReadDefinitionLevel();
  if (definition_level < 0) return false;

  if (definition_level != max_def_level()) {
    // Null value
    DCHECK_LT(definition_level, max_def_level());
    tuple->SetNull(slot_desc()->null_indicator_offset());
    return true;
  }
  return ReadSlot(tuple->GetSlot(slot_desc()->tuple_offset()), pool, conjuncts_failed);
}

Status HdfsParquetScanner::ProcessSplit() {
  // First process the file metadata in the footer
  bool eosr;
  RETURN_IF_ERROR(ProcessFooter(&eosr));
  if (eosr) return Status::OK();

  // We've processed the metadata and there are columns that need to be materialized.
  RETURN_IF_ERROR(CreateColumnReaders());
  COUNTER_SET(num_cols_counter_, static_cast<int64_t>(column_readers_.size()));

  // The scanner-wide stream was used only to read the file footer.  Each column has added
  // its own stream.
  stream_ = NULL;

  // Iterate through each row group in the file and read all the materialized columns
  // per row group.  Row groups are independent, so this this could be parallelized.
  // However, having multiple row groups per file should be seen as an edge case and
  // we can do better parallelizing across files instead.
  // TODO: not really an edge case since MR writes multiple row groups
  for (int i = 0; i < file_metadata_.row_groups.size(); ++i) {
    // Attach any resources and clear the streams before starting a new row group. These
    // streams could either be just the footer stream or streams for the previous row
    // group.
    context_->ReleaseCompletedResources(batch_, /* done */ true);
    // Commit the rows to flush the row batch from the previous row group
    CommitRows(0);

    RETURN_IF_ERROR(InitColumns(i));
    RETURN_IF_ERROR(AssembleRows(i));
  }
  return Status::OK();
}

// TODO: this needs to be codegen'd.  The ReadValue function needs to be codegen'd,
// specific to type and encoding and then inlined into AssembleRows().
Status HdfsParquetScanner::AssembleRows(int row_group_idx) {
  assemble_rows_timer_.Start();
  // Read at most as many rows as stated in the metadata
  int64_t expected_rows_in_group = file_metadata_.row_groups[row_group_idx].num_rows;
  int64_t rows_read = 0;
  bool reached_limit = scan_node_->ReachedLimit();
  bool cancelled = context_->cancelled();
  int num_column_readers = column_readers_.size();
  MemPool* pool;

  while (!reached_limit && !cancelled && rows_read < expected_rows_in_group) {
    Tuple* tuple;
    TupleRow* row;
    int64_t row_mem_limit = static_cast<int64_t>(GetMemory(&pool, &tuple, &row));
    int64_t expected_rows_to_read = expected_rows_in_group - rows_read;
    int64_t num_rows = std::min(expected_rows_to_read, row_mem_limit);

    int num_to_commit = 0;
    if (num_column_readers > 0) {
      for (int i = 0; i < num_rows; ++i) {
        bool conjuncts_failed = false;
        InitTuple(template_tuple_, tuple);
        for (int c = 0; c < num_column_readers; ++c) {
          if (!column_readers_[c]->ReadValue(pool, tuple, &conjuncts_failed)) {
            assemble_rows_timer_.Stop();
            // If we reach this point, it means that we have read the entire column chunk,
            // or we hit either a parse or a mem limit error.
            // For correctly formed files, this indicates we are done with this row
            // group and this should be the first column we are reading.
            DCHECK(c == 0 || !parse_status_.ok())
              << "c=" << c << " " << parse_status_.GetDetail();;
            COUNTER_ADD(scan_node_->rows_read_counter(), i);
            RETURN_IF_ERROR(CommitRows(num_to_commit));

            // Unless we terminated early due to mem limit exceeded error, test if the
            // actual number of rows in the file matches the expected number of rows
            // from metadata.
            rows_read += i;
            if (rows_read != expected_rows_in_group &&
                !parse_status_.IsMemLimitExceeded()) {
              HdfsParquetScanner::BaseColumnReader* reader = column_readers_[c];
              DCHECK(reader->stream_ != NULL);
              ErrorMsg msg(TErrorCode::PARQUET_GROUP_ROW_COUNT_ERROR,
                 reader->stream_->filename(), row_group_idx,
                 expected_rows_in_group, rows_read);
              msg.AddDetail(parse_status_.GetDetail());
              LOG_OR_RETURN_ON_ERROR(msg, scan_node_->runtime_state());
            }
            return parse_status_;
          }
        }
        if (conjuncts_failed) continue;
        row->SetTuple(scan_node_->tuple_idx(), tuple);
        if (EvalConjuncts(row)) {
          row = next_row(row);
          tuple = next_tuple(tuple);
          ++num_to_commit;
        }
      }
    } else {
      // Special case when there is no data for the accessed column(s) in the file.
      // This can happen, for example, due to schema evolution (alter table add column).
      // Since all the tuples are same, evaluating conjuncts only for the first tuple.
      DCHECK_GT(num_rows, 0);
      InitTuple(template_tuple_, tuple);
      row->SetTuple(scan_node_->tuple_idx(), tuple);
      if (EvalConjuncts(row)) {
        row = next_row(row);
        tuple = next_tuple(tuple);

        for (int i = 1; i < num_rows; ++i) {
          InitTuple(template_tuple_, tuple);
          row->SetTuple(scan_node_->tuple_idx(), tuple);
          row = next_row(row);
          tuple = next_tuple(tuple);
        }
        num_to_commit += num_rows;
      }
    }
    rows_read += num_rows;
    COUNTER_ADD(scan_node_->rows_read_counter(), num_rows);
    RETURN_IF_ERROR(CommitRows(num_to_commit));

    reached_limit = scan_node_->ReachedLimit();
    cancelled = context_->cancelled();
  }

  if (!reached_limit && !cancelled && (num_column_readers > 0)) {
    // If we get to this point, it means that we have read as many rows as the metadata
    // told us we should read. Attempt to read one more row and if that succeeds report
    // the error.
    DCHECK_EQ(rows_read, expected_rows_in_group);
    uint8_t dummy_tuple_mem[tuple_byte_size_];
    Tuple* dummy_tuple = reinterpret_cast<Tuple*>(&dummy_tuple_mem);
    InitTuple(template_tuple_, dummy_tuple);
    bool conjuncts_failed = false;
    if (column_readers_[0]->ReadValue(pool, dummy_tuple, &conjuncts_failed)) {
      // If another tuple is successfully read, it means that there are still values
      // in the file.
      HdfsParquetScanner::BaseColumnReader* reader = column_readers_[0];
      DCHECK(reader->stream_ != NULL);
      ErrorMsg msg(TErrorCode::PARQUET_GROUP_ROW_COUNT_OVERFLOW,
          reader->stream_->filename(), row_group_idx,
          expected_rows_in_group);
      LOG_OR_RETURN_ON_ERROR(msg, scan_node_->runtime_state());
    }
  }

  assemble_rows_timer_.Stop();
  return parse_status_;
}

Status HdfsParquetScanner::ProcessFooter(bool* eosr) {
  *eosr = false;
  uint8_t* buffer;
  int64_t len;

  RETURN_IF_ERROR(stream_->GetBuffer(false, &buffer, &len));
  DCHECK(stream_->eosr());

  // Number of bytes in buffer after the fixed size footer is accounted for.
  int remaining_bytes_buffered = len - sizeof(int32_t) - sizeof(PARQUET_VERSION_NUMBER);

  // Make sure footer has enough bytes to contain the required information.
  if (remaining_bytes_buffered < 0) {
    return Status(Substitute("File $0 is invalid.  Missing metadata.",
        stream_->filename()));
  }

  // Validate magic file bytes are correct.
  uint8_t* magic_number_ptr = buffer + len - sizeof(PARQUET_VERSION_NUMBER);
  if (memcmp(magic_number_ptr, PARQUET_VERSION_NUMBER,
             sizeof(PARQUET_VERSION_NUMBER)) != 0) {
    return Status(Substitute("File $0 is invalid.  Invalid file footer: $1",
        stream_->filename(),
        string((char*)magic_number_ptr, sizeof(PARQUET_VERSION_NUMBER))));
  }

  // The size of the metadata is encoded as a 4 byte little endian value before
  // the magic number
  uint8_t* metadata_size_ptr = magic_number_ptr - sizeof(int32_t);
  uint32_t metadata_size = *reinterpret_cast<uint32_t*>(metadata_size_ptr);
  uint8_t* metadata_ptr = metadata_size_ptr - metadata_size;
  // If the metadata was too big, we need to stitch it before deserializing it.
  // In that case, we stitch the data in this buffer.
  vector<uint8_t> metadata_buffer;
  metadata_range_ = stream_->scan_range();

  if (UNLIKELY(metadata_size > remaining_bytes_buffered)) {
    // In this case, the metadata is bigger than our guess meaning there are
    // not enough bytes in the footer range from IssueInitialRanges().
    // We'll just issue more ranges to the IoMgr that is the actual footer.
    const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(metadata_range_->file());
    DCHECK(file_desc != NULL);
    // The start of the metadata is:
    // file_length - 4-byte metadata size - footer-size - metadata size
    int64_t metadata_start = file_desc->file_length -
      sizeof(int32_t) - sizeof(PARQUET_VERSION_NUMBER) - metadata_size;
    int64_t metadata_bytes_to_read = metadata_size;
    if (metadata_start < 0) {
      return Status(Substitute("File $0 is invalid. Invalid metadata size in file "
          "footer: $1 bytes. File size: $2 bytes.", stream_->filename(), metadata_size,
          file_desc->file_length));
    }
    // IoMgr can only do a fixed size Read(). The metadata could be larger
    // so we stitch it here.
    // TODO: consider moving this stitching into the scanner context. The scanner
    // context usually handles the stitching but no other scanner need this logic
    // now.
    metadata_buffer.resize(metadata_size);
    metadata_ptr = &metadata_buffer[0];
    int64_t copy_offset = 0;
    DiskIoMgr* io_mgr = scan_node_->runtime_state()->io_mgr();

    while (metadata_bytes_to_read > 0) {
      int64_t to_read = ::min(static_cast<int64_t>(io_mgr->max_read_buffer_size()),
          metadata_bytes_to_read);
      DiskIoMgr::ScanRange* range = scan_node_->AllocateScanRange(
          metadata_range_->fs(), metadata_range_->file(), to_read,
          metadata_start + copy_offset, -1, metadata_range_->disk_id(),
          metadata_range_->try_cache(), metadata_range_->expected_local(),
          file_desc->mtime);

      DiskIoMgr::BufferDescriptor* io_buffer = NULL;
      RETURN_IF_ERROR(io_mgr->Read(scan_node_->reader_context(), range, &io_buffer));
      memcpy(metadata_ptr + copy_offset, io_buffer->buffer(), io_buffer->len());
      io_buffer->Return();

      metadata_bytes_to_read -= to_read;
      copy_offset += to_read;
    }
    DCHECK_EQ(metadata_bytes_to_read, 0);
  }
  // Deserialize file header
  // TODO: this takes ~7ms for a 1000-column table, figure out how to reduce this.
  Status status =
      DeserializeThriftMsg(metadata_ptr, &metadata_size, true, &file_metadata_);
  if (!status.ok()) {
    return Status(Substitute("File $0 has invalid file metadata at file offset $1. "
        "Error = $2.", stream_->filename(),
        metadata_size + sizeof(PARQUET_VERSION_NUMBER) + sizeof(uint32_t),
        status.GetDetail()));
  }

  RETURN_IF_ERROR(ValidateFileMetadata());
  // Parse file schema
  RETURN_IF_ERROR(CreateSchemaTree(file_metadata_.schema, &schema_));

  if (scan_node_->materialized_slots().empty()) {
    // No materialized columns.  We can serve this query from just the metadata.  We
    // don't need to read the column data.
    int64_t num_tuples = file_metadata_.num_rows;
    COUNTER_ADD(scan_node_->rows_read_counter(), num_tuples);

    while (num_tuples > 0) {
      MemPool* pool;
      Tuple* tuple;
      TupleRow* current_row;
      int max_tuples = GetMemory(&pool, &tuple, &current_row);
      max_tuples = min(static_cast<int64_t>(max_tuples), num_tuples);
      num_tuples -= max_tuples;

      int num_to_commit = WriteEmptyTuples(context_, current_row, max_tuples);
      RETURN_IF_ERROR(CommitRows(num_to_commit));
    }

    *eosr = true;
    return Status::OK();
  } else if (file_metadata_.num_rows == 0) {
    // Empty file
    *eosr = true;
    return Status::OK();
  }

  if (file_metadata_.row_groups.empty()) {
    return Status(Substitute("Invalid file. This file: $0 has no row groups",
                             stream_->filename()));
  }
  return Status::OK();
}

Status HdfsParquetScanner::CreateColumnReaders() {
  DCHECK(column_readers_.empty());
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = scan_node_->materialized_slots()[i];
    const SchemaPath& path = slot_desc->col_path();
    SchemaNode* node = &schema_;
    // Traverse path and resolve node to this slot's SchemaNode, or NULL if this slot
    // doesn't exist in this file's schema
    for (int j = 0; j < path.size(); ++j) {
      int idx = j > 0 ? path[j] : path[j] - scan_node_->num_partition_keys();
      if (node->children.size() <= idx) {
        // The selected column is not in the file
        VLOG_FILE << Substitute("File '$0' does not contain path '$1'",
            stream_->filename(), PrintPath(*scan_node_->hdfs_table(), path));
        node = NULL;
        break;
      }
      node = &node->children[idx];
    }

    if (node != NULL && node->children.size() > 0) {
      string error = Substitute("Path '$0' is not a supported type in file '$1'",
          PrintPath(*scan_node_->hdfs_table(), path), stream_->filename());
      VLOG_QUERY << error << endl << schema_.DebugString();
      return Status(error);
    }

    if (node == NULL) {
      // In this case, we are selecting a column that is not in the file.
      // Update the template tuple to put a NULL in this slot.
      if (template_tuple_ == NULL) {
        template_tuple_ = scan_node_->InitEmptyTemplateTuple();
      }
      template_tuple_->SetNull(slot_desc->null_indicator_offset());
      continue;
    }
    node->slot_desc = slot_desc;

    column_readers_.push_back(CreateReader(*node));
  }
  return Status::OK();
}

Status HdfsParquetScanner::InitColumns(int row_group_idx) {
  const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(metadata_range_->file());
  DCHECK(file_desc != NULL);
  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx];

  // All the scan ranges (one for each column).
  vector<DiskIoMgr::ScanRange*> col_ranges;

  for (int i = 0; i < column_readers_.size(); ++i) {
    const parquet::ColumnChunk& col_chunk =
        row_group.columns[column_readers_[i]->col_idx()];
    int64_t col_start = col_chunk.meta_data.data_page_offset;
    RETURN_IF_ERROR(ValidateColumn(*column_readers_[i], row_group_idx));

    // If there is a dictionary page, the file format requires it to come before
    // any data pages.  We need to start reading the column from the data page.
    if (col_chunk.meta_data.__isset.dictionary_page_offset) {
      if (col_chunk.meta_data.dictionary_page_offset >= col_start) {
        stringstream ss;
        ss << "File " << file_desc->filename << ": metadata is corrupt. "
           << "Dictionary page (offset=" << col_chunk.meta_data.dictionary_page_offset
           << ") must come before any data pages (offset=" << col_start << ").";
        return Status(ss.str());
      }
      col_start = col_chunk.meta_data.dictionary_page_offset;
    }
    int64_t col_len = col_chunk.meta_data.total_compressed_size;
    int64_t col_end = col_start + col_len;
    if (col_end <= 0 || col_end > file_desc->file_length) {
      stringstream ss;
      ss << "File " << file_desc->filename << ": metadata is corrupt. "
         << "Column " << column_readers_[i]->col_idx() << " has invalid column offsets "
         << "(offset=" << col_start << ", size=" << col_len << ", "
         << "file_size=" << file_desc->file_length << ").";
      return Status(ss.str());
    }
    if (file_version_.application == "parquet-mr" && file_version_.VersionLt(1, 2, 9)) {
      // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
      // dictionary page header size in total_compressed_size and total_uncompressed_size
      // (see IMPALA-694). We pad col_len to compensate.
      int64_t bytes_remaining = file_desc->file_length - col_end;
      int64_t pad = min(static_cast<int64_t>(MAX_DICT_HEADER_SIZE), bytes_remaining);
      col_len += pad;
    }

    // TODO: this will need to change when we have co-located files and the columns
    // are different files.
    if (!col_chunk.file_path.empty()) {
      DCHECK_EQ(col_chunk.file_path, string(metadata_range_->file()));
    }

    DiskIoMgr::ScanRange* col_range = scan_node_->AllocateScanRange(
        metadata_range_->fs(), metadata_range_->file(), col_len, col_start,
        column_readers_[i]->col_idx(), metadata_range_->disk_id(),
        metadata_range_->try_cache(), metadata_range_->expected_local(),
        file_desc->mtime);
    col_ranges.push_back(col_range);

    // Get the stream that will be used for this column
    ScannerContext::Stream* stream = context_->AddStream(col_range);
    DCHECK(stream != NULL);

    RETURN_IF_ERROR(column_readers_[i]->Reset(&col_chunk.meta_data, stream));

    if (!scan_node_->materialized_slots()[i]->type().IsStringType() ||
        col_chunk.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED) {
      // Non-string types are always compact.  Compressed columns don't reference data
      // in the io buffers after tuple materialization.  In both cases, we can set compact
      // to true and recycle buffers more promptly.
      stream->set_contains_tuple_data(false);
    }
  }
  DCHECK_EQ(col_ranges.size(), column_readers_.size());
  DCHECK_GE(scan_node_->materialized_slots().size(), column_readers_.size());

  // Issue all the column chunks to the io mgr and have them scheduled immediately.
  // This means these ranges aren't returned via DiskIoMgr::GetNextRange and
  // instead are scheduled to be read immediately.
  RETURN_IF_ERROR(scan_node_->runtime_state()->io_mgr()->AddScanRanges(
      scan_node_->reader_context(), col_ranges, true));

  return Status::OK();
}

Status HdfsParquetScanner::CreateSchemaTree(const vector<parquet::SchemaElement>& schema,
    HdfsParquetScanner::SchemaNode* node) const {
  int max_def_level = 0;
  int max_rep_level = 0;
  int idx = 0;
  int col_idx = 0;
  return CreateSchemaTree(schema, max_def_level, max_rep_level, &idx, &col_idx, node);
}

Status HdfsParquetScanner::CreateSchemaTree(
    const vector<parquet::SchemaElement>& schema, int max_def_level, int max_rep_level,
    int* idx, int* col_idx, HdfsParquetScanner::SchemaNode* node) const {
  if (*idx >= schema.size()) {
    return Status(Substitute("File $0 corrupt: could not reconstruct schema tree from "
            "flattened schema in file metadata", stream_->filename()));
  }
  node->element = &schema[*idx];
  ++(*idx);

  if (node->element->num_children == 0) {
    // node is a leaf node, meaning it's materialized in the file and appears in
    // file_metadata_.row_groups.columns
    node->col_idx = *col_idx;
    ++(*col_idx);
  }

  if (node->element->repetition_type == parquet::FieldRepetitionType::OPTIONAL) {
    ++max_def_level;
  } else if (node->element->repetition_type == parquet::FieldRepetitionType::REPEATED) {
    ++max_rep_level;
    // Repeated fields add a definition level. This is used to distinguish between an
    // empty list and a list with an item in it.
    ++max_def_level;
  }
  node->max_def_level = max_def_level;
  node->max_rep_level = max_rep_level;

  node->children.resize(node->element->num_children);
  for (int i = 0; i < node->element->num_children; ++i) {
    RETURN_IF_ERROR(CreateSchemaTree(
        schema, max_def_level, max_rep_level, idx, col_idx, &node->children[i]));
  }
  return Status::OK();
}

HdfsParquetScanner::FileVersion::FileVersion(const string& created_by) {
  string created_by_lower = created_by;
  std::transform(created_by_lower.begin(), created_by_lower.end(),
      created_by_lower.begin(), ::tolower);
  is_impala_internal = false;

  vector<string> tokens;
  split(tokens, created_by_lower, is_any_of(" "), token_compress_on);
  // Boost always creates at least one token
  DCHECK_GT(tokens.size(), 0);
  application = tokens[0];

  if (tokens.size() >= 3 && tokens[1] == "version") {
    string version_string = tokens[2];
    // Ignore any trailing nodextra characters
    int n = version_string.find_first_not_of("0123456789.");
    string version_string_trimmed = version_string.substr(0, n);

    vector<string> version_tokens;
    split(version_tokens, version_string_trimmed, is_any_of("."));
    version.major = version_tokens.size() >= 1 ? atoi(version_tokens[0].c_str()) : 0;
    version.minor = version_tokens.size() >= 2 ? atoi(version_tokens[1].c_str()) : 0;
    version.patch = version_tokens.size() >= 3 ? atoi(version_tokens[2].c_str()) : 0;

    if (application == "impala") {
      if (version_string.find("-internal") != string::npos) is_impala_internal = true;
    }
  } else {
    version.major = 0;
    version.minor = 0;
    version.patch = 0;
  }
}

bool HdfsParquetScanner::FileVersion::VersionLt(int major, int minor, int patch) const {
  if (version.major < major) return true;
  if (version.major > major) return false;
  DCHECK_EQ(version.major, major);
  if (version.minor < minor) return true;
  if (version.minor > minor) return false;
  DCHECK_EQ(version.minor, minor);
  return version.patch < patch;
}

bool HdfsParquetScanner::FileVersion::VersionEq(int major, int minor, int patch) const {
  return version.major == major && version.minor == minor && version.patch == patch;
}

Status HdfsParquetScanner::ValidateFileMetadata() {
  if (file_metadata_.version > PARQUET_CURRENT_VERSION) {
    stringstream ss;
    ss << "File: " << stream_->filename() << " is of an unsupported version. "
       << "file version: " << file_metadata_.version;
    return Status(ss.str());
  }

  // Parse out the created by application version string
  if (file_metadata_.__isset.created_by) {
    file_version_ = FileVersion(file_metadata_.created_by);
  }
  return Status::OK();
}

bool IsEncodingSupported(parquet::Encoding::type e) {
  switch (e) {
    case parquet::Encoding::PLAIN:
    case parquet::Encoding::PLAIN_DICTIONARY:
    case parquet::Encoding::BIT_PACKED:
    case parquet::Encoding::RLE:
      return true;
    default:
      return false;
  }
}

Status HdfsParquetScanner::ValidateColumn(
    const BaseColumnReader& col_reader, int row_group_idx) {
  const SlotDescriptor* slot_desc = col_reader.slot_desc();
  int col_idx = col_reader.col_idx();
  const parquet::SchemaElement& schema_element = col_reader.schema_element();
  parquet::ColumnChunk& file_data =
      file_metadata_.row_groups[row_group_idx].columns[col_idx];

  // Check the encodings are supported
  vector<parquet::Encoding::type>& encodings = file_data.meta_data.encodings;
  for (int i = 0; i < encodings.size(); ++i) {
    if (!IsEncodingSupported(encodings[i])) {
      stringstream ss;
      ss << "File '" << metadata_range_->file() << "' uses an unsupported encoding: "
         << PrintEncoding(encodings[i]) << " for column '" << schema_element.name
         << "'.";
      return Status(ss.str());
    }
  }

  // Check the compression is supported
  if (file_data.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED &&
      file_data.meta_data.codec != parquet::CompressionCodec::SNAPPY &&
      file_data.meta_data.codec != parquet::CompressionCodec::GZIP) {
    stringstream ss;
    ss << "File '" << metadata_range_->file() << "' uses an unsupported compression: "
        << file_data.meta_data.codec << " for column '" << schema_element.name
        << "'.";
    return Status(ss.str());
  }

  // Check the type in the file is compatible with the catalog metadata.
  parquet::Type::type type = IMPALA_TO_PARQUET_TYPES[slot_desc->type().type];
  if (type != file_data.meta_data.type) {
    stringstream ss;
    ss << "File '" << metadata_range_->file() << "' has an incompatible type with the"
       << " table schema for column '" << schema_element.name << "'.  Expected type: "
       << type << ".  Actual type: " << file_data.meta_data.type;
    return Status(ss.str());
  }

  // Check that this column is optional or required
  if (schema_element.repetition_type != parquet::FieldRepetitionType::OPTIONAL &&
      schema_element.repetition_type != parquet::FieldRepetitionType::REQUIRED) {
    stringstream ss;
    ss << "File '" << metadata_range_->file() << "' column '" << schema_element.name
       << "' contains an unsupported column repetition type: "
       << schema_element.repetition_type;
    return Status(ss.str());
  }

  // Check the decimal scale in the file matches the metastore scale and precision.
  // We fail the query if the metadata makes it impossible for us to safely read
  // the file. If we don't require the metadata, we will fail the query if
  // abort_on_error is true, otherwise we will just log a warning.
  bool is_converted_type_decimal = schema_element.__isset.converted_type &&
      schema_element.converted_type == parquet::ConvertedType::DECIMAL;
  if (slot_desc->type().type == TYPE_DECIMAL) {
    // We require that the scale and byte length be set.
    if (schema_element.type != parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      stringstream ss;
      ss << "File '" << metadata_range_->file() << "' column '" << schema_element.name
         << "' should be a decimal column encoded using FIXED_LEN_BYTE_ARRAY.";
      return Status(ss.str());
    }

    if (!schema_element.__isset.type_length) {
      stringstream ss;
      ss << "File '" << metadata_range_->file() << "' column '" << schema_element.name
         << "' does not have type_length set.";
      return Status(ss.str());
    }

    int expected_len = ParquetPlainEncoder::DecimalSize(slot_desc->type());
    if (schema_element.type_length != expected_len) {
      stringstream ss;
      ss << "File '" << metadata_range_->file() << "' column '" << schema_element.name
         << "' has an invalid type length. Expecting: " << expected_len
         << " len in file: " << schema_element.type_length;
      return Status(ss.str());
    }

    if (!schema_element.__isset.scale) {
      stringstream ss;
      ss << "File '" << metadata_range_->file() << "' column '" << schema_element.name
         << "' does not have the scale set.";
      return Status(ss.str());
    }

    if (schema_element.scale != slot_desc->type().scale) {
      // TODO: we could allow a mismatch and do a conversion at this step.
      stringstream ss;
      ss << "File '" << metadata_range_->file() << "' column '" << schema_element.name
         << "' has a scale that does not match the table metadata scale."
         << " File metadata scale: " << schema_element.scale
         << " Table metadata scale: " << slot_desc->type().scale;
      return Status(ss.str());
    }

    // The other decimal metadata should be there but we don't need it.
    if (!schema_element.__isset.precision) {
      ErrorMsg msg(TErrorCode::PARQUET_MISSING_PRECISION,
          metadata_range_->file(), schema_element.name);
      LOG_OR_RETURN_ON_ERROR(msg, state_);
    } else {
      if (schema_element.precision != slot_desc->type().precision) {
        // TODO: we could allow a mismatch and do a conversion at this step.
        ErrorMsg msg(TErrorCode::PARQUET_WRONG_PRECISION,
            metadata_range_->file(), schema_element.name,
            schema_element.precision, slot_desc->type().precision);
        LOG_OR_RETURN_ON_ERROR(msg, state_);
      }
    }

    if (!is_converted_type_decimal) {
      // TODO: is this validation useful? It is not required at all to read the data and
      // might only serve to reject otherwise perfectly readable files.
      ErrorMsg msg(TErrorCode::PARQUET_BAD_CONVERTED_TYPE,
          metadata_range_->file(), schema_element.name);
      LOG_OR_RETURN_ON_ERROR(msg, state_);
    }
  } else if (schema_element.__isset.scale || schema_element.__isset.precision ||
      is_converted_type_decimal) {
    ErrorMsg msg(TErrorCode::PARQUET_INCOMPATIBLE_DECIMAL,
        metadata_range_->file(), schema_element.name, slot_desc->type().DebugString());
    LOG_OR_RETURN_ON_ERROR(msg, state_);
  }
  return Status::OK();
}

string PrintRepetitionType(const parquet::FieldRepetitionType::type& t) {
  switch (t) {
    case parquet::FieldRepetitionType::REQUIRED: return "required";
    case parquet::FieldRepetitionType::OPTIONAL: return "optional";
    case parquet::FieldRepetitionType::REPEATED: return "repeated";
    default: return "<unknown>";
  }
}

string PrintParquetType(const parquet::Type::type& t) {
  switch (t) {
    case parquet::Type::BOOLEAN: return "boolean";
    case parquet::Type::INT32: return "int32";
    case parquet::Type::INT64: return "int64";
    case parquet::Type::INT96: return "int96";
    case parquet::Type::FLOAT: return "float";
    case parquet::Type::DOUBLE: return "double";
    case parquet::Type::BYTE_ARRAY: return "byte_array";
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: return "fixed_len_byte_array";
    default: return "<unknown>";
  }
}

string HdfsParquetScanner::SchemaNode::DebugString(int indent) const {
  stringstream ss;
  for (int i = 0; i < indent; ++i) ss << " ";
  ss << PrintRepetitionType(element->repetition_type) << " ";
  if (element->num_children > 0) {
    ss << "struct";
  } else {
    ss << PrintParquetType(element->type);
  }
  ss << " " << element->name << " [i:" << col_idx << " d:" << max_def_level << "]";
  if (element->num_children > 0) {
    ss << " {" << endl;
    for (int i = 0; i < element->num_children; ++i) {
      ss << children[i].DebugString(indent + 2) << endl;
    }
    for (int i = 0; i < indent; ++i) ss << " ";
    ss << "}";
  }
  return ss.str();
}
