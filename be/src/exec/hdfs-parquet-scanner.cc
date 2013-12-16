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

#include <boost/algorithm/string.hpp>

#include "common/object-pool.h"
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
#include "util/bit-util.h"
#include "util/decompress.h"
#include "util/debug-util.h"
#include "util/dict-encoding.h"
#include "util/rle-encoding.h"
#include "util/runtime-profile.h"
#include "rpc/thrift-util.h"

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace impala;

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
        // We are expecting each file to be one hdfs block (so all the scan range offsets
        // should be 0).  This is not incorrect but we will issue a warning.
        stringstream ss;
        ss << "Parquet file should not be split into multiple hdfs-blocks."
           << " file=" << files[i]->filename;
        scan_node->runtime_state()->LogError(ss.str());
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
          reinterpret_cast<ScanRangeMetadata*>(files[i]->splits[0]->meta_data());
      DiskIoMgr::ScanRange* footer_range = scan_node->AllocateScanRange(
          files[i]->filename.c_str(), footer_size,
          footer_start, metadata->partition_id, files[i]->splits[0]->disk_id());
      footer_ranges.push_back(footer_range);
    }
  }
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(footer_ranges));
  return Status::OK;
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
  Status Reset(const parquet::SchemaElement* schema_element,
      const parquet::ColumnMetaData* metadata, ScannerContext::Stream* stream) {
    DCHECK(stream != NULL);
    DCHECK(metadata != NULL);

    field_repetition_type_ = schema_element->repetition_type;
    num_buffered_values_ = 0;
    data_ = NULL;
    stream_ = stream;
    metadata_ = metadata;
    dict_decoder_base_ = NULL;
    if (metadata_->codec != parquet::CompressionCodec::UNCOMPRESSED) {
      RETURN_IF_ERROR(Codec::CreateDecompressor(
          NULL, false, PARQUET_TO_IMPALA_CODEC[metadata_->codec], &decompressor_));
    }
    return Status::OK;
  }

  // Called once when the scanner is complete for final cleanup.
  void Close() {
    if (decompressor_.get() != NULL) decompressor_->Close();
  }

  int64_t total_len() const { return metadata_->total_compressed_size; }
  const SlotDescriptor* slot_desc() const { return desc_; }
  int file_idx() const { return file_idx_; }
  THdfsCompression::type codec() const {
    if (metadata_ == NULL) return THdfsCompression::NONE;
    return PARQUET_TO_IMPALA_CODEC[metadata_->codec];
  }

  // Read the next value into tuple for this column.  Returns
  // false if there are no more values in the file.
  // TODO: this is the function that needs to be codegen'd (e.g. CodegenReadValue())
  // The codegened functions from all the materialized cols will then be combined
  // into one function.
  // TODO: another option is to materialize col by col for the entire row batch in
  // one call.  e.g. MaterializeCol would write out 1024 values.  Our row batches
  // are currently dense so we'll need to figure out something there.
  bool ReadValue(MemPool* pool, Tuple* tuple);

 protected:
  friend class HdfsParquetScanner;

  HdfsParquetScanner* parent_;
  const SlotDescriptor* desc_;

  // Index of this column in the this parquet file.
  int file_idx_;

  // This is either required, optional or repeated.
  // If it is required, the column cannot have NULLs.
  parquet::FieldRepetitionType::type field_repetition_type_;

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

  // Decoder for definition.  Only one of these is valid at a time, depending on
  // the data page metadata.
  RleDecoder rle_def_levels_;
  BitReader bit_packed_def_levels_;

  // Decoder for dictionary-encoded columns. Set by the subclass.
  DictDecoderBase* dict_decoder_base_;

  // The number of values seen so far. Updated per data page.
  int64_t num_values_;

  BaseColumnReader(HdfsParquetScanner* parent, const SlotDescriptor* desc, int file_idx)
    : parent_(parent),
      desc_(desc),
      file_idx_(file_idx),
      field_repetition_type_(parquet::FieldRepetitionType::OPTIONAL),
      metadata_(NULL),
      stream_(NULL),
      decompressed_data_pool_(new MemPool(parent->scan_node_->mem_tracker())),
      num_buffered_values_(0),
      num_values_(0) {
  }

  // Read the next data page.  If a dictionary page is encountered, that will
  // be read and this function will continue reading for the next data page.
  Status ReadDataPage();

  // Returns the definition level for the next value
  // Returns -1 if there was a error parsing it.
  int ReadDefinitionLevel();

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
  virtual bool ReadSlot(void* slot, MemPool* pool) = 0;
};

// Per column type reader.
template<typename T>
class HdfsParquetScanner::ColumnReader : public HdfsParquetScanner::BaseColumnReader {
 public:
  ColumnReader(HdfsParquetScanner* parent, const SlotDescriptor* desc, int file_idx)
    : BaseColumnReader(parent, desc, file_idx) {
    DCHECK_NE(desc->type(), TYPE_BOOLEAN);
  }

 protected:
  virtual void CreateDictionaryDecoder(uint8_t* values, int size) {
    dict_decoder_.reset(new DictDecoder<T>(values, size));
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
    return Status::OK;
  }

  virtual bool ReadSlot(void* slot, MemPool* pool)  {
    parquet::Encoding::type page_encoding =
        current_page_header_.data_page_header.encoding;
    bool result;
    if (page_encoding == parquet::Encoding::PLAIN_DICTIONARY) {
      result = dict_decoder_->GetValue(reinterpret_cast<T*>(slot));
    } else {
      DCHECK(page_encoding == parquet::Encoding::PLAIN);
      data_ += ParquetPlainEncoder::Decode<T>(data_, reinterpret_cast<T*>(slot));
      if (stream_->compact_data()) CopySlot(reinterpret_cast<T*>(slot), pool);
      return true;
    }
    return result;
  }

 private:
  void CopySlot(T* slot, MemPool* pool) {
    // no-op for non-string columns.
  }

  scoped_ptr<DictDecoder<T> > dict_decoder_;
};

template<>
void HdfsParquetScanner::ColumnReader<StringValue>::CopySlot(
    StringValue* slot, MemPool* pool) {
  if (slot->len == 0) return;
  uint8_t* buffer = pool->Allocate(slot->len);
  memcpy(buffer, slot->ptr, slot->len);
  slot->ptr = reinterpret_cast<char*>(buffer);
}

class HdfsParquetScanner::BoolColumnReader : public HdfsParquetScanner::BaseColumnReader {
 public:
  BoolColumnReader(HdfsParquetScanner* parent, const SlotDescriptor* desc, int file_idx)
    : BaseColumnReader(parent, desc, file_idx) {
    DCHECK_EQ(desc->type(), TYPE_BOOLEAN);
  }

 protected:
  virtual void CreateDictionaryDecoder(uint8_t* values, int size) {
    DCHECK(false) << "Dictionary encoding is not supported for bools. Should never "
                  << "have gotten this far.";
  }

  virtual Status InitDataPage(uint8_t* data, int size) {
    // Initialize bool decoder
    bool_values_ = BitReader(data, size);
    return Status::OK;
  }

  virtual bool ReadSlot(void* slot, MemPool* pool)  {
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
  decompress_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "DecompressionTime");
  num_cols_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumColumns", TCounterType::UNIT);

  scan_node_->IncNumScannersCodegenDisabled();
  return Status::OK;
}

void HdfsParquetScanner::Close() {
  vector<THdfsCompression::type> compression_types;
  for (int i = 0; i < column_readers_.size(); ++i) {
    if (column_readers_[i]->decompressed_data_pool_.get() != NULL) {
      AttachPool(column_readers_[i]->decompressed_data_pool_.get());
    }
    column_readers_[i]->Close();
    compression_types.push_back(column_readers_[i]->codec());
  }
  AttachPool(dictionary_pool_.get());
  AddFinalRowBatch();
  context_->Close();

  // If this was a metadata only read (i.e. count(*)), there are no columns.
  if (compression_types.empty()) compression_types.push_back(THdfsCompression::NONE);
  scan_node_->RangeComplete(THdfsFileFormat::PARQUET, compression_types);
  assemble_rows_timer_.Stop();
  assemble_rows_timer_.ReleaseCounter();

  HdfsScanner::Close();
}

HdfsParquetScanner::BaseColumnReader* HdfsParquetScanner::CreateReader(
    SlotDescriptor* desc, int file_idx) {
  BaseColumnReader* reader = NULL;
  switch (desc->type()) {
    case TYPE_BOOLEAN:
      reader = new BoolColumnReader(this, desc, file_idx);
      break;
    case TYPE_TINYINT:
      reader = new ColumnReader<int8_t>(this, desc, file_idx);
      break;
    case TYPE_SMALLINT:
      reader = new ColumnReader<int16_t>(this, desc, file_idx);
      break;
    case TYPE_INT:
      reader = new ColumnReader<int32_t>(this, desc, file_idx);
      break;
    case TYPE_BIGINT:
      reader = new ColumnReader<int64_t>(this, desc, file_idx);
      break;
    case TYPE_FLOAT:
      reader = new ColumnReader<float>(this, desc, file_idx);
      break;
    case TYPE_DOUBLE:
      reader = new ColumnReader<double>(this, desc, file_idx);
      break;
    case TYPE_TIMESTAMP:
      reader = new ColumnReader<TimestampValue>(this, desc, file_idx);
      break;
    case TYPE_STRING:
      reader = new ColumnReader<StringValue>(this, desc, file_idx);
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
  int num_bytes;

  // We're about to move to the next data page.  The previous data page is
  // now complete, pass along the memory allocated for it.
  parent_->AttachPool(decompressed_data_pool_.get());

  // Read the next data page, skipping page types we don't care about.
  // We break out of this loop on the non-error case (a data page was found or we read all
  // the pages).
  while (true) {
    DCHECK_EQ(num_buffered_values_, 0);
    if (num_values_ >= parent_->file_metadata_.num_rows) {
      // No more pages to read
      DCHECK_EQ(num_values_, parent_->file_metadata_.num_rows);
      break;
    }

    RETURN_IF_ERROR(stream_->GetBuffer(true, &buffer, &num_bytes));
    if (num_bytes == 0) {
      DCHECK(stream_->eosr());
      stringstream ss;
      ss << "File metadata states there are " << parent_->file_metadata_.num_rows
         << " rows, but only read " << num_values_ << " values from column "
         << (desc_->col_pos() - parent_->scan_node_->num_partition_keys());
      if (parent_->scan_node_->runtime_state()->abort_on_error()) {
        return Status(ss.str());
      } else {
        parent_->scan_node_->runtime_state()->LogError(ss.str());
      }
    }

    // We don't know the actual header size until the thrift object is deserialized.
    uint32_t header_size = num_bytes;
    status = DeserializeThriftMsg(buffer, &header_size, true, &current_page_header_);
    if (!status.ok()) {
      if (header_size >= MAX_PAGE_HEADER_SIZE) {
        status.AddErrorMsg("ParquetScanner: Could not deserialize page header.");
        return status;
      }
      // Stitch the header bytes that are split across buffers.
      uint8_t header_buffer[MAX_PAGE_HEADER_SIZE];
      int header_first_part = header_size;
      memcpy(header_buffer, buffer, header_first_part);

      if (!stream_->SkipBytes(header_first_part, &status)) return status;
      RETURN_IF_ERROR(stream_->GetBuffer(true, &buffer, &num_bytes));
      if (num_bytes == 0) return status;

      uint32_t header_second_part =
          ::min(num_bytes, MAX_PAGE_HEADER_SIZE - header_first_part);
      memcpy(header_buffer + header_first_part, buffer, header_second_part);
      header_size = MAX_PAGE_HEADER_SIZE;
      status =
          DeserializeThriftMsg(header_buffer, &header_size, true, &current_page_header_);

      if (!status.ok()) {
        status.AddErrorMsg("ParquetScanner: Could not deserialize page header");
        return status;
      }
      DCHECK_GT(header_size, header_first_part);
      header_size = header_size - header_first_part;
    }
    if (!stream_->SkipBytes(header_size, &status)) return status;

    int data_size = current_page_header_.compressed_page_size;
    int uncompressed_size = current_page_header_.uncompressed_page_size;

    if (current_page_header_.type == parquet::PageType::DICTIONARY_PAGE) {
      if (dict_decoder_base_ != NULL) {
        return Status("Column chunk should not contain two dictionary pages.");
      }
      if (desc_->type() == TYPE_BOOLEAN) {
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
        RETURN_IF_ERROR(decompressor_->ProcessBlock(true, data_size, data_,
            &uncompressed_size, &dict_values));
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
        stringstream ss;
        ss << "Invalid dictionary. Expected " << dict_header->num_values
           << " entries but data contained " << dict_decoder_base_->num_entries()
           << " entries.";
        return Status(ss.str());
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
    num_values_ += num_buffered_values_;

    if (decompressor_.get() != NULL) {
      SCOPED_TIMER(parent_->decompress_timer_);
      uint8_t* decompressed_buffer = decompressed_data_pool_->Allocate(uncompressed_size);
      RETURN_IF_ERROR(decompressor_->ProcessBlock(
          true, current_page_header_.compressed_page_size, data_,
          &uncompressed_size, &decompressed_buffer));
      DCHECK_EQ(current_page_header_.uncompressed_page_size, uncompressed_size);
      data_ = decompressed_buffer;
      data_size = current_page_header_.uncompressed_page_size;
    } else {
      DCHECK_EQ(metadata_->codec, parquet::CompressionCodec::UNCOMPRESSED);
      DCHECK_EQ(current_page_header_.compressed_page_size, uncompressed_size);
    }

    if (field_repetition_type_ == parquet::FieldRepetitionType::OPTIONAL) {
      // Initialize the definition level data
      int32_t num_definition_bytes = 0;
      switch (current_page_header_.data_page_header.definition_level_encoding) {
        case parquet::Encoding::RLE:
          if (!ReadWriteUtil::Read(&data_, &data_size, &num_definition_bytes, &status)) {
            return status;
          }
          rle_def_levels_ = RleDecoder(data_, num_definition_bytes, 1);
          break;
        case parquet::Encoding::BIT_PACKED:
          num_definition_bytes = BitUtil::Ceil(num_buffered_values_, 8);
          bit_packed_def_levels_ = BitReader(data_, num_definition_bytes);
          break;
        default: {
          stringstream ss;
          ss << "Unsupported definition level encoding: "
            << current_page_header_.data_page_header.definition_level_encoding;
          return Status(ss.str());
        }
      }
      DCHECK_GT(num_definition_bytes, 0);
      data_ += num_definition_bytes;
      data_size -= num_definition_bytes;
    }

    // Data can be empty if the column contains all NULLs
    if (data_size != 0) RETURN_IF_ERROR(InitDataPage(data_, data_size));
    break;
  }

  return Status::OK;
}

// TODO More codegen here as well.
inline int HdfsParquetScanner::BaseColumnReader::ReadDefinitionLevel() {
  if (field_repetition_type_ == parquet::FieldRepetitionType::REQUIRED) {
    // This column is required so there is nothing encoded for the definition
    // levels.
    return 1;
  }

  uint8_t definition_level;
  bool valid = false;
  switch (current_page_header_.data_page_header.definition_level_encoding) {
    case parquet::Encoding::RLE:
      valid = rle_def_levels_.Get(&definition_level);
      break;
    case parquet::Encoding::BIT_PACKED: {
      valid = bit_packed_def_levels_.GetValue(1, &definition_level);
      break;
    }
    default:
      DCHECK(false);
  }
  if (!valid) return -1;
  return definition_level;
}

inline bool HdfsParquetScanner::BaseColumnReader::ReadValue(MemPool* pool, Tuple* tuple) {
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

  if (definition_level == 0) {
    // Null value
    tuple->SetNull(desc_->null_indicator_offset());
    return true;
  }

  DCHECK_EQ(definition_level, 1);
  return ReadSlot(tuple->GetSlot(desc_->tuple_offset()), pool);
}

Status HdfsParquetScanner::ProcessSplit() {
  HdfsFileDesc* file_desc = scan_node_->GetFileDesc(stream_->filename());
  DCHECK(file_desc != NULL);

  // First process the file metadata in the footer
  bool eosr;
  RETURN_IF_ERROR(ProcessFooter(&eosr));

  if (eosr) return Status::OK;

  // We've processed the metadata and there are columns that need to be
  // materialized.
  COUNTER_SET(num_cols_counter_, static_cast<int64_t>(column_readers_.size()));
  RETURN_IF_ERROR(CreateColumnReaders());

  // Iterate through each row group in the file and read all the materialized columns
  // per row group.  Row groups are independent, so this this could be parallelized.
  // However, having multiple row groups per file should be seen as an edge case and
  // we can do better parallelizing across files instead.
  for (int i = 0; i < file_metadata_.row_groups.size(); ++i) {
    // Close the streams before starting a new row group. These streams could either be
    // just the footer stream or streams for the previous row group.
    context_->CloseStreams();

    RETURN_IF_ERROR(InitColumns(i));
    RETURN_IF_ERROR(AssembleRows());
  }

  return Status::OK;
}

// TODO: this needs to be codegen'd.  The ReadValue function needs to be codegen'd,
// specific to type and encoding and then inlined into AssembleRows().
Status HdfsParquetScanner::AssembleRows() {
  assemble_rows_timer_.Start();
  while (!scan_node_->ReachedLimit() && !context_->cancelled()) {
    MemPool* pool;
    Tuple* tuple;
    TupleRow* row;
    int num_rows = GetMemory(&pool, &tuple, &row);
    int num_to_commit = 0;

    for (int i = 0; i < num_rows; ++i) {
      InitTuple(template_tuple_, tuple);
      for (int c = 0; c < column_readers_.size(); ++c) {
        if (!column_readers_[c]->ReadValue(pool, tuple)) {
          assemble_rows_timer_.Stop();
          // This column is complete and has no more data.  This indicates
          // we are done with this row group.
          // For correctly formed files, this should be the first column we
          // are reading.
          DCHECK(c == 0 || !parse_status_.ok()) << "c=" << c << " "
              << parse_status_.GetErrorMsg();;
          COUNTER_UPDATE(scan_node_->rows_read_counter(), i);
          RETURN_IF_ERROR(CommitRows(num_to_commit));
          return parse_status_;
        }
      }

      row->SetTuple(scan_node_->tuple_idx(), tuple);
      if (ExecNode::EvalConjuncts(&(*conjuncts_)[0], num_conjuncts_, row)) {
        row = next_row(row);
        tuple = next_tuple(tuple);
        ++num_to_commit;
      }
    }
    COUNTER_UPDATE(scan_node_->rows_read_counter(), num_rows);
    RETURN_IF_ERROR(CommitRows(num_to_commit));
  }

  assemble_rows_timer_.Stop();
  return parse_status_;
}

Status HdfsParquetScanner::ProcessFooter(bool* eosr) {
  *eosr = false;

  // Need to loop in case we need to read more bytes.
  while (true) {
    uint8_t* buffer;
    int len;

    RETURN_IF_ERROR(stream_->GetBuffer(false, &buffer, &len));
    DCHECK(stream_->eosr());

    // Number of bytes in buffer after the fixed size footer is accounted for.
    int remaining_bytes_buffered = len - sizeof(int32_t) - sizeof(PARQUET_VERSION_NUMBER);

    // Make sure footer has enough bytes to contain the required information.
    if (remaining_bytes_buffered < 0) {
      stringstream ss;
      ss << "File " << stream_->filename() << " is invalid.  Missing metadata.";
      return Status(ss.str());
    }

    // Validate magic file bytes are correct
    uint8_t* magic_number_ptr = buffer + len - sizeof(PARQUET_VERSION_NUMBER);
    if (memcmp(magic_number_ptr,
        PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER) != 0)) {
      stringstream ss;
      ss << "File " << stream_->filename() << " is invalid.  Invalid file footer: "
         << string((char*)magic_number_ptr, sizeof(PARQUET_VERSION_NUMBER));
      return Status(ss.str());
    }

    // The size of the metadata is encoded as a 4 byte little endian value before
    // the magic number
    uint8_t* metadata_size_ptr = magic_number_ptr - sizeof(int32_t);
    uint32_t metadata_size = *reinterpret_cast<uint32_t*>(metadata_size_ptr);

    // TODO: we need to read more from the file, the footer size guess was wrong.
    DCHECK_LE(metadata_size, remaining_bytes_buffered);

    // Deserialize file header
    uint8_t* metadata_ptr = metadata_size_ptr - metadata_size;
    Status status =
        DeserializeThriftMsg(metadata_ptr, &metadata_size, true, &file_metadata_);
    if (!status.ok()) {
      stringstream ss;
      ss << "File " << stream_->filename() << " has invalid file metadata at file offset "
         << (metadata_size + sizeof(PARQUET_VERSION_NUMBER) + sizeof(uint32_t)) << ". "
         << "Error = " << status.GetErrorMsg();
      return Status(ss.str());
    }

    metadata_range_ = stream_->scan_range();
    RETURN_IF_ERROR(ValidateFileMetadata());

    // Tell the scan node this file has been taken care of.
    HdfsFileDesc* desc = scan_node_->GetFileDesc(stream_->filename());
    scan_node_->MarkFileDescIssued(desc);

    if (scan_node_->materialized_slots().empty()) {
      // No materialized columns.  We can serve this query from just the metadata.  We
      // don't need to read the column data.
      int64_t num_tuples = file_metadata_.num_rows;
      COUNTER_UPDATE(scan_node_->rows_read_counter(), num_tuples);

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
      return Status::OK;
    } else if (file_metadata_.num_rows == 0) {
      // Empty file
      *eosr = true;
      return Status::OK;
    }

    if (file_metadata_.row_groups.empty()) {
      stringstream ss;
      ss << "Invalid file. This file: " << stream_->filename() << " has no row groups";
      return Status(ss.str());
    }
    break;
  }
  return Status::OK;
}

Status HdfsParquetScanner::CreateColumnReaders() {
  DCHECK(column_readers_.empty());
  int num_partition_keys = scan_node_->num_partition_keys();

  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = scan_node_->materialized_slots()[i];
    int col_idx = slot_desc->col_pos() - num_partition_keys;
    if (col_idx >= file_metadata_.schema[0].num_children) {
      // In this case, we are selecting a column that is not in the file.
      // Update the template tuple to put a NULL in this slot.
      if (template_tuple_ == NULL) {
        template_tuple_ = scan_node_->InitEmptyTemplateTuple();
      }
      template_tuple_->SetNull(slot_desc->null_indicator_offset());
      continue;
    }

    column_readers_.push_back(CreateReader(slot_desc, col_idx));
  }
  return Status::OK;
}

Status HdfsParquetScanner::InitColumns(int row_group_idx) {
  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx];

  // All the scan ranges (one for each col).
  vector<DiskIoMgr::ScanRange*> col_ranges;

  for (int i = 0; i < column_readers_.size(); ++i) {
    const SlotDescriptor* slot_desc = column_readers_[i]->desc_;
    int file_col_idx = column_readers_[i]->file_idx();

    const parquet::SchemaElement& schema_element =
        file_metadata_.schema[file_col_idx + 1];
    const parquet::ColumnChunk& col_chunk = row_group.columns[file_col_idx];
    int64_t col_start = col_chunk.meta_data.data_page_offset;

    RETURN_IF_ERROR(ValidateColumn(slot_desc, file_col_idx));

    // If there is a dictionary page, the file format requires it to come before
    // any data pages.  We need to start reading the column from the data page.
    if (col_chunk.meta_data.__isset.dictionary_page_offset) {
      if (col_chunk.meta_data.dictionary_page_offset >= col_start) {
        stringstream ss;
        ss << "File " << stream_->filename() << ": metadata is corrupt. "
           << "Dictionary page (offset=" << col_chunk.meta_data.dictionary_page_offset
           << ") must come before any data pages (offset=" << col_start << ").";
        return Status(ss.str());
      }
      col_start = col_chunk.meta_data.dictionary_page_offset;
    }
    int64_t col_len = col_chunk.meta_data.total_compressed_size;
    if (file_version_.application == "parquet-mr" && file_version_.VersionLt(1, 2, 9)) {
      // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
      // dictionary page header size in total_compressed_size and total_uncompressed_size
      // (see IMPALA-694). We pad col_len to compensate.
      col_len += MAX_PAGE_HEADER_SIZE;
    }

    // TODO: this will need to change when we have co-located files and the columns
    // are different files.
    if (!col_chunk.file_path.empty()) {
      DCHECK_EQ(col_chunk.file_path, string(metadata_range_->file()));
    }

    DiskIoMgr::ScanRange* col_range = scan_node_->AllocateScanRange(
        metadata_range_->file(), col_len, col_start, file_col_idx,
        metadata_range_->disk_id());
    col_ranges.push_back(col_range);

    // Get the stream that will be used for this column
    ScannerContext::Stream* stream = context_->AddStream(col_range);
    DCHECK(stream != NULL);

    RETURN_IF_ERROR(column_readers_[i]->Reset(&schema_element,
        &col_chunk.meta_data, stream));

    if (scan_node_->materialized_slots()[i]->type() != TYPE_STRING ||
        col_chunk.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED) {
      // Non-string types are always compact.  Compressed columns don't reference data
      // in the io buffers after tuple materialization.  In both cases, we can set compact
      // to true and recycle buffers more promptly.
      stream->set_compact_data(true);
    }
  }
  DCHECK_EQ(col_ranges.size(), column_readers_.size());
  DCHECK_GE(scan_node_->materialized_slots().size(), column_readers_.size());

  // The super class stream is not longer valid/used.  It was used
  // just to parse the file header.
  stream_ = NULL;

  // Issue all the column chunks to the io mgr and have them scheduled immediately.
  // This means these ranges aren't returned via DiskIoMgr::GetNextRange and
  // instead are scheduled to be read immediately.
  RETURN_IF_ERROR(scan_node_->runtime_state()->io_mgr()->AddScanRanges(
      scan_node_->reader_context(), col_ranges, true));

  return Status::OK;
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
    // Ignore any trailing extra characters
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
  return Status::OK;
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

Status HdfsParquetScanner::ValidateColumn(const SlotDescriptor* slot_desc, int col_idx) {
  parquet::ColumnChunk& file_data = file_metadata_.row_groups[0].columns[col_idx];
  const parquet::SchemaElement& schema_element = file_metadata_.schema[col_idx + 1];

  // Check the encodings are supported
  vector<parquet::Encoding::type>& encodings = file_data.meta_data.encodings;
  for (int i = 0; i < encodings.size(); ++i) {
    if (!IsEncodingSupported(encodings[i])) {
      stringstream ss;
      ss << "File " << stream_->filename() << " uses an unsupported encoding: "
         << PrintEncoding(encodings[i]) << " for column " << schema_element.name;
      return Status(ss.str());
    }
  }

  // Check the compression is supported
  if (file_data.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED &&
      file_data.meta_data.codec != parquet::CompressionCodec::SNAPPY &&
      file_data.meta_data.codec != parquet::CompressionCodec::GZIP) {
    stringstream ss;
    ss << "File " << stream_->filename() << " uses an unsupported compression: "
        << file_data.meta_data.codec << " for column " << schema_element.name;
    return Status(ss.str());
  }

  // Check the type in the file is compatible with the catalog metadata.
  parquet::Type::type type = IMPALA_TO_PARQUET_TYPES[slot_desc->type()];
  if (type != file_data.meta_data.type) {
    stringstream ss;
    ss << "File " << stream_->filename() << " has an incompatible type with the"
       << " table schema for column " << schema_element.name << ".  Expected type: "
       << type << ".  Actual type: " << file_data.meta_data.type;
    return Status(ss.str());
  }

  // Check that the column is not nested
  const vector<string> schema_path = file_data.meta_data.path_in_schema;
  if (schema_path.size() != 1) {
    stringstream ss;
    ss << "File " << stream_->filename() << " contains a nested schema for column "
       << schema_element.name << ".  This is currently not supported.";
    return Status(ss.str());
  }

  // Check that this column is optional or required
  if (schema_element.repetition_type != parquet::FieldRepetitionType::OPTIONAL &&
      schema_element.repetition_type != parquet::FieldRepetitionType::REQUIRED) {
    stringstream ss;
    ss << "File " << stream_->filename() << " column " << schema_element.name
       << " contains an unsupported column repetition type: "
       << schema_element.repetition_type;
    return Status(ss.str());
  }

  return Status::OK;
}

