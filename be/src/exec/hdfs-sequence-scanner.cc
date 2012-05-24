// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
#include "runtime/runtime-state.h"
#include "exec/hdfs-sequence-scanner.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/row-batch.h"
#include "exec/text-converter.h"
#include "util/cpu-info.h"
#include "exec/hdfs-scan-node.h"
#include "exec/delimited-text-parser.h"
#include "exec/serde-utils.h"
#include "exec/buffered-byte-stream.h"
#include "util/decompress.h"
#include "exec/text-converter.inline.h"
#include <glog/logging.h>

using namespace std;
using namespace boost;
using namespace impala;

const char* const HdfsSequenceScanner::SEQFILE_KEY_CLASS_NAME =
  "org.apache.hadoop.io.BytesWritable";

const char* const HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.io.Text";

const uint8_t HdfsSequenceScanner::SEQFILE_VERSION_HEADER[4] = {'S', 'E', 'Q', 6};

const int HdfsSequenceScanner::SEQFILE_KEY_LENGTH = 4;

static const uint8_t SEQUENCE_FILE_RECORD_DELIMITER = 0xff;

HdfsSequenceScanner::HdfsSequenceScanner(HdfsScanNode* scan_node, RuntimeState* state,
                                         Tuple* template_tuple, MemPool* tuple_pool)
    : HdfsScanner(scan_node, state, template_tuple, tuple_pool),
      delimited_text_parser_(NULL),
      text_converter_(NULL),
      unparsed_data_buffer_pool_(new MemPool()),
      unparsed_data_buffer_(NULL),
      unparsed_data_buffer_size_(0),
      num_buffered_records_in_compressed_block_(0) {
  const HdfsTableDescriptor* hdfs_table =
    reinterpret_cast<const HdfsTableDescriptor*>(scan_node->tuple_desc()->table_desc());

  text_converter_.reset(new TextConverter(hdfs_table->escape_char(), tuple_pool_));

  delimited_text_parser_.reset(new DelimitedTextParser(scan_node, '\0',
      hdfs_table->field_delim(), hdfs_table->collection_delim(),
      hdfs_table->escape_char()));
  // use the parser to find bytes that are -1
  find_first_parser_.reset(new DelimitedTextParser(scan_node, 
      SEQUENCE_FILE_RECORD_DELIMITER));
}

HdfsSequenceScanner::~HdfsSequenceScanner() {
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      unparsed_data_buffer_pool_->peak_allocated_bytes());
}

Status HdfsSequenceScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  field_locations_.resize(state_->batch_size() * scan_node_->materialized_slots().size());

  return Status::OK;
}

Status HdfsSequenceScanner::InitCurrentScanRange(HdfsScanRange* scan_range,
                                                 Tuple* template_tuple,
                                                 ByteStream* byte_stream) {
  RETURN_IF_ERROR(
      HdfsScanner::InitCurrentScanRange(scan_range, template_tuple, byte_stream));
  end_of_scan_range_ = scan_range->length + scan_range->offset;
  unbuffered_byte_stream_ = byte_stream;

  // If the file is blocked-compressed then we don't want to double buffer
  // the compressed blocks.  In that case we read meta information in
  // filesystem block sizes (4KB) otherwise we read large chunks (1MB)
  // and pick meta data and data from that buffer.
  buffered_byte_stream_.reset(new BufferedByteStream(
      unbuffered_byte_stream_,
      is_blk_compressed_ ? FILE_BLOCK_SIZE : state_->file_buffer_size(),
      scan_node_));

  // Check the Location (file name) to see if we have changed files.
  // If this a new file then we need to read and process the header.
  if (previous_location_ != unbuffered_byte_stream_->GetLocation()) {
    RETURN_IF_ERROR(buffered_byte_stream_->Seek(0));
    RETURN_IF_ERROR(ReadFileHeader());
    if (is_blk_compressed_) {
      unparsed_data_buffer_size_ = state_->file_buffer_size();
    }
    previous_location_ = unbuffered_byte_stream_->GetLocation();
  }

  delimited_text_parser_->ParserReset();

  // Offset may not point to record boundary
  if (scan_range->offset != 0) {
    RETURN_IF_ERROR(unbuffered_byte_stream_->Seek(scan_range->offset));
    RETURN_IF_ERROR(find_first_parser_->FindSyncBlock(end_of_scan_range_,
        SYNC_HASH_SIZE, sync_, unbuffered_byte_stream_));
    buffered_byte_stream_->SeekToParent();
  }

  num_buffered_records_in_compressed_block_ = 0;

  return Status::OK;
}

inline Status HdfsSequenceScanner::GetRecordFromCompressedBlock(uint8_t** record_ptr,
                                                                int64_t* record_len,
                                                                bool* eosr) {
  if (num_buffered_records_in_compressed_block_ == 0) {
    int64_t position;
    RETURN_IF_ERROR(buffered_byte_stream_->GetPosition(&position));
    if (position >= end_of_scan_range_) {
      *eosr = true;
      return Status::OK;
    }
    RETURN_IF_ERROR(ReadCompressedBlock());
  }
  // Adjust next_record_ to move past the size of the length indicator.
  int size = SerDeUtils::GetVLong(next_record_in_compressed_block_, record_len);
  next_record_in_compressed_block_ += size;
  *record_ptr = next_record_in_compressed_block_;
  // Point to the length of the next record.
  next_record_in_compressed_block_ += *record_len;
  --num_buffered_records_in_compressed_block_;
  return Status::OK;
}

inline Status HdfsSequenceScanner::GetRecord(uint8_t** record_ptr,
                                             int64_t* record_len, bool* eosr) {
  int64_t position;
  RETURN_IF_ERROR(buffered_byte_stream_->GetPosition(&position));
  if (position >= end_of_scan_range_) {
    *eosr = true;
  }

  // If we are past the end of the range we must read to the next sync block.
  // TODO: We need better error returns from bytestream functions.
  bool sync;
  Status stat = ReadBlockHeader(&sync);
  if (!stat.ok()) {
    // Since we are past the end of the range then we might be at the end of the file.
    bool eof;
    RETURN_IF_ERROR(buffered_byte_stream_->Eof(&eof));

    if (!*eosr || !eof) {
      return stat;
    } else {
      return Status::OK;
    }
  }

  if (sync && *eosr) return Status::OK;
  *eosr = false;

  // We don't look at the keys, only the values.
  RETURN_IF_ERROR(
      SerDeUtils::SkipBytes(buffered_byte_stream_.get(), current_key_length_));

  if (is_compressed_) {
    int in_size = current_block_length_ - current_key_length_;
    RETURN_IF_ERROR(
        SerDeUtils::ReadBytes(buffered_byte_stream_.get(), in_size, &scratch_buf_));

    RETURN_IF_ERROR(decompressor_->ProcessBlock(in_size,
        &scratch_buf_[0], 0, &unparsed_data_buffer_));
    *record_ptr = unparsed_data_buffer_;
    // Read the length of the record.
    int size = SerDeUtils::GetVLong(*record_ptr, record_len);
    *record_ptr += size;
  } else {
    // Uncompressed records
    RETURN_IF_ERROR(SerDeUtils::ReadVLong(buffered_byte_stream_.get(), record_len));
    if (has_noncompact_strings_ || *record_len > unparsed_data_buffer_size_) {
      unparsed_data_buffer_ = unparsed_data_buffer_pool_->Allocate(*record_len);
      unparsed_data_buffer_size_ = *record_len;
    }
    RETURN_IF_ERROR(SerDeUtils::ReadBytes(buffered_byte_stream_.get(),
        *record_len, unparsed_data_buffer_));
    *record_ptr = unparsed_data_buffer_;
  }
  return Status::OK;
}

// Add rows to the row_batch until it is full or we run off the end of the scan range.
Status HdfsSequenceScanner::GetNext(RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;

  // We count the time here since there is too much overhead to do
  // this on each record.
  COUNTER_SCOPED_TIMER(scan_node_->parse_time_counter());

  // Read records from the sequence file and parse the data for each record into
  // columns.  These are added to the row_batch.  The loop continues until either
  // the row batch is full or we are off the end of the range.
  while (true) {
    // Current record to process and its length.
    uint8_t* record = NULL;
    int64_t record_len;
    // Get the next record and record length.
    // There are 3 cases:
    //  Block-compressed -- each block contains several records.
    //  Record-compressed -- like a regular record, but the data is compressed.
    //  Uncompressed.
    if (is_blk_compressed_) {
      RETURN_IF_ERROR(GetRecordFromCompressedBlock(&record, &record_len, eosr));
    } else {
      // Get the next compressed or uncompressed record.
      RETURN_IF_ERROR(GetRecord(&record, &record_len, eosr));
    }

    if (*eosr) break;

    // Parse the current record.
    if (scan_node_->materialized_slots().size() != 0) {
      char* col_start;
      uint8_t* record_start = record;
      int num_tuples = 0;
      int num_fields = 0;

      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(
          row_batch->capacity() - row_batch->num_rows(), record_len,
          reinterpret_cast<char**>(&record),
          &field_locations_, &num_tuples, &num_fields, &col_start));
      DCHECK(num_tuples == 1);

      if (num_fields != 0) {
        if (!WriteFields(row_batch, num_fields, &row_idx).ok()) {
          // Report all the fields that have errors.
          ++num_errors_in_file_;
          if (state_->LogHasSpace()) {
            state_->error_stream() << "file: "
                << buffered_byte_stream_->GetLocation() << endl;
            state_->error_stream() << "record: ";
            state_->error_stream()
                << string(reinterpret_cast<char*>(record_start), record_len);
            state_->LogErrorStream();
          }
          if (state_->abort_on_error()) {
            state_->ReportFileErrors(buffered_byte_stream_->GetLocation(), 1);
            return Status("Aborted HdfsSequenceScanner due to parse errors."
                          "View error log for details.");
          }
        }
      }
    } else {
      WriteEmptyTuples(row_batch, 1);
    }
    if (scan_node_->ReachedLimit() || row_batch->IsFull()) {
      row_batch->tuple_data_pool()->AcquireData(tuple_pool_, true);
      *eosr = false;
      break;
    }
  }
  if (has_noncompact_strings_) {
    // Pass the buffer data to the row_batch.
    // If we are at the end of a scan range then release the ownership
    row_batch->tuple_data_pool()->AcquireData(unparsed_data_buffer_pool_.get(), !*eosr);
  }
  return Status::OK;
}

// TODO: apply conjuncts as slots get materialized and skip to the end of the row
// if we determine it's not a match.
Status HdfsSequenceScanner::WriteFields(RowBatch* row_batch,
                                        int num_fields, int* row_idx) {
  // This has too much overhead to do it per-tuple
  // COUNTER_SCOPED_TIMER(scan_node_->tuple_write_timer());
  DCHECK_EQ(num_fields, scan_node_->materialized_slots().size());

  // Keep track of where lines begin as we write out fields for error reporting
  int next_line_offset = 0;

  // Initialize tuple_ from the partition key template tuple before writing the slots
  if (template_tuple_ != NULL) {
    memcpy(tuple_, template_tuple_, tuple_byte_size_);
  }

  // Loop through all the parsed_data and parse out the values to slots
  bool error_in_row = false;
  for (int n = 0; n < num_fields; ++n) {
    int need_escape = false;
    int len = field_locations_[n].len;
    if (len < 0) {
      len = -len;
      need_escape = true;
    }
    next_line_offset += (len + 1);

    const SlotDescriptor* desc = scan_node_->materialized_slots()[n];
    if (!text_converter_->WriteSlot(desc, tuple_,
        reinterpret_cast<char*>(field_locations_[n].start),
        len, !has_noncompact_strings_, need_escape)) {
      ReportColumnParseError(desc, field_locations_[n].start, len);
      error_in_row = true;
    }
  }

  DCHECK_EQ(num_fields, scan_node_->materialized_slots().size());

  // TODO: The code from here down is more or less common to all scanners. Move it.
  // We now have a complete row, with everything materialized
  DCHECK(!row_batch->IsFull());
  if (*row_idx == RowBatch::INVALID_ROW_INDEX) {
    *row_idx = row_batch->AddRow();
  }
  TupleRow* current_row = row_batch->GetRow(*row_idx);
  current_row->SetTuple(tuple_idx_, tuple_);

  // Evaluate the conjuncts and add the row to the batch
  bool conjuncts_true = scan_node_->EvalConjunctsForScanner(current_row);

  if (conjuncts_true) {
    row_batch->CommitLastRow();
    *row_idx = RowBatch::INVALID_ROW_INDEX;
    scan_node_->IncrNumRowsReturned();
    if (scan_node_->ReachedLimit() || row_batch->IsFull()) {
      tuple_ = NULL;
      return Status::OK;
    }
    uint8_t* new_tuple = reinterpret_cast<uint8_t*>(tuple_);
    new_tuple += tuple_byte_size_;
    tuple_ = reinterpret_cast<Tuple*>(new_tuple);
  }

  // Need to reset the tuple_ if
  //  1. eval failed (clear out null-indicator bits) OR
  //  2. there are partition keys that need to be copied
  // TODO: if the slots that need to be updated are very sparse (very few NULL slots
  // or very few partition keys), updating all the tuple memory is probably bad
  if (!conjuncts_true || template_tuple_ != NULL) {
    if (template_tuple_ != NULL) {
      memcpy(tuple_, template_tuple_, tuple_byte_size_);
    } else {
      tuple_->Init(tuple_byte_size_);
    }
  }

  if (error_in_row) return Status("Conversion from string failed");
  return Status::OK;
}

Status HdfsSequenceScanner::ReadFileHeader() {
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(buffered_byte_stream_.get(),
      sizeof(SEQFILE_VERSION_HEADER), &scratch_buf_));
  if (memcmp(&scratch_buf_[0], SEQFILE_VERSION_HEADER, sizeof(SEQFILE_VERSION_HEADER))) {
    if (state_->LogHasSpace()) {
      state_->error_stream() << "Invalid SEQFILE_VERSION_HEADER: '"
         << SerDeUtils::HexDump(&scratch_buf_[0], sizeof(SEQFILE_VERSION_HEADER)) << "'";
    }
    return Status("Invalid SEQFILE_VERSION_HEADER");
  }

  std::vector<char> scratch_text;
  RETURN_IF_ERROR(SerDeUtils::ReadText(buffered_byte_stream_.get(), &scratch_text));
  if (strncmp(&scratch_text[0],
      HdfsSequenceScanner::SEQFILE_KEY_CLASS_NAME, scratch_text.size())) {
    if (state_->LogHasSpace()) {
      state_->error_stream() << "Invalid SEQFILE_KEY_CLASS_NAME: '"
         << string(&scratch_text[0], strlen(HdfsSequenceScanner::SEQFILE_KEY_CLASS_NAME))
         << "'";
    }
    return Status("Invalid SEQFILE_KEY_CLASS_NAME");
  }

  RETURN_IF_ERROR(SerDeUtils::ReadText(buffered_byte_stream_.get(), &scratch_text));
  if (strncmp(&scratch_text[0], HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME,
      scratch_text.size())) {
    if (state_->LogHasSpace()) {
      state_->error_stream() << "Invalid SEQFILE_VALUE_CLASS_NAME: '"
         << string(
             scratch_text[0], strlen(HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME))
         << "'";
    }
    return Status("Invalid SEQFILE_VALUE_CLASS_NAME");
  }

  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(buffered_byte_stream_.get(), &is_compressed_));
  RETURN_IF_ERROR(
      SerDeUtils::ReadBoolean(buffered_byte_stream_.get(), &is_blk_compressed_));

  vector<char> codec;
  if (is_compressed_) {
    vector<char> codec;
    // For record-comrpessed data we always want to copy since they tend to be
    // small and occupy a bigger mempool chunk.
    if (!is_blk_compressed_) {
      has_noncompact_strings_ = false;
    }
    RETURN_IF_ERROR(SerDeUtils::ReadText(buffered_byte_stream_.get(), &codec));
    RETURN_IF_ERROR(Decompressor::CreateDecompressor(state_,
        unparsed_data_buffer_pool_.get(),
        !has_noncompact_strings_, codec, &decompressor_));
  }
  VLOG(1) << unbuffered_byte_stream_->GetLocation() << ": "
      << (is_compressed_ ?
      (is_blk_compressed_ ?  "block compressed" : "record compresed") : "not compressed");
  if (is_compressed_) VLOG(1) << string(&codec[0], codec.size());

  RETURN_IF_ERROR(ReadFileHeaderMetadata());
  RETURN_IF_ERROR(ReadSync());
  return Status::OK;
}

Status HdfsSequenceScanner::ReadFileHeaderMetadata() {
  int map_size = 0;
  RETURN_IF_ERROR(SerDeUtils::ReadInt(buffered_byte_stream_.get(), &map_size));

  for (int i = 0; i < map_size; ++i) {
    RETURN_IF_ERROR(SerDeUtils::SkipText(buffered_byte_stream_.get()));
    RETURN_IF_ERROR(SerDeUtils::SkipText(buffered_byte_stream_.get()));

  }
  return Status::OK;
}

Status HdfsSequenceScanner::ReadSync() {
  RETURN_IF_ERROR(
      SerDeUtils::ReadBytes(buffered_byte_stream_.get(), SYNC_HASH_SIZE, sync_));
  return Status::OK;
}

Status HdfsSequenceScanner::ReadBlockHeader(bool* sync) {
  RETURN_IF_ERROR(
      SerDeUtils::ReadInt(buffered_byte_stream_.get(), &current_block_length_));
  *sync = false;
  if (current_block_length_ == HdfsSequenceScanner::SYNC_MARKER) {
    RETURN_IF_ERROR(CheckSync());
    RETURN_IF_ERROR(
        SerDeUtils::ReadInt(buffered_byte_stream_.get(), &current_block_length_));
    *sync = true;
  }
  RETURN_IF_ERROR(SerDeUtils::ReadInt(buffered_byte_stream_.get(), &current_key_length_));
  DCHECK_EQ(current_key_length_, SEQFILE_KEY_LENGTH);
  return Status::OK;
}

Status HdfsSequenceScanner::CheckSync() {
  uint8_t hash[SYNC_HASH_SIZE];
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(buffered_byte_stream_.get(),
      HdfsSequenceScanner::SYNC_HASH_SIZE, hash));

  bool sync_compares_equal = memcmp(static_cast<void*>(hash),
      static_cast<void*>(sync_), HdfsSequenceScanner::SYNC_HASH_SIZE) == 0;
  if (!sync_compares_equal) {
    if (state_->LogHasSpace()) {
      state_->error_stream() << "Bad sync hash in current HdfsSequenceScanner: "
           << buffered_byte_stream_->GetLocation() << "." << endl
           << "Expected: '"
           << SerDeUtils::HexDump(sync_, HdfsSequenceScanner::SYNC_HASH_SIZE)
           << "'" << endl
           << "Actual:   '"
           << SerDeUtils::HexDump(hash, HdfsSequenceScanner::SYNC_HASH_SIZE)
           << "'" << endl;
    }
    return Status("Bad sync hash");
  }
  return Status::OK;
}


Status HdfsSequenceScanner::ReadCompressedBlock() {
  // Read the sync indicator and check the sync block.
  RETURN_IF_ERROR(SerDeUtils::SkipBytes(buffered_byte_stream_.get(), sizeof (uint32_t)));
  RETURN_IF_ERROR(CheckSync());

  RETURN_IF_ERROR(SerDeUtils::ReadVLong(buffered_byte_stream_.get(),
      &num_buffered_records_in_compressed_block_));

  // Read the compressed key length and key buffers, we don't need them.
  RETURN_IF_ERROR(SerDeUtils::SkipText(buffered_byte_stream_.get()));
  RETURN_IF_ERROR(SerDeUtils::SkipText(buffered_byte_stream_.get()));

  // Read the compressed value length buffer. We don't need these either since the
  // records are in Text format with length included.
  RETURN_IF_ERROR(SerDeUtils::SkipText(buffered_byte_stream_.get()));

  // Read the compressed value buffer from the unbuffered stream.
  int block_size;
  RETURN_IF_ERROR(SerDeUtils::ReadVInt(buffered_byte_stream_.get(), &block_size));
  RETURN_IF_ERROR(buffered_byte_stream_->SyncParent());
  {
    COUNTER_SCOPED_TIMER(scan_node_->scanner_timer());
    RETURN_IF_ERROR(
        SerDeUtils::ReadBytes(unbuffered_byte_stream_, block_size, &scratch_buf_));
  }
  RETURN_IF_ERROR(buffered_byte_stream_->SeekToParent());

  RETURN_IF_ERROR(decompressor_->ProcessBlock(block_size, &scratch_buf_[0],
      0, &unparsed_data_buffer_));
  next_record_in_compressed_block_ = unparsed_data_buffer_;
  return Status::OK;
}
