// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
#include "runtime/runtime-state.h"
#include "exec/hdfs-sequence-scanner.h"
#include "runtime/tuple.h"
#include "runtime/row-batch.h"
#include "exec/text-converter.h"
#include "util/cpu-info.h"
#include "exec/hdfs-scan-node.h"
#include "exec/delimited-text-parser.h"
#include "exec/serde-utils.h"
#include "exec/buffered-byte-stream.h"
#include "exec/text-converter.inline.h"

// Compression libraries
#include <zlib.h>
#include <bzlib.h>
#include "snappy.h"

using namespace std;
using namespace boost;
using namespace impala;

const char* const HdfsSequenceScanner::SEQFILE_KEY_CLASS_NAME =
  "org.apache.hadoop.io.BytesWritable";

const char* const HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.io.Text";

const char* const HdfsSequenceScanner::SEQFILE_DEFAULT_COMPRESSION =
  "org.apache.hadoop.io.compress.DefaultCodec";

const char* const HdfsSequenceScanner::SEQFILE_GZIP_COMPRESSION =
  "org.apache.hadoop.io.compress.GzipCodec";

const char* const HdfsSequenceScanner::SEQFILE_BZIP2_COMPRESSION =
  "org.apache.hadoop.io.compress.BZip2Codec";

const char* const HdfsSequenceScanner::SEQFILE_SNAPPY_COMPRESSION =
  "org.apache.hadoop.io.compress.SnappyCodec";

const uint8_t HdfsSequenceScanner::SEQFILE_VERSION_HEADER[4] = {'S', 'E', 'Q', 6};

const int HdfsSequenceScanner::SEQFILE_KEY_LENGTH = 4;

// These are magic numbers from zlib.h.  Not clear why they are not defined there.
// 15 == window size, 32 == figure out if libz or gzip.
#define WINDOW_BITS 15
#define DETECT_CODEC 32

// Decompress a block encoded by gzip or lzip.
// Inputs:
//     input_length: length of input buffer.
//     in: input buffer, contains compressed data
//     output_length: length of output buffer.
// In/Out:
//     out: output buffer, place to put decompressed data.
// Output:
//     too_small: set to true if the output_length is too small.
static Status DecompressGzipBlock(int input_length, uint8_t* in,
                                  int output_length, uint8_t* out, bool* too_small) {
  z_stream stream;
  bzero(&stream, sizeof(stream));
  stream.next_in = reinterpret_cast<Bytef*>(in);
  stream.avail_in = input_length;
  stream.next_out = reinterpret_cast<Bytef*>(out);
  stream.avail_out = output_length;
 
  *too_small = false;
  int ret;
  // Initialize and run either zlib or gzib inflate. 
  if ((ret = inflateInit2(&stream, WINDOW_BITS | DETECT_CODEC)) != Z_OK) {
    stringstream ss;
    ss << "zlib inflateInit failed: " << stream.msg;
    return Status(ss.str());
  }
  if ((ret = inflate(&stream, 1)) != Z_STREAM_END) {
    (void)inflateEnd(&stream);
    if (ret == Z_OK) {
      *too_small = true;
      return Status::OK;
    }

    stringstream ss;
    ss << "zlib inflate failed: " << stream.msg;
    return Status(ss.str());
  }
  if (inflateEnd(&stream) != Z_OK) {
    stringstream ss;
    ss << "zlib inflateEnd failed: " << stream.msg;
    return Status(ss.str());
  }

  return Status::OK;
}

// Decompress a block encoded by bzip2.
// Inputs:
//     input_length: length of input buffer.
//     in: input buffer, contains compressed data
//     output_length: length of output buffer.
// In/Out:
//     out: output buffer, place to put decompressed data.
// Output:
//     too_small: set to true if the output_length is too small.
static Status DecompressBzip2Block(int input_length, uint8_t* in,
                                   int output_length, uint8_t* out, bool* too_small) {
  bz_stream stream;
  bzero(&stream, sizeof(stream));
  stream.next_in = reinterpret_cast<char*>(in);
  stream.avail_in = input_length;
  stream.next_out = reinterpret_cast<char*>(out);
  stream.avail_out = output_length;
 
  *too_small = false;
  int ret;
  if ((ret = BZ2_bzDecompressInit(&stream, 0, 0)) != BZ_OK) {
    stringstream ss;
    ss << "bzlib BZ2_bzDecompressInit failed: " << ret;
    return Status(ss.str());
  }
  if ((ret = BZ2_bzDecompress(&stream)) != BZ_STREAM_END) {
    (void)BZ2_bzDecompressEnd(&stream);
    if (ret == BZ_OK) {
      *too_small = true;
      return Status::OK;
    }
    stringstream ss;
    ss << "bzlib BZ2_bzDecompress failed: " << ret;
    return Status(ss.str());
  }
  if ((ret = BZ2_bzDecompressEnd(&stream)) != BZ_OK) {
    stringstream ss;
    ss << "bzlib BZ2_bzDecompressEnd failed: " << ret;
    return Status(ss.str());
  }

  return Status::OK;
}

// Decompress a block encoded by Snappy.
// Inputs:
//     input_length: length of input buffer.
//     in: input buffer, contains compressed data
//     output_length: length of output buffer.
// In/Out:
//     out: output buffer, place to put decompressed data.
// Output:
//     too_small: set to true if the output_length is too small.
static Status DecompressSnappyBlock(int input_length, uint8_t* in,
                                    int output_length, uint8_t* out, bool* too_small) {
  *too_small = false;

  // Hadoop uses a block compression scheme on top of snappy.  First there is
  // an integer which is the size of the decompressed data followed by a
  // sequence of compressed blocks each preceded with an integer size.
  int32_t len = SerDeUtils::GetInt(in);

  // TODO: Snappy knows how big the output is, we should just use that.
  if (output_length < len) {
    *too_small = true;
    return Status::OK;
  }
  in += sizeof(len);
  input_length -= sizeof(len);
 
  do {
    // Read the length of the next block.
    len = SerDeUtils::GetInt(in);

    if (len == 0) break;

    in += sizeof(len);
    input_length -= sizeof(len);

    // Read how big the output will be.
    size_t uncompressed_len;
    if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(in),
        input_length, &uncompressed_len)) {
      return Status("Snappy: GetUncompressedLength failed");
    }

    DCHECK_GT(output_length, 0);
    if (!snappy::RawUncompress(reinterpret_cast<char*>(in),
        static_cast<size_t>(len), reinterpret_cast<char*>(out))) {
      return Status("Snappy: RawUncompress failed");
    }
    in += len;
    input_length -= len;
    out += uncompressed_len;
    output_length -= uncompressed_len;
  } while (input_length > 0);

  return Status::OK;
}

HdfsSequenceScanner::HdfsSequenceScanner(HdfsScanNode* scan_node,
                                         const TupleDescriptor* tuple_desc,
                                         Tuple* template_tuple, MemPool* tuple_pool)
    : HdfsScanner(scan_node, tuple_desc, template_tuple, tuple_pool),
      delimited_text_parser_(NULL),
      text_converter_(NULL),
      unparsed_data_buffer_pool_(new MemPool()),
      unparsed_data_buffer_(NULL),
      unparsed_data_buffer_size_(0),
      num_buffered_records_in_compressed_block_(0) {
  const HdfsTableDescriptor* hdfs_table =
    reinterpret_cast<const HdfsTableDescriptor*>(tuple_desc->table_desc());
  
  text_converter_.reset(new TextConverter(hdfs_table->escape_char(), tuple_pool_));

  delimited_text_parser_.reset(new DelimitedTextParser(scan_node->column_to_slot_index(),
      scan_node->GetNumPartitionKeys(), NULL, '\0',
      hdfs_table->field_delim(), hdfs_table->collection_delim(),
      hdfs_table->escape_char()));
  // use the parser to find bytes that are -1
  find_first_parser_.reset(new DelimitedTextParser(scan_node->column_to_slot_index(),
      scan_node->GetNumPartitionKeys(), scan_node->parse_time_counter(),
      static_cast<char>(0xff)));
}

Status HdfsSequenceScanner::InitCurrentScanRange(RuntimeState* state,
                                                 HdfsScanRange* scan_range,
                                                 ByteStream* byte_stream) {
  HdfsScanner::InitCurrentScanRange(state, scan_range, byte_stream);
  end_of_scan_range_ = scan_range->length + scan_range->offset;
  unbuffered_byte_stream_ = byte_stream;

  // If the file is blocked-compressed then we don't want to double buffer
  // the compressed blocks.  In that case we read meta information in
  // filesystem block sizes (4KB) otherwise we read large chunks (1MB)
  // and pick meta data and data from that buffer.
  buffered_byte_stream_.reset(new BufferedByteStream(
      unbuffered_byte_stream_,
      is_blk_compressed_ ? FILE_BLOCK_SIZE : state->file_buffer_size(),
      scan_node_->scanner_timer()));

  // Check the Location (file name) to see if we have changed files.
  // If this a new file then we need to read and process the header.
  if (previous_location_ != unbuffered_byte_stream_->GetLocation()) {
    RETURN_IF_ERROR(buffered_byte_stream_->Seek(0));
    RETURN_IF_ERROR(ReadFileHeader());
    if (is_blk_compressed_) {
      unparsed_data_buffer_size_ = state->file_buffer_size();
    }
    previous_location_ = unbuffered_byte_stream_->GetLocation();
  }

  delimited_text_parser_->ParserReset();

  // Offset may not point to record boundary
  if (scan_range->offset != 0) {
    RETURN_IF_ERROR(unbuffered_byte_stream_->Seek(scan_range->offset));
    RETURN_IF_ERROR(FindFirstRecord(state));
  } 


  return Status::OK;
}

// The start of the sync block is specified by an integer of -1.  We search
// bytes till we find a -1 and then look for 3 more -1 bytes which will make up
// the integer.  This is followed by the 16 byte sync block which was specified in
// the file header.
Status HdfsSequenceScanner::FindFirstRecord(RuntimeState* state) {
  // A sync block is preceeded by 4 bytes of -1 (0xff).
  int sync_flag_counter = 0;
  // Starting offset of the buffer we are scanning
  int64_t buf_start = 0;
  // Number of bytes read from stream
  int64_t num_bytes_read = 0;
  // Current offset into buffer.
  int64_t off = 0;
  // Bytes left to process in buffer.
  int64_t bytes_left = 0;
  // Size of buffer to read.
  int64_t read_size = FILE_BLOCK_SIZE;
  // Buffer to scan.
  uint8_t buf[read_size];

  // Loop until we find a Sync block or get to the end of the range.
  while (buf_start + off < end_of_scan_range_ || sync_flag_counter != 0) {
    // If there are no bytes left to process in the buffer get some more.
    // We may make bytes_left < 0 while looping for 0xff bytes below.
    if (bytes_left <= 0) {
      if (buf_start == 0) {
        RETURN_IF_ERROR(unbuffered_byte_stream_->GetPosition(&buf_start));
      } else {
        // Seek to the next buffer, in case we read the byte stream below.
        buf_start += num_bytes_read;
#ifndef NDEBUG
        int64_t position;
        RETURN_IF_ERROR(unbuffered_byte_stream_->GetPosition(&position));
        DCHECK_EQ(buf_start, position);
#endif
      }
      // Do not read past the end of range, unless we stopped at a -1 byte.
      // This could be the start of a sync block and we must process the
      // following data.
      if (buf_start + read_size >= end_of_scan_range_) {
        read_size = (end_of_scan_range_ - buf_start);
        if (sync_flag_counter != 0 && read_size < 4 - sync_flag_counter) {
          read_size = 4 - sync_flag_counter;
        }
      }
      if (read_size == 0) {
        return Status::OK;
      }
      RETURN_IF_ERROR(unbuffered_byte_stream_->Read(buf, read_size, &num_bytes_read));
      off = 0;
      if (num_bytes_read == 0) {
        RETURN_IF_ERROR(buffered_byte_stream_->SeekToParent());
        return Status::OK;
      }
      bytes_left = num_bytes_read;
    }

    if (sync_flag_counter == 0) {
      off += find_first_parser_->FindFirstTupleStart(
          reinterpret_cast<char*>(buf) + off, bytes_left);
      bytes_left = num_bytes_read - off;

      if (bytes_left == 0) continue;

      sync_flag_counter = 1;
    }
    
    // We found a -1 see if there are 3 more
    while (bytes_left != 0) {
      --bytes_left;
      if (buf[off++] != static_cast<uint8_t>(0xff)) {
        sync_flag_counter = 0;
        break;
      }

      if (++sync_flag_counter == 4) {
        RETURN_IF_ERROR(buffered_byte_stream_->Seek(buf_start + off));
        bool verified;
        RETURN_IF_ERROR(CheckSync(false, &verified));
        if (verified) {
          // Seek back to the beginning of the sync so the protocol readers are right.
          RETURN_IF_ERROR(buffered_byte_stream_->Seek(buf_start + off - 4));
          return Status::OK;
        }
        sync_flag_counter = 0;
        break;
      }
    }
  }
  RETURN_IF_ERROR(buffered_byte_stream_->SeekToParent());
  return Status::OK;
    
}
  
Status HdfsSequenceScanner::Prepare(RuntimeState* state, ByteStream* byte_stream) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(state, byte_stream));

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  field_locations_.resize(state->batch_size() * scan_node_->materialized_slots().size());

  return Status::OK;
}

inline Status HdfsSequenceScanner::GetRecordFromCompressedBlock(RuntimeState* state,
                                                                uint8_t** record_ptr,
                                                                int64_t* record_len,
                                                                bool* eosr) {
  if (num_buffered_records_in_compressed_block_ == 0) {
    int64_t position;
    RETURN_IF_ERROR(buffered_byte_stream_->GetPosition(&position));
    if (position >= end_of_scan_range_) {
      *eosr = true;
      return Status::OK;
    }
    RETURN_IF_ERROR(ReadCompressedBlock(state));
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

  // Reading a compressed record, we don't know how big the output is.
  // If we are told our output buffer is too small, double it and try again.
  if (is_compressed_) {
    int in_size = current_block_length_ - current_key_length_;
    RETURN_IF_ERROR(
        SerDeUtils::ReadBytes(buffered_byte_stream_.get(), in_size, &scratch_buf_));

    int out_size = in_size;
    bool too_small = false;
    do {
      out_size *= 2;
      if (has_string_slots_ || unparsed_data_buffer_size_ < out_size) {
        unparsed_data_buffer_ = unparsed_data_buffer_pool_->Allocate(out_size);
        unparsed_data_buffer_size_ = out_size;
      }
      
      RETURN_IF_ERROR(decompress_block_function_(in_size, &scratch_buf_[0],
          out_size, unparsed_data_buffer_, &too_small));
    } while (too_small);

    *record_ptr = unparsed_data_buffer_;
    // Read the length of the record.
    int size = SerDeUtils::GetVLong(*record_ptr, record_len);
    *record_ptr += size;
  } else {
    // Uncompressed records
    RETURN_IF_ERROR(SerDeUtils::ReadVLong(buffered_byte_stream_.get(), record_len));
    if (has_string_slots_ || *record_len > unparsed_data_buffer_size_) {
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
Status HdfsSequenceScanner::GetNext(RuntimeState* state,
                                    RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;
  runtime_state_ = state;

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
    //  Block compressed -- each block contains several records.
    //  Record compressed -- like a regular record, but the data is compressed.
    //  Uncompressed.
    if (is_blk_compressed_) {
      RETURN_IF_ERROR(GetRecordFromCompressedBlock(state, &record, &record_len, eosr));
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
        if (!WriteFields(state, row_batch, num_fields, &row_idx).ok()) {
          // Report all the fields that have errors.
          ++num_errors_in_file_;
          if (state->LogHasSpace()) {
            state->error_stream() << "file: "
                << buffered_byte_stream_->GetLocation() << endl;
            state->error_stream() << "record: ";
            state->error_stream()
                << string(reinterpret_cast<char*>(record_start), record_len);
            state->LogErrorStream();
          }
          if (state->abort_on_error()) {
            state->ReportFileErrors(buffered_byte_stream_->GetLocation(), 1);
            return Status("Aborted HdfsSequenceScanner due to parse errors." 
                          "View error log for details.");
          }
        }
      }
    } else {
      RETURN_IF_ERROR(WriteTuples(state, row_batch, 1, &row_idx));
    }
    if (row_batch->IsFull()) {
      row_batch->tuple_data_pool()->AcquireData(tuple_pool_, true);
      *eosr = false;
      break;
    }
  }
  if (has_string_slots_) {
    // Pass the buffer data to the row_batch.
    // If we are at the end of a scan range then release the ownership
    row_batch->tuple_data_pool()->AcquireData(unparsed_data_buffer_pool_.get(), !*eosr);
  }
  return Status::OK;
}

// TODO: apply conjuncts as slots get materialized and skip to the end of the row
// if we determine it's not a match.
Status HdfsSequenceScanner::WriteFields(RuntimeState* state, RowBatch* row_batch,
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

    if (!text_converter_->WriteSlot(state, scan_node_->materialized_slots()[n].second,
        tuple_, reinterpret_cast<char*>(field_locations_[n].start),
        len, false, need_escape).ok()) {
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
    if (runtime_state_->LogHasSpace()) {
      runtime_state_->error_stream() << "Invalid SEQFILE_VERSION_HEADER: '"
         << SerDeUtils::HexDump(&scratch_buf_[0], sizeof(SEQFILE_VERSION_HEADER)) << "'";
    }
    return Status("Invalid SEQFILE_VERSION_HEADER");
  }

  std::vector<char> scratch_text;
  RETURN_IF_ERROR(SerDeUtils::ReadText(buffered_byte_stream_.get(), &scratch_text));
  if (strncmp(&scratch_text[0],
      HdfsSequenceScanner::SEQFILE_KEY_CLASS_NAME, scratch_text.size())) {
    if (runtime_state_->LogHasSpace()) {
      runtime_state_->error_stream() << "Invalid SEQFILE_KEY_CLASS_NAME: '"
         << string(&scratch_text[0], strlen(HdfsSequenceScanner::SEQFILE_KEY_CLASS_NAME))
         << "'";
    }
    return Status("Invalid SEQFILE_KEY_CLASS_NAME");
  }

  RETURN_IF_ERROR(SerDeUtils::ReadText(buffered_byte_stream_.get(), &scratch_text));
  if (strncmp(&scratch_text[0], HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME,
      scratch_text.size())) {
    if (runtime_state_->LogHasSpace()) {
      runtime_state_->error_stream() << "Invalid SEQFILE_VALUE_CLASS_NAME: '"
         << string(
             scratch_text[0], strlen(HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME))
         << "'";
    }
    return Status("Invalid SEQFILE_VALUE_CLASS_NAME");
  }

  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(buffered_byte_stream_.get(), &is_compressed_));
  RETURN_IF_ERROR(
      SerDeUtils::ReadBoolean(buffered_byte_stream_.get(), &is_blk_compressed_));

  if (is_compressed_) {
    RETURN_IF_ERROR(
        SerDeUtils::ReadText(buffered_byte_stream_.get(), &compression_codec_));
    RETURN_IF_ERROR(SetCompression());
  }
  
  RETURN_IF_ERROR(ReadFileHeaderMetadata());
  RETURN_IF_ERROR(ReadSync());
  return Status::OK;
}

Status HdfsSequenceScanner::SetCompression() {
  if (strncmp(&compression_codec_[0], HdfsSequenceScanner::SEQFILE_DEFAULT_COMPRESSION,
      compression_codec_.size()) == 0 ||
      strncmp(&compression_codec_[0], HdfsSequenceScanner::SEQFILE_GZIP_COMPRESSION,
      compression_codec_.size()) == 0) {
    decompress_block_function_ = DecompressGzipBlock;

  } else if (strncmp(&compression_codec_[0],
      HdfsSequenceScanner::SEQFILE_BZIP2_COMPRESSION, compression_codec_.size()) == 0) {
    decompress_block_function_ = DecompressBzip2Block;

  } else if (strncmp(&compression_codec_[0],
      HdfsSequenceScanner::SEQFILE_SNAPPY_COMPRESSION, compression_codec_.size()) == 0) {
    decompress_block_function_ = DecompressSnappyBlock;
  } else {
    if (runtime_state_->LogHasSpace()) {
      runtime_state_->error_stream() << "Unknown Codec: " 
         << string(&compression_codec_[0], compression_codec_.size());
    }
    return Status("Unknown Codec");
  }

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
    RETURN_IF_ERROR(CheckSync(true, NULL));
    RETURN_IF_ERROR(
        SerDeUtils::ReadInt(buffered_byte_stream_.get(), &current_block_length_));
    *sync = true;
  }
  RETURN_IF_ERROR(SerDeUtils::ReadInt(buffered_byte_stream_.get(), &current_key_length_));
  DCHECK_EQ(current_key_length_, SEQFILE_KEY_LENGTH); 
  return Status::OK;
}

Status HdfsSequenceScanner::CheckSync(bool report_error, bool* verified) {
  uint8_t hash[SYNC_HASH_SIZE];
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(buffered_byte_stream_.get(),
      HdfsSequenceScanner::SYNC_HASH_SIZE, hash));

  bool sync_compares_equal = memcmp(static_cast<void*>(hash),
      static_cast<void*>(sync_), HdfsSequenceScanner::SYNC_HASH_SIZE) == 0;
  if (report_error && !sync_compares_equal) {
    if (runtime_state_->LogHasSpace()) {
      runtime_state_->error_stream() << "Bad sync hash in current HdfsSequenceScanner: "
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
  if (verified != NULL) *verified = sync_compares_equal;
  return Status::OK;
}


Status HdfsSequenceScanner::ReadCompressedBlock(RuntimeState* state) {
  // Read the sync indicator and check the sync block.
  RETURN_IF_ERROR(SerDeUtils::SkipBytes(current_byte_stream_, sizeof (uint32_t)));
  RETURN_IF_ERROR(CheckSync(true, NULL));
  
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

  bool too_small = false;
  do {
    if (too_small || has_string_slots_ || unparsed_data_buffer_ == NULL) {
      unparsed_data_buffer_ =
          unparsed_data_buffer_pool_->Allocate(unparsed_data_buffer_size_);
    }

    RETURN_IF_ERROR(decompress_block_function_(block_size,
        &scratch_buf_[0], unparsed_data_buffer_size_, unparsed_data_buffer_, &too_small));

    if (too_small) {
      unparsed_data_buffer_size_ *= 2;
    }
  } while (too_small);

  next_record_in_compressed_block_ = unparsed_data_buffer_;
  return Status::OK;
}
