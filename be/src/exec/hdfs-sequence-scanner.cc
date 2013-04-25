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

#include "exec/hdfs-sequence-scanner.h"

#include "codegen/llvm-codegen.h"
#include "exec/delimited-text-parser.inline.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.inline.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/codec.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

const char* const HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME =
    "org.apache.hadoop.io.Text";

const uint8_t HdfsSequenceScanner::SEQFILE_VERSION_HEADER[4] = {'S', 'E', 'Q', 6};

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

HdfsSequenceScanner::HdfsSequenceScanner(HdfsScanNode* scan_node, RuntimeState* state) 
    : BaseSequenceScanner(scan_node, state, /* marker_precedes_sync */ true),
      unparsed_data_buffer_(NULL),
      num_buffered_records_in_compressed_block_(0) {
}

HdfsSequenceScanner::~HdfsSequenceScanner() {
}

// Codegen for materialized parsed data into tuples.  
// TODO: sequence file scanner needs to be split into a cross compiled ir file,
// probably just for the block compressed path.  WriteCompleteTuple should be 
// injected into that function.
Function* HdfsSequenceScanner::Codegen(HdfsScanNode* node,
    const vector<Expr*>& conjuncts) {
  LlvmCodeGen* codegen = node->runtime_state()->llvm_codegen();
  if (codegen == NULL) return NULL;
  Function* write_complete_tuple_fn = CodegenWriteCompleteTuple(node, codegen, conjuncts);
  if (write_complete_tuple_fn == NULL) return NULL;
  return CodegenWriteAlignedTuples(node, codegen, write_complete_tuple_fn);
}

Status HdfsSequenceScanner::InitNewRange() {
  DCHECK(header_ != NULL);
  only_parsing_header_ = false;

  HdfsPartitionDescriptor* hdfs_partition = context_->partition_descriptor();
  
  text_converter_.reset(new TextConverter(hdfs_partition->escape_char()));
  
  delimited_text_parser_.reset(new DelimitedTextParser(scan_node_, '\0',
      hdfs_partition->field_delim(), hdfs_partition->collection_delim(),
      hdfs_partition->escape_char()));
  
  num_buffered_records_in_compressed_block_ = 0;

  SeqFileHeader* seq_header = reinterpret_cast<SeqFileHeader*>(header_);
  if (seq_header->is_compressed) {
    // For record-compressed data we always want to copy since they tend to be
    // small and occupy a bigger mempool chunk.
    if (seq_header->is_row_compressed) stream_->set_compact_data(true);
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
        data_buffer_pool_.get(), stream_->compact_data(),
        header_->codec, &decompressor_));
  }
  
  // Initialize codegen fn
  RETURN_IF_ERROR(InitializeCodegenFn(hdfs_partition, 
      THdfsFileFormat::SEQUENCE_FILE, "HdfsSequenceScanner"));
  return Status::OK;
}

Status HdfsSequenceScanner::Prepare() {
  RETURN_IF_ERROR(BaseSequenceScanner::Prepare());

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  record_locations_.resize(state_->batch_size());
  field_locations_.resize(state_->batch_size() * scan_node_->materialized_slots().size());
  return Status::OK;
}

BaseSequenceScanner::FileHeader* HdfsSequenceScanner::AllocateFileHeader() {
  return new SeqFileHeader;
}
  
inline Status HdfsSequenceScanner::GetRecord(uint8_t** record_ptr,
                                             int64_t* record_len, bool *eosr) {
  // There are 2 cases:
  //  Record-compressed -- like a regular record, but the data is compressed.
  //  Uncompressed.
    
  block_start_ = stream_->file_offset();
  bool sync;
  *eosr = stream_->eosr();
  Status stat = ReadBlockHeader(&sync);
  if (!stat.ok()) {
    *record_ptr = NULL;
    if (*eosr) return Status::OK;
    return stat;
  }

  // If we read a sync mark and are past the end of the scan range we are done.
  if (sync && *eosr) {
    *record_ptr = NULL;
    return Status::OK;
  }

  // If we have not read the end the next sync mark keep going.
  *eosr = false;

  // We don't look at the keys, only the values.
  RETURN_IF_FALSE(stream_->SkipBytes(current_key_length_, &parse_status_));

  if (header_->is_compressed) {
    int in_size = current_block_length_ - current_key_length_;
    // Check for a reasonable size
    if (in_size > stream_->scan_range()->len() || in_size < 0) {
      stringstream ss;
      ss << "Compressed record size is: " << in_size;
      if (state_->LogHasSpace()) state_->LogError(ss.str());
      return Status(ss.str());
    }
    uint8_t* compressed_data;
    RETURN_IF_FALSE(
        stream_->ReadBytes(in_size, &compressed_data, &parse_status_));

    int len = 0;
    RETURN_IF_ERROR(decompressor_->ProcessBlock(in_size, compressed_data,
        &len, &unparsed_data_buffer_));
    *record_ptr = unparsed_data_buffer_;
    // Read the length of the record.
    int size = ReadWriteUtil::GetVLong(*record_ptr, record_len);
    if (size == -1) {
        stringstream ss;
        ss << "Invalid record size";
        if (state_->LogHasSpace()) state_->LogError(ss.str());
        return Status(ss.str());
    }
    *record_ptr += size;
  } else {
    // Uncompressed records
    RETURN_IF_FALSE(stream_->ReadVLong(record_len, &parse_status_));
    if (*record_len > stream_->scan_range()->len() || *record_len < 0) {
      stringstream ss;
      ss << "Record length is: " << record_len;
      if (state_->LogHasSpace()) state_->LogError(ss.str());
      return Status(ss.str());
    }
    RETURN_IF_FALSE(
        stream_->ReadBytes(*record_len, record_ptr, &parse_status_));
  }
  return Status::OK;
}

// Process block compressed sequence files.  This is the most used sequence file
// format.  The general strategy is to process the data in large chunks to minimize
// function calls.  The process is:
// 1. Decompress an entire block
// 2. In row batch sizes:
//   a. Collect the start of records and their lengths
//   b. Parse cols locations to field_locations_
//   c. Materialize those field locations to row batches
// This mimics the technique for text.
// This function only returns on error or when the entire scan range is complete.
Status HdfsSequenceScanner::ProcessBlockCompressedScanRange() {
  DCHECK(header_->is_compressed);

  while (!stream_->eosr() || num_buffered_records_in_compressed_block_ > 0) {
    if (scan_node_->ReachedLimit()) return Status::OK;
    if (context_->cancelled()) return Status::CANCELLED;

    if (num_buffered_records_in_compressed_block_ == 0) {
      if (stream_->eosr()) return Status::OK;
      // No more decompressed data, decompress the next block
      RETURN_IF_ERROR(ReadCompressedBlock());
      if (num_buffered_records_in_compressed_block_ < 0) return parse_status_;
    }

    // Step 2
    RETURN_IF_ERROR(ProcessDecompressedBlock());

    // If we're finished processing the current block, read the marker + sync of
    // the next block. We don't do this at the beginning of the loop since
    // BaseSequenceScanner starts us after a sync, meaning we start by reading
    // data.
    if (num_buffered_records_in_compressed_block_ == 0) {
      // If we are exactly at the end of the scan range, we don't attempt to
      // read a sync marker because we may be at the end of file, and we don't
      // want to trigger an "incomplete read" parse status and abort the
      // query. This is safe to do even if we're not at the end of the file
      // since we'll be finished after reading this sync.
      // TODO: check EOF instead
      if (stream_->bytes_left() == 0) return Status::OK;

      // Read the sync indicator and check the sync block.
      int sync_indicator;
      if (!stream_->ReadInt(&sync_indicator, &parse_status_)) {
        // We've reached the end of the file
        // TODO: check that we actually reached the end of the file
        DCHECK(stream_->eosr());
        return Status::OK;
      }
      if (sync_indicator != -1) {
        if (state_->LogHasSpace()) {
          stringstream ss;
          ss << "Expecting sync indicator (-1) at file offset "
             << (stream_->file_offset() - sizeof(int)) << ".  "
             << "Sync indicator found " << sync_indicator << ".";
          state_->LogError(ss.str());
        }
        return Status("Bad sync hash");
      }
      RETURN_IF_ERROR(ReadSync());
    }
  }

  return Status::OK;
}

Status HdfsSequenceScanner::ProcessDecompressedBlock() {
  MemPool* pool;
  TupleRow* tuple_row;
  int64_t max_tuples = context_->GetMemory(&pool, &tuple_, &tuple_row);
  int num_to_process = min(max_tuples, num_buffered_records_in_compressed_block_);
  num_buffered_records_in_compressed_block_ -= num_to_process;

  if (scan_node_->materialized_slots().empty()) {
    // Handle case where there are no slots to materialize (e.g. count(*))
    num_to_process = WriteEmptyTuples(context_, tuple_row, num_to_process);
    if (num_to_process > 0) context_->CommitRows(num_to_process);
    COUNTER_UPDATE(scan_node_->rows_read_counter(), num_to_process);
    return Status::OK;
  }

  // Parse record starts and lengths
  int field_location_offset = 0;
  for (int i = 0; i < num_to_process; ++i) {
    DCHECK_LT(i, record_locations_.size());
    int bytes_read = ReadWriteUtil::GetVLong(
        next_record_in_compressed_block_, &record_locations_[i].len);
    if (UNLIKELY(bytes_read == -1)) {
      stringstream ss;
      ss << "Invalid record size in compressed block.";
      if (state_->LogHasSpace()) state_->LogError(ss.str());
      return Status(ss.str());
    }
    next_record_in_compressed_block_ += bytes_read;
    record_locations_[i].record = next_record_in_compressed_block_;
    next_record_in_compressed_block_ += record_locations_[i].len;
  }

  // Parse records to find field locations.
  for (int i = 0; i < num_to_process; ++i) {
    int num_fields = 0;
    if (delimited_text_parser_->escape_char() == '\0') {
      delimited_text_parser_->ParseSingleTuple<false>(
          record_locations_[i].len,
          reinterpret_cast<char*>(record_locations_[i].record),
          &field_locations_[field_location_offset], &num_fields);
    } else {
      delimited_text_parser_->ParseSingleTuple<true>(
          record_locations_[i].len,
          reinterpret_cast<char*>(record_locations_[i].record),
          &field_locations_[field_location_offset], &num_fields);
    }
    DCHECK_EQ(num_fields, scan_node_->materialized_slots().size());
    field_location_offset += num_fields;
    DCHECK_LE(field_location_offset, field_locations_.size());
  }

  int max_added_tuples = (scan_node_->limit() == -1) ?
                         num_to_process :
                         scan_node_->limit() - scan_node_->rows_returned();

  // Materialize parsed cols to tuples
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  // Call jitted function if possible
  int tuples_returned;
  if (write_tuples_fn_ != NULL) {
    // last argument: seq always starts at record_location[0]
    tuples_returned = write_tuples_fn_(this, pool, tuple_row, 
        context_->row_byte_size(), &field_locations_[0], num_to_process,
        max_added_tuples, scan_node_->materialized_slots().size(), 0); 
  } else {
    tuples_returned = WriteAlignedTuples(pool, tuple_row, 
        context_->row_byte_size(), &field_locations_[0], num_to_process,
        max_added_tuples, scan_node_->materialized_slots().size(), 0);
  }

  if (tuples_returned == -1) return parse_status_;
  COUNTER_UPDATE(scan_node_->rows_read_counter(), num_to_process);
  context_->CommitRows(tuples_returned);
  return Status::OK;
}

Status HdfsSequenceScanner::ProcessRange() {
  num_buffered_records_in_compressed_block_ = 0;

  SeqFileHeader* seq_header = reinterpret_cast<SeqFileHeader*>(header_);
  // Block compressed is handled separately to minimize function calls.
  if (seq_header->is_compressed && !seq_header->is_row_compressed) {
    return ProcessBlockCompressedScanRange();
  }
  
  // We count the time here since there is too much overhead to do
  // this on each record.
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  
  bool eosr = false;
  while (!eosr) {
    DCHECK_GT(record_locations_.size(), 0);
    // Get the next compressed or uncompressed record.
    RETURN_IF_ERROR(
        GetRecord(&record_locations_[0].record, &record_locations_[0].len, &eosr));

    if (eosr) {
      DCHECK(record_locations_[0].record == NULL);
      break;
    }

    MemPool* pool;
    TupleRow* tuple_row_mem;
    int max_tuples = context_->GetMemory(&pool, &tuple_, &tuple_row_mem);
    DCHECK_GT(max_tuples, 0);
    
    // Parse the current record.
    bool add_row = false;

    // Parse the current record.
    if (scan_node_->materialized_slots().size() != 0) {
      char* col_start;
      uint8_t* record_start = record_locations_[0].record;
      int num_tuples = 0;
      int num_fields = 0;
      char* row_end_loc;
      uint8_t error_in_row = false;

      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(
          1, record_locations_[0].len, reinterpret_cast<char**>(&record_start), 
          &row_end_loc, &field_locations_[0], &num_tuples, &num_fields, &col_start));
      DCHECK(num_tuples == 1);
      
      uint8_t errors[num_fields];
      memset(errors, 0, sizeof(errors));

      add_row = WriteCompleteTuple(pool, &field_locations_[0], tuple_, tuple_row_mem,
          context_->template_tuple(), &errors[0], &error_in_row);

      if (UNLIKELY(error_in_row)) {
        ReportTupleParseError(&field_locations_[0], errors, 0);
        RETURN_IF_ERROR(parse_status_);
      }
    } else {
      add_row = WriteEmptyTuples(context_, tuple_row_mem, 1);
    }

    if (add_row) context_->CommitRows(1);
    COUNTER_UPDATE(scan_node_->rows_read_counter(), 1);
    if (scan_node_->ReachedLimit()) break;
    if (context_->cancelled()) return Status::CANCELLED;
  }

  return Status::OK;
}

Status HdfsSequenceScanner::ReadFileHeader() {
  uint8_t* header;

  RETURN_IF_FALSE(stream_->ReadBytes(
      sizeof(SEQFILE_VERSION_HEADER), &header, &parse_status_));

  if (memcmp(header, SEQFILE_VERSION_HEADER, sizeof(SEQFILE_VERSION_HEADER))) {
    stringstream ss;
    ss << "Invalid SEQFILE_VERSION_HEADER: '"
       << ReadWriteUtil::HexDump(header, sizeof(SEQFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  // We don't care what this is since we don't use the keys.
  RETURN_IF_FALSE(stream_->SkipText(&parse_status_));

  uint8_t* class_name;
  int len;
  RETURN_IF_FALSE(stream_->ReadText(&class_name, &len, &parse_status_));
  if (memcmp(class_name, HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME, len)) {
    stringstream ss;
    ss << "Invalid SEQFILE_VALUE_CLASS_NAME: '"
       << string(reinterpret_cast<char*>(class_name), len) << "'";
    return Status(ss.str());
  }

  SeqFileHeader* seq_header = reinterpret_cast<SeqFileHeader*>(header_);
  bool is_blk_compressed;
  RETURN_IF_FALSE(
      stream_->ReadBoolean(&header_->is_compressed, &parse_status_));
  RETURN_IF_FALSE(
      stream_->ReadBoolean(&is_blk_compressed, &parse_status_));
  seq_header->is_row_compressed = !is_blk_compressed;
  
  if (header_->is_compressed) {
    uint8_t* codec_ptr;
    RETURN_IF_FALSE(stream_->ReadText(&codec_ptr, &len, &parse_status_));
    header_->codec = string(reinterpret_cast<char*>(codec_ptr), len);
    Codec::CodecMap::const_iterator it = Codec::CODEC_MAP.find(header_->codec);
    DCHECK(it != Codec::CODEC_MAP.end());
    header_->compression_type = it->second;
  } else {
    header_->compression_type = THdfsCompression::NONE;
  }
  VLOG_FILE << stream_->filename() << ": "
            << (header_->is_compressed ? 
               (seq_header->is_row_compressed ?  "row compressed" : "block compressed") :
                "not compressed");
  if (header_->is_compressed) VLOG_FILE << header_->codec;

  // Skip file metadata
  int map_size = 0;
  RETURN_IF_FALSE(stream_->ReadInt(&map_size, &parse_status_));

  for (int i = 0; i < map_size; ++i) {
    RETURN_IF_FALSE(stream_->SkipText(&parse_status_));
    RETURN_IF_FALSE(stream_->SkipText(&parse_status_));
  }
  
  // Read file sync marker
  uint8_t* sync;
  RETURN_IF_FALSE(stream_->ReadBytes(SYNC_HASH_SIZE, &sync, &parse_status_));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);

  if (header_->is_compressed && !seq_header->is_row_compressed) {
    // With block compression, record blocks have a leading -1 marker and sync
    // (i.e., we just read the sync in the file header, but there is another
    // sync immediately following it which is the beginning of the first
    // block). We include this extra marker and sync in the header so that
    // BaseSequenceScanner skips directly to the actual data in the first record
    // block.
    // TODO: this will cause the entire query to fail if this sync is corrupt
    int marker;
    RETURN_IF_FALSE(stream_->ReadInt(&marker, &parse_status_));
    if (marker != HdfsSequenceScanner::SYNC_MARKER) {
      return Status("Didn't find sync marker after file header");
    }
    RETURN_IF_ERROR(ReadSync());
  }

  header_->header_size = stream_->total_bytes_returned();
  return Status::OK;
}

Status HdfsSequenceScanner::ReadBlockHeader(bool* sync) {
  RETURN_IF_FALSE(stream_->ReadInt(&current_block_length_, &parse_status_));
  *sync = false;
  if (current_block_length_ == HdfsSequenceScanner::SYNC_MARKER) {
    RETURN_IF_ERROR(ReadSync());
    RETURN_IF_FALSE(
        stream_->ReadInt(&current_block_length_, &parse_status_));
    *sync = true;
  }
  if (current_block_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad block length: " << current_block_length_ << " at offset " << position;
    return Status(ss.str());
  }
  
  RETURN_IF_FALSE(stream_->ReadInt(&current_key_length_, &parse_status_));
  if (current_key_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad key length: " << current_key_length_ << " at offset " << position;
    return Status(ss.str());
  }

  return Status::OK;
}

Status HdfsSequenceScanner::ReadCompressedBlock() {
  // We are reading a new compressed block.  Pass the previous buffer pool 
  // bytes to the batch.  We don't need them anymore.
  if (!stream_->compact_data()) {
    context_->AcquirePool(data_buffer_pool_.get());
  }

  block_start_ = stream_->file_offset();

  RETURN_IF_FALSE(stream_->ReadVLong(
      &num_buffered_records_in_compressed_block_, &parse_status_));
  if (num_buffered_records_in_compressed_block_ < 0) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss << "Bad compressed block record count: "
         << num_buffered_records_in_compressed_block_;
      state_->LogError(ss.str());
    }
    return Status("bad record count");
  }

  // Skip the compressed key length and key buffers, we don't need them.
  RETURN_IF_FALSE(stream_->SkipText(&parse_status_));
  RETURN_IF_FALSE(stream_->SkipText(&parse_status_));

  // Skip the compressed value length buffer. We don't need these either since the
  // records are in Text format with length included.
  RETURN_IF_FALSE(stream_->SkipText(&parse_status_));

  // Read the compressed value buffer from the unbuffered stream.
  int block_size = 0;
  RETURN_IF_FALSE(stream_->ReadVInt(&block_size, &parse_status_));
  // Check for a reasonable size
  if (block_size > MAX_BLOCK_SIZE || block_size < 0) {
    stringstream ss;
    ss << "Compressed block size is: " << block_size;
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }
  
  uint8_t* compressed_data = NULL;
  RETURN_IF_FALSE(stream_->ReadBytes(block_size, &compressed_data, &parse_status_));

  {
    int len = 0;
    SCOPED_TIMER(decompress_timer_);
    RETURN_IF_ERROR(decompressor_->ProcessBlock(block_size, compressed_data,
                                                &len, &unparsed_data_buffer_));
    next_record_in_compressed_block_ = unparsed_data_buffer_;
  }

  return Status::OK;
}

void HdfsSequenceScanner::LogRowParseError(int row_idx, stringstream* ss) {
  DCHECK_LT(row_idx, record_locations_.size());
  *ss << string(reinterpret_cast<const char*>(record_locations_[row_idx].record), 
                  record_locations_[row_idx].len);
}

