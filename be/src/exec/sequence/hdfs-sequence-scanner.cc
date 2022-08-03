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

#include "exec/sequence/hdfs-sequence-scanner.h"

#include <string.h>
#include <algorithm>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "exec/delimited-text-parser.h"
#include "exec/delimited-text-parser.inline.h"
#include "exec/hdfs-scan-node-base.h"
#include "exec/read-write-util.h"
#include "exec/scanner-context.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.h"
#include "gen-cpp/ErrorCodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "util/codec.h"
#include "util/error-util.h"
#include "util/runtime-profile-counters.h"
#include "util/stopwatch.h"

#include "common/names.h"

namespace impala {
class LlvmCodeGen;
class ScalarExpr;
class TupleRow;
}

namespace llvm {
class Function;
}

using namespace impala;

const char* const HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME =
    "org.apache.hadoop.io.Text";

const uint8_t HdfsSequenceScanner::SEQFILE_VERSION_HEADER[4] = {'S', 'E', 'Q', 6};

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

HdfsSequenceScanner::HdfsSequenceScanner(HdfsScanNodeBase* scan_node,
    RuntimeState* state) : BaseSequenceScanner(scan_node, state) {
}

HdfsSequenceScanner::~HdfsSequenceScanner() {
}

// Codegen for materialized parsed data into tuples.
Status HdfsSequenceScanner::Codegen(HdfsScanPlanNode* node, FragmentState* state,
    llvm::Function** write_aligned_tuples_fn) {
  *write_aligned_tuples_fn = nullptr;
  DCHECK(state->ShouldCodegen());
  DCHECK(state->codegen() != nullptr);
  llvm::Function* write_complete_tuple_fn;
  RETURN_IF_ERROR(CodegenWriteCompleteTuple(node, state, &write_complete_tuple_fn));
  DCHECK(write_complete_tuple_fn != nullptr);
  RETURN_IF_ERROR(CodegenWriteAlignedTuples(
      node, state, write_complete_tuple_fn, write_aligned_tuples_fn));
  DCHECK(*write_aligned_tuples_fn != nullptr);
  return Status::OK();
}

Status HdfsSequenceScanner::InitNewRange() {
  DCHECK(header_ != nullptr);
  only_parsing_header_ = false;

  HdfsPartitionDescriptor* hdfs_partition = context_->partition_descriptor();

  text_converter_.reset(new TextConverter(hdfs_partition->escape_char(),
      scan_node_->hdfs_table()->null_column_value()));

  delimited_text_parser_.reset(new SequenceDelimitedTextParser(
      scan_node_->hdfs_table()->num_cols(), scan_node_->num_partition_keys(),
      scan_node_->is_materialized_col(), '\0', hdfs_partition->field_delim(),
      hdfs_partition->collection_delim(), hdfs_partition->escape_char()));

  num_buffered_records_in_compressed_block_ = 0;

  SeqFileHeader* seq_header = reinterpret_cast<SeqFileHeader*>(header_);
  if (seq_header->is_compressed) {
    RETURN_IF_ERROR(UpdateDecompressor(header_->codec));
  }

  // Initialize codegen fn
  RETURN_IF_ERROR(InitializeWriteTuplesFn(hdfs_partition,
      THdfsFileFormat::SEQUENCE_FILE, "HdfsSequenceScanner"));
  return Status::OK();
}

Status HdfsSequenceScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(BaseSequenceScanner::Open(context));

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  record_locations_.resize(state_->batch_size());
  field_locations_.resize(state_->batch_size() * scan_node_->materialized_slots().size());
  return Status::OK();
}

BaseSequenceScanner::FileHeader* HdfsSequenceScanner::AllocateFileHeader() {
  return new SeqFileHeader;
}

inline Status HdfsSequenceScanner::GetRecord(uint8_t** record_ptr,
                                             int64_t* record_len) {
  // There are 2 cases:
  //  - Record-compressed -- like a regular record, but the data is compressed.
  //  - Uncompressed.
  RETURN_IF_ERROR(ReadBlockHeader());

  // We don't look at the keys, only the values.
  RETURN_IF_FALSE(stream_->SkipBytes(current_key_length_, &parse_status_));

  if (header_->is_compressed) {
    int64_t in_size = current_block_length_ - current_key_length_;
    if (in_size < 0) {
      stringstream ss;
      ss << stream_->filename() << " Invalid record size: " << in_size;
      return Status(ss.str());
    }
    uint8_t* compressed_data;
    RETURN_IF_FALSE(
        stream_->ReadBytes(in_size, &compressed_data, &parse_status_));

    int64_t len;
    {
      SCOPED_TIMER(decompress_timer_);
      RETURN_IF_ERROR(decompressor_->ProcessBlock(false, in_size, compressed_data,
          &len, &unparsed_data_buffer_));
      VLOG_FILE << "Decompressed " << in_size << " to " << len;
    }
    *record_ptr = unparsed_data_buffer_;
    // Read the length of the record.
    int size = ReadWriteUtil::GetVLong(*record_ptr, record_len, in_size);
    if (size == -1) {
      stringstream ss;
      ss << stream_->filename() << " Invalid record size: " << in_size;
      return Status(ss.str());
    }
    *record_ptr += size;
  } else {
    // Uncompressed records
    RETURN_IF_FALSE(stream_->ReadVLong(record_len, &parse_status_));
    if (*record_len < 0) {
      stringstream ss;
      ss << stream_->filename() << " Invalid record length: " << *record_len;
      return Status(ss.str());
    }
    RETURN_IF_FALSE(
        stream_->ReadBytes(*record_len, record_ptr, &parse_status_));
  }
  return Status::OK();
}

// Process block compressed sequence files.  This is the most used sequence file
// format.  The general strategy is to process the data in large chunks to minimize
// function calls.  The process is:
// 1. Decompress an entire block
// 2. In row batch sizes:
//   a. Collect the start of records and their lengths
//   b. Parse cols locations to field_locations_
//   c. Materialize those field locations to row batches
// 3. Read the sync indicator and check the sync block
// This mimics the technique for text.
// This function only returns on error or when the entire scan range is complete.
Status HdfsSequenceScanner::ProcessBlockCompressedScanRange(RowBatch* row_batch) {
  DCHECK(header_->is_compressed);

  if (num_buffered_records_in_compressed_block_ == 0) {
    // We are reading a new compressed block. Pass the previous buffer pool bytes to the
    // batch. We don't need them anymore.
    if (!decompressor_->reuse_output_buffer()) {
      row_batch->tuple_data_pool()->AcquireData(data_buffer_pool_.get(), false);
      RETURN_IF_ERROR(CommitRows(0, row_batch));
      if (row_batch->AtCapacity()) return Status::OK();
    }
    // Step 1
    RETURN_IF_ERROR(ReadCompressedBlock());
    if (num_buffered_records_in_compressed_block_ < 0) return parse_status_;
  }

  // Step 2
  while (num_buffered_records_in_compressed_block_ > 0) {
    RETURN_IF_ERROR(ProcessDecompressedBlock(row_batch));
    if (row_batch->AtCapacity() || scan_node_->ReachedLimitShared()) break;
  }

  if (num_buffered_records_in_compressed_block_ == 0) {
    // SequenceFiles don't end with syncs.
    if (stream_->eof()) {
      eos_ = true;
      return Status::OK();
    }

    // Step 3
    int sync_indicator;
    RETURN_IF_FALSE(stream_->ReadInt(&sync_indicator, &parse_status_));
    if (sync_indicator != -1) {
      if (state_->LogHasSpace()) {
        stringstream ss;
        ss << stream_->filename() << " Expecting sync indicator (-1) at file offset "
           << (stream_->file_offset() - sizeof(int)) << ".  "
           << "Sync indicator found " << sync_indicator << ".";
        state_->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
      }
      return Status("Bad sync hash");
    }
    RETURN_IF_ERROR(ReadSync());
  }

  return Status::OK();
}

Status HdfsSequenceScanner::ProcessDecompressedBlock(RowBatch* row_batch) {
  int64_t max_tuples = row_batch->capacity() - row_batch->num_rows();
  int num_to_process = min(max_tuples, num_buffered_records_in_compressed_block_);
  num_buffered_records_in_compressed_block_ -= num_to_process;

  TupleRow* tuple_row = row_batch->GetRow(row_batch->AddRow());
  if (scan_node_->materialized_slots().empty()) {
    // Handle case where there are no slots to materialize (e.g. count(*))
    num_to_process = WriteTemplateTuples(tuple_row, num_to_process);
    COUNTER_ADD(scan_node_->rows_read_counter(), num_to_process);
    RETURN_IF_ERROR(CommitRows(num_to_process, row_batch));
    return Status::OK();
  }

  // Parse record starts and lengths
  int field_location_offset = 0;
  for (int i = 0; i < num_to_process; ++i) {
    if (i >= record_locations_.size() || record_locations_[i].len < 0
        || next_record_in_compressed_block_ > data_buffer_end_) {
      stringstream ss;
      ss << stream_->filename() << " Invalid compressed block";
      return Status(ss.str());
    }
    int bytes_read = ReadWriteUtil::GetVLong(next_record_in_compressed_block_,
        &record_locations_[i].len, next_record_in_compressed_block_len_);
    if (UNLIKELY(bytes_read == -1)) {
      stringstream ss;
      ss << stream_->filename() << " Invalid compressed block";
      return Status(ss.str());
    }
    next_record_in_compressed_block_ += bytes_read;
    next_record_in_compressed_block_len_ -= bytes_read;
    if (next_record_in_compressed_block_len_ <= 0) {
      stringstream ss;
      ss << stream_->filename() << " Invalid compressed block";
      return Status(ss.str());
    }
    record_locations_[i].record = next_record_in_compressed_block_;
    next_record_in_compressed_block_ += record_locations_[i].len;
    next_record_in_compressed_block_len_ -= record_locations_[i].len;
    if (next_record_in_compressed_block_len_ < 0) {
      stringstream ss;
      ss << stream_->filename() << " Invalid compressed block";
      return Status(ss.str());
    }
  }

  // Parse records to find field locations.
  for (int i = 0; i < num_to_process; ++i) {
    int num_fields = 0;
    if (delimited_text_parser_->escape_char() == '\0') {
      RETURN_IF_ERROR(delimited_text_parser_->ParseSingleTuple<false>(
          record_locations_[i].len,
          reinterpret_cast<char*>(record_locations_[i].record),
          &field_locations_[field_location_offset], &num_fields));
    } else {
      RETURN_IF_ERROR(delimited_text_parser_->ParseSingleTuple<true>(
          record_locations_[i].len,
          reinterpret_cast<char*>(record_locations_[i].record),
          &field_locations_[field_location_offset], &num_fields));
    }
    if (num_fields != scan_node_->materialized_slots().size()) {
      stringstream ss;
      ss << stream_->filename() << " Invalid compressed block";
      return Status(ss.str());
    }
    field_location_offset += num_fields;
    if (field_location_offset > field_locations_.size()) {
      stringstream ss;
      ss << stream_->filename() << " Invalid compressed block";
      return Status(ss.str());
    }
  }

  int max_added_tuples = (scan_node_->limit() == -1) ?
      num_to_process :
      scan_node_->limit() - scan_node_->rows_returned_shared();

  // Materialize parsed cols to tuples
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());

  // Need to copy out strings if they may reference the original I/O buffer.
  const bool copy_strings = !header_->is_compressed && !string_slot_offsets_.empty();

  if (write_tuples_fn_ != nullptr) {
    // HdfsScanner::InitializeWriteTuplesFn() will skip codegen if there are string slots
    // and escape characters. TextConverter::WriteSlot() will be used instead.
    DCHECK(scan_node_->tuple_desc()->string_slots().empty() ||
        delimited_text_parser_->escape_char() == '\0');
  }

  int tuples_returned = WriteAlignedTuplesCodegenOrInterpret(row_batch->tuple_data_pool(),
      tuple_row, field_locations_.data(), num_to_process, max_added_tuples,
      scan_node_->materialized_slots().size(), 0, copy_strings);

  if (tuples_returned == -1) return parse_status_;
  COUNTER_ADD(scan_node_->rows_read_counter(), num_to_process);
  RETURN_IF_ERROR(CommitRows(tuples_returned, row_batch));
  return Status::OK();
}

Status HdfsSequenceScanner::ProcessRange(RowBatch* row_batch) {
  SeqFileHeader* seq_header = reinterpret_cast<SeqFileHeader*>(header_);
  // Block compressed is handled separately to minimize function calls.
  if (seq_header->is_compressed && !seq_header->is_row_compressed) {
    return ProcessBlockCompressedScanRange(row_batch);
  }

  // We count the time here since there is too much overhead to do
  // this on each record.
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  int64_t num_rows_read = 0;

  const bool copy_strings = !seq_header->is_compressed && !string_slot_offsets_.empty();
  const bool has_materialized_slots = !scan_node_->materialized_slots().empty();
  while (!eos_) {
    DCHECK_GT(record_locations_.size(), 0);
    TupleRow* tuple_row_mem = row_batch->GetRow(row_batch->AddRow());

    // Get the next compressed or uncompressed record and parse it.
    RETURN_IF_ERROR(GetRecord(&record_locations_[0].record, &record_locations_[0].len));
    bool add_row = false;
    if (has_materialized_slots) {
      char* col_start;
      uint8_t* record_start = record_locations_[0].record;
      int num_tuples = 0;
      int num_fields = 0;
      char* row_end_loc;
      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(
          1, record_locations_[0].len, reinterpret_cast<char**>(&record_start),
          &row_end_loc, field_locations_.data(), &num_tuples, &num_fields, &col_start));
      DCHECK_EQ(num_tuples, 1);

      uint8_t error_in_row = false;
      uint8_t errors[num_fields];
      memset(errors, 0, num_fields);
      MemPool* pool = row_batch->tuple_data_pool();
      add_row = WriteCompleteTuple(pool, field_locations_.data(),
          tuple_, tuple_row_mem, template_tuple_, &errors[0], &error_in_row);
      if (UNLIKELY(error_in_row)) {
        ReportTupleParseError(field_locations_.data(), errors);
        RETURN_IF_ERROR(parse_status_);
      }
      if (add_row && copy_strings) {
        if (UNLIKELY(!tuple_->CopyStrings("HdfsSequenceScanner::ProcessRange()",
              state_, string_slot_offsets_.data(), string_slot_offsets_.size(), pool,
              &parse_status_))) {
          return parse_status_;
        }
      }
    } else {
      add_row = WriteTemplateTuples(tuple_row_mem, 1) > 0;
    }
    num_rows_read++;
    if (add_row) RETURN_IF_ERROR(CommitRows(1, row_batch));

    // Sequence files don't end with syncs.
    if (stream_->eof()) {
      eos_ = true;
      break;
    }

    // Check for sync by looking for the marker that precedes syncs.
    int marker;
    RETURN_IF_FALSE(stream_->ReadInt(&marker, &parse_status_, /* peek */ true));
    if (marker == SYNC_MARKER) {
      RETURN_IF_FALSE(stream_->ReadInt(&marker, &parse_status_, /* peek */ false));
      RETURN_IF_ERROR(ReadSync());
    }

    // These checks must come after advancing past the next sync such that the stream is
    // at the start of the next data block when this function is called again.
    if (row_batch->AtCapacity() || scan_node_->ReachedLimitShared()) break;
  }

  COUNTER_ADD(scan_node_->rows_read_counter(), num_rows_read);
  return Status::OK();
}

Status HdfsSequenceScanner::ReadFileHeader() {
  uint8_t* header;

  RETURN_IF_FALSE(stream_->ReadBytes(
      sizeof(SEQFILE_VERSION_HEADER), &header, &parse_status_));

  if (memcmp(header, SEQFILE_VERSION_HEADER, sizeof(SEQFILE_VERSION_HEADER))) {
    stringstream ss;
    ss << stream_->filename() << " Invalid SEQFILE_VERSION_HEADER: '"
       << ReadWriteUtil::HexDump(header, sizeof(SEQFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  // We don't care what this is since we don't use the keys.
  RETURN_IF_FALSE(stream_->SkipText(&parse_status_));

  uint8_t* class_name;
  int64_t len;
  RETURN_IF_FALSE(stream_->ReadText(&class_name, &len, &parse_status_));
  if (memcmp(class_name, HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME, len)) {
    stringstream ss;
    ss << stream_->filename() << " Invalid SEQFILE_VALUE_CLASS_NAME: '"
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
    if (it == Codec::CODEC_MAP.end()) {
      return Status(TErrorCode::COMPRESSED_FILE_BLOCK_CORRUPTED, header_->codec);
    }
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

  header_->header_size = stream_->total_bytes_returned();

  if (!header_->is_compressed || seq_header->is_row_compressed) {
    // Block-compressed scan ranges have an extra sync following the sync in the header,
    // all other formats do not
    header_->header_size -= SYNC_HASH_SIZE;
  }
  return Status::OK();
}

Status HdfsSequenceScanner::ReadBlockHeader() {
  RETURN_IF_FALSE(stream_->ReadInt(&current_block_length_, &parse_status_));
  if (current_block_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << stream_->filename() << " Bad block length: " << current_block_length_
       << " at offset " << position;
    return Status(ss.str());
  }

  RETURN_IF_FALSE(stream_->ReadInt(&current_key_length_, &parse_status_));
  if (current_key_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << stream_->filename() << " Bad key length: " << current_key_length_
       << " at offset " << position;
    return Status(ss.str());
  }

  return Status::OK();
}

Status HdfsSequenceScanner::ReadCompressedBlock() {
  int64_t num_buffered_records;
  RETURN_IF_FALSE(stream_->ReadVLong(
      &num_buffered_records, &parse_status_));
  if (num_buffered_records < 0) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss << stream_->filename()
         << " Bad compressed block record count: " << num_buffered_records;
      state_->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
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
  int64_t block_size = 0;
  RETURN_IF_FALSE(stream_->ReadVLong(&block_size, &parse_status_));
  // Check for a reasonable size
  if (block_size > MAX_BLOCK_SIZE || block_size < 0) {
    stringstream ss;
    ss << stream_->filename() << " Compressed block size is: " << block_size;
    return Status(ss.str());
  }

  uint8_t* compressed_data = nullptr;
  RETURN_IF_FALSE(stream_->ReadBytes(block_size, &compressed_data, &parse_status_));

  {
    int64_t len;
    SCOPED_TIMER(decompress_timer_);
    RETURN_IF_ERROR(decompressor_->ProcessBlock(false, block_size, compressed_data,
                                                &len, &unparsed_data_buffer_));
    VLOG_FILE << "Decompressed " << block_size << " to " << len;
    next_record_in_compressed_block_ = unparsed_data_buffer_;
    next_record_in_compressed_block_len_ = len;
    data_buffer_end_ = unparsed_data_buffer_ + len;
  }
  num_buffered_records_in_compressed_block_ = num_buffered_records;
  return Status::OK();
}
