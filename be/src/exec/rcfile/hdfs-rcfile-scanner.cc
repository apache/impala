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

#include "exec/rcfile/hdfs-rcfile-scanner.h"

#include <boost/algorithm/string.hpp>

#include "exec/exec-node.inline.h"
#include "exec/hdfs-scan-node.h"
#include "exec/sequence/hdfs-sequence-scanner.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.inline.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/codec.h"
#include "util/string-parser.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"
using namespace impala;

const char* const HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer";

const char* const HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer";

const char* const HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS =
  "hive.io.rcfile.column.number";

const uint8_t HdfsRCFileScanner::RCFILE_VERSION_HEADER[4] = {'R', 'C', 'F', 1};

// Check max column limit, set to 8 million
const int HdfsRCFileScanner::MAX_NCOLS = 8000000;

// Macro to convert between SerdeUtil errors to Status returns.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

HdfsRCFileScanner::HdfsRCFileScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
    : BaseSequenceScanner(scan_node, state) {
}

HdfsRCFileScanner::~HdfsRCFileScanner() {
}

Status HdfsRCFileScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(BaseSequenceScanner::Open(context));
  text_converter_.reset(
      new TextConverter(0, scan_node_->hdfs_table()->null_column_value()));
  scan_node_->IncNumScannersCodegenDisabled();
  return Status::OK();
}

Status HdfsRCFileScanner::InitNewRange() {
  DCHECK(header_ != nullptr);

  only_parsing_header_ = false;
  row_group_buffer_size_ = 0;

  // Can reuse buffer if there are no string columns (since the tuple won't contain
  // ptrs into the decompressed data).
  reuse_row_group_buffer_ = scan_node_->tuple_desc()->string_slots().empty();

  if (header_->is_compressed) {
    RETURN_IF_ERROR(Codec::CreateDecompressor(nullptr,
        reuse_row_group_buffer_, header_->codec, &decompressor_));
  }

  int ncols = reinterpret_cast<RcFileHeader*>(header_)->num_cols;
  if (ncols < 0 || ncols > MAX_NCOLS) {
    stringstream ss;
    ss << stream_->filename() << " Column limit has exceeded " << MAX_NCOLS
       << " limit, the number of columns are " << ncols;
    return Status(ss.str());
  }
  // Allocate the buffers for the key information that is used to read and decode
  // the column data.
  columns_.resize(reinterpret_cast<RcFileHeader*>(header_)->num_cols);
  int num_table_cols =
      scan_node_->hdfs_table()->num_cols() - scan_node_->num_partition_keys();
  for (int i = 0; i < columns_.size(); ++i) {
    if (i < num_table_cols) {
      int col_idx = i + scan_node_->num_partition_keys();
      columns_[i].materialize_column = scan_node_->GetMaterializedSlotIdx(
          vector<int>(1, col_idx)) != HdfsScanNodeBase::SKIP_COLUMN;
    } else {
      // Treat columns not found in table metadata as extra unmaterialized columns
      columns_[i].materialize_column = false;
    }
  }

  // TODO: Initialize codegen fn here
  return Status::OK();
}

Status HdfsRCFileScanner::ReadFileHeader() {
  RcFileHeader* rc_header = reinterpret_cast<RcFileHeader*>(header_);

  // Validate file version
  uint8_t* header;
  RETURN_IF_FALSE(stream_->ReadBytes(
      sizeof(RCFILE_VERSION_HEADER), &header, &parse_status_));
  if (!memcmp(header, HdfsSequenceScanner::SEQFILE_VERSION_HEADER,
      sizeof(HdfsSequenceScanner::SEQFILE_VERSION_HEADER))) {
    rc_header->version = SEQ6;
  } else if (!memcmp(header, RCFILE_VERSION_HEADER, sizeof(RCFILE_VERSION_HEADER))) {
    rc_header->version = RCF1;
  } else {
    stringstream ss;
    ss << stream_->filename() << " Invalid RCFILE_VERSION_HEADER: '"
       << ReadWriteUtil::HexDump(header, sizeof(RCFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  if (rc_header->version == SEQ6) {
    // Validate class name key/value
    uint8_t* class_name_key;
    int64_t len;
    RETURN_IF_FALSE(
        stream_->ReadText(&class_name_key, &len, &parse_status_));
    if (len != strlen(HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME) ||
        memcmp(class_name_key, HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME, len)) {
      stringstream ss;
      ss << stream_->filename() << " Invalid RCFILE_KEY_CLASS_NAME: '"
         << string(reinterpret_cast<char*>(class_name_key), len) << "' len=" << len;
      return Status(ss.str());
    }

    uint8_t* class_name_val;
    RETURN_IF_FALSE(
        stream_->ReadText(&class_name_val, &len, &parse_status_));
    if (len != strlen(HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME) ||
        memcmp(class_name_val, HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME, len)) {
      stringstream ss;
      ss << stream_->filename() << " Invalid RCFILE_VALUE_CLASS_NAME: '"
         << string(reinterpret_cast<char*>(class_name_val), len) << "' len=" << len;
      return Status(ss.str());
    }
  }

  // Check for compression
  RETURN_IF_FALSE(
      stream_->ReadBoolean(&header_->is_compressed, &parse_status_));
  if (rc_header->version == SEQ6) {
    // Read the is_blk_compressed header field. This field should *always*
    // be FALSE, and is the result of using the sequence file header format in the
    // original RCFile format.
    bool is_blk_compressed;
    RETURN_IF_FALSE(
        stream_->ReadBoolean(&is_blk_compressed, &parse_status_));
    if (is_blk_compressed) {
      stringstream ss;
      ss << stream_->filename() << " RC files does not support block compression.";
      return Status(ss.str());
    }
  }
  if (header_->is_compressed) {
    uint8_t* codec_ptr;
    int64_t len;
    // Read the codec and get the right decompressor class.
    RETURN_IF_FALSE(stream_->ReadText(&codec_ptr, &len, &parse_status_));
    header_->codec = string(reinterpret_cast<char*>(codec_ptr), len);
    Codec::CodecMap::const_iterator it = Codec::CODEC_MAP.find(header_->codec);
    if (it == Codec::CODEC_MAP.end()) {
      stringstream ss;
      ss << stream_->filename() << " Invalid codec: " << header_->codec;
      return Status(ss.str());
    }
    header_->compression_type = it->second;
  } else {
    header_->compression_type = THdfsCompression::NONE;
  }
  VLOG_FILE << stream_->filename() << ": "
            << (header_->is_compressed ?  "compressed" : "not compressed");
  if (header_->is_compressed) VLOG_FILE << header_->codec;

  RETURN_IF_ERROR(ReadNumColumnsMetadata());

  // Read file sync marker
  uint8_t* sync;
  RETURN_IF_FALSE(stream_->ReadBytes(SYNC_HASH_SIZE, &sync, &parse_status_));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);

  header_->header_size = stream_->total_bytes_returned() - SYNC_HASH_SIZE;
  return Status::OK();
}

Status HdfsRCFileScanner::ReadNumColumnsMetadata() {
  int map_size = 0;
  RETURN_IF_FALSE(stream_->ReadInt(&map_size, &parse_status_));

  for (int i = 0; i < map_size; ++i) {
    uint8_t* key, *value;
    int64_t key_len, value_len;
    RETURN_IF_FALSE(stream_->ReadText(&key, &key_len, &parse_status_));
    RETURN_IF_FALSE(stream_->ReadText(&value, &value_len, &parse_status_));

    if (key_len == strlen(RCFILE_METADATA_KEY_NUM_COLS) &&
        !memcmp(key, HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS, key_len)) {
      string value_str(reinterpret_cast<char*>(value), value_len);
      StringParser::ParseResult result;
      int num_cols =
          StringParser::StringToInt<int>(value_str.c_str(), value_str.size(), &result);
      if (result != StringParser::PARSE_SUCCESS || num_cols < 0) {
        stringstream ss;
        ss << " Could not parse number of columns in file " << stream_->filename()
           << " : " << value_str;
        if (result == StringParser::PARSE_OVERFLOW) ss << " (result overflowed)";
        return Status(ss.str());
      }
      RcFileHeader* rc_header = reinterpret_cast<RcFileHeader*>(header_);
      rc_header->num_cols = num_cols;
    }
  }
  return Status::OK();
}

BaseSequenceScanner::FileHeader* HdfsRCFileScanner::AllocateFileHeader() {
  return new RcFileHeader;
}

Status HdfsRCFileScanner::StartRowGroup() {
  num_rows_ = 0;
  row_pos_ = 0;
  key_length_ = 0;
  compressed_key_length_ = 0;

  for (int i = 0; i < columns_.size(); ++i) {
    columns_[i].buffer_len = 0;
    columns_[i].buffer_pos = 0;
    columns_[i].uncompressed_buffer_len = 0;
    columns_[i].key_buffer_len = 0;
    columns_[i].key_buffer_pos = 0;
    columns_[i].current_field_len = 0;
    columns_[i].current_field_len_rep = 0;
  }

  while (num_rows_ == 0) {
    RETURN_IF_ERROR(ReadRowGroupHeader());
    RETURN_IF_ERROR(ReadKeyBuffers());
    if (!reuse_row_group_buffer_ || row_group_buffer_size_ < row_group_length_) {
      // Allocate a new buffer for reading the row group.  Row groups have a
      // fixed number of rows so take a guess at how big it will be based on
      // the previous row group size.
      // The row group length depends on the user data and can be very big. This
      // can cause us to go way over the mem limit so use TryAllocate instead.
      row_group_buffer_ = data_buffer_pool_->TryAllocate(row_group_length_);
      if (UNLIKELY(row_group_buffer_ == nullptr)) {
        string details("RC file scanner failed to allocate row group buffer.");
        return scan_node_->mem_tracker()->MemLimitExceeded(state_, details,
            row_group_length_);
      }
      row_group_buffer_size_ = row_group_length_;
    }
    RETURN_IF_ERROR(ReadColumnBuffers());
  }
  return Status::OK();
}

Status HdfsRCFileScanner::ReadRowGroupHeader() {
  int32_t record_length;
  RETURN_IF_FALSE(stream_->ReadInt(&record_length, &parse_status_));
  if (record_length < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << stream_->filename() << " Bad record length: " << record_length
       << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_FALSE(stream_->ReadInt(&key_length_, &parse_status_));
  if (key_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << stream_->filename() << " Bad key length: " << key_length_
       << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_FALSE(stream_->ReadInt(&compressed_key_length_, &parse_status_));
  if (compressed_key_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << stream_->filename() << " Bad compressed key length: " << compressed_key_length_
       << " at offset: " << position;
    return Status(ss.str());
  }
  return Status::OK();
}

Status HdfsRCFileScanner::ReadKeyBuffers() {
  if (key_buffer_.size() < key_length_) key_buffer_.resize(key_length_);
  uint8_t* key_buffer = key_buffer_.data();

  if (header_->is_compressed) {
    uint8_t* compressed_buffer;
    RETURN_IF_FALSE(stream_->ReadBytes(
        compressed_key_length_, &compressed_buffer, &parse_status_));
    {
      SCOPED_TIMER(decompress_timer_);
      RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, compressed_key_length_,
          compressed_buffer, &key_length_, &key_buffer));
      VLOG_FILE << "Decompressed " << compressed_key_length_ << " to " << key_length_;
    }
  } else {
    uint8_t* buffer;
    RETURN_IF_FALSE(
        stream_->ReadBytes(key_length_, &buffer, &parse_status_));
    // Make a copy of this buffer.  The underlying IO buffer will get recycled
    memcpy(key_buffer, buffer, key_length_);
  }

  uint8_t* key_buf_ptr = key_buffer;
  row_group_length_ = 0;
  int remain_len = key_length_;
  int bytes_read = ReadWriteUtil::GetVInt(key_buf_ptr, &num_rows_, key_length_);
  if (bytes_read == -1 || num_rows_ < 0) {
    stringstream ss;
    ss << stream_->filename() << " Bad row group key buffer, key length: " << key_length_;
    return Status(ss.str());
  }
  key_buf_ptr += bytes_read;
  remain_len = remain_len - bytes_read;

  // Track the starting position in the buffer.
  uint8_t* start_key_buf_ptr = key_buf_ptr;
  for (int col_idx = 0; col_idx < columns_.size(); ++col_idx) {
    if (key_buf_ptr < start_key_buf_ptr || (key_buf_ptr > key_buffer + key_length_)
        || remain_len <= 0) {
      stringstream ss;
      ss << stream_->filename() << " Bad row group key buffer, column idx: " << col_idx;
      return Status(ss.str());
    }
    RETURN_IF_ERROR(GetCurrentKeyBuffer(
        col_idx, !columns_[col_idx].materialize_column, &key_buf_ptr, remain_len));
    remain_len = remain_len - (key_buf_ptr - start_key_buf_ptr);
    start_key_buf_ptr = key_buf_ptr;
  }

  return Status::OK();
}

Status HdfsRCFileScanner::BadColumnInfo(int col_idx) {
  stringstream ss;
  ss << stream_->filename() << " Corrupt column at index: " << col_idx;
  return Status(ss.str());
}

Status HdfsRCFileScanner::GetCurrentKeyBuffer(
    int col_idx, bool skip_col_data, uint8_t** key_buf_ptr, int buf_length) {
  ColumnInfo& col_info = columns_[col_idx];
  int remain_len = buf_length;

  if (remain_len <= 0) {
    return BadColumnInfo(col_idx);
  }

  DCHECK_GT(remain_len, 0);
  int bytes_read = ReadWriteUtil::GetVInt(*key_buf_ptr, &col_info.buffer_len, remain_len);
  if (bytes_read == -1) {
    return BadColumnInfo(col_idx);
  }
  *key_buf_ptr += bytes_read;
  remain_len -= bytes_read;
  DCHECK_GT(remain_len, 0);

  bytes_read =
      ReadWriteUtil::GetVInt(*key_buf_ptr, &col_info.uncompressed_buffer_len, remain_len);
  if (bytes_read == -1) {
    return BadColumnInfo(col_idx);
  }
  *key_buf_ptr += bytes_read;
  remain_len -= bytes_read;
  if (remain_len <= 0) {
    return BadColumnInfo(col_idx);
  }

  int col_key_buf_len;
  bytes_read = ReadWriteUtil::GetVInt(*key_buf_ptr, &col_key_buf_len, remain_len);
  if (bytes_read == -1) {
    return BadColumnInfo(col_idx);
  }

  *key_buf_ptr += bytes_read;
  remain_len -= bytes_read;
  if (col_info.uncompressed_buffer_len < 0 || remain_len <= 0) {
    return BadColumnInfo(col_idx);
  }

  if (!skip_col_data) {
    col_info.key_buffer = *key_buf_ptr;

    DCHECK_GE(col_info.uncompressed_buffer_len, 0);

    // Set the offset for the start of the data for this column in the allocated buffer.
    col_info.start_offset = row_group_length_;
    row_group_length_ += col_info.uncompressed_buffer_len;
  }
  col_info.buf_length = col_key_buf_len;
  *key_buf_ptr += col_key_buf_len;
  remain_len -= bytes_read;
  DCHECK_GE(remain_len, 0);

  return Status::OK();
}

inline Status HdfsRCFileScanner::NextField(int col_idx) {
  ColumnInfo& col_info = columns_[col_idx];
  col_info.buffer_pos += col_info.current_field_len;

  if (col_info.current_field_len_rep > 0) {
    // repeat the previous length
    --col_info.current_field_len_rep;
  } else {
    // Get the next column length or repeat count
    int64_t length = 0;
    uint8_t* col_key_buf = col_info.key_buffer;
    int bytes_read = ReadWriteUtil::GetVLong(
        col_key_buf, col_info.key_buffer_pos, &length, col_info.buf_length);
    if (bytes_read == -1) {
        int64_t position = stream_->file_offset();
        stringstream ss;
        ss << stream_->filename() << " Invalid column length at offset: " << position;
        return Status(ss.str());
    }
    col_info.key_buffer_pos += bytes_read;

    if (length < 0) {
      // The repeat count is stored as the logical negation of the number of repetitions.
      // See the column-key-buffer comment in hdfs-rcfile-scanner.h.
      col_info.current_field_len_rep = ~length - 1;
    } else {
      col_info.current_field_len = length;
    }
  }
  return Status::OK();
}

inline Status HdfsRCFileScanner::NextRow() {
  // TODO: Wrap this in an iterator and prevent people from alternating
  // calls to NextField()/NextRow()
  DCHECK_LT(row_pos_, num_rows_);
  for (int col_idx = 0; col_idx < columns_.size(); ++col_idx) {
    if (columns_[col_idx].materialize_column) {
      RETURN_IF_ERROR(NextField(col_idx));
    }
  }
  ++row_pos_;
  return Status::OK();
}

Status HdfsRCFileScanner::ReadColumnBuffers() {
  for (int col_idx = 0; col_idx < columns_.size(); ++col_idx) {
    ColumnInfo& column = columns_[col_idx];
    if (!columns_[col_idx].materialize_column) {
      if (column.buffer_len < 0) {
        stringstream ss;
        ss << stream_->filename() << " Bad column buffer len: " << column.buffer_len;
        return Status(ss.str());
      }
      // Not materializing this column, just skip it.
      RETURN_IF_FALSE(
          stream_->SkipBytes(column.buffer_len, &parse_status_));
      continue;
    }

    // TODO: Stream through these column buffers instead of reading everything
    // in at once.
    // Uncompressed buffer size for a column should not exceed the row_group_length_
    // as row_group_length_ is a sum of uncompressed buffer length for all the columns
    // so this check ensures that there is enough space in row_group_buffer for the
    // uncompressed data.
    if (column.uncompressed_buffer_len + column.start_offset > row_group_length_) {
      stringstream ss;
      ss << stream_->filename() << " Bad column buffer uncompressed buffer length: "
         << column.uncompressed_buffer_len << " at offset " << column.start_offset;
      return Status(ss.str());
    }

    if (header_->is_compressed) {
      uint8_t* compressed_input;
      RETURN_IF_FALSE(stream_->ReadBytes(
          column.buffer_len, &compressed_input, &parse_status_));
      uint8_t* compressed_output = row_group_buffer_ + column.start_offset;
      {
        SCOPED_TIMER(decompress_timer_);
        RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, column.buffer_len,
            compressed_input, &column.uncompressed_buffer_len,
            &compressed_output));
        VLOG_FILE << "Decompressed " << column.buffer_len << " to "
                  << column.uncompressed_buffer_len;
      }
    } else {
      uint8_t* uncompressed_data;
      RETURN_IF_FALSE(stream_->ReadBytes(
          column.buffer_len, &uncompressed_data, &parse_status_));
      memcpy(row_group_buffer_ + column.start_offset,
          uncompressed_data, column.buffer_len);
    }
  }
  return Status::OK();
}

Status HdfsRCFileScanner::ProcessRange(RowBatch* row_batch) {
  // HdfsRCFileScanner effectively does buffered IO, in that it reads all the
  // materialized columns into a row group buffer.
  // It will then materialize tuples from the row group buffer.  When the row
  // group is complete, it will move onto the next row group.
  if (row_pos_ == num_rows_) {
    // Finished materializing the current row group, read the next one.
    RETURN_IF_ERROR(StartRowGroup());
    if (num_rows_ == 0) {
      eos_ = true;
      return Status::OK();
    }
  }

  while (row_pos_ != num_rows_) {
    SCOPED_TIMER(scan_node_->materialize_tuple_timer());

    // Materialize rows from this row group in row batch sizes
    Tuple* tuple = tuple_;
    TupleRow* current_row = row_batch->GetRow(row_batch->AddRow());
    int max_tuples = row_batch->capacity() - row_batch->num_rows();
    max_tuples = min(max_tuples, num_rows_ - row_pos_);

    const vector<SlotDescriptor*>& materialized_slots =
        scan_node_->materialized_slots();
    if (materialized_slots.empty()) {
      // If there are no materialized slots (e.g. count(*) or just partition cols)
      // we can shortcircuit the parse loop
      row_pos_ += max_tuples;
      int num_to_commit = WriteTemplateTuples(current_row, max_tuples);
      COUNTER_ADD(scan_node_->rows_read_counter(), max_tuples);
      RETURN_IF_ERROR(CommitRows(num_to_commit, row_batch));
      if (row_batch->AtCapacity()) break;
      continue;
    }

    int num_to_commit = 0;
    for (int i = 0; i < max_tuples; ++i) {
      RETURN_IF_ERROR(NextRow());
      InitTuple(template_tuple_, tuple);

      bool error_in_row = false;
      for (const SlotDescriptor* slot_desc: materialized_slots) {
        int file_column_idx = slot_desc->col_pos() - scan_node_->num_partition_keys();

        // Set columns missing in this file to NULL
        if (file_column_idx >= columns_.size()) {
          tuple->SetNull(slot_desc->null_indicator_offset());
          continue;
        }

        const ColumnInfo& column = columns_[file_column_idx];
        DCHECK(column.materialize_column);

        const char* col_start = reinterpret_cast<const char*>(
            row_group_buffer_ + column.start_offset + column.buffer_pos);
        const int field_len = column.current_field_len;
        const char* row_group_end =
            reinterpret_cast<const char*>(row_group_buffer_ + row_group_length_);
        const char* col_end = col_start + field_len;
        if (col_end > row_group_end || column.start_offset < 0 || column.buffer_pos < 0
            || col_start > row_group_end || field_len < 0) {
          stringstream ss;
          ss << stream_->filename()
             << " Bad column index at offset : " << column.start_offset;
          return Status(ss.str());
        }

        if (!text_converter_->WriteSlot(slot_desc, tuple, col_start, field_len, false,
              false, row_batch->tuple_data_pool())) {
          ReportColumnParseError(slot_desc, col_start, field_len);
          error_in_row = true;
        }
      }

      if (error_in_row) {
        error_in_row = false;
        ErrorMsg msg(TErrorCode::GENERAL, Substitute("file: $0", stream_->filename()));
        RETURN_IF_ERROR(state_->LogOrReturnError(msg));
      }

      current_row->SetTuple(0, tuple);
      // Evaluate the conjuncts and add the row to the batch
      if (EvalConjuncts(current_row)) {
        ++num_to_commit;
        current_row = next_row(current_row);
        tuple = next_tuple(tuple_byte_size_, tuple);
      }
    }
    COUNTER_ADD(scan_node_->rows_read_counter(), max_tuples);
    RETURN_IF_ERROR(CommitRows(num_to_commit, row_batch));
    if (row_batch->AtCapacity() || scan_node_->ReachedLimitShared()) break;
  }

  if (row_pos_ == num_rows_) {
    // We are done with this row group, pass along external buffers if necessary.
    if (!reuse_row_group_buffer_) {
      row_batch->tuple_data_pool()->AcquireData(data_buffer_pool_.get(), false);
      row_group_buffer_size_ = 0;
    }

    // RCFiles don't end with syncs
    if (stream_->eof()) {
      eos_ = true;
      return Status::OK();
    }

    // Check for sync by looking for the marker that precedes syncs.
    int marker;
    RETURN_IF_FALSE(stream_->ReadInt(&marker, &parse_status_, /* peek */ true));
    if (marker == HdfsRCFileScanner::SYNC_MARKER) {
      RETURN_IF_FALSE(stream_->ReadInt(&marker, &parse_status_, /* peek */ false));
      RETURN_IF_ERROR(ReadSync());
    }
  }
  return Status::OK();
}
