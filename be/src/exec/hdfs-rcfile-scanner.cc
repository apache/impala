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

#include "exec/hdfs-rcfile-scanner.h"

#include <boost/algorithm/string.hpp>

#include "exec/hdfs-scan-node.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.inline.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/codec.h"

#include "gen-cpp/PlanNodes_types.h"

using namespace std;
using namespace boost;
using namespace impala;

const char* const HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer";

const char* const HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer";

const char* const HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS =
  "hive.io.rcfile.column.number";

const uint8_t HdfsRCFileScanner::RCFILE_VERSION_HEADER[4] = {'R', 'C', 'F', 1};

// Macro to convert between SerdeUtil errors to Status returns.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

HdfsRCFileScanner::HdfsRCFileScanner(HdfsScanNode* scan_node, RuntimeState* state)
    : BaseSequenceScanner(scan_node, state, /* marker_precedes_sync */ true) {
}

HdfsRCFileScanner::~HdfsRCFileScanner() {
}

Status HdfsRCFileScanner::Prepare() {
  RETURN_IF_ERROR(BaseSequenceScanner::Prepare());

  text_converter_.reset(new TextConverter(0));

  // Allocate the buffers for the key information that is used to read and decode
  // the column data.
  num_cols_ = scan_node_->num_cols() - scan_node_->num_partition_keys();
  columns_.resize(num_cols_);
  for (int i = 0; i < columns_.size(); ++i) {
    int col_idx = i + scan_node_->num_partition_keys();
    columns_[i].materialize_column =
        scan_node_->GetMaterializedSlotIdx(col_idx) != HdfsScanNode::SKIP_COLUMN;
  }
  return Status::OK;
}

Status HdfsRCFileScanner::InitNewRange() {
  DCHECK(header_ != NULL);
  
  only_parsing_header_ = false;
  
  row_group_buffer_size_ = 0;
  template_tuple_ = context_->template_tuple();

  if (header_->is_compressed) {
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
        data_buffer_pool_.get(), stream_->compact_data(),
        header_->codec, &decompressor_));
  } 
  
  // TODO: Initialize codegen fn here
  return Status::OK;
}

Status HdfsRCFileScanner::ReadFileHeader() {
  uint8_t* header;

  RcFileHeader* rc_header = reinterpret_cast<RcFileHeader*>(header_);
  // Validate file version
  RETURN_IF_FALSE(stream_->ReadBytes(
      sizeof(RCFILE_VERSION_HEADER), &header, &parse_status_));
  if (!memcmp(header, HdfsSequenceScanner::SEQFILE_VERSION_HEADER, 
      sizeof(HdfsSequenceScanner::SEQFILE_VERSION_HEADER))) {
    rc_header->version = SEQ6;
  } else if (!memcmp(header, RCFILE_VERSION_HEADER, sizeof(RCFILE_VERSION_HEADER))) {
    rc_header->version = RCF1;
  } else {
    stringstream ss;
    ss << "Invalid RCFILE_VERSION_HEADER: '"
       << SerDeUtils::HexDump(header, sizeof(RCFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }
  
  if (rc_header->version == SEQ6) {
    // Validate class name key/value
    uint8_t* class_name_key;
    int len;
    RETURN_IF_FALSE(
        stream_->ReadText(&class_name_key, &len, &parse_status_));
    if (len != strlen(HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME) ||
        memcmp(class_name_key, HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME, len)) {
      stringstream ss;
      ss << "Invalid RCFILE_KEY_CLASS_NAME: '"
         << string(reinterpret_cast<char*>(class_name_key), len)
         << "' len=" << len;
      return Status(ss.str());
    }

    uint8_t* class_name_val;
    RETURN_IF_FALSE(
        stream_->ReadText(&class_name_val, &len, &parse_status_));
    if (len != strlen(HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME) ||
        memcmp(class_name_val, HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME, len)) {
      stringstream ss;
      ss << "Invalid RCFILE_VALUE_CLASS_NAME: '"
         << string(reinterpret_cast<char*>(class_name_val), len)
         << "' len=" << len;
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
      ss << "RC files do no support block compression.";
      return Status(ss.str());
    }
  }
  if (header_->is_compressed) {
    uint8_t* codec_ptr;
    int len;
    // Read the codec and get the right decompressor class.
    RETURN_IF_FALSE(stream_->ReadText(&codec_ptr, &len, &parse_status_));
    header_->codec = string(reinterpret_cast<char*>(codec_ptr), len);
    Codec::CodecMap::const_iterator it = Codec::CODEC_MAP.find(header_->codec);
    DCHECK(it != Codec::CODEC_MAP.end());
    header_->compression_type = it->second;
  } else {
    header_->compression_type = THdfsCompression::NONE;
  }
  VLOG_FILE << stream_->filename() << ": "
            << (header_->is_compressed ?  "compressed" : "not compressed");
  if (header_->is_compressed) VLOG_FILE << header_->codec;

  // Read file metadata and verify the number of cols is correct
  RETURN_IF_ERROR(VerifyNumColumnsMetadata());

  // Read file sync marker
  uint8_t* sync;
  RETURN_IF_FALSE(stream_->ReadBytes(SYNC_HASH_SIZE, &sync, &parse_status_));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);
  
  header_->header_size = stream_->total_bytes_returned();
  return Status::OK;
}

Status HdfsRCFileScanner::VerifyNumColumnsMetadata() {
  int map_size = 0;
  RETURN_IF_FALSE(stream_->ReadInt(&map_size, &parse_status_));

  for (int i = 0; i < map_size; ++i) {
    uint8_t* key, *value;
    int key_len, value_len;
    RETURN_IF_FALSE(stream_->ReadText(&key, &key_len, &parse_status_));
    RETURN_IF_FALSE(stream_->ReadText(&value, &value_len, &parse_status_));

    if (key_len == strlen(RCFILE_METADATA_KEY_NUM_COLS) &&
        !memcmp(key, HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS, key_len)) {
      string tmp(reinterpret_cast<char*>(value), value_len);
      int file_num_cols = atoi(tmp.c_str());
      if (file_num_cols != num_cols_) {
        stringstream ss;
        ss << "Unexpected hive.io.rcfile.column.number value!"
           << " Expected: " << num_cols_ << "." << " Read: " << file_num_cols;
        return Status(ss.str());
      }
    }
  }
  return Status::OK;
}

BaseSequenceScanner::FileHeader* HdfsRCFileScanner::AllocateFileHeader() {
  return new RcFileHeader;
}

void HdfsRCFileScanner::ResetRowGroup() {
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

  if (!stream_->compact_data()) {
    // We are done with this row group, pass along non-compact external buffers
    context_->AcquirePool(data_buffer_pool_.get());
    row_group_buffer_size_ = 0;
  }
}

Status HdfsRCFileScanner::ReadRowGroup() {
  ResetRowGroup();

  while (num_rows_ == 0) {
    RETURN_IF_ERROR(ReadRowGroupHeader());
    RETURN_IF_ERROR(ReadKeyBuffers());
    if (stream_->compact_data() || row_group_buffer_size_ < row_group_length_) {
      // Allocate a new buffer for reading the row group.  Row groups have a
      // fixed number of rows so take a guess at how big it will be based on
      // the previous row group size.
      row_group_buffer_ = data_buffer_pool_->Allocate(row_group_length_);
      row_group_buffer_size_ = row_group_length_;
    }
    RETURN_IF_ERROR(ReadColumnBuffers());
    if (stream_->eosr()) {
      // We must read up to the next sync marker.
      int32_t record_length;
      bool read_success = stream_->ReadInt(&record_length, &parse_status_);
      if (!read_success) {
        // We are at the end of the file, nothing left
        break;
      }

      if (record_length == HdfsRCFileScanner::SYNC_MARKER) {
        // If the marker is there, it's an error not to have a Sync block following the
        // Marker.
        RETURN_IF_ERROR(ReadSync());
        break;
      } 
    }
  }
  return Status::OK;
}

Status HdfsRCFileScanner::ReadRowGroupHeader() {
  int32_t record_length;
  RETURN_IF_FALSE(stream_->ReadInt(&record_length, &parse_status_));
  // The sync block is marked with a record_length of -1.
  if (record_length == HdfsRCFileScanner::SYNC_MARKER) {
    RETURN_IF_ERROR(ReadSync());
    RETURN_IF_FALSE(stream_->ReadInt(&record_length, &parse_status_));
  }
  if (record_length < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad record length: " << record_length << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_FALSE(stream_->ReadInt(&key_length_, &parse_status_));
  if (key_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad key length: " << key_length_ << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_FALSE(stream_->ReadInt(&compressed_key_length_, &parse_status_));
  if (compressed_key_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad compressed key length: " << compressed_key_length_ 
       << " at offset: " << position;
    return Status(ss.str());
  }
  return Status::OK;
}

Status HdfsRCFileScanner::ReadKeyBuffers() {
  if (key_buffer_.size() < key_length_) key_buffer_.resize(key_length_);
  uint8_t* key_buffer = &key_buffer_[0];

  if (header_->is_compressed) {
    uint8_t* compressed_buffer;
    RETURN_IF_FALSE(stream_->ReadBytes(
        compressed_key_length_, &compressed_buffer, &parse_status_));
    RETURN_IF_ERROR(decompressor_->ProcessBlock(compressed_key_length_,
        compressed_buffer, &key_length_, &key_buffer));
  } else {
    uint8_t* buffer;
    RETURN_IF_FALSE(
        stream_->ReadBytes(key_length_, &buffer, &parse_status_));
    // Make a copy of this buffer.  The underlying IO buffer will get recycled
    memcpy(key_buffer, buffer, key_length_);
  }

  row_group_length_ = 0;
  uint8_t* key_buf_ptr = key_buffer;
  int bytes_read = SerDeUtils::GetVInt(key_buf_ptr, &num_rows_);
  key_buf_ptr += bytes_read;

  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    GetCurrentKeyBuffer(col_idx, !columns_[col_idx].materialize_column, &key_buf_ptr);
    DCHECK_LE(key_buf_ptr, key_buffer + key_length_);
  }
  DCHECK_EQ(key_buf_ptr, key_buffer + key_length_);

  return Status::OK;
}

void HdfsRCFileScanner::GetCurrentKeyBuffer(int col_idx, bool skip_col_data,
                                            uint8_t** key_buf_ptr) {
  ColumnInfo& col_info = columns_[col_idx];

  int bytes_read = SerDeUtils::GetVInt(*key_buf_ptr, &col_info.buffer_len);
  *key_buf_ptr += bytes_read;

  bytes_read = SerDeUtils::GetVInt(*key_buf_ptr, &col_info.uncompressed_buffer_len);
  *key_buf_ptr += bytes_read;

  int col_key_buf_len;
  bytes_read = SerDeUtils::GetVInt(*key_buf_ptr , &col_key_buf_len);
  *key_buf_ptr += bytes_read;

  if (!skip_col_data) {
    col_info.key_buffer = *key_buf_ptr;

    // Set the offset for the start of the data for this column in the allocated buffer.
    col_info.start_offset = row_group_length_;
    row_group_length_ += col_info.uncompressed_buffer_len;
  }
  *key_buf_ptr += col_key_buf_len;
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
    int bytes_read = SerDeUtils::GetVLong(col_key_buf, col_info.key_buffer_pos, &length);
    if (bytes_read == -1) {
        int64_t position = stream_->file_offset();
        stringstream ss;
        ss << "Invalid column length at offset: " << position;
        if (state_->LogHasSpace()) state_->LogError(ss.str());
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
  return Status::OK;
}

inline Status HdfsRCFileScanner::NextRow() {
  // TODO: Wrap this in an iterator and prevent people from alternating
  // calls to NextField()/NextRow()
  DCHECK_LT(row_pos_, num_rows_);
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    if (columns_[col_idx].materialize_column) {
      RETURN_IF_ERROR(NextField(col_idx));
    }
  }
  ++row_pos_;
  return Status::OK;
}

Status HdfsRCFileScanner::ReadColumnBuffers() {
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    ColumnInfo& column = columns_[col_idx];
    if (!columns_[col_idx].materialize_column) {
      // Not materializing this column, just skip it.
      RETURN_IF_FALSE(
          stream_->SkipBytes(column.buffer_len, &parse_status_));
      continue;
    } 
      
    // TODO: Stream through these column buffers instead of reading everything
    // in at once.
    DCHECK_LE(column.uncompressed_buffer_len + column.start_offset, row_group_length_);
    if (header_->is_compressed) {
      uint8_t* compressed_input;
      RETURN_IF_FALSE(stream_->ReadBytes(
          column.buffer_len, &compressed_input, &parse_status_));
      uint8_t* compressed_output = row_group_buffer_ + column.start_offset;
      RETURN_IF_ERROR(decompressor_->ProcessBlock(column.buffer_len,
          compressed_input, &column.uncompressed_buffer_len,
          &compressed_output));
    } else {
      uint8_t* uncompressed_data;
      RETURN_IF_FALSE(stream_->ReadBytes(
          column.buffer_len, &uncompressed_data, &parse_status_));
      // TODO: this is bad.  Remove this copy.
      memcpy(row_group_buffer_ + column.start_offset, 
          uncompressed_data, column.buffer_len);
    }
  }
  return Status::OK;
}

Status HdfsRCFileScanner::ProcessRange() {
  ResetRowGroup();

  // HdfsRCFileScanner effectively does buffered IO, in that it reads all the
  // materialized columns into a row group buffer.
  // It will then materialize tuples from the row group buffer.  When the row
  // group is complete, it will move onto the next row group.
  while (!stream_->eosr() || num_rows_ != row_pos_) {
    if (num_rows_ == row_pos_) {
      // Finished materializing this row group, read the next one.
      RETURN_IF_ERROR(ReadRowGroup());
      if (num_rows_ == 0) break;
    }
    
    SCOPED_TIMER(scan_node_->materialize_tuple_timer());
    
    // Indicates whether the current row has errors.
    bool error_in_row = false;
    const vector<SlotDescriptor*>& materialized_slots = 
        scan_node_->materialized_slots();
    vector<SlotDescriptor*>::const_iterator it;
    
    // Materialize rows from this row group in row batch sizes
    MemPool* pool;
    Tuple* tuple;
    TupleRow* current_row;
    int max_tuples = context_->GetMemory(&pool, &tuple, &current_row);
    max_tuples = min(max_tuples, num_rows_ - row_pos_);

    if (materialized_slots.empty()) {
      // If there are no materialized slots (e.g. count(*) or just partition cols)
      // we can shortcircuit the parse loop
      row_pos_ += max_tuples;
      int num_to_commit = WriteEmptyTuples(context_, current_row, max_tuples);
      if (num_to_commit > 0) context_->CommitRows(num_to_commit);
      continue;
    }

    int num_to_commit = 0;
    for (int i = 0; i < max_tuples; ++i) {
      RETURN_IF_ERROR(NextRow());

      // Initialize tuple from the partition key template tuple before writing the
      // slots
      InitTuple(template_tuple_, tuple);

      for (it = materialized_slots.begin(); it != materialized_slots.end(); ++it) {
        const SlotDescriptor* slot_desc = *it;
        int rc_column_idx = slot_desc->col_pos() - scan_node_->num_partition_keys();
        ColumnInfo& column = columns_[rc_column_idx];
        DCHECK(column.materialize_column);

        const char* col_start = reinterpret_cast<const char*>(
            row_group_buffer_ + column.start_offset + column.buffer_pos);
        int field_len = column.current_field_len;
        DCHECK_LE(col_start + field_len,
            reinterpret_cast<const char*>(row_group_buffer_ + row_group_length_));

        if (!text_converter_->WriteSlot(slot_desc, tuple, 
              col_start, field_len, stream_->compact_data(), false, pool)) {
          ReportColumnParseError(slot_desc, col_start, field_len);
          error_in_row = true;
        }
      }

      if (error_in_row) {
        error_in_row = false;
        if (state_->LogHasSpace()) {
          stringstream ss;
          ss << "file: " << stream_->filename();
          state_->LogError(ss.str());
        }
        if (state_->abort_on_error()) {
          state_->ReportFileErrors(stream_->filename(), 1);
          return Status(state_->ErrorLog());
        }
      }

      current_row->SetTuple(scan_node_->tuple_idx(), tuple);
      // Evaluate the conjuncts and add the row to the batch
      if (ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, current_row)) {
        ++num_to_commit;
        current_row = context_->next_row(current_row);
        tuple = context_->next_tuple(tuple);
      }
    }
    context_->CommitRows(num_to_commit);
    if (scan_node_->ReachedLimit()) break;
  }
  return Status::OK;
}

void HdfsRCFileScanner::DebugString(int indentation_level, stringstream* out) const {
  // TODO: Add more details of internal state.
  *out << string(indentation_level * 2, ' ')
       << "HdfsRCFileScanner(tupleid=" << scan_node_->tuple_idx() 
       << " file=" << stream_->filename();
  // TODO: Scanner::DebugString
  //  ExecNode::DebugString(indentation_level, out);
  *out << "])" << endl;
}
