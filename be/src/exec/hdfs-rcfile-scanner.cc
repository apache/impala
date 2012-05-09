// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <boost/algorithm/string.hpp>
#include "text-converter.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/string-value.h"
#include "util/runtime-profile.h"
#include "util/decompress.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/serde-utils.h"
#include "exec/text-converter.inline.h"

using namespace std;
using namespace boost;
using namespace impala;

const char* const HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer";

const char* const HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer";

const char* const HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS =
  "hive.io.rcfile.column.number";

const uint8_t HdfsRCFileScanner::RCFILE_VERSION_HEADER[4] = {'S', 'E', 'Q', 6};

static const uint8_t RC_FILE_RECORD_DELIMITER = 0xff;

HdfsRCFileScanner::HdfsRCFileScanner(HdfsScanNode* scan_node, RuntimeState* state,
    Tuple* template_tuple, MemPool* mem_pool)
    : HdfsScanner(scan_node, state, template_tuple, mem_pool),
      scan_range_fully_buffered_(false),
      key_buffer_pool_(new MemPool()),
      key_buffer_length_(0),
      compressed_data_buffer_length_(0),
      column_buffer_pool_(new MemPool()) {
  // Initialize the parser to find bytes that are 0xff.
  find_first_parser_.reset(new DelimitedTextParser(scan_node, RC_FILE_RECORD_DELIMITER));
}

HdfsRCFileScanner::~HdfsRCFileScanner() {
  // Collect the maximum amount of memory we used to process this file.
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      column_buffer_pool_->peak_allocated_bytes());
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      key_buffer_pool_->peak_allocated_bytes());
}

Status HdfsRCFileScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());

  text_converter_.reset(new TextConverter(0, tuple_pool_));


  // Allocate the buffers for the key information that is used to read and decode
  // the column data from its run length encoding.
  int num_part_keys = scan_node_->num_partition_keys();
  num_cols_ = scan_node_->num_cols() - num_part_keys;

  col_buf_len_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  col_buf_uncompressed_len_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  col_key_bufs_ = reinterpret_cast<uint8_t**>
      (key_buffer_pool_->Allocate(sizeof(uint8_t*) * num_cols_));

  col_key_bufs_len_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  col_bufs_off_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  key_buf_pos_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  cur_field_length_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  cur_field_length_rep_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  col_buf_pos_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  return Status::OK;
}

Status HdfsRCFileScanner::InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition, 
    HdfsScanRange* scan_range, Tuple* template_tuple, ByteStream* current_byte_stream_) {
  RETURN_IF_ERROR(HdfsScanner::InitCurrentScanRange(hdfs_partition, scan_range, 
      template_tuple, current_byte_stream_));
  end_of_scan_range_ = scan_range->length_ + scan_range->offset_;

  // Check the Location (file name) to see if we have changed files.
  // If this a new file then we need to read and process the header.
  if (previous_location_ != current_byte_stream_->GetLocation()) {
    RETURN_IF_ERROR(current_byte_stream_->Seek(0));
    RETURN_IF_ERROR(ReadFileHeader());
    previous_location_ = current_byte_stream_->GetLocation();
  }

  // Offset may not point to row group boundary so we need to search for the next
  // sync block.
  if (scan_range->offset_ != 0) {
    RETURN_IF_ERROR(current_byte_stream_->Seek(scan_range->offset_));
    {
      COUNTER_SCOPED_TIMER(scan_node_->parse_time_counter());
      RETURN_IF_ERROR(find_first_parser_->FindSyncBlock(end_of_scan_range_,
            SYNC_HASH_SIZE, &(sync_hash_[0]), current_byte_stream_));
    }
  }
  int64_t position;
  RETURN_IF_ERROR(current_byte_stream_->GetPosition(&position));
  row_group_idx_ = -1;

  ResetRowGroup();

  scan_range_fully_buffered_ = false;
  previous_total_length_ = 0;
  tuple_ = NULL;

  return Status::OK;
}

Status HdfsRCFileScanner::ReadFileHeader() {
  vector<uint8_t> head;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
      sizeof(RCFILE_VERSION_HEADER), &head));
  if (memcmp(&head[0], RCFILE_VERSION_HEADER, sizeof(RCFILE_VERSION_HEADER))) {
    stringstream ss;
    ss << "Invalid RCFILE_VERSION_HEADER: '"
       << SerDeUtils::HexDump(&head[0], sizeof(RCFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  vector<char> buf;
  RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &buf));
  if (strncmp(&buf[0], HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME,
      strlen(HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME))) {
    stringstream ss;
    ss << "Invalid RCFILE_KEY_CLASS_NAME: '"
       << string(&buf[0], strlen(HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME))
       << "'";
    return Status(ss.str());
  }

  RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &buf));
  if (strncmp(&buf[0], HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME,
      strlen(HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME))) {
    stringstream ss;
    ss << "Invalid RCFILE_VALUE_CLASS_NAME: '"
       << string(&buf[0], strlen(HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME))
       << "'";
    return Status(ss.str());
  }

  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(current_byte_stream_, &is_compressed_));

  // Read the is_blk_compressed header field. This field should *always*
  // be FALSE, and is the result of a defect in the original RCFile
  // implementation contained in Hive.
  bool is_blk_compressed;
  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(current_byte_stream_, &is_blk_compressed));
  if (is_blk_compressed) {
    stringstream ss;
    ss << "RC files do no support block compression, set in: '"
       << current_byte_stream_->GetLocation() << "'";
    return Status(ss.str());
  }

  vector<char> codec;
  if (is_compressed_) {
    // Read the codec and get the right decompressor class.
    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &codec));
    RETURN_IF_ERROR(Decompressor::CreateDecompressor(state_,
        column_buffer_pool_.get(), !has_noncompact_strings_, codec, &decompressor_));
  }

  VLOG(1) << current_byte_stream_->GetLocation() << ": "
      << (is_compressed_ ?  "block compressed" : "not compressed");
  if (is_compressed_) VLOG(1) << string(&codec[0], codec.size());

  RETURN_IF_ERROR(ReadFileHeaderMetadata());

  RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
      HdfsRCFileScanner::SYNC_HASH_SIZE, &sync_hash_[0]));
  return Status::OK;
}

Status HdfsRCFileScanner::ReadFileHeaderMetadata() {
  int map_size = 0;
  vector<char> key;
  vector<char> value;

  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &map_size));

  for (int i = 0; i < map_size; ++i) {
    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &key));
    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &value));

    if (!strncmp(&key[0], HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS,
        strlen(HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS))) {
      string tmp(&value[0], value.size());
      int file_num_cols = atoi(tmp.c_str());
      if (file_num_cols != num_cols_) {
        return Status("Unexpected hive.io.rcfile.column.number value!");
      }
    }
  }
  return Status::OK;
}

Status HdfsRCFileScanner::ReadSync() {
  uint8_t hash[HdfsRCFileScanner::SYNC_HASH_SIZE];
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
      HdfsRCFileScanner::SYNC_HASH_SIZE, &hash[0]));
  if (memcmp(&hash[0], &sync_hash_[0], HdfsRCFileScanner::SYNC_HASH_SIZE)) {
    if (state_->LogHasSpace()) {
      state_->error_stream() << "Bad sync hash in current HdfsRCFileScanner: "
           << current_byte_stream_->GetLocation() << "." << endl
           << "Expected: '"
           << SerDeUtils::HexDump(sync_hash_, HdfsRCFileScanner::SYNC_HASH_SIZE)
           << "'" << endl
           << "Actual:   '"
           << SerDeUtils::HexDump(hash, HdfsRCFileScanner::SYNC_HASH_SIZE)
           << "'" << endl;
    }
    return Status("bad sync hash block");
  }
  return Status::OK;
}

void HdfsRCFileScanner::ResetRowGroup() {
  num_rows_ = 0;
  row_pos_ = 0;
  key_length_ = 0;
  compressed_key_length_ = 0;

  memset(col_buf_len_, 0, num_cols_ * sizeof(int32_t));
  memset(col_buf_uncompressed_len_, 0, num_cols_ * sizeof(int32_t));
  memset(col_key_bufs_len_, 0, num_cols_ * sizeof(int32_t));

  memset(key_buf_pos_, 0, num_cols_ * sizeof(int32_t));
  memset(cur_field_length_, 0, num_cols_ * sizeof(int32_t));
  memset(cur_field_length_rep_, 0, num_cols_ * sizeof(int32_t));

  memset(col_buf_pos_, 0, num_cols_ * sizeof(int32_t));
}

Status HdfsRCFileScanner::ReadRowGroup() {
  ResetRowGroup();
  int64_t position;
  bool eof;

  COUNTER_SCOPED_TIMER(scan_node_->scanner_timer());
  row_group_idx_ = -1;
  while (num_rows_ == 0) {
    RETURN_IF_ERROR(ReadHeader());
    RETURN_IF_ERROR(ReadKeyBuffers());
    if (has_noncompact_strings_ || previous_total_length_ < total_col_length_) {
      column_buffer_ = column_buffer_pool_->Allocate(total_col_length_);
      previous_total_length_ = total_col_length_;
    }
    RETURN_IF_ERROR(ReadColumnBuffers());
    RETURN_IF_ERROR(current_byte_stream_->GetPosition(&position));
    if (position >= end_of_scan_range_) {
      RETURN_IF_ERROR(current_byte_stream_->Eof(&eof));
      if (eof) {
        scan_range_fully_buffered_ = true;
        break;
      }

      // We must read up to the next sync marker.
      int32_t record_length;
      RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &record_length));
      if (record_length == HdfsRCFileScanner::SYNC_MARKER) {
        // If the marker is there, it's an error not to have a Sync block following the
        // Marker.
        RETURN_IF_ERROR(ReadSync());
        scan_range_fully_buffered_ = true;
        break;
      } else {
        RETURN_IF_ERROR(current_byte_stream_->Seek(position));
      }
    }
  }
  return Status::OK;
}

Status HdfsRCFileScanner::ReadHeader() {
  int32_t record_length;
  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &record_length));
  // The sync block is marked with a record_length of -1.
  if (record_length == HdfsRCFileScanner::SYNC_MARKER) {
    RETURN_IF_ERROR(ReadSync());
    RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &record_length));
  }
  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &key_length_));
  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &compressed_key_length_));
  return Status::OK;
}

Status HdfsRCFileScanner::ReadKeyBuffers() {
  if (key_buffer_length_ < key_length_) {
    key_buffer_ = key_buffer_pool_->Allocate(key_length_);
    key_buffer_length_ = key_length_;
  }
  if (is_compressed_) {
    if (compressed_data_buffer_length_ < compressed_key_length_) {
      compressed_data_buffer_ = key_buffer_pool_->Allocate(compressed_key_length_);
      compressed_data_buffer_length_ = compressed_key_length_;
    }
    RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
        compressed_key_length_, compressed_data_buffer_));
    RETURN_IF_ERROR(decompressor_->ProcessBlock(compressed_key_length_,
        compressed_data_buffer_, key_length_, &key_buffer_));
  } else {
    RETURN_IF_ERROR(
        SerDeUtils::ReadBytes(current_byte_stream_, key_length_, key_buffer_));
  }

  total_col_length_ = 0;
  uint8_t* key_buf_ptr = key_buffer_;
  int bytes_read = SerDeUtils::GetVInt(key_buf_ptr, &num_rows_);
  key_buf_ptr += bytes_read;

  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    GetCurrentKeyBuffer(col_idx, !ReadColumn(col_idx), &key_buf_ptr);
    DCHECK_LE(key_buf_ptr, key_buffer_ + key_length_);
  }
  DCHECK_EQ(key_buf_ptr, key_buffer_ + key_length_);

  return Status::OK;
}

void HdfsRCFileScanner::GetCurrentKeyBuffer(int col_idx, bool skip_col_data,
                                            uint8_t** key_buf_ptr) {
  int bytes_read = SerDeUtils::GetVInt(*key_buf_ptr, &(col_buf_len_[col_idx]));
  *key_buf_ptr += bytes_read;

  bytes_read = SerDeUtils::GetVInt(*key_buf_ptr, &(col_buf_uncompressed_len_[col_idx]));
  *key_buf_ptr += bytes_read;

  int col_key_buf_len;
  bytes_read = SerDeUtils::GetVInt(*key_buf_ptr , &col_key_buf_len);
  *key_buf_ptr += bytes_read;

  if (!skip_col_data) {
    col_key_bufs_[col_idx] = *key_buf_ptr;

    // Set the offset for the start of the data for this column in the allocated buffer.
    col_bufs_off_[col_idx] = total_col_length_;
    total_col_length_ += col_buf_uncompressed_len_[col_idx];
  }
  *key_buf_ptr += col_key_buf_len;
}

inline void HdfsRCFileScanner::NextField(int col_idx) {
  col_buf_pos_[col_idx] += cur_field_length_[col_idx];

  if (cur_field_length_rep_[col_idx] > 0) {
    // repeat the previous length
    --cur_field_length_rep_[col_idx];
  } else {
    DCHECK_GE(cur_field_length_rep_[col_idx], 0);
    // Get the next column length or repeat count
    int64_t length = 0;
    uint8_t* col_key_buf = &col_key_bufs_[col_idx][0];
    int bytes_read = SerDeUtils::GetVLong(col_key_buf, key_buf_pos_[col_idx], &length);
    key_buf_pos_[col_idx] += bytes_read;

    if (length < 0) {
      // The repeat count is stored as the logical negation of the number of repetitions.
      // See the column-key-buffer comment in hdfs-rcfile-scanner.h.
      cur_field_length_rep_[col_idx] = ~length - 1;
    } else {
      cur_field_length_[col_idx] = length;
    }
  }
}

inline bool HdfsRCFileScanner::NextRow() {
  // TODO: Wrap this in an iterator and prevent people from alternating
  // calls to NextField()/NextRow()
  if (row_pos_ >= num_rows_) return false;
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    if (ReadColumn(col_idx)) {
      NextField(col_idx);
    }
  }
  ++row_pos_;
  return true;
}

Status HdfsRCFileScanner::ReadColumnBuffers() {
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    if (!ReadColumn(col_idx)) {
      RETURN_IF_ERROR(SerDeUtils::SkipBytes(current_byte_stream_, col_buf_len_[col_idx]));
    } else {
      // TODO: Stream through these column buffers instead of reading everything
      // in at once.
      DCHECK_LE(
          col_buf_uncompressed_len_[col_idx] + col_bufs_off_[col_idx], total_col_length_);
      if (is_compressed_) {
        if (compressed_data_buffer_length_ < col_buf_len_[col_idx]) {
          compressed_data_buffer_ = key_buffer_pool_->Allocate(col_buf_len_[col_idx]);
          compressed_data_buffer_length_ = col_buf_len_[col_idx];
        }
        RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
            col_buf_len_[col_idx], compressed_data_buffer_));
        uint8_t* compressed_output = column_buffer_ + col_bufs_off_[col_idx];
        RETURN_IF_ERROR(decompressor_->ProcessBlock(col_buf_len_[col_idx],
            compressed_data_buffer_, col_buf_uncompressed_len_[col_idx],
            &compressed_output));
      } else {
        RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
            col_buf_len_[col_idx], column_buffer_ + col_bufs_off_[col_idx]));
      }
    }
  }
  return Status::OK;
}

// TODO: We should be able to skip over badly fomrated data and move to the
//       next sync block to restart the scan.
Status HdfsRCFileScanner::GetNext(RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  // Indicates whether the current row has errors.
  bool error_in_row = false;

  if (scan_node_->ReachedLimit()) {
    tuple_ = NULL;
    *eosr = true;
    return Status::OK;
  }

  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;

  TupleRow* current_row = NULL;

  // HdfsRCFileScanner effectively does buffered IO, in that it reads the
  // rcfile into a column buffer, and sets an eof marker at that
  // point, before the buffer is drained.
  // In the following loop, eosr tracks the end of stream flag that
  // tells the scan node to move onto the next scan range.
  // scan_range_fully_buffered_ is set once the RC file has been fully
  // read into RCRowGroups.
  while (!scan_node_->ReachedLimit() && !row_batch->IsFull() && !(*eosr)) {
    if (num_rows_ == row_pos_) {
      // scan_range_fully_buffered_ is set iff the scan range has been
      // exhausted due to a previous read.
      if (scan_range_fully_buffered_) {
        *eosr = true;
        break;
      }
      RETURN_IF_ERROR(ReadRowGroup());

      if (num_rows_ == 0) break;
    }

    // Copy rows out of the current row group into the row_batch
    while (!scan_node_->ReachedLimit() && !row_batch->IsFull() && NextRow()) {
      if (row_idx == RowBatch::INVALID_ROW_INDEX) {
        row_idx = row_batch->AddRow();
        current_row = row_batch->GetRow(row_idx);
        current_row->SetTuple(0, tuple_);
        // Initialize tuple_ from the partition key template tuple before writing the
        // slots
        InitTuple(tuple_);
      }

      const vector<SlotDescriptor*>& materialized_slots = 
          scan_node_->materialized_slots();
      vector<SlotDescriptor*>::const_iterator it;

      for (it = materialized_slots.begin(); it != materialized_slots.end(); ++it) {
        const SlotDescriptor* slot_desc = *it;
        int rc_column_idx = slot_desc->col_pos() - scan_node_->num_partition_keys();

        const char* col_start = reinterpret_cast<const char*>(&((column_buffer_ +
            col_bufs_off_[rc_column_idx])[col_buf_pos_[rc_column_idx]]));
        int field_len = cur_field_length_[rc_column_idx];
        DCHECK_LE(col_start + field_len,
            reinterpret_cast<const char*>(column_buffer_ + total_col_length_));

        if (!text_converter_->WriteSlot(slot_desc, tuple_, 
              col_start, field_len, !has_noncompact_strings_, false)) {
          if (state_->LogHasSpace()) {
            state_->error_stream() << "Error converting column: " << rc_column_idx <<
                " TO " << TypeToString(slot_desc->type()) << endl;
          }
          error_in_row = true;
        }
      }

      if (error_in_row) {
        error_in_row = false;
        if (state_->LogHasSpace()) {
          state_->error_stream() << "file: " <<
            current_byte_stream_->GetLocation() << endl;
          state_->error_stream() << "row group: " << row_group_idx_ << endl;
          state_->error_stream() << "row index: " << row_idx;
          state_->LogErrorStream();
        }
        if (state_->abort_on_error()) {
          state_->ReportFileErrors(current_byte_stream_->GetLocation(), 1);
          return Status(
              "Aborted HdfsRCFileScanner due to parse errors. View error log for "
              "details.");
        }
      }

      // Evaluate the conjuncts and add the row to the batch
      bool conjuncts_true = scan_node_->EvalConjunctsForScanner(current_row);

      if (conjuncts_true) {
        row_batch->CommitLastRow();
        row_idx = RowBatch::INVALID_ROW_INDEX;
        scan_node_->IncrNumRowsReturned();
        if (scan_node_->ReachedLimit() || row_batch->IsFull()) {
          tuple_ = NULL;
          return Status::OK;
        }
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_byte_size_;
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
      }

      // Need to reset the tuple_ if
      //  1. eval failed (clear out null-indicator bits) OR
      //  2. there are partition keys that need to be copied
      // TODO: if the slots that need to be updated are very sparse (very few NULL slots
      // or very few partition keys), updating all the tuple memory is probably bad
      if (!conjuncts_true || template_tuple_ != NULL) {
        InitTuple(tuple_);
      }
    }
  }

  // Note that number of rows remaining == 0 -> scan_range_fully_buffered_
  // at this point, hence we don't need to explicitly check that
  // scan_range_fully_buffered_ == true.
  if (scan_node_->ReachedLimit() || num_rows_ == 0 || num_rows_ == row_pos_) {
    // We reached the limit, drained the row group or hit the end of the table.
    // No more work to be done. Clean up all pools with the last row batch.
    *eosr = true;
  } else {
    DCHECK(row_batch->IsFull());
    // The current row_batch is full, but we haven't yet reached our limit.
    // Hang on to the last chunks. We'll continue from there in the next
    // call to GetNext().
    *eosr = false;
  }
  // Maintian ownership of last memory chunk if not at the end of the scan range.
  if (has_noncompact_strings_) {
    row_batch->tuple_data_pool()->AcquireData(column_buffer_pool_.get(), !*eosr);
  }
  row_batch->tuple_data_pool()->AcquireData(tuple_pool_, !*eosr);

  return Status::OK;
}

void HdfsRCFileScanner::DebugString(int indentation_level, stringstream* out) const {
  // TODO: Add more details of internal state.
  *out << string(indentation_level * 2, ' ');
  *out << "HdfsRCFileScanner(tupleid=" << tuple_idx_ <<
    " file=" << current_byte_stream_->GetLocation();
  // TODO: Scanner::DebugString
  //  ExecNode::DebugString(indentation_level, out);
  *out << "])" << endl;
}
