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
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exec/buffered-byte-stream.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/serde-utils.inline.h"
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

const uint8_t HdfsRCFileScanner::RCFILE_VERSION_HEADER[4] = {'R', 'C', 'F', 1};

static const uint8_t RC_FILE_RECORD_DELIMITER = 0xff;

HdfsRCFileScanner::HdfsRCFileScanner(HdfsScanNode* scan_node, RuntimeState* state,
    MemPool* mem_pool)
    : HdfsScanner(scan_node, state, mem_pool),
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

  text_converter_.reset(new TextConverter(0));

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

Status HdfsRCFileScanner::Close() {
  return Status::OK;
}

Status HdfsRCFileScanner::InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition, 
    DiskIoMgr::ScanRange* scan_range, Tuple* template_tuple, 
    ByteStream* current_byte_stream_) {
  RETURN_IF_ERROR(HdfsScanner::InitCurrentScanRange(hdfs_partition, scan_range, 
      template_tuple, current_byte_stream_));
  end_of_scan_range_ = scan_range->len() + scan_range->offset();

  // Check the Location (file name) to see if we have changed files.
  // If this a new file then we need to read and process the header.
  if (previous_location_ != current_byte_stream_->GetLocation()) {
    RETURN_IF_ERROR(current_byte_stream_->Seek(0));
    RETURN_IF_ERROR(ReadFileHeader());
    // Save the current file name if we get the same file we can avoid
    // rereading the header information.
    previous_location_ = current_byte_stream_->GetLocation();
    // Save the seek offset of the end of the header information.  If we
    // get a scan range that starts at the beginning of the file we can avoid
    // reading the header but we must seek past it to get to the beginning of the data.
    current_byte_stream_->GetPosition(&header_end_);
  } else if (scan_range->offset() == 0) {
    // If are at the beginning of the file and we previously read the file header
    // we do not have to read it again but we need to seek past the file header.
    // We saved the offset above when we previously read and processed the header.
    RETURN_IF_ERROR(current_byte_stream_->Seek(header_end_));
  }

  // Offset may not point to row group boundary so we need to search for the next
  // sync block.
  if (scan_range->offset() != 0) {
    RETURN_IF_ERROR(current_byte_stream_->Seek(scan_range->offset()));
    RETURN_IF_ERROR(find_first_parser_->FindSyncBlock(end_of_scan_range_,
          SYNC_HASH_SIZE, &(sync_hash_[0]), current_byte_stream_));
  }
  int64_t position;
  RETURN_IF_ERROR(current_byte_stream_->GetPosition(&position));

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
  if (!memcmp(&head[0], HdfsSequenceScanner::SEQFILE_VERSION_HEADER, 
      sizeof(HdfsSequenceScanner::SEQFILE_VERSION_HEADER))) {
    version_ = SEQ6;
  } else if (!memcmp(&head[0], RCFILE_VERSION_HEADER, sizeof(RCFILE_VERSION_HEADER))) {
    version_ = RCF1;
  } else {
    stringstream ss;
    ss << "Invalid RCFILE_VERSION_HEADER: '"
       << SerDeUtils::HexDump(&head[0], sizeof(RCFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }
  
  if (version_ == SEQ6) {
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
  }

  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(current_byte_stream_, &is_compressed_));

  if (version_ == SEQ6) {
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
  }

  vector<char> codec;
  if (is_compressed_) {
    // Read the codec and get the right decompressor class.
    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &codec));
    string codec_str(&codec[0], codec.size());
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
        column_buffer_pool_.get(), !has_noncompact_strings_, codec_str, &decompressor_));
  }

  VLOG_FILE << current_byte_stream_->GetLocation() << ": "
            << (is_compressed_ ?  "block compressed" : "not compressed");
  if (is_compressed_) VLOG_FILE << string(&codec[0], codec.size());

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
      stringstream ss;
      ss  << "Bad sync hash in current HdfsRCFileScanner: "
          << current_byte_stream_->GetLocation() << "." << endl
          << "Expected: '"
          << SerDeUtils::HexDump(sync_hash_, HdfsRCFileScanner::SYNC_HASH_SIZE)
          << "'" << endl
          << "Actual:   '"
          << SerDeUtils::HexDump(hash, HdfsRCFileScanner::SYNC_HASH_SIZE)
          << "'" << endl;
      state_->LogError(ss.str());
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
  if (record_length < 0) {
    stringstream ss;
    int64_t position;
    current_byte_stream_->GetPosition(&position);
    position -= sizeof(int32_t);
    ss << "Bad record length: " << record_length << " in file: "
        << current_byte_stream_->GetLocation() << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &key_length_));
  if (key_length_ < 0) {
    stringstream ss;
    int64_t position;
    current_byte_stream_->GetPosition(&position);
    position -= sizeof(int32_t);
    ss << "Bad key length: " << key_length_ << " in file: "
        << current_byte_stream_->GetLocation() << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &compressed_key_length_));
  if (compressed_key_length_ < 0) {
    stringstream ss;
    int64_t position;
    current_byte_stream_->GetPosition(&position);
    position -= sizeof(int32_t);
    ss << "Bad compressed key length: " << compressed_key_length_ << " in file: "
        << current_byte_stream_->GetLocation() << " at offset: " << position;
    return Status(ss.str());
  }
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
        compressed_data_buffer_, &key_length_, &key_buffer_));
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

inline Status HdfsRCFileScanner::NextField(int col_idx) {
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
    if (bytes_read == -1) {
        int64_t position;
        current_byte_stream_->GetPosition(&position);
        stringstream ss;
        ss << "Invalid column length in file: "
           << current_byte_stream_->GetLocation() << " at offset: " << position;
        if (state_->LogHasSpace()) state_->LogError(ss.str());
        return Status(ss.str());
    }
    key_buf_pos_[col_idx] += bytes_read;

    if (length < 0) {
      // The repeat count is stored as the logical negation of the number of repetitions.
      // See the column-key-buffer comment in hdfs-rcfile-scanner.h.
      cur_field_length_rep_[col_idx] = ~length - 1;
    } else {
      cur_field_length_[col_idx] = length;
    }
  }
  return Status::OK;
}

inline Status HdfsRCFileScanner::NextRow(bool* eorg) {
  // TODO: Wrap this in an iterator and prevent people from alternating
  // calls to NextField()/NextRow()
  if (row_pos_ >= num_rows_) {
    *eorg = true;
    return Status::OK;
  }
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    if (ReadColumn(col_idx)) {
      RETURN_IF_ERROR(NextField(col_idx));
    }
  }
  ++row_pos_;
  *eorg = false;
  return Status::OK;
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
            compressed_data_buffer_, &col_buf_uncompressed_len_[col_idx],
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

    SCOPED_TIMER(scan_node_->materialize_tuple_timer());
    // Copy rows out of the current row group into the row_batch
    while (!scan_node_->ReachedLimit() && !row_batch->IsFull()) {
      bool eorg = false;
      RETURN_IF_ERROR(NextRow(&eorg));
      if (eorg) break;

      // Index into current row in row_batch.
      int row_idx = row_batch->AddRow();
      DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
      TupleRow* current_row = row_batch->GetRow(row_idx);
      current_row->SetTuple(scan_node_->tuple_idx(), tuple_);
      // Initialize tuple_ from the partition key template tuple before writing the
      // slots
      InitTuple(template_tuple_, tuple_);

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
              col_start, field_len, !has_noncompact_strings_, false, tuple_pool_)) {
          ReportColumnParseError(slot_desc, col_start, field_len);
          error_in_row = true;
        }
      }

      if (error_in_row) {
        error_in_row = false;
        if (state_->LogHasSpace()) {
          stringstream ss;
          ss << "file: " << current_byte_stream_->GetLocation();
          state_->LogError(ss.str());
        }
        if (state_->abort_on_error()) {
          state_->ReportFileErrors(current_byte_stream_->GetLocation(), 1);
          return Status(state_->ErrorLog());
        }
      }

      // Evaluate the conjuncts and add the row to the batch
      if (ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, current_row)) {
        row_batch->CommitLastRow();
        if (scan_node_->ReachedLimit() || row_batch->IsFull()) {
          tuple_ = NULL;
          return Status::OK;
        }
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_byte_size_;
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
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
  *out << "HdfsRCFileScanner(tupleid=" << scan_node_->tuple_idx() <<
    " file=" << current_byte_stream_->GetLocation();
  // TODO: Scanner::DebugString
  //  ExecNode::DebugString(indentation_level, out);
  *out << "])" << endl;
}
