// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/rcfile-reader.h"

#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <cstring>
#include <sstream>
#include <stdint.h>
#include <glog/logging.h>

#include "common/status.h"
#include "exec/hdfs-scan-node.h"

using namespace std;
using namespace impala;

RCFileRowGroup::RCFileRowGroup(const std::vector<bool>& column_read_mask)
  : column_read_mask_(column_read_mask),
    sync_hash_(NULL),
    num_cols_(0),
    is_compressed_(false),
    num_rows_(0),
    row_pos_(0),
    record_length_(0),
    key_length_(0),
    compressed_key_length_(0) {
  num_cols_ = column_read_mask_.size();

  col_buf_len_.reserve(num_cols_);
  col_buf_len_.resize(num_cols_);

  col_buf_uncompressed_len_.reserve(num_cols_);
  col_buf_uncompressed_len_.resize(num_cols_);

  col_key_bufs_.reserve(num_cols_);
  col_key_bufs_.resize(num_cols_);

  col_bufs_.reserve(num_cols_);
  col_bufs_.resize(num_cols_);

  key_buf_pos_.reserve(num_cols_);
  key_buf_pos_.resize(num_cols_);

  cur_field_length_.reserve(num_cols_);
  cur_field_length_.resize(num_cols_);

  cur_field_length_rep_.reserve(num_cols_);
  cur_field_length_rep_.resize(num_cols_);

  col_buf_pos_.reserve(num_cols_);
  col_buf_pos_.resize(num_cols_);
}

void RCFileRowGroup::SetSyncHash(const std::vector<uint8_t>* sync_hash) {
  sync_hash_ = sync_hash;
}

Status RCFileRowGroup::ReadHeader(ByteStream* byte_stream) {
  RETURN_IF_ERROR(SerDeUtils::ReadInt(byte_stream, &record_length_));
  if (record_length_ == RCFileReader::SYNC_MARKER) {
    RETURN_IF_ERROR(ReadSync(byte_stream));
    RETURN_IF_ERROR(SerDeUtils::ReadInt(byte_stream, &record_length_));
  }
  RETURN_IF_ERROR(SerDeUtils::ReadInt(byte_stream, &key_length_));
  RETURN_IF_ERROR(SerDeUtils::ReadInt(byte_stream, &compressed_key_length_));
  RETURN_IF_ERROR(SerDeUtils::ReadVInt(byte_stream, &num_rows_));
  return Status::OK;
}

Status RCFileRowGroup::ReadSync(ByteStream* byte_stream) {
  vector<uint8_t> hash;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream, RCFileReader::SYNC_HASH_SIZE,
                                        &hash));
  if (sync_hash_ == NULL) {
    return Status("Sync hash has not been set for this RowGroup!");
  }
  if (!memcmp((void*)hash[0], sync_hash_, RCFileReader::SYNC_HASH_SIZE)) {
    std::stringstream ss;
    // TODO: Also print file name
    ss << "Bad sync hash in current RowGroup!" << endl;
    ss << "Expected: '"
       << SerDeUtils::HexDump(&((*sync_hash_)[0]), RCFileReader::SYNC_HASH_SIZE)
       << "'" << endl;
    ss << "Actual:   '"
       << SerDeUtils::HexDump(&(hash[0]), RCFileReader::SYNC_HASH_SIZE)
       << "'" << endl;
    return Status(ss.str());
  }
  return Status::OK;
}

Status RCFileRowGroup::ReadKeyBuffers(ByteStream* byte_stream) {
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    RETURN_IF_ERROR(ReadCurrentKeyBuffer(byte_stream, col_idx,
                                         !column_read_mask_[col_idx]));
  }
  return Status::OK;
}

Status RCFileRowGroup::ReadCurrentKeyBuffer(ByteStream* byte_stream, int col_idx,
                                            bool skip_col_data) {
  int col_key_buf_len;
  RETURN_IF_ERROR(SerDeUtils::ReadVInt(byte_stream, &(col_buf_len_[col_idx])));
  RETURN_IF_ERROR(SerDeUtils::ReadVInt(byte_stream,
                                       &(col_buf_uncompressed_len_[col_idx])));
  RETURN_IF_ERROR(SerDeUtils::ReadVInt(byte_stream, &col_key_buf_len));
  if (skip_col_data) {
    // TODO: Figure out why this call to SkipBytes is causing a SIGSEGV in JNI
    //RETURN_IF_ERROR(SerDeUtils::SkipBytes(byte_stream, col_key_buf_len));
    col_key_bufs_[col_idx].resize(col_key_buf_len);
    RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream, col_key_buf_len,
                                          &(col_key_bufs_[col_idx])));
  } else {
    // TODO: Stream through these key buffers instead of reading everything in at once.
    col_key_bufs_[col_idx].resize(col_key_buf_len);
    RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream, col_key_buf_len,
                                          &(col_key_bufs_[col_idx])));
  }
  return Status::OK;
}

Status RCFileRowGroup::ReadColumnBuffers(ByteStream* byte_stream) {
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    RETURN_IF_ERROR(ReadCurrentColumnBuffer(byte_stream, col_idx,
                                            !column_read_mask_[col_idx]));
  }
  return Status::OK;
}

Status RCFileRowGroup::ReadCurrentColumnBuffer(ByteStream* byte_stream, int col_idx,
                                               bool skip_col_data) {
  if (skip_col_data) {
    return SerDeUtils::SkipBytes(byte_stream, col_buf_len_[col_idx]);
  } else {
    // TODO: Stream through these column buffers instead of reading everything in at once.
    return SerDeUtils::ReadBytes(byte_stream, col_buf_len_[col_idx],
                                 &(col_bufs_[col_idx]));
  }
}

Status RCFileRowGroup::ReadNext(ByteStream* byte_stream) {
  num_rows_ = 0;
  row_pos_ = 0;
  RETURN_IF_ERROR(ReadHeader(byte_stream));
  RETURN_IF_ERROR(ReadKeyBuffers(byte_stream));
  RETURN_IF_ERROR(ReadColumnBuffers(byte_stream));
  return Status::OK;
}

void RCFileRowGroup::Reset() {
  num_rows_ = 0;
  row_pos_ = 0;
  record_length_ = 0;
  key_length_ = 0;
  compressed_key_length_ = 0;

  col_buf_len_.assign(col_buf_len_.size(), 0);
  col_buf_uncompressed_len_.assign(col_buf_uncompressed_len_.size(), 0);

  for (int i = 0; i < col_key_bufs_.size(); ++i) {
    col_key_bufs_[i].resize(0);
  }
  key_buf_pos_.assign(key_buf_pos_.size(), 0);
  cur_field_length_.assign(cur_field_length_.size(), 0);
  cur_field_length_rep_.assign(cur_field_length_rep_.size(), 0);

  for (int i = 0; i < col_bufs_.size(); ++i) {
    col_bufs_[i].resize(0);
  }
  col_buf_pos_.assign(col_buf_pos_.size(), 0);
}

int RCFileRowGroup::NumRowsRemaining() {
  return num_rows_ - row_pos_;
}

void RCFileRowGroup::NextField(int col_idx) {
  col_buf_pos_[col_idx] += cur_field_length_[col_idx];

  if (cur_field_length_rep_[col_idx] > 0) {
    // repeat the previous length
    --cur_field_length_rep_[col_idx];
  } else {
    DCHECK_GE(cur_field_length_rep_[col_idx], 0);
    // Get the next column length or repeat count
    int64_t length = 0;
    vector<uint8_t>* col_key_buf = &col_key_bufs_[col_idx];
    int bytes_read = SerDeUtils::GetVLong(&(*col_key_buf)[0],
                                           key_buf_pos_[col_idx],
                                           &length);
    key_buf_pos_[col_idx] += bytes_read;

    if (length < 0) {
      cur_field_length_rep_[col_idx] = -length - 2;
    } else {
      cur_field_length_[col_idx] = length;
    }
  }
}

bool RCFileRowGroup::NextRow(void) {
  // TODO: Wrap this in an iterator and prevent people from alternating
  // calls to NextField()/NextRow()
  if (row_pos_ >= num_rows_) return false;
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    if (column_read_mask_[col_idx]) {
      NextField(col_idx);
    }
  }
  ++row_pos_;
  return true;
}

int RCFileRowGroup::GetFieldLength(int col_id) {
  return cur_field_length_[col_id];
}

const uint8_t* RCFileRowGroup::GetFieldPtr(int col_id) {
  return &col_bufs_[col_id][col_buf_pos_[col_id]];
}

const char* const RCFileReader::RCFILE_KEY_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer";

const char* const RCFileReader::RCFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer";

const char* const RCFileReader::RCFILE_METADATA_KEY_NUM_COLS =
  "hive.io.rcfile.column.number";

const uint8_t RCFileReader::RCFILE_VERSION_HEADER[4] = {'S', 'E', 'Q', 6};


RCFileReader::RCFileReader(const std::vector<bool>& column_read_mask,
                           ByteStream* byte_stream)
  : byte_stream_(byte_stream),
    column_read_mask_(column_read_mask),
    row_group_idx_(-1),
    num_cols_(column_read_mask.size()) {
}

RCFileRowGroup* RCFileReader::NewRCFileRowGroup() {
  return new RCFileRowGroup(column_read_mask_);
}

Status RCFileReader::ReadFileHeader() {
  vector<uint8_t> buf;

  RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream_, sizeof(RCFILE_VERSION_HEADER),
                                        &buf));
  if (memcmp(&buf[0], RCFILE_VERSION_HEADER, sizeof(RCFILE_VERSION_HEADER))) {
    std::stringstream ss;
    ss << "Invalid RCFILE_VERSION_HEADER: '"
       << SerDeUtils::HexDump(&buf[0], sizeof(RCFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  vector<char> name;
  RETURN_IF_ERROR(SerDeUtils::ReadText(byte_stream_, &name));
  if (strncmp(&name[0], RCFileReader::RCFILE_KEY_CLASS_NAME,
              strlen(RCFileReader::RCFILE_KEY_CLASS_NAME))) {
    std::stringstream ss;
    ss << "Invalid RCFILE_KEY_CLASS_NAME: '"
       << std::string(&name[0], strlen(RCFileReader::RCFILE_KEY_CLASS_NAME))
       << "'";
    return Status(ss.str());
  }

  RETURN_IF_ERROR(SerDeUtils::ReadText(byte_stream_, &name));
  if (strncmp(&name[0], RCFileReader::RCFILE_VALUE_CLASS_NAME,
              strlen(RCFileReader::RCFILE_VALUE_CLASS_NAME))) {
    std::stringstream ss;
    ss << "Invalid RCFILE_VALUE_CLASS_NAME: '"
       << std::string(&name[0], strlen(RCFileReader::RCFILE_VALUE_CLASS_NAME))
       << "'";
    return Status(ss.str());
  }

  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(byte_stream_, &is_compressed_));

  // Read the is_blk_compressed header field. This field should *always*
  // be FALSE, and is the result of a defect in the original RCFile
  // implementation contained in Hive.
  bool is_blk_compressed;
  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(byte_stream_, &is_blk_compressed));
  if (is_blk_compressed) {
    std::stringstream ss;
    ss << "Encountered is_blk_compressed=TRUE in file '"
       << byte_stream_->GetLocation() << "'";
    return Status(ss.str());
  }

  if (is_compressed_) {
    RETURN_IF_ERROR(SerDeUtils::ReadText(byte_stream_, &compression_codec_));
    return Status("Compressed RCFiles are not currently supported!");
  }

  RETURN_IF_ERROR(ReadFileHeaderMetadata());
  RETURN_IF_ERROR(ReadSync());
  return Status::OK;
}

Status RCFileReader::ReadFileHeaderMetadata() {
  int map_size = 0;
  vector<char> key;
  vector<char> value;

  RETURN_IF_ERROR(SerDeUtils::ReadInt(byte_stream_, &map_size));

  for (int i = 0; i < map_size; ++i) {
    RETURN_IF_ERROR(SerDeUtils::ReadText(byte_stream_, &key));
    RETURN_IF_ERROR(SerDeUtils::ReadText(byte_stream_, &value));

    if (!strncmp(&key[0], RCFileReader::RCFILE_METADATA_KEY_NUM_COLS,
                 strlen(RCFileReader::RCFILE_METADATA_KEY_NUM_COLS))) {
      string tmp(&value[0], value.size());
      int file_num_cols = atoi(tmp.c_str());
      if (file_num_cols != num_cols_) {
        return Status("Unexpected hive.io.rcfile.column.number value!");
      }
    }
  }
  return Status::OK;
}

Status RCFileReader::ReadSync() {
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream_, SYNC_HASH_SIZE, &sync_));
  return Status::OK;
}

Status RCFileReader::InitCurrentScanRange(HdfsScanRange* scan_range) {
  byte_stream_->Seek(0L);
  RETURN_IF_ERROR(ReadFileHeader());
  // TODO: Respect scan range start / end?
  //  RETURN_IF_ERROR(byte_stream_->Seek(scan_range->offset));
  return Status::OK;
}

RCFileReader::~RCFileReader() {
}

Status RCFileReader::ReadRowGroup(RCFileRowGroup* row_group, HdfsScanRange* scan_range,
                                  bool* eosr) {
  row_group->Reset();
  row_group->SetSyncHash(&sync_);

  row_group_idx_ = -1;
  while (row_group->num_rows() == 0) {
    RETURN_IF_ERROR(row_group->ReadNext(byte_stream_));
    int64_t position;
    RETURN_IF_ERROR(byte_stream_->GetPosition(&position));
    if (position >= scan_range->length) {
      *eosr = true;
      return Status::OK;
    }
  }
  return Status::OK;
}
