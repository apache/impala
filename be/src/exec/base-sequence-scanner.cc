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

#include <boost/bind.hpp>

#include "exec/base-sequence-scanner.h"

#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-search.h"
#include "util/codec.h"

#include "common/names.h"

using namespace impala;

const int BaseSequenceScanner::HEADER_SIZE = 1024;
const int BaseSequenceScanner::SYNC_MARKER = -1;

// Constants used in ReadPastSize()
static const double BLOCK_SIZE_PADDING_PERCENT = 0.1;
static const int REMAINING_BLOCK_SIZE_GUESS = 100 * 1024; // bytes
static const int MIN_SYNC_READ_SIZE = 10 * 1024; // bytes

// Macro to convert between SerdeUtil errors to Status returns.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

Status BaseSequenceScanner::IssueInitialRanges(HdfsScanNode* scan_node,
    const vector<HdfsFileDesc*>& files) {
  // Issue just the header range for each file.  When the header is complete,
  // we'll issue the splits for that file.  Splits cannot be processed until the
  // header is parsed (the header object is then shared across splits for that file).
  vector<DiskIoMgr::ScanRange*> header_ranges;
  for (int i = 0; i < files.size(); ++i) {
    ScanRangeMetadata* metadata =
        reinterpret_cast<ScanRangeMetadata*>(files[i]->splits[0]->meta_data());
    int64_t header_size = min<int64_t>(HEADER_SIZE, files[i]->file_length);
    // The header is almost always a remote read. Set the disk id to -1 and indicate
    // it is not cached.
    // TODO: add remote disk id and plumb that through to the io mgr.  It should have
    // 1 queue for each NIC as well?
    DiskIoMgr::ScanRange* header_range = scan_node->AllocateScanRange(
        files[i]->fs, files[i]->filename.c_str(), header_size, 0, metadata->partition_id,
        -1, false, false, files[i]->mtime);
    header_ranges.push_back(header_range);
  }
  // Issue the header ranges only.  ProcessSplit() will issue the files' scan ranges
  // and those ranges will need scanner threads, so no files are marked completed yet.
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(header_ranges, 0));
  return Status::OK();
}

BaseSequenceScanner::BaseSequenceScanner(HdfsScanNode* node, RuntimeState* state)
  : HdfsScanner(node, state),
    header_(NULL),
    block_start_(0),
    total_block_size_(0),
    num_syncs_(0) {
}

BaseSequenceScanner::~BaseSequenceScanner() {
}

Status BaseSequenceScanner::Prepare(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(context));
  stream_->set_read_past_size_cb(bind(&BaseSequenceScanner::ReadPastSize, this, _1));
  bytes_skipped_counter_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "BytesSkipped", TUnit::BYTES);
  return Status::OK();
}

void BaseSequenceScanner::Close() {
  VLOG_FILE << "Bytes read past scan range: " << -stream_->bytes_left();
  VLOG_FILE << "Average block size: "
            << (num_syncs_ > 1 ? total_block_size_ / (num_syncs_ - 1) : 0);
  // Need to close the decompressor before releasing the resources at AddFinalRowBatch(),
  // because in some cases there is memory allocated in decompressor_'s temp_memory_pool_.
  if (decompressor_.get() != NULL) {
    decompressor_->Close();
    decompressor_.reset(NULL);
  }
  AttachPool(data_buffer_pool_.get(), false);
  AddFinalRowBatch();
  if (!only_parsing_header_) {
    scan_node_->RangeComplete(file_format(), header_->compression_type);
  }
  HdfsScanner::Close();
}

Status BaseSequenceScanner::ProcessSplit() {
  header_ = reinterpret_cast<FileHeader*>(
      scan_node_->GetFileMetadata(stream_->filename()));
  if (header_ == NULL) {
    // This is the initial scan range just to parse the header
    only_parsing_header_ = true;
    header_ = state_->obj_pool()->Add(AllocateFileHeader());
    Status status = ReadFileHeader();
    if (!status.ok()) {
      if (state_->abort_on_error()) return status;
      state_->LogError(status.msg());
      // We need to complete the ranges for this file.
      CloseFileRanges(stream_->filename());
      return Status::OK();
    }

    // Header is parsed, set the metadata in the scan node and issue more ranges
    scan_node_->SetFileMetadata(stream_->filename(), header_);
    HdfsFileDesc* desc = scan_node_->GetFileDesc(stream_->filename());
    scan_node_->AddDiskIoRanges(desc);
    return Status::OK();
  }

  // Initialize state for new scan range
  finished_ = false;
  // If the file is compressed, the buffers in the stream_ are not used directly.
  if (header_->is_compressed) stream_->set_contains_tuple_data(false);
  RETURN_IF_ERROR(InitNewRange());

  Status status = Status::OK();

  // Skip to the first record
  if (stream_->file_offset() < header_->header_size) {
    // If the scan range starts within the header, skip to the end of the header so we
    // don't accidentally skip to an extra sync within the header
    RETURN_IF_FALSE(stream_->SkipBytes(
        header_->header_size - stream_->file_offset(), &parse_status_));
  }
  RETURN_IF_ERROR(SkipToSync(header_->sync, SYNC_HASH_SIZE));

  // Process Range.
  while (!finished_) {
    status = ProcessRange();
    if (status.ok()) break;
    if (status.IsCancelled() || status.IsMemLimitExceeded()) return status;

    // Log error from file format parsing.
    state_->LogError(ErrorMsg(TErrorCode::SEQUENCE_SCANNER_PARSE_ERROR,
        stream_->filename(), stream_->file_offset(),
        (stream_->eof() ? "(EOF)" : "")));

    // Make sure errors specified in the status are logged as well
    state_->LogError(status.msg());

    // If abort on error then return, otherwise try to recover.
    if (state_->abort_on_error()) return status;

    // Recover by skipping to the next sync.
    parse_status_ = Status::OK();
    int64_t error_offset = stream_->file_offset();
    status = SkipToSync(header_->sync, SYNC_HASH_SIZE);
    COUNTER_ADD(bytes_skipped_counter_, stream_->file_offset() - error_offset);
    RETURN_IF_ERROR(status);
    DCHECK(parse_status_.ok());
  }

  // All done with this scan range.
  return Status::OK();
}

Status BaseSequenceScanner::ReadSync() {
  // We are finished when we read a sync marker occurring completely in the next
  // scan range
  finished_ = stream_->eosr();

  uint8_t* hash;
  int64_t out_len;
  RETURN_IF_FALSE(stream_->GetBytes(SYNC_HASH_SIZE, &hash, &out_len, &parse_status_));
  if (out_len != SYNC_HASH_SIZE || memcmp(hash, header_->sync, SYNC_HASH_SIZE)) {
    stringstream ss;
    ss  << "Bad synchronization marker" << endl
        << "  Expected: '"
        << ReadWriteUtil::HexDump(header_->sync, SYNC_HASH_SIZE) << "'" << endl
        << "  Actual:   '"
        << ReadWriteUtil::HexDump(hash, SYNC_HASH_SIZE) << "'";
    return Status(ss.str());
  }
  finished_ |= stream_->eof();
  total_block_size_ += stream_->file_offset() - block_start_;
  block_start_ = stream_->file_offset();
  ++num_syncs_;
  return Status::OK();
}

int BaseSequenceScanner::FindSyncBlock(const uint8_t* buffer, int buffer_len,
                                       const uint8_t* sync, int sync_len) {
  char* sync_str = reinterpret_cast<char*>(const_cast<uint8_t*>(sync));
  StringValue needle = StringValue(sync_str, sync_len);

  StringValue haystack(
      const_cast<char*>(reinterpret_cast<const char*>(buffer)), buffer_len);

  StringSearch search(&needle);
  int offset = search.Search(&haystack);

  if (offset != -1) {
    // Advance offset past sync
    offset += sync_len;
  }
  return offset;
}

Status BaseSequenceScanner::SkipToSync(const uint8_t* sync, int sync_size) {
  // offset into current buffer of end of sync (once found, -1 until then)
  int offset = -1;
  uint8_t* buffer;
  int64_t buffer_len;
  Status status;

  // Read buffers until we find a sync or reach the end of the scan range. If we read all
  // the buffers remaining in the scan range and none of them contain a sync (including a
  // sync that starts at the end of this scan range and continues into the next one), then
  // there are no more syncs in this scan range and we're finished.
  while (!stream_->eosr()) {
    // Check if there's a sync fully contained in the current buffer
    RETURN_IF_ERROR(stream_->GetBuffer(true, &buffer, &buffer_len));
    offset = FindSyncBlock(buffer, buffer_len, sync, sync_size);
    DCHECK_LE(offset, buffer_len);
    if (offset != -1) break;

    // No sync found in the current buffer, so check if there's a sync spanning the
    // current buffer and the next. First we skip so there are sync_size - 1 bytes left,
    // then we read these bytes plus the first sync_size - 1 bytes of the next buffer.
    // This guarantees that we find any syncs that start in the current buffer and end in
    // the next buffer.
    int64_t to_skip = max<int64_t>(0, buffer_len - (sync_size - 1));
    RETURN_IF_FALSE(stream_->SkipBytes(to_skip, &parse_status_));
    // Peek so we don't advance stream_ into the next buffer. If we don't find a sync here
    // then we'll need to check all of the next buffer, including the first sync_size -1
    // bytes.
    RETURN_IF_FALSE(stream_->GetBytes(
        (sync_size - 1) * 2, &buffer, &buffer_len, &parse_status_, true));
    offset = FindSyncBlock(buffer, buffer_len, sync, sync_size);
    DCHECK_LE(offset, buffer_len);
    if (offset != -1) break;

    // No sync starting in this buffer, so advance stream_ to the beginning of the next
    // buffer.
    RETURN_IF_ERROR(stream_->GetBuffer(false, &buffer, &buffer_len));
  }

  if (offset == -1) {
    // No more syncs in this scan range
    DCHECK(stream_->eosr());
    finished_ = true;
    return Status::OK();
  }
  DCHECK_GE(offset, sync_size);

  // Make sure sync starts in our scan range
  if (offset - sync_size >= stream_->bytes_left()) {
    finished_ = true;
    return Status::OK();
  }

  RETURN_IF_FALSE(stream_->SkipBytes(offset, &parse_status_));
  VLOG_FILE << "Found sync for: " << stream_->filename()
            << " at " << stream_->file_offset() - sync_size;
  if (stream_->eof()) finished_ = true;
  block_start_ = stream_->file_offset();
  ++num_syncs_;
  return Status::OK();
}

void BaseSequenceScanner::CloseFileRanges(const char* filename) {
  DCHECK(only_parsing_header_);
  HdfsFileDesc* desc = scan_node_->GetFileDesc(filename);
  const vector<DiskIoMgr::ScanRange*>& splits = desc->splits;
  for (int i = 0; i < splits.size(); ++i) {
    COUNTER_ADD(bytes_skipped_counter_, splits[i]->len());
    scan_node_->RangeComplete(file_format(), THdfsCompression::NONE);
  }
}

int BaseSequenceScanner::ReadPastSize(int64_t file_offset) {
  DCHECK_GE(total_block_size_, 0);
  if (total_block_size_ == 0) {
    // This scan range didn't include a complete block, so we have no idea how many bytes
    // remain in the block. Guess.
    return REMAINING_BLOCK_SIZE_GUESS;
  }
  DCHECK_GE(num_syncs_, 2);
  int average_block_size = total_block_size_ / (num_syncs_ - 1);

  // Number of bytes read in the current block
  int block_bytes_read = file_offset - block_start_;
  DCHECK_GE(block_bytes_read, 0);
  int bytes_left = max(average_block_size - block_bytes_read, 0);
  // Include some padding
  bytes_left += average_block_size * BLOCK_SIZE_PADDING_PERCENT;

  int max_read_size = state_->io_mgr()->max_read_buffer_size();
  return min(max(bytes_left, MIN_SYNC_READ_SIZE), max_read_size);
}
