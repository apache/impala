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

#include "exec/base-sequence-scanner.h"

#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-search.h"
#include "util/codec.h"

using namespace impala;
using namespace std;

const int BaseSequenceScanner::HEADER_SIZE = 1024;
const int BaseSequenceScanner::SYNC_MARKER = -1;

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
    // TODO: add remote disk id and plumb that through to the io mgr.  It should have
    // 1 queue for each NIC as well?
    DiskIoMgr::ScanRange* header_range = scan_node->AllocateScanRange(
        files[i]->filename.c_str(), HEADER_SIZE, 0, metadata->partition_id, -1);
    header_ranges.push_back(header_range);
  }
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(header_ranges));
  return Status::OK;
}
  
BaseSequenceScanner::BaseSequenceScanner(HdfsScanNode* node, RuntimeState* state)
  : HdfsScanner(node, state),
    header_(NULL),
    block_start_(0),
    data_buffer_pool_(new MemPool(state->mem_limits())) {
}

BaseSequenceScanner::~BaseSequenceScanner() {
}

Status BaseSequenceScanner::Prepare(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(context));
  decompress_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "DecompressionTime");
  bytes_skipped_counter_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "BytesSkipped", TCounterType::BYTES);
  return Status::OK;
}

Status BaseSequenceScanner::Close() {
  AttachPool(data_buffer_pool_.get());
  AddFinalRowBatch();
  context_->Close();
  if (!only_parsing_header_) {
    scan_node_->RangeComplete(file_format(), header_->compression_type);
  }
  scan_node_->ReleaseCodegenFn(file_format(), codegen_fn_);
  codegen_fn_ = NULL;
  // Collect the maximum amount of memory we used to process this file.
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      data_buffer_pool_->peak_allocated_bytes());
  return Status::OK;
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
      // We need to complete the ranges for this file.
      CloseFileRanges(stream_->filename());
      return Status::OK;
    }

    // Header is parsed, set the metadata in the scan node and issue more ranges
    scan_node_->SetFileMetadata(stream_->filename(), header_);
    HdfsFileDesc* desc = scan_node_->GetFileDesc(stream_->filename());
    scan_node_->AddDiskIoRanges(desc);
    return Status::OK;
  }
  
  // Initialize state for new scan range
  finished_ = false;
  RETURN_IF_ERROR(InitNewRange());

  Status status = Status::OK;

  // Skip to the first record
  if (stream_->file_offset() < header_->header_size) {
    // If the scan range starts within the header, skip to the end of the header so we
    // don't accidentally skip to an extra sync within the header
    RETURN_IF_FALSE(stream_->SkipBytes(
        header_->header_size - stream_->file_offset(), &parse_status_));
  }
  RETURN_IF_ERROR(SkipToSync(header_->sync, SYNC_HASH_SIZE));

  // Process Range.
  int64_t first_error_offset = 0;
  int num_errors = 0;

  // We can continue through errors by skipping to the next SYNC hash. 
  while (!finished_) {
    status = ProcessRange();
    if (status.IsCancelled()) return status;
    // Save the offset of any error.
    if (first_error_offset == 0) first_error_offset = stream_->file_offset();

    // Catch errors from file format parsing.  We call some utilities
    // that do not log errors so generate a reasonable message.
    if (!status.ok()) {
      if (state_->LogHasSpace()) {
        stringstream ss;
        ss << "Format error in record or block header ";
        if (stream_->eosr()) {
          ss << "at end of file.";
        } else {
          ss << "at offset: "  << block_start_;
        }
        state_->LogError(ss.str());
      }
    }

    // If no errors or we abort on error then exit loop, otherwise try to recover.
    if (state_->abort_on_error() || status.ok()) break;

    // Recover by skipping to the next sync.
    parse_status_ = Status::OK;
    ++num_errors;
    int64_t error_offset = stream_->file_offset();
    status = SkipToSync(header_->sync, SYNC_HASH_SIZE);
    COUNTER_UPDATE(bytes_skipped_counter_, stream_->file_offset() - error_offset);
    RETURN_IF_ERROR(status);
    DCHECK(parse_status_.ok());
  }

  if (num_errors != 0 || !status.ok()) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss  << "First error while processing: " << stream_->filename()
          << " at offset: "  << first_error_offset;
      state_->LogError(ss.str());
      state_->ReportFileErrors(stream_->filename(), num_errors == 0 ? 1 : num_errors);
    }
    if (state_->abort_on_error()) return status;
  }

  // All done with this scan range.
  return Status::OK;
}

Status BaseSequenceScanner::ReadSync() {
  // We are finished when we read a sync marker occurring completely in the next
  // scan range
  finished_ = stream_->eosr();

  uint8_t* hash;
  int out_len;
  RETURN_IF_FALSE(
      stream_->GetBytes(SYNC_HASH_SIZE, &hash, &out_len, &parse_status_));
  if (out_len != SYNC_HASH_SIZE || memcmp(hash, header_->sync, SYNC_HASH_SIZE)) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss  << "Bad sync hash at file offset "
          << (stream_->file_offset() - SYNC_HASH_SIZE) << "." << endl
          << "Expected: '"
          << ReadWriteUtil::HexDump(header_->sync, SYNC_HASH_SIZE)
          << "'" << endl
          << "Actual:   '"
          << ReadWriteUtil::HexDump(hash, SYNC_HASH_SIZE)
          << "'" << endl;
      state_->LogError(ss.str());
    }
    return Status("bad sync hash block");
  }
  finished_ |= stream_->eof();
  return Status::OK;
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
  int buffer_len;
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
    int to_skip = max(0, buffer_len - (sync_size - 1));
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
    return Status::OK;
  }
  DCHECK_GE(offset, sync_size);

  // Make sure sync starts in our scan range
  if (offset - sync_size >= stream_->bytes_left()) {
    finished_ = true;
    return Status::OK;
  }

  RETURN_IF_FALSE(stream_->SkipBytes(offset, &parse_status_));
  VLOG_FILE << "Found sync for: " << stream_->filename()
            << " at " << stream_->file_offset() - sync_size;
  if (stream_->eof()) finished_ = true;
  return Status::OK;
}

void BaseSequenceScanner::CloseFileRanges(const char* filename) {
  DCHECK(only_parsing_header_);
  HdfsFileDesc* desc = scan_node_->GetFileDesc(filename);
  const vector<DiskIoMgr::ScanRange*>& splits = desc->splits;
  for (int i = 0; i < splits.size(); ++i) {
    COUNTER_UPDATE(bytes_skipped_counter_, splits[i]->len());
    scan_node_->RangeComplete(file_format(), THdfsCompression::NONE);
  }
}
