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
#include "exec/scan-range-context.h"
#include "exec/serde-utils.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-search.h"
#include "util/codec.h"

using namespace impala;
using namespace std;

const int BaseSequenceScanner::HEADER_SIZE = 1024;
const int BaseSequenceScanner::SYNC_MARKER = -1;

// Macro to convert between SerdeUtil errors to Status returns.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

void BaseSequenceScanner::IssueInitialRanges(HdfsScanNode* scan_node, 
    const vector<HdfsFileDesc*>& files) {
  // Issue just the header range for each file.  When the header is complete,
  // we'll issue the ranges for that file.  Ranges cannot be processed until the
  // header is parsed (the header object is then shared across scan ranges of that
  // file).
  for (int i = 0; i < files.size(); ++i) {
    int64_t partition_id = reinterpret_cast<int64_t>(files[i]->ranges[0]->meta_data());
    // TODO: add remote disk id and plumb that through to the io mgr.  It should have
    // 1 queue for each NIC as well?
    DiskIoMgr::ScanRange* header_range = scan_node->AllocateScanRange(
        files[i]->filename.c_str(), HEADER_SIZE, 0, partition_id, -1);
    scan_node->AddDiskIoRange(header_range);
  }
}
  
BaseSequenceScanner::BaseSequenceScanner(HdfsScanNode* node, RuntimeState* state,
                                         bool marker_precedes_sync)
  : HdfsScanner(node, state),
    header_(NULL),
    block_start_(0),
    data_buffer_pool_(new MemPool()),
    marker_precedes_sync_(marker_precedes_sync) {
  data_buffer_pool_->set_limits(*state->mem_limits());
}

BaseSequenceScanner::~BaseSequenceScanner() {
}

Status BaseSequenceScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());
  decompress_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "DecompressionTime");
  bytes_skipped_counter_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "BytesSkipped", TCounterType::BYTES);
  return Status::OK;
}

Status BaseSequenceScanner::Close() {
  context_->AcquirePool(data_buffer_pool_.get());
  context_->Flush();
  if (!only_parsing_header_) {
    scan_node_->RangeComplete(file_format(), header_->compression_type);
  }
  // Collect the maximum amount of memory we used to process this file.
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      data_buffer_pool_->peak_allocated_bytes());
  return Status::OK;
}

Status BaseSequenceScanner::ProcessScanRange(ScanRangeContext* context) {
  context_ = context;

  header_ = reinterpret_cast<FileHeader*>(
      scan_node_->GetFileMetadata(context_->filename()));
  if (header_ == NULL) {
    // This is the initial scan range just to parse the header
    only_parsing_header_ = true;
    header_ = state_->obj_pool()->Add(AllocateFileHeader());
    Status status = ReadFileHeader();
    if (!status.ok()) {
      if (state_->abort_on_error()) return status;
      // We need to complete the ranges for this file.
      CloseFileRanges(context_->filename());
      return Status::OK;
    }

    // Header is parsed, set the metadata in the scan node and issue more ranges
    scan_node_->SetFileMetadata(context_->filename(), header_);
    IssueFileRanges(context_->filename());
    return Status::OK;
  }
  
  // Initialize state for new scan range
  finished_ = false;
  RETURN_IF_ERROR(InitNewRange());

  Status status;

  // Find the first record
  if (context_->scan_range()->offset() == 0) {
    // scan range that starts at the beginning of the file, just skip ahead by
    // the header size.
    if (!SerDeUtils::SkipBytes(context_, header_->header_size, &status)) return status;
  } else {
    status = SkipToSync(header_->sync, SYNC_HASH_SIZE);
    if (context_->eosr()) {
      // We don't care about status here -- OK if we can't find the sync but
      // we're at the end of the scan range
      return Status::OK;
    }
    RETURN_IF_ERROR(status);
  }

  // Process Range.
  int64_t first_error_offset = 0;
  int num_errors = 0;

  // We can continue through errors by skipping to the next SYNC hash. 
  do {
    status = ProcessRange();
    if (status.IsCancelled()) return status;
    // Save the offset of any error.
    if (first_error_offset == 0) first_error_offset = context_->file_offset();

    // Catch errors from file format parsing.  We call some utilities
    // that do not log errors so generate a reasonable message.
    if (!status.ok()) {
      if (state_->LogHasSpace()) {
        stringstream ss;
        ss << "Format error in record or block header ";
        if (context_->eosr()) {
          ss << "at end of file.";
        } else {
          ss << "at offset: "  << block_start_;
        }
        state_->LogError(ss.str());
      }
    }

    // If no errors or we abort on error then exit loop, otherwise try to recover.
    if (state_->abort_on_error() || status.ok()) break;

    if (!context_->eosr()) {
      parse_status_ = Status::OK;
      ++num_errors;
      // Recover by skipping to the next sync.
      int64_t error_offset = context_->file_offset();
      status = SkipToSync(header_->sync, SYNC_HASH_SIZE);
      COUNTER_UPDATE(bytes_skipped_counter_, context_->file_offset() - error_offset);
      if (status.IsCancelled()) return status;
      if (context_->eosr()) break;

      // An error status is explicitly ignored here so we can skip over bad blocks.
      // We will continue through this loop again looking for the next sync.
    }
  } while (!context_->eosr());

  if (num_errors != 0 || !status.ok()) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss  << "First error while processing: " << context_->filename()
          << " at offset: "  << first_error_offset;
      state_->LogError(ss.str());
      state_->ReportFileErrors(context_->filename(), num_errors == 0 ? 1 : num_errors);
    }
    if (state_->abort_on_error()) return status;
  }

  // All done with this scan range.
  return Status::OK;
}

Status BaseSequenceScanner::ReadSync() {
  // We are finished when we read a sync marker occurring completely in the next
  // scan range
  finished_ = context_->eosr();

  uint8_t* hash;
  int out_len;
  bool eos;
  RETURN_IF_FALSE(
      context_->GetBytes(&hash, SYNC_HASH_SIZE, &out_len, &eos, &parse_status_));
  if (out_len != SYNC_HASH_SIZE || memcmp(hash, header_->sync, SYNC_HASH_SIZE)) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss  << "Bad sync hash at file offset "
          << (context_->file_offset() - SYNC_HASH_SIZE) << "." << endl
          << "Expected: '"
          << SerDeUtils::HexDump(header_->sync, SYNC_HASH_SIZE)
          << "'" << endl
          << "Actual:   '"
          << SerDeUtils::HexDump(hash, SYNC_HASH_SIZE)
          << "'" << endl;
      state_->LogError(ss.str());
    }
    return Status("bad sync hash block");
  }
  // TODO: finished_ |= end of file (this will prevent us from reading off
  // the end of the file)
  return Status::OK;
}

int BaseSequenceScanner::FindSyncBlock(const uint8_t* buffer, int buffer_len,
                                       const uint8_t* sync, int sync_len) {
  StringValue needle;
  char marker_and_sync[4 + sync_len];
  if (marker_precedes_sync_) {
    marker_and_sync[0] = marker_and_sync[1] =
        marker_and_sync[2] = marker_and_sync[3] = 0xff;
    memcpy(marker_and_sync + 4, sync, sync_len);
    needle = StringValue(marker_and_sync, 4 + sync_len);
  } else {
    char* sync_str = reinterpret_cast<char*>(const_cast<uint8_t*>(sync));
    needle = StringValue(sync_str, sync_len);
  }

  StringValue haystack(
      const_cast<char*>(reinterpret_cast<const char*>(buffer)), buffer_len);

  StringSearch search(&needle);
  int offset = search.Search(&haystack);

  if (offset != -1) {
    // Advance offset past sync
    offset += sync_len;
    if (marker_precedes_sync_) {
      offset += 4;
    }
  }
  return offset;
}

Status BaseSequenceScanner::SkipToSync(const uint8_t* sync, int sync_size) {
  // offset into current buffer of end of sync (once found, -1 until then)
  int offset = -1;
  uint8_t* buffer;
  int buffer_len;
  bool eosr;
  Status status;
  
  // A sync marker can span multiple buffers.  In that case, we use this staging
  // buffer to combine bytes from the buffers.  
  // The -1 marker (if present) and the sync can start anywhere in the last 19 bytes 
  // of the buffer, so we save the 19-byte tail of the buffer.
  int tail_size = sync_size + sizeof(int32_t) - 1;
  uint8_t split_buffer[2 * tail_size];

  // Read buffers until we find a sync or reach end of scan range
  RETURN_IF_ERROR(context_->GetRawBytes(&buffer, &buffer_len, &eosr));
  while (true) {
    // Check if sync fully contained in current buffer
    offset = FindSyncBlock(buffer, buffer_len, sync, sync_size);
    DCHECK_LE(offset, buffer_len);
    if (offset != -1) break;

    // It wasn't in the full buffer, copy the bytes at the end
    int bytes_first_buffer = ::min(tail_size, buffer_len);
    uint8_t* bp = buffer + buffer_len - bytes_first_buffer;
    memcpy(split_buffer, bp, bytes_first_buffer);

    // Read the next buffer
    if (!SerDeUtils::SkipBytes(context_, buffer_len, &status)) return status;
    RETURN_IF_ERROR(context_->GetRawBytes(&buffer, &buffer_len, &eosr));

    // Copy the first few bytes of the next buffer and check again.
    int bytes_second_buffer = ::min(tail_size, buffer_len);
    memcpy(split_buffer + bytes_first_buffer, buffer, bytes_second_buffer);
    offset = FindSyncBlock(split_buffer, 
        bytes_first_buffer + bytes_second_buffer, sync, sync_size);
    if (offset != -1) {
      DCHECK_GE(offset, bytes_first_buffer);
      // Adjust the offset to be relative to the start of the new buffer
      offset -= bytes_first_buffer;
      break;
    }

    if (eosr) {
      // No sync marker found in this scan range
      return Status::OK;
    }
  }

  // We found a sync at offset. offset cannot be 0 since it points to the end of
  // the sync in the current buffer.
  DCHECK_GT(offset, 0);
  if (!SerDeUtils::SkipBytes(context_, offset, &status)) return status;
  VLOG_FILE << "Found sync for: " << context_->filename()
            << " at " << context_->file_offset() - sync_size;
  return Status::OK;
}

void BaseSequenceScanner::CloseFileRanges(const char* filename) {
  DCHECK(only_parsing_header_);
  HdfsFileDesc* desc = scan_node_->GetFileDesc(filename);
  const vector<DiskIoMgr::ScanRange*>& ranges = desc->ranges;
  for (int i = 0; i < ranges.size(); ++i) {
    COUNTER_UPDATE(bytes_skipped_counter_, ranges[i]->len());
    scan_node_->RangeComplete(file_format(), THdfsCompression::NONE);
  }
}
