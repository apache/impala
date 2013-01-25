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
  
BaseSequenceScanner::BaseSequenceScanner(HdfsScanNode* node, 
    RuntimeState* state) : 
  HdfsScanner(node, state),
  header_(NULL),
  have_sync_(false),
  block_start_(0),
  data_buffer_pool_(new MemPool()) {
}

BaseSequenceScanner::~BaseSequenceScanner() {
  // Collect the maximum amount of memory we used to process this file.
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      data_buffer_pool_->peak_allocated_bytes());
}

Status BaseSequenceScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());
  decompress_timer_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "DecompressionTime", TCounterType::CPU_TICKS);
  return Status::OK;
}

Status BaseSequenceScanner::Close() {
  context_->AcquirePool(data_buffer_pool_.get());
  context_->Flush();
  if (!only_parsing_header_) {
    scan_node_->RangeComplete(header_->file_type, header_->compression_type);
  }
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
    RETURN_IF_ERROR(ReadFileHeader());

    // Header is parsed, set the metadata in the scan node and issue more ranges
    scan_node_->SetFileMetadata(context_->filename(), header_);
    IssueFileRanges(context_->filename());
    return Status::OK;
  }
  
  // Initialize state for new scan range
  RETURN_IF_ERROR(InitNewRange());

  Status status;

  // Find the first record
  if (context_->scan_range()->offset() == 0) {
    // scan range that starts at the beginning of the file, just skip ahead by
    // the header size.
    if (!SerDeUtils::SkipBytes(context_, header_->header_size, &status)) return status;
  } else {
    RETURN_IF_ERROR(SkipToSync(header_->sync, SYNC_HASH_SIZE, &have_sync_));
    if (context_->eosr()) return Status::OK;
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
      status = SkipToSync(header_->sync, SYNC_HASH_SIZE, &have_sync_);
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
  uint8_t* hash;
  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context_, SYNC_HASH_SIZE, &hash, &parse_status_));
  if (memcmp(hash, header_->sync, SYNC_HASH_SIZE)) {
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
  return Status::OK;
}

// Utility function to look for 'sync' in buffer.  Sync markers are preceded with
// 4 bytes of 0xFFFFFFFF.  Returns the offset into buffer if it is found, otherwise, 
// returns buffer_len.
static int FindSyncBlock(const uint8_t* buffer, int buffer_len, 
    const uint8_t* sync, int sync_len) {
  char marker_and_sync[4 + sync_len];
  marker_and_sync[0] = marker_and_sync[1] = 
      marker_and_sync[2] = marker_and_sync[3] = 0xff;
  memcpy(marker_and_sync + 4, sync, sync_len);

  StringValue needle(marker_and_sync, 4 + sync_len);
  StringValue haystack(
      const_cast<char*>(reinterpret_cast<const char*>(buffer)), buffer_len);

  StringSearch search(&needle);
  int offset = search.Search(&haystack);
  if (offset == -1) return buffer_len;
  return offset;
}

Status BaseSequenceScanner::SkipToSync(const uint8_t* sync, int sync_size, 
    bool* sync_found) {
  bool eosr = false;
  int offset = sync_size;
  int buffer_len;
  Status status;
  do {
    uint8_t* buffer;
  
    RETURN_IF_ERROR(context_->GetRawBytes(&buffer, &buffer_len, &eosr));
    offset = FindSyncBlock(buffer, buffer_len, sync, sync_size);
    DCHECK_LE(offset, buffer_len);

    // We need to check for a sync that spans buffers.
    if (offset == buffer_len) {
      // The marker (-1) and the sync can start anywhere in the
      // last 19 bytes of the buffer.  
      int tail_size = sync_size + sizeof(int32_t) - 1;
      uint8_t* bp = buffer + buffer_len - tail_size;
      uint8_t save[2 * tail_size];

      // Save the tail of the buffer.
      memcpy(save, bp, tail_size);

      // Read the next buffer.
      if (!SerDeUtils::SkipBytes(context_, offset, &status)) return status;
      RETURN_IF_ERROR(context_->GetRawBytes(&buffer, &buffer_len, &eosr));
      offset = buffer_len;
      if (buffer_len >= tail_size) {
        memcpy(save + tail_size, buffer, tail_size);
        offset = FindSyncBlock(save, 2 * tail_size, sync, sync_size);

        // The sync mark does not span the buffers search the whole new buffer.
        if (offset == 2 * tail_size) {
          offset = buffer_len;
          continue;
        }

        *sync_found = true;
        // Adjust the offset to be relative to the start of the new buffer
        offset -= tail_size;
        // Adjust offset to be past the sync since it spans buffers.
        offset += sync_size + sizeof(int32_t);
      }
    }

    // Advance to the offset.  If sync_found is set then this is past the sync block.
    if (offset != 0) {
      if (!SerDeUtils::SkipBytes(context_, offset, &status)) return status;
    }
  } while (offset >= buffer_len && !eosr);

  if (!eosr) {
    VLOG_FILE << "Found sync for: " << context_->filename()
              << " at " << context_->file_offset() - (*sync_found ? offset : 0);
  }

  return Status::OK;
}
