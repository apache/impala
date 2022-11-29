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

#include <memory>
#include <boost/bind.hpp>

#include "exec/base-sequence-scanner.h"

#include "exec/hdfs-scan-node-base.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-search.h"
#include "util/codec.h"
#include "util/runtime-profile-counters.h"
#include "util/test-info.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;

const int BaseSequenceScanner::HEADER_SIZE = 1024;
const int BaseSequenceScanner::SYNC_MARKER = -1;

// Constants used in ReadPastSize()
static const double BLOCK_SIZE_PADDING_PERCENT = 0.1;
static const int MIN_SYNC_READ_SIZE = 64 * 1024; // bytes

// Macro to convert between SerdeUtil errors to Status returns.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

Status BaseSequenceScanner::IssueInitialRanges(HdfsScanNodeBase* scan_node,
    const vector<HdfsFileDesc*>& files) {
  DCHECK(!files.empty());
  // Issue just the header range for each file.  When the header is complete,
  // we'll issue the splits for that file.  Splits cannot be processed until the
  // header is parsed (the header object is then shared across splits for that file).
  vector<ScanRange*> header_ranges;
  for (int i = 0; i < files.size(); ++i) {
    ScanRangeMetadata* metadata =
        static_cast<ScanRangeMetadata*>(files[i]->splits[0]->meta_data());
    int64_t header_size = min<int64_t>(HEADER_SIZE, files[i]->file_length);
    // The header is almost always a remote read. Set the disk id to -1 and indicate
    // it is not cached.
    // TODO: add remote disk id and plumb that through to the io mgr.  It should have
    // 1 queue for each NIC as well?
    bool expected_local = false;
    int cache_options = !scan_node->IsDataCacheDisabled() ? BufferOpts::USE_DATA_CACHE :
        BufferOpts::NO_CACHING;
    ScanRange* header_range = scan_node->AllocateScanRange(files[i]->GetFileInfo(),
        header_size, 0, metadata->partition_id, -1, expected_local,
        BufferOpts(cache_options), metadata->original_split);
    ScanRangeMetadata* header_metadata =
            static_cast<ScanRangeMetadata*>(header_range->meta_data());
    header_metadata->is_sequence_header = true;
    header_ranges.push_back(header_range);
  }
  // When the header is parsed, we will issue more AddDiskIoRanges in
  // the scanner threads.
  scan_node->UpdateRemainingScanRangeSubmissions(header_ranges.size());
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(header_ranges, EnqueueLocation::TAIL));
  return Status::OK();
}

bool BaseSequenceScanner::FileFormatIsSequenceBased(THdfsFileFormat::type format) {
  return format == THdfsFileFormat::SEQUENCE_FILE ||
         format == THdfsFileFormat::RC_FILE ||
         format == THdfsFileFormat::AVRO;
}

BaseSequenceScanner::BaseSequenceScanner(HdfsScanNodeBase* node, RuntimeState* state)
  : HdfsScanner(node, state) {
}

BaseSequenceScanner::BaseSequenceScanner() : HdfsScanner() {
  DCHECK(TestInfo::is_test());
}

BaseSequenceScanner::~BaseSequenceScanner() {
}

Status BaseSequenceScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Open(context));
  stream_->set_read_past_size_cb(bind(&BaseSequenceScanner::ReadPastSize, this, _1));
  bytes_skipped_counter_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "BytesSkipped", TUnit::BYTES);

  header_ = reinterpret_cast<FileHeader*>(
      scan_node_->GetFileMetadata(
          context->partition_descriptor()->id(), stream_->filename()));
  if (header_ == nullptr) {
    only_parsing_header_ = true;
    return Status::OK();
  }
  RETURN_IF_ERROR(InitNewRange());

  // Skip to the first record
  if (stream_->file_offset() < header_->header_size) {
    // If the scan range starts within the header, skip to the end of the header so we
    // don't accidentally skip to an extra sync within the header
    RETURN_IF_FALSE(stream_->SkipBytes(
        header_->header_size - stream_->file_offset(), &parse_status_));
  }
  RETURN_IF_ERROR(SkipToSync(header_->sync, SYNC_HASH_SIZE));
  return Status::OK();
}

void BaseSequenceScanner::Close(RowBatch* row_batch) {
  DCHECK(!is_closed_);
  VLOG_FILE << "Bytes read past scan range: " << -stream_->bytes_left();
  VLOG_FILE << "Average block size: "
            << (num_syncs_ > 1 ? total_block_size_ / (num_syncs_ - 1) : 0);
  // Need to close the decompressor before releasing the resources at AddFinalRowBatch(),
  // because in some cases there is memory allocated in decompressor_'s temp_memory_pool_.
  if (decompressor_.get() != nullptr) {
    decompressor_->Close();
    decompressor_.reset();
  }
  if (row_batch != nullptr) {
    row_batch->tuple_data_pool()->AcquireData(data_buffer_pool_.get(), false);
    row_batch->tuple_data_pool()->AcquireData(template_tuple_pool_.get(), false);
    if (scan_node_->HasRowBatchQueue()) {
      static_cast<HdfsScanNode*>(scan_node_)->AddMaterializedRowBatch(
        unique_ptr<RowBatch>(row_batch));
    }
  } else {
    data_buffer_pool_->FreeAll();
    template_tuple_pool_->FreeAll();
  }
  context_->ReleaseCompletedResources(true);

  // Verify all resources (if any) have been transferred.
  DCHECK_EQ(template_tuple_pool_.get()->total_allocated_bytes(), 0);
  DCHECK_EQ(data_buffer_pool_.get()->total_allocated_bytes(), 0);
  // 'header_' can be nullptr if HdfsScanNodeBase::CreateAndOpenScanner() failed.
  if (!only_parsing_header_ && header_ != nullptr) {
    scan_node_->RangeComplete(file_format(), header_->compression_type);
  }
  CloseInternal();
}

Status BaseSequenceScanner::GetNextInternal(RowBatch* row_batch) {
  if (only_parsing_header_) {
    DCHECK(header_ == nullptr);
    eos_ = true;
    header_ = state_->obj_pool()->Add(AllocateFileHeader());
    Status status = ReadFileHeader();
    if (!status.ok()) {
      scan_node_->UpdateRemainingScanRangeSubmissions(-1);
      RETURN_IF_ERROR(state_->LogOrReturnError(status.msg()));
      // We need to complete the ranges for this file.
      CloseFileRanges(stream_->filename());
      return Status::OK();
    }
    // Header is parsed, set the metadata in the scan node and issue more ranges.
    scan_node_->SetFileMetadata(
        context_->partition_descriptor()->id(), stream_->filename(), header_);
    const HdfsFileDesc* desc = scan_node_->GetFileDesc(
        context_->partition_descriptor()->id(), stream_->filename());
    // Issue the scan range with priority since it would result in producing a RowBatch.
    status = scan_node_->AddDiskIoRanges(desc, EnqueueLocation::HEAD);
    scan_node_->UpdateRemainingScanRangeSubmissions(-1);
    return status;
  }
  if (eos_) return Status::OK();

  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size, &tuple_mem_));
  tuple_ = reinterpret_cast<Tuple*>(tuple_mem_);
  DCHECK_GT(row_batch->capacity(), 0);

  Status status = ProcessRange(row_batch);
  if (!status.ok()) {
    // Log error from file format parsing.
    // TODO(IMPALA-5922): Include the file and offset in errors inside the scanners.
    if (!status.IsCancelled() &&
        !status.IsMemLimitExceeded() &&
        !status.IsInternalError() &&
        !status.IsDiskIoError() &&
        !status.IsThreadPoolError()) {
      state_->LogError(ErrorMsg(TErrorCode::SEQUENCE_SCANNER_PARSE_ERROR,
          stream_->filename(), stream_->file_offset(),
          (stream_->eof() ? "(EOF)" : "")));
    }

    // This checks for abort_on_error.
    RETURN_IF_ERROR(state_->LogOrReturnError(status.msg()));

    // Recover by skipping to the next sync.
    parse_status_ = Status::OK();
    int64_t error_offset = stream_->file_offset();
    status = SkipToSync(header_->sync, SYNC_HASH_SIZE);
    COUNTER_ADD(bytes_skipped_counter_, stream_->file_offset() - error_offset);
    RETURN_IF_ERROR(status);
    DCHECK(parse_status_.ok());
  }
  return Status::OK();
}

Status BaseSequenceScanner::ReadSync() {
  DCHECK(!eos_);
  if (stream_->eosr()) {
    // Either we're at the end of file or the next sync marker is completely in the next
    // scan range.
    eos_ = true;
  } else {
    // Not at end of scan range or file - we expect there to be another sync marker, which
    // is either followed by another block or the end of the file.
    uint8_t* hash;
    int64_t out_len;
    bool success = stream_->GetBytes(SYNC_HASH_SIZE, &hash, &out_len, &parse_status_);
    if (!success) return parse_status_;
    if (out_len != SYNC_HASH_SIZE) {
      return Status(Substitute("Hit end of stream after reading $0 bytes of $1-byte "
          "synchronization marker", out_len, SYNC_HASH_SIZE));
    } else if (memcmp(hash, header_->sync, SYNC_HASH_SIZE) != 0) {
      stringstream ss;
      ss  << "Bad synchronization marker" << endl
          << "  Expected: '"
          << ReadWriteUtil::HexDump(header_->sync, SYNC_HASH_SIZE) << "'" << endl
          << "  Actual:   '"
          << ReadWriteUtil::HexDump(hash, SYNC_HASH_SIZE) << "'";
      return Status(ss.str());
    }
    // If we read the sync marker at end of file then we're done!
    eos_ = stream_->eof();
    ++num_syncs_;
    block_start_ = stream_->file_offset();
  }
  total_block_size_ += stream_->file_offset() - block_start_;
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
    eos_ = true;
    return Status::OK();
  }
  DCHECK_GE(offset, sync_size);

  // Make sure sync starts in our scan range
  if (offset - sync_size >= stream_->bytes_left()) {
    eos_ = true;
    return Status::OK();
  }

  RETURN_IF_FALSE(stream_->SkipBytes(offset, &parse_status_));
  VLOG_FILE << "Found sync for: " << stream_->filename()
            << " at " << stream_->file_offset() - sync_size;
  if (stream_->eof()) eos_ = true;
  block_start_ = stream_->file_offset();
  ++num_syncs_;
  return Status::OK();
}

void BaseSequenceScanner::CloseFileRanges(const char* filename) {
  DCHECK(only_parsing_header_);
  const HdfsFileDesc* desc = scan_node_->GetFileDesc(
      context_->partition_descriptor()->id(), filename);
  const vector<ScanRange*>& splits = desc->splits;
  for (int i = 0; i < splits.size(); ++i) {
    COUNTER_ADD(bytes_skipped_counter_, splits[i]->bytes_to_read());
  }
  scan_node_->SkipFile(file_format(), desc);
}

int BaseSequenceScanner::ReadPastSize(int64_t file_offset) {
  DCHECK_GE(total_block_size_, 0);
  // This scan range didn't include a complete block, so we have no idea how many bytes
  // remain in the block. Let ScannerContext use its default strategy.
  if (total_block_size_ == 0) return 0;
  DCHECK_GE(num_syncs_, 2);
  int average_block_size = total_block_size_ / (num_syncs_ - 1);

  // Number of bytes read in the current block
  int block_bytes_read = file_offset - block_start_;
  DCHECK_GE(block_bytes_read, 0);
  int bytes_left = max(average_block_size - block_bytes_read, 0);
  // Include some padding
  bytes_left += average_block_size * BLOCK_SIZE_PADDING_PERCENT;
  return max(bytes_left, MIN_SYNC_READ_SIZE);
}
