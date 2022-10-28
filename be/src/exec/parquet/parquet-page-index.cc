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

#include "common/logging.h"
#include "exec/parquet/hdfs-parquet-scanner.h"
#include "exec/parquet/parquet-page-index.h"
#include "gutil/strings/substitute.h"
#include "rpc/thrift-util.h"
#include "runtime/io/request-context.h"
#include "runtime/io/request-ranges.h"

#include <memory>

#include "common/names.h"

using namespace parquet;
using namespace impala::io;

namespace impala {

ParquetPageIndex::ParquetPageIndex(HdfsParquetScanner* scanner) :
    scanner_(scanner),
    page_index_buffer_(scanner_->scan_node_->mem_tracker())
{
}

bool ParquetPageIndex::DeterminePageIndexRangesInRowGroup(
    const parquet::RowGroup& row_group, int64_t* column_index_start,
    int64_t* column_index_size, int64_t* offset_index_start, int64_t* offset_index_size) {
  int64_t ci_start = numeric_limits<int64_t>::max();
  int64_t oi_start = numeric_limits<int64_t>::max();
  int64_t ci_end = -1;
  int64_t oi_end = -1;
  for (const ColumnChunk& col_chunk : row_group.columns) {
    if (col_chunk.__isset.column_index_offset && col_chunk.__isset.column_index_length) {
      ci_start = min(ci_start, col_chunk.column_index_offset);
      ci_end = max(ci_end, col_chunk.column_index_offset + col_chunk.column_index_length);
    }
    if (col_chunk.__isset.offset_index_offset && col_chunk.__isset.offset_index_length) {
      oi_start = min(oi_start, col_chunk.offset_index_offset);
      oi_end = max(oi_end, col_chunk.offset_index_offset + col_chunk.offset_index_length);
    }
  }
  bool has_page_index = oi_end != -1 && ci_end != -1;
  if (has_page_index) {
    *column_index_start = ci_start;
    *column_index_size = ci_end - ci_start;
    *offset_index_start = oi_start;
    *offset_index_size = oi_end - oi_start;
  }
  return has_page_index;
}

Status ParquetPageIndex::ReadAll(int row_group_idx) {
  DCHECK(page_index_buffer_.buffer() == nullptr);
  bool has_page_index = DeterminePageIndexRangesInRowGroup(
      scanner_->file_metadata_.row_groups[row_group_idx],
      &column_index_base_offset_, &column_index_size_,
      &offset_index_base_offset_, &offset_index_size_);

  // It's not an error if there is no page index.
  if (!has_page_index) return Status::OK();

  int64_t scan_range_start = column_index_base_offset_;
  int64_t scan_range_size =
      offset_index_base_offset_ + offset_index_size_ - column_index_base_offset_;
  vector<ScanRange::SubRange> sub_ranges;
  if (column_index_base_offset_ + column_index_size_ <= offset_index_base_offset_) {
    // The sub-ranges will be merged if they are contiguous.
    sub_ranges.push_back({column_index_base_offset_, column_index_size_});
    sub_ranges.push_back({offset_index_base_offset_, offset_index_size_});
  } else {
    return Status(Substitute("Found unsupported Parquet page index layout for file '$1'.",
            scanner_->filename()));
  }
  int64_t buffer_size = column_index_size_ + offset_index_size_;
  if (!page_index_buffer_.TryAllocate(buffer_size)) {
    return Status(Substitute("Could not allocate buffer of $0 bytes for Parquet "
        "page index for file '$1'.", buffer_size, scanner_->filename()));
  }
  int64_t partition_id = scanner_->context_->partition_descriptor()->id();
  int cache_options =
      scanner_->metadata_range_->cache_options() & ~BufferOpts::USE_HDFS_CACHE;
  ScanRange* object_range = scanner_->scan_node_->AllocateScanRange(
      scanner_->metadata_range_->GetFileInfo(), scan_range_size, scan_range_start,
      move(sub_ranges), partition_id, scanner_->metadata_range_->disk_id(),
      scanner_->metadata_range_->expected_local(), BufferOpts::ReadInto(
          page_index_buffer_.buffer(), page_index_buffer_.Size(), cache_options));

  unique_ptr<BufferDescriptor> io_buffer;
  bool needs_buffers;
  RETURN_IF_ERROR(
      scanner_->scan_node_->reader_context()->StartScanRange(object_range,
          &needs_buffers));
  DCHECK(!needs_buffers) << "Already provided a buffer";
  RETURN_IF_ERROR(object_range->GetNext(&io_buffer));
  DCHECK_EQ(io_buffer->buffer(), page_index_buffer_.buffer());
  DCHECK_EQ(io_buffer->len(), page_index_buffer_.Size());
  DCHECK(io_buffer->eosr());
  scanner_->AddSyncReadBytesCounter(io_buffer->len());
  object_range->ReturnBuffer(move(io_buffer));

  return Status::OK();
}

Status ParquetPageIndex::DeserializeColumnIndex(const ColumnChunk& col_chunk,
    ColumnIndex* column_index) {
  if (page_index_buffer_.buffer() == nullptr) {
    return Status(Substitute("No page index for file $0.", scanner_->filename()));
  }

  int64_t buffer_offset = col_chunk.column_index_offset - column_index_base_offset_;
  uint32_t length = col_chunk.column_index_length;
  DCHECK_GE(buffer_offset, 0);
  DCHECK_LE(buffer_offset + length, column_index_size_);
  return DeserializeThriftMsg(page_index_buffer_.buffer() + buffer_offset,
      &length, true, column_index);
}

Status ParquetPageIndex::DeserializeOffsetIndex(const ColumnChunk& col_chunk,
    OffsetIndex* offset_index) {
  if (page_index_buffer_.buffer() == nullptr) {
    return Status(Substitute("No page index for file $0.", scanner_->filename()));
  }

  int64_t buffer_offset = col_chunk.offset_index_offset - offset_index_base_offset_ +
      column_index_size_;
  uint32_t length = col_chunk.offset_index_length;
  DCHECK_GE(buffer_offset, 0);
  DCHECK_LE(buffer_offset + length, page_index_buffer_.Size());
  return DeserializeThriftMsg(page_index_buffer_.buffer() + buffer_offset,
      &length, true, offset_index);
}

}
