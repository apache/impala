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

#include "exec/parquet/parquet-page-reader.h"

#include <sstream>
#include <string>
#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>

#include "common/names.h"
#include "exec/scanner-context.inline.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "util/pretty-printer.h"


// Max data page header size in bytes. This is an estimate and only needs to be an upper
// bound. It is theoretically possible to have a page header of any size due to string
// value statistics, but in practice we'll have trouble reading string values this large.
// Also, this limit is in place to prevent impala from reading corrupt parquet files.
DEFINE_int32(max_page_header_size, 8*1024*1024, "max parquet page header size in bytes");

using namespace impala::io;

using parquet::Encoding;

namespace impala {

// Max dictionary page header size in bytes. This is an estimate and only needs to be an
// upper bound.
static const int MAX_DICT_HEADER_SIZE = 100;

Status ParquetPageReader::InitColumnChunk(const HdfsFileDesc& file_desc,
    const parquet::ColumnChunk& col_chunk, int row_group_idx,
    std::vector<io::ScanRange::SubRange>&& sub_ranges) {
  int64_t col_start = col_chunk.meta_data.data_page_offset;
  if (col_chunk.meta_data.__isset.dictionary_page_offset) {
    // Already validated in ValidateColumnOffsets()
    DCHECK_LT(col_chunk.meta_data.dictionary_page_offset, col_start);
    col_start = col_chunk.meta_data.dictionary_page_offset;
  }

  int64_t col_len = col_chunk.meta_data.total_compressed_size;
  if (col_len <= 0) {
    return Status(Substitute("File '$0' contains invalid column chunk size: $1",
          filename(), col_len));
  }

  int64_t col_end = col_start + col_len;
  // Already validated in ValidateColumnOffsets()
  DCHECK_GT(col_end, 0);
  DCHECK_LT(col_end, file_desc.file_length);
  const ParquetFileVersion& file_version = parent_->file_version_;
  if (file_version.application == "parquet-mr" && file_version.VersionLt(1, 2, 9)) {
    // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
    // dictionary page header size in total_compressed_size and total_uncompressed_size
    // (see IMPALA-694). We pad col_len to compensate.
    int64_t bytes_remaining = file_desc.file_length - col_end;
    int64_t pad = min<int64_t>(MAX_DICT_HEADER_SIZE, bytes_remaining);
    col_len += pad;
    col_end += pad;
  }

  if (!col_chunk.file_path.empty() && col_chunk.file_path != filename()) {
    return Status(Substitute("Expected parquet column file path '$0' to match "
          "filename '$1'", col_chunk.file_path, filename()));
  }

  const ScanRange* metadata_range = parent_->metadata_range_;
  int64_t partition_id = parent_->context_->partition_descriptor()->id();
  const ScanRange* split_range =
      static_cast<ScanRangeMetadata*>(metadata_range->meta_data())->original_split;
  // Determine if the column is completely contained within a local split.
  bool col_range_local = split_range->ExpectedLocalRead(col_start, col_len);
  ScanRange::FileInfo fi = metadata_range->GetFileInfo();
  fi.mtime = file_desc.mtime;
  scan_range_ = parent_->scan_node_->AllocateScanRange(fi,
      col_len, col_start, move(sub_ranges), partition_id, split_range->disk_id(),
      col_range_local, BufferOpts(split_range->cache_options()));
  page_headers_read_ = 0;
  dictionary_header_encountered_ = false;
  state_ = State::Initialized;
  return Status::OK();
}

Status ParquetPageReader::StartScan(int io_reservation) {
  DCHECK_EQ(state_, State::Initialized);
  DCHECK_GT(io_reservation, 0);
  DCHECK(scan_range_ != nullptr) << "Must Reset() before starting scan.";

  RETURN_IF_ERROR(
      parent_->context_->AddAndStartStream(scan_range_, io_reservation, &stream_));
  DCHECK(stream_ != nullptr);

  state_ = State::ToReadHeader;
  return Status::OK();
}

Status ParquetPageReader::ReadPageHeader(bool* eos) {
  DCHECK(state_ == State::ToReadHeader || state_ == State::ToReadData);
  DCHECK(stream_ != nullptr);

  *eos = false;
  if (state_ == State::ToReadData) return Status::OK();

  uint8_t* buffer;
  int64_t buffer_size;
  RETURN_IF_ERROR(stream_->GetBuffer(true, &buffer, &buffer_size));
  // Check for end of stream.
  if (buffer_size == 0) {
    DCHECK(stream_->eosr());
    *eos = true;
    return Status::OK();
  }
  // We don't know the actual header size until the thrift object is deserialized. Loop
  // until we successfully deserialize the header or exceed the maximum header size.
  uint32_t header_size;
  Status status;
  parquet::PageHeader header;
  while (true) {
    header_size = buffer_size;
    status = DeserializeThriftMsg(buffer, &header_size, true, &header);
    if (status.ok()) break;

    if (buffer_size >= FLAGS_max_page_header_size) {
      stringstream ss;
      ss << "ParquetScanner: could not read data page because page header exceeded "
         << "maximum size of "
         << PrettyPrinter::Print(FLAGS_max_page_header_size, TUnit::BYTES);
      status.AddDetail(ss.str());
      return status;
    }
    // Didn't read entire header, increase buffer size and try again
    int64_t new_buffer_size = max<int64_t>(buffer_size * 2, 1024);
    status = Status::OK();
    bool success = stream_->GetBytes(
        new_buffer_size, &buffer, &new_buffer_size, &status, /* peek */ true);
    if (!success) {
      DCHECK(!status.ok());
      return status;
    }
    DCHECK(status.ok());
    // Even though we increased the allowed buffer size, the number of bytes
    // read did not change. The header is not limited by the buffer space,
    // so it must be incomplete in the file.
    if (buffer_size == new_buffer_size) {
      DCHECK_NE(new_buffer_size, 0);
      return Status(TErrorCode::PARQUET_HEADER_EOF, filename());
    }
    DCHECK_GT(new_buffer_size, buffer_size);
    buffer_size = new_buffer_size;
  }
  int data_size = header.compressed_page_size;
  if (UNLIKELY(data_size < 0)) {
    return Status(Substitute("Corrupt Parquet file '$0': negative page size $1 for "
        "column '$2'", filename(), data_size, schema_name_));
  }
  int uncompressed_size = header.uncompressed_page_size;
  if (UNLIKELY(uncompressed_size < 0)) {
    return Status(Substitute("Corrupt Parquet file '$0': negative uncompressed page "
        "size $1 for column '$2'", filename(), uncompressed_size,
        schema_name_));
  }
  const bool is_dictionary = header.__isset.dictionary_page_header;
  if (UNLIKELY(page_headers_read_ != 0 && is_dictionary)) {
    // Any dictionary is already initialized as it has to be the first page.
    // There are two possibilities:
    // 1. The parquet file has two dictionary pages
    // OR
    // 2. The parquet file does not have the dictionary as the first data page.
    // Both are errors in the parquet file.
    if (dictionary_header_encountered_) {
      return Status( Substitute("Corrupt Parquet file '$0': multiple dictionary pages "
            "for column '$1'", filename(), schema_name_));
    } else {
      return Status(Substitute("Corrupt Parquet file: '$0': dictionary page for "
                "column '$1' is not the first page", filename(), schema_name_));
    }
  }
  RETURN_IF_ERROR(AdvanceStream(header_size));
  parent_->AddAsyncReadBytesCounter(header_size);
  current_page_header_ = header;
  header_initialized_ = true;
  page_headers_read_++;
  dictionary_header_encountered_ = dictionary_header_encountered_ || is_dictionary;
  state_ = State::ToReadData;
  return Status::OK();
}

Status ParquetPageReader::ReadPageData(uint8_t** data) {
  DCHECK_EQ(state_, State::ToReadData);
  Status status;
  if (!stream_->ReadBytes(current_page_header_.compressed_page_size, data, &status)) {
    DCHECK(!status.ok());
    return status;
  }
  parent_->AddAsyncReadBytesCounter(current_page_header_.compressed_page_size);
  state_ = State::ToReadHeader;
  return Status::OK();
}

Status ParquetPageReader::SkipPageData() {
  DCHECK_EQ(state_, State::ToReadData);
  RETURN_IF_ERROR(AdvanceStream(current_page_header_.compressed_page_size));
  parent_->AddSkippedReadBytesCounter(current_page_header_.compressed_page_size);
  state_ = State::ToReadHeader;
  return Status::OK();
}

Status ParquetPageReader::AdvanceStream(int64_t bytes) {
  Status status;
  if (!stream_->SkipBytes(bytes, &status)) return status;
  return Status::OK();
}

std::ostream& operator<<(std::ostream& out, const ParquetPageReader::State state) {
  switch (state) {
    case ParquetPageReader::State::Uninitialized: out << "Uninitialized"; break;
    case ParquetPageReader::State::Initialized: out << "Initialized"; break;
    case ParquetPageReader::State::ToReadHeader: out << "ToReadHeader"; break;
    case ParquetPageReader::State::ToReadData: out << "ToReadData"; break;
  }
  return out;
}

} // namespace impala
