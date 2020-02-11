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

#pragma once

#include "exec/parquet/hdfs-parquet-scanner.h"

namespace impala {

/// A class used to read Parquet page headers and data pages. Memory management is not
/// handled by this class.  Before reading, InitColumnChunk() followed by StartScan() must
/// be called.
class ParquetPageReader {
 public:
  const char* filename() const { return parent_->filename(); }
  io::ScanRange* scan_range() const { return scan_range_; }
  ScannerContext::Stream* stream() const { return stream_; }
  uint64_t PageHeadersRead() const { return page_headers_read_; }

  ParquetPageReader(HdfsParquetScanner* parent, std::string schema_name)
      : parent_(parent),
        schema_name_(schema_name)
  {
  }

  /// Resets the reader for each row group in the file and creates the scan range for the
  /// column, but does not start it. To start scanning, call StartScan().
  Status InitColumnChunk(const HdfsFileDesc& file_desc,
      const parquet::ColumnChunk& col_chunk, int row_group_idx,
      std::vector<io::ScanRange::SubRange>&& sub_ranges);

  /// Starts the column scan range. InitColumnChunk() needs to be called before this. This
  /// method must be called before any of the column data can be read (including
  /// dictionary and data pages). 'io_reservation' is the amount of reservation assigned
  /// to the column. Returns an error status if there was an error starting the scan or
  /// allocating buffers for it.
  Status StartScan(int io_reservation);

  /// Reads the next page header if the data belonging to the current header has been read
  /// or skipped, otherwise does nothing. If the stream reaches the end before reading a
  /// complete page header, '*eos' is set to true.
  Status ReadPageHeader(bool* eos);

  /// Reads the next data page to 'data'. The header must already be read. It is invalid
  /// to call this or SkipPageData() again without reading the next header.
  Status ReadPageData(uint8_t** data);

  /// Skips the next data page. The header must already be read. It is invalid to call
  /// this or ReadPageData() again without reading the next header.
  Status SkipPageData();

  const parquet::PageHeader& CurrentPageHeader() const {
    DCHECK(header_initialized_);
    return current_page_header_;
  }

 private:
  Status AdvanceStream(int64_t bytes);

  HdfsParquetScanner* parent_;
  std::string schema_name_;

  /// Header for current data page.
  parquet::PageHeader current_page_header_;
  bool header_initialized_ = false;

  /// The scan range for the column's data. Initialized for each row group by Reset().
  io::ScanRange* scan_range_ = nullptr;

  /// Stream used to read data from 'scan_range_'. Initialized by StartScan().
  ScannerContext::Stream* stream_ = nullptr;

  uint64_t page_headers_read_ = 0;
  bool dictionary_header_encountered_ = false;

  /// We maintain a state machine for debug purposes. The state transitions are
  /// the following:
  ///
  ///                              Uninitialized
  ///                                    +
  ///                                    |
  ///                                    |  (InitColumnChunk)
  ///                                    |
  ///                                    v
  ///                   +-------->  Initialized
  ///                   |                +
  ///                   |                |
  ///                   |                |     (StartScan)
  ///                   |                |
  ///                   |                v
  /// (InitColumnChunk) +--------+ ToReadHeader <--------------+
  ///                   |                +                     |
  ///                   |                |                     |
  ///                   |                |   (ReadPageHeader)  | (ReadPageData,
  ///                   |                |                     |  SkipPageData)
  ///                   |                v                     |
  ///                   +--------+  ToReadData  +--------------+
  ///
  enum class State {
    Uninitialized,
    Initialized,
    ToReadHeader,
    ToReadData,
  };

  friend
  std::ostream& operator<<(std::ostream& out, const ParquetPageReader::State state);

  State state_ = State::Uninitialized;
};

} // namespace impala
