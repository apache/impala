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

#include <boost/scoped_ptr.hpp>

#include "exec/parquet/hdfs-parquet-scanner.h"
#include "exec/parquet/parquet-page-reader.h"

namespace impala {

class Codec;
class MemPool;
class ScopedBuffer;

/// A class to read data from Parquet pages. It handles the page headers, decompression
/// and the possible copying of the data buffers.
/// Before reading, InitColumnChunk(), set_io_reservation() and StartScan() must be called
/// in this order.
class ParquetColumnChunkReader {
 public:

  /// An enum containing information about the type and/or intended use of the values. It
  /// is used to make decisions about memory management, for example when a buffer needs
  /// to be copied.
  /// In the future, more variants could be added if a new use case needs different memory
  /// management.
  enum class ValueMemoryType {
    /// The values will not be read.
    NO_SLOT_DESC,

    /// Scalar (non-string) values.
    SCALAR,
    FIXED_LEN_STR,
    VAR_LEN_STR
  };

  const char* filename() const { return parent_->filename(); }

  const parquet::PageHeader& CurrentPageHeader() const {
    return page_reader_.CurrentPageHeader();
  }

  io::ScanRange* scan_range() const { return page_reader_.scan_range(); }
  parquet::PageType::type page_type() const { return CurrentPageHeader().type; }
  ScannerContext::Stream* stream() const { return page_reader_.stream(); }

  parquet::Encoding::type encoding() const {
    return CurrentPageHeader().data_page_header.encoding;
  }

  /// Moved to implementation to be able to forward declare class in scoped_ptr.
  ParquetColumnChunkReader(HdfsParquetScanner* parent, std::string schema_name,
      int slot_id, ValueMemoryType value_mem_type);
  ~ParquetColumnChunkReader();

  /// Resets the reader for each row group in the file and creates the scan
  /// range for the column, but does not start it. To start scanning,
  /// set_io_reservation() must be called to assign reservation to this
  /// column, followed by StartScan().
  Status InitColumnChunk(
      const HdfsFileDesc& file_desc, const parquet::ColumnChunk& col_chunk,
      int row_group_idx, std::vector<io::ScanRange::SubRange>&& sub_ranges);

  void set_io_reservation(int bytes) {
    io_reservation_ = bytes;
  }

  /// Starts the column scan range. InitColumnChunk() has to have been called and the
  /// reader must have a reservation assigned via set_io_reservation(). This must be
  /// called before any of the column data can be read (including dictionary and data
  /// pages). Returns an error status if there was an error starting the scan or
  /// allocating buffers for it.
  Status StartScan();

  /// If the column type is a variable length string and 'mem_pool' is not NULL, transfers
  /// the remaining resources backing tuples to 'mem_pool' and frees up other resources.
  /// Otherwise frees all resources.
  void Close(MemPool* mem_pool);

  /// The following functions can all advance stream_, which invalidates the buffer
  /// returned by the previous call (unless copy_buffer was true).

  /// Checks whether the next page is a dictionary page and if it is, reads the header and
  /// either reads or skips the dictionary data, depending on 'skip_data'.
  ///
  /// After this method returns, the value of '*is_dictionary' can be used to determine
  /// whether the page was a dictionary page.
  /// If the data is read, '*dict_values' is set to point to the data and '*data_size' is
  /// set to the length of the data. '*num_entries' is set to the number of elements. If
  /// the column type is a string, then the buffer is allocated from the scanner's
  /// dictionary_pool_ and will be valid as long as the scanner lives. Otherwise the
  /// returned buffer will be valid only until the next function call that advances the
  /// buffer.
  /// 'uncompressed_buffer' is used to store data if a temporary buffer is needed for
  /// decompression.
  Status TryReadDictionaryPage(bool* is_dictionary_page, bool* eos, bool skip_data,
      ScopedBuffer* uncompressed_buffer, uint8_t** dict_values,
      int64_t* data_size, int* num_entries);

  /// Reads the next data page to '*data' and '*data_size', if 'read_data' is true.
  /// Else reads page header only, following which client should either call
  /// 'ReadDataPageData' or 'SkipPageData'.
  /// Skips other types of pages (except for dictionary) until it finds a data page. If it
  /// finds a dictionary page, returns an error as the dictionary page should be the first
  /// page and this method should only be called if a data page is expected.
  /// If the stream reaches the end before reading a complete page header, '*eos' is set
  /// to true.
  Status ReadNextDataPage(
      bool* eos, uint8_t** data, int* data_size, bool read_data = true);

  /// If the column type is a variable length string, transfers the remaining resources
  /// backing tuples to 'mem_pool' and frees up other resources. Otherwise frees all
  /// resources.
  void ReleaseResourcesOfLastPage(MemPool& mem_pool);

  /// Skips the data part of the page. The header must be already read.
  Status SkipPageData();

  /// Reads the data part of the next data page. Sets '*data' to point to the buffer and
  /// '*data_size' to its size.
  /// If the column type is a variable length string, the buffer is allocated from
  /// data_page_pool_. Otherwise the returned buffer will be valid only until the next
  /// function call that advances the buffer.
  Status ReadDataPageData(uint8_t** data, int* data_size);

 private:
  HdfsParquetScanner* parent_;
  std::string schema_name_;

  ParquetPageReader page_reader_;

  /// Used to track reads in the scanners counters.
  int slot_id_;

  /// Pool to allocate storage for data pages from - either decompression buffers for
  /// compressed data pages or copies of the data page with var-len data to attach to
  /// batches.
  boost::scoped_ptr<MemPool> data_page_pool_;

  /// Reservation in bytes to use for I/O buffers in 'scan_range_'/'stream_'. Must be set
  /// with set_io_reservation() before 'stream_' is initialized. Reset for each row group
  /// by Reset().
  int64_t io_reservation_ = 0;

  boost::scoped_ptr<Codec> decompressor_;

  /// See TryReadDictionaryPage() for information about the parameters.
  Status ReadDictionaryData(ScopedBuffer* uncompressed_buffer, uint8_t** dict_values,
      int64_t* data_size, int* num_entries);

  /// Allocate memory for the uncompressed contents of a data page of 'size' bytes from
  /// 'data_page_pool_'. 'err_ctx' provides context for error messages. On success,
  /// 'buffer' points to the allocated memory. Otherwise an error status is returned.
  Status AllocateUncompressedDataPage(
      int64_t size, const char* err_ctx, uint8_t** buffer);

  ValueMemoryType value_mem_type_;
};

} // namespace impala
