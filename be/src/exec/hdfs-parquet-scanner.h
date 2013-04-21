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


#ifndef IMPALA_EXEC_HDFS_PARQUET_SCANNER_H
#define IMPALA_EXEC_HDFS_PARQUET_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "util/integer-array.h"
#include "exec/parquet-common.h"

namespace impala {

struct HdfsFileDesc;

// This scanner parses Parquet files located in HDFS, and writes the
// content as tuples in the Impala in-memory representation of data, e.g.
// (tuples, rows, row batches).
// For the file format spec, see: github.com/Parquet/parquet-format
//
// Parquet (and other columnar formats) use scanner ranges differently than
// other formats.  Each materialized column maps to a single ScanRange.  For
// streaming reads, all the columns need to be read in parallel.  Most of the heavy
// lifting is done in the io mgr using the "grouped ranges" functionality.
// Like the other scanners, each parquet scanner object is one to one with a 
// ScannerContext.
// Unlike the other scanners though, the context will have multiple streams, one for 
// each column.  When the HdfsParquetScanner object is created, it is passed the
// ScannerContext has only 1 stream just for the metadata.  After parsing the
// metadata, the ScannerContext will create one stream per column.
class HdfsParquetScanner : public HdfsScanner {
 public:
  HdfsParquetScanner(HdfsScanNode* scan_node, RuntimeState* state);

  virtual ~HdfsParquetScanner();
  virtual Status Prepare();
  virtual Status Close();
  virtual Status ProcessSplit(ScannerContext* context);
  static void IssueInitialRanges(HdfsScanNode*, const std::vector<HdfsFileDesc*>&);

 private:
  // Size of the file footer.  This is a guess.  If this value is too little, we will
  // need to issue another read.
  static const int FOOTER_SIZE = 100 * 1024;

  // Max page header size in bytes.
  static const int MAX_PAGE_HEADER_SIZE = 100;

  // Per column reader.
  class ColumnReader;
  friend class ColumnReader;

  // Column reader for each materialized columns for this file.
  std::vector<ColumnReader*> column_readers_;

  // File metadata thrift object
  parquet::FileMetaData file_metadata_;
  
  // The scan range group for this scanner.
  DiskIoMgr::ScanRangeGroup scan_range_group_;

  // Returned in ProcessSplit
  Status parse_status_;

  // Timer for materializing rows.  This ignores time getting the next buffer.
  ScopedTimer<MonotonicStopWatch> assemble_rows_timer_;
  
  // Time spent decompressing bytes
  RuntimeProfile::Counter* decompress_timer_;
  
  // Number of cols that need to be read.
  RuntimeProfile::Counter* num_cols_counter_;

  // Reads data from all the columns (in parallel) and assembles rows into the context
  // object.
  // Returns when the entire row group is complete or an error occurred.
  Status AssembleRows();

  // Process the file footer and parse file_metadata_.  This should be called with the
  // last FOOTER_SIZE bytes in context_.
  // *eosr is a return value.  If true, the scan range is complete (e.g. select count(*))
  Status ProcessFooter(bool* eosr);

  // Walks file_metadata_ and initiates reading the materialized columns.  This 
  // initializes column_readers_ and issues the reads for the columns.
  Status InitColumns();

  // Validates the file metadata
  Status ValidateFileMetadata();

  // Validates the column metadata at 'col_idx' to make sure this column is supported 
  // (e.g. encoding, type, etc) and matches the type for the slot at 'slot_idx'
  Status ValidateColumn(int slot_idx, int col_idx);
};

} // namespace impala

#endif 
