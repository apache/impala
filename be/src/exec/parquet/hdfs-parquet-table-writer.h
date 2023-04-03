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


#ifndef IMPALA_EXEC_HDFS_PARQUET_TABLE_WRITER_H
#define IMPALA_EXEC_HDFS_PARQUET_TABLE_WRITER_H

#include "exec/table-sink-base.h"

#include <hdfs.h>
#include <map>
#include <boost/scoped_ptr.hpp>

#include "exec/hdfs-table-writer.h"
#include "exec/parquet/parquet-common.h"
#include "runtime/descriptors.h"
#include "util/compress.h"

#include "gen-cpp/control_service.pb.h"

namespace impala {

class Expr;
struct OutputPartition;
class RuntimeState;
class ThriftSerializer;
class TupleRow;

/// The writer consumes all rows passed to it and writes the evaluated output_exprs
/// as a parquet file in hdfs.
/// TODO: (parts of the format that are not implemented)
/// - group var encoding
/// - compression
/// - multiple row groups per file
/// TODO: we need a mechanism to pass the equivalent of serde params to this class
/// from the FE.  This includes:
/// - compression & codec
/// - type of encoding to use for each type

class HdfsParquetTableWriter : public HdfsTableWriter {
 public:
  HdfsParquetTableWriter(TableSinkBase* parent, RuntimeState* state,
      OutputPartition* output_partition, const HdfsPartitionDescriptor* part_desc,
      const HdfsTableDescriptor* table_desc);

  ~HdfsParquetTableWriter();

  /// Initialize column information.
  virtual Status Init() override;

  /// Initializes a new file.  This resets the file metadata object and writes
  /// the file header to the output file.
  virtual Status InitNewFile() override;

  /// Appends parquet representation of rows in the batch to the current file.
  virtual Status AppendRows(RowBatch* batch,
      const std::vector<int32_t>& row_group_indices, bool* new_file) override;

  /// Write out all the data.
  virtual Status Finalize() override;

  virtual void Close() override;

  /// Returns the target HDFS block size to use.
  virtual uint64_t default_block_size() const override { return default_block_size_; }

  virtual std::string file_extension() const override { return "parq"; }

  int32_t page_row_count_limit() const { return page_row_count_limit_; }
  int64_t default_plain_page_size() const { return default_plain_page_size_; }
  int64_t dict_page_size() const { return dict_page_size_; }

 private:
  /// Default data page size. In bytes.
  static const int DEFAULT_DATA_PAGE_SIZE = 64 * 1024;

  /// Max data page size. In bytes.
  /// TODO: May need to be increased after addressing IMPALA-1619.
  static const int64_t MAX_DATA_PAGE_SIZE = 1024 * 1024 * 1024;

  /// Default hdfs block size. In bytes.
  static const int HDFS_BLOCK_SIZE = 256 * 1024 * 1024;

  /// Align block sizes to this constant. In bytes.
  static const int HDFS_BLOCK_ALIGNMENT = 1024 * 1024;

  /// Default row group size.  In bytes.
  static const int ROW_GROUP_SIZE = HDFS_BLOCK_SIZE;

  /// Minimum file size.  If the configured size is less, fail.
  static const int HDFS_MIN_FILE_SIZE = 8 * 1024 * 1024;

  /// Maximum statistics size. If the size of a single thrift parquet::Statistics struct
  /// for a page or row group exceed this value, we'll not write it. We use the same value
  /// as 'parquet-mr'.
  static const int MAX_COLUMN_STATS_SIZE = 4 * 1024;

  /// In parquet::ColumnIndex we store the min and max values for each page.
  /// However, we don't want to store very long strings, so we truncate them.
  /// The value of it must not be too small, since we don't want to truncate
  /// non-string values.
  static const int PAGE_INDEX_MAX_STRING_LENGTH = 64;

  /// Per-column information state.  This contains some metadata as well as the
  /// data buffers.
  class BaseColumnWriter;
  friend class BaseColumnWriter;

  template<typename T> class ColumnWriter;
  template<typename T> friend class ColumnWriter;
  class BoolColumnWriter;
  friend class BoolColumnWriter;
  class Int64TimestampColumnWriterBase;
  friend class Int64TimestampColumnWriterBase;
  class Int64MicroTimestampColumnWriter;
  friend class Int64MicroTimestampColumnWriter;
  class Int64MilliTimestampColumnWriter;
  friend class Int64MilliTimestampColumnWriter;
  class Int64NanoTimestampColumnWriter;
  friend class Int64NanoTimestampColumnWriter;

  /// Minimum allowable block size in bytes. This is a function of the number of columns
  /// in the target file.
  int64_t MinBlockSize(int64_t num_file_cols) const;

  /// Fills in the schema portion of the file metadata, converting the schema in
  /// table_desc_ into the format in the file metadata
  Status CreateSchema();

  /// Writes the file header information to the output file.
  Status WriteFileHeader();

  /// Writes the column index and offset index of each page in the file.
  /// It also resets the column writers.
  Status WritePageIndex();

  /// Writes the file metadata and footer.
  Status WriteFileFooter();

  /// Writes the ParquetBloomFilter of 'col_writer' if it has one, including the header,
  /// and updates '*meta_data'.
  Status WriteParquetBloomFilter(BaseColumnWriter* col_writer,
      parquet::ColumnMetaData* meta_data) WARN_UNUSED_RESULT;

  /// Add per-column statistics to 'iceberg_file_stats_' for the current data file.
  void CollectIcebergDmlFileColumnStats(int field_id, const BaseColumnWriter* col_writer);

  /// Flushes the current row group to file.  This will compute the final
  /// offsets of column chunks, updating the file metadata.
  Status FlushCurrentRowGroup();

  /// Adds a row group to the metadata and updates current_row_group_ to the
  /// new row group.  current_row_group_ will be flushed.
  Status AddRowGroup();

  /// Configures writer for non-Iceberg tables.
  /// 'num_cols' is the number of non-partitioning columns in the target table.
  /// Selects the Parquet timestamp type to be used by this writer.
  /// Sets 'string_utf8_' based on query options and table type.
  /// Sets 'default_block_size_', 'default_plain_page_size_' and 'dict_page_size_'.
  void Configure(int num_cols);

  /// Configures writer for Iceberg tables.
  /// 'num_cols' is the number of non-partitioning columns in the target table.
  /// Selects the Parquet timestamp type to be used by this writer.
  /// Sets 'string_utf8_' to true.
  /// Sets 'default_block_size_', 'default_plain_page_size_' and 'dict_page_size_'.
  void ConfigureForIceberg(int num_cols);

  /// Updates output partition with some summary about the written file.
  void FinalizePartitionInfo();

  /// Thrift serializer utility object.  Reusing this object allows for
  /// fewer memory allocations.
  boost::scoped_ptr<ThriftSerializer> thrift_serializer_;

  /// File metdata thrift description.
  parquet::FileMetaData file_metadata_;

  /// The current row group being written to.
  parquet::RowGroup* current_row_group_;

  /// Array of pointers to column information. The column writers are owned by the
  /// table writer, as there is no reason for the column writers to outlive the table
  /// writer.
  std::vector<std::unique_ptr<BaseColumnWriter>> columns_;

  /// Number of rows in current file
  int64_t row_count_;

  /// Current estimate of the total size of the file.  The file size estimate includes
  /// the running size of the (uncompressed) dictionary, the size of all finalized
  /// (compressed) data pages and their page headers.
  /// If this size exceeds file_size_limit_, the current data is written and a new file
  /// is started.
  int64_t file_size_estimate_;

  /// Limit on the total size of the file.
  int64_t file_size_limit_;

  /// The file location in the current output file.  This is the number of bytes
  /// that have been written to the file so far.  The metadata uses file offsets
  /// in a few places.
  int64_t file_pos_;

  /// Memory for column/block buffers that are reused for the duration of the
  /// writer (i.e. reused across files).
  boost::scoped_ptr<MemPool> reusable_col_mem_pool_;

  /// Memory for column/block buffers that is allocated per file.  We need to
  /// reset this pool after flushing a file.
  boost::scoped_ptr<MemPool> per_file_mem_pool_;

  /// Current position in the batch being written.  This must be persistent across
  /// calls since the writer may stop in the middle of a row batch and ask for a new
  /// file.
  int row_idx_;

  /// Staging buffer to use to compress data.  This is used only if compression is
  /// enabled and is reused between all data pages.
  std::vector<uint8_t> compression_staging_buffer_;

  /// For each column, the on disk size written.
  ParquetDmlStatsPB parquet_dml_stats_;

  /// Maximum row count written in a page.
  int32_t page_row_count_limit_ = std::numeric_limits<int32_t>::max();

  /// The Timestamp type used to write timestamp values.
  TParquetTimestampType::type timestamp_type_;

  /// True if we are writing an Iceberg data file. In that case the writer behaves a
  /// bit differently, e.g. writes specific type of timestamps, fills some extra metadata.
  bool is_iceberg_file_ = false;

  /// If true, STRING values are annotated with UTF8 in Parquet metadata.
  bool string_utf8_ = false;

  // File block size, set in Configure() or ConfigureForIceberg().
  int64_t default_block_size_;

  // Default plain page size, set in Configure() or ConfigureForIceberg().
  int64_t default_plain_page_size_;

  // Dictionary page size, set in Configure() or ConfigureForIceberg().
  int64_t dict_page_size_;
};

}
#endif
