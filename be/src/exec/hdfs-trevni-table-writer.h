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


#ifndef IMPALA_EXEC_HDFS_TEXT_TREVNI_WRITER_H
#define IMPALA_EXEC_HDFS_TEXT_TREVNI_WRITER_H

#include "exec/data-sink.h"

#include <hdfs.h>
#include <boost/scoped_ptr.hpp>

#include "util/integer-array.h"
#include "util/compress.h"
#include "runtime/descriptors.h"
#include "exec/hdfs-table-writer.h"
#include "exec/trevni-def.h"

namespace impala {

class Expr;
class TupleDescriptor;
class TupleRow;
class RuntimeState;
class OutputPartition;

// The writer consumes all rows passed to it and writes the evaluated output_exprs
// as a Trevni format Hdfs File.
class HdfsTrevniTableWriter : public HdfsTableWriter {
 public:
  HdfsTrevniTableWriter(RuntimeState* state, OutputPartition* output_partition,
                        const HdfsPartitionDescriptor* part_desc,
                        const HdfsTableDescriptor* table_desc,
                        const std::vector<Expr*>& output_exprs);

  ~HdfsTrevniTableWriter() { }

  // Appends trevni representation of rows in the batch to the current file.
  virtual Status AppendRowBatch(RowBatch* batch,
                                const std::vector<int32_t>& row_group_indices,
                                bool* new_file);

  // Initialize column information.
  virtual Status Init();

  // Write out all the data.
  virtual Status Finalize();

 private:
  static const int BLOCK_SIZE = 64 * 1024;

  // Per-block information.
  struct TrevniBlockInfo {
    TrevniBlockInfo()
        : row_count(0),
          size(0),
          previous_size(0),
          compressed_size(0),
          first_value(NULL),
          data(NULL),
          compressed_data(NULL) {
    }

    // Number of rows in this block;
    int32_t row_count; 

    // Size of the block.
    int32_t size; 

    // Size of the block before the last append.
    int32_t previous_size; 

    // Compressed (on disk) size.
    int32_t compressed_size; 

    // first value in block.
    void* first_value; 

    // Repetition and definition levels are used for optional, repeated and
    // nested data.  This is from the Dremel system:
    // http://sergey.melnix.com/pub/melnik_VLDB10.pdf
    // TODO: support nested and repeated data.  Currently only a definition level
    // of 1 or 0 is supported where a level of 1 implies that the column is nullable.

    // Array of definition levels.
    IntegerArrayBuilder def_level;

    // Array of repetition levels.
    IntegerArrayBuilder rep_level;

    // Array of boolean bits for columns of type TREVNI_BOOL
    IntegerArrayBuilder bool_column;

    // block data.
    uint8_t* data;

    // Compressed data.
    uint8_t* compressed_data;
  };

  // Per-column Information.
  struct TrevniColumnInfo {
    TrevniColumnInfo() 
        : type(TREVNI_UNDEFINED),
          type_length(0),
          has_values(false),
          is_array(false),
          max_def_level(0),
          max_rep_level(0),
          has_crc(false),
          compressor(NULL),
          current_value(NULL) {
      }

    // Offset of this column in file.
    int64_t start;

    // Type of this column.
    TrevniType type;

    // Length of the column data type, 0 implies variable length;
    int type_length;

    // Name of column.
    std::string name;

    // Name of parent column, if any.
    std::string parent;

    // True if the initial value of a block is recorded.
    bool has_values; 

    // True if this column is an array.
    bool is_array;

    // Maximum definition level.
    int max_def_level;

    // Maximum repetition level.
    int max_rep_level;

    // True if column has CRC;
    bool has_crc;

    // Per-column compressor, if any.
    Codec* compressor;

    // Vector of block descriptors.
    std::vector<TrevniBlockInfo> block_desc;

    // Pointer into the column buffer, pointed to by TrevniBlock.data,
    // for putting the column value in the current row.
    uint8_t* current_value;

    // Limit on the number of data bytes to go into the current buffer.
    int32_t limit;
  };

  // Create and return a new initialized block descriptor and data buffer
  // pointed to by blockp.
  // If the current block needs to be compressed that will be done first.
  // This routine will check that the additional metadata needed for this
  // block will not exceed the file size limit.  If it does blockp is
  // returned as NULL.
  // Updates bytes_added_.
  Status  CreateNewBlock(TrevniColumnInfo* column, TrevniBlockInfo** blockp);

  // Write the file header information to the output file.
  Status WriteFileHeader();

  // Update bytes_added_ with the maximum size the file header could be.
  // We only need to ensure that we do not write a file bigger than is
  // specified by file_limit_.  This is done using the largest possible sizes
  // for ZInts and type names.
  void SetHeaderFileSize();

  // Write the file metadata to the output file.
  Status WriteFileMetadata();

  // Write the meta data for each column to the output file.
  Status WriteColumnMetadata(TrevniColumnInfo* column);

  // Write column data to the output file.
  // Write the block descriptor information followed by the block data.
  Status WriteColumn(TrevniColumnInfo* column);

  // Write descriptor for a block to the output file.
  Status WriteBlockDescriptor(TrevniColumnInfo* column, TrevniBlockInfo* block);

  // Write level and definition arrays and data for a block to the output file.
  Status WriteBlock(TrevniColumnInfo* column, TrevniBlockInfo* block);

  // Set the definition level to 1 if the current value is not null.
  // If the array is full allocate a new block.
  Status SetNull(TrevniColumnInfo* column, TrevniBlockInfo** blockp, bool null);

  // Append a variable length value to the block in this column.
  // Allocate a new block if the current one is full.
  // input:
  //   value -- value to put
  //   column -- descriptor of the column, may have new block allocated.
  // in/out:
  //   blockp -- block to append to.  Set to newly allocated block if needed, 
  //             NULL if a new block was needed and could not be allocated.
  // Updates bytes_added_.
  Status AppendVariableLengthValue(void* value,
                                   TrevniColumnInfo* column, TrevniBlockInfo** blockp);

  // Append a fixed length value to the block in this column.
  // Allocate a new block if the current one is full.
  // input:
  //   value -- value to put
  //   column -- descriptor of the column, may have new block allocated.
  // in/out:
  //   blockp -- block to append to.  Set to newly allocated block if needed, 
  //             NULL if a new block was needed and could not be allocated.
  // Updates bytes_added_.
  Status AppendFixedLengthValue(void* value,
                                TrevniColumnInfo* column, TrevniBlockInfo** block);

  // Compress a block of data.
  // First the definition and repetition arrays, if any are copied
  // to the end of the block.  bytes_added_ is updated with the space
  // saved by the compression.  block->data will point to the compressed
  // data on return.
  Status CompressBlock(TrevniColumnInfo* column, TrevniBlockInfo* block);

  // Reset all columns at the end of a file so that another file can be opened.
  Status ResetColumns();

  // Write a string in Trevni format to current output file.
  Status WriteString(const std::string& str);

  // Write bytes in Trevni format to current output file.
  Status WriteBytes(const std::vector<uint8_t>& bytes);

  // Write an integer in little endian format to current output file.
  Status WriteInt(int32_t integer);

  // Write a long integer in little endian format to current output file.
  Status WriteLong(int64_t longint);

  // Write an integer in Trevni format to current output file.
  Status WriteZInt(int32_t integer);

  // Write a long integer in Trevni format to current output file.
  Status WriteZLong(int64_t longint);

  // array of pointers to column information.
  std::vector<TrevniColumnInfo> columns_;

  // Number of columns.
  int32_t column_count_;

  // Number of rows in the file
  int64_t row_count_;

  // Current upper bound total size of the file.
  // If this size would become greater than file_limit_ the current data
  // is written and a new file is started.
  int64_t bytes_added_;

  // Limit on the total size of the file.
  int64_t file_limit_;

  // Default compressor type for the file.
  THdfsCompression::type compression_;

  // Default compressor name for the file.
  std::string compression_name_;

  // Default compressor if any.
  Codec* compressor_;

  // Memory for column/block buffers
  boost::scoped_ptr<MemPool> col_mem_pool_;

  // Object pool for holding column/file compressors.
  boost::scoped_ptr<ObjectPool> object_pool_;

  // Current position in the batch being written.  This must be persistent across
  // calls since the writer may stop in the middle of a row batch and ask for a new
  // file.
  int32_t row_idx_;
};

}
#endif
