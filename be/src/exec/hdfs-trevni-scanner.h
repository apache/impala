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


#ifndef IMPALA_EXEC_HDFS_TREVNI_SCANNER_H
#define IMPALA_EXEC_HDFS_TREVNI_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "exec/delimited-text-parser.h"
#include "util/integer-array.h"

namespace impala {

// This scanner parses Trevni file located in HDFS, and writes the
// content as tuples in the Impala in-memory representation of data, e.g.
// (tuples, rows, row batches).
class HdfsTrevniScanner : public HdfsScanner {
 public:
  HdfsTrevniScanner(HdfsScanNode* scan_node, RuntimeState* state, MemPool* tuple_pool);

  virtual ~HdfsTrevniScanner();
  virtual Status Prepare();
  virtual Status GetNext(RowBatch* row_batch, bool* eosr);
  virtual Status Close();

 private:
  // Per-block Information.
  struct TrevniBlockInfo {
    TrevniBlockInfo()
        : row_count(),
          size(0),
          compressed_size(0),
          first_value(NULL) {
    }
    // Number of rows in this block;
    int32_t row_count; 

    // Size of the block in bytes.
    int32_t size; 

    // Compressed (on disk) size in bytes.
    int32_t compressed_size; 

    // first value in block.
    // TODO: implement optional storing of first value.
    void* first_value; 
  };

  // Per-column Information.
  struct TrevniColumnInfo {
    TrevniColumnInfo()
        : current_offset(0),
          current_block(0),
          current_row_count(0),
          type(TREVNI_UNDEFINED),
          has_values(false),
          is_array(false),
          max_rep_level(0),
          max_def_level(0),
          decompressor(NULL),
          current_buffer_size(0),
          buffer(NULL) {
    }

    // Return if the current value in the column is null.
    bool ValueIsNull() {
      return (max_def_level == 0) ? false : def_level.GetNextValue() < max_def_level;
    }

    // Offset of the start of the column in the file or of the
    // next block we will read.
    int64_t current_offset;

    // Index of the current block being scanned.
    int32_t current_block;

    // Number of rows left in current block.
    int32_t current_row_count;

    // Type of column.
    TrevniType type;

    // Length of the column, 0 implies variable length;
    int length;

    // Name of column.
    std::string name;

    // Name of parent column, if any.
    std::string parent;

    // True if the initial value of a block is recorded.
    bool has_values; 

    // True if this column is an array.
    bool is_array;

    // Repetition and definition levels are used for optional, repeated and
    // nested data.  This is from the Dremel system:
    // http://sergey.melnix.com/pub/melnik_VLDB10.pdf
    // TODO: support nested and repeated data.  Currently only a definition level
    // of 1 or 0 is supported where a level of 1 implies that the column is nullable.
    //
    // Maximum repetition level.
    int max_rep_level;

    // Maximum definition level.
    int max_def_level;

    // Per-column decompressor, if any.
    Codec* decompressor;

    // Descriptor for each block.
    std::vector<TrevniBlockInfo> block_desc;

    // Memory pool for buffer for this column. If the column contains strings
    // then the memory may get passed to the row batch.
    MemPool* mem_pool;

    // allocated size of buffer.
    uint32_t current_buffer_size;

    // buffer holding current block.
    uint8_t* buffer;

    // Pointer into buffer containing the current row value
    uint8_t* current_value;

    // If we are not compacting strings and this column has strings, note it here.
    bool has_noncompact_strings;

    // Array of repetition levels.
    IntegerArray rep_level;

    // Array of definition levels.
    IntegerArray def_level;

    // We store TREVNI_BOOL values as a bit array, this field is used to
    // read the array.
    IntegerArray bool_column;
  };

  // Initialises any state required at the beginning of a new scan range.
  virtual Status InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition,
                                      DiskIoMgr::ScanRange* scan_range, 
                                      Tuple* template_tuple, ByteStream* byte_stream);

  // Read the current Trevni file header from the beginning of the file.
  // Verifies:
  //   version number
  // Sets:
  //   row_count_
  //   column_count_
  //   column_start_
  // Calls ReadFileHeaderMetadata
  // Calls ReadColumnMetadata
  Status ReadFileHeader();

  // Read the Trevni file Header Metadata section in the current file.
  // Sets:
  //   decompressor_
  //   file_checksum_
  Status ReadFileHeaderMetadata();

  // Read the Column metatdata for the current column.
  // Input:
  //   column_number: column number whose metadata is to be read.
  // Sets:
  //   column_info_[column_number];
  Status ReadColumnMetadata(int column_number);

  // Skip over metadata for columns we don't read.
  Status SkipColumnMetadata();

  // Seek to the column start and read the column information.
  // Sets information in the TrevniColumnInfo for that column.
  // Calls ReadBlockDescriptor for each block in the column.
  Status ReadColumnInfo(TrevniColumnInfo* column);

  // Read a Block Descriptor.
  // Sets TrevniBlockInfo for that block.
  Status ReadBlockDescriptor(const TrevniColumnInfo& column, TrevniBlockInfo* block);

  // Read a column data block.  Decompress if necessary. Set up levels arrays.
  Status ReadBlock(const TrevniBlockInfo& column, TrevniBlockInfo* block);

  // Read the starting value of a block. Not implemented yet.
  Status ReadValue(const TrevniColumnInfo& column, void** value);

  // Create a decompressor
  // Translates the Trevni Codec names to the class names used by Hadoop.
  // Input:
  //   value: codec value from metadata.
  // Output:
  //   decompressor: instance of codec class to use.
  Status CreateDecompressor(const std::vector<uint8_t>& value,
                            Codec** decompressor);

  // File read error reporting. Logs file name and msg.
  Status FileReadError(const std::string& msg);

  // Read the current block for column.
  Status ReadCurrentBlock(TrevniColumnInfo* column);

  // The default decompressor class to use.
  Codec* decompressor_;

  // Number of rows in file.
  int64_t row_count_;

};

} // namespace impala

#endif // IMPALA_EXEC_HDFS_TREVNI_SCANNER_H
