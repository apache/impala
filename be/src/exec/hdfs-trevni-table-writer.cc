// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-trevni-table-writer.h"
#include "exec/read-write-util.h"
#include "exec/exec-node.h"
#include "util/hdfs-util.h"
#include "exprs/expr.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/primitive-type.h"

#include <vector>
#include <sstream>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <boost/assign/list_of.hpp>
#include <stdlib.h>

#include "gen-cpp/ImpalaService_types.h"

using namespace std;
using namespace boost;
using namespace boost::assign;

namespace impala {
HdfsTrevniTableWriter::HdfsTrevniTableWriter(RuntimeState* state, OutputPartition* output,
                                             const HdfsPartitionDescriptor* part_desc,
                                             const HdfsTableDescriptor* table_desc,
                                             const vector<Expr*>& output_exprs)
    : HdfsTableWriter(state, output, part_desc, table_desc, output_exprs),
      row_count_(0),
      file_limit_(0),
      compression_(part_desc->compression()),
      compressor_(NULL),
      col_mem_pool_(new MemPool),
      object_pool_(new ObjectPool),
      row_idx_(0) {
}

Status HdfsTrevniTableWriter::Init() {
  columns_.resize(table_desc_->num_cols() - table_desc_->num_clustering_cols());

  // Initialize compressor if any.
  if (compression_ != THdfsCompression::NONE) {
    RETURN_IF_ERROR(Codec::CreateCompressor(state_,
        col_mem_pool_.get(), false, compression_, &compressor_));
    compression_name_ = Codec::GetCodecName(compression_);
  }
  // Initialize each column structure.
  map<PrimitiveType, TrevniType>::const_iterator tm;
  for (int j = 0; j < columns_.size(); ++j) {
    tm = type_map_trevni.find(output_exprs_[j]->type());
    DCHECK(tm != type_map_trevni.end());
    columns_[j].type = tm->second;
    columns_[j].type_length = GetTrevniTypeLength(columns_[j].type);
    // Make up a name for the column.
    char buf[16];
    snprintf(buf, 16, "col_%d", j + 1);
    columns_[j].name = string(buf);

    // For now every column uses the same compression
    columns_[j].compressor = compressor_;

    // For now every column supports nulls
    columns_[j].max_def_level = 1;

    SetHeaderFileSize();
    // Allocate the first block buffer and descriptor
    TrevniBlockInfo* block;
    RETURN_IF_ERROR(CreateNewBlock(&columns_[j], &block));
  }
  return Status::OK;
}

Status HdfsTrevniTableWriter::ResetColumns() {
  for (int i = 0; i < columns_.size(); ++i) {
    columns_[i].block_desc.clear();
    TrevniBlockInfo* block;
    RETURN_IF_ERROR(CreateNewBlock(&columns_[i], &block));
  }
  return Status::OK;
}

void HdfsTrevniTableWriter::SetHeaderFileSize() {
  bytes_added_ = 0;
  bytes_added_ += sizeof(TREVNI_VERSION_HEADER);
  bytes_added_ += sizeof(int64_t);
  bytes_added_ += sizeof(columns_.size());
  // File metadata, room for the number of elements followed by the key value pair sizes.
  bytes_added_ += sizeof(int32_t);  
  if (compression_ != THdfsCompression::NONE) {
    // Strings have a ZInt length. So add its size, assuming the largest value
    // we could see.  1 byte represents up to 127 bytes.
    bytes_added_ += sizeof(TREVNI_CODEC) + 1;
    bytes_added_ += compression_name_.size() + 1;
  }
  for (int i = 0; i < columns_.size(); ++i) {
    // Column metadata, room for the number of elements followed by the key value
    // pair sizes.
    bytes_added_ += sizeof(int32_t);
    // Strings have a ZInt length. So add its size, the largest string length.
    bytes_added_ += sizeof(TREVNI_NAME) + 1;
    bytes_added_ += columns_[i].name.size() + 2;
    bytes_added_ += sizeof(TREVNI_TYPE) + 1;
    // Block count.
    bytes_added_ += sizeof(int32_t);
    // Just use the biggest type name.
    bytes_added_ += sizeof("timestamp") + 1;
    if (columns_[i].max_def_level > 0) {
      bytes_added_ += sizeof(TREVNI_DEFINITION) + 1;
      // assume the definition level is not more than 2 digits.
      bytes_added_ += sizeof("xx") + 1;
      if (columns_[i].max_rep_level > 0) {
        bytes_added_ += sizeof(TREVNI_REPETITION) + 1;
        // assume the repetition level is not more than 2 digits.
        bytes_added_ += sizeof("xx") + 1;
      }
    }
  }
}

Status HdfsTrevniTableWriter::CreateNewBlock(TrevniColumnInfo* column,
                                             TrevniBlockInfo** blockp) {
  *blockp = NULL;
  uint8_t* buffer = NULL;
  if (column->compressor != NULL && !column->block_desc.empty()) {
    TrevniBlockInfo* previous_block = &column->block_desc.back();
    buffer = previous_block->data;
    RETURN_IF_ERROR(CompressBlock(column, previous_block));
  }
  column->block_desc.resize(column->block_desc.size() + 1);
  TrevniBlockInfo* block = &column->block_desc.back();

  // If we are over the limit just return.
  bytes_added_ += sizeof(block->row_count) +
      sizeof(block->size) + sizeof(block->compressed_size);
  if (bytes_added_ > file_limit_ && file_limit_ != 0)  return Status::OK;

  // The limit is the amount of the block that is not taken up by the arrays.
  column->limit = BLOCK_SIZE;
  if (column->max_def_level > 0) {
    // TODO: Add rep_level initialization.
    int length = column->type_length;
    int count;
    // First calculate how big the definition level array will be and then how
    // much space is left in the block for data values.
    int size = IntegerArray::IntegerSize(column->max_def_level);
    if (length != 0) {
      count = BLOCK_SIZE / length;
    } else if (column->type == TREVNI_BOOL) {
      // Boolean is a bit array. The count is determined by the number of bits needed
      // for each element.
      count = (BLOCK_SIZE * 8) / (size + 1);
    } else if (length == 0) {
      if (column->block_desc.size() > 1) {
        // Use the average length of the previous block.
        TrevniBlockInfo* previous_block =
            &column->block_desc[column->block_desc.size() - 2];
        length = previous_block->size / previous_block->row_count;
      } else {
        // Guess small so we are not likely to run out.
        switch (column->type) {
          case TREVNI_INT:
            length = 2;
            break;

          case TREVNI_LONG:
            length = 4;
            break;

          default:
            length = 50;
            break;
        }
      }
      count = BLOCK_SIZE / length;
    }
    bytes_added_ += IntegerArray::ArraySize(size, count);
    if (bytes_added_ > file_limit_ && file_limit_ != 0)  return Status::OK;

    block->def_level = IntegerArrayBuilder(size, count, col_mem_pool_.get());
    column->limit -= IntegerArray::ArraySize(size, count);
  }

  // For boolean  we use a 1 bit integer array.
  if (column->type == TREVNI_BOOL) {
    block->bool_column = IntegerArrayBuilder(1, column->limit * 8, col_mem_pool_.get());
    block->data = block->bool_column.array();
  } else {
    if (buffer != NULL) {
      block->data = buffer;
    } else {
      block->data = col_mem_pool_->Allocate(BLOCK_SIZE);
    }
  }
  column->current_value = block->data;
  *blockp = block;
  return Status::OK;
}

Status HdfsTrevniTableWriter::CompressBlock(TrevniColumnInfo* column,
                                            TrevniBlockInfo* block) {
  // Copy the levels array to the end of the block.
  if (column->max_def_level > 0) {
    memcpy(block->data + block->size,
            block->def_level.array(), block->def_level.CurrentByteCount());
    block->size += block->def_level.CurrentByteCount();
    if (column->max_rep_level > 0) {
      memcpy(block->data + block->size,
              block->rep_level.array(), block->rep_level.CurrentByteCount());
      block->size += block->rep_level.CurrentByteCount();
    }
  }

  uint8_t* output_buf;
  block->compressed_size = 0;
  RETURN_IF_ERROR(column->compressor->ProcessBlock(
      block->size, block->data, &block->compressed_size, &output_buf));
  block->data = output_buf;
  // Remove the saved space from the file size.
  bytes_added_ -= block->size - block->compressed_size;
  return Status::OK;
}

inline Status HdfsTrevniTableWriter::SetNull(TrevniColumnInfo* column,
                                             TrevniBlockInfo** blockp, bool null) {
  // TODO: Should we reallocate the integer array when we run out?
  if (!(*blockp)->def_level.Put(!null)) {
    RETURN_IF_ERROR(CreateNewBlock(column, blockp));
    if (*blockp == NULL) return Status::OK;
    (*blockp)->def_level.Put(!null);
  }
  return Status::OK;
}

Status HdfsTrevniTableWriter::AppendVariableLengthValue(void* value,
                                                        TrevniColumnInfo* column,
                                                        TrevniBlockInfo** blockp) {
  int int_len = 0;
  int data_len = 0;
  switch (column->type) {
    case TREVNI_STRING:
    case TREVNI_BYTES: {
      const StringValue* string_val = reinterpret_cast<StringValue*>(value);
      // Assume worst case for the length.
      if (ReadWriteUtil::MAX_ZINT_LEN +
          string_val->len + (*blockp)->size > column->limit) {
        RETURN_IF_ERROR(CreateNewBlock(column, blockp));
        if (*blockp == NULL) return Status::OK;
      }
      RETURN_IF_ERROR(SetNull(column, blockp, false));
      if (*blockp == NULL) return Status::OK;
      int_len = ReadWriteUtil::PutZInt(string_val->len, column->current_value);

      data_len = string_val->len;
      column->current_value += int_len;
      memcpy(column->current_value, string_val->ptr, data_len);
      break;
    }

    case TREVNI_INT: {
      // A ZInt is at most 5 bytes.
      if (ReadWriteUtil::MAX_ZINT_LEN + (*blockp)->size > column->limit) {
        RETURN_IF_ERROR(CreateNewBlock(column, blockp));
        if (*blockp == NULL) return Status::OK;
      }
      RETURN_IF_ERROR(SetNull(column, blockp, false));
      if (*blockp == NULL) return Status::OK;
      data_len = ReadWriteUtil::PutZInt(*reinterpret_cast<int32_t*>(value),
          column->current_value);

      break;
    }

    case TREVNI_LONG: {
      // A ZLong is at most 10 bytes.
      if (ReadWriteUtil::MAX_ZLONG_LEN + (*blockp)->size > column->limit) {
        RETURN_IF_ERROR(CreateNewBlock(column, blockp));
        if (*blockp == NULL) return Status::OK;
      }
      RETURN_IF_ERROR(SetNull(column, blockp, false));
      if (*blockp == NULL) return Status::OK;
      data_len = ReadWriteUtil::PutZLong(*reinterpret_cast<int64_t*>(value),
          column->current_value);

      break;
    }

    case TREVNI_BOOL: {
      // boolean is stored as a bit array
      if (((*blockp)->row_count % 8) == 0) {
        if (1 + (*blockp)->size > column->limit) {
          RETURN_IF_ERROR(CreateNewBlock(column, blockp));
          if (*blockp == NULL) return Status::OK;
        }

        ++bytes_added_;
        if (bytes_added_ > file_limit_) return Status::OK;

        ++(*blockp)->size;
      }
      RETURN_IF_ERROR(SetNull(column, blockp, false));
      // This should be calculated so that there is enough room.
      DCHECK(*blockp !=  NULL);
      (*blockp)->bool_column.Put(*reinterpret_cast<bool*>(value) ? 1 : 0);

      return Status::OK;
    }
    
    default:
      DCHECK(false) << "Unknown type in AppendVariableLengthValue";
  }

  // We may have put too much in the buffer, but only the valid part will be written.
  bytes_added_ += int_len + data_len;
  if (bytes_added_ > file_limit_) return Status::OK;

  column->current_value += data_len;
  (*blockp)->size += int_len + data_len;

  return Status::OK;
}

Status HdfsTrevniTableWriter::AppendFixedLengthValue(void* value,
                                                     TrevniColumnInfo* column,
                                                     TrevniBlockInfo** blockp) {
  if (column->type_length + (*blockp)->size > column->limit) {
      RETURN_IF_ERROR(CreateNewBlock(column, blockp));
      if (*blockp == NULL) return Status::OK;
  }
  RETURN_IF_ERROR(SetNull(column, blockp, false));
  if (*blockp == NULL) return Status::OK;

  bytes_added_ += column->type_length;
  if (bytes_added_ > file_limit_) return Status::OK;

  memcpy(column->current_value, value, column->type_length);
  column->current_value += column->type_length;
  (*blockp)->size += column->type_length;
  return Status::OK;
}

Status HdfsTrevniTableWriter::AppendRowBatch(RowBatch* batch,
                                             const vector<int32_t>& row_group_indices,
                                             bool* new_file) {
  *new_file = false;
  // Get the file limit, we may have switched files.
  if (file_limit_ == 0) {
    RETURN_IF_ERROR(HdfsTableSink::GetFileBlockSize(output_, &file_limit_));
  }

  int32_t limit;
  if (row_group_indices.empty()) {
    limit = batch->num_rows();
  } else {
    limit = row_group_indices.size();
  }
      
  bool all_rows = row_group_indices.empty();
  for (; row_idx_ < limit; ++row_idx_) {
    TupleRow* current_row = all_rows ?
        batch->GetRow(row_idx_) : batch->GetRow(row_group_indices[row_idx_]);
    for (int j = 0; j < columns_.size(); ++j) {
      TrevniColumnInfo* column = &columns_[j];
      TrevniBlockInfo* block = &column->block_desc.back();
      block->previous_size = block->size;
      void* value = output_exprs_[j]->GetValue(current_row);

      // Check for null value.
      if (value == NULL) {
        RETURN_IF_ERROR(SetNull(column, &block, true));
        if (block == NULL) return Status::OK;
      } else if (column->type_length == 0) {
        // Variable length column.
        RETURN_IF_ERROR(AppendVariableLengthValue(value, column, &block));
        if (block == NULL) return Status::OK;
      } else  {
        // Fixed length column.
        RETURN_IF_ERROR(AppendFixedLengthValue(value, column, &block));
        if (block == NULL) return Status::OK;
      }

      // If we have exceeded the file limit signal we need a new file.
      // The sink will call our finalize routine.
      if (bytes_added_ > file_limit_) {
        // Back out the data we previously inserted.
        while (j >= 0) {
          block = &column->block_desc[column->block_desc.size() - 1];
          block->size = block->previous_size;
          --j;
          --column;
        }
        *new_file = true;
        return Status::OK;
      }
      ++block->row_count;
    }
    ++row_count_;
    ++output_->num_rows;
  }

  // Reset the row_idx_ when we exhaust the batch.  We can exit before exhausting 
  // the batch if we run out of file space and will continue from the last index.
  row_idx_ = 0;
  return Status::OK;
}

Status HdfsTrevniTableWriter::Finalize() {
  RETURN_IF_ERROR(WriteFileHeader());

  for (int32_t i = 0; i < columns_.size(); ++i) {
    RETURN_IF_ERROR(WriteColumn(&columns_[i]));
  }
  bytes_added_ = file_limit_ = 0;
  row_count_ = 0;
  SetHeaderFileSize();
  return ResetColumns();
}

Status HdfsTrevniTableWriter::WriteFileHeader() {
  RETURN_IF_ERROR(Write(TREVNI_VERSION_HEADER, sizeof(TREVNI_VERSION_HEADER)));
  RETURN_IF_ERROR(WriteLong(row_count_));
  RETURN_IF_ERROR(WriteInt(columns_.size()));

  RETURN_IF_ERROR(WriteFileMetadata());

  for (int i  = 0; i < columns_.size(); ++i) {
    RETURN_IF_ERROR(WriteColumnMetadata(&columns_[i]));
  }

  // Calculate and write out the start offset of each column.
  int64_t start = hdfsTell(output_->hdfs_connection, output_->tmp_hdfs_file);
  // add the start array.
  start += columns_.size() * sizeof(int64_t);
  for (int i = 0; i < columns_.size() - 1; ++i) {
    RETURN_IF_ERROR(WriteLong(start));

    TrevniColumnInfo* column = &columns_[i];
    // Compress the last block if needed.
    if (column->compressor != NULL && !column->block_desc.empty()) {
      RETURN_IF_ERROR(CompressBlock(column, &column->block_desc.back()));
    }
    start += sizeof(int32_t);

    // Account for the size of each block.
    for (int i = 0; i < column->block_desc.size(); ++i) {
      TrevniBlockInfo& block = column->block_desc[i];
      // Account for size of the bock descriptor information.
      start += sizeof(block.row_count);
      start += sizeof(block.size);
      start += sizeof(block.compressed_size);
      if (column->has_values) {
        // TODO: Account for size of the value
        DCHECK(false);
      }
      if (column->compressor == NULL) {
        if (column->max_def_level > 0) {
          start += block.def_level.CurrentByteCount();
          if (column->max_rep_level > 0) {
            start += block.rep_level.CurrentByteCount();
          }
        }
        start += block.size;
      } else {
        // The level arrays are included in the compressed block.
        start += block.compressed_size;
      }
    }
  }
  // Write out the value for the last column.
  RETURN_IF_ERROR(WriteLong(start));
  TrevniColumnInfo* column = &columns_[columns_.size() - 1];
  // Compress the last block if needed.
  if (column->compressor != NULL && !column->block_desc.empty()) {
    RETURN_IF_ERROR(CompressBlock(column, &column->block_desc.back()));
  }
  return Status::OK;
}

// Write the file meta data.
Status HdfsTrevniTableWriter::WriteFileMetadata() {
  if (compression_ != THdfsCompression::NONE) {
    RETURN_IF_ERROR(WriteZInt(1));
    RETURN_IF_ERROR(WriteString("trevni.codec"));
    RETURN_IF_ERROR(WriteString(compression_name_));
  } else {
    RETURN_IF_ERROR(WriteZInt(0));
  }
  return Status::OK;
}

Status HdfsTrevniTableWriter::WriteColumnMetadata(TrevniColumnInfo* column) {
  // Every column has a name and type in the metadata.
  int count = 2;
  if (column->max_def_level > 0) ++count;
  if (column->max_rep_level > 0) ++count;
  // TODO: include column specific codec
  RETURN_IF_ERROR(WriteZInt(count));

  RETURN_IF_ERROR(WriteString("trevni.name"));
  // The format of Trevni bytes is the same as Trevni string so we can call WriteString
  // rather then WriteBytes.
  RETURN_IF_ERROR(WriteString(column->name));
  RETURN_IF_ERROR(WriteString("trevni.type"));
  map<TrevniType, const string>::const_iterator type = type_map_string.find(column->type);
  DCHECK(type != type_map_string.end());
  RETURN_IF_ERROR(WriteString(type->second));
  if (column->max_def_level > 0) {
    RETURN_IF_ERROR(WriteString("trevni.definition"));
    stringstream ds;
    ds << column->max_def_level;
    RETURN_IF_ERROR(WriteString(ds.str()));
    if (column->max_rep_level > 0) {
      RETURN_IF_ERROR(WriteString("trevni.repetition"));
      stringstream rs;
      rs << column->max_rep_level;
      RETURN_IF_ERROR(WriteString(rs.str()));
    }
  }
  return Status::OK;
}

Status HdfsTrevniTableWriter::WriteColumn(TrevniColumnInfo* column) {
  RETURN_IF_ERROR(WriteInt(column->block_desc.size()));
  // Write the block descriptors.
  for (int i = 0; i < column->block_desc.size(); ++i) {
    TrevniBlockInfo& block = column->block_desc[i];
    RETURN_IF_ERROR(WriteBlockDescriptor(column, &block));
  }
  // Write the block data.
  for (int i = 0; i < column->block_desc.size(); ++i) {
    TrevniBlockInfo& block = column->block_desc[i];
    RETURN_IF_ERROR(WriteBlock(column, &block));
  }
  return Status::OK;
}

Status HdfsTrevniTableWriter::WriteBlockDescriptor(TrevniColumnInfo* column,
                                                   TrevniBlockInfo* block) {
  RETURN_IF_ERROR(WriteInt(block->row_count));
  // The size of the block includes the size of the level arrays.
  int32_t size = block->size;
  if (column->compressor == NULL) {
    if (column->max_def_level != 0) {
      size += block->def_level.CurrentByteCount();
      if (column->max_rep_level != 0) {
        size += block->rep_level.CurrentByteCount();
      }
    }
  }
  RETURN_IF_ERROR(WriteInt(size));
  RETURN_IF_ERROR(WriteInt(block->compressed_size));

  // TODO: Handle initial values.
  if (column->has_values) {
    DCHECK(false);
  }

  return Status::OK;
}

Status HdfsTrevniTableWriter::WriteBlock(TrevniColumnInfo* column,
                                         TrevniBlockInfo* block) {
  if (column->compressor != NULL) {
    // Write the compressed data, the level arrays are included.
    RETURN_IF_ERROR(Write(block->data, block->compressed_size));
  } else {
    // Write uncompressed data.
    RETURN_IF_ERROR(Write(block->data, block->size));
    
    // Write out the definition and repetition arrays if present.
    if (column->max_def_level != 0) {
      RETURN_IF_ERROR(
          Write(block->def_level.array(), block->def_level.CurrentByteCount()));
      if (column->max_rep_level != 0) {
        RETURN_IF_ERROR(
            Write(block->rep_level.array(), block->rep_level.CurrentByteCount()));
      }
    }
  }
  // TODO: Handle CRC.
  if (column->has_crc) {
    DCHECK_EQ(column->has_crc, false);
  }
  return Status::OK;
}

Status HdfsTrevniTableWriter::WriteInt(int32_t integer) {
  uint8_t buf[sizeof(int32_t)];
  ReadWriteUtil::PutInt(integer, buf);
  return Write(buf, sizeof(int32_t));
}

Status HdfsTrevniTableWriter::WriteLong(int64_t longint) {
  uint8_t buf[sizeof(int64_t)];
  ReadWriteUtil::PutLong(longint, buf);
  return Write(buf, sizeof(int64_t));
}

Status HdfsTrevniTableWriter::WriteZInt(int32_t integer) {
  uint8_t buf[ReadWriteUtil::MAX_ZINT_LEN];
  int len = ReadWriteUtil::PutZInt(integer, buf);
  return Write(buf, len);
}

Status HdfsTrevniTableWriter::WriteZLong(int64_t longint) {
  uint8_t buf[ReadWriteUtil::MAX_ZLONG_LEN];
  int len = ReadWriteUtil::PutZLong(longint, buf);
  return Write(buf, len);
}

Status HdfsTrevniTableWriter::WriteString(const string& str) {
  RETURN_IF_ERROR(WriteZInt(str.size()));
  return Write(reinterpret_cast<const char*>(&str[0]), str.size());
}

Status HdfsTrevniTableWriter::WriteBytes(const vector<uint8_t>& bytes) {
  RETURN_IF_ERROR(WriteZInt(bytes.size()));
  return Write(reinterpret_cast<const char*>(&bytes[0]), bytes.size());
}

}
