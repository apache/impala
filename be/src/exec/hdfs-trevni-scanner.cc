// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "text-converter.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/string-value.h"
#include "util/runtime-profile.h"
#include "util/decompress.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exec/hdfs-trevni-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exec/serde-utils.h"
#include "exec/text-converter.inline.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/JavaConstants_constants.h"

using namespace std;
using namespace boost;
using namespace impala;

HdfsTrevniScanner::HdfsTrevniScanner(HdfsScanNode* scan_node, RuntimeState* state,
                                     MemPool* mem_pool)
    : HdfsScanner(scan_node, state, mem_pool),
      decompressor_(NULL),
      compressed_data_pool_(new MemPool()),
      file_checksum_(false),
      compressed_buffer_size_(0),
      object_pool_(new ObjectPool) {
}

HdfsTrevniScanner::~HdfsTrevniScanner() {
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      compressed_data_pool_->peak_allocated_bytes());
  for (int i = 0; i < column_info_.size(); ++i) {
    COUNTER_UPDATE(scan_node_->memory_used_counter(),
        column_info_[i].mem_pool->peak_allocated_bytes());
  }
}

Status HdfsTrevniScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());
  tuple_ = NULL;
  return Status::OK;
}

Status HdfsTrevniScanner::Close() {
  return Status::OK;
}

Status HdfsTrevniScanner::InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition,
    DiskIoMgr::ScanRange* scan_range, Tuple* template_tuple, 
    ByteStream* current_byte_stream) {
  RETURN_IF_ERROR(HdfsScanner::InitCurrentScanRange(hdfs_partition,
      scan_range, template_tuple, current_byte_stream));

  // Trevni files are required to be in a single Hdfs block.
  if (scan_range->offset() != 0) {
    return FileReadError(
        "Bad scan range, offset != 0, Trevni files must be in a single block.");
  }

  // Clear the information from the previous file.
  file_checksum_ = false;
  column_info_.clear();
  RETURN_IF_ERROR(ReadFileHeader());

  // Read the column information for the columns we are interested in.
  for (int i = 0; i < column_info_.size(); ++i) {
    RETURN_IF_ERROR(ReadColumnInfo(&column_info_[i]));
  }

  return Status::OK;
}

Status HdfsTrevniScanner::ReadFileHeader() {
  vector<uint8_t> head;
  int64_t bytes_read;
  head.resize(sizeof(TREVNI_VERSION_HEADER));
  RETURN_IF_ERROR(
      current_byte_stream_->Read(&head[0], sizeof(TREVNI_VERSION_HEADER), &bytes_read));
  if (bytes_read != sizeof(TREVNI_VERSION_HEADER)) {
    return FileReadError("Short read of file header");
  }
  if (memcmp(&head[0], TREVNI_VERSION_HEADER, sizeof(TREVNI_VERSION_HEADER))) {
    stringstream ss;
    ss << "Invalid TREVNI_VERSION_HEADER: '"
       << SerDeUtils::HexDump(&head[0], sizeof(TREVNI_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  RETURN_IF_ERROR(ReadWriteUtil::ReadInt<int64_t>(current_byte_stream_, &row_count_));
  RETURN_IF_ERROR(ReadWriteUtil::ReadInt<int32_t>(current_byte_stream_, &column_count_));

  RETURN_IF_ERROR(ReadFileHeaderMetadata());

  // Allocate the column information and read the meta data for those columns in
  // the query.
  column_info_.resize(scan_node_->materialized_slots().size());
  for (int i = 0; i < column_count_; ++i) {
    int slot_idx =
        scan_node_->GetMaterializedSlotIdx(i + scan_node_->num_partition_keys());
    if (slot_idx == HdfsScanNode::SKIP_COLUMN) {
      RETURN_IF_ERROR(SkipColumnMetadata());
    } else {
      RETURN_IF_ERROR(ReadColumnMetadata(slot_idx));
    }
  }

  // Read the column start locations.
  vector<uint64_t> buf;
  buf.resize(column_count_);
  RETURN_IF_ERROR(current_byte_stream_->Read(
      reinterpret_cast<uint8_t*>(&buf[0]), sizeof(int64_t) * column_count_, &bytes_read));
  if (bytes_read != sizeof(int64_t) * column_count_) {
    return FileReadError("Short read of column start locations");
  }
  for (int i = 0; i < column_count_; ++i) {
    int slot_idx =
        scan_node_->GetMaterializedSlotIdx(i + scan_node_->num_partition_keys());

    if (slot_idx == HdfsScanNode::SKIP_COLUMN) continue;

    column_info_[slot_idx].current_offset = buf[i];
    // Each column needs its own memory pool since we must hold on to each
    // columns block buffer when passing memory to our caller.
    column_info_[slot_idx].mem_pool = object_pool_->Add(new MemPool());
  }
  return Status::OK;
}

Status HdfsTrevniScanner::ReadFileHeaderMetadata() {
  int32_t map_size;
  RETURN_IF_ERROR(ReadWriteUtil::ReadZInt(current_byte_stream_, &map_size));

  for (int i = 0; i < map_size; ++i) {
    string key;
    RETURN_IF_ERROR(ReadWriteUtil::ReadString(current_byte_stream_, &key));
    vector<uint8_t> value;
    RETURN_IF_ERROR(ReadWriteUtil::ReadBytes(current_byte_stream_, &value));

    map<const string, FileMeta>::const_iterator meta = file_meta_map.find(key);

    // There can be user defined meta data, we ignore it.
    if (meta == file_meta_map.end()) continue;

    switch (meta->second) {
      case CODEC:
        RETURN_IF_ERROR(CreateDecompressor(value, &decompressor_));
        break;

      case CHECKSUM: {
        string strval(reinterpret_cast<char*>(&value[0]), value.size());
        map<const string, Checksum>::const_iterator cksum = checksum_map.find(strval);
        if (cksum == checksum_map.end()) {
          return FileReadError("Unknown checksum specification: " + strval);
        }
        switch (cksum->second) {
          case NONE:
            break;
          case CRC32:
            file_checksum_ = true;
            break;
        }
      }
    }
  }
  return Status::OK;
}

Status HdfsTrevniScanner::SkipColumnMetadata() {
  int32_t map_size;
  RETURN_IF_ERROR(ReadWriteUtil::ReadZInt(current_byte_stream_, &map_size));

  for (int i = 0; i < map_size; ++i) {
    RETURN_IF_ERROR(ReadWriteUtil::SkipBytes(current_byte_stream_));
    RETURN_IF_ERROR(ReadWriteUtil::SkipBytes(current_byte_stream_));
  }
  return Status::OK;
}

Status HdfsTrevniScanner::ReadColumnMetadata(int column_idx) {
  struct TrevniColumnInfo* col_info = &column_info_[column_idx];
  col_info->decompressor = decompressor_;
  int32_t map_size;
  RETURN_IF_ERROR(ReadWriteUtil::ReadZInt(current_byte_stream_, &map_size));

  for (int i = 0; i < map_size; ++i) {
    string key;
    RETURN_IF_ERROR(ReadWriteUtil::ReadString(current_byte_stream_, &key));
    vector<uint8_t> value;
    RETURN_IF_ERROR(ReadWriteUtil::ReadBytes(current_byte_stream_, &value));

    map<const string, ColumnMeta>::const_iterator meta = column_meta_map.find(key);

    // There can be user defined meta data, we ignore it.
    if (meta == column_meta_map.end()) continue;

    string strval;
    if (value.size() != 0) {
      strval.assign(reinterpret_cast<char*>(&value[0]), value.size());
    }
    switch (meta->second) {
      case COL_CODEC:
        RETURN_IF_ERROR(CreateDecompressor(value, &col_info->decompressor));
        break;
      case COL_PARENT:
        col_info->parent = strval;
        break;
      case COL_NAME:
        col_info->name = strval;
        break;
      case COL_TYPE: {
        map<const string, TrevniType>::const_iterator type =
          type_map.find(strval);
        if (type == type_map.end()) {
          return FileReadError("Unknown type specification: " + strval);
        }
        col_info->type = type->second;
        break;
      }
      case COL_VALUES:
        col_info->has_values = true;
        break;
      case COL_ARRAY:
        col_info->is_array = true;
        break;
      case COL_REPETITION:
        col_info->max_rep_level = atoi(strval.c_str());
        if (col_info->max_rep_level < 0) {
          return FileReadError("Bad repetition level specification: " + strval);
        }
        break;
      case COL_DEFINITION:
        col_info->max_def_level = atoi(strval.c_str());
        if (col_info->max_def_level < 0) {
          return FileReadError("Bad definition level specification: " + strval);
        }
        break;
    }
  }

  // Check metadata consistency.
  if (col_info->name.size() == 0) {
    return FileReadError("Missing column name");
  }
  if (col_info->type == TREVNI_UNDEFINED) {
      return FileReadError("Missing column type for: " + col_info->name);
  }
  if (col_info->type == TREVNI_STRING) {
    col_info->has_noncompact_strings = has_noncompact_strings_;
  }
  col_info->length = GetTrevniTypeLength(col_info->type);
  return Status::OK;
}

Status HdfsTrevniScanner::CreateDecompressor(const vector<uint8_t>& value,
                                             Codec** decompressor) {
  const string strval(reinterpret_cast<const char*>(&value[0]), value.size());
  map<string, THdfsCompression::type>::const_iterator codec =
      g_JavaConstants_constants.COMPRESSION_MAP.find(strval);
  if (codec == g_JavaConstants_constants.COMPRESSION_MAP.end()) {
    return FileReadError("Unknown compression codec" + strval);
  }
  if (codec->second == THdfsCompression::NONE) {
    *decompressor = NULL;
  } else {
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_, NULL,
        !has_noncompact_strings_, codec->second, decompressor));
    object_pool_->Add(*decompressor);
  }

  return Status::OK;
}

Status HdfsTrevniScanner::ReadColumnInfo(TrevniColumnInfo* column) {
  RETURN_IF_ERROR(current_byte_stream_->Seek(column->current_offset));
  int32_t block_count = 0;
  RETURN_IF_ERROR(
      ReadWriteUtil::ReadInt<int32_t>(current_byte_stream_, &block_count));
  column->block_desc.resize(block_count);

  for (int i = 0; i < block_count; ++i) {
    RETURN_IF_ERROR(ReadBlockDescriptor(*column, &column->block_desc[i]));
  }
  RETURN_IF_ERROR(current_byte_stream_->GetPosition(&column->current_offset));

  return Status::OK;
}

Status HdfsTrevniScanner::ReadBlockDescriptor(const TrevniColumnInfo& column,
                                              TrevniBlockInfo* block) {
  RETURN_IF_ERROR(
      ReadWriteUtil::ReadInt<int32_t>(current_byte_stream_, &block->row_count));
  RETURN_IF_ERROR(ReadWriteUtil::ReadInt<int32_t>(current_byte_stream_, &block->size));
  RETURN_IF_ERROR(
      ReadWriteUtil::ReadInt<int32_t>(current_byte_stream_, &block->compressed_size));
  if (column.has_values) {
    RETURN_IF_ERROR(ReadValue(column, &block->first_value));
  }
  return Status::OK;
}

// TODO: Implement reading the initial value for a data block (if has_values is set).
Status HdfsTrevniScanner::ReadValue(const TrevniColumnInfo& column, void** value) {
  DCHECK(false);
  return Status::OK;
}

Status HdfsTrevniScanner::ReadCurrentBlock(TrevniColumnInfo* column) {
  DCHECK_EQ(column->current_row_count, 0);
  DCHECK_LT(column->current_block, column->block_desc.size());
  RETURN_IF_ERROR(current_byte_stream_->Seek(column->current_offset));
  TrevniBlockInfo* block = &column->block_desc[column->current_block];
  if (column->has_noncompact_strings || column->current_buffer_size < block->size) {
    column->current_buffer_size = block->size;
    column->buffer = column->mem_pool->Allocate(column->current_buffer_size);
  }

  int64_t bytes_read;
  if (column->decompressor == NULL) {
    RETURN_IF_ERROR(current_byte_stream_->Read(column->buffer, block->size, &bytes_read));
    if (bytes_read != block->size) {
      return FileReadError("Short read of data buffer");
    }
  } else {
    if (block->compressed_size > compressed_buffer_size_) {
      compressed_buffer_size_ = block->compressed_size;
      compressed_buffer_ = compressed_data_pool_->Allocate(compressed_buffer_size_);
    }
    RETURN_IF_ERROR(current_byte_stream_->Read(
        compressed_buffer_, block->compressed_size, &bytes_read));
    if (bytes_read != block->compressed_size) {
      return FileReadError("Short read of compressed data");
    }
    RETURN_IF_ERROR(column->decompressor->ProcessBlock(
        block->compressed_size, compressed_buffer_, &block->size, &column->buffer));
  }

  if (column->type == TREVNI_BOOL) {
    column->bool_column = IntegerArray(1, block->row_count, column->buffer);
  }
  
  // The definition and repetition arrays are after the data.
  if (column->max_def_level > 0) {
    uint8_t* bp = &column->buffer[block->size];
    if (column->max_rep_level > 0) {
      int size = IntegerArray::IntegerSize(column->max_rep_level);
      int array_size = IntegerArray::ArraySize(size, block->row_count);
      // Copy the array out for alignment.
      uint8_t* array = compressed_data_pool_->Allocate(array_size);
      bp -= array_size;
      memcpy(array, bp, array_size);
      column->rep_level = IntegerArray(size, block->row_count, array);
    }
    int size = IntegerArray::IntegerSize(column->max_def_level);
    int array_size = IntegerArray::ArraySize(size, block->row_count);
    // Copy the array out for alignment.
    uint8_t* array = compressed_data_pool_->Allocate(array_size);
    bp -= array_size;
    memcpy(array, bp, array_size);
    column->def_level = IntegerArray(size, block->row_count, array);
  }
  column->current_value = column->buffer;
  column->current_row_count = block->row_count;
  ++column->current_block;
  RETURN_IF_ERROR(current_byte_stream_->GetPosition(&column->current_offset));
  return Status::OK;
}

Status HdfsTrevniScanner::FileReadError(const string& msg) {
  if (state_->LogHasSpace()) {
    state_->error_stream() << "file: " <<
        current_byte_stream_->GetLocation() << ": " << msg << endl;
    state_->LogErrorStream();
  }
  return Status(msg);
}

// Store a value into a slot, return true if there is overflow.
template <typename T>
bool WriteSlot(void* slot, int64_t value) {
  T val = static_cast<T>(value);
  *reinterpret_cast<T*>(slot) = val;
  return val != value;
}

Status HdfsTrevniScanner::GetNext(RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  // Indicates whether the current row has errors.
  bool error_in_row = false;
  string error_str;

  if (scan_node_->ReachedLimit()) {
    tuple_ = NULL;
    *eosr = true;
    return Status::OK;
  }

  COUNTER_SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  // Index into current row in row_batch.
  int row_idx = RowBatch::INVALID_ROW_INDEX;

  while (!scan_node_->ReachedLimit() && !row_batch->IsFull() && row_count_ > 0) {
    const vector<SlotDescriptor*>& materialized_slots = scan_node_->materialized_slots();
    vector<SlotDescriptor*>::const_iterator it;

    InitTuple(template_tuple_, tuple_);
    TrevniColumnInfo* column = &column_info_[0];
    for (it = materialized_slots.begin();
         it != materialized_slots.end(); ++it, ++column) {
      const SlotDescriptor* slot_desc = *it;

      if (UNLIKELY(column->current_row_count == 0)) {
        RETURN_IF_ERROR(ReadCurrentBlock(column));
      }

      if (column->ValueIsNull()) {
        tuple_->SetNull(slot_desc->null_indicator_offset());
        --column->current_row_count;
        continue;
      }

      void* slot = tuple_->GetSlot(slot_desc->tuple_offset());
      if (column->type == TREVNI_BOOL) {
        *reinterpret_cast<bool*>(slot) = column->bool_column.GetNextValue();
      } else if (column->length == 0) {
        // Handle variable length values.
        int64_t value;
        int len = ReadWriteUtil::GetZLong(column->current_value, &value);
        column->current_value += len;
        switch (column->type) {
          case TREVNI_INT:
          case TREVNI_LONG: {
            switch (slot_desc->type()) {
              case TYPE_TINYINT:
                error_in_row = WriteSlot<int8_t>(slot, value);
                break;
              case TYPE_SMALLINT:
                 error_in_row = WriteSlot<int16_t>(slot, value);
                 break;
              case TYPE_INT:
                error_in_row = WriteSlot<int32_t>(slot, value);
                break;
              case TYPE_BIGINT:
                *reinterpret_cast<int64_t*>(slot) = value;
                break;
              default:
                DCHECK(false);
                return Status("Bad type");
            }
            break;
          }

          // TODO: Does TREVNI_BYTES map to a data type?
          case TREVNI_BYTES:
          case TREVNI_STRING: {
            StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
            str_slot->len = value;
            if (!has_noncompact_strings_) {
              char* slot_data = reinterpret_cast<char*>(tuple_pool_->Allocate(value));
              memcpy(slot_data, column->current_value, str_slot->len);
              str_slot->ptr = slot_data;
            } else {
              str_slot->ptr = reinterpret_cast<char*>(column->current_value);
            }
            column->current_value += value;
            break;
          }
          default:
            DCHECK(0);
            return Status("Bad Trevni type");
        }
      } else {
        // Fixed length types: They are read into an aligned buffer.
        switch (slot_desc->type()) {
          case TYPE_TINYINT:
            *reinterpret_cast<int8_t*>(slot) =
                *reinterpret_cast<int8_t*>(column->current_value);
            break;
          case TYPE_SMALLINT:
            *reinterpret_cast<int16_t*>(slot) =
                *reinterpret_cast<int16_t*>(column->current_value);
            break;
          case TYPE_INT:
            *reinterpret_cast<int32_t*>(slot) =
                *reinterpret_cast<int32_t*>(column->current_value);
            break;
          case TYPE_BIGINT:
            *reinterpret_cast<int64_t*>(slot) =
                *reinterpret_cast<int64_t*>(column->current_value);
            break;
          case TYPE_FLOAT:
            *reinterpret_cast<float*>(slot) =
                *reinterpret_cast<float*>(column->current_value);
            break;
          case TYPE_DOUBLE:
            *reinterpret_cast<double*>(slot) =
                *reinterpret_cast<double*>(column->current_value);
            break;
          case TYPE_TIMESTAMP: {
            // This may not be aligned.
            memcpy(slot, column->current_value, column->length);
            break;
          }
          default:
            DCHECK(0);
            return Status("Bad type");
        }
        column->current_value += column->length;
      }
      --column->current_row_count;
    }
    
    if (error_in_row) {
      error_in_row = false;
      if (state_->LogHasSpace()) {
        state_->error_stream() << "file " << current_byte_stream_->GetLocation() <<
            error_str <<endl;
        state_->LogErrorStream();
      }
      if (state_->abort_on_error()) {
        state_->ReportFileErrors(current_byte_stream_->GetLocation(), 1);
        return Status(
            "Aborted HdfsTrevniScanner due to parse errors. View error log for "
            "details.");
      }
    }
    --row_count_;

    // TODO: The code from here down is more or less common to all scanners. Move it.
    // We now have a complete row, with everything materialized
    DCHECK(!row_batch->IsFull());
    if (row_idx == RowBatch::INVALID_ROW_INDEX) {
      row_idx = row_batch->AddRow();
    }
    TupleRow* current_row = row_batch->GetRow(row_idx);
    current_row->SetTuple(tuple_idx_, tuple_);

    // Evaluate the conjuncts and add the row to the batch
    if (ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, current_row)) {
      row_batch->CommitLastRow();
      row_idx = RowBatch::INVALID_ROW_INDEX;
      if (scan_node_->ReachedLimit() || row_batch->IsFull()) {
        tuple_ = NULL;
        break;
      }
      uint8_t* new_tuple = reinterpret_cast<uint8_t*>(tuple_);
      new_tuple += tuple_byte_size_;
      tuple_ = reinterpret_cast<Tuple*>(new_tuple);
    }
  }

  if (scan_node_->ReachedLimit() || row_count_ == 0) {
    // We reached the limit, drained the row group or hit the end of the table.
    // No more work to be done. Clean up all pools with the last row batch.
    *eosr = true;
  } else {
    DCHECK(row_batch->IsFull());
    // The current row_batch is full, but we haven't yet reached our limit.
    *eosr = false;
  }

  // Maintain ownership of last memory chunk if not at the end of the scan range.
  if (has_noncompact_strings_) {
    for (int i = 0; i < column_info_.size(); ++i) {
      if (column_info_[i].has_noncompact_strings) {
        row_batch->tuple_data_pool()->AcquireData(column_info_[i].mem_pool, !*eosr);
      }
    }
  }
  row_batch->tuple_data_pool()->AcquireData(tuple_pool_, !*eosr);

  return Status::OK;
}
