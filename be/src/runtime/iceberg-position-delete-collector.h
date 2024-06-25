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

#include <memory>
#include <unordered_map>

#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace impala {

/// Helper class for collecting position delete records and efficiently
/// serializing them to outbound row batches. The serialization algorithm
/// is the following:
/// for each entry in 'file_to_positions_':
///   write out the file path
///   for each position for the current file path:
///     write out a tuple: (StringValue object pointing to the file path, position)
/// E.g.:
/// +----------------+-------------+--------+-------------+--------+-----+
/// |   File path    | StringValue | BigInt | StringValue | BigInt | ... |
/// +----------------+-------------+--------+-------------+--------+-----+
/// | /.../a.parquet | ptr, len    |     42 | ptr, len    |     43 | ... |
/// +----------------+-------------+--------+-------------+--------+-----+
///
/// IcebergPositionDeleteCollector tracks the memory of the file paths, but the
/// positions are stored in a std::vector<int64_t> untracked (since it only store records
/// up to a KrpcDataStreamSender::Channel's capacity, the memory consumption should
/// not be too significant).
class IcebergPositionDeleteCollector {
public:
  IcebergPositionDeleteCollector(TupleDescriptor* desc) {
    desc_ = desc;
    DCHECK_EQ(desc_->slots().size(), 2);
    SlotDescriptor* file_path_desc = desc_->slots()[0];
    SlotDescriptor* position_desc = desc_->slots()[1];
    DCHECK(file_path_desc->type().IsVarLenStringType());
    DCHECK(position_desc->type().type == TYPE_BIGINT);
    file_path_offset_ = file_path_desc->tuple_offset();
    pos_offset_ = position_desc->tuple_offset();
    row_count_ = 0;
    insert_it_ = file_to_positions_.end();
  }

  void Init(MemTracker* parent_mem_tracker) {
    pool_ = std::make_unique<MemPool>(parent_mem_tracker);
  }

  Status AddRow(TupleRow* row) {
    Tuple* tuple = row->GetTuple(0);
    StringValue* filename_value = tuple->GetStringSlot(file_path_offset_);
    int64_t pos = *tuple->GetBigIntSlot(pos_offset_);

    RETURN_IF_ERROR(SetInsertIterator(filename_value));
    insert_it_->second.push_back(pos);
    ++row_count_;
    return Status::OK();
  }

  int RowCount() const { return row_count_; }

  void Close() {
    Reset();
    pool_->FreeAll();
  }

  /// Serializes the collected position delete records into 'dest',
  /// then resets the internal structures of 'this'.
  Status Serialize(OutboundRowBatch* dest) {
    dest->tuple_offsets_.clear();
    int64_t tuple_data_size = TupleDataSize();
    dest->tuple_data_.resize(tuple_data_size);
    char* tuple_data = const_cast<char*>(dest->tuple_data_.data());
    Ubsan::MemSet(tuple_data, 0, tuple_data_size);
    int offset = 0;
    for (const auto& [path, positions] : file_to_positions_) {
      int path_start = offset;
      int path_len = path.Len();
      Ubsan::MemCpy(tuple_data + offset, path.Ptr(), path_len);
      offset += path_len;
      for (int64_t pos : positions) {
        dest->tuple_offsets_.push_back(offset);
        Tuple* t = reinterpret_cast<Tuple*>(tuple_data + offset);
        StringValue* sv = t->GetStringSlot(file_path_offset_);
        sv->Assign(reinterpret_cast<char*>(path_start), path_len);
        int64_t* pos_slot = t->GetBigIntSlot(pos_offset_);
        DCHECK_GE(pos, 0);
        *pos_slot = pos;
        offset += desc_->byte_size();
      }
    }
    DCHECK_EQ(offset, tuple_data_size);
    Reset();
    return Status::OK();
  }

private:
  void Reset() {
    file_to_positions_.clear();
    insert_it_ = file_to_positions_.end();
    pool_->Clear();
    row_count_ = 0;
  }

  int64_t TupleDataSize() const {
    int64_t total_size = 0;
    for (const auto& [filename, positions] : file_to_positions_) {
      total_size += filename.Len();
      total_size += positions.size() * desc_->byte_size();
    }
    return total_size;
  }

  Status SetInsertIterator(StringValue* filename_value) {
    if (insert_it_ != file_to_positions_.end() &&
        insert_it_->first == *filename_value) {
      return Status::OK();
    }
    insert_it_ = file_to_positions_.find(*filename_value);
    if (insert_it_ != file_to_positions_.end()) {
      return Status::OK();
    }
    StringValue::SimpleString ss = filename_value->ToSimpleString();
    char* ptr = reinterpret_cast<char*>(pool_->TryAllocate(ss.len));
    if (UNLIKELY(ptr == nullptr)) {
      return Status(strings::Substitute(
          "Could not allocate $0 bytes in IcebergPositionDeleteChannel.", ss.len));
    }
    Ubsan::MemCpy(ptr, ss.ptr, ss.len);
    StringValue sv(ptr, ss.len);
    insert_it_ = file_to_positions_.insert(insert_it_, {sv, {}});
    return Status::OK();
  }

  std::unique_ptr<MemPool> pool_;
  TupleDescriptor* desc_;
  int file_path_offset_;
  int pos_offset_;
  int row_count_;
  using FileToPositionsMap = std::unordered_map<StringValue, vector<int64_t>>;
  FileToPositionsMap file_to_positions_;
  FileToPositionsMap::iterator insert_it_;
};

}