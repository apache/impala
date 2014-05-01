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

#include "testutil/desc-tbl-builder.h"
#include "util/bit-util.h"


#include "runtime/descriptors.h"

using namespace std;

namespace impala {

DescriptorTblBuilder::DescriptorTblBuilder(ObjectPool* obj_pool) : obj_pool_(obj_pool) {
}

TupleDescBuilder& DescriptorTblBuilder::DeclareTuple() {
  TupleDescBuilder* tuple_builder = obj_pool_->Add(new TupleDescBuilder());
  tuples_descs_.push_back(tuple_builder);
  return *tuple_builder;
}

static TSlotDescriptor MakeSlotDescriptor(int id, int parent_id, const ColumnType& type,
    int slot_idx, int byte_offset) {
  int null_byte = slot_idx / 8;
  int null_bit = slot_idx % 8;
  TSlotDescriptor slot_desc;
  slot_desc.__set_id(id);
  slot_desc.__set_parent(parent_id);
  slot_desc.__set_slotType(type.ToThrift());
  slot_desc.__set_columnPos(slot_idx);
  slot_desc.__set_byteOffset(byte_offset);
  slot_desc.__set_nullIndicatorByte(null_byte);
  slot_desc.__set_nullIndicatorBit(null_bit);
  slot_desc.__set_slotIdx(slot_idx);
  slot_desc.__set_isMaterialized(true);
  return slot_desc;
}

static TTupleDescriptor MakeTupleDescriptor(int id, int byte_size, int num_null_bytes) {
  TTupleDescriptor tuple_desc;
  tuple_desc.__set_id(id);
  tuple_desc.__set_byteSize(byte_size);
  tuple_desc.__set_numNullBytes(num_null_bytes);
  return tuple_desc;
}

DescriptorTbl* DescriptorTblBuilder::Build() {
  DescriptorTbl* desc_tbl;
  TDescriptorTable thrift_desc_tbl;
  int slot_id = tuples_descs_.size(); // First ids reserved for TupleDescriptors

  for (int i = 0; i < tuples_descs_.size(); ++i) {
    vector<ColumnType> slot_types = tuples_descs_[i]->slot_types();
    int num_null_bytes = BitUtil::Ceil(slot_types.size(), 8);
    int byte_offset = num_null_bytes;
    int tuple_id = i;

    for(int j = 0; j < slot_types.size(); ++j) {
      thrift_desc_tbl.slotDescriptors.push_back(
          MakeSlotDescriptor(++slot_id, tuple_id, slot_types[j], j, byte_offset));

      int byte_size = slot_types[j].GetByteSize();
      if (byte_size == 0) {
        // can only handle strings right now
        DCHECK(slot_types[j].type == TYPE_STRING || slot_types[j].type == TYPE_VARCHAR);
        byte_size = 16;
      }
      byte_offset += byte_size;
    }

    thrift_desc_tbl.tupleDescriptors.push_back(
        MakeTupleDescriptor(tuple_id, byte_offset, num_null_bytes));
  }

  Status status = DescriptorTbl::Create(obj_pool_, thrift_desc_tbl, &desc_tbl);
  DCHECK(status.ok());
  return desc_tbl;
}

}
