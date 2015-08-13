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

#include "common/names.h"

namespace impala {

DescriptorTblBuilder::DescriptorTblBuilder(ObjectPool* obj_pool) : obj_pool_(obj_pool) {
}

TupleDescBuilder& DescriptorTblBuilder::DeclareTuple() {
  TupleDescBuilder* tuple_builder = obj_pool_->Add(new TupleDescBuilder());
  tuples_descs_.push_back(tuple_builder);
  return *tuple_builder;
}

// item_id of -1 indicates no itemTupleId
static TSlotDescriptor MakeSlotDescriptor(int id, int parent_id, const ColumnType& type,
    int slot_idx, int byte_offset, int item_id) {
  int null_byte = slot_idx / 8;
  int null_bit = slot_idx % 8;
  TSlotDescriptor slot_desc;
  slot_desc.__set_id(id);
  slot_desc.__set_parent(parent_id);
  slot_desc.__set_slotType(type.ToThrift());
  // Make the column path empty to avoid having to also construct a table descriptor (see
  // the SlotDescriptor ctor that takes a TSlotDescriptor, which modifies the column path
  // based on the table schema)
  slot_desc.__set_columnPath(vector<int>());
  slot_desc.__set_byteOffset(byte_offset);
  slot_desc.__set_nullIndicatorByte(null_byte);
  slot_desc.__set_nullIndicatorBit(null_bit);
  slot_desc.__set_slotIdx(slot_idx);
  slot_desc.__set_isMaterialized(true);
  if (item_id != -1) slot_desc.__set_itemTupleId(item_id);
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
  int tuple_id = 0;
  int slot_id = 0;

  for (int i = 0; i < tuples_descs_.size(); ++i) {
    BuildTuple(tuples_descs_[i]->slot_types(), &thrift_desc_tbl, &tuple_id, &slot_id);
  }

  Status status = DescriptorTbl::Create(obj_pool_, thrift_desc_tbl, &desc_tbl);
  DCHECK(status.ok());
  return desc_tbl;
}

TTupleDescriptor DescriptorTblBuilder::BuildTuple(
    const vector<ColumnType>& slot_types, TDescriptorTable* thrift_desc_tbl,
    int* next_tuple_id, int* slot_id) {
  // We never materialize struct slots (there's no in-memory representation of structs,
  // instead the materialized fields appear directly in the tuple), but array types can
  // still have a struct item type. In this case, the array item tuple contains the
  // "inlined" struct fields.
  if (slot_types.size() == 1 && slot_types[0].type == TYPE_STRUCT) {
    return BuildTuple(slot_types[0].children, thrift_desc_tbl, next_tuple_id, slot_id);
  }

  int num_null_bytes = BitUtil::Ceil(slot_types.size(), 8);
  int byte_offset = num_null_bytes;
  int tuple_id = *next_tuple_id;
  ++(*next_tuple_id);

  for (int i = 0; i < slot_types.size(); ++i) {
    DCHECK_NE(slot_types[i].type, TYPE_STRUCT);
    int item_id = -1;
    if (slot_types[i].IsCollectionType()) {
      TTupleDescriptor item_desc =
          BuildTuple(slot_types[i].children, thrift_desc_tbl, next_tuple_id, slot_id);
      item_id = item_desc.id;
    }

    thrift_desc_tbl->slotDescriptors.push_back(
        MakeSlotDescriptor(*slot_id, tuple_id, slot_types[i], i, byte_offset, item_id));
    byte_offset += slot_types[i].GetSlotSize();
    ++(*slot_id);
  }

  TTupleDescriptor result = MakeTupleDescriptor(tuple_id, byte_offset, num_null_bytes);
  thrift_desc_tbl->tupleDescriptors.push_back(result);
  return result;
}

}
