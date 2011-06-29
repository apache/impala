// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/descriptors.h"

#include "gen-cpp/Descriptors_types.h"

using namespace std;

namespace impala {

static PrimitiveType ConvertType(TPrimitiveType::type ttype) {
  switch (ttype) {
    case TPrimitiveType::INVALID_TYPE: return INVALID_TYPE;
    case TPrimitiveType::BOOLEAN: return TYPE_BOOLEAN;
    case TPrimitiveType::TINYINT: return TYPE_TINYINT;
    case TPrimitiveType::SMALLINT: return TYPE_SMALLINT;
    case TPrimitiveType::INT: return TYPE_INT;
    case TPrimitiveType::BIGINT: return TYPE_BIGINT;
    case TPrimitiveType::FLOAT: return TYPE_FLOAT;
    case TPrimitiveType::DOUBLE: return TYPE_DOUBLE;
    case TPrimitiveType::DATE: return TYPE_DATE;
    case TPrimitiveType::DATETIME: return TYPE_DATETIME;
    case TPrimitiveType::TIMESTAMP: return TYPE_TIMESTAMP;
    case TPrimitiveType::STRING: return TYPE_STRING;
    default: return INVALID_TYPE;
  }
}

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
  : type_(ConvertType(tdesc.slotType)),
    tuple_offset_(tdesc.byteOffset),
    null_indicator_offset_(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit) {
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc)
  : byte_size_(tdesc.byteSize),
    slots_() {
}

void TupleDescriptor::AddSlot(SlotDescriptor* slot) {
  slots_.push_back(slot);
}

Status DescriptorTbl::Create(const TDescriptorTable& thrift_tbl, DescriptorTbl** tbl) {
  *tbl = new DescriptorTbl();
  for (int i = 0; i < thrift_tbl.tupleDescriptors.size(); ++i) {
    const TTupleDescriptor& tdesc = thrift_tbl.tupleDescriptors[i];
    (*tbl)->tuple_desc_map_[tdesc.id] = new TupleDescriptor(tdesc);
  }
  for (int i = 0; i < thrift_tbl.slotDescriptors.size(); ++i) {
    const TSlotDescriptor& tdesc = thrift_tbl.slotDescriptors[i];
    SlotDescriptor* slot_d = new SlotDescriptor(tdesc);
    (*tbl)->slot_desc_map_[tdesc.id] = slot_d;

    // link to parent
    TupleDescriptorMap::iterator entry = (*tbl)->tuple_desc_map_.find(tdesc.parent);
    if (entry == (*tbl)->tuple_desc_map_.end()) {
      return Status("unknown tid in slot descriptor msg");
    }
    entry->second->AddSlot(slot_d);
  }
  return Status::OK;
}

const TupleDescriptor* DescriptorTbl::GetTupleDescriptor(TupleId id) const {
  // TODO: is there some boost function to do exactly this?
  TupleDescriptorMap::const_iterator i = tuple_desc_map_.find(id);
  if (i == tuple_desc_map_.end()) {
    return NULL;
  } else {
    return i->second;
  }
}

const SlotDescriptor* DescriptorTbl::GetSlotDescriptor(SlotId id) const {
  // TODO: is there some boost function to do exactly this?
  SlotDescriptorMap::const_iterator i = slot_desc_map_.find(id);
  if (i == slot_desc_map_.end()) {
    return NULL;
  } else {
    return i->second;
  }
}

// return all registered tuple descriptors
void DescriptorTbl::GetTupleDescs(vector<const TupleDescriptor*>* descs) const {
  descs->clear();
  for (TupleDescriptorMap::const_iterator i = tuple_desc_map_.begin();
       i != tuple_desc_map_.end(); ++i) {
    descs->push_back(i->second);
  }
}

}
