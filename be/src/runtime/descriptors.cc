// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/descriptors.h"

#include <ios>
#include <sstream>

#include "common/object-pool.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;

namespace impala {

PrimitiveType ThriftToType(TPrimitiveType::type ttype) {
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

std::string TypeToString(PrimitiveType t) {
  switch (t) {
    case INVALID_TYPE: return "INVALID";
    case TYPE_BOOLEAN: return "BOOL";
    case TYPE_TINYINT: return "TINYINT";
    case TYPE_SMALLINT: return "SMALLINT";
    case TYPE_INT: return "INT";
    case TYPE_BIGINT: return "BIGINT";
    case TYPE_FLOAT: return "FLOAT";
    case TYPE_DOUBLE: return "DOUBLE";
    case TYPE_DATE: return "DATE";
    case TYPE_DATETIME: return "DATETIME";
    case TYPE_TIMESTAMP: return "TIMESTAMP";
    case TYPE_STRING: return "STRING";
  };
  return "";
}

string NullIndicatorOffset::DebugString() const {
  stringstream out;
  out << "(offset=" << byte_offset
      << " mask=" << hex << static_cast<int>(bit_mask) << dec << ")";
  return out.str();
}

ostream& operator<<(ostream& os, const NullIndicatorOffset& null_indicator) {
  os << null_indicator.DebugString();
  return os;
}

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
  : id_(tdesc.id),
    type_(ThriftToType(tdesc.slotType)),
    col_pos_(tdesc.columnPos),
    tuple_offset_(tdesc.byteOffset),
    null_indicator_offset_(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit) {
}

std::string SlotDescriptor::DebugString() const {
  stringstream out;
  out << "Slot(id=" << id_ << " type=" << TypeToString(type_)
      << " col=" << col_pos_ << " offset=" << tuple_offset_
      << " null=" << null_indicator_offset_.DebugString() << ")";
  return out.str();
}

TableDescriptor::TableDescriptor(const TTable& ttable)
  : num_cols_(ttable.numCols),
    num_partition_keys_(ttable.numPartitionKeys),
    line_delim_(ttable.lineDelim),
    field_delim_(ttable.fieldDelim),
    collection_delim_(ttable.collectionDelim),
    escape_char_(ttable.escapeChar),
    quote_char_((ttable.__isset.quoteChar) ? ttable.quoteChar : -1),
    strings_are_quoted_(ttable.__isset.quoteChar) {
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc)
  : id_(tdesc.id),
    table_desc_((tdesc.__isset.table) ? new TableDescriptor(tdesc.table) : NULL),
    byte_size_(tdesc.byteSize),
    slots_() {
}

void TupleDescriptor::AddSlot(SlotDescriptor* slot) {
  slots_.push_back(slot);
}

string TupleDescriptor::DebugString() const {
  stringstream out;
  out << "Tuple(id=" << id_ << " size=" << byte_size_ << " slots=[";
  for (size_t i = 0; i < slots_.size(); ++i) {
    if (i > 0) out << ", ";
    out << slots_[i]->DebugString();
  }
  out << "]";
  if (table_desc_.get() != NULL) {
    out << " #cols=" << table_desc_->num_cols()
        << " #pkeys=" << table_desc_->num_partition_keys()
        << " line_delim='" << table_desc_->line_delim() << "'"
        << " field_delim='" << table_desc_->field_delim() << "'"
        << " coll_delim='" << table_desc_->collection_delim() << "'"
        << " escape_char='" << table_desc_->escape_char() << "'"
        << " quote_char='" << table_desc_->quote_char() << "'"
        << " quoted=" << table_desc_->strings_are_quoted();
  }
  out << ")";
  return out.str();
}

Status DescriptorTbl::Create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                             DescriptorTbl** tbl) {
  *tbl = pool->Add(new DescriptorTbl());
  for (size_t i = 0; i < thrift_tbl.tupleDescriptors.size(); ++i) {
    const TTupleDescriptor& tdesc = thrift_tbl.tupleDescriptors[i];
    (*tbl)->tuple_desc_map_[tdesc.id] = pool->Add(new TupleDescriptor(tdesc));
  }
  for (size_t i = 0; i < thrift_tbl.slotDescriptors.size(); ++i) {
    const TSlotDescriptor& tdesc = thrift_tbl.slotDescriptors[i];
    SlotDescriptor* slot_d = pool->Add(new SlotDescriptor(tdesc));
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

TupleDescriptor* DescriptorTbl::GetTupleDescriptor(TupleId id) const {
  // TODO: is there some boost function to do exactly this?
  TupleDescriptorMap::const_iterator i = tuple_desc_map_.find(id);
  if (i == tuple_desc_map_.end()) {
    return NULL;
  } else {
    return i->second;
  }
}

SlotDescriptor* DescriptorTbl::GetSlotDescriptor(SlotId id) const {
  // TODO: is there some boost function to do exactly this?
  SlotDescriptorMap::const_iterator i = slot_desc_map_.find(id);
  if (i == slot_desc_map_.end()) {
    return NULL;
  } else {
    return i->second;
  }
}

// return all registered tuple descriptors
void DescriptorTbl::GetTupleDescs(vector<TupleDescriptor*>* descs) const {
  descs->clear();
  for (TupleDescriptorMap::const_iterator i = tuple_desc_map_.begin();
       i != tuple_desc_map_.end(); ++i) {
    descs->push_back(i->second);
  }
}

string DescriptorTbl::DebugString() const {
  stringstream out;
  out << "tuples:\n";
  for (TupleDescriptorMap::const_iterator i = tuple_desc_map_.begin();
       i != tuple_desc_map_.end(); ++i) {
    out << i->second->DebugString() << '\n';
  }
  return out.str();
}

}
