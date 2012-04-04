// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/descriptors.h"

#include <boost/algorithm/string/join.hpp>
#include <glog/logging.h>
#include <ios>
#include <sstream>

#include "common/object-pool.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;
using namespace boost::algorithm;

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

ostream& operator<<(ostream& os, const TUniqueId& id) {
  os << id.hi << ":" << id.lo;
  return os;
}

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
  : id_(tdesc.id),
    type_(ThriftToType(tdesc.slotType)),
    parent_(tdesc.parent),
    col_pos_(tdesc.columnPos),
    tuple_offset_(tdesc.byteOffset),
    null_indicator_offset_(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit),
    is_materialized_(tdesc.isMaterialized) {
}

std::string SlotDescriptor::DebugString() const {
  stringstream out;
  out << "Slot(id=" << id_ << " type=" << TypeToString(type_)
      << " col=" << col_pos_ << " offset=" << tuple_offset_
      << " null=" << null_indicator_offset_.DebugString() << ")";
  return out.str();
}

TableDescriptor::TableDescriptor(const TTableDescriptor& tdesc)
  : id_(tdesc.id),
    num_cols_(tdesc.numCols),
    num_clustering_cols_(tdesc.numClusteringCols) {
}

string TableDescriptor::DebugString() const {
  stringstream out;
  out << "#cols=" << num_cols_ << " #clustering_cols=" << num_clustering_cols_;
  return out.str();
}

HdfsTableDescriptor::HdfsTableDescriptor(const TTableDescriptor& tdesc)
  : TableDescriptor(tdesc),
    line_delim_(tdesc.hdfsTable.lineDelim),
    field_delim_(tdesc.hdfsTable.fieldDelim),
    collection_delim_(tdesc.hdfsTable.collectionDelim),
    escape_char_(tdesc.hdfsTable.escapeChar),
    hdfs_base_dir_(tdesc.hdfsTable.hdfsBaseDir),
    partition_key_names_(tdesc.hdfsTable.partitionKeyNames),
    null_partition_key_value_(tdesc.hdfsTable.nullPartitionKeyValue) {
}

string HdfsTableDescriptor::DebugString() const {
  stringstream out;
  out << "HdfsTable(" << TableDescriptor::DebugString()
      << " hdfs_base_dir='" << hdfs_base_dir_ << "'"
      << " partition_key_names=[";

  out << join(partition_key_names_, ":");

  out << "]";
  out << "null_partition_key_value='" << null_partition_key_value_ << "'"
      << " line_delim='" << line_delim_ << "'"
      << " field_delim='" << field_delim_ << "'"
      << " coll_delim='" << collection_delim_ << "'"
      << " escape_char='" << escape_char_ << "')";
  return out.str();
}

HBaseTableDescriptor::HBaseTableDescriptor(const TTableDescriptor& tdesc)
  : TableDescriptor(tdesc),
    table_name_(tdesc.hbaseTable.tableName) {
  for (int i = 0; i < tdesc.hbaseTable.families.size(); ++i) {
    cols_.push_back(make_pair(tdesc.hbaseTable.families[i],
        tdesc.hbaseTable.qualifiers[i]));
  }
}

string HBaseTableDescriptor::DebugString() const {
  stringstream out;
  out << "HBaseTable(" << TableDescriptor::DebugString() << " table=" << table_name_;
  out << " cols=[";
  for (int i = 0; i < cols_.size(); ++i) {
    out << (i > 0 ? " " : "") << cols_[i].first << ":" << cols_[i].second;
  }
  out << "])";
  return out.str();
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc)
  : id_(tdesc.id),
    table_desc_(NULL),
    byte_size_(tdesc.byteSize),
    slots_() {
}

void TupleDescriptor::AddSlot(SlotDescriptor* slot) {
  slots_.push_back(slot);
  if (slot->type() == TYPE_STRING && slot->is_materialized()) {
    string_slots_.push_back(slot);
  }
}

string TupleDescriptor::DebugString() const {
  stringstream out;
  out << "Tuple(id=" << id_ << " size=" << byte_size_;
  if (table_desc_ != NULL) {
    out << " " << table_desc_->DebugString();
  }
  out << " slots=[";
  for (size_t i = 0; i < slots_.size(); ++i) {
    if (i > 0) out << ", ";
    out << slots_[i]->DebugString();
  }
  out << "]";
  out << ")";
  return out.str();
}

RowDescriptor::RowDescriptor(const DescriptorTbl& desc_tbl,
                             const std::vector<TTupleId>& row_tuples,
                             const std::vector<bool>& nullable_tuples) {
  DCHECK(nullable_tuples.size() == row_tuples.size());
  for (int i = 0; i < row_tuples.size(); ++i) {
    tuple_desc_map_.push_back(desc_tbl.GetTupleDescriptor(row_tuples[i]));
    DCHECK(tuple_desc_map_.back() != NULL);
    tuple_idx_nullable_map_.push_back(nullable_tuples[i]);
  }

  // find max id
  TupleId max_id = 0;
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    max_id = max(tuple_desc_map_[i]->id(), max_id);
  }

  tuple_idx_map_.resize(max_id + 1, INVALID_IDX);
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    tuple_idx_map_[tuple_desc_map_[i]->id()] = i;
  }
}

int RowDescriptor::GetRowSize() const {
  int size = 0;
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    size += tuple_desc_map_[i]->byte_size();
  }
  return size;
}

int RowDescriptor::GetTupleIdx(TupleId id) const {
  DCHECK_LT(id, tuple_idx_map_.size());
  return tuple_idx_map_[id];
}

bool RowDescriptor::TupleIsNullable(int tuple_idx) const {
  DCHECK_LT(tuple_idx, tuple_idx_nullable_map_.size());
  return tuple_idx_nullable_map_[tuple_idx];
}

void RowDescriptor::ToThrift(std::vector<TTupleId>* row_tuple_ids) {
  row_tuple_ids->clear();
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    row_tuple_ids->push_back(tuple_desc_map_[i]->id());
  }
}

bool RowDescriptor::IsPrefixOf(const RowDescriptor& other_desc) const {
  if (tuple_desc_map_.size() > other_desc.tuple_desc_map_.size()) return false;
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    // pointer comparison okay, descriptors are unique
    if (tuple_desc_map_[i] != other_desc.tuple_desc_map_[i]) return false;
  }
  return true;
}

Status DescriptorTbl::Create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                             DescriptorTbl** tbl) {
  *tbl = pool->Add(new DescriptorTbl());
  // deserialize table descriptors first, they are being referenced by tuple descriptors
  for (size_t i = 0; i < thrift_tbl.tableDescriptors.size(); ++i) {
    const TTableDescriptor& tdesc = thrift_tbl.tableDescriptors[i];
    TableDescriptor* desc = NULL;
    switch (tdesc.tableType) {
      case TTableType::HDFS_TEXT_TABLE:
      case TTableType::HDFS_RCFILE_TABLE:
      case TTableType::HDFS_SEQFILE_TABLE:
        desc = pool->Add(new HdfsTableDescriptor(tdesc));
        break;
      case TTableType::HBASE_TABLE:
        desc = pool->Add(new HBaseTableDescriptor(tdesc));
        break;
      default:
        DCHECK(false) << "invalid table type: " << tdesc.tableType;
    }
    (*tbl)->tbl_desc_map_[tdesc.id] = desc;
  }

  for (size_t i = 0; i < thrift_tbl.tupleDescriptors.size(); ++i) {
    const TTupleDescriptor& tdesc = thrift_tbl.tupleDescriptors[i];
    TupleDescriptor* desc = pool->Add(new TupleDescriptor(tdesc));
    // fix up table pointer
    if (tdesc.__isset.tableId) {
      desc->table_desc_ = (*tbl)->GetTableDescriptor(tdesc.tableId);
      DCHECK(desc->table_desc_ != NULL);
    }
    (*tbl)->tuple_desc_map_[tdesc.id] = desc;
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

TableDescriptor* DescriptorTbl::GetTableDescriptor(TableId id) const {
  // TODO: is there some boost function to do exactly this?
  TableDescriptorMap::const_iterator i = tbl_desc_map_.find(id);
  if (i == tbl_desc_map_.end()) {
    return NULL;
  } else {
    return i->second;
  }
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
