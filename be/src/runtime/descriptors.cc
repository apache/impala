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

#include "runtime/descriptors.h"

#include <boost/algorithm/string/join.hpp>
#include <gutil/strings/substitute.h>
#include <ios>
#include <sstream>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/DataLayout.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/runtime-state.h"

#include "common/names.h"

using boost::algorithm::join;
using namespace strings;

// In 'thrift_partition', the location is stored in a compressed format that references
// the 'partition_prefixes' of 'thrift_table'. This function decompresses that format into
// a string and stores it in 'result'. If 'location' is not set in the THdfsPartition,
// 'result' is set to the empty string.
static void DecompressLocation(const impala::THdfsTable& thrift_table,
    const impala::THdfsPartition& thrift_partition, string* result) {
  if (!thrift_partition.__isset.location) {
    result->clear();
    return;
  }
  *result = thrift_partition.location.suffix;
  if (thrift_partition.location.prefix_index != -1) {
    // -1 means an uncompressed location
    DCHECK_GE(thrift_partition.location.prefix_index, 0);
    DCHECK_LT(
        thrift_partition.location.prefix_index, thrift_table.partition_prefixes.size());
    *result =
        thrift_table.partition_prefixes[thrift_partition.location.prefix_index] + *result;
  }
}

namespace impala {

const int RowDescriptor::INVALID_IDX;

const char* TupleDescriptor::LLVM_CLASS_NAME = "class.impala::TupleDescriptor";
const char* NullIndicatorOffset::LLVM_CLASS_NAME = "struct.impala::NullIndicatorOffset";

string NullIndicatorOffset::DebugString() const {
  stringstream out;
  out << "(offset=" << byte_offset
      << " mask=" << hex << static_cast<int>(bit_mask) << dec << ")";
  return out.str();
}

llvm::Constant* NullIndicatorOffset::ToIR(LlvmCodeGen* codegen) const {
  llvm::StructType* null_indicator_offset_type =
      codegen->GetStructType<NullIndicatorOffset>();
  // Populate padding at end of struct with zeroes.
  llvm::ConstantAggregateZero* zeroes =
      llvm::ConstantAggregateZero::get(null_indicator_offset_type);
  return llvm::ConstantStruct::get(null_indicator_offset_type,
      {codegen->GetI32Constant(byte_offset),
          codegen->GetI8Constant(bit_mask),
          zeroes->getStructElement(2)});
}

ostream& operator<<(ostream& os, const NullIndicatorOffset& null_indicator) {
  os << null_indicator.DebugString();
  return os;
}

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc,
    const TupleDescriptor* parent, const TupleDescriptor* collection_item_descriptor)
  : id_(tdesc.id),
    type_(ColumnType::FromThrift(tdesc.slotType)),
    parent_(parent),
    collection_item_descriptor_(collection_item_descriptor),
    col_path_(tdesc.materializedPath),
    tuple_offset_(tdesc.byteOffset),
    null_indicator_offset_(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit),
    slot_idx_(tdesc.slotIdx),
    slot_size_(type_.GetSlotSize()) {
  DCHECK_NE(type_.type, TYPE_STRUCT);
  DCHECK(parent_ != nullptr) << tdesc.parent;
  if (type_.IsCollectionType()) {
    DCHECK(tdesc.__isset.itemTupleId);
    DCHECK(collection_item_descriptor_ != nullptr) << tdesc.itemTupleId;
  } else {
    DCHECK(!tdesc.__isset.itemTupleId);
    DCHECK(collection_item_descriptor == nullptr);
  }
}

bool SlotDescriptor::ColPathLessThan(const SlotDescriptor* a, const SlotDescriptor* b) {
  int common_levels = min(a->col_path().size(), b->col_path().size());
  for (int i = 0; i < common_levels; ++i) {
    if (a->col_path()[i] == b->col_path()[i]) continue;
    return a->col_path()[i] < b->col_path()[i];
  }
  return a->col_path().size() < b->col_path().size();
}

string SlotDescriptor::DebugString() const {
  stringstream out;
  out << "Slot(id=" << id_ << " type=" << type_.DebugString()
      << " col_path=[";
  if (col_path_.size() > 0) out << col_path_[0];
  for (int i = 1; i < col_path_.size(); ++i) {
    out << ",";
    out << col_path_[i];
  }
  out << "]";
  if (collection_item_descriptor_ != nullptr) {
    out << " collection_item_tuple_id=" << collection_item_descriptor_->id();
  }
  out << " offset=" << tuple_offset_ << " null=" << null_indicator_offset_.DebugString()
      << " slot_idx=" << slot_idx_ << " field_idx=" << llvm_field_idx_
      << ")";
  return out.str();
}

bool SlotDescriptor::LayoutEquals(const SlotDescriptor& other_desc) const {
  if (type() != other_desc.type()) return false;
  if (is_nullable() != other_desc.is_nullable()) return false;
  if (slot_size() != other_desc.slot_size()) return false;
  if (tuple_offset() != other_desc.tuple_offset()) return false;
  if (!null_indicator_offset().Equals(other_desc.null_indicator_offset())) return false;
  return true;
}

ColumnDescriptor::ColumnDescriptor(const TColumnDescriptor& tdesc)
  : name_(tdesc.name),
    type_(ColumnType::FromThrift(tdesc.type)) {
}

string ColumnDescriptor::DebugString() const {
  return Substitute("$0: $1", name_, type_.DebugString());
}

TableDescriptor::TableDescriptor(const TTableDescriptor& tdesc)
  : name_(tdesc.tableName),
    database_(tdesc.dbName),
    id_(tdesc.id),
    type_(tdesc.tableType),
    num_clustering_cols_(tdesc.numClusteringCols) {
  for (int i = 0; i < tdesc.columnDescriptors.size(); ++i) {
    col_descs_.push_back(ColumnDescriptor(tdesc.columnDescriptors[i]));
  }
}

string TableDescriptor::fully_qualified_name() const {
  return Substitute("$0.$1", database_, name_);
}

string TableDescriptor::DebugString() const {
  vector<string> cols;
  for (const ColumnDescriptor& col_desc: col_descs_) {
    cols.push_back(col_desc.DebugString());
  }
  stringstream out;
  out << "#cols=" << num_cols() << " #clustering_cols=" << num_clustering_cols_;
  out << " cols=[";
  out << join(cols, ", ");
  out << "]";
  return out.str();
}

HdfsPartitionDescriptor::HdfsPartitionDescriptor(
    const THdfsTable& thrift_table, const THdfsPartition& thrift_partition)
  : line_delim_(thrift_partition.lineDelim),
    field_delim_(thrift_partition.fieldDelim),
    collection_delim_(thrift_partition.collectionDelim),
    escape_char_(thrift_partition.escapeChar),
    block_size_(thrift_partition.blockSize),
    id_(thrift_partition.id),
    thrift_partition_key_exprs_(thrift_partition.partitionKeyExprs),
    file_format_(thrift_partition.fileFormat) {
  DecompressLocation(thrift_table, thrift_partition, &location_);
}

string HdfsPartitionDescriptor::DebugString() const {
  stringstream out;
  out << " file_format=" << file_format_ << "'"
      << " line_delim='" << line_delim_ << "'"
      << " field_delim='" << field_delim_ << "'"
      << " coll_delim='" << collection_delim_ << "'"
      << " escape_char='" << escape_char_ << "')";
  return out.str();
}

string DataSourceTableDescriptor::DebugString() const {
  stringstream out;
  out << "DataSourceTable(" << TableDescriptor::DebugString() << ")";
  return out.str();
}

HdfsTableDescriptor::HdfsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool)
  : TableDescriptor(tdesc),
    hdfs_base_dir_(tdesc.hdfsTable.hdfsBaseDir),
    null_partition_key_value_(tdesc.hdfsTable.nullPartitionKeyValue),
    null_column_value_(tdesc.hdfsTable.nullColumnValue) {
  for (const auto& entry : tdesc.hdfsTable.partitions) {
    HdfsPartitionDescriptor* partition =
        pool->Add(new HdfsPartitionDescriptor(tdesc.hdfsTable, entry.second));
    partition_descriptors_[entry.first] = partition;
  }
  avro_schema_ = tdesc.hdfsTable.__isset.avroSchema ? tdesc.hdfsTable.avroSchema : "";
}

void HdfsTableDescriptor::ReleaseResources() {
  for (const auto& part_entry: partition_descriptors_) {
    for (ScalarExprEvaluator* eval :
           part_entry.second->partition_key_value_evals()) {
      eval->Close(nullptr);
      const_cast<ScalarExpr&>(eval->root()).Close();
    }
  }
}

string HdfsTableDescriptor::DebugString() const {
  stringstream out;
  out << "HdfsTable(" << TableDescriptor::DebugString()
      << " hdfs_base_dir='" << hdfs_base_dir_ << "'";
  out << " partitions=[";
  vector<string> partition_strings;
  map<int64_t, HdfsPartitionDescriptor*>::const_iterator it;
  for (it = partition_descriptors_.begin(); it != partition_descriptors_.end(); ++it) {
    stringstream s;
    s << " (id: " << it->first << ", partition: " << it->second->DebugString() << ")";
    partition_strings.push_back(s.str());
  }
  out << join(partition_strings, ",") << "]";

  out << " null_partition_key_value='" << null_partition_key_value_ << "'";
  out << " null_column_value='" << null_column_value_ << "'";
  return out.str();
}

HBaseTableDescriptor::HBaseTableDescriptor(const TTableDescriptor& tdesc)
  : TableDescriptor(tdesc),
    table_name_(tdesc.hbaseTable.tableName) {
  for (int i = 0; i < tdesc.hbaseTable.families.size(); ++i) {
    bool is_binary_encoded = tdesc.hbaseTable.__isset.binary_encoded &&
        tdesc.hbaseTable.binary_encoded[i];
    cols_.push_back(HBaseTableDescriptor::HBaseColumnDescriptor(
        tdesc.hbaseTable.families[i], tdesc.hbaseTable.qualifiers[i], is_binary_encoded));
  }
}

string HBaseTableDescriptor::DebugString() const {
  stringstream out;
  out << "HBaseTable(" << TableDescriptor::DebugString() << " table=" << table_name_;
  out << " cols=[";
  for (int i = 0; i < cols_.size(); ++i) {
    out << (i > 0 ? " " : "") << cols_[i].family << ":" << cols_[i].qualifier << ":"
        << cols_[i].binary_encoded;
  }
  out << "])";
  return out.str();
}

KuduTableDescriptor::KuduTableDescriptor(const TTableDescriptor& tdesc)
  : TableDescriptor(tdesc),
    table_name_(tdesc.kuduTable.table_name),
    key_columns_(tdesc.kuduTable.key_columns),
    master_addresses_(tdesc.kuduTable.master_addresses) {
}

string KuduTableDescriptor::DebugString() const {
  stringstream out;
  out << "KuduTable(" << TableDescriptor::DebugString() << " table=" << table_name_;
  out << " master_addrs=[" <<   join(master_addresses_, ",") << "]";
  out << " key_columns=[";
  out << join(key_columns_, ":");
  out << "])";
  return out.str();
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc)
  : id_(tdesc.id),
    byte_size_(tdesc.byteSize),
    num_null_bytes_(tdesc.numNullBytes),
    null_bytes_offset_(tdesc.byteSize - tdesc.numNullBytes),
    has_varlen_slots_(false),
    tuple_path_(tdesc.tuplePath) {
}

void TupleDescriptor::AddSlot(SlotDescriptor* slot) {
  slots_.push_back(slot);
  if (slot->type().IsVarLenStringType()) {
    string_slots_.push_back(slot);
    has_varlen_slots_ = true;
  }
  if (slot->type().IsCollectionType()) {
    collection_slots_.push_back(slot);
    has_varlen_slots_ = true;
  }
}

bool TupleDescriptor::ContainsStringData() const {
  if (!string_slots_.empty()) return true;
  for (int i = 0; i < collection_slots_.size(); ++i) {
    if (collection_slots_[i]->collection_item_descriptor_->ContainsStringData()) {
      return true;
    }
  }
  return false;
}

string TupleDescriptor::DebugString() const {
  stringstream out;
  out << "Tuple(id=" << id_ << " size=" << byte_size_;
  if (table_desc_ != NULL) {
    //out << " " << table_desc_->DebugString();
  }
  out << " slots=[";
  for (size_t i = 0; i < slots_.size(); ++i) {
    if (i > 0) out << ", ";
    out << slots_[i]->DebugString();
  }
  out << "]";
  out << " tuple_path=[";
  for (size_t i = 0; i < tuple_path_.size(); ++i) {
    if (i > 0) out << ", ";
    out << tuple_path_[i];
  }
  out << "]";
  out << ")";
  return out.str();
}

bool TupleDescriptor::LayoutEquals(const TupleDescriptor& other_desc) const {
  if (byte_size() != other_desc.byte_size()) return false;
  if (slots().size() != other_desc.slots().size()) return false;

  vector<SlotDescriptor*> slots = SlotsOrderedByIdx();
  vector<SlotDescriptor*> other_slots = other_desc.SlotsOrderedByIdx();
  for (int i = 0; i < slots.size(); ++i) {
    if (!slots[i]->LayoutEquals(*other_slots[i])) return false;
  }
  return true;
}

RowDescriptor::RowDescriptor(const DescriptorTbl& desc_tbl,
                             const vector<TTupleId>& row_tuples,
                             const vector<bool>& nullable_tuples)
  : tuple_idx_nullable_map_(nullable_tuples) {
  DCHECK_EQ(nullable_tuples.size(), row_tuples.size());
  DCHECK_GT(row_tuples.size(), 0);
  for (int i = 0; i < row_tuples.size(); ++i) {
    tuple_desc_map_.push_back(desc_tbl.GetTupleDescriptor(row_tuples[i]));
    DCHECK(tuple_desc_map_.back() != NULL);
  }
  InitTupleIdxMap();
  InitHasVarlenSlots();
}

RowDescriptor::RowDescriptor(const RowDescriptor& lhs_row_desc,
    const RowDescriptor& rhs_row_desc) {
  tuple_desc_map_.insert(tuple_desc_map_.end(), lhs_row_desc.tuple_desc_map_.begin(),
      lhs_row_desc.tuple_desc_map_.end());
  tuple_desc_map_.insert(tuple_desc_map_.end(), rhs_row_desc.tuple_desc_map_.begin(),
      rhs_row_desc.tuple_desc_map_.end());
  tuple_idx_nullable_map_.insert(tuple_idx_nullable_map_.end(),
      lhs_row_desc.tuple_idx_nullable_map_.begin(),
      lhs_row_desc.tuple_idx_nullable_map_.end());
  tuple_idx_nullable_map_.insert(tuple_idx_nullable_map_.end(),
      rhs_row_desc.tuple_idx_nullable_map_.begin(),
      rhs_row_desc.tuple_idx_nullable_map_.end());
  InitTupleIdxMap();
  InitHasVarlenSlots();
}

RowDescriptor::RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable)
  : tuple_desc_map_(1, tuple_desc),
    tuple_idx_nullable_map_(1, is_nullable) {
  InitTupleIdxMap();
  InitHasVarlenSlots();
}

void RowDescriptor::InitTupleIdxMap() {
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

void RowDescriptor::InitHasVarlenSlots() {
  has_varlen_slots_ = false;
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    if (tuple_desc_map_[i]->HasVarlenSlots()) {
      has_varlen_slots_ = true;
      break;
    }
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
  DCHECK_LT(id, tuple_idx_map_.size()) << "RowDescriptor: " << DebugString();
  return tuple_idx_map_[id];
}

bool RowDescriptor::TupleIsNullable(int tuple_idx) const {
  DCHECK_LT(tuple_idx, tuple_idx_nullable_map_.size());
  return tuple_idx_nullable_map_[tuple_idx];
}

bool RowDescriptor::IsAnyTupleNullable() const {
  for (int i = 0; i < tuple_idx_nullable_map_.size(); ++i) {
    if (tuple_idx_nullable_map_[i]) return true;
  }
  return false;
}

void RowDescriptor::ToThrift(vector<TTupleId>* row_tuple_ids) const {
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

bool RowDescriptor::Equals(const RowDescriptor& other_desc) const {
  if (tuple_desc_map_.size() != other_desc.tuple_desc_map_.size()) return false;
  return IsPrefixOf(other_desc);
}

bool RowDescriptor::LayoutIsPrefixOf(const RowDescriptor& other_desc) const {
  if (tuple_desc_map_.size() > other_desc.tuple_desc_map_.size()) return false;
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    if (!tuple_desc_map_[i]->LayoutEquals(*other_desc.tuple_desc_map_[i])) return false;
  }
  return true;
}

bool RowDescriptor::LayoutEquals(const RowDescriptor& other_desc) const {
  if (tuple_desc_map_.size() != other_desc.tuple_desc_map_.size()) return false;
  return LayoutIsPrefixOf(other_desc);
}

string RowDescriptor::DebugString() const {
  stringstream ss;
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    ss << tuple_desc_map_[i]->DebugString() << endl;
  }
  return ss.str();
}

Status DescriptorTbl::CreatePartKeyExprs(
    const HdfsTableDescriptor& hdfs_tbl, ObjectPool* pool) {
  // Prepare and open partition exprs
  for (const auto& part_entry : hdfs_tbl.partition_descriptors()) {
    HdfsPartitionDescriptor* part_desc = part_entry.second;
    vector<ScalarExpr*> partition_key_value_exprs;
    RETURN_IF_ERROR(ScalarExpr::Create(part_desc->thrift_partition_key_exprs_,
         RowDescriptor(), nullptr, pool, &partition_key_value_exprs));
    for (const ScalarExpr* partition_expr : partition_key_value_exprs) {
      DCHECK(partition_expr->IsLiteral());
      DCHECK(!partition_expr->HasFnCtx());
      DCHECK_EQ(partition_expr->GetNumChildren(), 0);
    }
    // TODO: RowDescriptor should arguably be optional in Prepare for known literals.
    // Partition exprs are not used in the codegen case. Don't codegen them.
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(partition_key_value_exprs, nullptr,
        pool, nullptr, nullptr, &part_desc->partition_key_value_evals_));
    RETURN_IF_ERROR(ScalarExprEvaluator::Open(
        part_desc->partition_key_value_evals_, nullptr));
  }
  return Status::OK();
}

Status DescriptorTbl::CreateHdfsTblDescriptor(const TDescriptorTable& thrift_tbl,
    TableId tbl_id, ObjectPool* pool, HdfsTableDescriptor** desc) {
  for (const TTableDescriptor& tdesc: thrift_tbl.tableDescriptors) {
    if (tdesc.id == tbl_id) {
      DCHECK(tdesc.__isset.hdfsTable);
      RETURN_IF_ERROR(CreateTblDescriptorInternal(
          tdesc, pool, reinterpret_cast<TableDescriptor**>(desc)));
      return Status::OK();
    }
  }
  string error = Substitute("table $0 not found in descriptor table",  tbl_id);
  DCHECK(false) << error;
  return Status(error);
}

Status DescriptorTbl::CreateTblDescriptorInternal(const TTableDescriptor& tdesc,
    ObjectPool* pool, TableDescriptor** desc) {
  *desc = nullptr;
  switch (tdesc.tableType) {
    case TTableType::HDFS_TABLE: {
      HdfsTableDescriptor* hdfs_tbl = pool->Add(new HdfsTableDescriptor(tdesc, pool));
      *desc = hdfs_tbl;
      RETURN_IF_ERROR(CreatePartKeyExprs(*hdfs_tbl, pool));
      break;
    }
    case TTableType::HBASE_TABLE:
      *desc = pool->Add(new HBaseTableDescriptor(tdesc));
      break;
    case TTableType::DATA_SOURCE_TABLE:
      *desc = pool->Add(new DataSourceTableDescriptor(tdesc));
      break;
    case TTableType::KUDU_TABLE:
      *desc = pool->Add(new KuduTableDescriptor(tdesc));
      break;
    default:
      DCHECK(false) << "invalid table type: " << tdesc.tableType;
  }
  return Status::OK();
}

Status DescriptorTbl::Create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
    DescriptorTbl** tbl) {
  *tbl = pool->Add(new DescriptorTbl());
  // deserialize table descriptors first, they are being referenced by tuple descriptors
  for (const TTableDescriptor& tdesc: thrift_tbl.tableDescriptors) {
    TableDescriptor* desc;
    RETURN_IF_ERROR(CreateTblDescriptorInternal(tdesc, pool, &desc));
    (*tbl)->tbl_desc_map_[tdesc.id] = desc;
  }

  for (size_t i = 0; i < thrift_tbl.tupleDescriptors.size(); ++i) {
    const TTupleDescriptor& tdesc = thrift_tbl.tupleDescriptors[i];
    TupleDescriptor* desc = pool->Add(new TupleDescriptor(tdesc));
    // fix up table pointer
    if (tdesc.__isset.tableId) {
      desc->table_desc_ = (*tbl)->GetTableDescriptor(tdesc.tableId);
    }
    (*tbl)->tuple_desc_map_[tdesc.id] = desc;
  }

  for (size_t i = 0; i < thrift_tbl.slotDescriptors.size(); ++i) {
    const TSlotDescriptor& tdesc = thrift_tbl.slotDescriptors[i];
    // Tuple descriptors are already populated in tbl
    TupleDescriptor* parent = (*tbl)->GetTupleDescriptor(tdesc.parent);
    DCHECK(parent != nullptr);
    TupleDescriptor* collection_item_descriptor = tdesc.__isset.itemTupleId ?
        (*tbl)->GetTupleDescriptor(tdesc.itemTupleId) : nullptr;
    SlotDescriptor* slot_d = pool->Add(
        new SlotDescriptor(tdesc, parent, collection_item_descriptor));
    (*tbl)->slot_desc_map_[tdesc.id] = slot_d;
    parent->AddSlot(slot_d);
  }
  return Status::OK();
}

void DescriptorTbl::ReleaseResources() {
  // close partition exprs of hdfs tables
  for (auto entry: tbl_desc_map_) {
    if (entry.second->type() != TTableType::HDFS_TABLE) continue;
    static_cast<HdfsTableDescriptor*>(entry.second)->ReleaseResources();
  }
}

TableDescriptor* DescriptorTbl::GetTableDescriptor(TableId id) const {
  TableDescriptorMap::const_iterator i = tbl_desc_map_.find(id);
  return i == tbl_desc_map_.end() ? nullptr : i->second;
}

TupleDescriptor* DescriptorTbl::GetTupleDescriptor(TupleId id) const {
  TupleDescriptorMap::const_iterator i = tuple_desc_map_.find(id);
  return i == tuple_desc_map_.end() ? nullptr : i->second;
}

SlotDescriptor* DescriptorTbl::GetSlotDescriptor(SlotId id) const {
  SlotDescriptorMap::const_iterator i = slot_desc_map_.find(id);
  return i == slot_desc_map_.end() ? nullptr : i->second;
}

void DescriptorTbl::GetTupleDescs(vector<TupleDescriptor*>* descs) const {
  descs->clear();
  for (TupleDescriptorMap::const_iterator i = tuple_desc_map_.begin();
       i != tuple_desc_map_.end(); ++i) {
    descs->push_back(i->second);
  }
}

llvm::Value* SlotDescriptor::CodegenIsNull(
    LlvmCodeGen* codegen, LlvmBuilder* builder, llvm::Value* tuple) const {
  return CodegenIsNull(codegen, builder, null_indicator_offset_, tuple);
}

// Example IR for getting the first null bit:
//  %0 = bitcast { i8, [7 x i8], %"class.impala::TimestampValue" }* %agg_tuple to i8*
//  %null_byte_ptr = getelementptr i8, i8* %0, i32 0
//  %null_byte = load i8, i8* %null_byte_ptr
//  %null_mask = and i8 %null_byte, 1
//  %is_null = icmp ne i8 %null_mask, 0
llvm::Value* SlotDescriptor::CodegenIsNull(LlvmCodeGen* codegen, LlvmBuilder* builder,
    const NullIndicatorOffset& null_indicator_offset, llvm::Value* tuple) {
  llvm::Value* null_byte =
      CodegenGetNullByte(codegen, builder, null_indicator_offset, tuple, nullptr);
  llvm::Constant* mask = codegen->GetI8Constant(null_indicator_offset.bit_mask);
  llvm::Value* null_mask = builder->CreateAnd(null_byte, mask, "null_mask");
  llvm::Constant* zero = codegen->GetI8Constant(0);
  return builder->CreateICmpNE(null_mask, zero, "is_null");
}

// Example IR for setting the first null bit to a non-constant 'is_null' value:
//  %14 = bitcast { i8, [7 x i8], %"class.impala::TimestampValue" }* %agg_tuple to i8*
//  %null_byte_ptr3 = getelementptr i8, i8* %14, i32 0
//  %null_byte4 = load i8, i8* %null_byte_ptr3
//  %null_bit_cleared = and i8 %null_byte4, -2
//  %15 = sext i1 %result_is_null to i8
//  %null_bit = and i8 %15, 1
//  %null_bit_set = or i8 %null_bit_cleared, %null_bit
//  store i8 %null_bit_set, i8* %null_byte_ptr3
void SlotDescriptor::CodegenSetNullIndicator(
    LlvmCodeGen* codegen, LlvmBuilder* builder, llvm::Value* tuple, llvm::Value* is_null)
    const {
  DCHECK_EQ(is_null->getType(), codegen->bool_type());
  llvm::Value* null_byte_ptr;
  llvm::Value* null_byte =
      CodegenGetNullByte(codegen, builder, null_indicator_offset_, tuple, &null_byte_ptr);
  llvm::Constant* mask = codegen->GetI8Constant(null_indicator_offset_.bit_mask);
  llvm::Constant* not_mask = codegen->GetI8Constant(~null_indicator_offset_.bit_mask);

  llvm::ConstantInt* constant_is_null = llvm::dyn_cast<llvm::ConstantInt>(is_null);
  llvm::Value* result = nullptr;
  if (constant_is_null != nullptr) {
    if (constant_is_null->isOne()) {
      result = builder->CreateOr(null_byte, mask, "null_bit_set");
    } else {
      DCHECK(constant_is_null->isZero());
      result = builder->CreateAnd(null_byte, not_mask, "null_bit_cleared");
    }
  } else {
    // Avoid branching by computing the new byte as:
    // (null_byte & ~mask) | (-null & mask);
    llvm::Value* byte_with_cleared_bit =
        builder->CreateAnd(null_byte, not_mask, "null_bit_cleared");
    llvm::Value* sign_extended_null =
        builder->CreateSExt(is_null, codegen->i8_type());
    llvm::Value* bit_only = builder->CreateAnd(sign_extended_null, mask, "null_bit");
    result = builder->CreateOr(byte_with_cleared_bit, bit_only, "null_bit_set");
  }

  builder->CreateStore(result, null_byte_ptr);
}

llvm::Value* SlotDescriptor::CodegenGetNullByte(
    LlvmCodeGen* codegen, LlvmBuilder* builder,
    const NullIndicatorOffset& null_indicator_offset, llvm::Value* tuple,
    llvm::Value** null_byte_ptr) {
  llvm::Constant* byte_offset =
      codegen->GetI32Constant(null_indicator_offset.byte_offset);
  llvm::Value* tuple_bytes = builder->CreateBitCast(tuple, codegen->ptr_type());
  llvm::Value* byte_ptr =
      builder->CreateInBoundsGEP(tuple_bytes, byte_offset, "null_byte_ptr");
  if (null_byte_ptr != nullptr) *null_byte_ptr = byte_ptr;
  return builder->CreateLoad(byte_ptr, "null_byte");
}

vector<SlotDescriptor*> TupleDescriptor::SlotsOrderedByIdx() const {
  vector<SlotDescriptor*> sorted_slots(slots().size());
  for (SlotDescriptor* slot: slots()) sorted_slots[slot->slot_idx_] = slot;
  return sorted_slots;
}

llvm::StructType* TupleDescriptor::GetLlvmStruct(LlvmCodeGen* codegen) const {
  // Get slots in the order they will appear in LLVM struct.
  vector<SlotDescriptor*> sorted_slots = SlotsOrderedByIdx();

  // Add the slot types to the struct description.
  vector<llvm::Type*> struct_fields;
  int curr_struct_offset = 0;
  for (SlotDescriptor* slot: sorted_slots) {
    // IMPALA-3207: Codegen for CHAR is not yet implemented: bail out of codegen here.
    if (slot->type().type == TYPE_CHAR) return nullptr;
    DCHECK_EQ(curr_struct_offset, slot->tuple_offset());
    slot->llvm_field_idx_ = struct_fields.size();
    struct_fields.push_back(codegen->GetSlotType(slot->type()));
    curr_struct_offset = slot->tuple_offset() + slot->slot_size();
  }
  // For each null byte, add a byte to the struct
  for (int i = 0; i < num_null_bytes_; ++i) {
    struct_fields.push_back(codegen->i8_type());
    ++curr_struct_offset;
  }

  DCHECK_LE(curr_struct_offset, byte_size_);
  if (curr_struct_offset < byte_size_) {
    struct_fields.push_back(llvm::ArrayType::get(codegen->i8_type(),
        byte_size_ - curr_struct_offset));
  }

  // Construct the struct type. Use the packed layout although not strictly necessary
  // because the fields are already aligned, so LLVM should not add any padding. The
  // fields are already aligned because we order the slots by descending size and only
  // have powers-of-two slot sizes. Note that STRING and TIMESTAMP slots both occupy
  // 16 bytes although their useful payload is only 12 bytes.
  llvm::StructType* tuple_struct = llvm::StructType::get(codegen->context(),
      llvm::ArrayRef<llvm::Type*>(struct_fields), true);
  const llvm::DataLayout& data_layout = codegen->execution_engine()->getDataLayout();
  const llvm::StructLayout* layout = data_layout.getStructLayout(tuple_struct);
  for (SlotDescriptor* slot: slots()) {
    // Verify that the byte offset in the llvm struct matches the tuple offset
    // computed in the FE.
    DCHECK_EQ(layout->getElementOffset(slot->llvm_field_idx()), slot->tuple_offset());
  }
  return tuple_struct;
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
