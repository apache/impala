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

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "rpc/thrift-util.h"
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

const int SchemaPathConstants::ARRAY_ITEM;
const int SchemaPathConstants::ARRAY_POS;
const int SchemaPathConstants::MAP_KEY;
const int SchemaPathConstants::MAP_VALUE;

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
    const TupleDescriptor* parent, const TupleDescriptor* children_tuple_descriptor)
  : id_(tdesc.id),
    type_(ColumnType::FromThrift(tdesc.slotType)),
    parent_(parent),
    children_tuple_descriptor_(children_tuple_descriptor),
    col_path_(tdesc.materializedPath),
    tuple_offset_(tdesc.byteOffset),
    null_indicator_offset_(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit),
    slot_idx_(tdesc.slotIdx),
    slot_size_(type_.GetSlotSize()),
    virtual_column_type_(tdesc.virtual_col_type) {
  DCHECK(parent_ != nullptr) << tdesc.parent;
  if (type_.IsComplexType()) {
    DCHECK(tdesc.__isset.itemTupleId);
    DCHECK(children_tuple_descriptor_ != nullptr) << tdesc.itemTupleId;
  } else {
    DCHECK(!tdesc.__isset.itemTupleId);
    DCHECK(children_tuple_descriptor == nullptr);
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
  if (children_tuple_descriptor_ != nullptr) {
    out << " children_tuple_id=" << children_tuple_descriptor_->id();
  }
  out << " offset=" << tuple_offset_ << " null=" << null_indicator_offset_.DebugString()
      << " slot_idx=" << slot_idx_ << " field_idx=" << slot_idx_;
  if (IsVirtual()) {
    out << " virtual_column_type=" << virtual_column_type_;
  }
  out << ")";
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

inline bool SlotDescriptor::IsChildOfStruct() const {
  return parent_->isTupleOfStructSlot();
}

ColumnDescriptor::ColumnDescriptor(const TColumnDescriptor& tdesc)
  : name_(tdesc.name),
    type_(ColumnType::FromThrift(tdesc.type)) {
  if (tdesc.__isset.icebergFieldId) {
    field_id_ = tdesc.icebergFieldId;
    // Get key and value field_id for Iceberg table column with Map type.
    field_map_key_id_ = tdesc.icebergFieldMapKeyId;
    field_map_value_id_ = tdesc.icebergFieldMapValueId;
  }
}

string ColumnDescriptor::DebugString() const {
  return Substitute("$0: $1$2", name_, type_.DebugString(),
      field_id_ != -1 ? " field_id: " + std::to_string(field_id_): "");
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
  : id_(thrift_partition.id),
    thrift_partition_key_exprs_(thrift_partition.partitionKeyExprs) {
  THdfsStorageDescriptor sd = thrift_partition.hdfs_storage_descriptor;
  line_delim_ = sd.lineDelim;
  field_delim_ = sd.fieldDelim;
  collection_delim_ = sd.collectionDelim;
  escape_char_ = sd.escapeChar;
  block_size_ = sd.blockSize;
  file_format_ = sd.fileFormat;
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
  prototype_partition_descriptor_ = pool->Add(new HdfsPartitionDescriptor(
    tdesc.hdfsTable, tdesc.hdfsTable.prototype_partition));
  avro_schema_ = tdesc.hdfsTable.__isset.avroSchema ? tdesc.hdfsTable.avroSchema : "";
  is_full_acid_ = tdesc.hdfsTable.is_full_acid;
  valid_write_id_list_ = tdesc.hdfsTable.valid_write_ids;
  if (tdesc.__isset.icebergTable) {
    is_iceberg_ = true;
    iceberg_table_location_ = tdesc.icebergTable.table_location;
    iceberg_spec_id_ = tdesc.icebergTable.default_partition_spec_id;
    iceberg_partition_specs_ = tdesc.icebergTable.partition_spec;
    const TIcebergPartitionSpec& spec = iceberg_partition_specs_[iceberg_spec_id_];
    DCHECK_EQ(spec.spec_id, iceberg_spec_id_);
    for (const TIcebergPartitionField& spec_field : spec.partition_fields) {
      auto transform_type = spec_field.transform.transform_type;
      if (transform_type == TIcebergPartitionTransformType::VOID) continue;
      iceberg_non_void_partition_fields_.push_back(spec_field);
    }
    iceberg_parquet_compression_codec_ = tdesc.icebergTable.parquet_compression_codec;
    iceberg_parquet_row_group_size_ = tdesc.icebergTable.parquet_row_group_size;
    iceberg_parquet_plain_page_size_ = tdesc.icebergTable.parquet_plain_page_size;
    iceberg_parquet_dict_page_size_ = tdesc.icebergTable.parquet_dict_page_size;
  }
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
  out << " is_full_acid=" << std::boolalpha << is_full_acid_;
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

SystemTableDescriptor::SystemTableDescriptor(const TTableDescriptor& tdesc)
  : TableDescriptor(tdesc), table_name_(tdesc.systemTable.table_name) {}

string SystemTableDescriptor::DebugString() const {
  stringstream out;
  out << "SystemTable(" << TableDescriptor::DebugString() << " table=" << table_name_
      << ")";
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
  // If this is a tuple for struct children then we populate the 'string_slots_' field (in
  // case of a var len string type) or the 'collection_slots_' field (in case of a
  // collection type) of the topmost tuple and not this one.
  TupleDescriptor* const target_tuple = isTupleOfStructSlot() ? master_tuple_ : this;
  if (slot->type().IsVarLenStringType()) {
    target_tuple->string_slots_.push_back(slot);
    target_tuple->has_varlen_slots_ = true;
  }
  if (slot->type().IsCollectionType()) {
    target_tuple->collection_slots_.push_back(slot);
    target_tuple->has_varlen_slots_ = true;
  }
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

Status DescriptorTbl::DeserializeThrift(const TDescriptorTableSerialized& serial_tbl,
    TDescriptorTable* desc_tbl) {
  uint32_t serial_tbl_len = serial_tbl.thrift_desc_tbl.length();
  return DeserializeThriftMsg(
      reinterpret_cast<const uint8_t*>(serial_tbl.thrift_desc_tbl.data()),
      &serial_tbl_len, false, desc_tbl);
}

Status DescriptorTbl::CreateHdfsTblDescriptor(
    const TDescriptorTableSerialized& serialized_thrift_tbl,
    TableId tbl_id, ObjectPool* pool, HdfsTableDescriptor** desc) {
  TDescriptorTable thrift_tbl;
  RETURN_IF_ERROR(DeserializeThrift(serialized_thrift_tbl, &thrift_tbl));
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
    case TTableType::ICEBERG_TABLE:
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
    case TTableType::SYSTEM_TABLE:
      *desc = pool->Add(new SystemTableDescriptor(tdesc));
      break;
    default:
      DCHECK(false) << "invalid table type: " << tdesc.tableType;
  }
  return Status::OK();
}

Status DescriptorTbl::Create(ObjectPool* pool,
    const TDescriptorTableSerialized& serialized_thrift_tbl, DescriptorTbl** tbl) {
  TDescriptorTable thrift_tbl;
  RETURN_IF_ERROR(DeserializeThrift(serialized_thrift_tbl, &thrift_tbl));
  return CreateInternal(pool, thrift_tbl, tbl);
}

Status DescriptorTbl::CreateInternal(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
    DescriptorTbl** tbl) {
  *tbl = pool->Add(new DescriptorTbl());
  // deserialize table descriptors first, they are being referenced by tuple descriptors
  for (const TTableDescriptor& tdesc: thrift_tbl.tableDescriptors) {
    TableDescriptor* desc;
    RETURN_IF_ERROR(CreateTblDescriptorInternal(tdesc, pool, &desc));
    (*tbl)->tbl_desc_map_[tdesc.id] = desc;
  }

  for (const TTupleDescriptor& tdesc : thrift_tbl.tupleDescriptors) {
    TupleDescriptor* desc = pool->Add(new TupleDescriptor(tdesc));
    // fix up table pointer
    if (tdesc.__isset.tableId) {
      desc->table_desc_ = (*tbl)->GetTableDescriptor(tdesc.tableId);
    }
    (*tbl)->tuple_desc_map_[tdesc.id] = desc;
  }

  for (const TSlotDescriptor& tdesc : thrift_tbl.slotDescriptors) {
    // Tuple descriptors are already populated in tbl
    TupleDescriptor* parent = (*tbl)->GetTupleDescriptor(tdesc.parent);
    DCHECK(parent != nullptr);
    TupleDescriptor* children_tuple_descriptor = tdesc.__isset.itemTupleId ?
        (*tbl)->GetTupleDescriptor(tdesc.itemTupleId) : nullptr;
    SlotDescriptor* slot_d = pool->Add(
        new SlotDescriptor(tdesc, parent, children_tuple_descriptor));
    if (slot_d->type().IsStructType() && children_tuple_descriptor != nullptr &&
        children_tuple_descriptor->getMasterTuple() == nullptr) {
      TupleDescriptor* master_tuple = parent;
      // If this struct is nested into other struct(s) then get the topmost tuple for the
      // master.
      if (parent->getMasterTuple() != nullptr) master_tuple = parent->getMasterTuple();
      children_tuple_descriptor->setMasterTuple(master_tuple);
    }
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

void SlotDescriptor::CodegenLoadAnyVal(CodegenAnyVal* any_val, llvm::Value* raw_val_ptr) {
  DCHECK(raw_val_ptr->getType()->isPointerTy());
  llvm::Type* raw_val_type = raw_val_ptr->getType()->getPointerElementType();
  LlvmCodeGen* const codegen = any_val->codegen();
  LlvmBuilder* const builder = any_val->builder();
  const ColumnType& type = any_val->type();
  DCHECK_EQ(raw_val_type, codegen->GetSlotType(type))
      << endl
      << LlvmCodeGen::Print(raw_val_ptr) << endl
      << type << " => " << LlvmCodeGen::Print(
          codegen->GetSlotType(type));
  switch (type.type) {
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      // Convert StringValue to StringVal
      llvm::Function* str_ptr_fn = codegen->GetFunction(
          IRFunction::STRING_VALUE_PTR, false);
      llvm::Function* str_len_fn = codegen->GetFunction(
          IRFunction::STRING_VALUE_LEN, false);

      llvm::Value* ptr = builder->CreateCall(str_ptr_fn,
          llvm::ArrayRef<llvm::Value*>({raw_val_ptr}), "ptr");
      llvm::Value* len = builder->CreateCall(str_len_fn,
          llvm::ArrayRef<llvm::Value*>({raw_val_ptr}), "len");

      any_val->SetPtr(ptr);
      any_val->SetLen(len);
      break;
    }
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE: {
      // Convert fixed-size slot to StringVal.
      any_val->SetPtr(builder->CreateBitCast(raw_val_ptr, codegen->ptr_type()));
      any_val->SetLen(codegen->GetI32Constant(type.len));
      break;
    }
    case TYPE_TIMESTAMP: {
      // Convert TimestampValue to TimestampVal
      // TimestampValue has type
      //   { boost::posix_time::time_duration, boost::gregorian::date }
      // = { {{{i64}}}, {{i32}} }

      llvm::Value* ts_value = builder->CreateLoad(raw_val_ptr, "ts_value");
      // Extract time_of_day i64 from boost::posix_time::time_duration.
      uint32_t time_of_day_idxs[] = {0, 0, 0, 0};
      llvm::Value* time_of_day =
          builder->CreateExtractValue(ts_value, time_of_day_idxs, "time_of_day");
      DCHECK(time_of_day->getType()->isIntegerTy(64));
      any_val->SetTimeOfDay(time_of_day);
      // Extract i32 from boost::gregorian::date
      uint32_t date_idxs[] = {1, 0, 0};
      llvm::Value* date = builder->CreateExtractValue(ts_value, date_idxs, "date");
      DCHECK(date->getType()->isIntegerTy(32));
      any_val->SetDate(date);
      break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMAL:
    case TYPE_DATE:
      any_val->SetVal(builder->CreateLoad(raw_val_ptr, "raw_val"));
      break;
    default:
      DCHECK(false) << "NYI: " << type.DebugString();
      break;
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

// Example IR for materializing a string column with non-NULL 'pool'. Includes the part
// that is generated by CodegenAnyVal::ToReadWriteInfo().
//
// Produced for the following query as part of the @MaterializeExprs() function.
//   select string_col from functional_orc_def.alltypes order by string_col limit 2;
//
//   ; [insert point starts here]
//   br label %entry1
//
// entry1:                                           ; preds = %entry
//   %1 = extractvalue { i64, i8* } %src, 0
//   %is_null = trunc i64 %1 to i1
//   br i1 %is_null, label %null, label %non_null
//
// non_null:                                         ; preds = %entry1
//   %src2 = extractvalue { i64, i8* } %src, 1
//   %2 = extractvalue { i64, i8* } %src, 0
//   %3 = ashr i64 %2, 32
//   %4 = trunc i64 %3 to i32
//   %slot = getelementptr inbounds <{ %"struct.impala::StringValue", i8 }>,
//                                  <{ %"struct.impala::StringValue", i8 }>* %tuple,
//                                  i32 0,
//                                  i32 0
//   %5 = insertvalue %"struct.impala::StringValue" zeroinitializer, i32 %4, 1
//   %6 = sext i32 %4 to i64
//   %new_ptr = call i8* @_ZN6impala7MemPool8AllocateILb0EEEPhli(
//       %"class.impala::MemPool"* %pool,
//       i64 %6,
//       i32 8)
//   call void @llvm.memcpy.p0i8.p0i8.i32(
//       i8* %new_ptr,
//       i8* %src2,
//       i32 %4,
//       i32 0,
//       i1 false)
//   %7 = insertvalue %"struct.impala::StringValue" %5, i8* %new_ptr, 0
//   store %"struct.impala::StringValue" %7, %"struct.impala::StringValue"* %slot
//   br label %end_write
//
// null:                                             ; preds = %entry1
//   %8 = bitcast <{ %"struct.impala::StringValue", i8 }>* %tuple to i8*
//   %null_byte_ptr = getelementptr inbounds i8, i8* %8, i32 12
//   %null_byte = load i8, i8* %null_byte_ptr
//   %null_bit_set = or i8 %null_byte, 1
//   store i8 %null_bit_set, i8* %null_byte_ptr
//   br label %end_write
//
// end_write:                                        ; preds = %null, %non_null
//   ; [insert point ends here]
void SlotDescriptor::CodegenWriteToSlot(const CodegenAnyValReadWriteInfo& read_write_info,
    llvm::Value* tuple_llvm_struct_ptr, llvm::Value* pool_val,
    llvm::BasicBlock* insert_before) const {
  DCHECK(tuple_llvm_struct_ptr->getType()->isPointerTy());
  DCHECK(tuple_llvm_struct_ptr->getType()->getPointerElementType()->isStructTy());
  LlvmBuilder* builder = read_write_info.builder();
  llvm::LLVMContext& context = read_write_info.codegen()->context();
  llvm::Function* fn = builder->GetInsertBlock()->getParent();

  // Create new block that will come after conditional blocks if necessary
  if (insert_before == nullptr) {
    insert_before = llvm::BasicBlock::Create(context, "end_write", fn);
  }

  read_write_info.entry_block().BranchTo(builder);

  CodegenWriteToSlotHelper(read_write_info, tuple_llvm_struct_ptr,
      tuple_llvm_struct_ptr, pool_val, NonWritableBasicBlock(insert_before));

  // Leave builder_ after conditional blocks
  builder->SetInsertPoint(insert_before);
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

// TODO: Maybe separate null handling and non-null-handling so that it is easier to insert
// a different null handling mechanism (for example in hash tables when structs are
// supported there).
void SlotDescriptor::CodegenWriteToSlotHelper(
    const CodegenAnyValReadWriteInfo& read_write_info,
    llvm::Value* main_tuple_llvm_struct_ptr, llvm::Value* tuple_llvm_struct_ptr,
    llvm::Value* pool_val,
    const NonWritableBasicBlock& insert_before) const {
  DCHECK(main_tuple_llvm_struct_ptr->getType()->isPointerTy());
  DCHECK(main_tuple_llvm_struct_ptr->getType()->getPointerElementType()->isStructTy());
  DCHECK(tuple_llvm_struct_ptr->getType()->isPointerTy());
  DCHECK(tuple_llvm_struct_ptr->getType()->getPointerElementType()->isStructTy());
  LlvmBuilder* builder = read_write_info.builder();

  // Non-null block: write slot
  builder->SetInsertPoint(read_write_info.non_null_block());
  llvm::Value* slot = builder->CreateStructGEP(nullptr, tuple_llvm_struct_ptr,
      llvm_field_idx(), "slot");
  if (read_write_info.type().IsStructType()) {
    CodegenStoreStructToNativePtr(read_write_info, main_tuple_llvm_struct_ptr,
        slot, pool_val, insert_before);
  } else {
    CodegenStoreNonNullAnyVal(read_write_info, slot, pool_val, this, insert_before);

    // We only need this branch if we are not a struct, because for structs, the last leaf
    // (non-struct) field will add this branch.
    insert_before.BranchTo(builder);
  }

  // Null block: set null bit
  builder->SetInsertPoint(read_write_info.null_block());
  CodegenSetToNull(read_write_info, main_tuple_llvm_struct_ptr);
  insert_before.BranchTo(builder);
}

void SlotDescriptor::CodegenStoreStructToNativePtr(
    const CodegenAnyValReadWriteInfo& read_write_info, llvm::Value* main_tuple_ptr,
    llvm::Value* struct_slot_ptr, llvm::Value* pool_val,
    const NonWritableBasicBlock& insert_before) const {
  DCHECK(type_.IsStructType());
  DCHECK(children_tuple_descriptor_ != nullptr);
  DCHECK(read_write_info.type().IsStructType());
  DCHECK(main_tuple_ptr->getType()->isPointerTy());
  DCHECK(main_tuple_ptr->getType()->getPointerElementType()->isStructTy());
  DCHECK(struct_slot_ptr->getType()->isPointerTy());
  DCHECK(struct_slot_ptr->getType()->getPointerElementType()->isStructTy());

  LlvmBuilder* builder = read_write_info.builder();
  const std::vector<SlotDescriptor*>& slots = children_tuple_descriptor_->slots();
  DCHECK_GE(slots.size(), 1);
  DCHECK_EQ(slots.size(), read_write_info.children().size());

  read_write_info.children()[0].entry_block().BranchTo(builder);
  for (int i = 0; i < slots.size(); ++i) {
    const SlotDescriptor* const child_slot_desc = slots[i];
    const CodegenAnyValReadWriteInfo& child_read_write_info =
        read_write_info.children()[i];

    NonWritableBasicBlock next_block = i == slots.size() - 1
        ? insert_before : read_write_info.children()[i+1].entry_block();
    child_slot_desc->CodegenWriteToSlotHelper(child_read_write_info, main_tuple_ptr,
        struct_slot_ptr, pool_val, next_block);
  }
}

// Create a 'CodegenAnyValReadWriteInfo' but without creating basic blocks for null
// handling as this function should only be called if we assume that the value is not
// null.
CodegenAnyValReadWriteInfo CodegenAnyValToReadWriteInfo(CodegenAnyVal& any_val,
    llvm::Value* pool_val) {
  CodegenAnyValReadWriteInfo rwi(any_val.codegen(), any_val.builder(), any_val.type());

  switch (rwi.type().type) {
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_ARRAY: // CollectionVal has same memory layout as StringVal.
    case TYPE_MAP: { // CollectionVal has same memory layout as StringVal.
      rwi.SetPtrAndLen(any_val.GetPtr(), any_val.GetLen());
      break;
    }
    case TYPE_CHAR:
      rwi.SetPtrAndLen(any_val.GetPtr(), rwi.codegen()->GetI32Constant(rwi.type().len));
      break;
    case TYPE_FIXED_UDA_INTERMEDIATE:
      DCHECK(false) << "FIXED_UDA_INTERMEDIATE does not need to be copied: the "
                    << "StringVal must be set up to point to the output slot";
      break;
    case TYPE_TIMESTAMP: {
      rwi.SetTimeAndDate(any_val.GetTimeOfDay(), any_val.GetDate());
      break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMAL:
    case TYPE_DATE:
      // The representations of the types match - just store the value.
      rwi.SetSimpleVal(any_val.GetVal());
      break;
    case TYPE_STRUCT:
      DCHECK(false) << "Invalid type for this function. "
                    << "Call 'StoreStructToNativePtr()' instead.";
      break;
    default:
      DCHECK(false) << "NYI: " << rwi.type().DebugString();
      break;
  }

  return rwi;
}

void SlotDescriptor::CodegenStoreNonNullAnyVal(CodegenAnyVal& any_val,
      llvm::Value* raw_val_ptr, llvm::Value* pool_val,
      const SlotDescriptor* slot_desc, const NonWritableBasicBlock& insert_before) {
  CodegenAnyValReadWriteInfo rwi = CodegenAnyValToReadWriteInfo(any_val, pool_val);
  CodegenStoreNonNullAnyVal(rwi, raw_val_ptr, pool_val, slot_desc, insert_before);
}

void SlotDescriptor::CodegenStoreNonNullAnyVal(
    const CodegenAnyValReadWriteInfo& read_write_info, llvm::Value* raw_val_ptr,
    llvm::Value* pool_val, const SlotDescriptor* slot_desc,
    const NonWritableBasicBlock& insert_before) {
  LlvmBuilder* builder = read_write_info.builder();
  const ColumnType& type = read_write_info.type();
  switch (type.type) {
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_ARRAY:
    case TYPE_MAP: {
      CodegenWriteStringOrCollectionToSlot(read_write_info, raw_val_ptr,
          pool_val, slot_desc, insert_before);
      break;
    }
    case TYPE_CHAR:
      read_write_info.codegen()->CodegenMemcpy(
          builder, raw_val_ptr, read_write_info.GetPtrAndLen().ptr, type.len);
      break;
    case TYPE_FIXED_UDA_INTERMEDIATE:
      DCHECK(false) << "FIXED_UDA_INTERMEDIATE does not need to be copied: the "
                    << "StringVal must be set up to point to the output slot";
      break;
    case TYPE_TIMESTAMP: {
      llvm::Value* timestamp_value = CodegenToTimestampValue(read_write_info);
      builder->CreateStore(timestamp_value, raw_val_ptr);
      break;
    }
    case TYPE_BOOLEAN: {
      llvm::Value* bool_as_i1 = builder->CreateTrunc(
          read_write_info.GetSimpleVal(), builder->getInt1Ty(), "bool_as_i1");
      builder->CreateStore(bool_as_i1, raw_val_ptr);
      break;
    }
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMAL:
    case TYPE_DATE:
      // The representations of the types match - just store the value.
      builder->CreateStore(read_write_info.GetSimpleVal(), raw_val_ptr);
      break;
    case TYPE_STRUCT:
      DCHECK(false) << "Invalid type for this function. "
                    << "Call 'StoreStructToNativePtr()' instead.";
      break;
    default:
      DCHECK(false) << "NYI: " << type.DebugString();
      break;
  }
}

llvm::Value* SlotDescriptor::CodegenStoreNonNullAnyValToNewAlloca(
      const CodegenAnyValReadWriteInfo& read_write_info, llvm::Value* pool_val) {
  LlvmCodeGen* codegen = read_write_info.codegen();
  llvm::Value* native_ptr = codegen->CreateEntryBlockAlloca(*read_write_info.builder(),
      codegen->GetSlotType(read_write_info.type()));
  SlotDescriptor::CodegenStoreNonNullAnyVal(read_write_info, native_ptr, pool_val);
  return native_ptr;
}

llvm::Value* SlotDescriptor::CodegenStoreNonNullAnyValToNewAlloca(
      CodegenAnyVal& any_val, llvm::Value* pool_val) {
  CodegenAnyValReadWriteInfo rwi = CodegenAnyValToReadWriteInfo(any_val, pool_val);
  return CodegenStoreNonNullAnyValToNewAlloca(rwi, pool_val);
}

void SlotDescriptor::CodegenSetToNull(const CodegenAnyValReadWriteInfo& read_write_info,
    llvm::Value* tuple) const {
  LlvmCodeGen* codegen = read_write_info.codegen();
  LlvmBuilder* builder = read_write_info.builder();
  CodegenSetNullIndicator(
      codegen, builder, tuple, codegen->true_value());
  if (type_.IsStructType()) {
    DCHECK(children_tuple_descriptor_ != nullptr);
    for (SlotDescriptor* child_slot_desc : children_tuple_descriptor_->slots()) {
      child_slot_desc->CodegenSetToNull(read_write_info, tuple);
    }
  }
}

// Example IR for materializing an int and an array<string> column with non-NULL 'pool'.
// Includes the part that is generated by CodegenAnyVal::ToReadWriteInfo().
//
// Produced for the following query as part of the @MaterializeExprs() function.
//   select id, arr_string_1d from functional_parquet.collection_tbl order by id limit 2;
//
//   ; [insert point starts here]
//   br label %entry1
//
// entry1:                               ; preds = %entry
//   %is_null = trunc i64 %src to i1
//   br i1 %is_null, label %null, label %non_null
//
// non_null:                             ; preds = %entry1
//   %1 = ashr i64 %src, 32
//   %2 = trunc i64 %1 to i32
//   %slot = getelementptr inbounds <{ %"struct.impala::CollectionValue", i32, i8 }>,
//       <{ %"struct.impala::CollectionValue", i32, i8 }>* %tuple, i32 0, i32 1
//   store i32 %2, i32* %slot
//   br label %end_write
//
// null:                                 ; preds = %entry1
//   %3 = bitcast <{ %"struct.impala::CollectionValue", i32, i8 }>* %tuple to i8*
//   %null_byte_ptr = getelementptr inbounds i8, i8* %3, i32 16
//   %null_byte = load i8, i8* %null_byte_ptr
//   %null_bit_set = or i8 %null_byte, 2
//   store i8 %null_bit_set, i8* %null_byte_ptr
//   br label %end_write
//
// end_write:                            ; preds = %null, %non_null
//   %4 = getelementptr %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %slot_materialize_expr_evals, i32 1
//   %expr_eval2 = load %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %4
//   %src3 = call { i64, i8* } @GetSlotRef.4(
//       %"class.impala::ScalarExprEvaluator"* %expr_eval2,
//       %"class.impala::TupleRow"* %row)
//   br label %entry4
//
// entry4:                               ; preds = %end_write
//   %5 = extractvalue { i64, i8* } %src3, 0
//   %is_null7 = trunc i64 %5 to i1
//   br i1 %is_null7, label %null6, label %non_null5
//
// non_null5:                            ; preds = %entry4
//   %src8 = extractvalue { i64, i8* } %src3, 1
//   %6 = extractvalue { i64, i8* } %src3, 0
//   %7 = ashr i64 %6, 32
//   %8 = trunc i64 %7 to i32
//   %slot10 = getelementptr inbounds <{ %"struct.impala::CollectionValue", i32, i8 }>,
//       <{ %"struct.impala::CollectionValue", i32, i8 }>* %tuple, i32 0, i32 0
//   %9 = insertvalue %"struct.impala::CollectionValue" zeroinitializer, i32 %8, 1
//   %coll_tuple_byte_len = mul i32 %8, 13
//   %10 = sext i32 %coll_tuple_byte_len to i64
//   %new_coll_val_ptr = call i8* @_ZN6impala7MemPool8AllocateILb0EEEPhli(
//       %"class.impala::MemPool"* %pool, i64 %10, i32 8)
//   call void @llvm.memcpy.p0i8.p0i8.i32(i8* %new_coll_val_ptr, i8* %src8,
//       i32 %coll_tuple_byte_len, i32 0, i1 false)
//   %11 = insertvalue %"struct.impala::CollectionValue" %9, i8* %new_coll_val_ptr, 0
//   store i32 0, i32* %item_index_addr
//   br label %loop_condition_block
//
// null6:                                ; preds = %entry4
//   %12 = bitcast <{ %"struct.impala::CollectionValue", i32, i8 }>* %tuple to i8*
//   %null_byte_ptr14 = getelementptr inbounds i8, i8* %12, i32 16
//   %null_byte15 = load i8, i8* %null_byte_ptr14
//   %null_bit_set16 = or i8 %null_byte15, 1
//   store i8 %null_bit_set16, i8* %null_byte_ptr14
//   br label %end_write9
//
// loop_condition_block:                 ; preds = %loop_increment_block, %non_null5
//   %item_index = load i32, i32* %item_index_addr
//   %continue_loop = icmp slt i32 %item_index, %8
//   br i1 %continue_loop, label %loop_body_block, label %loop_exit_block
//
// loop_body_block:                      ; preds = %loop_condition_block
//   %children_tuple_array = bitcast i8* %new_coll_val_ptr
//       to <{ %"class.impala::StringValue", i8 }>*
//   %children_tuple = getelementptr inbounds <{ %"class.impala::StringValue", i8 }>,
//       <{ %"class.impala::StringValue", i8 }>* %children_tuple_array, i32 %item_index
//   %13 = bitcast <{ %"class.impala::StringValue", i8 }>* %children_tuple to i8*
//   %null_byte_ptr11 = getelementptr inbounds i8, i8* %13, i32 12
//   %null_byte12 = load i8, i8* %null_byte_ptr11
//   %null_mask = and i8 %null_byte12, 1
//   %is_null13 = icmp ne i8 %null_mask, 0
//   br i1 %is_null13, label %next_block_after_child_is_written,
//       label %child_non_null_block
//
// loop_increment_block:                 ; preds = %next_block_after_child_is_written
//   %item_index_incremented = add i32 %item_index, 1
//   store i32 %item_index_incremented, i32* %item_index_addr
//   br label %loop_condition_block
//
// loop_exit_block:                      ; preds = %loop_condition_block
//   store %"struct.impala::CollectionValue" %11,
//       %"struct.impala::CollectionValue"* %slot10
//   br label %end_write9
//
// child_non_null_block:                 ; preds = %loop_body_block
//   %child_str_or_coll_value_addr = getelementptr inbounds
//       <{ %"class.impala::StringValue", i8 }>,
//       <{ %"class.impala::StringValue", i8 }>* %children_tuple, i32 0, i32 0
//   %child_str_or_coll_value_ptr = call i8* @_ZNK6impala11StringValue5IrPtrEv(
//       %"class.impala::StringValue"* %child_str_or_coll_value_addr)
//   %child_str_or_coll_value_len = call i32 @_ZNK6impala11StringValue5IrLenEv(
//       %"class.impala::StringValue"* %child_str_or_coll_value_addr)
//   %14 = sext i32 %child_str_or_coll_value_len to i64
//   %new_ptr = call i8* @_ZN6impala7MemPool8AllocateILb0EEEPhli(
//       %"class.impala::MemPool"* %pool, i64 %14, i32 8)
//   call void @llvm.memcpy.p0i8.p0i8.i32(i8* %new_ptr, i8* %child_str_or_coll_value_ptr,
//       i32 %child_str_or_coll_value_len, i32 0, i1 false)
//   call void @_ZN6impala11StringValue8IrAssignEPci(
//       %"class.impala::StringValue"* %child_str_or_coll_value_addr, i8* %new_ptr,
//       i32 %child_str_or_coll_value_len)
//   br label %next_block_after_child_is_written
//
// next_block_after_child_is_written:    ; preds = %child_non_null_block, %loop_body_block
//   br label %loop_increment_block
//
// end_write9:                           ; preds = %null6, %loop_exit_block
//   ; [insert point ends here]
void SlotDescriptor::CodegenWriteStringOrCollectionToSlot(
    const CodegenAnyValReadWriteInfo& read_write_info,
    llvm::Value* slot_ptr, llvm::Value* pool_val, const SlotDescriptor* slot_desc,
    const NonWritableBasicBlock& insert_before) {
  const ColumnType& type = read_write_info.type();
  if (type.IsStringType()) {
    CodegenWriteStringToSlot(read_write_info, slot_ptr, pool_val, slot_desc);
  } else {
    DCHECK(type.IsCollectionType());
    CodegenWriteCollectionToSlot(read_write_info, slot_ptr, pool_val, slot_desc,
        insert_before);
  }
}

namespace {
constexpr int COLL_VALUE_PTR_IDX = 0;
constexpr int COLL_VALUE_LEN_IDX = 1;

llvm::Value* CodegenStrOrCollValueGetPtr(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Value* str_or_coll_value_addr, const string& name = "") {
  if (str_or_coll_value_addr->getType() ==
      codegen->GetStructType<StringValue>()->getPointerTo()) {
    llvm::Function* str_ptr_fn = codegen->GetFunction(
        IRFunction::STRING_VALUE_PTR, false);
    return builder->CreateCall(str_ptr_fn,
        llvm::ArrayRef<llvm::Value*>({str_or_coll_value_addr}), name);
  } else {
    DCHECK(str_or_coll_value_addr->getType() ==
        codegen->GetStructType<CollectionValue>()->getPointerTo());
    llvm::Value* ptr_addr = builder->CreateStructGEP(nullptr, str_or_coll_value_addr,
        COLL_VALUE_PTR_IDX, name + "_addr");
    return builder->CreateLoad(ptr_addr, name);
  }
}

llvm::Value* CodegenStrOrCollValueGetLen(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Value* str_or_coll_value_addr, const string& name = "") {
  if (str_or_coll_value_addr->getType() ==
      codegen->GetStructType<StringValue>()->getPointerTo()) {
    llvm::Function* str_len_fn = codegen->GetFunction(
        IRFunction::STRING_VALUE_LEN, false);
    return builder->CreateCall(str_len_fn,
        llvm::ArrayRef<llvm::Value*>({str_or_coll_value_addr}), name);
  } else {
    DCHECK(str_or_coll_value_addr->getType() ==
        codegen->GetStructType<CollectionValue>()->getPointerTo());
    llvm::Value* len_addr = builder->CreateStructGEP(nullptr, str_or_coll_value_addr,
        COLL_VALUE_LEN_IDX, name + "_addr");
    return builder->CreateLoad(len_addr, name);
  }
}

llvm::Value* CodegenCollValueSetPtr(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Value* str_or_coll_value, llvm::Value* ptr, const string& name = "") {
  DCHECK(str_or_coll_value->getType() == codegen->GetStructType<CollectionValue>());
  return builder->CreateInsertValue(str_or_coll_value, ptr, COLL_VALUE_PTR_IDX,
      name);
}

llvm::Value* CodegenCollValueSetLen(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Value* str_or_coll_value, llvm::Value* len, const string& name = "") {
  DCHECK(str_or_coll_value->getType() == codegen->GetStructType<CollectionValue>());
  return builder->CreateInsertValue(str_or_coll_value, len, COLL_VALUE_LEN_IDX,
      name);
}
} /* anonymous namespace */

void SlotDescriptor::CodegenWriteCollectionToSlot(
    const CodegenAnyValReadWriteInfo& read_write_info,
    llvm::Value* slot_ptr, llvm::Value* pool_val, const SlotDescriptor* slot_desc,
    const NonWritableBasicBlock& insert_before) {
  LlvmCodeGen* codegen = read_write_info.codegen();
  LlvmBuilder* builder = read_write_info.builder();
  const ColumnType& type = read_write_info.type();
  DCHECK(type.IsCollectionType());

  // Convert to 'CollectionValue'.
  llvm::Type* raw_type = codegen->GetSlotType(type);
  llvm::Value* coll_value = llvm::Constant::getNullValue(raw_type);
  coll_value = CodegenCollValueSetLen(codegen, builder, coll_value,
      read_write_info.GetPtrAndLen().len);
  if (pool_val != nullptr) {
    llvm::Value* num_tuples = read_write_info.GetPtrAndLen().len;
    DCHECK(slot_desc != nullptr) << "SlotDescriptor needed to calculate the size of "
        << "the collection for copying.";
    // For a 'CollectionValue', 'len' is not the byte size of the whole data but the
    // number of items, so we have to multiply it with the byte size of the item tuple
    // to get the data size.
    int item_tuple_byte_size = slot_desc->children_tuple_descriptor()->byte_size();
    llvm::Value* byte_len = builder->CreateMul(num_tuples,
        codegen->GetI32Constant(item_tuple_byte_size), "coll_tuple_byte_len");

    // Allocate a 'new_ptr' from 'pool_val' and copy the data from 'read_write_info->ptr'.
    llvm::Value* new_ptr = codegen->CodegenMemPoolAllocate(
        builder, pool_val, byte_len, "new_coll_val_ptr");
    codegen->CodegenMemcpy(builder, new_ptr, read_write_info.GetPtrAndLen().ptr,
        byte_len);
    coll_value = CodegenCollValueSetPtr(codegen, builder, coll_value, new_ptr);

    slot_desc->CodegenWriteCollectionItemsToSlot(codegen, builder, new_ptr,
        read_write_info.GetPtrAndLen().len, pool_val, insert_before);
  } else {
    coll_value = CodegenCollValueSetPtr(codegen, builder, coll_value,
        read_write_info.GetPtrAndLen().ptr);
  }
  builder->CreateStore(coll_value, slot_ptr);
}

void SlotDescriptor::CodegenWriteStringToSlot(
    const CodegenAnyValReadWriteInfo& read_write_info,
    llvm::Value* slot_ptr, llvm::Value* pool_val, const SlotDescriptor* slot_desc) {
  LlvmCodeGen* codegen = read_write_info.codegen();
  LlvmBuilder* builder = read_write_info.builder();
  const ColumnType& type = read_write_info.type();
  DCHECK(type.IsStringType());

  llvm::Value* ptr = read_write_info.GetPtrAndLen().ptr;
  llvm::Value* len = read_write_info.GetPtrAndLen().len;
  if (pool_val != nullptr) {
    // Allocate a 'new_ptr' from 'pool_val' and copy the data from 'read_write_info->ptr'.
    llvm::Value* new_ptr = codegen->CodegenMemPoolAllocate(
        builder, pool_val, len, "new_ptr");
    codegen->CodegenMemcpy(builder, new_ptr, ptr, len);
    ptr = new_ptr;
  }
  llvm::Function* str_assign_fn = codegen->GetFunction(
      IRFunction::STRING_VALUE_ASSIGN, false);
  builder->CreateCall(str_assign_fn,
      llvm::ArrayRef<llvm::Value*>({slot_ptr, ptr, len}));
}

void SlotDescriptor::CodegenWriteCollectionItemsToSlot(LlvmCodeGen* codegen,
    LlvmBuilder* builder, llvm::Value* collection_value_ptr, llvm::Value* num_tuples,
    llvm::Value* pool_val, const NonWritableBasicBlock& insert_before) const {
  DCHECK(pool_val != nullptr);
  // We construct a while-like loop using basic blocks and conditional branches to iterate
  // through the items of the collection, recursively.
  llvm::Function* fn = builder->GetInsertBlock()->getParent();
  llvm::BasicBlock* loop_condition_block = insert_before.CreateBasicBlockBefore(
      codegen->context(), "loop_condition_block", fn);
  llvm::BasicBlock* loop_body_block = insert_before.CreateBasicBlockBefore(
      codegen->context(), "loop_body_block", fn);
  llvm::BasicBlock* loop_increment_block = insert_before.CreateBasicBlockBefore(
      codegen->context(), "loop_increment_block", fn);
  llvm::BasicBlock* loop_exit_block = insert_before.CreateBasicBlockBefore(
      codegen->context(), "loop_exit_block", fn);

  // Initialise the loop counter.
  llvm::Value* item_index_addr = codegen->CreateEntryBlockAlloca(
      *builder, codegen->i32_type(), "item_index_addr");
  builder->CreateStore(codegen->GetI32Constant(0), item_index_addr);

  builder->CreateBr(loop_condition_block);

  // Loop condition block
  builder->SetInsertPoint(loop_condition_block);
  llvm::Value* item_index = builder->CreateLoad(item_index_addr, "item_index");
  llvm::Value* continue_loop = builder->CreateICmpSLT(
      item_index, num_tuples, "continue_loop");
  builder->CreateCondBr(continue_loop, loop_body_block, loop_exit_block);

  // Loop body
  builder->SetInsertPoint(loop_body_block);
  CodegenWriteCollectionItemLoopBody(codegen, builder, collection_value_ptr, num_tuples,
      item_index, fn, insert_before, pool_val);
  builder->CreateBr(loop_increment_block);

  // Loop increment
  builder->SetInsertPoint(loop_increment_block);
  llvm::Value* item_index_incremented = builder->CreateAdd(
      item_index, codegen->GetI32Constant(1), "item_index_incremented");
  builder->CreateStore(item_index_incremented, item_index_addr);
  builder->CreateBr(loop_condition_block);

  // Loop exit
  builder->SetInsertPoint(loop_exit_block);
}

void SlotDescriptor::CodegenWriteCollectionItemLoopBody(LlvmCodeGen* codegen,
    LlvmBuilder* builder, llvm::Value* collection_value_ptr, llvm::Value* num_tuples,
    llvm::Value* item_index, llvm::Function* fn,
    const NonWritableBasicBlock& insert_before, llvm::Value* pool_val) const {
  DCHECK(pool_val != nullptr);
  const TupleDescriptor* children_tuple_desc = children_tuple_descriptor();
  DCHECK(children_tuple_desc != nullptr);

  llvm::Type* children_tuple_struct_type = children_tuple_desc->GetLlvmStruct(codegen);
  DCHECK(children_tuple_struct_type != nullptr);
  llvm::PointerType* children_tuple_type = codegen->GetPtrType(
      children_tuple_struct_type);

  llvm::Value* children_tuple_array = builder->CreateBitCast(collection_value_ptr,
      children_tuple_type, "children_tuple_array");
  llvm::Value* children_tuple = builder->CreateInBoundsGEP(children_tuple_array,
      item_index, "children_tuple");

  CodegenWriteCollectionIterateOverChildren(codegen, builder, children_tuple,
      children_tuple, fn, insert_before, pool_val);
}

void SlotDescriptor::CodegenWriteCollectionIterateOverChildren(LlvmCodeGen* codegen,
    LlvmBuilder* builder, llvm::Value* master_tuple, llvm::Value* children_tuple,
    llvm::Function* fn, const NonWritableBasicBlock& insert_before,
    llvm::Value* pool_val) const {
  DCHECK(pool_val != nullptr);
  const TupleDescriptor* children_tuple_desc = children_tuple_descriptor();
  DCHECK(children_tuple_desc != nullptr);

  for (const SlotDescriptor* child_slot_desc : children_tuple_desc->slots()) {
    DCHECK(child_slot_desc != nullptr);

    const ColumnType& child_type = child_slot_desc->type();
    if (child_type.IsVarLenStringType() || child_type.IsCollectionType()) {
      child_slot_desc->CodegenWriteCollectionVarlenChild(codegen, builder, master_tuple,
          children_tuple, fn, insert_before, pool_val);
    } else if (child_type.IsStructType()) {
      child_slot_desc->CodegenWriteCollectionStructChild(codegen, builder,
          master_tuple, children_tuple, fn, insert_before, pool_val);
    }
  }
}

void SlotDescriptor::CodegenWriteCollectionStructChild(LlvmCodeGen* codegen,
    LlvmBuilder* builder, llvm::Value* master_tuple, llvm::Value* tuple,
    llvm::Function* fn, const NonWritableBasicBlock& insert_before,
    llvm::Value* pool_val) const {
  DCHECK(type().IsStructType());

  const TupleDescriptor* children_tuple_desc = children_tuple_descriptor();
  DCHECK(children_tuple_desc != nullptr);

  llvm::Value* children_tuple = builder->CreateStructGEP(nullptr, tuple,
      llvm_field_idx(), "struct_children_tuple");

  // TODO IMPALA-12775: Check whether the struct itself is NULL.
  CodegenWriteCollectionIterateOverChildren(codegen, builder, master_tuple,
      children_tuple, fn, insert_before, pool_val);
}

void SlotDescriptor::CodegenWriteCollectionVarlenChild(LlvmCodeGen* codegen,
    LlvmBuilder* builder, llvm::Value* master_tuple, llvm::Value* children_tuple,
    llvm::Function* fn, const NonWritableBasicBlock& insert_before,
    llvm::Value* pool_val) const {
  DCHECK(pool_val != nullptr);
  DCHECK(type_.IsVarLenStringType() || type_.IsCollectionType());

  llvm::BasicBlock* child_non_null_block = insert_before.CreateBasicBlockBefore(
      codegen->context(), "child_non_null_block", fn);
  llvm::BasicBlock* child_written_block = insert_before.CreateBasicBlockBefore(
      codegen->context(), "next_block_after_child_is_written", fn);

  llvm::Value* child_is_null = CodegenIsNull(codegen, builder, master_tuple);
  builder->CreateCondBr(child_is_null, child_written_block, child_non_null_block);

  // Note: Although the input of CodegenWriteStringOrCollectionToSlot() is a '*Val', not a
  // '*Value', the items of a collection are still '*Value' objects, because the pointer
  // of the collection points to an array of tuples (the items). 'StringValue' has Small
  // String Optimisation, but smallness is not preserved here: even if the 'StringValue'
  // was originally small, the new copy will be a long string.
  builder->SetInsertPoint(child_non_null_block);
  llvm::Value* child_str_or_coll_value_slot = builder->CreateStructGEP(nullptr,
      children_tuple, llvm_field_idx(), "child_str_or_coll_value_addr");
  llvm::Value* child_str_or_coll_value_ptr = CodegenStrOrCollValueGetPtr(codegen, builder,
      child_str_or_coll_value_slot, "child_str_or_coll_value_ptr");
  llvm::Value* child_str_or_coll_value_len = CodegenStrOrCollValueGetLen(codegen, builder,
      child_str_or_coll_value_slot, "child_str_or_coll_value_len");

  CodegenAnyValReadWriteInfo child_rwi(codegen, builder, type());
  child_rwi.SetPtrAndLen(child_str_or_coll_value_ptr, child_str_or_coll_value_len);

  CodegenWriteStringOrCollectionToSlot(child_rwi, child_str_or_coll_value_slot, pool_val,
      this, insert_before);
  builder->CreateBr(child_written_block);
  builder->SetInsertPoint(child_written_block);
}

llvm::Value* SlotDescriptor::CodegenToTimestampValue(
    const CodegenAnyValReadWriteInfo& read_write_info) {
  const ColumnType& type = read_write_info.type();
  DCHECK_EQ(type.type, TYPE_TIMESTAMP);
  // Convert TimestampVal to TimestampValue
  // TimestampValue has type
  //   { boost::posix_time::time_duration, boost::gregorian::date }
  // = { {{{i64}}}, {{i32}} }
  llvm::Type* raw_type = read_write_info.codegen()->GetSlotType(type);
  llvm::Value* timestamp_value = llvm::Constant::getNullValue(raw_type);
  uint32_t time_of_day_idxs[] = {0, 0, 0, 0};

  LlvmBuilder* builder = read_write_info.builder();
  timestamp_value = builder->CreateInsertValue(
      timestamp_value, read_write_info.GetTimeAndDate().time_of_day, time_of_day_idxs);
  uint32_t date_idxs[] = {1, 0, 0};
  timestamp_value = builder->CreateInsertValue(
      timestamp_value, read_write_info.GetTimeAndDate().date, date_idxs);
  return timestamp_value;
}

vector<SlotDescriptor*> TupleDescriptor::SlotsOrderedByIdx() const {
  vector<SlotDescriptor*> sorted_slots(slots().size());
  for (SlotDescriptor* slot: slots()) sorted_slots[slot->slot_idx_] = slot;
  // Check that the size of sorted_slots has not changed. This ensures that the series
  // of slot indexes starts at 0 and increases by 1 for each slot. This also ensures that
  // the returned vector has no nullptr elements.
  DCHECK_EQ(slots().size(), sorted_slots.size());
  return sorted_slots;
}

llvm::StructType* TupleDescriptor::GetLlvmStruct(LlvmCodeGen* codegen) const {
  int curr_struct_offset = 0;
  auto struct_fields_and_offset = GetLlvmTypesAndOffset(codegen, curr_struct_offset);
  vector<llvm::Type*> struct_fields = struct_fields_and_offset.first;
  curr_struct_offset = struct_fields_and_offset.second;

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

  return CreateLlvmStructTypeFromFieldTypes(codegen, struct_fields, 0);
}

pair<vector<llvm::Type*>, int> TupleDescriptor::GetLlvmTypesAndOffset(
    LlvmCodeGen* codegen, int curr_struct_offset) const {
  // Get slots in the order they will appear in LLVM struct.
  vector<SlotDescriptor*> sorted_slots = SlotsOrderedByIdx();

  // Add the slot types to the struct description.
  vector<llvm::Type*> struct_fields;
  for (SlotDescriptor* slot: sorted_slots) {
    DCHECK_EQ(curr_struct_offset, slot->tuple_offset());
    if (slot->type().IsStructType()) {
      const int slot_offset = slot->tuple_offset();
      const TupleDescriptor* children_tuple = slot->children_tuple_descriptor();
      DCHECK(children_tuple != nullptr);
      vector<llvm::Type*> child_field_types = children_tuple->GetLlvmTypesAndOffset(
          codegen, curr_struct_offset).first;
      llvm::StructType* struct_type = children_tuple->CreateLlvmStructTypeFromFieldTypes(
          codegen, child_field_types, slot_offset);
      struct_fields.push_back(struct_type);
    } else {
      struct_fields.push_back(codegen->GetSlotType(slot->type()));
    }
    curr_struct_offset = slot->tuple_offset() + slot->slot_size();
  }
  return make_pair(struct_fields, curr_struct_offset);
}

llvm::StructType* TupleDescriptor::CreateLlvmStructTypeFromFieldTypes(
    LlvmCodeGen* codegen, const vector<llvm::Type*>& field_types,
    int parent_slot_offset) const {
  // Construct the struct type. Use the packed layout although not strictly necessary
  // because the fields are already aligned, so LLVM should not add any padding. The
  // fields are already aligned because we order the slots by descending size and only
  // have powers-of-two slot sizes. Note that STRING and TIMESTAMP slots both occupy
  // 16 bytes although their useful payload is only 12 bytes.
  llvm::StructType* tuple_struct = llvm::StructType::get(codegen->context(),
      llvm::ArrayRef<llvm::Type*>(field_types), true);
  DCHECK(tuple_struct != nullptr);
  const llvm::DataLayout& data_layout = codegen->execution_engine()->getDataLayout();
  const llvm::StructLayout* layout = data_layout.getStructLayout(tuple_struct);
  for (SlotDescriptor* slot: slots()) {
    // Verify that the byte offset in the llvm struct matches the tuple offset
    // computed in the FE.
    DCHECK_EQ(layout->getElementOffset(slot->llvm_field_idx()) + parent_slot_offset,
        slot->tuple_offset()) << id_;
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

std::ostream& operator<<(std::ostream& out,
    const TDescriptorTableSerialized& serial_tbl) {
  out << "TDescriptorTableSerialized(";
  TDescriptorTable desc_tbl;
  if (DescriptorTbl::DeserializeThrift(serial_tbl, &desc_tbl).ok()) {
    out << desc_tbl;
  } else {
    const uint8_t* p =
        reinterpret_cast<const uint8_t*>(serial_tbl.thrift_desc_tbl.data());
    const uint8_t* const end = p + serial_tbl.thrift_desc_tbl.length();
    while (p != end) {
      out << ios::hex << (int)*p++;
    }
  }
  out << ")";
  return out;
}

}

