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

#include "runtime/descriptors.h"

#include <boost/algorithm/string/join.hpp>
#include <gutil/strings/substitute.h>
#include <ios>
#include <sstream>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/DataLayout.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exprs/expr.h"
#include "runtime/runtime-state.h"

#include "common/names.h"

using boost::algorithm::join;
using namespace llvm;
using namespace strings;

namespace impala {

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

SlotDescriptor::SlotDescriptor(
    const TSlotDescriptor& tdesc, const TupleDescriptor* parent,
    const TupleDescriptor* collection_item_descriptor)
  : id_(tdesc.id),
    type_(ColumnType::FromThrift(tdesc.slotType)),
    parent_(parent),
    collection_item_descriptor_(collection_item_descriptor),
    col_path_(tdesc.materializedPath),
    tuple_offset_(tdesc.byteOffset),
    null_indicator_offset_(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit),
    slot_idx_(tdesc.slotIdx),
    slot_size_(type_.GetByteSize()),
    field_idx_(-1),
    is_materialized_(tdesc.isMaterialized),
    is_null_fn_(NULL),
    set_not_null_fn_(NULL),
    set_null_fn_(NULL) {
  DCHECK_NE(type_.type, TYPE_STRUCT);
  DCHECK(parent_ != NULL) << tdesc.parent;
  if (type_.IsCollectionType()) {
    DCHECK(tdesc.__isset.itemTupleId);
    DCHECK(collection_item_descriptor_ != NULL) << tdesc.itemTupleId;
  } else {
    DCHECK(!tdesc.__isset.itemTupleId);
    DCHECK(collection_item_descriptor == NULL);
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
  if (collection_item_descriptor_ != NULL) {
    out << " collection_item_tuple_id=" << collection_item_descriptor_->id();
  }
  out << " offset=" << tuple_offset_ << " null=" << null_indicator_offset_.DebugString()
      << " slot_idx=" << slot_idx_ << " field_idx=" << field_idx_
      << ")";
  return out.str();
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
    num_clustering_cols_(tdesc.numClusteringCols) {
  for (int i = 0; i < tdesc.columnDescriptors.size(); ++i) {
    col_descs_.push_back(ColumnDescriptor(tdesc.columnDescriptors[i]));
  }
}

string TableDescriptor::DebugString() const {
  vector<string> cols;
  BOOST_FOREACH(const ColumnDescriptor& col_desc, col_descs_) {
    cols.push_back(col_desc.DebugString());
  }
  stringstream out;
  out << "#cols=" << num_cols() << " #clustering_cols=" << num_clustering_cols_;
  out << " cols=[";
  out << join(cols, ", ");
  out << "]";
  return out.str();
}

HdfsPartitionDescriptor::HdfsPartitionDescriptor(const THdfsPartition& thrift_partition,
    ObjectPool* pool)
  : line_delim_(thrift_partition.lineDelim),
    field_delim_(thrift_partition.fieldDelim),
    collection_delim_(thrift_partition.collectionDelim),
    escape_char_(thrift_partition.escapeChar),
    block_size_(thrift_partition.blockSize),
    location_(thrift_partition.location),
    id_(thrift_partition.id),
    exprs_prepared_(false),
    exprs_opened_(false),
    exprs_closed_(false),
    file_format_(thrift_partition.fileFormat),
    object_pool_(pool) {

  for (int i = 0; i < thrift_partition.partitionKeyExprs.size(); ++i) {
    ExprContext* ctx;
    // TODO: Move to dedicated Init method and treat Status return correctly
    Status status = Expr::CreateExprTree(object_pool_,
        thrift_partition.partitionKeyExprs[i], &ctx);
    DCHECK(status.ok());
    partition_key_value_ctxs_.push_back(ctx);
  }
}

Status HdfsPartitionDescriptor::PrepareExprs(RuntimeState* state) {
  if (!exprs_prepared_) {
    // TODO: RowDescriptor should arguably be optional in Prepare for known literals
    exprs_prepared_ = true;
    // Partition exprs are not used in the codegen case.  Don't codegen them.
    RETURN_IF_ERROR(Expr::Prepare(partition_key_value_ctxs_, state, RowDescriptor(),
                                  state->instance_mem_tracker()));
  }
  return Status::OK();
}

Status HdfsPartitionDescriptor::OpenExprs(RuntimeState* state) {
  if (exprs_opened_) return Status::OK();
  exprs_opened_ = true;
  return Expr::Open(partition_key_value_ctxs_, state);
}

void HdfsPartitionDescriptor::CloseExprs(RuntimeState* state) {
  if (exprs_closed_ || !exprs_prepared_) return;
  exprs_closed_ = true;
  Expr::Close(partition_key_value_ctxs_, state);
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

HdfsTableDescriptor::HdfsTableDescriptor(const TTableDescriptor& tdesc,
    ObjectPool* pool)
  : TableDescriptor(tdesc),
    hdfs_base_dir_(tdesc.hdfsTable.hdfsBaseDir),
    null_partition_key_value_(tdesc.hdfsTable.nullPartitionKeyValue),
    null_column_value_(tdesc.hdfsTable.nullColumnValue),
    object_pool_(pool) {
  map<int64_t, THdfsPartition>::const_iterator it;
  for (it = tdesc.hdfsTable.partitions.begin(); it != tdesc.hdfsTable.partitions.end();
       ++it) {
    HdfsPartitionDescriptor* partition = new HdfsPartitionDescriptor(it->second, pool);
    object_pool_->Add(partition);
    partition_descriptors_[it->first] = partition;
  }
  avro_schema_ = tdesc.hdfsTable.__isset.avroSchema ? tdesc.hdfsTable.avroSchema : "";
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

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc)
  : id_(tdesc.id),
    table_desc_(NULL),
    byte_size_(tdesc.byteSize),
    num_null_bytes_(tdesc.numNullBytes),
    num_materialized_slots_(0),
    slots_(),
    has_varlen_slots_(false),
    tuple_path_(tdesc.tuplePath),
    llvm_struct_(NULL) {
}

void TupleDescriptor::AddSlot(SlotDescriptor* slot) {
  slots_.push_back(slot);
  if (slot->is_materialized()) {
    ++num_materialized_slots_;
    if (slot->type().IsVarLenStringType()) {
      string_slots_.push_back(slot);
      has_varlen_slots_ = true;
    }
    if (slot->type().IsCollectionType()) {
      collection_slots_.push_back(slot);
      has_varlen_slots_ = true;
    }
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

RowDescriptor::RowDescriptor(const DescriptorTbl& desc_tbl,
                             const vector<TTupleId>& row_tuples,
                             const vector<bool>& nullable_tuples)
  : tuple_idx_nullable_map_(nullable_tuples) {
  DCHECK_EQ(nullable_tuples.size(), row_tuples.size());
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

RowDescriptor::RowDescriptor(const vector<TupleDescriptor*>& tuple_descs,
                             const vector<bool>& nullable_tuples)
  : tuple_desc_map_(tuple_descs),
    tuple_idx_nullable_map_(nullable_tuples) {
  DCHECK_EQ(nullable_tuples.size(), tuple_descs.size());
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

void RowDescriptor::ToThrift(vector<TTupleId>* row_tuple_ids) {
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
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    // pointer comparison okay, descriptors are unique
    if (tuple_desc_map_[i] != other_desc.tuple_desc_map_[i]) return false;
  }
  return true;
}

string RowDescriptor::DebugString() const {
  stringstream ss;
  for (int i = 0; i < tuple_desc_map_.size(); ++i) {
    ss << tuple_desc_map_[i]->DebugString() << endl;
  }
  return ss.str();
}

Status DescriptorTbl::Create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                             DescriptorTbl** tbl) {
  *tbl = pool->Add(new DescriptorTbl());
  // deserialize table descriptors first, they are being referenced by tuple descriptors
  for (size_t i = 0; i < thrift_tbl.tableDescriptors.size(); ++i) {
    const TTableDescriptor& tdesc = thrift_tbl.tableDescriptors[i];
    TableDescriptor* desc = NULL;
    switch (tdesc.tableType) {
      case TTableType::HDFS_TABLE:
        desc = pool->Add(new HdfsTableDescriptor(tdesc, pool));
        break;
      case TTableType::HBASE_TABLE:
        desc = pool->Add(new HBaseTableDescriptor(tdesc));
        break;
      case TTableType::DATA_SOURCE_TABLE:
        desc = pool->Add(new DataSourceTableDescriptor(tdesc));
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
    // Tuple descriptors are already populated in tbl
    TupleDescriptor* parent = (*tbl)->GetTupleDescriptor(tdesc.parent);
    TupleDescriptor* collection_item_descriptor = tdesc.__isset.itemTupleId ?
        (*tbl)->GetTupleDescriptor(tdesc.itemTupleId) : NULL;
    SlotDescriptor* slot_d = pool->Add(
        new SlotDescriptor(tdesc, parent, collection_item_descriptor));
    (*tbl)->slot_desc_map_[tdesc.id] = slot_d;

    // link to parent
    TupleDescriptorMap::iterator entry = (*tbl)->tuple_desc_map_.find(tdesc.parent);
    if (entry == (*tbl)->tuple_desc_map_.end()) {
      return Status("unknown tid in slot descriptor msg");
    }
    entry->second->AddSlot(slot_d);
  }
  return Status::OK();
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

// Generate function to check if a slot is null.  The resulting IR looks like:
// (in this case the tuple contains only a nullable double)
// define i1 @IsNull({ i8, double }* %tuple) {
// entry:
//   %null_byte_ptr = getelementptr inbounds { i8, double }* %tuple, i32 0, i32 0
//   %null_byte = load i8* %null_byte_ptr
//   %null_mask = and i8 %null_byte, 1
//   %is_null = icmp ne i8 %null_mask, 0
//   ret i1 %is_null
// }
Function* SlotDescriptor::CodegenIsNull(LlvmCodeGen* codegen, StructType* tuple) {
  if (is_null_fn_ != NULL) return is_null_fn_;
  PointerType* tuple_ptr_type = PointerType::get(tuple, 0);
  LlvmCodeGen::FnPrototype prototype(codegen, "IsNull", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_ptr_type));

  Value* mask = codegen->GetIntConstant(TYPE_TINYINT, null_indicator_offset_.bit_mask);
  Value* zero = codegen->GetIntConstant(TYPE_TINYINT, 0);
  int byte_offset = null_indicator_offset_.byte_offset;

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* tuple_ptr;
  Function* fn = prototype.GeneratePrototype(&builder, &tuple_ptr);

  Value* null_byte_ptr = builder.CreateStructGEP(tuple_ptr, byte_offset, "null_byte_ptr");
  Value* null_byte = builder.CreateLoad(null_byte_ptr, "null_byte");
  Value* null_mask = builder.CreateAnd(null_byte, mask, "null_mask");
  Value* is_null = builder.CreateICmpNE(null_mask, zero, "is_null");
  builder.CreateRet(is_null);

  return is_null_fn_ = codegen->FinalizeFunction(fn);
}

// Generate function to set a slot to be null or not-null.  The resulting IR
// for SetNotNull looks like:
// (in this case the tuple contains only a nullable double)
// define void @SetNotNull({ i8, double }* %tuple) {
// entry:
//   %null_byte_ptr = getelementptr inbounds { i8, double }* %tuple, i32 0, i32 0
//   %null_byte = load i8* %null_byte_ptr
//   %0 = and i8 %null_byte, -2
//   store i8 %0, i8* %null_byte_ptr
//   ret void
// }
Function* SlotDescriptor::CodegenUpdateNull(LlvmCodeGen* codegen,
    StructType* tuple, bool set_null) {
  if (set_null && set_null_fn_ != NULL) return set_null_fn_;
  if (!set_null && set_not_null_fn_ != NULL) return set_not_null_fn_;

  PointerType* tuple_ptr_type = PointerType::get(tuple, 0);
  LlvmCodeGen::FnPrototype prototype(codegen, (set_null) ? "SetNull" :"SetNotNull",
      codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_ptr_type));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* tuple_ptr;
  Function* fn = prototype.GeneratePrototype(&builder, &tuple_ptr);

  Value* null_byte_ptr =
      builder.CreateStructGEP(
          tuple_ptr, null_indicator_offset_.byte_offset, "null_byte_ptr");
  Value* null_byte = builder.CreateLoad(null_byte_ptr, "null_byte");
  Value* result = NULL;

  if (set_null) {
    Value* null_set = codegen->GetIntConstant(
        TYPE_TINYINT, null_indicator_offset_.bit_mask);
    result = builder.CreateOr(null_byte, null_set);
  } else {
    Value* null_clear_val =
        codegen->GetIntConstant(TYPE_TINYINT, ~null_indicator_offset_.bit_mask);
    result = builder.CreateAnd(null_byte, null_clear_val);
  }

  builder.CreateStore(result, null_byte_ptr);
  builder.CreateRetVoid();

  fn = codegen->FinalizeFunction(fn);
  if (set_null) {
    set_null_fn_ = fn;
  } else {
    set_not_null_fn_ = fn;
  }
  return fn;
}

// The default llvm packing is identical to what we do in the FE.  Each field is aligned
// to begin on the size for that type.
// TODO: Understand llvm::SetTargetData which allows you to explicitly define the packing
// rules.
StructType* TupleDescriptor::GenerateLlvmStruct(LlvmCodeGen* codegen) {
  // If we already generated the llvm type, just return it.
  if (llvm_struct_ != NULL) return llvm_struct_;

  // For each null byte, add a byte to the struct
  vector<Type*> struct_fields;
  struct_fields.resize(num_null_bytes_ + num_materialized_slots_);
  for (int i = 0; i < num_null_bytes_; ++i) {
    struct_fields[i] = codegen->GetType(TYPE_TINYINT);
  }

  // Add the slot types to the struct description.
  for (int i = 0; i < slots().size(); ++i) {
    SlotDescriptor* slot_desc = slots()[i];
    if (slot_desc->type().type == TYPE_CHAR) return NULL;
    if (slot_desc->is_materialized()) {
      slot_desc->field_idx_ = slot_desc->slot_idx_ + num_null_bytes_;
      DCHECK_LT(slot_desc->field_idx(), struct_fields.size());
      struct_fields[slot_desc->field_idx()] = codegen->GetType(slot_desc->type());
    }
  }

  // Construct the struct type.
  StructType* tuple_struct = StructType::get(codegen->context(),
      ArrayRef<Type*>(struct_fields));

  // Verify the alignment is correct.  It is essential that the layout matches
  // identically.  If the layout does not match, return NULL indicating the
  // struct could not be codegen'd.  This will trigger codegen for anything using
  // the tuple to be disabled.
  const DataLayout* data_layout = codegen->execution_engine()->getDataLayout();
  const StructLayout* layout = data_layout->getStructLayout(tuple_struct);
  if (layout->getSizeInBytes() != byte_size()) {
    DCHECK_EQ(layout->getSizeInBytes(), byte_size());
    return NULL;
  }
  for (int i = 0; i < slots().size(); ++i) {
    SlotDescriptor* slot_desc = slots()[i];
    if (slot_desc->is_materialized()) {
      int field_idx = slot_desc->field_idx();
      // Verify that the byte offset in the llvm struct matches the tuple offset
      // computed in the FE
      if (layout->getElementOffset(field_idx) != slot_desc->tuple_offset()) {
        DCHECK_EQ(layout->getElementOffset(field_idx), slot_desc->tuple_offset());
        return NULL;
      }
    }
  }
  llvm_struct_ = tuple_struct;
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
