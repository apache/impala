// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/descriptors.h"

#include <boost/algorithm/string/join.hpp>
#include <glog/logging.h>
#include <ios>
#include <sstream>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/Target/TargetData.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exprs/expr.h"

using namespace llvm;
using namespace std;
using namespace boost::algorithm;

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
    slot_idx_(tdesc.slotIdx),
    field_idx_(-1),
    is_materialized_(tdesc.isMaterialized),
    is_null_fn_(NULL),
    set_not_null_fn_(NULL),
    set_null_fn_(NULL) {
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

HdfsPartitionDescriptor::HdfsPartitionDescriptor(const THdfsPartition& thrift_partition,
    ObjectPool* pool)
  : line_delim_(thrift_partition.lineDelim),
    field_delim_(thrift_partition.fieldDelim),
    collection_delim_(thrift_partition.collectionDelim),
    escape_char_(thrift_partition.escapeChar),
    exprs_prepared_(false),
    file_format_(thrift_partition.fileFormat),
    object_pool_(pool)  {

  for (int i = 0; i < thrift_partition.partitionKeyExprs.size(); ++i) {
    Expr* expr;
    // TODO: Move to dedicated Init method and treat Status return correctly
    Status status = Expr::CreateExprTree(object_pool_,
        thrift_partition.partitionKeyExprs[i], &expr);
    DCHECK(status.ok());
    partition_key_values_.push_back(expr);
  }
}

Status HdfsPartitionDescriptor::PrepareExprs(RuntimeState* state) {
  if (exprs_prepared_ == false) {
    // TODO: RowDescriptor should arguably be optional in Prepare for known literals
    exprs_prepared_ = true;
    RETURN_IF_ERROR(Expr::Prepare(partition_key_values_, state, RowDescriptor()));
  }
  return Status::OK;
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

HdfsTableDescriptor::HdfsTableDescriptor(const TTableDescriptor& tdesc,
    ObjectPool* pool)
  : TableDescriptor(tdesc),
    hdfs_base_dir_(tdesc.hdfsTable.hdfsBaseDir),
    partition_key_names_(tdesc.hdfsTable.partitionKeyNames),
    null_partition_key_value_(tdesc.hdfsTable.nullPartitionKeyValue),
    object_pool_(pool) {
  map<int64_t, THdfsPartition>::const_iterator it;
  for (it = tdesc.hdfsTable.partitions.begin(); it != tdesc.hdfsTable.partitions.end();
       ++it) {
    HdfsPartitionDescriptor* partition = new HdfsPartitionDescriptor(it->second, pool);
    object_pool_->Add(partition);
    partition_descriptors_[it->first] =  partition;
  }

}

string HdfsTableDescriptor::DebugString() const {
  stringstream out;
  out << "HdfsTable(" << TableDescriptor::DebugString()
      << " hdfs_base_dir='" << hdfs_base_dir_ << "'"
      << " partition_key_names=[";

  out << join(partition_key_names_, ":");

  out << "]";
  out << " partitions=[";
  vector<string> partition_strings;
  map<int64_t, HdfsPartitionDescriptor*>::const_iterator it;
  for (it = partition_descriptors_.begin(); it != partition_descriptors_.end(); ++it) {
    stringstream s;
    s << " (id: " << it->first << ", partition: " << it->second->DebugString() << ")";
    partition_strings.push_back(s.str());
  }
  out << join(partition_strings, ",") << "]";

  out << "null_partition_key_value='" << null_partition_key_value_ << "'";
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
    num_null_bytes_(tdesc.numNullBytes),
    num_materialized_slots_(0),
    slots_(),
    llvm_struct_(NULL) {
}

void TupleDescriptor::AddSlot(SlotDescriptor* slot) {
  slots_.push_back(slot);
  if (slot->type() == TYPE_STRING && slot->is_materialized()) {
    string_slots_.push_back(slot);
  }
  if (slot->is_materialized()) ++num_materialized_slots_;
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
      case TTableType::HDFS_TABLE:
        desc = pool->Add(new HdfsTableDescriptor(tdesc, pool));
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
  const TargetData* target_data = codegen->execution_engine()->getTargetData();
  const StructLayout* layout = target_data->getStructLayout(tuple_struct);
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
