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

#include <cstdint>
#include <iosfwd>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "codegen/codegen-anyval-read-write-info.h"
#include "codegen/impala-ir.h"
#include "common/global-types.h"
#include "common/status.h"
#include "runtime/types.h"

#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/Types_types.h"

namespace llvm {
  class Constant;
  class Function;
  class StructType;
  class Type;
  class Value;
};

namespace impala {

class CodegenAnyVal;
class LlvmBuilder;
class LlvmCodeGen;
class ObjectPool;
class ScalarExprEvaluator;
class TColumnDescriptor;
class TDescriptorTable;
class TDescriptorTableSerialized;
class TExpr;
class TSlotDescriptor;
class TTableDescriptor;
class TTupleDescriptor;

/// A path into a table schema (e.g. a vector of ColumnTypes) pointing to a particular
/// column/field. The i-th element of the path is the ordinal position of the column/field
/// of the schema at level i. For example, the path [0] would be the first column of the
/// table, and path [1,0] would be the first field in the second column of the table.
///
/// Arrays are represented as having two fields. The first is the item element in the
/// schema. The second is an artifical position element, which does not actually exist in
/// the table schema. For example, if path [0] is an array, path [0,0] would refer to the
/// array's item, and path [0,1] would refer to the position element, which is the item
/// count of the array.
///
/// Maps are also represented as having two fields, the key and the value elements (maps
/// do not have a position element).
typedef std::vector<int> SchemaPath;
class SchemaPathConstants {
 public:
  static const int ARRAY_ITEM = 0;
  static const int ARRAY_POS = 1;
  static const int MAP_KEY = 0;
  static const int MAP_VALUE = 1;
 private:
  DISALLOW_COPY_AND_ASSIGN(SchemaPathConstants);
};

/// Location information for null indicator bit for particular slot.
/// For non-nullable slots, the byte_offset will be 0 and the bit_mask will be 0.
/// This allows us to do the NullIndicatorOffset operations (tuple + byte_offset &/|
/// bit_mask) regardless of whether the slot is nullable or not.
/// This is more efficient than branching to check if the slot is non-nullable.
///
/// ToIR() generates a constant version of this struct in LLVM IR. If the struct
/// layout is updated, then ToIR() must also be updated.
struct NullIndicatorOffset {
  int byte_offset;
  uint8_t bit_mask;  /// to extract null indicator

  NullIndicatorOffset(int byte_offset = 0, int bit_offset = -1)
    : byte_offset(byte_offset),
      bit_mask(bit_offset == -1 ? 0 : 1 << bit_offset) {
  }

  bool Equals(const NullIndicatorOffset& o) const {
    return this->byte_offset == o.byte_offset && this->bit_mask == o.bit_mask;
  }

  std::string DebugString() const;

  // Generates an LLVM IR constant of this offset. Needs to be updated if the layout of
  // this struct changes.
  llvm::Constant* ToIR(LlvmCodeGen* codegen) const;

  static const char* LLVM_CLASS_NAME;
};

std::ostream& operator<<(std::ostream& os, const NullIndicatorOffset& null_indicator);

class TupleDescriptor;

class SlotDescriptor {
 public:
  SlotId id() const { return id_; }
  const ColumnType& type() const { return type_; }
  const TupleDescriptor* parent() const { return parent_; }
  const TupleDescriptor* children_tuple_descriptor() const {
    return children_tuple_descriptor_;
  }
  /// Returns the column index of this slot, including partition keys.
  /// (e.g., col_pos - num_partition_keys = the table column this slot corresponds to)
  /// TODO: This function should eventually be replaced by col_path(). It is currently
  /// convenient for table formats for which we only support flat data.
  int col_pos() const { return col_path_[0]; }
  const SchemaPath& col_path() const { return col_path_; }

  /// Returns the field index in the generated llvm struct for this slot's tuple.
  /// The 'llvm_field_idx' is the index of the slot in the llvm codegen'd tuple struct.
  /// This takes into account any padding bytes.
  int llvm_field_idx() const { return slot_idx_; }

  int tuple_offset() const { return tuple_offset_; }
  const NullIndicatorOffset& null_indicator_offset() const {
    return null_indicator_offset_;
  }
  bool is_nullable() const { return null_indicator_offset_.bit_mask != 0; }
  int slot_size() const { return slot_size_; }

  TVirtualColumnType::type virtual_column_type() const { return virtual_column_type_; }
  bool IsVirtual() const { return virtual_column_type_ != TVirtualColumnType::NONE; }

  /// Comparison function for ordering slot descriptors by their col_path_.
  /// Returns true if 'a' comes before 'b'.
  /// Orders the paths as in a depth-first traversal of the schema tree, as follows:
  /// - for each level i in min(a.path.size, b.path.size),
  ///   order the paths ascending by col_path_[i]
  /// - in case of ties, the path with smaller size comes first
  static bool ColPathLessThan(const SlotDescriptor* a, const SlotDescriptor* b);

  std::string DebugString() const;

  /// Return true if the physical layout of this descriptor matches the physical layout
  /// of other_desc, but not necessarily ids.
  bool LayoutEquals(const SlotDescriptor& other_desc) const;

  /// Load "any_val"'s value from 'raw_val_ptr', which must be a pointer to the matching
  /// native type, e.g. a StringValue or TimestampValue slot in a tuple.
  static void CodegenLoadAnyVal(CodegenAnyVal* any_val, llvm::Value* raw_val_ptr);

  /// Generate LLVM code at the insert position of 'builder' that returns a boolean value
  /// represented as a LLVM i1 indicating whether this slot is null in 'tuple'.
  llvm::Value* CodegenIsNull(
      LlvmCodeGen* codegen, LlvmBuilder* builder, llvm::Value* tuple) const;

  /// Generate LLVM code at the insert position of 'builder' that returns a boolean value
  /// represented as a LLVM i1 with value of the NULL bit at 'null_indicator_offset' in
  /// 'tuple'.
  static llvm::Value* CodegenIsNull(LlvmCodeGen* codegen, LlvmBuilder* builder,
      const NullIndicatorOffset& null_indicator_offset, llvm::Value* tuple);

  /// Generate LLVM code at the insert position of 'builder' to set this slot's
  /// NULL bit in the given 'tuple' to the value 'is_null'. 'tuple' is a pointer
  /// to the tuple, and 'is_null' is an boolean value represented as a LLVM i1.
  void CodegenSetNullIndicator(LlvmCodeGen* codegen, LlvmBuilder* builder,
      llvm::Value* tuple, llvm::Value* is_null) const;

  void CodegenWriteToSlot(const CodegenAnyValReadWriteInfo& read_write_info,
      llvm::Value* tuple_llvm_struct_ptr,
      llvm::Value* pool_val, llvm::BasicBlock* insert_before = nullptr) const;

  /// Stores this 'any_val' into a native slot, e.g. a StringValue or TimestampValue.
  /// This should only be used if 'any_val' is not null.
  ///
  /// Not valid to call for FIXED_UDA_INTERMEDIATE: in that case the StringVal must be
  /// set up to point directly to the underlying slot, e.g. by LoadFromNativePtr().
  ///
  /// Not valid to call for structs.
  ///
  /// If 'pool_val' is non-NULL, var-len data will be copied into 'pool_val'.
  /// 'pool_val' has to be of type MemPool*.
  ///
  /// 'slot_desc' is needed when 'pool_val' is non-NULL and the value is a collection. In
  /// this case the collection is copied and the slot desc is needed to calculate its byte
  /// size.
  ///
  /// If 'insert_before' contains a non-NULL pointer to a basic block, any newly created
  /// blocks will be inserted before that.
  static void CodegenStoreNonNullAnyVal(CodegenAnyVal& any_val,
      llvm::Value* raw_val_ptr, llvm::Value* pool_val = nullptr,
      const SlotDescriptor* slot_desc = nullptr,
      const NonWritableBasicBlock& insert_before = NonWritableBasicBlock(nullptr));

  /// Like the above, but takes a 'CodegenAnyValReadWriteInfo' instead of a
  /// 'CodegenAnyVal'.
  static void CodegenStoreNonNullAnyVal(const CodegenAnyValReadWriteInfo& read_write_info,
      llvm::Value* raw_val_ptr, llvm::Value* pool_val = nullptr,
      const SlotDescriptor* slot_desc = nullptr,
      const NonWritableBasicBlock& insert_before = NonWritableBasicBlock(nullptr));

  /// Like 'CodegenStoreNonNullAnyVal' but stores the value into a new alloca()
  /// allocation. Returns a pointer to the stored value.
  static llvm::Value* CodegenStoreNonNullAnyValToNewAlloca(
      CodegenAnyVal& any_val, llvm::Value* pool_val = nullptr);

  /// Like the above, but takes a 'CodegenAnyValReadWriteInfo' instead of a
  /// 'CodegenAnyVal'.
  static llvm::Value* CodegenStoreNonNullAnyValToNewAlloca(
      const CodegenAnyValReadWriteInfo& read_write_info, llvm::Value* pool_val = nullptr);

  /// Returns true if this slot is a child of a struct slot.
  inline bool IsChildOfStruct() const;
 private:
  friend class DescriptorTbl;
  friend class TupleDescriptor;

  const SlotId id_;
  const ColumnType type_;
  const TupleDescriptor* parent_;
  /// Non-NULL only for complex type slots
  const TupleDescriptor* children_tuple_descriptor_;
  // TODO for 2.3: rename to materialized_path_
  const SchemaPath col_path_;
  const int tuple_offset_;
  const NullIndicatorOffset null_indicator_offset_;

  /// the idx of the slot in the tuple descriptor (0-based).
  /// this is provided by the FE
  const int slot_idx_;

  /// the byte size of this slot.
  const int slot_size_;

  const TVirtualColumnType::type virtual_column_type_;

  /// 'children_tuple_descriptor' should be non-NULL iff this is a complex type slot.
  SlotDescriptor(const TSlotDescriptor& tdesc, const TupleDescriptor* parent,
      const TupleDescriptor* children_tuple_descriptor);

  /// Generate LLVM code at the insert position of 'builder' to get the i8 value of
  /// the byte containing 'null_indicator_offset' in 'tuple'. If 'null_byte_ptr' is
  /// non-NULL, sets that to a pointer to the null byte.
  static llvm::Value* CodegenGetNullByte(LlvmCodeGen* codegen, LlvmBuilder* builder,
      const NullIndicatorOffset& null_indicator_offset, llvm::Value* tuple,
      llvm::Value** null_byte_ptr);

  void CodegenWriteToSlotHelper(const CodegenAnyValReadWriteInfo& read_write_info,
      llvm::Value* main_tuple_llvm_struct_ptr, llvm::Value* tuple_llvm_struct_ptr,
      llvm::Value* pool_val, const NonWritableBasicBlock& insert_before) const;

  /// Stores a struct value into a native slot. This should only be used if this struct is
  /// not null.
  ///
  /// 'main_tuple_ptr' should be a pointer to the beginning of the whole main tuple, while
  /// 'struct_slot_ptr' should be the slot where the struct should be stored. The first
  /// one is needed because the offsets of the struct's children are calculated from the
  /// beginning of the main tuple.
  void CodegenStoreStructToNativePtr(const CodegenAnyValReadWriteInfo& read_write_info,
      llvm::Value* main_tuple_ptr, llvm::Value* struct_slot_ptr, llvm::Value* pool_val,
      const NonWritableBasicBlock& insert_before) const;

  // Sets the null indicator bit to 0 - recursively for structs.
  void CodegenSetToNull(const CodegenAnyValReadWriteInfo& read_write_info,
      llvm::Value* tuple) const;

  /// Codegens writing a string or a collection to the address pointed to by 'slot_ptr'.
  /// If 'pool_val' is non-NULL, the data will be deep-copied into 'pool_val'. 'pool_val'
  /// has to be of type MemPool*. 'slot_desc' is needed when 'pool_val' is non-NULL and
  /// the value is a collection. In this case the collection is copied and the slot desc
  /// is needed to calculate its byte size.
  static void CodegenWriteStringOrCollectionToSlot(
      const CodegenAnyValReadWriteInfo& read_write_info,
      llvm::Value* slot_ptr, llvm::Value* pool_val, const SlotDescriptor* slot_desc,
      const NonWritableBasicBlock& insert_before);
  static void CodegenWriteCollectionToSlot(
      const CodegenAnyValReadWriteInfo& read_write_info,
      llvm::Value* slot_ptr, llvm::Value* pool_val, const SlotDescriptor* slot_desc,
      const NonWritableBasicBlock& insert_before);
  static void CodegenWriteStringToSlot(
      const CodegenAnyValReadWriteInfo& read_write_info,
      llvm::Value* slot_ptr, llvm::Value* pool_val, const SlotDescriptor* slot_desc);

  void CodegenWriteCollectionItemsToSlot(LlvmCodeGen* codegen, LlvmBuilder* builder,
      llvm::Value* collection_value, llvm::Value* num_tuples, llvm::Value* pool_val,
      const NonWritableBasicBlock& insert_before) const;

  // The below functions are helpers of 'CodegenWriteStringOrCollectionToSlot()'.
  void CodegenWriteCollectionItemLoopBody(LlvmCodeGen* codegen, LlvmBuilder* builder,
      llvm::Value* collection_value_ptr, llvm::Value* num_tuples,
      llvm::Value* child_index, llvm::Function* fn,
      const NonWritableBasicBlock& insert_before, llvm::Value* pool_val) const;

  // 'master_tuple' differs from 'children_tuple' if this is inside a struct -
  // 'master_tuple' is the first non-struct parent tuple. Applies to the below functions
  // too.
  void CodegenWriteCollectionIterateOverChildren(LlvmCodeGen* codegen,
      LlvmBuilder* builder, llvm::Value* master_tuple, llvm::Value* children_tuple,
      llvm::Function* fn, const NonWritableBasicBlock& insert_before,
      llvm::Value* pool_val) const;

  void CodegenWriteCollectionStructChild(LlvmCodeGen* codegen, LlvmBuilder* builder,
      llvm::Value* master_tuple, llvm::Value* tuple, llvm::Function* fn,
      const NonWritableBasicBlock& insert_before, llvm::Value* pool_val) const;

  void CodegenWriteCollectionVarlenChild(LlvmCodeGen* codegen, LlvmBuilder* builder,
      llvm::Value* master_tuple, llvm::Value* child_tuple, llvm::Function* fn,
      const NonWritableBasicBlock& insert_before, llvm::Value* pool_val) const;

  static llvm::Value* CodegenToTimestampValue(
      const CodegenAnyValReadWriteInfo& read_write_info);
};

class ColumnDescriptor {
 public:
  ColumnDescriptor(const TColumnDescriptor& tdesc);
  const std::string& name() const { return name_; }
  const ColumnType& type() const { return type_; }

  int field_id() const { return field_id_; }
  int field_map_key_id() const { return field_map_key_id_; }
  int field_map_value_id() const { return field_map_value_id_; }

  std::string DebugString() const;

 private:
  std::string name_;
  ColumnType type_;

  int field_id_ = -1;
  int field_map_key_id_ = -1;
  int field_map_value_id_ = -1;
};

/// Base class for table descriptors.
class TableDescriptor {
 public:
  TableDescriptor(const TTableDescriptor& tdesc);
  virtual ~TableDescriptor() {}
  int num_cols() const { return col_descs_.size(); }
  int num_clustering_cols() const { return num_clustering_cols_; }
  virtual std::string DebugString() const;

  /// The first num_clustering_cols_ columns by position are clustering
  /// columns.
  bool IsClusteringCol(const SlotDescriptor* slot_desc) const {
    return slot_desc->col_path().size() == 1 &&
        slot_desc->col_path()[0] < num_clustering_cols_ &&
        !slot_desc->IsVirtual();
  }

  /// Get ColumnDesc based on SchemaPath.
  const ColumnDescriptor& GetColumnDesc(const SlotDescriptor* slot_desc) const {
    DCHECK_EQ(slot_desc->col_path().size(), 1); // Not supported for nested types.
    DCHECK_LT(slot_desc->col_path()[0], col_descs_.size());
    return col_descs_[slot_desc->col_path()[0]];
  }

  const std::string& name() const { return name_; }
  const std::string& database() const { return database_; }
  int id() const { return id_; }
  TTableType::type type() const { return type_; }
  const std::vector<ColumnDescriptor>& col_descs() const { return col_descs_; }

  /// Returns "<database>.<name>"
  std::string fully_qualified_name() const;

 protected:
  std::string name_;
  std::string database_;
  TableId id_;
  TTableType::type type_;
  int num_clustering_cols_;
  std::vector<ColumnDescriptor> col_descs_;
};

/// Metadata for a single partition inside an Hdfs table.
class HdfsPartitionDescriptor {
 public:
  HdfsPartitionDescriptor(const THdfsTable& thrift_table,
      const THdfsPartition& thrift_partition);

  char line_delim() const { return line_delim_; }
  char field_delim() const { return field_delim_; }
  char collection_delim() const { return collection_delim_; }
  char escape_char() const { return escape_char_; }
  THdfsFileFormat::type file_format() const { return file_format_; }
  int block_size() const { return block_size_; }
  const std::string& location() const { return location_; }
  int64_t id() const { return id_; }
  std::string DebugString() const;

  /// It is safe to call the returned expr evaluators concurrently from multiple
  /// threads because all exprs are literals, after the descriptor table has been
  /// opened.
  const std::vector<ScalarExprEvaluator*>& partition_key_value_evals() const {
    return partition_key_value_evals_;
  }

 private:
  friend class DescriptorTbl;

  char line_delim_;
  char field_delim_;
  char collection_delim_;
  char escape_char_;
  int block_size_;
  // TODO: use the same representation as the Catalog does, in which common prefixes are
  // stripped.
  std::string location_;
  int64_t id_;

  /// List of literal (and therefore constant) expressions for each partition key. Their
  /// order corresponds to the first num_clustering_cols of the parent table.
  /// The Prepare()/Open()/Close() cycle is controlled by the containing descriptor table
  /// because the same partition descriptor may be used by multiple exec nodes with
  /// different lifetimes.
  const std::vector<TExpr>& thrift_partition_key_exprs_;

  /// These evaluators are safe to be shared by all fragment instances as all expressions
  /// are Literals.
  /// TODO: replace this with vector<Literal> instead.
  std::vector<ScalarExprEvaluator*> partition_key_value_evals_;

  /// The format (e.g. text, sequence file etc.) of data in the files in this partition
  THdfsFileFormat::type file_format_;
};

class HdfsTableDescriptor : public TableDescriptor {
 public:
  HdfsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
  const std::string& hdfs_base_dir() const { return hdfs_base_dir_; }
  const std::string& null_partition_key_value() const {
    return null_partition_key_value_;
  }
  const std::string& null_column_value() const { return null_column_value_; }
  const std::string& avro_schema() const { return avro_schema_; }

  typedef std::map<int64_t, HdfsPartitionDescriptor*> PartitionIdToDescriptorMap;

  HdfsPartitionDescriptor* GetPartition(int64_t partition_id) const {
    PartitionIdToDescriptorMap::const_iterator it =
        partition_descriptors_.find(partition_id);
    if (it == partition_descriptors_.end()) return NULL;
    return it->second;
  }

  /// Release resources by closing partition descriptors.
  void ReleaseResources();

  const PartitionIdToDescriptorMap& partition_descriptors() const {
    return partition_descriptors_;
  }

  const HdfsPartitionDescriptor* prototype_partition_descriptor() const {
    return prototype_partition_descriptor_;
  }

  bool IsTableFullAcid() const { return is_full_acid_; }
  const TValidWriteIdList& ValidWriteIdList() const { return valid_write_id_list_; }

  bool IsIcebergTable() const { return is_iceberg_; }
  const std::string& IcebergTableLocation() const { return iceberg_table_location_; }
  const std::vector<TIcebergPartitionSpec>& IcebergPartitionSpecs() const {
    return iceberg_partition_specs_;
  }
  const std::vector<TIcebergPartitionField>& IcebergNonVoidPartitionFields() const {
    return iceberg_non_void_partition_fields_;
  }
  const TCompressionCodec& IcebergParquetCompressionCodec() const {
    return iceberg_parquet_compression_codec_;
  }
  int64_t IcebergParquetRowGroupSize() const {
    return iceberg_parquet_row_group_size_;
  }

  int64_t IcebergParquetPlainPageSize() const {
    return iceberg_parquet_plain_page_size_;
  }
  int64_t IcebergParquetDictPageSize() const {
    return iceberg_parquet_dict_page_size_;
  }

  int32_t IcebergSpecId() const {
    return iceberg_spec_id_;
  }

  virtual std::string DebugString() const;

 protected:
  std::string hdfs_base_dir_;
  std::string null_partition_key_value_;
  /// Special string to indicate NULL values in text-encoded columns.
  std::string null_column_value_;
  PartitionIdToDescriptorMap partition_descriptors_;
  HdfsPartitionDescriptor* prototype_partition_descriptor_;
  /// Set to the table's Avro schema if this is an Avro table, empty string otherwise
  std::string avro_schema_;
  bool is_full_acid_;
  TValidWriteIdList valid_write_id_list_;
  bool is_iceberg_ = false;
  std::string iceberg_table_location_;
  std::vector<TIcebergPartitionSpec> iceberg_partition_specs_;
  std::vector<TIcebergPartitionField> iceberg_non_void_partition_fields_;
  TCompressionCodec iceberg_parquet_compression_codec_;
  int64_t iceberg_parquet_row_group_size_;
  int64_t iceberg_parquet_plain_page_size_;
  int64_t iceberg_parquet_dict_page_size_;
  int32_t iceberg_spec_id_;
};

class HBaseTableDescriptor : public TableDescriptor {
 public:
  HBaseTableDescriptor(const TTableDescriptor& tdesc);
  virtual std::string DebugString() const;
  const std::string table_name() const { return table_name_; }

  struct HBaseColumnDescriptor {
    std::string family;
    std::string qualifier;
    bool binary_encoded;

    HBaseColumnDescriptor(const std::string& col_family, const std::string& col_qualifier,
        bool col_binary_encoded)
      : family(col_family),
        qualifier(col_qualifier),
        binary_encoded(col_binary_encoded){
    }
  };
  const std::vector<HBaseColumnDescriptor>& cols() const { return cols_; }

 protected:
  /// native name of hbase table
  std::string table_name_;

  /// List of family/qualifier pairs.
  std::vector<HBaseColumnDescriptor> cols_;
};

/// Descriptor for a DataSourceTable
class DataSourceTableDescriptor : public TableDescriptor {
 public:
  DataSourceTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) { }
  virtual std::string DebugString() const;
};

// Descriptor for a KuduTable
class KuduTableDescriptor : public TableDescriptor {
 public:
  explicit KuduTableDescriptor(const TTableDescriptor& tdesc);
  virtual std::string DebugString() const;
  const std::string table_name() const { return table_name_; }
  const std::vector<std::string>& key_columns() const { return key_columns_; }
  const std::vector<std::string>& kudu_master_addresses() const {
    return master_addresses_;
  }

 private:
  // native name of Kudu table
  std::string table_name_;
  std::vector<std::string> key_columns_;
  std::vector<std::string> master_addresses_;
};

// Descriptor for a SystemTable
class SystemTableDescriptor : public TableDescriptor {
 public:
  SystemTableDescriptor(const TTableDescriptor& tdesc);
  virtual std::string DebugString() const;

 private:
  TSystemTableName::type table_name_;
};

class TupleDescriptor {
 public:
  int byte_size() const { return byte_size_; }
  // num_null_bytes() and null_bytes_offset() are not inlined so they can be replaced with
  // constants during codegen.
  int IR_NO_INLINE num_null_bytes() const { return num_null_bytes_; }
  int IR_NO_INLINE null_bytes_offset() const { return null_bytes_offset_; }
  const std::vector<SlotDescriptor*>& slots() const { return slots_; }

  const std::vector<SlotDescriptor*>& string_slots() const {
    DCHECK(!isTupleOfStructSlot());
    return string_slots_;
  }

  const std::vector<SlotDescriptor*>& collection_slots() const {
    DCHECK(!isTupleOfStructSlot());
    return collection_slots_;
  }

  bool HasVarlenSlots() const {
    DCHECK(!isTupleOfStructSlot());
    return has_varlen_slots_;
  }

  const SchemaPath& tuple_path() const { return tuple_path_; }

  const TableDescriptor* table_desc() const { return table_desc_; }

  TupleId id() const { return id_; }
  std::string DebugString() const;

  bool isTupleOfStructSlot() const { return master_tuple_ != nullptr; }

  TupleDescriptor* getMasterTuple() const { return master_tuple_; }
  void setMasterTuple(TupleDescriptor* desc) { master_tuple_ = desc; }

  /// Return true if the physical layout of this descriptor matches that of other_desc,
  /// but not necessarily the id.
  bool LayoutEquals(const TupleDescriptor& other_desc) const;

  /// Creates and returns a typed struct description for llvm. The layout of the struct is
  /// computed by the FE which includes the order of the fields in the resulting struct.
  /// For example, the aggregation tuple for this query: select count(*), min(int_col_a)
  /// would map to:
  /// struct Tuple {
  ///   int8_t   null_byte;
  ///   int32_t  min_a;
  ///   int64_t  count_val;
  /// };
  llvm::StructType* GetLlvmStruct(LlvmCodeGen* codegen) const;

  static const char* LLVM_CLASS_NAME;

 protected:
  friend class DescriptorTbl;

  const TupleId id_;
  TableDescriptor* table_desc_ = nullptr;
  const int byte_size_;
  const int num_null_bytes_;
  const int null_bytes_offset_;

  /// Contains all slots. Slots are in the same order as the expressions that materialize
  /// them. See Tuple::MaterializeExprs().
  std::vector<SlotDescriptor*> slots_;

  /// Contains the materialized string slots of this TupleDescriptor and also those of its
  /// struct children (recursively). Not valid for item tuple descriptors of structs.
  std::vector<SlotDescriptor*> string_slots_;

  /// Contains the materialized map and array slots of this TupleDescriptor and also those
  /// of its struct children (recursively). Not valid for item tuple descriptors of
  /// structs.
  std::vector<SlotDescriptor*> collection_slots_;

  /// Provides a quick way to check if there are variable length slots in this tuple or
  /// its struct children (recursively), i.e. true if string_slots_ or collection_slots_
  /// has entries. Not valid for item tuple descriptors of structs.
  bool has_varlen_slots_;

  /// Absolute path into the table schema pointing to the collection whose fields are
  /// materialized into this tuple. Non-empty if this tuple belongs to a nested
  /// collection, empty otherwise.
  SchemaPath tuple_path_;

  /// If this tuple represents the children of a struct slot then the master tuple is
  /// defined as follows:
  ///  - starting from this tuple, go upwards in the slot-tuple tree as long as the parent
  ///    slot is a struct
  ///  - the tuple holding the last such struct slot, i.e. the slot whose immediate parent
  ///    (if it exists) is not a struct, is the master tuple for this tuple (and all other
  ///    tuples on the path).
  /// Therefore the master tuple is either the main tuple or the item tuple of a
  /// collection.
  ///
  /// For example:
  /// - Tuple0 (Main tuple)
  ///   - Slot0: INT
  ///   - Slot1: STRUCT
  ///     - Tuple1 (Holds the children of the struct in Slot1)
  ///       - Slot2: ARRAY
  ///         - Tuple2 (Holds the children of the array in Slot2)
  ///           - Slot3: STRUCT
  ///             - Tuple3 (Holds the children of the struct in Slot3)
  ///               - Slot4: STRUCT
  ///                 - Tuple4 (Holds the children of the struct in Slot4)
  ///                   - Slot5: INT
  ///                   - Slot6: STRING
  ///
  /// In the above example the master tuple for Tuple1 is Tuple0. For Tuple3 and Tuple4
  /// the master tuple is Tuple2 because it holds the struct in Slot3 which is not
  /// directly inside another struct. Tuple0 and Tuple2 do not have a master tuple because
  /// they do not hold the children of structs.
  ///
  /// The reason for this is that, in memory, the children of a struct do not have their
  /// own physical tuple but are placed in the tuple that conceptually holds their parent
  /// struct. This means that the whole of a pure struct tree (i.e. one that does not
  /// contain collections) of any depth will be placed in the same physical tuple, the
  /// master tuple. Slot offsets are calculated in terms of the master tuple, not the
  /// individual structs' virtual tuples.
  ///
  /// Children of a collection, on the other hand, do have their physical tuple that is
  /// distinct from the one that holds their parent collection. This is because the number
  /// of children a collection has is not known statically, in contrast to the number (and
  /// type) of the fields of structs.
  TupleDescriptor* master_tuple_ = nullptr;

  TupleDescriptor(const TTupleDescriptor& tdesc);
  void AddSlot(SlotDescriptor* slot);

  /// Returns slots in their physical order.
  std::vector<SlotDescriptor*> SlotsOrderedByIdx() const;

  std::pair<std::vector<llvm::Type*>, int> GetLlvmTypesAndOffset(LlvmCodeGen* codegen,
      int curr_struct_offset) const;
  llvm::StructType* CreateLlvmStructTypeFromFieldTypes(LlvmCodeGen* codegen,
      const std::vector<llvm::Type*>& field_types, int parent_slot_offset) const;
};

class DescriptorTbl {
 public:
  /// Creates an HdfsTableDescriptor (allocated in 'pool' and returned via 'desc') for
  /// table with id 'table_id' within serialized_thrift_tbl. DCHECKs if no such
  /// descriptor is present.
  static Status CreateHdfsTblDescriptor(
      const TDescriptorTableSerialized& serialized_thrift_tbl,
      TableId table_id, ObjectPool* pool, HdfsTableDescriptor** desc) WARN_UNUSED_RESULT;

  /// Creates a descriptor tbl within 'pool' from serialized_thrift_tbl and returns it
  /// via 'tbl'. Returns OK on success, otherwise error (in which case 'tbl' will be
  /// unset).
  static Status Create(ObjectPool* pool,
      const TDescriptorTableSerialized& serialized_thrift_tbl,
      DescriptorTbl** tbl) WARN_UNUSED_RESULT;

  /// Free memory allocated in Create().
  void ReleaseResources();

  TableDescriptor* GetTableDescriptor(TableId id) const;
  TupleDescriptor* GetTupleDescriptor(TupleId id) const;
  SlotDescriptor* GetSlotDescriptor(SlotId id) const;

  /// return all registered tuple descriptors
  void GetTupleDescs(std::vector<TupleDescriptor*>* descs) const;

  std::string DebugString() const;

  /// Converts a TDescriptorTableSerialized to a TDescriptorTable. Returns
  /// an error if deserialization fails.
  static Status DeserializeThrift(const TDescriptorTableSerialized& serial_tbl,
      TDescriptorTable* desc_tbl) WARN_UNUSED_RESULT;

 private:
  // The friend classes use CreateInternal().
  friend class DescriptorTblBuilder;
  friend class DataStreamTest;
  typedef std::unordered_map<TableId, TableDescriptor*> TableDescriptorMap;
  typedef std::unordered_map<TupleId, TupleDescriptor*> TupleDescriptorMap;
  typedef std::unordered_map<SlotId, SlotDescriptor*> SlotDescriptorMap;

  TableDescriptorMap tbl_desc_map_;
  TupleDescriptorMap tuple_desc_map_;
  SlotDescriptorMap slot_desc_map_;

  DescriptorTbl(): tbl_desc_map_(), tuple_desc_map_(), slot_desc_map_() {}

  static Status CreatePartKeyExprs(
      const HdfsTableDescriptor& hdfs_tbl, ObjectPool* pool) WARN_UNUSED_RESULT;

  /// Creates a TableDescriptor (allocated in 'pool', returned via 'desc')
  /// corresponding to tdesc. Returns error status on failure.
  static Status CreateTblDescriptorInternal(const TTableDescriptor& tdesc,
    ObjectPool* pool, TableDescriptor** desc) WARN_UNUSED_RESULT;

  /// Creates a descriptor tbl within 'pool' from thrift_tbl and returns it via 'tbl'.
  /// Returns OK on success, otherwise error (in which case 'tbl' will be unset).
  /// This is the same as Create(), except that it takes the deserialized thrift
  /// structure. This is useful for tests that produce their own thrift structures,
  /// as it avoids serialization and allows for incomplete thrift structures
  /// that cannot be serialized.
  static Status CreateInternal(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
      DescriptorTbl** tbl) WARN_UNUSED_RESULT;
};

/// Records positions of tuples within row produced by ExecNode. RowDescriptors are
/// typically owned by their ExecNode, and shared by reference across the plan tree.
///
/// TODO: this needs to differentiate between tuples contained in row
/// and tuples produced by ExecNode (parallel to PlanNode.rowTupleIds and
/// PlanNode.tupleIds); right now, we conflate the two (and distinguish based on
/// context; for instance, HdfsScanNode uses these tids to create row batches, ie, the
/// first case, whereas TopNNode uses these tids to copy output rows, ie, the second
/// case)
class RowDescriptor {
 public:
  RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples,
      const std::vector<bool>& nullable_tuples);

  /// standard copy c'tor, made explicit here
  RowDescriptor(const RowDescriptor& desc)
    : tuple_desc_map_(desc.tuple_desc_map_),
      tuple_idx_map_(desc.tuple_idx_map_),
      has_varlen_slots_(desc.has_varlen_slots_) {
  }

  /// c'tor for a row assembled from two rows
  RowDescriptor(const RowDescriptor& lhs_row_desc, const RowDescriptor& rhs_row_desc);

  RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable);

  /// dummy descriptor, needed for the JNI EvalPredicate() function
  RowDescriptor() {}

  /// Returns total size in bytes.
  /// TODO: also take avg string lengths into account, ie, change this
  /// to GetAvgRowSize()
  int GetRowSize() const;

  static const int INVALID_IDX = -1;

  /// Returns INVALID_IDX if id not part of this row.
  int GetTupleIdx(TupleId id) const;

  /// Return true if the Tuple of the given Tuple index is nullable.
  bool TupleIsNullable(int tuple_idx) const;

  /// Return true if any Tuple of the row is nullable.
  bool IsAnyTupleNullable() const;

  /// Return true if any Tuple has variable length slots.
  bool HasVarlenSlots() const { return has_varlen_slots_; }

  /// Return descriptors for all tuples in this row, in order of appearance.
  const std::vector<TupleDescriptor*>& tuple_descriptors() const {
    return tuple_desc_map_;
  }

  /// Populate row_tuple_ids with our ids.
  void ToThrift(std::vector<TTupleId>* row_tuple_ids) const;

  /// Return true if the tuple ids of this descriptor are a prefix
  /// of the tuple ids of other_desc.
  bool IsPrefixOf(const RowDescriptor& other_desc) const;

  /// Return true if the tuple ids of this descriptor match tuple ids of other desc.
  bool Equals(const RowDescriptor& other_desc) const;

  /// Return true if the tuples of this descriptor are a prefix of the tuples of
  /// other_desc. Tuples are compared by their physical layout and not by ids.
  bool LayoutIsPrefixOf(const RowDescriptor& other_desc) const;

  /// Return true if the physical layout of this descriptor matches the physical layout
  /// of other_desc, but not necessarily the ids.
  bool LayoutEquals(const RowDescriptor& other_desc) const;

  std::string DebugString() const;

 private:
  /// Initializes tupleIdxMap during c'tor using the tuple_desc_map_.
  void InitTupleIdxMap();

  /// Initializes has_varlen_slots_ during c'tor using the tuple_desc_map_.
  void InitHasVarlenSlots();

  /// map from position of tuple w/in row to its descriptor
  std::vector<TupleDescriptor*> tuple_desc_map_;

  /// tuple_idx_nullable_map_[i] is true if tuple i can be null
  std::vector<bool> tuple_idx_nullable_map_;

  /// map from TupleId to position of tuple w/in row
  std::vector<int> tuple_idx_map_;

  /// Provide quick way to check if there are variable length slots.
  bool has_varlen_slots_;
};
}
