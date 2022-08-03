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

#include <orc/OrcFile.hh>

#include "runtime/descriptors.h"

namespace impala {

// Key of Hive ACID version in ORC metadata.
const string HIVE_ACID_VERSION_KEY = "hive.acid.version";

// Table level indexes of ACID columns.
constexpr int ACID_FIELD_OPERATION_INDEX = 0;
constexpr int ACID_FIELD_ORIGINAL_TRANSACTION_INDEX = 1;
constexpr int ACID_FIELD_BUCKET_INDEX = 2;
constexpr int ACID_FIELD_ROWID_INDEX = 3;
constexpr int ACID_FIELD_CURRENT_TRANSACTION_INDEX = 4;
constexpr int ACID_FIELD_ROW = 5;

// ORC type id of column "currentTransaction" in full ACID ORC files.
constexpr int CURRENT_TRANSCACTION_TYPE_ID = 5;

/// Util class to resolve SchemaPaths of TupleDescriptors/SlotDescriptors into orc::Type.
class OrcSchemaResolver {
 public:
  OrcSchemaResolver(const HdfsTableDescriptor& tbl_desc, const orc::Type* root,
      const char* filename, bool is_table_acid,
      TSchemaResolutionStrategy::type schema_resolution);

  /// Resolve SchemaPath into orc::Type (ORC column representation)
  /// 'pos_field' is set to true if 'col_path' reference the index field of an array
  /// column. '*node' will be the array node if 'pos_field' is set to true.
  /// 'missing_field' is set to true if the column is missing in the ORC file.
  Status ResolveColumn(const SchemaPath& col_path, const orc::Type** node,
      bool* pos_field, bool* missing_field) const;

  /// Returns true if file schema corresponds to full ACIDv2 format.
  bool HasFullAcidV2Schema() const { return is_file_full_acid_; }

  /// Can be only invoked for original files of full transactional tables.
  /// Returns true if 'col_path' refers to an ACID column.
  bool IsAcidColumn(const SchemaPath& col_path) const;

  /// Translates 'col_path' to non-canonical table and file paths. These non-canonical
  /// paths have the same lengths. To achieve that they might contain -1 values that must
  /// be ignored. These paths are useful for tables that have different table and file
  /// schema (ACID tables, partitioned tables).
  /// E.g. ACID table schema is
  /// {
  ///   "row__id" : {...ACID columns...},
  ///   ...TABLE columns...
  /// }
  /// While ACID file schema is
  /// {
  ///   ...ACID columns...,
  ///   "row" : {...TABLE columns...}
  /// }
  /// Let's assume we have a non-partitioned ACID table and the first user column is
  /// called 'id'.
  /// In that case 'col_path' for 'id' looks like [5, 0]. This function converts it to
  /// non-canonical 'table_col_path' [-1, 1] and non-canonical 'file_col_path'
  /// [5, 0] (which is the same as the canonical in this case).
  /// Another example for ACID column 'rowid':
  /// 'col_path' is [3], 'table_col_path' is [0, 3], 'file_col_path' is [-1, 3].
  /// Different conversions are needed for original files and non-transactional tables
  /// (for the latter it only adjusts first column offsets if the table is partitioned).
  /// These non-canonical paths are easier to be processed by ResolveColumn().
  void TranslateColPaths(const SchemaPath& col_path,
      SchemaPath* table_col_path, SchemaPath* file_col_path) const;

 private:
  TSchemaResolutionStrategy::type schema_resolution_strategy_;

  /// Resolve column based on position. This only works when the fields in the HMS
  /// table schema match the file schema (apart from Hive ACID schema differences which
  /// are being handled).
  Status ResolveColumnByPosition(const SchemaPath& col_path, const orc::Type** node,
      bool* pos_field, bool* missing_field) const;

  /// Resolve column based on name.
  Status ResolveColumnByName(const SchemaPath& col_path, const orc::Type** node,
      bool* pos_field, bool* missing_field) const;

  /// Resolve column based on the Iceberg field ids. This way we will retrieve the
  /// Iceberg field ids from the HMS table via 'col_path', then find the corresponding
  /// field in the ORC file.
  Status ResolveColumnByIcebergFieldId(const SchemaPath& col_path, const orc::Type** node,
      bool* pos_field, bool* missing_field) const;

  /// Finds child of 'node' whose column name matches to provided 'name'.
  const orc::Type* FindChildWithName(
      const orc::Type* node, const std::string& name) const;

  /// Finds child of 'node' that has Iceberg field id equals to 'field_id'.
  const orc::Type* FindChildWithFieldId(const orc::Type* node, const int field_id) const;

  /// Generates field ids for the columns in the same order as Iceberg. The traversal is
  /// preorder, but the assigned field IDs are not in that order. When a node is
  /// processed, its child nodes are assigned an ID, hence the difference.
  void GenerateFieldIDs();

  inline int GetGeneratedFieldID(const orc::Type* type) const;

  SchemaPath GetCanonicalSchemaPath(const SchemaPath& col_path, int last_idx) const;

  /// Sets 'is_file_full_acid_' based on the file schema.
  void DetermineFullAcidSchema();

  const HdfsTableDescriptor& tbl_desc_;
  const orc::Type* const root_;
  const char* const filename_ = nullptr;
  const bool is_table_full_acid_;
  bool is_file_full_acid_;
  std::unordered_map<const orc::Type*, int> orc_type_to_field_id_;

  /// Validate whether the ColumnType is compatible with the orc type
  Status ValidateType(const ColumnType& type, const orc::Type& orc_type,
    const SchemaPath& col_path, int last_idx) const WARN_UNUSED_RESULT;
  Status ValidateStruct(const ColumnType& type, const orc::Type& orc_type,
      const SchemaPath& col_path, int last_idx) const WARN_UNUSED_RESULT;
  Status ValidateArray(const ColumnType& type, const orc::Type& orc_type,
      const SchemaPath& col_path, int last_idx) const WARN_UNUSED_RESULT;
  Status ValidateMap(const ColumnType& type, const orc::Type& orc_type,
      const SchemaPath& col_path, int last_idx) const WARN_UNUSED_RESULT;
  Status ValidatePrimitiveType(const ColumnType& type, const orc::Type& orc_type) const
      WARN_UNUSED_RESULT;
};
}
