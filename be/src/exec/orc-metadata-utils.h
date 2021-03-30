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
#include <queue>

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
      const char* filename, bool is_table_acid) : tbl_desc_(tbl_desc), root_(root),
      filename_(filename), is_table_full_acid_(is_table_acid) {
        DetermineFullAcidSchema();
      }

  /// Resolve SchemaPath into orc::Type (ORC column representation)
  /// 'pos_field' is set to true if 'col_path' reference the index field of an array
  /// column. '*node' will be the array node if 'pos_field' is set to true.
  /// 'missing_field' is set to true if the column is missing in the ORC file.
  Status ResolveColumn(const SchemaPath& col_path, const orc::Type** node,
      bool* pos_field, bool* missing_field) const;

  /// Build a map from each orc::Type id to a SchemaPath. The map will be used in
  /// creating OrcColumnReaders. It contains all ORC types including unmaterialized ones.
  Status BuildSchemaPaths(int num_partition_keys,
      std::vector<SchemaPath>* col_id_path_map) const;

  /// Returns true if file schema corresponds to full ACIDv2 format.
  bool HasFullAcidV2Schema() const { return is_file_full_acid_; }

  /// Can be only invoked for original files of full transactional tables.
  /// Returns true if 'col_path' refers to an ACID column.
  bool IsAcidColumn(const SchemaPath& col_path) const;

 private:
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

  SchemaPath GetCanonicalSchemaPath(const SchemaPath& col_path, int last_idx) const;

  /// Sets 'is_file_full_acid_' based on the file schema.
  void DetermineFullAcidSchema();

  const HdfsTableDescriptor& tbl_desc_;
  const orc::Type* const root_;
  const char* const filename_ = nullptr;
  const bool is_table_full_acid_;
  bool is_file_full_acid_;

  /// Validate whether the ColumnType is compatible with the orc type
  Status ValidateType(const ColumnType& type, const orc::Type& orc_type) const
      WARN_UNUSED_RESULT;

  /// Helper function to build 'col_id_path_map' start from 'node' whose corresponding
  /// SchemaPath is 'path'.
  static void BuildSchemaPathHelper(const orc::Type& node, SchemaPath* path,
      std::vector<SchemaPath>* col_id_path_map);
};
}
