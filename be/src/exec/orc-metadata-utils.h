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

// ORC type id of column "currentTransaction" in full ACID ORC files.
constexpr int CURRENT_TRANSCACTION_TYPE_ID = 5;

/// Util class to resolve SchemaPaths of TupleDescriptors/SlotDescriptors into orc::Type.
class OrcSchemaResolver {
 public:
  OrcSchemaResolver(const HdfsTableDescriptor& tbl_desc, const orc::Type* root,
      const char* filename, bool is_table_acid, bool is_file_acid) : tbl_desc_(tbl_desc),
      root_(root), filename_(filename), is_table_full_acid_(is_table_acid),
      is_file_full_acid_(is_file_acid) { }

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

  /// Returns error if the file should be in ACIDv2 format,
  /// but the actual file schema doesn't conform to it.
  Status ValidateFullAcidFileSchema() const;

 private:
  const HdfsTableDescriptor& tbl_desc_;
  const orc::Type* const root_;
  const char* const filename_ = nullptr;
  const bool is_table_full_acid_;
  const bool is_file_full_acid_;

  /// Validate whether the ColumnType is compatible with the orc type
  Status ValidateType(const ColumnType& type, const orc::Type& orc_type) const
      WARN_UNUSED_RESULT;

  /// Helper function to build 'col_id_path_map' start from 'node' whose corresponding
  /// SchemaPath is 'path'.
  static void BuildSchemaPathHelper(const orc::Type& node, SchemaPath* path,
      std::vector<SchemaPath>* col_id_path_map);
};
}
