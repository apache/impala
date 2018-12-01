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

/// Utils to build a map from each orc::Type id to a SchemaPath. The map will be used in
/// creating OrcColumnReaders.
class OrcMetadataUtils {
 public:
  static void BuildSchemaPaths(const orc::Type& root, int num_partition_keys,
      std::vector<SchemaPath>* paths);
 private:
  static void BuildSchemaPath(const orc::Type& node, SchemaPath* path,
      std::vector<SchemaPath>* paths);
};

/// Util class to resolve SchemaPaths of TupleDescriptors/SlotDescriptors into orc::Type.
class OrcSchemaResolver {
 public:
  OrcSchemaResolver(const HdfsTableDescriptor& tbl_desc, const orc::Type* root,
      const char* filename) : tbl_desc_(tbl_desc), root_(root), filename_(filename) { }

  /// Resolve SchemaPath into orc::Type (ORC column representation)
  /// 'pos_field' is set to true if 'col_path' reference the index field of an array
  /// column. '*node' will be the array node if 'pos_field' is set to true.
  /// 'missing_field' is set to true if the column is missing in the ORC file.
  Status ResolveColumn(const SchemaPath& col_path, const orc::Type** node,
      bool* pos_field, bool* missing_field) const;
 private:
  const HdfsTableDescriptor& tbl_desc_;
  const orc::Type* const root_;
  const char* const filename_ = nullptr;

  /// Validate whether the ColumnType is compatible with the orc type
  Status ValidateType(const ColumnType& type, const orc::Type& orc_type) const
      WARN_UNUSED_RESULT;
};
}
