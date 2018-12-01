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

#include "exec/orc-metadata-utils.h"

#include "util/debug-util.h"
#include "common/names.h"

namespace impala {

void OrcMetadataUtils::BuildSchemaPaths(const orc::Type& root, int num_partition_keys,
    vector<SchemaPath>* paths) {
  SchemaPath path;
  paths->push_back(path);
  DCHECK_EQ(root.getKind(), orc::TypeKind::STRUCT);
  int num_columns = root.getSubtypeCount();
  for (int i = 0; i < num_columns; ++i) {
    path.push_back(i + num_partition_keys);
    BuildSchemaPath(*root.getSubtype(i), &path, paths);
    path.pop_back();
  }
}

void OrcMetadataUtils::BuildSchemaPath(const orc::Type& node, SchemaPath* path,
    vector<SchemaPath>* paths) {
  DCHECK_EQ(paths->size(), node.getColumnId());
  paths->push_back(*path);
  if (node.getKind() == orc::TypeKind::STRUCT) {
    int size = node.getSubtypeCount();
    for (int i = 0; i < size; ++i) {
      path->push_back(i);
      const orc::Type* child = node.getSubtype(i);
      BuildSchemaPath(*child, path, paths);
      path->pop_back();
    }
  } else if (node.getKind() == orc::TypeKind::LIST) {
    DCHECK_EQ(node.getSubtypeCount(), 1);
    const orc::Type* child = node.getSubtype(0);
    path->push_back(SchemaPathConstants::ARRAY_ITEM);
    BuildSchemaPath(*child, path, paths);
    path->pop_back();
  } else if (node.getKind() == orc::TypeKind::MAP) {
    DCHECK_EQ(node.getSubtypeCount(), 2);
    const orc::Type* key_child = node.getSubtype(0);
    const orc::Type* value_child = node.getSubtype(1);
    path->push_back(SchemaPathConstants::MAP_KEY);
    BuildSchemaPath(*key_child, path, paths);
    (*path)[path->size() - 1] = SchemaPathConstants::MAP_VALUE;
    BuildSchemaPath(*value_child, path, paths);
    path->pop_back();
  }
}

Status OrcSchemaResolver::ResolveColumn(const SchemaPath& col_path,
    const orc::Type** node, bool* pos_field, bool* missing_field) const {
  const ColumnType* table_col_type = nullptr;
  *node = root_;
  *pos_field = false;
  *missing_field = false;
  for (int i = 0; i < col_path.size(); ++i) {
    int table_idx = col_path[i];
    int file_idx = table_idx;
    if (i == 0) {
      table_col_type = &tbl_desc_.col_descs()[table_idx].type();
      // For top-level columns, the first index in a path includes the table's partition
      // keys.
      file_idx -= tbl_desc_.num_clustering_cols();
    } else if (table_col_type->type == TYPE_ARRAY &&
        table_idx == SchemaPathConstants::ARRAY_POS) {
      // To materialize the positions, the ORC lib has to materialize the whole array
      // column.
      *pos_field = true;
      break;  // return *node as the ARRAY node
    } else {
      table_col_type = &table_col_type->children[table_idx];
    }

    if (file_idx >= (*node)->getSubtypeCount()) {
      *missing_field = true;
      return Status::OK();
    }
    *node = (*node)->getSubtype(file_idx);
    if (table_col_type->type == TYPE_ARRAY) {
      DCHECK_EQ(table_col_type->children.size(), 1);
      if ((*node)->getKind() != orc::TypeKind::LIST) {
        return Status(Substitute("File '$0' has an incompatible ORC schema for column "
            "'$1', Column type: $2, ORC schema:\\n$3", filename_,
            PrintSubPath(tbl_desc_, col_path, i), "array", (*node)->toString()));
      }
    } else if (table_col_type->type == TYPE_MAP) {
      DCHECK_EQ(table_col_type->children.size(), 2);
      if ((*node)->getKind() != orc::TypeKind::MAP) {
        return Status(Substitute("File '$0' has an incompatible ORC schema for column "
            "'$1', Column type: $2, ORC schema:\\n$3", filename_,
            PrintSubPath(tbl_desc_, col_path, i), "map", (*node)->toString()));
      }
    } else if (table_col_type->type == TYPE_STRUCT) {
      DCHECK_GT(table_col_type->children.size(), 0);
    } else {
      DCHECK(!table_col_type->IsComplexType());
      DCHECK_EQ(i, col_path.size() - 1);
      RETURN_IF_ERROR(ValidateType(*table_col_type, **node));
    }
  }
  return Status::OK();
}

Status OrcSchemaResolver::ValidateType(const ColumnType& type,
    const orc::Type& orc_type) const {
  switch (orc_type.getKind()) {
    case orc::TypeKind::BOOLEAN:
      if (type.type == TYPE_BOOLEAN) return Status::OK();
      break;
    case orc::TypeKind::BYTE:
      if (type.type == TYPE_TINYINT || type.type == TYPE_SMALLINT
          || type.type == TYPE_INT || type.type == TYPE_BIGINT) {
        return Status::OK();
      }
      break;
    case orc::TypeKind::SHORT:
      if (type.type == TYPE_SMALLINT || type.type == TYPE_INT
          || type.type == TYPE_BIGINT) {
        return Status::OK();
      }
      break;
    case orc::TypeKind::INT:
      if (type.type == TYPE_INT || type.type == TYPE_BIGINT) return Status::OK();
      break;
    case orc::TypeKind::LONG:
      if (type.type == TYPE_BIGINT) return Status::OK();
      break;
    case orc::TypeKind::FLOAT:
    case orc::TypeKind::DOUBLE:
      if (type.type == TYPE_FLOAT || type.type == TYPE_DOUBLE) return Status::OK();
      break;
    case orc::TypeKind::STRING:
    case orc::TypeKind::VARCHAR:
    case orc::TypeKind::CHAR:
      if (type.type == TYPE_STRING || type.type == TYPE_VARCHAR
          || type.type == TYPE_CHAR) {
        return Status::OK();
      }
      break;
    case orc::TypeKind::TIMESTAMP:
      if (type.type == TYPE_TIMESTAMP) return Status::OK();
      break;
    case orc::TypeKind::DECIMAL: {
      if (type.type != TYPE_DECIMAL || type.scale != orc_type.getScale()) break;
      bool overflow = false;
      int orc_precision = orc_type.getPrecision();
      if (orc_precision == 0 || orc_precision > ColumnType::MAX_DECIMAL8_PRECISION) {
        // For ORC decimals whose precision is larger than 18, its value can't fit into
        // an int64 (10^19 > 2^63). So we should use int128 (16 bytes) for this case.
        // The possible byte sizes for Impala decimals are 4, 8, 16.
        // We mark it as overflow if the target byte size is not 16.
        overflow = (type.GetByteSize() != 16);
      } else if (orc_type.getPrecision() > ColumnType::MAX_DECIMAL4_PRECISION) {
        // For ORC decimals whose precision <= 18 and > 9, int64 and int128 can fit them.
        // We only mark it as overflow if the target byte size is 4.
        overflow = (type.GetByteSize() == 4);
      }
      if (!overflow) return Status::OK();
      return Status(Substitute(
          "Column $0 in ORC file '$1' can't be truncated to table column $2",
          orc_type.toString(), filename_, type.DebugString()));
    }
    default: break;
  }
  return Status(Substitute(
      "Type mismatch: table column $0 is map to column $1 in ORC file '$2'",
      type.DebugString(), orc_type.toString(), filename_));
}
}
