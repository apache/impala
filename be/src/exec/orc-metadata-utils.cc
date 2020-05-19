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

Status OrcSchemaResolver::BuildSchemaPaths(int num_partition_keys,
    vector<SchemaPath>* col_id_path_map) const {
  if (root_->getKind() != orc::TypeKind::STRUCT) {
    return Status(TErrorCode::ORC_TYPE_NOT_ROOT_AT_STRUCT, "file", root_->toString(),
        filename_);
  }
  bool synthetic_acid_schema = is_table_full_acid_ && !is_file_full_acid_;
  SchemaPath path;
  col_id_path_map->push_back(path);
  int num_columns = root_->getSubtypeCount();
  // Original files don't have "row" field, let's add it manually.
  if (synthetic_acid_schema) path.push_back(ACID_FIELD_ROW + num_partition_keys);
  for (int i = 0; i < num_columns; ++i) {
    // For synthetic ACID schema these columns are not top-level, so don't need to
    // adjust them by 'num_part_keys'.
    int field_offset = synthetic_acid_schema ? 0 : num_partition_keys;
    path.push_back(i + field_offset);
    BuildSchemaPathHelper(*root_->getSubtype(i), &path, col_id_path_map);
    path.pop_back();
  }
  return Status::OK();
}

void OrcSchemaResolver::BuildSchemaPathHelper(const orc::Type& node, SchemaPath* path,
    vector<SchemaPath>* col_id_path_map) {
  DCHECK_EQ(col_id_path_map->size(), node.getColumnId()) << Substitute(
      "Failed building map from ORC type ids to SchemaPaths. $0 are built but current "
      "ORC type id is $1, missing ORC type with id=$0.",
      col_id_path_map->size(), node.getColumnId());
  // Map ORC type with id=size() to 'path'.
  col_id_path_map->push_back(*path);
  // Deal with subtypes recursively.
  if (node.getKind() == orc::TypeKind::STRUCT
      || node.getKind() == orc::TypeKind::UNION) {
    // It's possible the file schema contains more columns than the table schema.
    // We should deal with UNION type here in case it's not in the table schema and the
    // query is not reading them. Otherwise the map will miss some ORC types.
    int size = node.getSubtypeCount();
    for (int i = 0; i < size; ++i) {
      path->push_back(i);
      const orc::Type* child = node.getSubtype(i);
      BuildSchemaPathHelper(*child, path, col_id_path_map);
      path->pop_back();
    }
  } else if (node.getKind() == orc::TypeKind::LIST) {
    DCHECK_EQ(node.getSubtypeCount(), 1);
    const orc::Type* child = node.getSubtype(0);
    path->push_back(SchemaPathConstants::ARRAY_ITEM);
    BuildSchemaPathHelper(*child, path, col_id_path_map);
    path->pop_back();
  } else if (node.getKind() == orc::TypeKind::MAP) {
    DCHECK_EQ(node.getSubtypeCount(), 2);
    const orc::Type* key_child = node.getSubtype(0);
    const orc::Type* value_child = node.getSubtype(1);
    path->push_back(SchemaPathConstants::MAP_KEY);
    BuildSchemaPathHelper(*key_child, path, col_id_path_map);
    (*path)[path->size() - 1] = SchemaPathConstants::MAP_VALUE;
    BuildSchemaPathHelper(*value_child, path, col_id_path_map);
    path->pop_back();
  }
}

Status OrcSchemaResolver::ResolveColumn(const SchemaPath& col_path,
    const orc::Type** node, bool* pos_field, bool* missing_field) const {
  const ColumnType* table_col_type = nullptr;
  *node = root_;
  *pos_field = false;
  *missing_field = false;
  DCHECK_OK(ValidateFullAcidFileSchema()); // Should have already been validated.
  if (col_path.empty()) return Status::OK();
  SchemaPath table_path, file_path;
  TranslateColPaths(col_path, &table_path, &file_path);
  for (int i = 0; i < table_path.size(); ++i) {
    int table_idx = table_path[i];
    int file_idx = file_path[i];
    if (table_idx == -1 || file_idx == -1) {
      DCHECK_NE(table_idx, file_idx);
      if (table_idx == -1) {
        DCHECK_EQ(*node, root_);
        *node = (*node)->getSubtype(file_idx);
      } else {
        DCHECK(table_col_type == nullptr);
        table_col_type = &tbl_desc_.col_descs()[table_idx].type();
      }
      continue;
    }
    if (table_col_type == nullptr) {
      table_col_type = &tbl_desc_.col_descs()[table_idx].type();
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
        return Status(TErrorCode::ORC_NESTED_TYPE_MISMATCH, filename_,
            PrintPath(tbl_desc_, GetCanonicalSchemaPath(table_path, i)), "array",
            (*node)->toString());
      }
    } else if (table_col_type->type == TYPE_MAP) {
      DCHECK_EQ(table_col_type->children.size(), 2);
      if ((*node)->getKind() != orc::TypeKind::MAP) {
        return Status(TErrorCode::ORC_NESTED_TYPE_MISMATCH, filename_,
            PrintPath(tbl_desc_, GetCanonicalSchemaPath(table_path, i)), "map",
            (*node)->toString());
      }
    } else if (table_col_type->type == TYPE_STRUCT) {
      DCHECK_GT(table_col_type->children.size(), 0);
      if ((*node)->getKind() != orc::TypeKind::STRUCT) {
        return Status(TErrorCode::ORC_NESTED_TYPE_MISMATCH, filename_,
            PrintPath(tbl_desc_, GetCanonicalSchemaPath(table_path, i)), "struct",
            (*node)->toString());
      }
    } else {
      DCHECK(!table_col_type->IsComplexType());
      DCHECK_EQ(i, table_path.size() - 1);
      RETURN_IF_ERROR(ValidateType(*table_col_type, **node));
    }
  }
  return Status::OK();
}

SchemaPath OrcSchemaResolver::GetCanonicalSchemaPath(const SchemaPath& col_path,
    int last_idx) const {
  DCHECK_LT(last_idx, col_path.size());
  SchemaPath ret;
  ret.reserve(col_path.size());
  std::copy_if(col_path.begin(),
               col_path.begin() + last_idx + 1,
               std::back_inserter(ret),
               [](int i) { return i >= 0; });
  return ret;
}

void OrcSchemaResolver::TranslateColPaths(const SchemaPath& col_path,
    SchemaPath* table_col_path, SchemaPath* file_col_path) const {
  DCHECK(!col_path.empty());
  DCHECK(table_col_path != nullptr);
  DCHECK(file_col_path != nullptr);
  table_col_path->reserve(col_path.size() + 1);
  file_col_path->reserve(col_path.size() + 1);
  int first_idx = col_path[0];
  int num_part_cols = tbl_desc_.num_clustering_cols();
  int remaining_idx = 0;
  if (!is_table_full_acid_) {
    // Table is not full ACID. Only need to adjust partitioning columns.
    table_col_path->push_back(first_idx);
    file_col_path->push_back(first_idx - num_part_cols);
    remaining_idx = 1;
  } else if (is_file_full_acid_) {
    DCHECK(is_table_full_acid_);
    // Table is full ACID, and file is in full ACID format too. We need to do some
    // conversions since the Frontend table schema and file schema differs. See the
    // comment at the declaration of this function.
    if (first_idx == num_part_cols + ACID_FIELD_ROW) {
      // 'first_idx' refers to "row" column. Table definition doesn't have "row" column.
      table_col_path->push_back(-1);
      file_col_path->push_back(first_idx - num_part_cols);
      if (col_path.size() == 1 ) return;
      int second_idx = col_path[1];
      // Adjust table with num partitioning colums and the synthetic 'row__id' column.
      table_col_path->push_back(num_part_cols + 1 + second_idx);
      file_col_path->push_back(second_idx);
    } else {
      DCHECK_GE(first_idx, num_part_cols);
      // 'col_path' refers to the ACID columns. In table schema they are nested
      // under the synthetic 'row__id' column. 'row__id' is at index 'num_part_cols'.
      table_col_path->push_back(num_part_cols);
      file_col_path->push_back(-1);
      // The ACID column is under 'row__id' at index 'table_idx - num_part_cols'.
      int acid_col_idx = first_idx - num_part_cols;
      table_col_path->push_back(acid_col_idx);
      file_col_path->push_back(acid_col_idx);
    }
    remaining_idx = 2;
  } else if (!is_file_full_acid_) {
    DCHECK(is_table_full_acid_);
    // Table is full ACID, but file is in non-ACID format.
    if (first_idx == num_part_cols + ACID_FIELD_ROW) {
      if (col_path.size() == 1 ) return;
      // 'first_idx' refers to "row" column. Table definition doesn't have "row" column,
      // but neither the file schema here. We don't include it in the output paths.
      int second_idx = col_path[1];
      // Adjust table with num partitioning colums and the synthetic 'row__id' column.
      table_col_path->push_back(num_part_cols + 1 + second_idx);
      file_col_path->push_back(second_idx);
    } else {
      DCHECK_GE(first_idx, num_part_cols);
      // 'col_path' refers to the ACID columns. In table schema they are nested
      // under the synthetic 'row__id' column. 'row__id' is at index 'num_part_cols'.
      table_col_path->push_back(num_part_cols);
      file_col_path->push_back(-1);
      // The ACID column is under 'row__id' at index 'table_idx - num_part_cols'.
      int acid_col_idx = first_idx - num_part_cols;
      table_col_path->push_back(acid_col_idx);
      // ACID columns in original files should be considered as missing colums.
      file_col_path->push_back(std::numeric_limits<int>::max());
    }
    remaining_idx = 2;
  }
  // The rest of the path is unchanged.
  for (int i = remaining_idx; i < col_path.size(); ++i) {
    table_col_path->push_back(col_path[i]);
    file_col_path->push_back(col_path[i]);
  }
  DCHECK_EQ(table_col_path->size(), file_col_path->size());
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
    case orc::TypeKind::DATE:
      if (type.type == TYPE_DATE) return Status::OK();
      break;
    default: break;
  }
  return Status(Substitute(
      "Type mismatch: table column $0 is map to column $1 in ORC file '$2'",
      type.DebugString(), orc_type.toString(), filename_));
}

bool OrcSchemaResolver::IsAcidColumn(const SchemaPath& col_path) const {
  DCHECK(is_table_full_acid_);
  DCHECK(!is_file_full_acid_);
  int num_part_cols = tbl_desc_.num_clustering_cols();
  return col_path.size() == 1 &&
         col_path.front() >= num_part_cols && col_path.front() < num_part_cols + 5;
}

Status OrcSchemaResolver::ValidateFullAcidFileSchema() const {
  if (!is_file_full_acid_) return Status::OK();
  string error_msg = Substitute("File %0 should have full ACID schema.", filename_);
  if (root_->getKind() != orc::TypeKind::STRUCT) return Status(error_msg);
  if (root_->getSubtypeCount() != 6) return Status(error_msg);
  if (root_->getSubtype(0)->getKind() != orc::TypeKind::INT ||
      root_->getSubtype(1)->getKind() != orc::TypeKind::LONG ||
      root_->getSubtype(2)->getKind() != orc::TypeKind::INT ||
      root_->getSubtype(3)->getKind() != orc::TypeKind::LONG ||
      root_->getSubtype(4)->getKind() != orc::TypeKind::LONG ||
      root_->getSubtype(5)->getKind() != orc::TypeKind::STRUCT) {
    return Status(error_msg);
  }
  if (root_->getFieldName(0) != "operation" ||
      root_->getFieldName(1) != "originalTransaction" ||
      root_->getFieldName(2) != "bucket" ||
      root_->getFieldName(3) != "rowId" ||
      root_->getFieldName(4) != "currentTransaction" ||
      root_->getFieldName(5) != "row") {
    return Status(error_msg);
  }
  return Status::OK();
}

} // namespace impala
