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

#include "exec/orc/orc-metadata-utils.h"

#include <stack>

#include <boost/algorithm/string.hpp>

#include "util/debug-util.h"
#include "common/names.h"

using boost::algorithm::iequals;

namespace impala {

static const std::string& ICEBERG_FIELD_ID = "iceberg.id";

inline int GetFieldIdFromStr(const std::string& str) {
  try {
    return std::stoi(str);
  } catch (std::exception&) {
    return -1;
  }
}

OrcSchemaResolver::OrcSchemaResolver(const HdfsTableDescriptor& tbl_desc,
    const orc::Type* root, const char* filename, bool is_table_acid,
    TSchemaResolutionStrategy::type schema_resolution)
  : schema_resolution_strategy_(schema_resolution),
    tbl_desc_(tbl_desc),
    root_(root),
    filename_(filename),
    is_table_full_acid_(is_table_acid) {
  DetermineFullAcidSchema();
  if (tbl_desc_.IsIcebergTable()) {
    schema_resolution_strategy_ = TSchemaResolutionStrategy::FIELD_ID;

    if (root_->getSubtypeCount() > 0
        && !root_->getSubtype(0)->hasAttributeKey(ICEBERG_FIELD_ID)) {
      GenerateFieldIDs();
    }
  }
}

Status OrcSchemaResolver::ResolveColumn(const SchemaPath& col_path,
    const orc::Type** node, bool* pos_field, bool* missing_field) const {
  if (schema_resolution_strategy_ == TSchemaResolutionStrategy::POSITION) {
    return ResolveColumnByPosition(col_path, node, pos_field, missing_field);
  } else if (schema_resolution_strategy_ == TSchemaResolutionStrategy::NAME) {
    return ResolveColumnByName(col_path, node, pos_field, missing_field);
  } else if (schema_resolution_strategy_ == TSchemaResolutionStrategy::FIELD_ID) {
    return ResolveColumnByIcebergFieldId(col_path, node, pos_field, missing_field);
  } else {
    DCHECK(false);
    return Status(Substitute("Invalid schema resolution strategy: $0",
        schema_resolution_strategy_));
  }
}

Status OrcSchemaResolver::ResolveColumnByPosition(const SchemaPath& col_path,
    const orc::Type** node, bool* pos_field, bool* missing_field) const {
  const ColumnType* table_col_type = nullptr;
  *node = root_;
  *pos_field = false;
  *missing_field = false;
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
    RETURN_IF_ERROR(ValidateType(*table_col_type, **node, table_path, i));
  }
  return Status::OK();
}

Status OrcSchemaResolver::ValidateType(const ColumnType& table_col_type,
    const orc::Type& orc_type, const SchemaPath& table_path,
    int current_idx) const {
  if (table_col_type.type == TYPE_ARRAY) {
    RETURN_IF_ERROR(ValidateArray(table_col_type, orc_type, table_path, current_idx));
  } else if (table_col_type.type == TYPE_MAP) {
    RETURN_IF_ERROR(ValidateMap(table_col_type, orc_type, table_path, current_idx));
  } else if (table_col_type.type == TYPE_STRUCT) {
    RETURN_IF_ERROR(ValidateStruct(table_col_type, orc_type, table_path, current_idx));
  } else {
    DCHECK(!table_col_type.IsComplexType());
    DCHECK_EQ(current_idx, table_path.size() - 1);
    RETURN_IF_ERROR(ValidatePrimitiveType(table_col_type, orc_type));
  }
  return Status::OK();
}

Status OrcSchemaResolver::ValidateStruct(const ColumnType& type,
    const orc::Type& orc_type, const SchemaPath& col_path,
    int current_idx) const {
  DCHECK_GT(type.children.size(), 0);
  if (orc_type.getKind() != orc::TypeKind::STRUCT) {
    return Status(TErrorCode::ORC_NESTED_TYPE_MISMATCH, filename_,
        PrintPath(tbl_desc_, GetCanonicalSchemaPath(col_path, current_idx)), "struct",
        orc_type.toString());
  }
  return Status::OK();
}

Status OrcSchemaResolver::ValidateArray(const ColumnType& type,
    const orc::Type& orc_type, const SchemaPath& col_path,
    int current_idx) const {
  DCHECK_EQ(type.children.size(), 1);
  if (orc_type.getKind() != orc::TypeKind::LIST) {
    return Status(TErrorCode::ORC_NESTED_TYPE_MISMATCH, filename_,
        PrintPath(tbl_desc_, GetCanonicalSchemaPath(col_path, current_idx)), "array",
        orc_type.toString());
  }
  return Status::OK();
}

Status OrcSchemaResolver::ValidateMap(const ColumnType& type,
    const orc::Type& orc_type, const SchemaPath& col_path,
    int current_idx) const {
  DCHECK_EQ(type.children.size(), 2);
  if (orc_type.getKind() != orc::TypeKind::MAP) {
    return Status(TErrorCode::ORC_NESTED_TYPE_MISMATCH, filename_,
        PrintPath(tbl_desc_, GetCanonicalSchemaPath(col_path, current_idx)), "map",
        orc_type.toString());
  }
  return Status::OK();
}

Status OrcSchemaResolver::ResolveColumnByName(const SchemaPath& col_path,
    const orc::Type** node, bool* pos_field, bool* missing_field) const {
  const ColumnType* table_col_type = nullptr;
  *node = root_;
  *pos_field = false;
  *missing_field = false;
  if (col_path.empty()) return Status::OK();
  SchemaPath table_path, file_path;
  TranslateColPaths(col_path, &table_path, &file_path);

  int i = 0;

  // Resolve table and file ACID differences
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
    i++;
  }

  for (; i < table_path.size(); ++i) {
    table_idx = table_path[i];
    if (table_col_type == nullptr) {
      // non ACID table, or top level user column in ACID table
      table_col_type = &tbl_desc_.col_descs()[table_idx].type();
      const std::string& name = tbl_desc_.col_descs()[table_idx].name();
      *node = FindChildWithName(*node, name);
      if (*node == nullptr) {
        *missing_field = true;
        return Status::OK();
      }
      RETURN_IF_ERROR(ValidateType(*table_col_type, **node, table_path, i));
      continue;
    } else if (table_col_type->type == TYPE_STRUCT) {
      // Resolve struct field by name.
      DCHECK_LT(table_idx, table_col_type->field_names.size());
      const std::string& name = table_col_type->field_names[table_idx];
      *node = FindChildWithName(*node, name);
    } else if (table_col_type->type == TYPE_ARRAY) {
      if (table_idx == SchemaPathConstants::ARRAY_POS) {
        *pos_field = true;
        break; // return *node as the ARRAY node
      }
      DCHECK_EQ(table_idx, SchemaPathConstants::ARRAY_ITEM);
      *node = (*(node))->getSubtype(table_idx);
    } else if (table_col_type->type == TYPE_MAP) {
      DCHECK(table_idx == SchemaPathConstants::MAP_KEY
          || table_idx == SchemaPathConstants::MAP_VALUE);
      // At this point we've found a MAP with a matching name. It's safe to resolve
      // the child (key or value) by position.
      *node = (*(node))->getSubtype(table_idx);
    }
    if (*node == nullptr) {
      *missing_field = true;
      return Status::OK();
    }
    table_col_type = &table_col_type->children[table_idx];
    RETURN_IF_ERROR(ValidateType(*table_col_type, **node, table_path, i));
  }
  return Status::OK();
}

const orc::Type* OrcSchemaResolver::FindChildWithName(
    const orc::Type* node, const std::string& name) const {
  for (int i = 0; i < node->getSubtypeCount(); ++i) {
    const orc::Type* child = node->getSubtype(i);
    DCHECK(child != nullptr);
    const std::string& fieldName = node->getFieldName(i);
    if (iequals(fieldName, name)) return child;
  }
  return nullptr;
}

Status OrcSchemaResolver::ResolveColumnByIcebergFieldId(const SchemaPath& col_path,
    const orc::Type** node, bool* pos_field, bool* missing_field) const {
  const ColumnType* table_col_type = nullptr;
  *node = root_;
  *pos_field = false;
  *missing_field = false;
  if (col_path.empty()) return Status::OK();
  for (int i = 0; i < col_path.size(); ++i) {
    int table_idx = col_path[i];
    if (i == 0) {
      table_col_type = &tbl_desc_.col_descs()[table_idx].type();
      int field_id = tbl_desc_.col_descs()[table_idx].field_id();
      *node = FindChildWithFieldId(*node, field_id);
      if (*node == nullptr) {
        *missing_field = true;
        return Status::OK();
      }
      RETURN_IF_ERROR(ValidateType(*table_col_type, **node, col_path, i));
      continue;
    }
    if (table_col_type->type == TYPE_STRUCT) {
      // Resolve struct field by field id.
      DCHECK_LT(table_idx, table_col_type->field_ids.size());
      const int field_id = table_col_type->field_ids[table_idx];
      *node = FindChildWithFieldId(*node, field_id);
    } else if (table_col_type->type == TYPE_ARRAY) {
      if (table_idx == SchemaPathConstants::ARRAY_POS) {
        *pos_field = true;
        break;  // return *node as the ARRAY node
      }
      DCHECK_EQ(table_idx, SchemaPathConstants::ARRAY_ITEM);
      *node = (*(node))->getSubtype(table_idx);
    } else if (table_col_type->type == TYPE_MAP) {
      DCHECK(table_idx == SchemaPathConstants::MAP_KEY ||
             table_idx == SchemaPathConstants::MAP_VALUE);
      // At this point we've found a MAP with a matching field id. It's safe to resolve
      // the child (key or value) by position.
      *node = (*(node))->getSubtype(table_idx);
    }
    if (*node == nullptr) {
      *missing_field = true;
      return Status::OK();
    }
    table_col_type = &table_col_type->children[table_idx];
    RETURN_IF_ERROR(ValidateType(*table_col_type, **node, col_path, i));
  }
  return Status::OK();
}

const orc::Type* OrcSchemaResolver::FindChildWithFieldId(const orc::Type* node,
    const int field_id) const {
  for (int i = 0; i < node->getSubtypeCount(); ++i) {
    const orc::Type* child = node->getSubtype(i);
    DCHECK(child != nullptr);

    int child_field_id = 0;

    if (LIKELY(child->hasAttributeKey(ICEBERG_FIELD_ID))) {
      std::string field_id_str = child->getAttributeValue(ICEBERG_FIELD_ID);
      child_field_id = GetFieldIdFromStr(field_id_str);
    } else {
      child_field_id = GetGeneratedFieldID(child);
    }

    if (child_field_id == -1) return nullptr;
    if (child_field_id == field_id) return child;
  }
  return nullptr;
}

void OrcSchemaResolver::GenerateFieldIDs() {
  std::stack<const orc::Type*> nodes;

  nodes.push(root_);

  int fieldID = 1;

  while (!nodes.empty()) {
    const orc::Type* current = nodes.top();
    nodes.pop();

    uint64_t size = current->getSubtypeCount();

    for (uint64_t i = 0; i < size; i++) {
      auto retval = orc_type_to_field_id_.emplace(current->getSubtype(i), fieldID++);

      // Emplace has to be successful, otherwise we visited the same node twice
      DCHECK(retval.second);

      // Push children in reverse order to the stack so they are processed in the original
      // order
      nodes.push(current->getSubtype(size - i - 1));
    }
  }
}

int OrcSchemaResolver::GetGeneratedFieldID(const orc::Type* type) const {
  auto it = orc_type_to_field_id_.find(type);

  // First column has field ID, this one does not, file is corrupted
  if (UNLIKELY(it == orc_type_to_field_id_.end())) return -1;

  return it->second;
}

SchemaPath OrcSchemaResolver::GetCanonicalSchemaPath(const SchemaPath& col_path,
    int current_idx) const {
  DCHECK_LT(current_idx, col_path.size());
  SchemaPath ret;
  ret.reserve(col_path.size());
  std::copy_if(col_path.begin(),
               col_path.begin() + current_idx + 1,
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
      DCHECK_GE(first_idx, num_part_cols) << "col_path: " << PrintNumericPath(col_path);
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
      DCHECK_GE(first_idx, num_part_cols) << "col_path: " << PrintNumericPath(col_path);
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

Status OrcSchemaResolver::ValidatePrimitiveType(const ColumnType& type,
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
    case orc::TypeKind::BINARY:
      // orc::TypeKind::BINARY is handled as TYPE_STRING, TYPE_BINARY is not used.
      if (type.type == TYPE_STRING || type.type == TYPE_VARCHAR
          || type.type == TYPE_CHAR) {
        return Status::OK();
      }
      break;
    case orc::TypeKind::TIMESTAMP:
    case orc::TypeKind::TIMESTAMP_INSTANT:
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

void OrcSchemaResolver::DetermineFullAcidSchema() {
  is_file_full_acid_ = false;
  if (root_->getKind() != orc::TypeKind::STRUCT) return;
  if (root_->getSubtypeCount() != 6) return;
  if (root_->getSubtype(0)->getKind() != orc::TypeKind::INT ||
      root_->getSubtype(1)->getKind() != orc::TypeKind::LONG ||
      root_->getSubtype(2)->getKind() != orc::TypeKind::INT ||
      root_->getSubtype(3)->getKind() != orc::TypeKind::LONG ||
      root_->getSubtype(4)->getKind() != orc::TypeKind::LONG ||
      root_->getSubtype(5)->getKind() != orc::TypeKind::STRUCT) {
    return;
  }
  if (!iequals(root_->getFieldName(0), "operation") ||
      !iequals(root_->getFieldName(1), "originaltransaction") ||
      !iequals(root_->getFieldName(2), "bucket") ||
      !iequals(root_->getFieldName(3), "rowid") ||
      !iequals(root_->getFieldName(4), "currenttransaction") ||
      !iequals(root_->getFieldName(5), "row")) {
    return;
  }
  is_file_full_acid_ = true;
}

} // namespace impala
