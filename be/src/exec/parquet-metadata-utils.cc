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

#include "exec/parquet-metadata-utils.h"

#include <string>
#include <sstream>
#include <vector>
#include <strings.h>

#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/status.h"
#include "exec/parquet-common.h"
#include "exec/parquet-column-stats.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;

namespace impala {

// Needs to be in sync with the order of enum values declared in TParquetArrayResolution.
const std::vector<ParquetSchemaResolver::ArrayEncoding>
    ParquetSchemaResolver::ORDERED_ARRAY_ENCODINGS[] =
        {{ParquetSchemaResolver::THREE_LEVEL, ParquetSchemaResolver::ONE_LEVEL},
         {ParquetSchemaResolver::TWO_LEVEL, ParquetSchemaResolver::ONE_LEVEL},
         {ParquetSchemaResolver::TWO_LEVEL, ParquetSchemaResolver::THREE_LEVEL,
             ParquetSchemaResolver::ONE_LEVEL}};

Status ParquetMetadataUtils::ValidateFileVersion(
    const parquet::FileMetaData& file_metadata, const char* filename) {
  if (file_metadata.version > PARQUET_CURRENT_VERSION) {
    stringstream ss;
    ss << "File: " << filename << " is of an unsupported version. "
       << "file version: " << file_metadata.version;
    return Status(ss.str());
  }
  return Status::OK();
}

Status ParquetMetadataUtils::ValidateColumnOffsets(const string& filename,
    int64_t file_length, const parquet::RowGroup& row_group) {
  for (int i = 0; i < row_group.columns.size(); ++i) {
    const parquet::ColumnChunk& col_chunk = row_group.columns[i];
    RETURN_IF_ERROR(ValidateOffsetInFile(filename, i, file_length,
        col_chunk.meta_data.data_page_offset, "data page offset"));
    int64_t col_start = col_chunk.meta_data.data_page_offset;
    // The file format requires that if a dictionary page exists, it be before data pages.
    if (col_chunk.meta_data.__isset.dictionary_page_offset) {
      RETURN_IF_ERROR(ValidateOffsetInFile(filename, i, file_length,
            col_chunk.meta_data.dictionary_page_offset, "dictionary page offset"));
      if (col_chunk.meta_data.dictionary_page_offset >= col_start) {
        return Status(Substitute("Parquet file '$0': metadata is corrupt. Dictionary "
            "page (offset=$1) must come before any data pages (offset=$2).",
            filename, col_chunk.meta_data.dictionary_page_offset, col_start));
      }
      col_start = col_chunk.meta_data.dictionary_page_offset;
    }
    int64_t col_len = col_chunk.meta_data.total_compressed_size;
    int64_t col_end = col_start + col_len;
    if (col_end <= 0 || col_end > file_length) {
      return Status(Substitute("Parquet file '$0': metadata is corrupt. Column $1 has "
          "invalid column offsets (offset=$2, size=$3, file_size=$4).", filename, i,
          col_start, col_len, file_length));
    }
  }
  return Status::OK();
}

Status ParquetMetadataUtils::ValidateOffsetInFile(const string& filename, int col_idx,
    int64_t file_length, int64_t offset, const string& offset_name) {
  if (offset < 0 || offset >= file_length) {
    return Status(Substitute("File '$0': metadata is corrupt. Column $1 has invalid "
        "$2 (offset=$3 file_size=$4).", filename, col_idx, offset_name, offset,
        file_length));
  }
  return Status::OK();;
}

static bool IsEncodingSupported(parquet::Encoding::type e) {
  switch (e) {
    case parquet::Encoding::PLAIN:
    case parquet::Encoding::PLAIN_DICTIONARY:
    case parquet::Encoding::BIT_PACKED:
    case parquet::Encoding::RLE:
      return true;
    default:
      return false;
  }
}

Status ParquetMetadataUtils::ValidateColumn(const parquet::FileMetaData& file_metadata,
    const char* filename, int row_group_idx, int col_idx,
    const parquet::SchemaElement& schema_element, const SlotDescriptor* slot_desc,
    RuntimeState* state) {
  const parquet::ColumnChunk& file_data =
      file_metadata.row_groups[row_group_idx].columns[col_idx];

  // Check the encodings are supported.
  const vector<parquet::Encoding::type>& encodings = file_data.meta_data.encodings;
  for (int i = 0; i < encodings.size(); ++i) {
    if (!IsEncodingSupported(encodings[i])) {
      stringstream ss;
      ss << "File '" << filename << "' uses an unsupported encoding: "
         << PrintEncoding(encodings[i]) << " for column '" << schema_element.name
         << "'.";
      return Status(ss.str());
    }
  }

  // Check the compression is supported.
  if (file_data.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED &&
      file_data.meta_data.codec != parquet::CompressionCodec::SNAPPY &&
      file_data.meta_data.codec != parquet::CompressionCodec::GZIP) {
    stringstream ss;
    ss << "File '" << filename << "' uses an unsupported compression: "
        << file_data.meta_data.codec << " for column '" << schema_element.name
        << "'.";
    return Status(ss.str());
  }

  // Validation after this point is only if col_reader is reading values.
  if (slot_desc == NULL) return Status::OK();

  parquet::Type::type type = IMPALA_TO_PARQUET_TYPES[slot_desc->type().type];
  if (UNLIKELY(type != file_data.meta_data.type)) {
    return Status(Substitute("Unexpected Parquet type in file '$0' metadata expected $1 "
        "actual $2: file may be corrupt", filename, type, file_data.meta_data.type));
  }

  // Check the decimal scale in the file matches the metastore scale and precision.
  // We fail the query if the metadata makes it impossible for us to safely read
  // the file. If we don't require the metadata, we will fail the query if
  // abort_on_error is true, otherwise we will just log a warning.
  bool is_converted_type_decimal = schema_element.__isset.converted_type &&
      schema_element.converted_type == parquet::ConvertedType::DECIMAL;
  if (slot_desc->type().type == TYPE_DECIMAL) {
    // We require that the scale and byte length be set.
    if (schema_element.type != parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      stringstream ss;
      ss << "File '" << filename << "' column '" << schema_element.name
         << "' should be a decimal column encoded using FIXED_LEN_BYTE_ARRAY.";
      return Status(ss.str());
    }

    if (!schema_element.__isset.type_length) {
      stringstream ss;
      ss << "File '" << filename << "' column '" << schema_element.name
         << "' does not have type_length set.";
      return Status(ss.str());
    }

    int expected_len = ParquetPlainEncoder::DecimalSize(slot_desc->type());
    if (schema_element.type_length != expected_len) {
      stringstream ss;
      ss << "File '" << filename << "' column '" << schema_element.name
         << "' has an invalid type length. Expecting: " << expected_len
         << " len in file: " << schema_element.type_length;
      return Status(ss.str());
    }

    if (!schema_element.__isset.scale) {
      stringstream ss;
      ss << "File '" << filename << "' column '" << schema_element.name
         << "' does not have the scale set.";
      return Status(ss.str());
    }

    if (schema_element.scale != slot_desc->type().scale) {
      // TODO: we could allow a mismatch and do a conversion at this step.
      stringstream ss;
      ss << "File '" << filename << "' column '" << schema_element.name
         << "' has a scale that does not match the table metadata scale."
         << " File metadata scale: " << schema_element.scale
         << " Table metadata scale: " << slot_desc->type().scale;
      return Status(ss.str());
    }

    // The other decimal metadata should be there but we don't need it.
    if (!schema_element.__isset.precision) {
      ErrorMsg msg(TErrorCode::PARQUET_MISSING_PRECISION, filename,
          schema_element.name);
      RETURN_IF_ERROR(state->LogOrReturnError(msg));
    } else {
      if (schema_element.precision != slot_desc->type().precision) {
        // TODO: we could allow a mismatch and do a conversion at this step.
        ErrorMsg msg(TErrorCode::PARQUET_WRONG_PRECISION, filename, schema_element.name,
            schema_element.precision, slot_desc->type().precision);
        RETURN_IF_ERROR(state->LogOrReturnError(msg));
      }
    }

    if (!is_converted_type_decimal) {
      // TODO: is this validation useful? It is not required at all to read the data and
      // might only serve to reject otherwise perfectly readable files.
      ErrorMsg msg(TErrorCode::PARQUET_BAD_CONVERTED_TYPE, filename,
          schema_element.name);
      RETURN_IF_ERROR(state->LogOrReturnError(msg));
    }
  } else if (schema_element.__isset.scale || schema_element.__isset.precision ||
      is_converted_type_decimal) {
    ErrorMsg msg(TErrorCode::PARQUET_INCOMPATIBLE_DECIMAL, filename,
        schema_element.name, slot_desc->type().DebugString());
    RETURN_IF_ERROR(state->LogOrReturnError(msg));
  }
  return Status::OK();
}

bool ParquetMetadataUtils::HasRowGroupStats(const parquet::RowGroup& row_group,
    int col_idx) {
  DCHECK(col_idx < row_group.columns.size());
  const parquet::ColumnChunk& col_chunk = row_group.columns[col_idx];
  return col_chunk.__isset.meta_data && col_chunk.meta_data.__isset.statistics;
}

ParquetFileVersion::ParquetFileVersion(const string& created_by) {
  string created_by_lower = created_by;
  std::transform(created_by_lower.begin(), created_by_lower.end(),
      created_by_lower.begin(), ::tolower);
  is_impala_internal = false;

  vector<string> tokens;
  split(tokens, created_by_lower, is_any_of(" "), token_compress_on);
  // Boost always creates at least one token
  DCHECK_GT(tokens.size(), 0);
  application = tokens[0];

  if (tokens.size() >= 3 && tokens[1] == "version") {
    string version_string = tokens[2];
    // Ignore any trailing nodextra characters
    int n = version_string.find_first_not_of("0123456789.");
    string version_string_trimmed = version_string.substr(0, n);

    vector<string> version_tokens;
    split(version_tokens, version_string_trimmed, is_any_of("."));
    version.major = version_tokens.size() >= 1 ? atoi(version_tokens[0].c_str()) : 0;
    version.minor = version_tokens.size() >= 2 ? atoi(version_tokens[1].c_str()) : 0;
    version.patch = version_tokens.size() >= 3 ? atoi(version_tokens[2].c_str()) : 0;

    if (application == "impala") {
      if (version_string.find("-internal") != string::npos) is_impala_internal = true;
    }
  } else {
    version.major = 0;
    version.minor = 0;
    version.patch = 0;
  }
}

bool ParquetFileVersion::VersionLt(int major, int minor, int patch) const {
  if (version.major < major) return true;
  if (version.major > major) return false;
  DCHECK_EQ(version.major, major);
  if (version.minor < minor) return true;
  if (version.minor > minor) return false;
  DCHECK_EQ(version.minor, minor);
  return version.patch < patch;
}

bool ParquetFileVersion::VersionEq(int major, int minor, int patch) const {
  return version.major == major && version.minor == minor && version.patch == patch;
}

static string PrintRepetitionType(const parquet::FieldRepetitionType::type& t) {
  switch (t) {
    case parquet::FieldRepetitionType::REQUIRED: return "required";
    case parquet::FieldRepetitionType::OPTIONAL: return "optional";
    case parquet::FieldRepetitionType::REPEATED: return "repeated";
    default: return "<unknown>";
  }
}

static string PrintParquetType(const parquet::Type::type& t) {
  switch (t) {
    case parquet::Type::BOOLEAN: return "boolean";
    case parquet::Type::INT32: return "int32";
    case parquet::Type::INT64: return "int64";
    case parquet::Type::INT96: return "int96";
    case parquet::Type::FLOAT: return "float";
    case parquet::Type::DOUBLE: return "double";
    case parquet::Type::BYTE_ARRAY: return "byte_array";
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: return "fixed_len_byte_array";
    default: return "<unknown>";
  }
}

string SchemaNode::DebugString(int indent) const {
  stringstream ss;
  for (int i = 0; i < indent; ++i) ss << " ";
  ss << PrintRepetitionType(element->repetition_type) << " ";
  if (element->num_children > 0) {
    ss << "struct";
  } else {
    ss << PrintParquetType(element->type);
  }
  ss << " " << element->name << " [i:" << col_idx << " d:" << max_def_level
     << " r:" << max_rep_level << "]";
  if (element->num_children > 0) {
    ss << " {" << endl;
    for (int i = 0; i < element->num_children; ++i) {
      ss << children[i].DebugString(indent + 2) << endl;
    }
    for (int i = 0; i < indent; ++i) ss << " ";
    ss << "}";
  }
  return ss.str();
}

Status ParquetSchemaResolver::CreateSchemaTree(const vector<parquet::SchemaElement>& schema,
    SchemaNode* node) const {
  int idx = 0;
  int col_idx = 0;
  RETURN_IF_ERROR(CreateSchemaTree(schema, 0, 0, 0, &idx, &col_idx, node));
  if (node->children.empty()) {
    return Status(Substitute("Invalid file: '$0' has no columns.", filename_));
  }
  return Status::OK();
}

Status ParquetSchemaResolver::CreateSchemaTree(
    const vector<parquet::SchemaElement>& schema, int max_def_level, int max_rep_level,
    int ira_def_level, int* idx, int* col_idx, SchemaNode* node)
    const {
  if (*idx >= schema.size()) {
    return Status(Substitute("File '$0' corrupt: could not reconstruct schema tree from "
            "flattened schema in file metadata", filename_));
  }
  node->element = &schema[*idx];
  ++(*idx);

  if (node->element->num_children == 0) {
    // node is a leaf node, meaning it's materialized in the file and appears in
    // file_metadata_.row_groups.columns
    node->col_idx = *col_idx;
    ++(*col_idx);
  } else if (node->element->num_children > SCHEMA_NODE_CHILDREN_SANITY_LIMIT) {
    // Sanity-check the schema to avoid allocating absurdly large buffers below.
    return Status(Substitute("Schema in Parquet file '$0' has $1 children, more than limit of "
        "$2. File is likely corrupt", filename_, node->element->num_children,
        SCHEMA_NODE_CHILDREN_SANITY_LIMIT));
  } else if (node->element->num_children < 0) {
    return Status(Substitute("Corrupt Parquet file '$0': schema element has $1 children.",
        filename_, node->element->num_children));
  }

  // def_level_of_immediate_repeated_ancestor does not include this node, so set before
  // updating ira_def_level
  node->def_level_of_immediate_repeated_ancestor = ira_def_level;

  if (node->element->repetition_type == parquet::FieldRepetitionType::OPTIONAL) {
    ++max_def_level;
  } else if (node->element->repetition_type == parquet::FieldRepetitionType::REPEATED) {
    ++max_rep_level;
    // Repeated fields add a definition level. This is used to distinguish between an
    // empty list and a list with an item in it.
    ++max_def_level;
    // node is the new most immediate repeated ancestor
    ira_def_level = max_def_level;
  }
  node->max_def_level = max_def_level;
  node->max_rep_level = max_rep_level;

  node->children.resize(node->element->num_children);
  for (int i = 0; i < node->element->num_children; ++i) {
    RETURN_IF_ERROR(CreateSchemaTree(schema, max_def_level, max_rep_level, ira_def_level,
        idx, col_idx, &node->children[i]));
  }
  return Status::OK();
}

Status ParquetSchemaResolver::ResolvePath(const SchemaPath& path, SchemaNode** node,
    bool* pos_field, bool* missing_field) const {
  *missing_field = false;
  const vector<ArrayEncoding>& ordered_array_encodings =
      ORDERED_ARRAY_ENCODINGS[array_resolution_];

  bool any_missing_field = false;
  Status statuses[NUM_ARRAY_ENCODINGS];
  for (const auto& array_encoding: ordered_array_encodings) {
    bool current_missing_field;
    statuses[array_encoding] = ResolvePathHelper(
        array_encoding, path, node, pos_field, &current_missing_field);
    if (current_missing_field) DCHECK(statuses[array_encoding].ok());
    if (statuses[array_encoding].ok() && !current_missing_field) return Status::OK();
    any_missing_field = any_missing_field || current_missing_field;
  }
  // None of resolutions yielded a node. Set *missing_field to true if any of the
  // resolutions reported a missing a field.
  if (any_missing_field) {
    *node = NULL;
    *missing_field = true;
    return Status::OK();
  }

  // All resolutions failed. Log and return the most relevant status. The three-level
  // encoding is the Parquet standard, so always prefer that. Prefer the two-level over
  // the one-level because the two-level can be specifically selected via a query option.
  Status error_status = Status::OK();
  for (int i = THREE_LEVEL; i >= ONE_LEVEL; --i) {
    if (!statuses[i].ok()) {
      error_status = statuses[i];
      break;
    }
  }
  DCHECK(!error_status.ok());
  *node = NULL;
  VLOG_QUERY << error_status.msg().msg() << "\n" << GetStackTrace();
  return error_status;
}

Status ParquetSchemaResolver::ResolvePathHelper(ArrayEncoding array_encoding,
    const SchemaPath& path, SchemaNode** node, bool* pos_field, bool* missing_field) const {
  DCHECK(schema_.element != NULL)
      << "schema_ must be initialized before calling ResolvePath()";

  *pos_field = false;
  *missing_field = false;
  *node = const_cast<SchemaNode*>(&schema_);
  const ColumnType* col_type = NULL;

  // Traverse 'path' and resolve 'node' to the corresponding SchemaNode in 'schema_' (by
  // ordinal), or set 'node' to NULL if 'path' doesn't exist in this file's schema.
  for (int i = 0; i < path.size(); ++i) {
    // Advance '*node' if necessary
    if (i == 0 || col_type->type != TYPE_ARRAY || array_encoding == THREE_LEVEL) {
      *node = NextSchemaNode(col_type, path, i, *node, missing_field);
      if (*missing_field) return Status::OK();
    } else {
      // We just resolved an array, meaning *node is set to the repeated field of the
      // array. Since we are trying to resolve using one- or two-level array encoding, the
      // repeated field represents both the array and the array's item (i.e. there is no
      // explict item field), so we don't advance *node in this case.
      DCHECK(col_type != NULL);
      DCHECK_EQ(col_type->type, TYPE_ARRAY);
      DCHECK(array_encoding == ONE_LEVEL || array_encoding == TWO_LEVEL);
      DCHECK((*node)->is_repeated());
    }

    // Advance 'col_type'
    int table_idx = path[i];
    col_type = i == 0 ? &tbl_desc_.col_descs()[table_idx].type()
               : &col_type->children[table_idx];

    // Resolve path[i]
    if (col_type->type == TYPE_ARRAY) {
      DCHECK_EQ(col_type->children.size(), 1);
      RETURN_IF_ERROR(
          ResolveArray(array_encoding, path, i, node, pos_field, missing_field));
      if (*missing_field || *pos_field) return Status::OK();
    } else if (col_type->type == TYPE_MAP) {
      DCHECK_EQ(col_type->children.size(), 2);
      RETURN_IF_ERROR(ResolveMap(path, i, node, missing_field));
      if (*missing_field) return Status::OK();
    } else if (col_type->type == TYPE_STRUCT) {
      DCHECK_GT(col_type->children.size(), 0);
      // Nothing to do for structs
    } else {
      DCHECK(!col_type->IsComplexType());
      DCHECK_EQ(i, path.size() - 1);
      RETURN_IF_ERROR(ValidateScalarNode(**node, *col_type, path, i));
    }
  }
  DCHECK(*node != NULL);
  return Status::OK();
}

SchemaNode* ParquetSchemaResolver::NextSchemaNode(
    const ColumnType* col_type, const SchemaPath& path, int next_idx, SchemaNode* node,
    bool* missing_field) const {
  DCHECK_LT(next_idx, path.size());
  if (next_idx != 0) DCHECK(col_type != NULL);

  int file_idx;
  int table_idx = path[next_idx];
  if (fallback_schema_resolution_ == TParquetFallbackSchemaResolution::type::NAME) {
    if (next_idx == 0) {
      // Resolve top-level table column by name.
      DCHECK_LT(table_idx, tbl_desc_.col_descs().size());
      const string& name = tbl_desc_.col_descs()[table_idx].name();
      file_idx = FindChildWithName(node, name);
    } else if (col_type->type == TYPE_STRUCT) {
      // Resolve struct field by name.
      DCHECK_LT(table_idx, col_type->field_names.size());
      const string& name = col_type->field_names[table_idx];
      file_idx = FindChildWithName(node, name);
    } else if (col_type->type == TYPE_ARRAY) {
      // Arrays have only one child in the file.
      DCHECK_EQ(table_idx, SchemaPathConstants::ARRAY_ITEM);
      file_idx = table_idx;
    } else {
      DCHECK_EQ(col_type->type, TYPE_MAP);
      // Maps have two values, "key" and "value". These are supposed to be ordered and may
      // not have the right field names, but try to resolve by name in case they're
      // switched and otherwise use the order. See
      // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps for
      // more details.
      DCHECK(table_idx == SchemaPathConstants::MAP_KEY ||
             table_idx == SchemaPathConstants::MAP_VALUE);
      const string& name = table_idx == SchemaPathConstants::MAP_KEY ? "key" : "value";
      file_idx = FindChildWithName(node, name);
      if (file_idx >= node->children.size()) {
        // Couldn't resolve by name, fall back to resolution by position.
        file_idx = table_idx;
      }
    }
  } else {
    // Resolution by position.
    DCHECK_EQ(fallback_schema_resolution_,
        TParquetFallbackSchemaResolution::type::POSITION);
    if (next_idx == 0) {
      // For top-level columns, the first index in a path includes the table's partition
      // keys.
      file_idx = table_idx - tbl_desc_.num_clustering_cols();
    } else {
      file_idx = table_idx;
    }
  }

  if (file_idx >= node->children.size()) {
    string schema_resolution_mode = "unknown";
    auto entry = _TParquetFallbackSchemaResolution_VALUES_TO_NAMES.find(
        fallback_schema_resolution_);
    if (entry != _TParquetFallbackSchemaResolution_VALUES_TO_NAMES.end()) {
      schema_resolution_mode = entry->second;
    }
    VLOG_FILE << Substitute(
        "File '$0' does not contain path '$1' (resolving by $2)", filename_,
        PrintPath(tbl_desc_, path), schema_resolution_mode);
    *missing_field = true;
    return NULL;
  }
  return &node->children[file_idx];
}

int ParquetSchemaResolver::FindChildWithName(SchemaNode* node,
    const string& name) const {
  int idx;
  for (idx = 0; idx < node->children.size(); ++idx) {
    if (strcasecmp(node->children[idx].element->name.c_str(), name.c_str()) == 0) break;
  }
  return idx;
}

// There are three types of array encodings:
//
// 1. One-level encoding
//      A bare repeated field. This is interpreted as a required array of required
//      items.
//    Example:
//      repeated <item-type> item;
//
// 2. Two-level encoding
//      A group containing a single repeated field. This is interpreted as a
//      <list-repetition> array of required items (<list-repetition> is either
//      optional or required).
//    Example:
//      <list-repetition> group <name> {
//        repeated <item-type> item;
//      }
//
// 3. Three-level encoding
//      The "official" encoding according to the parquet spec. A group containing a
//      single repeated group containing the item field. This is interpreted as a
//      <list-repetition> array of <item-repetition> items (<list-repetition> and
//      <item-repetition> are each either optional or required).
//    Example:
//      <list-repetition> group <name> {
//        repeated group list {
//          <item-repetition> <item-type> item;
//        }
//      }
//
// We ignore any field annotations or names, making us more permissive than the
// Parquet spec dictates. Note that in any of the encodings, <item-type> may be a
// group containing more fields, which corresponds to a complex item type. See
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists for
// more details and examples.
//
// This function resolves the array at '*node' assuming one-, two-, or three-level
// encoding, determined by 'array_encoding'. '*node' is set to the repeated field for all
// three encodings (unless '*pos_field' or '*missing_field' are set to true).
Status ParquetSchemaResolver::ResolveArray(ArrayEncoding array_encoding,
    const SchemaPath& path, int idx, SchemaNode** node, bool* pos_field,
    bool* missing_field) const {
  if (array_encoding == ONE_LEVEL) {
    if (!(*node)->is_repeated()) {
      ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename_,
          PrintSubPath(tbl_desc_, path, idx), "array", (*node)->DebugString());
      return Status::Expected(msg);
    }
  } else {
    // In the multi-level case, we always expect the outer group to contain a single
    // repeated field
    if ((*node)->children.size() != 1 || !(*node)->children[0].is_repeated()) {
      ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename_,
          PrintSubPath(tbl_desc_, path, idx), "array", (*node)->DebugString());
      return Status::Expected(msg);
    }
    // Set *node to the repeated field
    *node = &(*node)->children[0];
  }
  DCHECK((*node)->is_repeated());

  if (idx + 1 < path.size()) {
    if (path[idx + 1] == SchemaPathConstants::ARRAY_POS) {
      // The next index in 'path' is the artifical position field.
      DCHECK_EQ(path.size(), idx + 2) << "position field cannot have children!";
      *pos_field = true;
      *node = NULL;
      return Status::OK();
    } else {
      // The next value in 'path' should be the item index
      DCHECK_EQ(path[idx + 1], SchemaPathConstants::ARRAY_ITEM);
    }
  }
  return Status::OK();
}

// According to the parquet spec, map columns are represented like:
// <map-repetition> group <name> (MAP) {
//   repeated group key_value {
//     required <key-type> key;
//     <value-repetition> <value-type> value;
//   }
// }
// We ignore any field annotations or names, making us more permissive than the
// Parquet spec dictates. See
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps for
// more details.
Status ParquetSchemaResolver::ResolveMap(const SchemaPath& path, int idx, SchemaNode** node,
    bool* missing_field) const {
  if ((*node)->children.size() != 1 || !(*node)->children[0].is_repeated() ||
      (*node)->children[0].children.size() != 2) {
    ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename_,
        PrintSubPath(tbl_desc_, path, idx), "map", (*node)->DebugString());
    return Status::Expected(msg);
  }
  *node = &(*node)->children[0];

  // The next index in 'path' should be the key or the value.
  if (idx + 1 < path.size()) {
    DCHECK(path[idx + 1] == SchemaPathConstants::MAP_KEY ||
           path[idx + 1] == SchemaPathConstants::MAP_VALUE);
  }
  return Status::OK();
}

Status ParquetSchemaResolver::ValidateScalarNode(const SchemaNode& node,
    const ColumnType& col_type, const SchemaPath& path, int idx) const {
  if (!node.children.empty()) {
    ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename_,
        PrintSubPath(tbl_desc_, path, idx), col_type.DebugString(), node.DebugString());
    return Status::Expected(msg);
  }
  parquet::Type::type type = IMPALA_TO_PARQUET_TYPES[col_type.type];
  if (type != node.element->type) {
    ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename_,
        PrintSubPath(tbl_desc_, path, idx), col_type.DebugString(), node.DebugString());
    return Status::Expected(msg);
  }
  return Status::OK();
}

}
