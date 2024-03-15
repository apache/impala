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

#include "exec/parquet/parquet-metadata-utils.h"

#include <strings.h>
#include <sstream>
#include <stack>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/status.h"
#include "exec/parquet/parquet-column-stats.h"
#include "exec/parquet/parquet-common.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/ubsan.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;

namespace impala {

namespace {

const map<PrimitiveType, set<parquet::Type::type>> SUPPORTED_PHYSICAL_TYPES = {
    {PrimitiveType::INVALID_TYPE, {parquet::Type::BOOLEAN}},
    {PrimitiveType::TYPE_NULL, {parquet::Type::BOOLEAN}},
    {PrimitiveType::TYPE_BOOLEAN, {parquet::Type::BOOLEAN}},
    {PrimitiveType::TYPE_TINYINT, {parquet::Type::INT32}},
    {PrimitiveType::TYPE_SMALLINT, {parquet::Type::INT32}},
    {PrimitiveType::TYPE_INT, {parquet::Type::INT32}},
    {PrimitiveType::TYPE_BIGINT, {parquet::Type::INT32, parquet::Type::INT64}},
    {PrimitiveType::TYPE_FLOAT, {parquet::Type::FLOAT}},
    {PrimitiveType::TYPE_DOUBLE, {parquet::Type::INT32, parquet::Type::FLOAT,
        parquet::Type::DOUBLE}},
    {PrimitiveType::TYPE_TIMESTAMP, {parquet::Type::INT96, parquet::Type::INT64}},
    {PrimitiveType::TYPE_STRING, {parquet::Type::BYTE_ARRAY}},
    {PrimitiveType::TYPE_DATE, {parquet::Type::INT32}},
    {PrimitiveType::TYPE_DATETIME, {parquet::Type::BYTE_ARRAY}},
    {PrimitiveType::TYPE_BINARY, {parquet::Type::BYTE_ARRAY}},
    {PrimitiveType::TYPE_DECIMAL, {parquet::Type::INT32, parquet::Type::INT64,
        parquet::Type::FIXED_LEN_BYTE_ARRAY, parquet::Type::BYTE_ARRAY}},
    {PrimitiveType::TYPE_CHAR, {parquet::Type::BYTE_ARRAY}},
    {PrimitiveType::TYPE_VARCHAR, {parquet::Type::BYTE_ARRAY}},
};

/// Returns true if 'element' describes a supported Timestamp column.
bool IsValidTimestampType(const parquet::SchemaElement& element) {
  ParquetTimestampDecoder::Precision precision; // unused
  bool needs_conversion; // unused
  return ParquetTimestampDecoder::GetTimestampInfoFromSchema(
      element, precision, needs_conversion);
}

/// Returns true if 'parquet_type' is a supported physical encoding for the Impala
/// primitive type, false otherwise. Some physical types are accepted only for certain
/// converted types.
bool IsSupportedType(PrimitiveType impala_type,
    const parquet::SchemaElement& element) {
  // Timestamps need special handling, because the supported physical types depend on
  // the logical type.
  if (impala_type == PrimitiveType::TYPE_TIMESTAMP) return IsValidTimestampType(element);
  auto encodings = SUPPORTED_PHYSICAL_TYPES.find(impala_type);
  DCHECK(encodings != SUPPORTED_PHYSICAL_TYPES.end());
  parquet::Type::type parquet_type = element.type;
  return encodings->second.find(parquet_type) != encodings->second.end();
}

/// Returns true if encoding 'e' is supported by Impala, false otherwise.
static bool IsEncodingSupported(parquet::Encoding::type e) {
  switch (e) {
    case parquet::Encoding::PLAIN:
    case parquet::Encoding::PLAIN_DICTIONARY:
    case parquet::Encoding::BIT_PACKED:
    case parquet::Encoding::RLE:
    case parquet::Encoding::RLE_DICTIONARY:
      return true;
    default:
      return false;
  }
}

/// Sets logical and converted types in 'col_schema' to signed 'bitwidth' bit integer.
void SetIntLogicalType(int bitwidth, parquet::SchemaElement* col_schema) {
  parquet::IntType int_type;
  int_type.__set_bitWidth(bitwidth);
  int_type.__set_isSigned(true);
  parquet::LogicalType logical_type;
  logical_type.__set_INTEGER(int_type);
  col_schema->__set_logicalType(logical_type);
}

/// Sets logical and converted types in 'col_schema' to DATE.
void SetDateLogicalType(parquet::SchemaElement* col_schema) {
  col_schema->__set_converted_type(parquet::ConvertedType::DATE);
  parquet::LogicalType logical_type;
  logical_type.__set_DATE(parquet::DateType());
  col_schema->__set_logicalType(logical_type);
}

/// Sets logical and converted types in 'col_schema' to UTF8.
void SetUtf8ConvertedAndLogicalType(parquet::SchemaElement* col_schema) {
  col_schema->__set_converted_type(parquet::ConvertedType::UTF8);
  parquet::LogicalType logical_type;
  logical_type.__set_STRING(parquet::StringType());
  col_schema->__set_logicalType(logical_type);
}

/// Sets logical and converted types in 'col_schema' to DECIMAL.
/// Precision and scale are set according to 'col_type'.
void SetDecimalConvertedAndLogicalType(
    const ColumnType& col_type, parquet::SchemaElement* col_schema) {
  DCHECK_EQ(col_type.type, TYPE_DECIMAL);

  col_schema->__set_type_length(ParquetPlainEncoder::DecimalSize(col_type));
  col_schema->__set_scale(col_type.scale);
  col_schema->__set_precision(col_type.precision);
  col_schema->__set_converted_type(parquet::ConvertedType::DECIMAL);

  parquet::DecimalType decimal_type;
  decimal_type.__set_scale(col_type.scale);
  decimal_type.__set_precision(col_type.precision);
  parquet::LogicalType logical_type;
  logical_type.__set_DECIMAL(decimal_type);
  col_schema->__set_logicalType(logical_type);
}

/// For int64 timestamps, sets logical_type in 'col_schema' to TIMESTAMP and fills its
/// parameters.
/// converted_type is not set because Impala always writes timestamps without UTC
/// normalization, and older readers that do not use logical types would incorrectly
/// interpret TIMESTAMP_MILLIS/MICROS as UTC normalized.
/// Leaves logical type empty for int96 timestamps.
void SetTimestampLogicalType(TParquetTimestampType::type parquet_timestamp_type,
    parquet::SchemaElement* col_schema) {
  if (parquet_timestamp_type == TParquetTimestampType::INT96_NANOS) return;

  parquet::TimeUnit time_unit;
  switch (parquet_timestamp_type) {
    case TParquetTimestampType::INT64_MILLIS:
      time_unit.__set_MILLIS(parquet::MilliSeconds());
      break;
    case TParquetTimestampType::INT64_MICROS:
      time_unit.__set_MICROS(parquet::MicroSeconds());
      break;
    case TParquetTimestampType::INT64_NANOS:
      time_unit.__set_NANOS(parquet::NanoSeconds());
      break;
    default:
      DCHECK(false);
  }

  parquet::TimestampType timestamp_type;
  timestamp_type.__set_unit(time_unit);
  timestamp_type.__set_isAdjustedToUTC(false);

  parquet::LogicalType logical_type;
  logical_type.__set_TIMESTAMP(timestamp_type);
  col_schema->__set_logicalType(logical_type);
}

bool IsScaleSet(const parquet::SchemaElement& schema_element) {
  // Scale is required in DecimalType
  return (schema_element.__isset.logicalType
             && schema_element.logicalType.__isset.DECIMAL)
      || schema_element.__isset.scale;
}

bool IsPrecisionSet(const parquet::SchemaElement& schema_element) {
  // Precision is required in DecimalType
  return (schema_element.__isset.logicalType
             && schema_element.logicalType.__isset.DECIMAL)
      || schema_element.__isset.precision;
}

int32_t GetScale(const parquet::SchemaElement& schema_element) {
  if (schema_element.__isset.logicalType && schema_element.logicalType.__isset.DECIMAL) {
    return schema_element.logicalType.DECIMAL.scale;
  }

  if (schema_element.__isset.scale) return schema_element.scale;

  // If not specified, the scale is 0
  return 0;
}

// Precision is required, this should be called after checking IsPrecisionSet()
int32_t GetPrecision(const parquet::SchemaElement& schema_element) {
  DCHECK(IsPrecisionSet(schema_element));
  if (schema_element.__isset.logicalType && schema_element.logicalType.__isset.DECIMAL) {
    return schema_element.logicalType.DECIMAL.precision;
  }

  return schema_element.precision;
}

/// Mapping of impala's internal types to parquet storage types. This is indexed by
/// PrimitiveType enum
const parquet::Type::type INTERNAL_TO_PARQUET_TYPES[] = {
  parquet::Type::BOOLEAN,     // Invalid
  parquet::Type::BOOLEAN,     // NULL type
  parquet::Type::BOOLEAN,
  parquet::Type::INT32,
  parquet::Type::INT32,
  parquet::Type::INT32,
  parquet::Type::INT64,
  parquet::Type::FLOAT,
  parquet::Type::DOUBLE,
  parquet::Type::INT96,       // Timestamp
  parquet::Type::BYTE_ARRAY,  // String
  parquet::Type::INT32,       // Date
  parquet::Type::BYTE_ARRAY,  // DateTime, NYI
  parquet::Type::BYTE_ARRAY,  // Binary NYI
  parquet::Type::FIXED_LEN_BYTE_ARRAY, // Decimal
  parquet::Type::BYTE_ARRAY,  // VARCHAR(N)
  parquet::Type::BYTE_ARRAY,  // CHAR(N)
};

const int INTERNAL_TO_PARQUET_TYPES_SIZE =
  sizeof(INTERNAL_TO_PARQUET_TYPES) / sizeof(INTERNAL_TO_PARQUET_TYPES[0]);

} // anonymous namespace

// Needs to be in sync with the order of enum values declared in TParquetArrayResolution.
const std::vector<ParquetSchemaResolver::ArrayEncoding>
    ParquetSchemaResolver::ORDERED_ARRAY_ENCODINGS[] =
        {{ParquetSchemaResolver::THREE_LEVEL, ParquetSchemaResolver::ONE_LEVEL},
         {ParquetSchemaResolver::TWO_LEVEL, ParquetSchemaResolver::ONE_LEVEL},
         {ParquetSchemaResolver::TWO_LEVEL, ParquetSchemaResolver::THREE_LEVEL,
             ParquetSchemaResolver::ONE_LEVEL}};

Status ParquetMetadataUtils::ValidateFileVersion(
    const parquet::FileMetaData& file_metadata, const char* filename) {
  if (file_metadata.version > PARQUET_MAX_SUPPORTED_VERSION) {
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
    if (col_end <= 0 || col_end >= file_length) {
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

Status ParquetMetadataUtils::ValidateRowGroupColumn(
    const parquet::FileMetaData& file_metadata, const char* filename, int row_group_idx,
    int col_idx, const parquet::SchemaElement& schema_element, RuntimeState* state) {
  DCHECK_GE(col_idx, 0);
  const parquet::ColumnMetaData& col_chunk_metadata =
      file_metadata.row_groups[row_group_idx].columns[col_idx].meta_data;

  // Check the encodings are supported.
  const vector<parquet::Encoding::type>& encodings = col_chunk_metadata.encodings;
  for (int i = 0; i < encodings.size(); ++i) {
    if (!IsEncodingSupported(encodings[i])) {
      return Status(Substitute("File '$0' uses an unsupported encoding: $1 for column "
          "'$2'.", filename, PrintValue(encodings[i]), schema_element.name));
    }
  }

  // Check the compression is supported.
  const auto codec = Ubsan::EnumToInt(&col_chunk_metadata.codec);
  if (codec != parquet::CompressionCodec::UNCOMPRESSED &&
      codec != parquet::CompressionCodec::SNAPPY &&
      codec != parquet::CompressionCodec::GZIP &&
      codec != parquet::CompressionCodec::ZSTD &&
      codec != parquet::CompressionCodec::LZ4) {
    return Status(Substitute("File '$0' uses an unsupported compression: $1 for column "
        "'$2'.", filename, codec, schema_element.name));
  }

  if (col_chunk_metadata.type != schema_element.type) {
    return Status(Substitute("Mismatched column chunk Parquet type in file '$0' column "
            "'$1'. Expected $2 actual $3: file may be corrupt", filename,
            schema_element.name, col_chunk_metadata.type, schema_element.type));
  }
  return Status::OK();
}

Status ParquetMetadataUtils::ValidateColumn(const char* filename,
    const parquet::SchemaElement& schema_element, const SlotDescriptor* slot_desc,
    RuntimeState* state) {
  // Following validation logic is only for non-complex types.
  if (slot_desc->type().IsComplexType()) return Status::OK();

  if (UNLIKELY(!IsSupportedType(slot_desc->type().type, schema_element))) {
    return Status(Substitute("Unsupported Parquet type in file '$0' metadata. Logical "
        "type: $1, physical type: $2. File may be corrupt.",
        filename, slot_desc->type().type, schema_element.type));
    }

  // Check the decimal scale in the file matches the metastore scale and precision.
  // We fail the query if the metadata makes it impossible for us to safely read
  // the file. If we don't require the metadata, we will fail the query if
  // abort_on_error is true, otherwise we will just log a warning.
  bool is_converted_type_decimal = schema_element.__isset.converted_type
      && schema_element.converted_type == parquet::ConvertedType::DECIMAL;
  if (slot_desc->type().type == TYPE_DECIMAL) {
    // We require that the scale and byte length be set.
    if (schema_element.type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      if (!schema_element.__isset.type_length) {
        return Status(Substitute("File '$0' column '$1' does not have type_length set.",
            filename, schema_element.name));
      }
      if (schema_element.type_length <= 0) {
        return Status(Substitute("File '$0' column '$1' has invalid type length: $2",
            filename, schema_element.name, schema_element.type_length));
      }
    }

    // We require that the precision be a positive value, and not larger than the
    // precision in table schema.
    if (!IsPrecisionSet(schema_element)) {
      ErrorMsg msg(TErrorCode::PARQUET_MISSING_PRECISION, filename, schema_element.name);
      return Status(msg);
    } else {
      int32_t precision = GetPrecision(schema_element);
      if (precision > slot_desc->type().precision || precision <= 0) {
        ErrorMsg msg(TErrorCode::PARQUET_WRONG_PRECISION, filename, schema_element.name,
            precision, slot_desc->type().precision);
        return Status(msg);
      }
      int32_t scale = GetScale(schema_element);
      if (scale < 0 || scale > precision) {
        return Status(
            Substitute("File '$0' column '$1' has invalid scale: $2. Precision is $3.",
                filename, schema_element.name, scale, precision));
      }
    }

    if (!is_converted_type_decimal) {
      // TODO: is this validation useful? It is not required at all to read the data and
      // might only serve to reject otherwise perfectly readable files.
      ErrorMsg msg(TErrorCode::PARQUET_BAD_CONVERTED_TYPE, filename,
          schema_element.name);
      RETURN_IF_ERROR(state->LogOrReturnError(msg));
    }
  } else if (IsScaleSet(schema_element) || IsPrecisionSet(schema_element)
      || is_converted_type_decimal) {
    ErrorMsg msg(TErrorCode::PARQUET_INCOMPATIBLE_DECIMAL, filename, schema_element.name,
        slot_desc->type().DebugString());
    RETURN_IF_ERROR(state->LogOrReturnError(msg));
  }
  return Status::OK();
}

parquet::Type::type ParquetMetadataUtils::ConvertInternalToParquetType(
    PrimitiveType type, TParquetTimestampType::type timestamp_type) {
  DCHECK_GE(type, 0);
  DCHECK_LT(type, INTERNAL_TO_PARQUET_TYPES_SIZE);
  if (type == TYPE_TIMESTAMP && timestamp_type != TParquetTimestampType::INT96_NANOS) {
    return parquet::Type::INT64;
  }
  return INTERNAL_TO_PARQUET_TYPES[type];
}

void ParquetMetadataUtils::FillSchemaElement(const ColumnType& col_type,
    bool string_utf8, TParquetTimestampType::type timestamp_type,
    parquet::SchemaElement* col_schema) {
  col_schema->__set_type(ConvertInternalToParquetType(col_type.type, timestamp_type));
  col_schema->__set_repetition_type(parquet::FieldRepetitionType::OPTIONAL);

  switch (col_type.type) {
    case TYPE_DECIMAL:
      SetDecimalConvertedAndLogicalType(col_type, col_schema);
      break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
      SetUtf8ConvertedAndLogicalType(col_schema);
      break;
    case TYPE_STRING:
      // By default STRING has no logical type, see IMPALA-5982.
      // VARCHAR and CHAR are always set to UTF8.
      if (string_utf8 && !col_type.IsBinaryType()) {
        SetUtf8ConvertedAndLogicalType(col_schema);
      }
      break;
    case TYPE_TINYINT:
      col_schema->__set_converted_type(parquet::ConvertedType::INT_8);
      SetIntLogicalType(8, col_schema);
      break;
    case TYPE_SMALLINT:
      col_schema->__set_converted_type(parquet::ConvertedType::INT_16);
      SetIntLogicalType(16, col_schema);
      break;
    case TYPE_INT:
      col_schema->__set_converted_type(parquet::ConvertedType::INT_32);
      SetIntLogicalType(32, col_schema);
      break;
    case TYPE_BIGINT:
      col_schema->__set_converted_type(parquet::ConvertedType::INT_64);
      SetIntLogicalType(64, col_schema);
      break;
    case TYPE_TIMESTAMP:
      SetTimestampLogicalType(timestamp_type, col_schema);
      break;
    case TYPE_DATE:
      SetDateLogicalType(col_schema);
      break;
    case TYPE_BOOLEAN:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      // boolean/float/double/INT96 encoded timestamp have no logical or converted types.
      break;
    default:
      DCHECK(false);
      break;
  }
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

Status ParquetSchemaResolver::CreateSchemaTree(
    const vector<parquet::SchemaElement>& schema, SchemaNode* node) const {
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
  bool is_root_schema = (*idx == 0);
  node->element = &schema[*idx];
  ++(*idx);

  if (node->element->num_children == 0) {
    // node is a leaf node, meaning it's materialized in the file and appears in
    // file_metadata_.row_groups.columns
    node->col_idx = *col_idx;
    ++(*col_idx);
  } else if (node->element->num_children > SCHEMA_NODE_CHILDREN_SANITY_LIMIT) {
    // Sanity-check the schema to avoid allocating absurdly large buffers below.
    return Status(Substitute("Schema in Parquet file '$0' has $1 children, more than "
        "limit of $2. File is likely corrupt", filename_, node->element->num_children,
        SCHEMA_NODE_CHILDREN_SANITY_LIMIT));
  } else if (node->element->num_children < 0) {
    return Status(Substitute("Corrupt Parquet file '$0': schema element has $1 children.",
        filename_, node->element->num_children));
  }

  // def_level_of_immediate_repeated_ancestor does not include this node, so set before
  // updating ira_def_level
  node->def_level_of_immediate_repeated_ancestor = ira_def_level;

  const auto repetition_type = Ubsan::EnumToInt(&node->element->repetition_type);
  if (repetition_type == parquet::FieldRepetitionType::OPTIONAL) {
    ++max_def_level;
  } else if (repetition_type == parquet::FieldRepetitionType::REPEATED &&
             !is_root_schema /*PARQUET-843*/) {
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
    const SchemaPath& path, SchemaNode** node, bool* pos_field,
    bool* missing_field) const {
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
      RETURN_IF_ERROR(ResolveStruct(**node, *col_type, path, i));
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
  if (fallback_schema_resolution_ == TSchemaResolutionStrategy::type::NAME) {
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
  } else if (fallback_schema_resolution_ ==
        TSchemaResolutionStrategy::type::FIELD_ID) {
    // Resolution by field id for Iceberg table.
    if (next_idx == 0) {
      // Resolve top-level table column by field id.
      DCHECK_LT(table_idx, tbl_desc_.col_descs().size());
      const int& field_id = tbl_desc_.col_descs()[table_idx].field_id();
      file_idx = FindChildWithFieldId(node, field_id);
    } else if (col_type->type == TYPE_STRUCT) {
      // Resolve struct field by field id.
      DCHECK_LT(table_idx, col_type->field_ids.size());
      const int& field_id = col_type->field_ids[table_idx];
      file_idx = FindChildWithFieldId(node, field_id);
    } else if (col_type->type == TYPE_ARRAY) {
      // Arrays have only one child in the file.
      DCHECK_EQ(table_idx, SchemaPathConstants::ARRAY_ITEM);
      file_idx = table_idx;
    } else {
      DCHECK_EQ(col_type->type, TYPE_MAP);
      DCHECK(table_idx == SchemaPathConstants::MAP_KEY ||
             table_idx == SchemaPathConstants::MAP_VALUE);
      // At this point we've found a MAP with a matching field id. It's safe to resolve
      // the child (key or value) by position.
      file_idx = table_idx;
    }
  } else {
    // Resolution by position.
    DCHECK_EQ(fallback_schema_resolution_,
        TSchemaResolutionStrategy::type::POSITION);
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
    auto entry = _TSchemaResolutionStrategy_VALUES_TO_NAMES.find(
        fallback_schema_resolution_);
    if (entry != _TSchemaResolutionStrategy_VALUES_TO_NAMES.end()) {
      schema_resolution_mode = entry->second;
    }
    VLOG_FILE << Substitute(
        "File '$0' does not contain path '$1' (resolving by $2)", filename_,
        PrintPath(tbl_desc_, path), schema_resolution_mode);
    *missing_field = true;
    return NULL;
  }

  if (UNLIKELY(file_idx == INVALID_ID)) {
    VLOG_FILE << Substitute("File '$0' is corrupted", filename_);
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

int ParquetSchemaResolver::FindChildWithFieldId(SchemaNode* node,
    const int& field_id) const {
  int idx;
  for (idx = 0; idx < node->children.size(); ++idx) {
    SchemaNode* child = &node->children[idx];

    int child_field_id = 0;

    if (LIKELY(child->element->__isset.field_id)) {
      child_field_id = child->element->field_id;
    } else {
      child_field_id = GetGeneratedFieldID(child);
    }
    if (child_field_id == field_id) return idx;
    if (UNLIKELY(child_field_id == INVALID_ID)) return INVALID_ID;
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
Status ParquetSchemaResolver::ResolveMap(const SchemaPath& path, int idx,
    SchemaNode** node, bool* missing_field) const {
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

Status ParquetSchemaResolver::ResolveStruct(const SchemaNode& node,
    const ColumnType& col_type, const SchemaPath& path, int idx) const {
  if (node.children.size() < 1) {
    ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename_,
        PrintSubPath(tbl_desc_, path, idx), "struct", node.DebugString());
    return Status::Expected(msg);
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
  if (!IsSupportedType(col_type.type, *node.element)) {
    ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename_,
        PrintSubPath(tbl_desc_, path, idx), col_type.DebugString(), node.DebugString());
    return Status::Expected(msg);
  }
  return Status::OK();
}

void ParquetSchemaResolver::GenerateFieldIDs() {
  std::stack<SchemaNode*> nodes;

  nodes.push(&schema_);

  int fieldID = 1;

  while (!nodes.empty()) {
    SchemaNode* current = nodes.top();
    nodes.pop();

    uint64_t size = current->children.size();

    for (uint64_t i = 0; i < size; i++) {
      auto retval = schema_node_to_field_id_.emplace(&current->children[i], fieldID++);

      // Emplace has to be successful, otherwise we visited the same node twice
      DCHECK(retval.second);

      // Push children in reverse order to the stack so they are processed in the original
      // order
      const uint64_t reverse_idx = size - i - 1;

      SchemaNode& current_child = current->children[reverse_idx];

      const parquet::ConvertedType::type child_type =
          current_child.element->converted_type;

      if (child_type == parquet::ConvertedType::type::LIST
          || child_type == parquet::ConvertedType::type::MAP) {
        // Skip middle level
        DCHECK(current_child.children.size() == 1);

        nodes.push(&current_child.children[0]);
      } else {
        nodes.push(&current_child);
      }
    }
  }
}

int ParquetSchemaResolver::GetGeneratedFieldID(SchemaNode* node) const {
  auto it = schema_node_to_field_id_.find(node);

  // First column has field ID, this one does not, file is corrupted
  if (UNLIKELY(it == schema_node_to_field_id_.end())) return INVALID_ID;

  return it->second;
}
}
