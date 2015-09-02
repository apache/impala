// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/hdfs-avro-scanner.h"

#include <avro/errors.h>
#include <avro/legacy.h>
#include <boost/foreach.hpp>
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exec/scanner-context.inline.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "util/codec.h"
#include "util/decompress.h"
#include "util/runtime-profile.h"

#include "common/names.h"

// Note: the Avro C++ library uses exceptions for error handling. Any Avro
// function that may throw an exception must be placed in a try/catch block.
using namespace impala;
using namespace llvm;
using namespace strings;

const char* HdfsAvroScanner::LLVM_CLASS_NAME = "class.impala::HdfsAvroScanner";

const uint8_t HdfsAvroScanner::AVRO_VERSION_HEADER[4] = {'O', 'b', 'j', 1};

const string HdfsAvroScanner::AVRO_SCHEMA_KEY("avro.schema");
const string HdfsAvroScanner::AVRO_CODEC_KEY("avro.codec");
const string HdfsAvroScanner::AVRO_NULL_CODEC("null");
const string HdfsAvroScanner::AVRO_SNAPPY_CODEC("snappy");
const string HdfsAvroScanner::AVRO_DEFLATE_CODEC("deflate");

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

HdfsAvroScanner::HdfsAvroScanner(HdfsScanNode* scan_node, RuntimeState* state)
  : BaseSequenceScanner(scan_node, state),
    avro_header_(NULL),
    codegend_decode_avro_data_(NULL) {
}

Function* HdfsAvroScanner::Codegen(HdfsScanNode* node,
                                   const vector<ExprContext*>& conjunct_ctxs) {
  if (!node->runtime_state()->codegen_enabled()) return NULL;
  LlvmCodeGen* codegen;
  if (!node->runtime_state()->GetCodegen(&codegen).ok()) return NULL;
  Function* materialize_tuple_fn = CodegenMaterializeTuple(node, codegen);
  if (materialize_tuple_fn == NULL) return NULL;
  return CodegenDecodeAvroData(node->runtime_state(), materialize_tuple_fn, conjunct_ctxs);
}

BaseSequenceScanner::FileHeader* HdfsAvroScanner::AllocateFileHeader() {
  AvroFileHeader* header = new AvroFileHeader();
  header->template_tuple = template_tuple_;
  return header;
}

Status HdfsAvroScanner::ReadFileHeader() {
  avro_header_ = reinterpret_cast<AvroFileHeader*>(header_);

  // Check version header
  uint8_t* header;
  RETURN_IF_FALSE(stream_->ReadBytes(
      sizeof(AVRO_VERSION_HEADER), &header, &parse_status_));
  if (memcmp(header, AVRO_VERSION_HEADER, sizeof(AVRO_VERSION_HEADER))) {
    stringstream ss;
    ss << "Invalid AVRO_VERSION_HEADER: '"
       << ReadWriteUtil::HexDump(header, sizeof(AVRO_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  // Decode relevant metadata (encoded as Avro map)
  RETURN_IF_ERROR(ParseMetadata());

  // Read file sync marker
  uint8_t* sync;
  RETURN_IF_FALSE(stream_->ReadBytes(SYNC_HASH_SIZE, &sync, &parse_status_));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);

  header_->header_size = stream_->total_bytes_returned() - SYNC_HASH_SIZE;
  return Status::OK();
}

Status HdfsAvroScanner::ParseMetadata() {
  header_->is_compressed = false;
  header_->compression_type = THdfsCompression::NONE;

  int64_t num_entries;
  RETURN_IF_FALSE(stream_->ReadZLong(&num_entries, &parse_status_));
  if (num_entries < 1) return Status("File header metadata has no data");

  while (num_entries != 0) {
    DCHECK_GT(num_entries, 0);
    for (int i = 0; i < num_entries; ++i) {
      // Decode Avro string-type key
      string key;
      uint8_t* key_buf;
      int64_t key_len;
      RETURN_IF_FALSE(stream_->ReadZLong(&key_len, &parse_status_));
      DCHECK_GE(key_len, 0);
      RETURN_IF_FALSE(stream_->ReadBytes(key_len, &key_buf, &parse_status_));
      key = string(reinterpret_cast<char*>(key_buf), key_len);

      // Decode Avro bytes-type value
      uint8_t* value;
      int64_t value_len;
      RETURN_IF_FALSE(stream_->ReadZLong(&value_len, &parse_status_));
      DCHECK_GE(value_len, 0);
      RETURN_IF_FALSE(stream_->ReadBytes(value_len, &value, &parse_status_));

      if (key == AVRO_SCHEMA_KEY) {
        avro_schema_t raw_file_schema;
        int error = avro_schema_from_json_length(
            reinterpret_cast<char*>(value), value_len, &raw_file_schema);
        if (error != 0) {
          stringstream ss;
          ss << "Failed to parse file schema: " << avro_strerror();
          return Status(ss.str());
        }
        AvroSchemaElement* file_schema = avro_header_->schema.get();
        RETURN_IF_ERROR(AvroSchemaElement::ConvertSchema(raw_file_schema, file_schema));

        RETURN_IF_ERROR(ResolveSchemas(scan_node_->avro_schema(), file_schema));

        // We currently codegen a function only for the table schema. If this file's
        // schema is different from the table schema, don't use the codegen'd function and
        // use the interpreted path instead.
        avro_header_->use_codegend_decode_avro_data = avro_schema_equal(
            scan_node_->avro_schema().schema, file_schema->schema);

      } else if (key == AVRO_CODEC_KEY) {
        string avro_codec(reinterpret_cast<char*>(value), value_len);
        if (avro_codec != AVRO_NULL_CODEC) {
          header_->is_compressed = true;
          // This scanner doesn't use header_->codec (Avro doesn't use the
          // Hadoop codec strings), but fill it in for logging
          header_->codec = avro_codec;
          if (avro_codec == AVRO_SNAPPY_CODEC) {
            header_->compression_type = THdfsCompression::SNAPPY;
          } else if (avro_codec == AVRO_DEFLATE_CODEC) {
            header_->compression_type = THdfsCompression::DEFLATE;
          } else {
            return Status("Unknown Avro compression codec: " + avro_codec);
          }
        }
      } else {
        VLOG_ROW << "Skipping metadata entry: " << key;
      }
    }
    RETURN_IF_FALSE(stream_->ReadZLong(&num_entries, &parse_status_));
  }

  VLOG_FILE << stream_->filename() << ": "
            << (header_->is_compressed ?  "compressed" : "not compressed");
  if (header_->is_compressed) VLOG_FILE << header_->codec;
  if (avro_header_->schema->children.empty()) {
    return Status("Schema not found in file header metadata");
  }
  return Status::OK();
}

// Schema resolution is performed per materialized slot (meaning we don't perform schema
// resolution for non-materialized columns). For each slot, we traverse the table schema
// using the column path (i.e., the traversal is by ordinal). We simultaneously traverse
// the file schema using the table schema's field names. The final field should exist in
// both schemas and be promotable to the slot type. If the file schema is missing a field,
// we check for a default value in the table schema and use that instead.
// TODO: test unresolvable schemas
// TODO: improve error messages
Status HdfsAvroScanner::ResolveSchemas(const AvroSchemaElement& table_root,
                                       AvroSchemaElement* file_root) {
  if (table_root.schema->type != AVRO_RECORD) return Status("Table schema is not a record");
  if (file_root->schema->type != AVRO_RECORD) return Status("File schema is not a record");

  // Associate each slot descriptor with a field in the file schema, or fill in the
  // template tuple with a default value from the table schema.
  BOOST_FOREACH(SlotDescriptor* slot_desc, scan_node_->materialized_slots()) {
    // Traverse the column path, simultaneously traversing the table schema by ordinal and
    // the file schema by field name from the table schema.
    const SchemaPath& path = slot_desc->col_path();
    const AvroSchemaElement* table_record = &table_root;
    AvroSchemaElement* file_record = file_root;

    for (int i = 0; i < path.size(); ++i) {
      int table_field_idx = i > 0 ? path[i] : path[i] - scan_node_->num_partition_keys();
      int num_fields = table_record->children.size();
      if (table_field_idx >= num_fields) {
        // TODO: add path to error message (and elsewhere)
        return Status(TErrorCode::AVRO_MISSING_FIELD, table_field_idx, num_fields);
      }

      const char* field_name =
          avro_schema_record_field_name(table_record->schema, table_field_idx);
      int file_field_idx =
          avro_schema_record_field_get_index(file_record->schema, field_name);

      if (file_field_idx < 0) {
        // This field doesn't exist in the file schema. Check if there is a default value.
        avro_datum_t default_value =
            avro_schema_record_field_default(table_record->schema, table_field_idx);
        if (default_value == NULL) {
          return Status(TErrorCode::AVRO_MISSING_DEFAULT, field_name);
        }
        RETURN_IF_ERROR(WriteDefaultValue(slot_desc, default_value, field_name));
        DCHECK_EQ(i, path.size() - 1) <<
            "WriteDefaultValue() doesn't support default records yet, should have failed";
        continue;
      }

      const AvroSchemaElement& table_field = table_record->children[table_field_idx];
      AvroSchemaElement& file_field = file_record->children[file_field_idx];
      RETURN_IF_ERROR(VerifyTypesMatch(table_field, file_field, field_name));

      if (i != path.size() - 1) {
        // All but the last index in 'path' should be a record field
        if (table_record->schema->type != AVRO_RECORD) {
          return Status(TErrorCode::AVRO_NOT_A_RECORD, field_name);
        } else {
          DCHECK_EQ(file_record->schema->type, AVRO_RECORD);
        }
        table_record = &table_field;
        file_record = &file_field;
      } else {
        // This should be the field corresponding to 'slot_desc'. Check that slot_desc can
        // be resolved to the table's Avro schema.
        RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, table_field.schema));
        file_field.slot_desc = slot_desc;
      }
    }
  }
  return Status::OK();
}

Status HdfsAvroScanner::WriteDefaultValue(
    SlotDescriptor* slot_desc, avro_datum_t default_value, const char* field_name) {
  if (avro_header_->template_tuple == NULL) {
    avro_header_->template_tuple = template_tuple_ != NULL ?
        template_tuple_ : scan_node_->InitEmptyTemplateTuple(*scan_node_->tuple_desc());
  }
  switch (default_value->type) {
    case AVRO_BOOLEAN: {
      // We don't call VerifyTypesMatch() above the switch statement so we don't want to
      // call it in the default case (since we VerifyTypesMatch() can't handle every type
      // either, and we want to return the correct error message).
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      int8_t v;
      if (avro_boolean_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
      break;
    }
    case AVRO_INT32: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      int32_t v;
      if (avro_int32_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
      break;
    }
    case AVRO_INT64: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      int64_t v;
      if (avro_int64_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
      break;
    }
    case AVRO_FLOAT: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      float v;
      if (avro_float_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
      break;
    }
    case AVRO_DOUBLE: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      double v;
      if (avro_double_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
      break;
    }
    case AVRO_STRING:
    case AVRO_BYTES: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      // Mempools aren't thread safe so make a local one and transfer it
      // to the scan node pool.
      MemPool pool(scan_node_->mem_tracker());
      char* v;
      if (avro_string_get(default_value, &v)) DCHECK(false);
      StringValue sv(v);
      RawValue::Write(&sv, avro_header_->template_tuple, slot_desc, &pool);
      scan_node_->TransferToScanNodePool(&pool);
      break;
    }
    case AVRO_NULL:
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      avro_header_->template_tuple->SetNull(slot_desc->null_indicator_offset());
      break;
    default:
      return Status(TErrorCode::AVRO_UNSUPPORTED_DEFAULT_VALUE, field_name,
          avro_type_name(default_value->type));
  }
  return Status::OK();
}

Status HdfsAvroScanner::VerifyTypesMatch(const AvroSchemaElement& table_schema,
    const AvroSchemaElement& file_schema, const string& field_name) {
  if (!table_schema.nullable() && file_schema.nullable()) {
    // Use ErrorMsg because corresponding Status ctor is ambiguous
    ErrorMsg msg(TErrorCode::AVRO_NULLABILITY_MISMATCH, field_name);
    return Status(msg);
  }

  if (file_schema.schema->type == AVRO_NULL) {
    if (table_schema.schema->type == AVRO_NULL || table_schema.nullable()) {
      return Status::OK();
    } else {
      return Status(TErrorCode::AVRO_SCHEMA_RESOLUTION_ERROR, field_name,
          avro_type_name(table_schema.schema->type),
          avro_type_name(file_schema.schema->type));
    }
  }

  // Can't convert records to ColumnTypes, check here instead of below
  // TODO: update if/when we have TYPE_STRUCT primitive type
  if ((table_schema.schema->type == AVRO_RECORD) ^
      (file_schema.schema->type == AVRO_RECORD)) {
    return Status(TErrorCode::AVRO_SCHEMA_RESOLUTION_ERROR, field_name,
        avro_type_name(table_schema.schema->type),
        avro_type_name(file_schema.schema->type));
  } else if (table_schema.schema->type == AVRO_RECORD) {
    DCHECK_EQ(file_schema.schema->type, AVRO_RECORD);
    return Status::OK();
  }

  ColumnType reader_type = AvroSchemaToColumnType(table_schema.schema);
  ColumnType writer_type = AvroSchemaToColumnType(file_schema.schema);
  bool match = VerifyTypesMatch(reader_type, writer_type);
  if (match) return Status::OK();
  return Status(TErrorCode::AVRO_SCHEMA_RESOLUTION_ERROR, field_name,
      avro_type_name(table_schema.schema->type), avro_type_name(file_schema.schema->type));
}

Status HdfsAvroScanner::VerifyTypesMatch(SlotDescriptor* slot_desc, avro_obj_t* schema) {
  // TODO: make this work for nested fields
  const string& col_name =
      scan_node_->hdfs_table()->col_descs()[slot_desc->col_pos()].name();

  // All Impala types are nullable
  if (schema->type == AVRO_NULL) return Status::OK();

  // Can't convert records to ColumnTypes, check here instead of below
  // TODO: update if/when we have TYPE_STRUCT primitive type
  if (schema->type == AVRO_RECORD) {
    return Status(TErrorCode::AVRO_SCHEMA_METADATA_MISMATCH, col_name,
      slot_desc->type().DebugString(), avro_type_name(schema->type));
  }

  bool match = VerifyTypesMatch(slot_desc->type(), AvroSchemaToColumnType(schema));
  if (match) return Status::OK();
  return Status(TErrorCode::AVRO_SCHEMA_METADATA_MISMATCH, col_name,
      slot_desc->type().DebugString(), avro_type_name(schema->type));
}

bool HdfsAvroScanner::VerifyTypesMatch(
    const ColumnType& reader_type, const ColumnType& writer_type) {
  switch (writer_type.type) {
    case TYPE_DECIMAL:
      if (reader_type.type != TYPE_DECIMAL) return false;
      if (reader_type.scale != writer_type.scale) return false;
      if (reader_type.precision != writer_type.precision) return false;
      return true;
    case TYPE_STRING: return reader_type.IsStringType();
    case TYPE_INT:
      switch(reader_type.type) {
        case TYPE_INT:
        // Type promotion
        case TYPE_BIGINT:
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          return true;
        default:
          return false;
      }
    case TYPE_BIGINT:
      switch(reader_type.type) {
        case TYPE_BIGINT:
        // Type promotion
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          return true;
        default:
          return false;
      }
    case TYPE_FLOAT:
      switch(reader_type.type) {
        case TYPE_FLOAT:
        // Type promotion
        case TYPE_DOUBLE:
          return true;
        default:
          return false;
      }
    case TYPE_DOUBLE: return reader_type.type == TYPE_DOUBLE;
    case TYPE_BOOLEAN: return reader_type.type == TYPE_BOOLEAN;
    default:
      DCHECK(false) << "NYI: " << writer_type.DebugString();
      return false;
  }
}

Status HdfsAvroScanner::InitNewRange() {
  DCHECK(header_ != NULL);
  only_parsing_header_ = false;
  avro_header_ = reinterpret_cast<AvroFileHeader*>(header_);
  template_tuple_ = avro_header_->template_tuple;
  if (header_->is_compressed) {
    RETURN_IF_ERROR(UpdateDecompressor(header_->compression_type));
  }

  if (avro_header_->use_codegend_decode_avro_data) {
    codegend_decode_avro_data_ = reinterpret_cast<DecodeAvroDataFn>(
        scan_node_->GetCodegenFn(THdfsFileFormat::AVRO));
  }
  if (codegend_decode_avro_data_ == NULL) {
    scan_node_->IncNumScannersCodegenDisabled();
  } else {
    VLOG(2) << "HdfsAvroScanner (node_id=" << scan_node_->id()
            << ") using llvm codegend functions.";
    scan_node_->IncNumScannersCodegenEnabled();
  }

  return Status::OK();
}

Status HdfsAvroScanner::ProcessRange() {
  while (!finished()) {
    int64_t num_records;
    uint8_t* compressed_data;
    int64_t compressed_size;
    uint8_t* data;

    // Read new data block
    RETURN_IF_FALSE(
        stream_->ReadZLong(&num_records, &parse_status_));
    DCHECK_GE(num_records, 0);
    RETURN_IF_FALSE(stream_->ReadZLong(&compressed_size, &parse_status_));
    DCHECK_GE(compressed_size, 0);
    RETURN_IF_FALSE(stream_->ReadBytes(
        compressed_size, &compressed_data, &parse_status_));

    if (header_->is_compressed) {
      if (header_->compression_type == THdfsCompression::SNAPPY) {
        // Snappy-compressed data block includes trailing 4-byte checksum,
        // decompressor_ doesn't expect this
        compressed_size -= SnappyDecompressor::TRAILING_CHECKSUM_LEN;
      }
      int64_t size;
      SCOPED_TIMER(decompress_timer_);
      RETURN_IF_ERROR(decompressor_->ProcessBlock(false, compressed_size, compressed_data,
                                                  &size, &data));
      VLOG_FILE << "Decompressed " << compressed_size << " to " << size;
    } else {
      data = compressed_data;
    }

    // Process block data
    while (num_records > 0) {
      SCOPED_TIMER(scan_node_->materialize_tuple_timer());

      MemPool* pool;
      Tuple* tuple;
      TupleRow* tuple_row;
      int max_tuples = GetMemory(&pool, &tuple, &tuple_row);
      max_tuples = min(num_records, static_cast<int64_t>(max_tuples));
      int num_to_commit;
      if (scan_node_->materialized_slots().empty()) {
        // No slots to materialize (e.g. count(*)), no need to decode data
        num_to_commit = WriteEmptyTuples(context_, tuple_row, max_tuples);
      } else {
        if (codegend_decode_avro_data_ != NULL) {
          num_to_commit = codegend_decode_avro_data_(
              this, max_tuples, pool, &data, tuple, tuple_row);
        } else {
          num_to_commit = DecodeAvroData(max_tuples, pool, &data, tuple, tuple_row);
        }
      }
      RETURN_IF_ERROR(CommitRows(num_to_commit));
      num_records -= max_tuples;
      COUNTER_ADD(scan_node_->rows_read_counter(), max_tuples);

      if (scan_node_->ReachedLimit()) return Status::OK();
    }

    if (decompressor_.get() != NULL && !decompressor_->reuse_output_buffer()) {
      AttachPool(data_buffer_pool_.get(), true);
    }
    RETURN_IF_ERROR(ReadSync());
  }

  return Status::OK();
}

void HdfsAvroScanner::MaterializeTuple(const AvroSchemaElement& record_schema,
    MemPool* pool, uint8_t** data, Tuple* tuple) {
  DCHECK_EQ(record_schema.schema->type, AVRO_RECORD);
  BOOST_FOREACH(const AvroSchemaElement& element, record_schema.children) {
    const SlotDescriptor* slot_desc = element.slot_desc;
    bool write_slot = false;
    void* slot = NULL;
    PrimitiveType slot_type = INVALID_TYPE;
    if (slot_desc != NULL) {
      write_slot = true;
      slot = tuple->GetSlot(slot_desc->tuple_offset());
      slot_type = slot_desc->type().type;
    }

    avro_type_t type = element.schema->type;
    if (element.nullable() && !ReadUnionType(element.null_union_position, data)) {
      type = AVRO_NULL;
    }

    switch (type) {
      case AVRO_NULL:
        if (slot_desc != NULL) tuple->SetNull(slot_desc->null_indicator_offset());
        break;
      case AVRO_BOOLEAN:
        ReadAvroBoolean(slot_type, data, write_slot, slot, pool);
        break;
      case AVRO_INT32:
        ReadAvroInt32(slot_type, data, write_slot, slot, pool);
        break;
      case AVRO_INT64:
        ReadAvroInt64(slot_type, data, write_slot, slot, pool);
        break;
      case AVRO_FLOAT:
        ReadAvroFloat(slot_type, data, write_slot, slot, pool);
        break;
      case AVRO_DOUBLE:
        ReadAvroDouble(slot_type, data, write_slot, slot, pool);
        break;
      case AVRO_STRING:
      case AVRO_BYTES:
        if (slot_desc != NULL && slot_desc->type().type == TYPE_VARCHAR) {
          ReadAvroVarchar(slot_type, slot_desc->type().len, data, write_slot, slot, pool);
        } else if (slot_desc != NULL && slot_desc->type().type == TYPE_CHAR) {
          ReadAvroChar(slot_type, slot_desc->type().len, data, write_slot, slot, pool);
        } else {
          ReadAvroString(slot_type, data, write_slot, slot, pool);
        }
        break;
      case AVRO_DECIMAL: {
        int slot_byte_size = 0;
        if (slot_desc != NULL) {
          DCHECK_EQ(slot_type, TYPE_DECIMAL);
          slot_byte_size = slot_desc->type().GetByteSize();
        }
        ReadAvroDecimal(slot_byte_size, data, write_slot, slot, pool);
        break;
      }
      case AVRO_RECORD:
        MaterializeTuple(element, pool, data, tuple);
        break;
      default:
        DCHECK(false) << "Unsupported SchemaElement: " << type;
    }
  }
}

// This function produces a codegen'd function equivalent to MaterializeTuple() but
// optimized for the table schema. Via helper functions CodegenReadRecord() and
// CodegenReadScalar(), it eliminates the conditionals necessary when interpreting the
// type of each element in the schema, instead generating code to handle each element in
// the schema. Example output:
//
// define void @MaterializeTuple(%"class.impala::HdfsAvroScanner"* %this,
//     %"class.impala::MemPool"* %pool, i8** %data, %"class.impala::Tuple"* %tuple) {
// entry:
//   %tuple_ptr = bitcast %"class.impala::Tuple"* %tuple to { i8, i32 }*
//   %is_not_null = call i1 @_ZN6impala15HdfsAvroScanner13ReadUnionTypeEiPPh(
//       %"class.impala::HdfsAvroScanner"* %this, i32 1, i8** %data)
//   br i1 %is_not_null, label %read_field, label %null_field
//
// read_field:                                       ; preds = %entry
//   %slot = getelementptr inbounds { i8, i32 }* %tuple_ptr, i32 0, i32 1
//   %opaque_slot = bitcast i32* %slot to i8*
//   call void
//    @_ZN6impala15HdfsAvroScanner13ReadAvroInt32ENS_13PrimitiveTypeEPPhPvPNS_7MemPoolE(
//        %"class.impala::HdfsAvroScanner"* %this, i32 5, i8** %data,
//        i8* %opaque_slot, %"class.impala::MemPool"* %pool)
//   br label %endif
//
// null_field:                                       ; preds = %entry
//   call void @SetNull({ i8, i32 }* %tuple_ptr)
//   br label %endif
//
// endif:                                            ; preds = %null_field, %read_field
//   %is_not_null4 = call i1 @_ZN6impala15HdfsAvroScanner13ReadUnionTypeEiPPh(
//       %"class.impala::HdfsAvroScanner"* %this, i32 1, i8** %data)
//   br i1 %is_not_null4, label %read_field1, label %null_field2
//
// read_field1:                                      ; preds = %endif
//   call void
//    @_ZN6impala15HdfsAvroScanner15ReadAvroBooleanENS_13PrimitiveTypeEPPhPvPNS_7MemPoolE(
//        %"class.impala::HdfsAvroScanner"* %this, i32 0, i8** %data,
//        i8* null, %"class.impala::MemPool"* %pool)
//   br label %endif3
//
// null_field2:                                      ; preds = %endif
//   br label %endif3
//
// endif3:                                           ; preds = %null_field2, %read_field1
//   ret void
// }
Function* HdfsAvroScanner::CodegenMaterializeTuple(
    HdfsScanNode* node, LlvmCodeGen* codegen) {
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Type* this_type = codegen->GetType(HdfsAvroScanner::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  TupleDescriptor* tuple_desc = const_cast<TupleDescriptor*>(node->tuple_desc());
  StructType* tuple_type = tuple_desc->GenerateLlvmStruct(codegen);
  if (tuple_type == NULL) return NULL; // Could not generate tuple struct
  Type* tuple_ptr_type = PointerType::get(tuple_type, 0);

  Type* tuple_opaque_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  PointerType* tuple_opaque_ptr_type = PointerType::get(tuple_opaque_type, 0);

  Type* data_ptr_type = PointerType::get(codegen->ptr_type(), 0); // char**
  Type* mempool_type = PointerType::get(codegen->GetType(MemPool::LLVM_CLASS_NAME), 0);
  Type* schema_element_type = codegen->GetPtrType(AvroSchemaElement::LLVM_CLASS_NAME);

  LlvmCodeGen::FnPrototype prototype(codegen, "MaterializeTuple", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("record_schema", schema_element_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("pool", mempool_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("data", data_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_opaque_ptr_type));
  Value* args[5];
  Function* fn = prototype.GeneratePrototype(&builder, args);

  Value* this_val = args[0];
  // Value* record_schema_val = args[1]; // don't need this
  Value* pool_val = args[2];
  Value* data_val = args[3];
  Value* opaque_tuple_val = args[4];

  Value* tuple_val = builder.CreateBitCast(opaque_tuple_val, tuple_ptr_type, "tuple_ptr");

  Status status = CodegenReadRecord(
      SchemaPath(), node->avro_schema(), node, codegen, &builder, fn, NULL, this_val,
      pool_val, tuple_val, data_val);
  if (!status.ok()) {
    VLOG_QUERY << status.GetDetail();
    fn->eraseFromParent();
    return NULL;
  }

  builder.SetInsertPoint(&fn->back());
  builder.CreateRetVoid();
  return codegen->FinalizeFunction(fn);
}

Status HdfsAvroScanner::CodegenReadRecord(
    const SchemaPath& path, const AvroSchemaElement& record, HdfsScanNode* node,
    LlvmCodeGen* codegen, void* void_builder, Function* fn, BasicBlock* insert_before,
    Value* this_val, Value* pool_val, Value* tuple_val, Value* data_val) {
  DCHECK_EQ(record.schema->type, AVRO_RECORD);
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder* builder =
      reinterpret_cast<LlvmCodeGen::LlvmBuilder*>(void_builder);

  // Codegen logic for parsing each field and, if necessary, populating a slot with the
  // result.
  for (int i = 0; i < record.children.size(); ++i) {
    const AvroSchemaElement* field = &record.children[i];
    int col_idx = i;
    // If we're about to process the table-level columns, account for the partition keys
    // when constructing 'path'
    if (path.empty()) col_idx += node->num_partition_keys();
    SchemaPath new_path = path;
    new_path.push_back(col_idx);
    int slot_idx = node->GetMaterializedSlotIdx(new_path);
    SlotDescriptor* slot_desc = (slot_idx == HdfsScanNode::SKIP_COLUMN) ?
                                NULL : node->materialized_slots()[slot_idx];

    // Block that calls appropriate Read<Type> function
    BasicBlock* read_field_block =
        BasicBlock::Create(context, "read_field", fn, insert_before);

    // Block that handles a NULL value. We fill this in below if the field is nullable,
    // otherwise we leave this block NULL.
    BasicBlock* null_block = NULL;

    // This is where we should end up after we're finished processing this field. Used to
    // put the builder in the right place for the next field.
    BasicBlock* end_field_block =
        BasicBlock::Create(context, "end_field", fn, insert_before);

    if (field->nullable()) {
      // Field could be null. Create conditional branch based on ReadUnionType result.
      null_block = BasicBlock::Create(context, "null_field", fn, end_field_block);
      Function* read_union_fn =
          codegen->GetFunction(IRFunction::READ_UNION_TYPE);
      Value* null_union_pos_val =
          codegen->GetIntConstant(TYPE_INT, field->null_union_position);
      Value* is_not_null_val = builder->CreateCall3(
          read_union_fn, this_val, null_union_pos_val, data_val, "is_not_null");
      builder->CreateCondBr(is_not_null_val, read_field_block, null_block);

      // Write null field IR
      builder->SetInsertPoint(null_block);
      if (slot_idx != HdfsScanNode::SKIP_COLUMN) {
        StructType* tuple_type =
            cast<StructType>(tuple_val->getType()->getPointerElementType());
        Function* set_null_fn = slot_desc->CodegenUpdateNull(codegen, tuple_type, true);
        DCHECK(set_null_fn != NULL);
        builder->CreateCall(set_null_fn, tuple_val);
      }
      // LLVM requires all basic blocks to end with a terminating instruction
      builder->CreateBr(end_field_block);
    } else {
      // Field is never null, read field unconditionally.
      builder->CreateBr(read_field_block);
    }

    // Write read_field_block IR
    builder->SetInsertPoint(read_field_block);
    if (field->schema->type == AVRO_RECORD) {
      BasicBlock* insert_before_block =
          (null_block != NULL) ? null_block : end_field_block;
      RETURN_IF_ERROR(CodegenReadRecord(new_path, *field, node, codegen, builder, fn,
          insert_before_block, this_val, pool_val, tuple_val, data_val));
    } else {
      RETURN_IF_ERROR(CodegenReadScalar(
          *field, slot_desc, codegen, builder, this_val, pool_val, tuple_val, data_val));
    }
    builder->CreateBr(end_field_block);

    // Set insertion point for next field
    builder->SetInsertPoint(end_field_block);
  }
  return Status::OK();
}

Status HdfsAvroScanner::CodegenReadScalar(const AvroSchemaElement& element,
    SlotDescriptor* slot_desc, LlvmCodeGen* codegen, void* void_builder, Value* this_val,
    Value* pool_val, Value* tuple_val, Value* data_val) {
  LlvmCodeGen::LlvmBuilder* builder =
      reinterpret_cast<LlvmCodeGen::LlvmBuilder*>(void_builder);
  Function* read_field_fn;
  switch (element.schema->type) {
    case AVRO_BOOLEAN:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_BOOLEAN);
      break;
    case AVRO_INT32:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_INT32);
      break;
    case AVRO_INT64:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_INT64);
      break;
    case AVRO_FLOAT:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_FLOAT);
      break;
    case AVRO_DOUBLE:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_DOUBLE);
      break;
    case AVRO_STRING:
    case AVRO_BYTES:
      if (slot_desc != NULL && slot_desc->type().type == TYPE_VARCHAR) {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_VARCHAR);
      } else {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_STRING);
      }
      break;
    default:
      return Status(Substitute(
          "Failed to codegen MaterializeTuple() due to unsupported type: $0",
          element.schema->type));
  }

  // Call appropriate ReadAvro<Type> function
  Value* write_slot_val = builder->getFalse();
  Value* slot_type_val = builder->getInt32(0);
  Value* opaque_slot_val = codegen->null_ptr_value();
  if (slot_desc != NULL) {
    // Field corresponds to a materialized column, fill in relevant arguments
    write_slot_val = builder->getTrue();
    if (slot_desc->type().type == TYPE_DECIMAL) {
      // ReadAvroDecimal() takes slot byte size instead of slot type
      slot_type_val = builder->getInt32(slot_desc->type().GetByteSize());
    } else {
      slot_type_val = builder->getInt32(slot_desc->type().type);
    }
    Value* slot_val =
        builder->CreateStructGEP(tuple_val, slot_desc->field_idx(), "slot");
    opaque_slot_val =
        builder->CreateBitCast(slot_val, codegen->ptr_type(), "opaque_slot");
  }

  // NOTE: ReadAvroVarchar/Char has different signature than rest of read functions
  if ((slot_desc != NULL) &&
      (slot_desc->type().type == TYPE_VARCHAR || slot_desc->type().type == TYPE_CHAR)) {
    // Need to pass an extra argument (the length) to the codegen function
    Value* fixed_len = builder->getInt32(slot_desc->type().len);
    Value* read_field_args[] = {this_val, slot_type_val, fixed_len, data_val,
                                write_slot_val, opaque_slot_val, pool_val};
    builder->CreateCall(read_field_fn, read_field_args);
  } else {
    Value* read_field_args[] =
        {this_val, slot_type_val, data_val, write_slot_val, opaque_slot_val, pool_val};
    builder->CreateCall(read_field_fn, read_field_args);
  }
  return Status::OK();
}

Function* HdfsAvroScanner::CodegenDecodeAvroData(RuntimeState* state,
    Function* materialize_tuple_fn, const vector<ExprContext*>& conjunct_ctxs) {
  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return NULL;
  SCOPED_TIMER(codegen->codegen_timer());
  DCHECK(materialize_tuple_fn != NULL);

  Function* decode_avro_data_fn = codegen->GetFunction(IRFunction::DECODE_AVRO_DATA);
  int replaced = 0;
  decode_avro_data_fn = codegen->ReplaceCallSites(decode_avro_data_fn, false,
      materialize_tuple_fn, "MaterializeTuple", &replaced);
  DCHECK_EQ(replaced, 1);

  Function* eval_conjuncts_fn = ExecNode::CodegenEvalConjuncts(state, conjunct_ctxs);
  decode_avro_data_fn = codegen->ReplaceCallSites(decode_avro_data_fn, false,
      eval_conjuncts_fn, "EvalConjuncts", &replaced);
  DCHECK_EQ(replaced, 1);
  decode_avro_data_fn->setName("DecodeAvroData");

  decode_avro_data_fn = codegen->OptimizeFunctionWithExprs(decode_avro_data_fn);
  DCHECK(decode_avro_data_fn != NULL);
  return decode_avro_data_fn;
}
