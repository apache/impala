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
#include <avro/schema.h>
#include <boost/foreach.hpp>

#include "codegen/llvm-codegen.h"
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exec/scanner-context.inline.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "util/codec.h"
#include "util/decompress.h"
#include "util/runtime-profile.h"

// Note: the Avro C++ library uses exceptions for error handling. Any Avro
// function that may throw an exception must be placed in a try/catch block.

using namespace impala;
using namespace llvm;
using namespace std;

const char* HdfsAvroScanner::LLVM_CLASS_NAME = "class.impala::HdfsAvroScanner";
const uint8_t HdfsAvroScanner::AVRO_VERSION_HEADER[4] = {'O', 'b', 'j', 1};

const string HdfsAvroScanner::AVRO_SCHEMA_KEY("avro.schema");
const string HdfsAvroScanner::AVRO_CODEC_KEY("avro.codec");
const string HdfsAvroScanner::AVRO_NULL_CODEC("null");
const string HdfsAvroScanner::AVRO_SNAPPY_CODEC("snappy");
const string HdfsAvroScanner::AVRO_DEFLATE_CODEC("deflate");

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

HdfsAvroScanner::ScopedAvroSchemaT::ScopedAvroSchemaT(const ScopedAvroSchemaT& other) {
  schema = other.schema;
  avro_schema_incref(schema);
}

HdfsAvroScanner::ScopedAvroSchemaT::~ScopedAvroSchemaT() {
  // avro_schema_decref can handle NULL
  avro_schema_decref(schema);
}

HdfsAvroScanner::ScopedAvroSchemaT& HdfsAvroScanner::ScopedAvroSchemaT::operator=(
    const avro_schema_t& s) {
  if (LIKELY(s != schema)) {
    avro_schema_decref(schema);
    schema = s;
  }
  return *this;
}

HdfsAvroScanner::ScopedAvroSchemaT& HdfsAvroScanner::ScopedAvroSchemaT::operator=(
    const ScopedAvroSchemaT& other) {
  if (this == &other) return *this;
  avro_schema_decref(schema);
  schema = other.schema;
  avro_schema_incref(schema);
  return *this;
}
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
  return Status::OK;
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
        ScopedAvroSchemaT file_schema(raw_file_schema);
        if (error != 0) {
          stringstream ss;
          ss << "Failed to parse file schema: " << avro_strerror();
          return Status(ss.str());
        }

        const string& table_schema_str = scan_node_->hdfs_table()->avro_schema();
        DCHECK_GT(table_schema_str.size(), 0);
        avro_schema_t raw_table_schema;
        error = avro_schema_from_json_length(
            table_schema_str.c_str(), table_schema_str.size(), &raw_table_schema);
        ScopedAvroSchemaT table_schema(raw_table_schema);
        if (error != 0) {
          stringstream ss;
          ss << "Failed to parse table schema: " << avro_strerror();
          return Status(ss.str());
        }
        RETURN_IF_ERROR(ResolveSchemas(table_schema.get(), file_schema.get()));

        // We currently codegen a function only for the table schema. If this file's
        // schema is different from the table schema, don't use the codegen'd function and
        // use the interpreted path instead.
        avro_header_->use_codegend_decode_avro_data =
            avro_schema_equal(table_schema.get(), file_schema.get());

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
  if (avro_header_->schema.empty()) {
    return Status("Schema not found in file header metadata");
  }
  return Status::OK;
}

// Schema resolution is performed by first iterating through the file schema's fields to
// construct avro_header_->schema, checking that materialized columns have resolvable file
// and table schema fields. Next we iterate through the table schema's fields and check
// that any materialized columns missing from the file schema have compatible default
// values in the table schema.
// Note that schema resolution is only performed for materialized columns.
// TODO: test unresolvable schemas
Status HdfsAvroScanner::ResolveSchemas(const avro_schema_t& table_schema,
                                       const avro_schema_t& file_schema) {
  if (table_schema->type != AVRO_RECORD) {
    return Status("Table schema is not a record");
  }
  if (file_schema->type != AVRO_RECORD) {
    return Status("File schema is not a record");
  }

  int num_table_fields = avro_schema_record_size(table_schema);
  DCHECK_GT(num_table_fields, 0);

  int num_cols = scan_node_->num_cols() - scan_node_->num_partition_keys();
  int max_materialized_col_idx = -1;
  if (!scan_node_->materialized_slots().empty()) {
    max_materialized_col_idx = scan_node_->materialized_slots().back()->col_pos()
                               - scan_node_->num_partition_keys();
  }
  if (num_table_fields < num_cols) {
    stringstream ss;
    ss << "The table has " << num_cols << " non-partition columns "
       << "but the table's Avro schema has " << num_table_fields << " fields.";
    state_->LogError(ss.str());
  }
  if (num_table_fields <= max_materialized_col_idx) {
    return Status("Cannot read column that doesn't appear in table schema");
  }

  // Maps table field index -> if a matching file field was found
  bool file_field_found[num_table_fields];
  memset(&file_field_found, 0, num_table_fields);

  int num_file_fields = avro_schema_record_size(file_schema);
  DCHECK_GT(num_file_fields, 0);
  for (int i = 0; i < num_file_fields; ++i) {
    avro_datum_t file_field = avro_schema_record_field_get_by_index(file_schema, i);
    SchemaElement element = ConvertSchema(file_field);
    if (is_avro_complex_type(element.schema.get())) {
      stringstream ss;
      ss << "Complex Avro data types (records, enums, arrays, maps, unions, and fixed) "
         << "are not supported. Got type: " << avro_type_name(element.schema->type);
      return Status(ss.str());
    }

    const char* field_name = avro_schema_record_field_name(file_schema, i);
    int table_field_idx = avro_schema_record_field_get_index(table_schema, field_name);
    if (table_field_idx < 0) {
      // File has extra field, ignore
      element.slot_desc = NULL;
      avro_header_->schema.push_back(element);
      continue;
    }
    file_field_found[table_field_idx] = true;

    // The table schema's fields define the table column ordering, and the table schema
    // can have more fields than the table has columns. Treat extra fields as
    // unmaterialized columns.
    int slot_idx = table_field_idx < num_cols ?
                       scan_node_->GetMaterializedSlotIdx(
                           table_field_idx + scan_node_->num_partition_keys())
                       : HdfsScanNode::SKIP_COLUMN;

    if (slot_idx != HdfsScanNode::SKIP_COLUMN) {
      SlotDescriptor* slot_desc = scan_node_->materialized_slots()[slot_idx];
      element.slot_desc = slot_desc;

      // Use element.type (rather than file_field->type) so that e.g. "[int, null]" is
      // treated as an int and not a union
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, element.schema.get()));

      // Check that the corresponding table field type matches the declared column
      // type. This check is not strictly necessary since we won't use its default value
      // (i.e., if the check doesn't pass, we can still process the file normally), but
      // allowing the table schema to differ from the table columns could lead to
      // confusing situations. Note that this check is only performed for materialized
      // columns.
      // TODO: get rid of separate table schema and table column concepts (i.e. get rid of
      // table schema and store default values somewhere else)
      avro_schema_t table_field =
          avro_schema_record_field_get_by_index(table_schema, table_field_idx);
      SchemaElement table_element = ConvertSchema(table_field);
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, table_element.schema.get()));
    } else {
      element.slot_desc = NULL;
    }
    avro_header_->schema.push_back(element);
  }
  DCHECK_EQ(avro_header_->schema.size(), num_file_fields);

  // Check that all materialized fields either appear in the file schema or have a default
  // value in the table schema
  BOOST_FOREACH(SlotDescriptor* slot_desc, scan_node_->materialized_slots()) {
    int col_idx = slot_desc->col_pos() - scan_node_->num_partition_keys();
    if (file_field_found[col_idx]) continue;

    avro_datum_t default_value = avro_schema_record_field_default(table_schema, col_idx);
    if (default_value == NULL) {
      stringstream ss;
      ss << "Field " << avro_schema_record_field_name(table_schema, col_idx)
         << " is missing from file and does not have a default value";
      return Status(ss.str());
    }
    RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));

    if (avro_header_->template_tuple == NULL) {
      avro_header_->template_tuple =
          template_tuple_ != NULL ?
          template_tuple_ : scan_node_->InitEmptyTemplateTuple();
    }

    switch(default_value->type) {
      case AVRO_BOOLEAN: {
        int8_t v;
        if (avro_boolean_get(default_value, &v)) DCHECK(false);
        RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
        break;
      }
      case AVRO_INT32: {
        int32_t v;
        if (avro_int32_get(default_value, &v)) DCHECK(false);
        RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
        break;
      }
      case AVRO_INT64: {
        int64_t v;
        if (avro_int64_get(default_value, &v)) DCHECK(false);
        RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
        break;
      }
      case AVRO_FLOAT: {
        float v;
        if (avro_float_get(default_value, &v)) DCHECK(false);
        RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
        break;
      }
      case AVRO_DOUBLE: {
        double v;
        if (avro_double_get(default_value, &v)) DCHECK(false);
        RawValue::Write(&v, avro_header_->template_tuple, slot_desc, NULL);
        break;
      }
      case AVRO_STRING:
      case AVRO_BYTES: {
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
        avro_header_->template_tuple->SetNull(slot_desc->null_indicator_offset());
        break;
      default:
        DCHECK(false);
    }
  }
  return Status::OK;
}

HdfsAvroScanner::SchemaElement HdfsAvroScanner::ConvertSchema(
    const avro_schema_t& schema) {
  SchemaElement element;
  element.schema = schema;
  // Increment the ref count of 'schema' on behalf of the ScopedAvroSchemaT it was
  // assigned to. This allows 'schema' to outlive the scope it was passed in from (e.g.,
  // a parent record schema).
  avro_schema_incref(schema);
  element.null_union_position = -1;

  // Look for special case of [<primitive type>, "null"] union
  if (element.schema->type == AVRO_UNION) {
    int num_fields = avro_schema_union_size(schema);
    DCHECK_GT(num_fields, 0);
    if (num_fields == 2) {
      avro_schema_t child0 = avro_schema_union_branch(schema, 0);
      avro_schema_t child1 = avro_schema_union_branch(schema, 1);
      int null_position = -1;
      if (child0->type == AVRO_NULL) {
        null_position = 0;
      } else if (child1->type == AVRO_NULL) {
        null_position = 1;
      }

      if (null_position != -1) {
        avro_schema_t non_null_child = null_position == 0 ? child1 : child0;
        SchemaElement child = ConvertSchema(non_null_child);

        // 'schema' is a [<child>, "null"] union. If child is a primitive type (i.e.,
        // not a complex type nor a [<primitive type>, "null"] union itself), we treat
        // this node as the same type as child except with null_union_position set
        // appropriately.
        if (is_avro_primitive(child.schema.get()) && child.null_union_position == -1) {
          element = child;
          element.null_union_position = null_position;
        }
      }
    }
  }
  // TODO: populate children of complex types
  return element;
}

 Status HdfsAvroScanner::VerifyTypesMatch(SlotDescriptor* slot_desc,
                                          avro_obj_t* schema) {
  switch (schema->type) {
    case AVRO_DECIMAL:
      if (slot_desc->type().type != TYPE_DECIMAL) break;
      if (slot_desc->type().scale != avro_schema_decimal_scale(schema)) {
        const string& col_name =
            scan_node_->hdfs_table()->col_names()[slot_desc->col_pos()];
        stringstream ss;
        ss << "File '" << stream_->filename() << "' column '" << col_name
           << "' has a scale that does not match the table metadata scale."
           << " File metadata scale: " << avro_schema_decimal_scale(schema)
           << " Table metadata scale: " << slot_desc->type().scale;
        return Status(ss.str());
      }
      if (slot_desc->type().precision != avro_schema_decimal_precision(schema)) {
        const string& col_name =
            scan_node_->hdfs_table()->col_names()[slot_desc->col_pos()];
        stringstream ss;
        ss << "File '" << stream_->filename() << "' column '" << col_name
           << "' has a precision that does not match the table metadata precision."
           << " File metadata precision: " << avro_schema_decimal_precision(schema)
           << " Table metadata precision: " << slot_desc->type().precision;
        return Status(ss.str());
      }
      return Status::OK;
    case AVRO_NULL:
      // All Impala types are nullable
      return Status::OK;
    case AVRO_STRING:
    case AVRO_BYTES:
      if (slot_desc->type().IsStringType()) return Status::OK;
      break;
    case AVRO_INT32:
      if (slot_desc->type().type == TYPE_INT) return Status::OK;
      // Type promotion
      if (slot_desc->type().type == TYPE_BIGINT) return Status::OK;
      if (slot_desc->type().type == TYPE_FLOAT) return Status::OK;
      if (slot_desc->type().type == TYPE_DOUBLE) return Status::OK;
      break;
    case AVRO_INT64:
      if (slot_desc->type().type == TYPE_BIGINT) return Status::OK;
      // Type promotion
      if (slot_desc->type().type == TYPE_FLOAT) return Status::OK;
      if (slot_desc->type().type == TYPE_DOUBLE) return Status::OK;
      break;
    case AVRO_FLOAT:
      if (slot_desc->type().type == TYPE_FLOAT) return Status::OK;
      // Type promotion
      if (slot_desc->type().type == TYPE_DOUBLE) return Status::OK;
      break;
    case AVRO_DOUBLE:
      if (slot_desc->type().type == TYPE_DOUBLE) return Status::OK;
      break;
    case AVRO_BOOLEAN:
      if (slot_desc->type().type == TYPE_BOOLEAN) return Status::OK;
      break;
    default:
      break;
  }
  stringstream ss;
  ss << "Unresolvable column types (column " << slot_desc->col_pos() << "): "
     << "declared type = " << slot_desc->type() << ", "
     << "Avro type = " << avro_type_name(schema->type);
  return Status(ss.str());
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

  return Status::OK;
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

      if (scan_node_->ReachedLimit()) return Status::OK;
    }

    if (decompressor_.get() != NULL && !decompressor_->reuse_output_buffer()) {
      AttachPool(data_buffer_pool_.get(), true);
    }
    RETURN_IF_ERROR(ReadSync());
  }

  return Status::OK;
}

void HdfsAvroScanner::MaterializeTuple(MemPool* pool, uint8_t** data, Tuple* tuple) {
  BOOST_FOREACH(const SchemaElement& element, avro_header_->schema) {
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
    if (element.null_union_position != -1
        && !ReadUnionType(element.null_union_position, data)) {
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
      default:
        DCHECK(false) << "Unsupported SchemaElement: " << type;
    }
  }
}

// This function produces a codegen'd function equivalent to MaterializeTuple() but
// optimized for the table schema. It eliminates the conditionals necessary when
// interpreting the type of each element in the schema, instead generating code to handle
// each element in a record. Example output:
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
Function* HdfsAvroScanner::CodegenMaterializeTuple(HdfsScanNode* node,
                                                   LlvmCodeGen* codegen) {
  const string& table_schema_str = node->hdfs_table()->avro_schema();

  // HdfsAvroScanner::Codegen() (which calls this function) gets called by HdfsScanNode
  // regardless of whether the table we're scanning contains Avro files or not. If this
  // isn't an Avro table, there is no table schema to codegen a function from (and there's
  // no need to anyway).
  // TODO: HdfsScanNode shouldn't codegen functions it doesn't need.
  if (table_schema_str.empty()) return NULL;

  avro_schema_t raw_table_schema;
  int error = avro_schema_from_json_length(
      table_schema_str.c_str(), table_schema_str.size(), &raw_table_schema);
  ScopedAvroSchemaT table_schema(raw_table_schema);
  if (error != 0) {
    stringstream ss;
    ss << "Failed to parse table schema: " << avro_strerror();
    node->runtime_state()->LogError(ss.str());
    return NULL;
  }
  int num_fields = avro_schema_record_size(table_schema.get());
  DCHECK_GT(num_fields, 0);
  // Disable Codegen for TYPE_CHAR
  for (int field_idx = 0; field_idx < num_fields; ++field_idx) {
    int col_idx = field_idx + node->num_partition_keys();
    int slot_idx = node->GetMaterializedSlotIdx(col_idx);
    if (slot_idx != HdfsScanNode::SKIP_COLUMN) {
      SlotDescriptor* slot_desc = node->materialized_slots()[slot_idx];
      if (slot_desc->type().type == TYPE_CHAR) {
        LOG(INFO) << "Avro codegen skipped because CHAR is not supported.";
        return NULL;
      }
    }
  }

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Type* this_type = codegen->GetType(HdfsAvroScanner::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  TupleDescriptor* tuple_desc = const_cast<TupleDescriptor*>(node->tuple_desc());
  StructType* tuple_type = tuple_desc->GenerateLlvmStruct(codegen);
  Type* tuple_ptr_type = PointerType::get(tuple_type, 0);

  Type* tuple_opaque_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  PointerType* tuple_opaque_ptr_type = PointerType::get(tuple_opaque_type, 0);

  Type* data_ptr_type = PointerType::get(codegen->ptr_type(), 0); // char**
  Type* mempool_type = PointerType::get(codegen->GetType(MemPool::LLVM_CLASS_NAME), 0);

  LlvmCodeGen::FnPrototype prototype(codegen, "MaterializeTuple", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("pool", mempool_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("data", data_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_opaque_ptr_type));
  Value* args[4];
  Function* fn = prototype.GeneratePrototype(&builder, args);

  Value* this_val = args[0];
  Value* pool_val = args[1];
  Value* data_val = args[2];
  Value* opaque_tuple_val = args[3];

  Value* tuple_val = builder.CreateBitCast(opaque_tuple_val, tuple_ptr_type, "tuple_ptr");

  // Codegen logic for parsing each field and, if necessary, populating a slot with the
  // result.
  for (int field_idx = 0; field_idx < num_fields; ++field_idx) {
    avro_datum_t field =
        avro_schema_record_field_get_by_index(table_schema.get(), field_idx);
    SchemaElement element = ConvertSchema(field);
    int col_idx = field_idx + node->num_partition_keys();
    int slot_idx = node->GetMaterializedSlotIdx(col_idx);

    // The previous iteration may have left the insert point somewhere else
    builder.SetInsertPoint(&fn->back());

    // Block that calls appropriate Read<Type> function
    BasicBlock* read_field_block = BasicBlock::Create(context, "read_field", fn);

    if (element.null_union_position != -1) {
      // Field could be null. Create conditional branch based on ReadUnionType result.
      BasicBlock* null_block = BasicBlock::Create(context, "null_field", fn);
      BasicBlock* endif_block = BasicBlock::Create(context, "endif", fn);
      Function* read_union_fn =
          codegen->GetFunction(IRFunction::READ_UNION_TYPE);
      Value* null_union_pos_val =
          codegen->GetIntConstant(TYPE_INT, element.null_union_position);
      Value* is_not_null_val = builder.CreateCall3(
          read_union_fn, this_val, null_union_pos_val, data_val, "is_not_null");
      builder.CreateCondBr(is_not_null_val, read_field_block, null_block);

      // Write branch at end of read_field_block, we fill in the rest later
      builder.SetInsertPoint(read_field_block);
      builder.CreateBr(endif_block);

      // Write null field IR
      builder.SetInsertPoint(null_block);
      if (slot_idx != HdfsScanNode::SKIP_COLUMN) {
        SlotDescriptor* slot_desc = node->materialized_slots()[slot_idx];
        Function* set_null_fn = slot_desc->CodegenUpdateNull(codegen, tuple_type, true);
        DCHECK(set_null_fn != NULL);
        builder.CreateCall(set_null_fn, tuple_val);
      }
      // LLVM requires all basic blocks to end with a terminating instruction
      builder.CreateBr(endif_block);
    } else {
      // Field is never null, read field unconditionally.
      builder.CreateBr(read_field_block);
    }

    SlotDescriptor* slot_desc = NULL;
    if (slot_idx != HdfsScanNode::SKIP_COLUMN) {
      slot_desc = node->materialized_slots()[slot_idx];
    }

    // Write read_field_block IR starting at the beginning of the block
    builder.SetInsertPoint(read_field_block, read_field_block->begin());
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
        if ((slot_idx != HdfsScanNode::SKIP_COLUMN) &&
            (slot_desc->type().type == TYPE_VARCHAR)) {
          read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_VARCHAR);
        } else {
          read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_STRING);
        }
        break;
      default:
        // Unsupported type, can't codegen
        VLOG(1) << "Failed to codegen MaterializeTuple() due to unsupported type: "
                << element.schema->type;
        fn->eraseFromParent();
        return NULL;
    }

    // Call appropriate ReadAvro<Type> function
    Value* write_slot_val = builder.getFalse();
    Value* slot_type_val = builder.getInt32(0);
    Value* opaque_slot_val = codegen->null_ptr_value();
    if (slot_idx != HdfsScanNode::SKIP_COLUMN) {
      // Field corresponds to a materialized column, fill in relevant arguments
      write_slot_val = builder.getTrue();
      if (slot_desc->type().type == TYPE_DECIMAL) {
        // ReadAvroDecimal() takes slot byte size instead of slot type
        slot_type_val = builder.getInt32(slot_desc->type().GetByteSize());
      } else {
        slot_type_val = builder.getInt32(slot_desc->type().type);
      }
      Value* slot_val =
          builder.CreateStructGEP(tuple_val, slot_desc->field_idx(), "slot");
      opaque_slot_val =
          builder.CreateBitCast(slot_val, codegen->ptr_type(), "opaque_slot");
    }

    // NOTE: ReadAvroVarchar/Char has different signature than rest of read functions
    if ((slot_idx != HdfsScanNode::SKIP_COLUMN) &&
        (slot_desc->type().type == TYPE_VARCHAR ||
         slot_desc->type().type == TYPE_CHAR)) {
      // Need to pass an extra argument (the length) to the codegen function
      Value* fixed_len = builder.getInt32(slot_desc->type().len);
      Value* read_field_args[] = {this_val, slot_type_val, fixed_len, data_val,
        write_slot_val, opaque_slot_val, pool_val};
      builder.CreateCall(read_field_fn, read_field_args);
    } else {
      Value* read_field_args[] =
          {this_val, slot_type_val, data_val, write_slot_val, opaque_slot_val, pool_val};
      builder.CreateCall(read_field_fn, read_field_args);
    }
  }
  builder.SetInsertPoint(&fn->back());
  builder.CreateRetVoid();
  return codegen->FinalizeFunction(fn);
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
