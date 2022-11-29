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

#include "exec/avro/hdfs-avro-scanner.h"

#include <avro/errors.h>
#include <algorithm>    // std::min
#include <avro/legacy.h>
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exec/scanner-context.inline.h"
#include "runtime/fragment-state.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "util/codec.h"
#include "util/decompress.h"
#include "util/runtime-profile-counters.h"
#include "util/test-info.h"

#include "common/names.h"

// Note: the Avro C++ library uses exceptions for error handling. Any Avro
// function that may throw an exception must be placed in a try/catch block.
using namespace impala;
using namespace strings;

const char* HdfsAvroScanner::LLVM_CLASS_NAME = "class.impala::HdfsAvroScanner";

const uint8_t HdfsAvroScanner::AVRO_VERSION_HEADER[4] = {'O', 'b', 'j', 1};

const string HdfsAvroScanner::AVRO_SCHEMA_KEY("avro.schema");
const string HdfsAvroScanner::AVRO_CODEC_KEY("avro.codec");
const string HdfsAvroScanner::AVRO_NULL_CODEC("null");
const string HdfsAvroScanner::AVRO_SNAPPY_CODEC("snappy");
const string HdfsAvroScanner::AVRO_DEFLATE_CODEC("deflate");

const string AVRO_MEM_LIMIT_EXCEEDED = "HdfsAvroScanner::$0() failed to allocate "
    "$1 bytes for $2.";

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

static Status CheckSchema(const AvroSchemaElement& avro_schema) {
  if (avro_schema.schema == nullptr) {
    return Status("Missing Avro schema in scan node. This could be due to stale "
        "metadata. Running 'invalidate metadata <tablename>' may resolve the problem.");
  }
  return Status::OK();
}

HdfsAvroScanner::HdfsAvroScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
  : BaseSequenceScanner(scan_node, state) {
}

HdfsAvroScanner::HdfsAvroScanner()
  : BaseSequenceScanner() {
  DCHECK(TestInfo::is_test());
}

Status HdfsAvroScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(BaseSequenceScanner::Open(context));
  RETURN_IF_ERROR(CheckSchema(scan_node_->avro_schema()));
  return Status::OK();
}

Status HdfsAvroScanner::Codegen(HdfsScanPlanNode* node,
   FragmentState* state, llvm::Function** decode_avro_data_fn) {
  *decode_avro_data_fn = nullptr;
  DCHECK(state->ShouldCodegen());
  DCHECK(state->codegen() != nullptr);
  RETURN_IF_ERROR(CodegenDecodeAvroData(node, state, decode_avro_data_fn));
  DCHECK(*decode_avro_data_fn != nullptr);
  return Status::OK();
}

BaseSequenceScanner::FileHeader* HdfsAvroScanner::AllocateFileHeader() {
  AvroFileHeader* header = new AvroFileHeader();
  header->template_tuple = template_tuple_;
  return header;
}

Status HdfsAvroScanner::ReadFileHeader() {
  DCHECK(only_parsing_header_);
  avro_header_ = reinterpret_cast<AvroFileHeader*>(header_);

  // Check version header
  uint8_t* header;
  RETURN_IF_FALSE(stream_->ReadBytes(
      sizeof(AVRO_VERSION_HEADER), &header, &parse_status_));
  if (memcmp(header, AVRO_VERSION_HEADER, sizeof(AVRO_VERSION_HEADER))) {
    return Status(TErrorCode::AVRO_BAD_VERSION_HEADER,
        stream_->filename(), ReadWriteUtil::HexDump(header, sizeof(AVRO_VERSION_HEADER)));
  }

  // Decode relevant metadata (encoded as Avro map)
  RETURN_IF_ERROR(ParseMetadata());

  // Read file sync marker
  uint8_t* sync;
  RETURN_IF_FALSE(stream_->ReadBytes(SYNC_HASH_SIZE, &sync, &parse_status_));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);

  header_->header_size = stream_->total_bytes_returned() - SYNC_HASH_SIZE;

  // Transfer ownership so the memory remains valid for subsequent scanners that process
  // the data portions of the file.
  scan_node_->TransferToSharedStatePool(template_tuple_pool_.get());
  return Status::OK();
}

Status HdfsAvroScanner::ParseMetadata() {
  header_->is_compressed = false;
  header_->compression_type = THdfsCompression::NONE;

  int64_t num_entries;
  RETURN_IF_FALSE(stream_->ReadZLong(&num_entries, &parse_status_));
  if (num_entries < 1) {
    return Status(TErrorCode::AVRO_INVALID_METADATA_COUNT, stream_->filename(),
        num_entries, stream_->file_offset());
  }

  while (num_entries != 0) {
    DCHECK_GT(num_entries, 0);
    for (int i = 0; i < num_entries; ++i) {
      // Decode Avro string-type key
      string key;
      uint8_t* key_buf;
      int64_t key_len;
      RETURN_IF_FALSE(stream_->ReadZLong(&key_len, &parse_status_));
      if (key_len < 0) {
        return Status(TErrorCode::AVRO_INVALID_LENGTH, stream_->filename(), key_len,
            stream_->file_offset());
      }
      RETURN_IF_FALSE(stream_->ReadBytes(key_len, &key_buf, &parse_status_));
      key = string(reinterpret_cast<char*>(key_buf), key_len);

      // Decode Avro bytes-type value
      uint8_t* value;
      int64_t value_len;
      RETURN_IF_FALSE(stream_->ReadZLong(&value_len, &parse_status_));
      if (value_len < 0) {
        return Status(TErrorCode::AVRO_INVALID_LENGTH, stream_->filename(), value_len,
            stream_->file_offset());
      }
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
    if (num_entries < 0) {
      return Status(TErrorCode::AVRO_INVALID_METADATA_COUNT, stream_->filename(),
          num_entries, stream_->file_offset());
    }
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
  for (SlotDescriptor* slot_desc: scan_node_->materialized_slots()) {
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
        if (default_value == nullptr) {
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
  // avro_header could have null template_tuple here if no partition columns are
  // materialized and no default values are set yet.
  if (avro_header_->template_tuple == nullptr) {
    if (template_tuple_ != nullptr) {
      avro_header_->template_tuple = template_tuple_;
    } else {
      avro_header_->template_tuple =
          Tuple::Create(tuple_byte_size_, template_tuple_pool_.get());
    }
  }
  switch (default_value->type) {
    case AVRO_BOOLEAN: {
      // We don't call VerifyTypesMatch() above the switch statement so we don't want to
      // call it in the default case (since we VerifyTypesMatch() can't handle every type
      // either, and we want to return the correct error message).
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      int8_t v;
      if (avro_boolean_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, nullptr);
      break;
    }
    case AVRO_INT32:
    case AVRO_DATE: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      int32_t v;
      if (avro_int32_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, nullptr);
      break;
    }
    case AVRO_INT64: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      int64_t v;
      if (avro_int64_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, nullptr);
      break;
    }
    case AVRO_FLOAT: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      float v;
      if (avro_float_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, nullptr);
      break;
    }
    case AVRO_DOUBLE: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      double v;
      if (avro_double_get(default_value, &v)) DCHECK(false);
      RawValue::Write(&v, avro_header_->template_tuple, slot_desc, nullptr);
      break;
    }
    case AVRO_STRING:
    case AVRO_BYTES: {
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value));
      char* v;
      if (avro_string_get(default_value, &v)) DCHECK(false);
      StringValue sv(v);
      RawValue::Write(&sv, avro_header_->template_tuple, slot_desc,
          template_tuple_pool_.get());
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

  ColumnType reader_type;
  RETURN_IF_ERROR(AvroSchemaToColumnType(table_schema.schema, field_name, &reader_type));
  ColumnType writer_type;
  RETURN_IF_ERROR(AvroSchemaToColumnType(file_schema.schema, field_name, &writer_type));
  bool match = VerifyTypesMatch(reader_type, writer_type);
  if (match) return Status::OK();
  return Status(TErrorCode::AVRO_SCHEMA_RESOLUTION_ERROR, field_name,
      avro_type_name(table_schema.schema->type),
      avro_type_name(file_schema.schema->type));
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

  ColumnType file_type;
  RETURN_IF_ERROR(AvroSchemaToColumnType(schema, col_name, &file_type));
  bool match = VerifyTypesMatch(slot_desc->type(), file_type);
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
    case TYPE_DATE:
      switch(reader_type.type) {
        case TYPE_INT:
        case TYPE_DATE:
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
  DCHECK(header_ != nullptr);
  only_parsing_header_ = false;
  avro_header_ = reinterpret_cast<AvroFileHeader*>(header_);
  template_tuple_ = avro_header_->template_tuple;
  if (header_->is_compressed) {
    RETURN_IF_ERROR(UpdateDecompressor(header_->compression_type));
  }

  if (avro_header_->use_codegend_decode_avro_data) {
    codegend_decode_avro_data_ = scan_node_->GetCodegenFn(THdfsFileFormat::AVRO);
  }
  if (codegend_decode_avro_data_ == nullptr) {
    scan_node_->IncNumScannersCodegenDisabled();
  } else {
    VLOG(2) << "HdfsAvroScanner (node_id=" << scan_node_->id()
            << ") using llvm codegend functions.";
    scan_node_->IncNumScannersCodegenEnabled();
  }

  return Status::OK();
}

Status HdfsAvroScanner::ProcessRange(RowBatch* row_batch) {
  // Process blocks until we hit eos, the limit or the batch fills up. Check
  // AtCapacity() at the end of the loop to guarantee that we process at least one row
  // so that we make progress even if the batch starts off with AtCapacity() == true,
  // which can happen if the tuple buffer is > 8MB.
  DCHECK_GT(row_batch->capacity(), row_batch->num_rows());
  while (!eos_ && !scan_node_->ReachedLimitShared()) {
    if (record_pos_ == num_records_in_block_) {
      // Read new data block. Reset members first to avoid corrupt state after
      // recovery from parse error.
      record_pos_ = 0;
      num_records_in_block_ = 0;
      data_block_len_ = 0;
      data_block_ = nullptr;
      data_block_end_ = nullptr;
      int64_t num_records_in_block;
      RETURN_IF_FALSE(stream_->ReadZLong(&num_records_in_block, &parse_status_));
      if (num_records_in_block < 0) {
        return Status(TErrorCode::AVRO_INVALID_RECORD_COUNT, stream_->filename(),
            num_records_in_block_, stream_->file_offset());
      }
      int64_t compressed_size;
      RETURN_IF_FALSE(stream_->ReadZLong(&compressed_size, &parse_status_));
      if (compressed_size < 0) {
        return Status(TErrorCode::AVRO_INVALID_COMPRESSED_SIZE, stream_->filename(),
            compressed_size, stream_->file_offset());
      }
      uint8_t* compressed_data;
      RETURN_IF_FALSE(stream_->ReadBytes(
          compressed_size, &compressed_data, &parse_status_));

      if (header_->is_compressed) {
        if (header_->compression_type == THdfsCompression::SNAPPY) {
          // Snappy-compressed data block includes trailing 4-byte checksum,
          // decompressor_ doesn't expect this
          compressed_size -= SnappyDecompressor::TRAILING_CHECKSUM_LEN;
        }
        SCOPED_TIMER(decompress_timer_);
        RETURN_IF_ERROR(decompressor_->ProcessBlock(false, compressed_size,
            compressed_data, &data_block_len_, &data_block_));
      } else {
        data_block_ = compressed_data;
        data_block_len_ = compressed_size;
      }
      num_records_in_block_ = num_records_in_block;
      data_block_end_ = data_block_ + data_block_len_;
    }

    int64_t prev_record_pos = record_pos_;
    int block_start_row = row_batch->num_rows();
    // Process the remaining data in the current block. Always process at least one row
    // to ensure we make progress even if the batch starts off with AtCapacity() == true.
    DCHECK_GT(row_batch->capacity(), row_batch->num_rows());
    while (record_pos_ != num_records_in_block_) {
      SCOPED_TIMER(scan_node_->materialize_tuple_timer());

      Tuple* tuple = tuple_;
      TupleRow* tuple_row = row_batch->GetRow(row_batch->AddRow());
      int max_tuples = row_batch->capacity() - row_batch->num_rows();
      DCHECK_GT(num_records_in_block_, record_pos_);
      max_tuples = min<int64_t>(max_tuples, num_records_in_block_ - record_pos_);
      int num_to_commit;
      if (scan_node_->materialized_slots().empty()) {
        // No slots to materialize (e.g. count(*)), no need to decode data
        num_to_commit = WriteTemplateTuples(tuple_row, max_tuples);
      } else {
        num_to_commit = DecodeAvroDataCodegenOrInterpret(max_tuples,
            row_batch->tuple_data_pool(), &data_block_,
            data_block_end_, tuple, tuple_row);
      }

      RETURN_IF_ERROR(parse_status_);
      RETURN_IF_ERROR(CommitRows(num_to_commit, row_batch));
      record_pos_ += max_tuples;
      COUNTER_ADD(scan_node_->rows_read_counter(), max_tuples);
      if (row_batch->AtCapacity() || scan_node_->ReachedLimitShared()) break;
    }

    if (record_pos_ == num_records_in_block_) {
      if (decompressor_.get() != nullptr && !decompressor_->reuse_output_buffer()) {
        if (prev_record_pos == 0 && row_batch->num_rows() == block_start_row) {
          // Did not return any rows from current block in this or a previous
          // ProcessRange() call - we can recycle the memory. This optimisation depends on
          // passing keep_current_chunk = false to the AcquireData() call below, so that
          // the current chunk only contains data for the current Avro block.
          data_buffer_pool_->Clear();
        } else {
          // Returned rows may reference data buffers - need to attach to batch.
          row_batch->tuple_data_pool()->AcquireData(data_buffer_pool_.get(), false);
        }
      }
      RETURN_IF_ERROR(ReadSync());
    }
    if (row_batch->AtCapacity()) break;
  }
  return Status::OK();
}

bool HdfsAvroScanner::MaterializeTuple(const AvroSchemaElement& record_schema,
    MemPool* pool, uint8_t** data, uint8_t* data_end, Tuple* tuple) {
  DCHECK_EQ(record_schema.schema->type, AVRO_RECORD);
  for (const AvroSchemaElement& element: record_schema.children) {
    DCHECK_LE(*data, data_end);

    const SlotDescriptor* slot_desc = element.slot_desc;
    bool write_slot = false;
    void* slot = nullptr;
    PrimitiveType slot_type = INVALID_TYPE;
    if (slot_desc != nullptr) {
      write_slot = true;
      slot = tuple->GetSlot(slot_desc->tuple_offset());
      slot_type = slot_desc->type().type;
    }

    avro_type_t type = element.schema->type;
    if (element.nullable()) {
      bool is_null;
      if (!ReadUnionType(element.null_union_position, data, data_end, &is_null)) {
        return false;
      }
      if (is_null) type = AVRO_NULL;
    }

    bool success;
    switch (type) {
      case AVRO_NULL:
        if (slot_desc != nullptr) tuple->SetNull(slot_desc->null_indicator_offset());
        success = true;
        break;
      case AVRO_BOOLEAN:
        success = ReadAvroBoolean(slot_type, data, data_end, write_slot, slot, pool);
        break;
      case AVRO_DATE:
      case AVRO_INT32:
        if (slot_type == TYPE_DATE) {
          success = ReadAvroDate(slot_type, data, data_end, write_slot, slot, pool);
        } else {
          success = ReadAvroInt32(slot_type, data, data_end, write_slot, slot, pool);
        }
        break;
      case AVRO_INT64:
        success = ReadAvroInt64(slot_type, data, data_end, write_slot, slot, pool);
        break;
      case AVRO_FLOAT:
        success = ReadAvroFloat(slot_type, data, data_end, write_slot, slot, pool);
        break;
      case AVRO_DOUBLE:
        success = ReadAvroDouble(slot_type, data, data_end, write_slot, slot, pool);
        break;
      case AVRO_STRING:
      case AVRO_BYTES:
        if (slot_desc != nullptr && slot_desc->type().type == TYPE_VARCHAR) {
          success = ReadAvroVarchar(slot_type, slot_desc->type().len, data, data_end,
              write_slot, slot, pool);
        } else if (slot_desc != nullptr && slot_desc->type().type == TYPE_CHAR) {
          success = ReadAvroChar(slot_type, slot_desc->type().len, data, data_end,
              write_slot, slot, pool);
        } else {
          success = ReadAvroString(slot_type, data, data_end, write_slot, slot, pool);
        }
        break;
      case AVRO_DECIMAL: {
        int slot_byte_size = 0;
        if (slot_desc != nullptr) {
          DCHECK_EQ(slot_type, TYPE_DECIMAL);
          slot_byte_size = slot_desc->type().GetByteSize();
        }
        success = ReadAvroDecimal(slot_byte_size, data, data_end, write_slot, slot, pool);
        break;
      }
      case AVRO_RECORD:
        success = MaterializeTuple(element, pool, data, data_end, tuple);
        break;
      default:
        success = false;
        DCHECK(false) << "Unsupported SchemaElement: " << type;
    }
    if (UNLIKELY(!success)) {
      DCHECK(!parse_status_.ok());
      return false;
    }
  }
  return true;
}

void HdfsAvroScanner::SetStatusCorruptData(TErrorCode::type error_code) {
  DCHECK(parse_status_.ok());
  if (TestInfo::is_test()) {
    parse_status_ = Status(error_code, "test file", 123);
  } else {
    parse_status_ = Status(error_code, stream_->filename(), stream_->file_offset());
  }
}

void HdfsAvroScanner::SetStatusInvalidValue(TErrorCode::type error_code, int64_t len) {
  DCHECK(parse_status_.ok());
  if (TestInfo::is_test()) {
    parse_status_ = Status(error_code, "test file", len, 123);
  } else {
    parse_status_ = Status(error_code, stream_->filename(), len, stream_->file_offset());
  }
}

void HdfsAvroScanner::SetStatusValueOverflow(TErrorCode::type error_code, int64_t len,
    int64_t limit) {
  DCHECK(parse_status_.ok());
  if (TestInfo::is_test()) {
    parse_status_ = Status(error_code, "test file", len, limit, 123);
  } else {
    parse_status_ = Status(error_code, stream_->filename(), len, limit,
        stream_->file_offset());
  }
}

// This function produces a codegen'd function equivalent to MaterializeTuple() but
// optimized for the table schema. Via helper functions CodegenReadRecord() and
// CodegenReadScalar(), it eliminates the conditionals necessary when interpreting the
// type of each element in the schema, instead generating code to handle each element in
// the schema.
//
// To avoid overly long codegen times for wide schemas, this function generates
// one function per 200 columns, and a function that calls them all together.
//
//
// Example output with 'select count(*) from tpch_avro.region':
//
// define i1 @MaterializeTuple-helper0(%"class.impala::HdfsAvroScanner"* %this, %"struct.impala::AvroSchemaElement"* %record_schema, %"class.impala::MemPool"* %pool, i8** %data, i8* %data_end, %"class.impala::Tuple"* %tuple) #34 {
// entry:
//   %is_null_ptr = alloca i1
//   %tuple_ptr = bitcast %"class.impala::Tuple"* %tuple to <{}>*
//   %0 = bitcast i1* %is_null_ptr to i8*
//   %read_union_ok = call i1 @_ZN6impala15HdfsAvroScanner13ReadUnionTypeEiPPhS1_Pb(%"class.impala::HdfsAvroScanner"* %this, i32 1, i8** %data, i8* %data_end, i8* %0)
//   br i1 %read_union_ok, label %read_union_ok1, label %bail_out
//
// read_union_ok1:                                   ; preds = %entry
//   %is_null = load i1, i1* %is_null_ptr
//   br i1 %is_null, label %null_field, label %read_field
//
// read_field:                                       ; preds = %read_union_ok1
//   %success = call i1 @_ZN6impala15HdfsAvroScanner13ReadAvroInt32ENS_13PrimitiveTypeEPPhS2_bPvPNS_7MemPoolE(%"class.impala::HdfsAvroScanner"* %this, i32 0, i8** %data, i8* %data_end, i1 false, i8* null, %"class.impala::MemPool"* %pool)
//   br i1 %success, label %end_field, label %bail_out
//
// null_field:                                       ; preds = %read_union_ok1
//   br label %end_field
//
// end_field:                                        ; preds = %read_field, %null_field
//   %1 = bitcast i1* %is_null_ptr to i8*
//   %read_union_ok4 = call i1 @_ZN6impala15HdfsAvroScanner13ReadUnionTypeEiPPhS1_Pb(%"class.impala::HdfsAvroScanner"* %this, i32 1, i8** %data, i8* %data_end, i8* %1)
//   br i1 %read_union_ok4, label %read_union_ok5, label %bail_out
//
// read_union_ok5:                                   ; preds = %end_field
//   %is_null7 = load i1, i1* %is_null_ptr
//   br i1 %is_null7, label %null_field6, label %read_field2
//
// read_field2:                                      ; preds = %read_union_ok5
//   %success8 = call i1 @_ZN6impala15HdfsAvroScanner14ReadAvroStringENS_13PrimitiveTypeEPPhS2_bPvPNS_7MemPoolE(%"class.impala::HdfsAvroScanner"* %this, i32 0, i8** %data, i8* %data_end, i1 false, i8* null, %"class.impala::MemPool"* %pool)
//   br i1 %success8, label %end_field3, label %bail_out
//
// null_field6:                                      ; preds = %read_union_ok5
//   br label %end_field3
//
// end_field3:                                       ; preds = %read_field2, %null_field6
//   %2 = bitcast i1* %is_null_ptr to i8*
//   %read_union_ok11 = call i1 @_ZN6impala15HdfsAvroScanner13ReadUnionTypeEiPPhS1_Pb(%"class.impala::HdfsAvroScanner"* %this, i32 1, i8** %data, i8* %data_end, i8* %2)
//   br i1 %read_union_ok11, label %read_union_ok12, label %bail_out
//
// read_union_ok12:                                  ; preds = %end_field3
//   %is_null14 = load i1, i1* %is_null_ptr
//   br i1 %is_null14, label %null_field13, label %read_field9
//
// read_field9:                                      ; preds = %read_union_ok12
//   %success15 = call i1 @_ZN6impala15HdfsAvroScanner14ReadAvroStringENS_13PrimitiveTypeEPPhS2_bPvPNS_7MemPoolE(%"class.impala::HdfsAvroScanner"* %this, i32 0, i8** %data, i8* %data_end, i1 false, i8* null, %"class.impala::MemPool"* %pool)
//   br i1 %success15, label %end_field10, label %bail_out
//
// null_field13:                                     ; preds = %read_union_ok12
//   br label %end_field10
//
// end_field10:                                      ; preds = %read_field9, %null_field13
//   ret i1 true
//
// bail_out:                                         ; preds = %read_field9, %end_field3, %read_field2, %end_field, %read_field, %entry
//   ret i1 false
// }
//
// define i1 @MaterializeTuple(%"class.impala::HdfsAvroScanner"* %this, %"struct.impala::AvroSchemaElement"* %record_schema, %"class.impala::MemPool"* %pool, i8** %data, i8* %data_end, %"class.impala::Tuple"* %tuple) #34 {
// entry:
//   %helper_01 = call i1 @MaterializeTuple-helper0(%"class.impala::HdfsAvroScanner"* %this, %"struct.impala::AvroSchemaElement"* %record_schema, %"class.impala::MemPool"* %pool, i8** %data, i8* %data_end, %"class.impala::Tuple"* %tuple)
//   br i1 %helper_01, label %helper_0, label %bail_out
//
// helper_0:                                         ; preds = %entry
//   ret i1 true
//
// bail_out:                                         ; preds = %entry
//   ret i1 false
// }
Status HdfsAvroScanner::CodegenMaterializeTuple(const HdfsScanPlanNode* node,
    LlvmCodeGen* codegen, llvm::Function** materialize_tuple_fn) {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  llvm::PointerType* this_ptr_type = codegen->GetStructPtrType<HdfsAvroScanner>();

  TupleDescriptor* tuple_desc = const_cast<TupleDescriptor*>(node->tuple_desc_);
  llvm::StructType* tuple_type = tuple_desc->GetLlvmStruct(codegen);
  if (tuple_type == nullptr) return Status("Could not generate tuple struct.");
  llvm::Type* tuple_ptr_type = llvm::PointerType::get(tuple_type, 0);

  llvm::PointerType* tuple_opaque_ptr_type = codegen->GetStructPtrType<Tuple>();

  llvm::Type* data_ptr_type = codegen->ptr_ptr_type(); // char**
  llvm::Type* mempool_type = codegen->GetStructPtrType<MemPool>();
  llvm::Type* schema_element_type = codegen->GetStructPtrType<AvroSchemaElement>();

  // Schema can be null if metadata is stale. See test in
  // queries/QueryTest/avro-schema-changes.test.
  RETURN_IF_ERROR(CheckSchema(*node->avro_schema_.get()));
  int num_children = node->avro_schema_->children.size();
  if (num_children == 0) {
    return Status("Invalid Avro record schema: contains no children.");
  }
  // We handle step_size columns per function. This minimizes LLVM
  // optimization time and was determined empirically. If there are
  // too many functions, it takes LLVM longer to optimize. If the functions
  // are too long, it takes LLVM longer too.
  int step_size = 200;
  std::vector<llvm::Function*> helper_functions;

  // prototype re-used several times by amending with SetName()
  LlvmCodeGen::FnPrototype prototype(codegen, "", codegen->bool_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("record_schema", schema_element_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("pool", mempool_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("data", data_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("data_end", codegen->ptr_type()));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_opaque_ptr_type));

  // Generate helper functions for every step_size columns.
  for (int i = 0; i < num_children; i += step_size) {
    prototype.SetName("MaterializeTuple-helper" + std::to_string(i));
    llvm::Value* args[6];
    llvm::Function* helper_fn = prototype.GeneratePrototype(&builder, args);

    llvm::Value* this_val = args[0];
    // llvm::Value* record_schema_val = args[1]; // don't need this
    llvm::Value* pool_val = args[2];
    llvm::Value* data_val = args[3];
    llvm::Value* data_end_val = args[4];
    llvm::Value* opaque_tuple_val = args[5];

    llvm::Value* tuple_val =
        builder.CreateBitCast(opaque_tuple_val, tuple_ptr_type, "tuple_ptr");

    // Create a bail out block to handle decoding failures.
    llvm::BasicBlock* bail_out_block =
        llvm::BasicBlock::Create(context, "bail_out", helper_fn, nullptr);

    Status status = CodegenReadRecord(
        SchemaPath(), *node->avro_schema_.get(), i, std::min(num_children, i + step_size),
        node, codegen, &builder, helper_fn, bail_out_block,
        bail_out_block, this_val, pool_val, tuple_val, data_val, data_end_val);
    if (!status.ok()) {
      VLOG_QUERY << status.GetDetail();
      return status;
    }

    // Returns true on successful decoding.
    builder.CreateRet(codegen->true_value());

    // Returns false on decoding errors.
    builder.SetInsertPoint(bail_out_block);
    builder.CreateRet(codegen->false_value());

    if (codegen->FinalizeFunction(helper_fn) == nullptr) {
      return Status("Failed to finalize helper_fn.");
    }
    helper_functions.push_back(helper_fn);
  }

  // Actual MaterializeTuple. Call all the helper functions.
  {
    llvm::Value* args[6];
    prototype.SetName("MaterializeTuple");
    llvm::Function* fn = prototype.GeneratePrototype(&builder, args);

    // These are the blocks that we go to after the helper runs.
    std::vector<llvm::BasicBlock*> helper_blocks;
    for (int i = 0; i < helper_functions.size(); ++i) {
      llvm::BasicBlock* helper_block =
          llvm::BasicBlock::Create(context, "helper_" + std::to_string(i), fn, nullptr);
      helper_blocks.push_back(helper_block);
    }

    // Block for failures
    llvm::BasicBlock* bail_out_block =
        llvm::BasicBlock::Create(context, "bail_out", fn, nullptr);

    // Call the helpers.
    for (int i = 0; i < helper_functions.size(); ++i) {
      if (i != 0) builder.SetInsertPoint(helper_blocks[i - 1]);
      llvm::Function* fnHelper = helper_functions[i];
      llvm::Value* helper_ok =
          builder.CreateCall(fnHelper, args, "helper_" + std::to_string(i));
      builder.CreateCondBr(helper_ok, helper_blocks[i], bail_out_block);
    }

    // Return false on errors
    builder.SetInsertPoint(bail_out_block);
    builder.CreateRet(codegen->false_value());

    // And true on success
    builder.SetInsertPoint(helper_blocks[helper_blocks.size() - 1]);
    builder.CreateRet(codegen->true_value());

    *materialize_tuple_fn = codegen->FinalizeFunction(fn);
    if (*materialize_tuple_fn == nullptr) {
      return Status("Failed to finalize materialize_tuple_fn.");
    }
  }

  return Status::OK();
}

Status HdfsAvroScanner::CodegenReadRecord(const SchemaPath& path,
    const AvroSchemaElement& record, int child_start, int child_end,
    const HdfsScanPlanNode* node, LlvmCodeGen* codegen, void* void_builder,
    llvm::Function* fn, llvm::BasicBlock* insert_before, llvm::BasicBlock* bail_out,
    llvm::Value* this_val, llvm::Value* pool_val, llvm::Value* tuple_val,
    llvm::Value* data_val, llvm::Value* data_end_val) {
  RETURN_IF_ERROR(CheckSchema(record));
  DCHECK_EQ(record.schema->type, AVRO_RECORD);
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder* builder = reinterpret_cast<LlvmBuilder*>(void_builder);

  // Codegen logic for parsing each field and, if necessary, populating a slot with the
  // result.

  // Used to store result of ReadUnionType() call
  llvm::Value* is_null_ptr = nullptr;
  for (int i = child_start; i < child_end; ++i) {
    const AvroSchemaElement& field = record.children[i];
    int col_idx = i;
    // If we're about to process the table-level columns, account for the partition keys
    // when constructing 'path'
    if (path.empty()) col_idx += node->hdfs_table_->num_clustering_cols();
    SchemaPath new_path = path;
    new_path.push_back(col_idx);
    int slot_idx = node->GetMaterializedSlotIdx(new_path);
    SlotDescriptor* slot_desc = (slot_idx == HdfsScanNodeBase::SKIP_COLUMN) ?
                                nullptr : node->materialized_slots_[slot_idx];

    // Block that calls appropriate Read<Type> function
    llvm::BasicBlock* read_field_block =
        llvm::BasicBlock::Create(context, "read_field", fn, insert_before);

    // Block that handles a nullptr value. We fill this in below if the field is nullable,
    // otherwise we leave this block nullptr.
    llvm::BasicBlock* null_block = nullptr;

    // This is where we should end up after we're finished processing this field. Used to
    // put the builder in the right place for the next field.
    llvm::BasicBlock* end_field_block =
        llvm::BasicBlock::Create(context, "end_field", fn, insert_before);

    if (field.nullable()) {
      // Field could be null. Create conditional branch based on ReadUnionType result.
      llvm::Function* read_union_fn =
          codegen->GetFunction(IRFunction::READ_UNION_TYPE, false);
      llvm::Value* null_union_pos_val =
          codegen->GetI32Constant(field.null_union_position);
      if (is_null_ptr == nullptr) {
        is_null_ptr = codegen->CreateEntryBlockAlloca(*builder, codegen->bool_type(),
            "is_null_ptr");
      }
      llvm::Value* is_null_ptr_cast =
          builder->CreateBitCast(is_null_ptr, codegen->ptr_type());
      llvm::Value* read_union_ok = builder->CreateCall(read_union_fn,
          llvm::ArrayRef<llvm::Value*>(
              {this_val, null_union_pos_val, data_val, data_end_val, is_null_ptr_cast}),
          "read_union_ok");
      llvm::BasicBlock* read_union_ok_block =
          llvm::BasicBlock::Create(context, "read_union_ok", fn, read_field_block);
      builder->CreateCondBr(read_union_ok, read_union_ok_block, bail_out);

      builder->SetInsertPoint(read_union_ok_block);
      null_block = llvm::BasicBlock::Create(context, "null_field", fn, end_field_block);
      llvm::Value* is_null = builder->CreateLoad(is_null_ptr, "is_null");
      builder->CreateCondBr(is_null, null_block, read_field_block);

      // Write null field IR
      builder->SetInsertPoint(null_block);
      if (slot_idx != HdfsScanNodeBase::SKIP_COLUMN) {
        slot_desc->CodegenSetNullIndicator(
            codegen, builder, tuple_val, codegen->true_value());
      }
      // LLVM requires all basic blocks to end with a terminating instruction
      builder->CreateBr(end_field_block);
    } else {
      // Field is never null, read field unconditionally.
      builder->CreateBr(read_field_block);
    }

    // Write read_field_block IR
    builder->SetInsertPoint(read_field_block);
    llvm::Value* ret_val = nullptr;
    if (field.schema->type == AVRO_RECORD) {
      llvm::BasicBlock* insert_before_block =
          (null_block != nullptr) ? null_block : end_field_block;
      RETURN_IF_ERROR(CodegenReadRecord(new_path, field, 0, field.children.size(),
          node, codegen, builder, fn,
          insert_before_block, bail_out, this_val, pool_val, tuple_val, data_val,
          data_end_val));
    } else {
      RETURN_IF_ERROR(CodegenReadScalar(field, slot_desc, codegen, builder,
          this_val, pool_val, tuple_val, data_val, data_end_val, &ret_val));
    }
    builder->CreateCondBr(ret_val, end_field_block, bail_out);

    // Set insertion point for next field.
    builder->SetInsertPoint(end_field_block);
  }
  return Status::OK();
}

Status HdfsAvroScanner::CodegenReadScalar(const AvroSchemaElement& element,
    SlotDescriptor* slot_desc, LlvmCodeGen* codegen, void* void_builder,
    llvm::Value* this_val, llvm::Value* pool_val, llvm::Value* tuple_val,
    llvm::Value* data_val, llvm::Value* data_end_val, llvm::Value** ret_val) {
  LlvmBuilder* builder = reinterpret_cast<LlvmBuilder*>(void_builder);
  llvm::Function* read_field_fn;
  switch (element.schema->type) {
    case AVRO_BOOLEAN:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_BOOLEAN, false);
      break;
    case AVRO_DATE:
      if (slot_desc != nullptr && slot_desc->type().type == TYPE_INT) {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_INT32, false);
      } else {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_DATE, false);
      }
      break;
    case AVRO_INT32:
      if (slot_desc != nullptr && slot_desc->type().type == TYPE_DATE) {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_DATE, false);
      } else {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_INT32, false);
      }
      break;
    case AVRO_INT64:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_INT64, false);
      break;
    case AVRO_FLOAT:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_FLOAT, false);
      break;
    case AVRO_DOUBLE:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_DOUBLE, false);
      break;
    case AVRO_STRING:
    case AVRO_BYTES:
      if (slot_desc != nullptr && slot_desc->type().type == TYPE_VARCHAR) {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_VARCHAR, false);
      } else if (slot_desc != nullptr && slot_desc->type().type == TYPE_CHAR) {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_CHAR, false);
      } else {
        read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_STRING, false);
      }
      break;
    case AVRO_DECIMAL:
      read_field_fn = codegen->GetFunction(IRFunction::READ_AVRO_DECIMAL, false);
      break;
    default:
      return Status::Expected(Substitute(
          "Failed to codegen MaterializeTuple() due to unsupported type: $0",
          element.schema->type));
  }

  // Call appropriate ReadAvro<Type> function
  llvm::Value* write_slot_val = builder->getFalse();
  llvm::Value* slot_type_val = builder->getInt32(0);
  llvm::Value* opaque_slot_val = codegen->null_ptr_value();
  if (slot_desc != nullptr) {
    // Field corresponds to a materialized column, fill in relevant arguments
    write_slot_val = builder->getTrue();
    if (slot_desc->type().type == TYPE_DECIMAL) {
      // ReadAvroDecimal() takes slot byte size instead of slot type
      slot_type_val = builder->getInt32(slot_desc->type().GetByteSize());
    } else {
      slot_type_val = builder->getInt32(slot_desc->type().type);
    }
    llvm::Value* slot_val =
        builder->CreateStructGEP(nullptr, tuple_val, slot_desc->llvm_field_idx(), "slot");
    opaque_slot_val =
        builder->CreateBitCast(slot_val, codegen->ptr_type(), "opaque_slot");
  }

  // NOTE: ReadAvroVarchar/Char has different signature than rest of read functions
  if (slot_desc != nullptr &&
      (slot_desc->type().type == TYPE_VARCHAR || slot_desc->type().type == TYPE_CHAR)) {
    // Need to pass an extra argument (the length) to the codegen function.
    llvm::Value* fixed_len = builder->getInt32(slot_desc->type().len);
    llvm::Value* read_field_args[] = {this_val, slot_type_val, fixed_len, data_val,
        data_end_val, write_slot_val, opaque_slot_val, pool_val};
    *ret_val = builder->CreateCall(read_field_fn, read_field_args, "success");
  } else {
    llvm::Value* read_field_args[] = {this_val, slot_type_val, data_val, data_end_val,
        write_slot_val, opaque_slot_val, pool_val};
    *ret_val = builder->CreateCall(read_field_fn, read_field_args, "success");
  }
  return Status::OK();
}

Status HdfsAvroScanner::CodegenDecodeAvroData(const HdfsScanPlanNode* node,
    FragmentState* state, llvm::Function** decode_avro_data_fn) {
  const vector<ScalarExpr*>& conjuncts = node->conjuncts_;
  LlvmCodeGen* codegen = state->codegen();

  llvm::Function* materialize_tuple_fn;
  RETURN_IF_ERROR(CodegenMaterializeTuple(node, codegen, &materialize_tuple_fn));
  DCHECK(materialize_tuple_fn != nullptr);

  llvm::Function* fn = codegen->GetFunction(IRFunction::DECODE_AVRO_DATA, true);

  llvm::Function* init_tuple_fn;
  RETURN_IF_ERROR(CodegenInitTuple(node, codegen, &init_tuple_fn));
  int replaced = codegen->ReplaceCallSites(fn, init_tuple_fn, "InitTuple");
  DCHECK_REPLACE_COUNT(replaced, 1);

  replaced = codegen->ReplaceCallSites(fn, materialize_tuple_fn, "MaterializeTuple");
  DCHECK_REPLACE_COUNT(replaced, 1);

  llvm::Function* eval_conjuncts_fn;
  RETURN_IF_ERROR(ExecNode::CodegenEvalConjuncts(codegen, conjuncts, &eval_conjuncts_fn));

  replaced = codegen->ReplaceCallSites(fn, eval_conjuncts_fn, "EvalConjuncts");
  DCHECK_REPLACE_COUNT(replaced, 1);

  llvm::Function* copy_strings_fn;
  RETURN_IF_ERROR(Tuple::CodegenCopyStrings(
      codegen, *node->tuple_desc_, &copy_strings_fn));
  replaced = codegen->ReplaceCallSites(fn, copy_strings_fn, "CopyStrings");
  DCHECK_REPLACE_COUNT(replaced, 1);

  int tuple_byte_size = node->tuple_desc_->byte_size();
  replaced = codegen->ReplaceCallSitesWithValue(fn,
      codegen->GetI32Constant(tuple_byte_size), "tuple_byte_size");
  DCHECK_REPLACE_COUNT(replaced, 1);

  fn->setName("DecodeAvroData");
  *decode_avro_data_fn = codegen->FinalizeFunction(fn);
  if (*decode_avro_data_fn == nullptr) {
    return Status("Failed to finalize decode_avro_data_fn.");
  }
  return Status::OK();
}

int HdfsAvroScanner::DecodeAvroDataCodegenOrInterpret(int max_tuples, MemPool* pool,
    uint8_t** data, uint8_t* data_end, Tuple* tuple, TupleRow* tuple_row) {
  return CallCodegendOrInterpreted<DecodeAvroDataFn>::invoke(this,
      codegend_decode_avro_data_, &HdfsAvroScanner::DecodeAvroData, max_tuples, pool,
      &data_block_, data_block_end_, tuple, tuple_row);
}
