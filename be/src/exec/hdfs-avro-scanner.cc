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
#include <boost/array.hpp>
#include <boost/foreach.hpp>
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exec/scanner-context.inline.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "util/codec.h"
#include "util/runtime-profile.h"

// Note: the Avro C++ library uses exceptions for error handling. Any Avro
// function that may throw an exception must be placed in a try/catch block.

using namespace impala;
using namespace std;

const uint8_t HdfsAvroScanner::AVRO_VERSION_HEADER[4] = {'O', 'b', 'j', 1};

const string HdfsAvroScanner::AVRO_SCHEMA_KEY("avro.schema");
const string HdfsAvroScanner::AVRO_CODEC_KEY("avro.codec");
const string HdfsAvroScanner::AVRO_NULL_CODEC("null");
const string HdfsAvroScanner::AVRO_SNAPPY_CODEC("snappy");
const string HdfsAvroScanner::AVRO_DEFLATE_CODEC("deflate");

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

// Wrapper for avro_schema_t's that handles decrementing the ref count
struct ScopedAvroSchemaT {
  ScopedAvroSchemaT(avro_schema_t s = NULL) : schema(s) { }

  // avro_schema_decref can handle NULL
  ~ScopedAvroSchemaT() { avro_schema_decref(schema); }

  avro_schema_t operator->() const { return schema; }

  ScopedAvroSchemaT& operator=(const avro_schema_t& s) {
    if (LIKELY(s != schema)) {
      avro_schema_decref(schema);
      schema = s;
    }
    return *this;
  }

  avro_schema_t schema;

 private:
  // Disable copy constructor and assignment
  ScopedAvroSchemaT(const ScopedAvroSchemaT&);
  ScopedAvroSchemaT& operator=(const ScopedAvroSchemaT&);
};

HdfsAvroScanner::HdfsAvroScanner(HdfsScanNode* scan_node, RuntimeState* state)
  : BaseSequenceScanner(scan_node, state),
    avro_header_(NULL) {
}

HdfsAvroScanner::~HdfsAvroScanner() {
}

Status HdfsAvroScanner::Prepare(ScannerContext* context) {
  RETURN_IF_ERROR(BaseSequenceScanner::Prepare(context));
  scan_node_->IncNumScannersCodegenDisabled();
  return Status::OK;
}

BaseSequenceScanner::FileHeader* HdfsAvroScanner::AllocateFileHeader() {
  AvroFileHeader* header = new AvroFileHeader;
  header->template_tuple = template_tuple_;
  header->default_data_pool.reset(new MemPool(state_->mem_limits()));
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
        ScopedAvroSchemaT file_schema;
        int error = avro_schema_from_json_length(
            reinterpret_cast<char*>(value), value_len, &file_schema.schema);
        if (error != 0) {
          stringstream ss;
          ss << "Failed to parse file schema: " << avro_strerror();
          return Status(ss.str());
        }

        const string& table_schema_str = scan_node_->hdfs_table()->avro_schema();
        DCHECK_GT(table_schema_str.size(), 0);
        ScopedAvroSchemaT table_schema;
        error = avro_schema_from_json_length(
            table_schema_str.c_str(), table_schema_str.size(), &table_schema.schema);
        if (error != 0) {
          stringstream ss;
          ss << "Failed to parse table schema: " << avro_strerror();
          return Status(ss.str());
        }
        RETURN_IF_ERROR(ResolveSchemas(table_schema.schema, file_schema.schema));
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
    SchemaElement element = ConvertSchemaNode(file_field);
    if (element.type >= COMPLEX_TYPE) {
      stringstream ss;
      ss << "Complex Avro types unsupported: " << element.type;
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
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, element.type));

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
      SchemaElement table_element = ConvertSchemaNode(table_field);
      RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, table_element.type));
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
    RETURN_IF_ERROR(VerifyTypesMatch(slot_desc, default_value->type));

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
        char* v;
        if (avro_string_get(default_value, &v)) DCHECK(false);
        StringValue sv(v);
        RawValue::Write(&sv, avro_header_->template_tuple, slot_desc,
                        avro_header_->default_data_pool.get());
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

HdfsAvroScanner::SchemaElement HdfsAvroScanner::ConvertSchemaNode(
    const avro_schema_t& node) {
  SchemaElement element;
  element.type = node->type;
  element.null_union_position = -1;

  // Look for special case of [<primitive type>, "null"] union
  if (element.type == AVRO_UNION) {
    int num_fields = avro_schema_union_size(node);
    DCHECK_GT(num_fields, 0);
    if (num_fields == 2) {
      avro_schema_t child0 = avro_schema_union_branch(node, 0);
      avro_schema_t child1 = avro_schema_union_branch(node, 1);
      int null_position = -1;
      if (child0->type == AVRO_NULL) {
        null_position = 0;
      } else if (child1->type == AVRO_NULL) {
        null_position = 1;
      }

      if (null_position != -1) {
        avro_schema_t non_null_child = null_position == 0 ? child1 : child0;
        SchemaElement child = ConvertSchemaNode(non_null_child);

        // node is a [<child>, "null"] union. If child is a primitive type (i.e., not a
        // complex type nor a [<primitive type>, "null"] union itself), we treat this node
        // as the same type as child except with null_union_position set appropriately.
        if (child.type < COMPLEX_TYPE && child.null_union_position == -1) {
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
                                         avro_type_t avro_type) {
  switch (avro_type) {
    case AVRO_NULL:
      // All Impala types are nullable
      return Status::OK;
    case AVRO_STRING:
    case AVRO_BYTES:
      if (slot_desc->type() == TYPE_STRING) return Status::OK;
      break;
    case AVRO_INT32:
      if (slot_desc->type() == TYPE_INT) return Status::OK;
      // Type promotion
      if (slot_desc->type() == TYPE_BIGINT) return Status::OK;
      if (slot_desc->type() == TYPE_FLOAT) return Status::OK;
      if (slot_desc->type() == TYPE_DOUBLE) return Status::OK;
      break;
    case AVRO_INT64:
      if (slot_desc->type() == TYPE_BIGINT) return Status::OK;
      // Type promotion
      if (slot_desc->type() == TYPE_FLOAT) return Status::OK;
      if (slot_desc->type() == TYPE_DOUBLE) return Status::OK;
      break;
    case AVRO_FLOAT:
      if (slot_desc->type() == TYPE_FLOAT) return Status::OK;
      // Type promotion
      if (slot_desc->type() == TYPE_DOUBLE) return Status::OK;
      break;
    case AVRO_DOUBLE:
      if (slot_desc->type() == TYPE_DOUBLE) return Status::OK;
      break;
    case AVRO_BOOLEAN:
      if (slot_desc->type() == TYPE_BOOLEAN) return Status::OK;
      break;
    default:
      break;
  }
  stringstream ss;
  ss << "Unresolvable column types (column " << slot_desc->col_pos() << "): "
     << "declared type = " << TypeToString(slot_desc->type()) << ", "
     << "Avro type = " << avro_type;
  return Status(ss.str());
}

Status HdfsAvroScanner::InitNewRange() {
  DCHECK(header_ != NULL);
  only_parsing_header_ = false;
  avro_header_ = reinterpret_cast<AvroFileHeader*>(header_);
  template_tuple_ = avro_header_->template_tuple;
  if (header_->is_compressed) {
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
        data_buffer_pool_.get(), stream_->compact_data(),
        header_->compression_type, &decompressor_));
  }
  return Status::OK;
}

Status HdfsAvroScanner::ProcessRange() {
  while (!finished()) {
    int64_t num_records;
    uint8_t* compressed_data;
    int64_t compressed_size;
    uint8_t* data;
    int size;

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
        compressed_size -= 4;
      }
      SCOPED_TIMER(decompress_timer_);
      RETURN_IF_ERROR(decompressor_->ProcessBlock(
          false, compressed_size, compressed_data, &size, &data));
    } else {
      data = compressed_data;
      size = compressed_size;
    }

    // Process block data
    while (num_records > 0) {
      SCOPED_TIMER(scan_node_->materialize_tuple_timer());

      MemPool* pool;
      Tuple* tuple;
      TupleRow* tuple_row;
      int max_tuples = GetMemory(&pool, &tuple, &tuple_row);
      if (scan_node_->materialized_slots().empty()) {
        // No slots to materialize (e.g. count(*)), no need to decode data
        int n = min(num_records, static_cast<int64_t>(max_tuples));
        int num_to_commit = WriteEmptyTuples(context_, tuple_row, n);
        num_records -= n;
        COUNTER_UPDATE(scan_node_->rows_read_counter(), n);
        RETURN_IF_ERROR(CommitRows(num_to_commit));
      } else {
        RETURN_IF_ERROR(DecodeAvroData(max_tuples, &num_records, pool, &data, &size,
                                       tuple, tuple_row));
      }
      if (scan_node_->ReachedLimit()) return Status::OK;
    }

    if (!stream_->compact_data()) {
      AttachPool(data_buffer_pool_.get());
    }
    RETURN_IF_ERROR(ReadSync());
  }

  return Status::OK;
}

Status HdfsAvroScanner::DecodeAvroData(int max_tuples, int64_t* num_records,
                                       MemPool* pool, uint8_t** data, int* data_len,
                                       Tuple* tuple, TupleRow* tuple_row) {
  int num_to_commit = 0;
  uint64_t n = min(*num_records, static_cast<int64_t>(max_tuples));
  for (int i = 0; i < n; ++i) {
    // Initialize tuple from the partition key template tuple before writing the
    // slots
    InitTuple(template_tuple_, tuple);

    // Decode record
    for (int j = 0; j < avro_header_->schema.size(); ++j) {
      const SchemaElement& element = avro_header_->schema[j];
      if (element.slot_desc != NULL) {
        RETURN_IF_FALSE(
            ReadPrimitive(element, *element.slot_desc, pool, data, data_len, tuple));
      } else {
        // Non-materialized column, skip
        if (LIKELY(element.type < COMPLEX_TYPE)) {
          RETURN_IF_FALSE(SkipPrimitive(element, data, data_len));
        } else {
          RETURN_IF_FALSE(SkipComplex(element, data, data_len));
        }
      }
    }

    tuple_row->SetTuple(scan_node_->tuple_idx(), tuple);

    // Evaluate the conjuncts and add the row to the batch
    if (ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, tuple_row)) {
      ++num_to_commit;
      tuple_row = next_row(tuple_row);
      tuple = next_tuple(tuple);
    }
  }
  (*num_records) -= n;
  COUNTER_UPDATE(scan_node_->rows_read_counter(), n);
  RETURN_IF_ERROR(CommitRows(num_to_commit));

  return Status::OK;
}

inline bool HdfsAvroScanner::ReadPrimitive(
    const SchemaElement& element, const SlotDescriptor& slot_desc, MemPool* pool,
    uint8_t** data, int* data_len, Tuple* tuple) {
  void* slot = tuple->GetSlot(slot_desc.tuple_offset());
  avro_type_t type;
  if (!ReadUnionType(element, data, data_len, &type)) return false;

  switch (type) {
    case AVRO_NULL:
      tuple->SetNull(slot_desc.null_indicator_offset());
      return true;
    case AVRO_BOOLEAN:
      DCHECK_EQ(slot_desc.type(), TYPE_BOOLEAN) << slot_desc.type();
      return ReadWriteUtil::Read<bool>(
          data, data_len, reinterpret_cast<bool*>(slot), &parse_status_);
    case AVRO_INT32: {
      int32_t val;
      if (!ReadWriteUtil::ReadZInt(data, data_len, &val, &parse_status_)) return false;
      if (slot_desc.type() == TYPE_INT) {
        *reinterpret_cast<int32_t*>(slot) = val;
      } else if (slot_desc.type() == TYPE_BIGINT) {
        *reinterpret_cast<int64_t*>(slot) = val;
      } else if (slot_desc.type() == TYPE_FLOAT) {
        *reinterpret_cast<float*>(slot) = val;
      } else if (slot_desc.type() == TYPE_DOUBLE) {
        *reinterpret_cast<double*>(slot) = val;
      } else {
        DCHECK(false) << slot_desc.type();
      }
      return true;
    }
    case AVRO_INT64: {
      int64_t val;
      if (!ReadWriteUtil::ReadZLong(data, data_len, &val, &parse_status_)) return false;
      if (slot_desc.type() == TYPE_BIGINT) {
        *reinterpret_cast<int64_t*>(slot) = val;
      } else if (slot_desc.type() == TYPE_FLOAT) {
        *reinterpret_cast<float*>(slot) = val;
      } else if (slot_desc.type() == TYPE_DOUBLE) {
        *reinterpret_cast<double*>(slot) = val;
      } else {
        DCHECK(false) << slot_desc.type();
      }
      return true;
    }
    case AVRO_FLOAT: {
      float val;
      if (!ReadWriteUtil::Read<float>(data, data_len, &val, &parse_status_)) return false;
      if (slot_desc.type() == TYPE_FLOAT) {
        *reinterpret_cast<float*>(slot) = val;
      } else if (slot_desc.type() == TYPE_DOUBLE) {
        *reinterpret_cast<double*>(slot) = val;
      } else {
        DCHECK(false) << slot_desc.type();
      }
      return true;
    }
    case AVRO_DOUBLE:
      DCHECK_EQ(slot_desc.type(), TYPE_DOUBLE) << slot_desc.type();
      return ReadWriteUtil::Read<double>(
          data, data_len, reinterpret_cast<double*>(slot), &parse_status_);
    case AVRO_STRING:
    case AVRO_BYTES: {
      DCHECK_EQ(slot_desc.type(), TYPE_STRING) << slot_desc.type();
      int64_t len;
      if (!ReadWriteUtil::ReadZLong(data, data_len, &len, &parse_status_)) return false;
      if (UNLIKELY(len < 0)) {
        stringstream ss;
        ss << "Negative string length: " << len;
        parse_status_ = Status(ss.str());
        return false;
      }
      // Save pointer and advance *data via SkipBytes before potentially memcpy'ing the
      // string data in case data_len < len (which is handled by SkipBytes).
      char* ptr = reinterpret_cast<char*>(*data);
      if (!ReadWriteUtil::SkipBytes(data, data_len, len, &parse_status_)) return false;
      StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
      str_slot->len = len;
      if (stream_->compact_data()) {
        str_slot->ptr = reinterpret_cast<char*>(pool->Allocate(len));
        memcpy(str_slot->ptr, ptr, len);
      } else {
        str_slot->ptr = ptr;
      }
      return true;
    }
    default:
      DCHECK(false) << "Unsupported SchemaElement: " << element.type;
  }
}

inline bool HdfsAvroScanner::SkipPrimitive(const SchemaElement& element, uint8_t** data,
                                           int* data_len) {
  avro_type_t type;
  if (!ReadUnionType(element, data, data_len, &type)) return false;

  switch (type) {
    case AVRO_NULL:
      return true;
    case AVRO_BOOLEAN:
      return ReadWriteUtil::SkipBytes(data, data_len, 1, &parse_status_);
    case AVRO_INT32:
    case AVRO_INT64:
      int64_t dummy;
      return ReadWriteUtil::ReadZLong(data, data_len, &dummy, &parse_status_);
    case AVRO_FLOAT:
      return ReadWriteUtil::SkipBytes(data, data_len, 4, &parse_status_);
    case AVRO_DOUBLE:
      return ReadWriteUtil::SkipBytes(data, data_len, 8, &parse_status_);
    case AVRO_STRING:
    case AVRO_BYTES:
      int64_t num_bytes;
      if (!ReadWriteUtil::ReadZLong(data, data_len, &num_bytes, &parse_status_)) {
        return false;
      }
      return ReadWriteUtil::SkipBytes(data, data_len, num_bytes, &parse_status_);
    default:
      DCHECK(false) << "Non-primitive SchemaElement: " << element.type;
      parse_status_ = Status("Non-primitive SchemaElement");
      return false;
  }
}

bool HdfsAvroScanner::SkipComplex(const SchemaElement& element, uint8_t** data,
                                  int* data_len) {
  // We should have failed in ParseMetadata
  DCHECK(false) << "SkipComplex unimplemented";
  parse_status_ = Status("SkipComplex unimplemented");
  return false;
}

inline bool HdfsAvroScanner::ReadUnionType(const SchemaElement& element, uint8_t** data,
                                           int* data_len, avro_type_t* type) {
  *type = element.type;
  if (element.null_union_position != -1) {
    DCHECK(element.null_union_position == 0 || element.null_union_position == 1);
    int64_t union_position;
    if (!ReadWriteUtil::ReadZLong(data, data_len, &union_position, &parse_status_)) {
      return false;
    }
    if (union_position == element.null_union_position) {
      *type = AVRO_NULL;
    }
  }
  return true;
}
