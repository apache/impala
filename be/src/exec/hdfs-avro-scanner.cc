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

// Avro headers
#include <Compiler.hh>
#include <Node.hh>
#include <ValidSchema.hh>

#include <boost/array.hpp>
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exec/scanner-context.inline.h"
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

HdfsAvroScanner::HdfsAvroScanner(HdfsScanNode* scan_node, RuntimeState* state)
    : BaseSequenceScanner(scan_node, state, /* marker_precedes_sync */ false) {
}

HdfsAvroScanner::~HdfsAvroScanner() {
}

Status HdfsAvroScanner::Prepare() {
  RETURN_IF_ERROR(BaseSequenceScanner::Prepare());
  slot_descs_.resize(scan_node_->num_cols() - scan_node_->num_partition_keys(), NULL);
  const vector<SlotDescriptor*>& materialized_slots = scan_node_->materialized_slots();
  vector<SlotDescriptor*>::const_iterator it;
  // Populate slot_descs_ with slot descriptors of materialized columns (entries for
  // non-materialized columns are left NULL)
  for (it = materialized_slots.begin(); it != materialized_slots.end(); ++it) {
    const SlotDescriptor* slot_desc = *it;
    int column = slot_desc->col_pos() - scan_node_->num_partition_keys();
    DCHECK_LT(column, slot_descs_.size());
    slot_descs_[column] = slot_desc;
  }
  scan_node_->IncNumScannersCodegenDisabled();
  return Status::OK;
}

BaseSequenceScanner::FileHeader* HdfsAvroScanner::AllocateFileHeader() {
  return new AvroFileHeader;
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

  header_->header_size = stream_->total_bytes_returned();
  return Status::OK;
}

Status HdfsAvroScanner::ParseMetadata() {
  bool found_schema = false;
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
        found_schema = true;
        avro::ValidSchema schema;
        try {
          schema = avro::compileJsonSchemaFromMemory(value, value_len);
        } catch (exception& e) {
          stringstream ss;
          ss << "Failed to parse schema: '" << e.what();
          return Status(ss.str());
        }
        const avro::NodePtr& root = schema.root();
        if (root->type() != avro::AVRO_RECORD) {
          stringstream ss;
          ss << "Expected record type, got " << root->type();
          return Status(ss.str());
        }
        for (int i = 0; i < root->leaves(); ++i) {
          SchemaElement element = ConvertSchemaNode(*(root->leafAt(i).get()));
          if (element.type >= SchemaElement::COMPLEX_TYPE) {
            stringstream ss;
            ss << "Complex Avro types unsupported: " << element.type;
            return Status(ss.str());
          }
          avro_header_->schema.push_back(element);
        }
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
  if (!found_schema) return Status("Schema not found in file header metadata");
  // TODO: check that schema matches table column types
  return Status::OK;
}

HdfsAvroScanner::SchemaElement HdfsAvroScanner::ConvertSchemaNode(
    const avro::Node& node) {
  SchemaElement element;
  element.null_union_position = -1;
  switch (node.type()) {
    case avro::AVRO_UNION:
      element.type = SchemaElement::UNION;

      // Look for special case of [<primitive type>, "null"] union
      if (node.leaves() == 2) {
        int null_position;
        if (node.leafAt(0)->type() == avro::AVRO_NULL) {
          null_position = 0;
        } else if (node.leafAt(1)->type() == avro::AVRO_NULL) {
          null_position = 1;
        } else {
          break;
        }
        DCHECK(null_position == 0 || null_position == 1);

        // The non-null child of node
        SchemaElement child = ConvertSchemaNode(*(node.leafAt(!null_position).get()));

        // node is a [<child>, "null"] union. If child is a primitive type (i.e., not a
        // complex type nor a [<primitive type>, "null"] union itself), we treat this node
        // as the same type as child except with null_union_position set appropriately.
        if (child.type < SchemaElement::COMPLEX_TYPE && child.null_union_position == -1) {
          element = child;
          element.null_union_position = null_position;
        }
      }
      break;
    case avro::AVRO_NULL:
      element.type = SchemaElement::NULL_TYPE;
      break;
    case avro::AVRO_BOOL:
      element.type = SchemaElement::BOOLEAN;
      break;
    case avro::AVRO_INT:
      element.type = SchemaElement::INT;
      break;
    case avro::AVRO_LONG:
      element.type = SchemaElement::LONG;
      break;
    case avro::AVRO_FLOAT:
      element.type = SchemaElement::FLOAT;
      break;
    case avro::AVRO_DOUBLE:
      element.type = SchemaElement::DOUBLE;
      break;
    case avro::AVRO_STRING:
      element.type = SchemaElement::STRING;
      break;
    case avro::AVRO_BYTES:
      element.type = SchemaElement::BYTES;
      break;
    case avro::AVRO_FIXED:
      element.type = SchemaElement::FIXED;
      break;
    case avro::AVRO_RECORD:
      element.type = SchemaElement::RECORD;
      break;
    case avro::AVRO_ENUM:
      element.type = SchemaElement::ENUM;
      break;
    case avro::AVRO_ARRAY:
      element.type = SchemaElement::ARRAY;
      break;
    case avro::AVRO_MAP:
      element.type = SchemaElement::MAP;
      break;
    default:
      DCHECK(false) << "Unknown avro::Node type: " << node.type();
  }
  for (int i = 0; i < node.leaves(); ++i) {
    element.children.push_back(ConvertSchemaNode(*(node.leafAt(i).get())));
  }
  return element;
}

Status HdfsAvroScanner::InitNewRange() {
  DCHECK(header_ != NULL);
  only_parsing_header_ = false;
  avro_header_ = reinterpret_cast<AvroFileHeader*>(header_);
  if (header_->is_compressed) {
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
        data_buffer_pool_.get(), stream_->compact_data(),
        header_->compression_type, &decompressor_));
  }
  return Status::OK;
}

Status HdfsAvroScanner::ProcessRange() {
  while (!finished()) {
    // Read new data block
    block_start_ = stream_->file_offset();

    int64_t num_records;
    uint8_t* compressed_data;
    int64_t compressed_size;
    uint8_t* data;
    int size;

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
      size = 0; // signal unknown output size
      SCOPED_TIMER(decompress_timer_);
      RETURN_IF_ERROR(decompressor_->ProcessBlock(
          compressed_size, compressed_data, &size, &data));
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
      int max_tuples = context_->GetMemory(&pool, &tuple, &tuple_row);
      if (scan_node_->materialized_slots().empty()) {
        // No slots to materialize (e.g. count(*)), no need to decode data
        int n = min(num_records, static_cast<int64_t>(max_tuples));
        int num_to_commit = WriteEmptyTuples(context_, tuple_row, n);
        if (num_to_commit > 0) context_->CommitRows(num_to_commit);
        num_records -= n;
        COUNTER_UPDATE(scan_node_->rows_read_counter(), n);
      } else {
        RETURN_IF_ERROR(DecodeAvroData(max_tuples, &num_records, pool, &data, &size,
                                       tuple, tuple_row));
      }
      if (scan_node_->ReachedLimit()) return Status::OK;
      if (context_->cancelled()) return Status::CANCELLED;
    }

    if (!stream_->compact_data()) {
      context_->AcquirePool(data_buffer_pool_.get());
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
    InitTuple(context_->template_tuple(), tuple);

    // Decode record
    for (int j = 0; j < slot_descs_.size(); ++j) {
      const SlotDescriptor* slot_desc = slot_descs_[j];
      const SchemaElement& element = avro_header_->schema[j];
      if (slot_desc != NULL) {
        DCHECK_LE(element.type, SchemaElement::COMPLEX_TYPE);
        RETURN_IF_FALSE(ReadPrimitive(element, *slot_desc, pool, data, data_len, tuple));
      } else {
        // Non-materialized column, skip
        if (LIKELY(element.type < SchemaElement::COMPLEX_TYPE)) {
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
      tuple_row = context_->next_row(tuple_row);
      tuple = context_->next_tuple(tuple);
    }
  }
  context_->CommitRows(num_to_commit);
  (*num_records) -= n;
  COUNTER_UPDATE(scan_node_->rows_read_counter(), n);

  return Status::OK;
}

inline bool HdfsAvroScanner::ReadPrimitive(
    const SchemaElement& element, const SlotDescriptor& slot_desc, MemPool* pool,
    uint8_t** data, int* data_len, Tuple* tuple) {
  void* slot = tuple->GetSlot(slot_desc.tuple_offset());
  SchemaElement::Type type;
  if (!ReadUnionType(element, data, data_len, &type)) return false;

  switch (type) {
    case SchemaElement::NULL_TYPE:
      tuple->SetNull(slot_desc.null_indicator_offset());
      return true;
    case SchemaElement::BOOLEAN:
      DCHECK_EQ(slot_desc.type(), TYPE_BOOLEAN) << slot_desc.type();
      return ReadWriteUtil::Read<bool>(
          data, data_len, reinterpret_cast<bool*>(slot), &parse_status_);
    case SchemaElement::INT: {
      int32_t val;
      if (!ReadWriteUtil::ReadZInt(data, data_len, &val, &parse_status_)) return false;
      // When creating a table from an Avro record schema, Hive maps Avro
      // "int" fields to INT table columns and Avro "long" fields to BIGINT
      // table columns. However, when writing a data file, Hive sometimes
      // uses a different schema than that originally specified and uses an
      // "int" field to represent a BIGINT column (and sometimes uses a
      // "long" field as one would expect). Thus, an "int" field may
      // correspond to either an INT or BIGINT table column.
      if (slot_desc.type() == TYPE_INT) {
        *reinterpret_cast<int32_t*>(slot) = val;
      } else if (slot_desc.type() == TYPE_BIGINT) {
        *reinterpret_cast<int64_t*>(slot) = val;
      } else {
        DCHECK(false) << slot_desc.type();
      }
      return true;
    }
    case SchemaElement::LONG:
      DCHECK_EQ(slot_desc.type(), TYPE_BIGINT) << slot_desc.type();
      return ReadWriteUtil::ReadZLong(
          data, data_len, reinterpret_cast<int64_t*>(slot), &parse_status_);
    case SchemaElement::FLOAT:
      DCHECK_EQ(slot_desc.type(), TYPE_FLOAT) << slot_desc.type();
      return ReadWriteUtil::Read<float>(
          data, data_len, reinterpret_cast<float*>(slot), &parse_status_);
    case SchemaElement::DOUBLE:
      DCHECK_EQ(slot_desc.type(), TYPE_DOUBLE) << slot_desc.type();
      return ReadWriteUtil::Read<double>(
          data, data_len, reinterpret_cast<double*>(slot), &parse_status_);
    case SchemaElement::STRING: {
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
  SchemaElement::Type type;
  if (!ReadUnionType(element, data, data_len, &type)) return false;

  switch (type) {
    case SchemaElement::NULL_TYPE:
      return true;
    case SchemaElement::BOOLEAN:
      return ReadWriteUtil::SkipBytes(data, data_len, 1, &parse_status_);
    case SchemaElement::INT:
    case SchemaElement::LONG:
      int64_t dummy;
      return ReadWriteUtil::ReadZLong(data, data_len, &dummy, &parse_status_);
    case SchemaElement::FLOAT:
      return ReadWriteUtil::SkipBytes(data, data_len, 4, &parse_status_);
    case SchemaElement::DOUBLE:
      return ReadWriteUtil::SkipBytes(data, data_len, 8, &parse_status_);
    case SchemaElement::BYTES:
    case SchemaElement::STRING:
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
                                           int* data_len, SchemaElement::Type* type) {
  *type = element.type;
  if (element.null_union_position != -1) {
    DCHECK(element.null_union_position == 0 || element.null_union_position == 1);
    int64_t union_position;
    if (!ReadWriteUtil::ReadZLong(data, data_len, &union_position, &parse_status_)) {
      return false;
    }
    if (union_position == element.null_union_position) {
      *type = SchemaElement::NULL_TYPE;
    }
  }
  return true;
}
