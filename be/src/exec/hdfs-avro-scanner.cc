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
#include <Generic.hh>
#include <Specific.hh>
#include <ValidSchema.hh>

#include <boost/array.hpp>
#include "exec/hdfs-scan-node.h"
#include "exec/serde-utils.inline.h"
#include "util/codec.h"

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

BaseSequenceScanner::FileHeader* HdfsAvroScanner::AllocateFileHeader() {
  return new AvroFileHeader;
}

Status HdfsAvroScanner::ReadFileHeader() {
  avro_header_ = reinterpret_cast<AvroFileHeader*>(header_);

  // Check version header
  uint8_t* header;
  RETURN_IF_FALSE(SerDeUtils::ReadBytes(
      context_, sizeof(AVRO_VERSION_HEADER), &header, &parse_status_));
  if (memcmp(header, AVRO_VERSION_HEADER, sizeof(AVRO_VERSION_HEADER))) {
    stringstream ss;
    ss << "Invalid AVRO_VERSION_HEADER: '"
       << SerDeUtils::HexDump(header, sizeof(AVRO_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  // Decode relevant metadata (encoded as Avro map)
  RETURN_IF_ERROR(ParseMetadata());

  // Read file sync marker
  uint8_t* sync;
  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context_, SYNC_HASH_SIZE, &sync, &parse_status_));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);

  header_->header_size = context_->total_bytes_returned();
  header_->file_type = THdfsFileFormat::AVRO;
  return Status::OK;
}

Status HdfsAvroScanner::ParseMetadata() {
  bool found_schema = false;
  header_->is_compressed = false;
  header_->compression_type = THdfsCompression::NONE;

  int64_t num_entries;
  RETURN_IF_FALSE(SerDeUtils::ReadZLong(context_, &num_entries, &parse_status_));
  if (num_entries < 1) return Status("File header metadata has no data");

  while (num_entries != 0) {
    DCHECK_GT(num_entries, 0);
    for (int i = 0; i < num_entries; ++i) {
      // Decode Avro string-type key
      string key;
      uint8_t* key_buf;
      int64_t key_len;
      RETURN_IF_FALSE(SerDeUtils::ReadZLong(context_, &key_len, &parse_status_));
      DCHECK_GE(key_len, 0);
      RETURN_IF_FALSE(SerDeUtils::ReadBytes(context_, key_len, &key_buf, &parse_status_));
      key = string(reinterpret_cast<char*>(key_buf), key_len);

      // Decode Avro bytes-type value
      uint8_t* value;
      int64_t value_len;
      RETURN_IF_FALSE(SerDeUtils::ReadZLong(context_, &value_len, &parse_status_));
      DCHECK_GE(value_len, 0);
      RETURN_IF_FALSE(SerDeUtils::ReadBytes(context_, value_len, &value, &parse_status_));

      if (key == AVRO_SCHEMA_KEY) {
        try {
          avro_header_->schema = avro::compileJsonSchemaFromMemory(value, value_len);
        } catch (exception& e) {
          stringstream ss;
          ss << "Failed to compile schema: '" << e.what();
          return Status(ss.str());
        }
        found_schema = true;
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
    RETURN_IF_FALSE(SerDeUtils::ReadZLong(context_, &num_entries, &parse_status_));
  }

  VLOG_FILE << context_->filename() << ": "
            << (header_->is_compressed ?  "compressed" : "not compressed");
  if (header_->is_compressed) VLOG_FILE << header_->codec;
  if (!found_schema) return Status("Schema not found in file header metadata");
  // TODO: check that schema matches table column types
  return Status::OK;
}

Status HdfsAvroScanner::InitNewRange() {
  DCHECK(header_ != NULL);
  only_parsing_header_ = false;
  avro_header_ = reinterpret_cast<AvroFileHeader*>(header_);
  template_tuple_ = context_->template_tuple();
  // We always copy strings out of IO buffers (I'm not sure how to avoid doing
  // this, see ReadRecord)
  context_->set_compact_data(true);
  if (header_->is_compressed) {
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
        data_buffer_pool_.get(), context_->compact_data(),
        header_->compression_type, &decompressor_));
  }
  return Status::OK;
}

Status HdfsAvroScanner::ProcessRange() {
  auto_ptr<avro::InputStream> stream;
  avro::DecoderPtr decoder(avro::binaryDecoder());
  avro::GenericDatum datum(avro_header_->schema);

  while (!finished()) {
    // Read new data block
    block_start_ = context_->file_offset();

    int64_t num_records;
    uint8_t* compressed_data;
    int64_t compressed_size;
    uint8_t* data;
    int size;

    RETURN_IF_FALSE(
        SerDeUtils::ReadZLong(context_, &num_records, &parse_status_));
    DCHECK_GE(num_records, 0);
    RETURN_IF_FALSE(SerDeUtils::ReadZLong(context_, &compressed_size, &parse_status_));
    DCHECK_GE(compressed_size, 0);
    RETURN_IF_FALSE(SerDeUtils::ReadBytes(
        context_, compressed_size, &compressed_data, &parse_status_));

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

    // Reset decoder. decoder->init() backs up old stream before switching to
    // new stream, so destruct *stream after calling init()
    auto_ptr<avro::InputStream> new_stream = avro::memoryInputStream(data, size);
    decoder->init(*new_stream);
    stream = new_stream;

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
      } else {
        RETURN_IF_ERROR(DecodeAvroData(decoder.get(), &datum, pool, tuple, tuple_row,
                                       max_tuples, &num_records));
      }
      if (scan_node_->ReachedLimit()) return Status::OK;
    }

    if (!context_->compact_data()) {
      context_->AcquirePool(data_buffer_pool_.get());
    }
    RETURN_IF_ERROR(ReadSync());
  }

  return Status::OK;
}

Status HdfsAvroScanner::DecodeAvroData(avro::Decoder* decoder, avro::GenericDatum* datum,
                                       MemPool* pool, Tuple* tuple, TupleRow* tuple_row,
                                       int max_tuples, int64_t* num_records) {
  // Decode records and write to tuples
  int num_to_commit = 0;
  uint64_t n = min(*num_records, static_cast<int64_t>(max_tuples));
  for (int i = 0; i < n; ++i) {
    try {
      avro::decode(*decoder, *datum);
    } catch (exception& e) {
      // This could mean we're not passing the decoder the right chunk of data
      // or a corrupted block.
      stringstream ss;
      ss << "Avro decoder raised exception: " << e.what();
      return Status(ss.str());
    }

    if (datum->type() == avro::AVRO_RECORD) {
      const avro::GenericRecord& record = datum->value<avro::GenericRecord>();
      RETURN_IF_ERROR(ReadRecord(record, pool, tuple));
      tuple_row->SetTuple(scan_node_->tuple_idx(), tuple);

      // Evaluate the conjuncts and add the row to the batch
      if (ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, tuple_row)) {
        ++num_to_commit;
        tuple_row = context_->next_row(tuple_row);
        tuple = context_->next_tuple(tuple);
      }
    } else {
      // TODO: does hive make/support non-record schemas?
      LOG(WARNING) << "Expected record type, got " << datum->type();
    }
  }
  context_->CommitRows(num_to_commit);
  (*num_records) -= n;

  return Status::OK;
}

inline Status HdfsAvroScanner::ReadRecord(const avro::GenericRecord& record,
                                          MemPool* pool, Tuple* tuple) {
  // Initialize tuple from the partition key template tuple before writing the
  // slots
  InitTuple(template_tuple_, tuple);

  const vector<SlotDescriptor*>& materialized_slots = scan_node_->materialized_slots();
  vector<SlotDescriptor*>::const_iterator it;
  for (it = materialized_slots.begin(); it != materialized_slots.end(); ++it) {
    const SlotDescriptor* slot_desc = *it;
    void* slot = tuple->GetSlot(slot_desc->tuple_offset());
    int field_idx = slot_desc->col_pos() - scan_node_->num_partition_keys();
    DCHECK_LT(field_idx, record.fieldCount());
    const avro::GenericDatum& field = record.fieldAt(field_idx);

    switch (field.type()) {
      case avro::AVRO_STRING: {
        DCHECK_EQ(slot_desc->type(), TYPE_STRING);
        // TODO: get rid of string copy
        const string& str = field.value<string>();
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->len = str.size();
        if (str.size() > 0) {
          str_slot->ptr = reinterpret_cast<char*>(pool->Allocate(str.size()));
          memcpy(str_slot->ptr, str.c_str(), str.size());
        }
        break;
      }
      case avro::AVRO_NULL:
        tuple->SetNull(slot_desc->null_indicator_offset());
        break;
      case avro::AVRO_BOOL:
        DCHECK_EQ(slot_desc->type(), TYPE_BOOLEAN);
        *reinterpret_cast<bool*>(slot) = field.value<bool>();
        break;
      case avro::AVRO_INT:
        // When creating a table from an Avro record schema, Hive maps Avro
        // "int" fields to INT table columns and Avro "long" fields to BIGINT
        // table columns. However, when writing a data file, Hive sometimes
        // uses a different schema than that originally specified and uses an
        // "int" field to represent a BIGINT column (and sometimes uses a
        // "long" field as one would expect). Thus, an "int" field may
        // correspond to either an INT or BIGINT table column.
        if (slot_desc->type() == TYPE_INT) {
          *reinterpret_cast<int32_t*>(slot) = field.value<int32_t>();
        } else if (slot_desc->type() == TYPE_BIGINT) {
          *reinterpret_cast<int64_t*>(slot) = field.value<int32_t>();
        } else {
          DCHECK(false);
        }
        break;
      case avro::AVRO_LONG:
        DCHECK_EQ(slot_desc->type(), TYPE_BIGINT);
        *reinterpret_cast<int64_t*>(slot) = field.value<int64_t>();
        break;
      case avro::AVRO_FLOAT:
        DCHECK_EQ(slot_desc->type(), TYPE_FLOAT);
        *reinterpret_cast<float*>(slot) = field.value<float>();
        break;
      case avro::AVRO_DOUBLE:
        DCHECK_EQ(slot_desc->type(), TYPE_DOUBLE);
        *reinterpret_cast<double*>(slot) = field.value<double>();
        break;
      default: {
        stringstream ss;
        ss << "Unsupported field type: " << field.type();
        return Status(ss.str());
      }
    }
  }
  return Status::OK;
}
