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

#include "exec/hdfs-avro-table-writer.h"

#include <vector>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <stdlib.h>
#include <gutil/strings/substitute.h>

#include "exec/exec-node.h"
#include "util/compress.h"
#include "util/hdfs-util.h"
#include "util/uid-util.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/runtime-profile-counters.h"
#include "write-stream.inline.h"

#include "common/names.h"

using namespace strings;
using namespace impala;

const uint8_t OBJ1[4] = {'O', 'b', 'j', 1};
const char* AVRO_SCHEMA_STR = "avro.schema";
const char* AVRO_CODEC_STR = "avro.codec";
const THdfsCompression::type AVRO_DEFAULT_CODEC = THdfsCompression::SNAPPY;
// Desired size of each Avro block (bytes); actual block size will vary +/- the
// size of a row. This is approximate size of the block before compression.
const int DEFAULT_AVRO_BLOCK_SIZE = 64 * 1024;

HdfsAvroTableWriter::HdfsAvroTableWriter(HdfsTableSink* parent,
    RuntimeState* state, OutputPartition* output,
    const HdfsPartitionDescriptor* partition, const HdfsTableDescriptor* table_desc,
    const vector<ExprContext*>& output_exprs) :
      HdfsTableWriter(parent, state, output, partition, table_desc, output_exprs),
      unflushed_rows_(0) {
  mem_pool_.reset(new MemPool(parent->mem_tracker()));
}

void HdfsAvroTableWriter::ConsumeRow(TupleRow* row) {
  ++unflushed_rows_;
  int num_non_partition_cols =
      table_desc_->num_cols() - table_desc_->num_clustering_cols();
  for (int j = 0; j < num_non_partition_cols; ++j) {
    void* value = output_expr_ctxs_[j]->GetValue(row);
    AppendField(output_expr_ctxs_[j]->root()->type(), value);
  }
}

inline void HdfsAvroTableWriter::AppendField(const ColumnType& type, const void* value) {
  // Each avro field is written as union, which is a ZLong indicating the union
  // field followed by the encoded value. Impala/Hive always stores values as
  // a union of [ColumnType, NULL].
  // TODO: the writer may be asked to write [NULL, ColumnType] unions. It is wrong
  // for us to assume [ColumnType, NULL].

  if (value == NULL) {
    // indicate the second field of the union
    out_.WriteZLong(1);
    // No bytes are written for a null value.
    return;
  }

  // indicate that we are using the first field of the union
  out_.WriteZLong(0);

  switch (type.type) {
    case TYPE_BOOLEAN:
      out_.WriteByte(*reinterpret_cast<const char*>(value));
      break;
    case TYPE_TINYINT:
      out_.WriteZInt(*reinterpret_cast<const int8_t*>(value));
      break;
    case TYPE_SMALLINT:
      out_.WriteZInt(*reinterpret_cast<const int16_t*>(value));
      break;
    case TYPE_INT:
      out_.WriteZInt(*reinterpret_cast<const int32_t*>(value));
      break;
    case TYPE_BIGINT:
      out_.WriteZLong(*reinterpret_cast<const int64_t*>(value));
      break;
    case TYPE_FLOAT:
      out_.WriteBytes(4, reinterpret_cast<const char*>(value));
      break;
    case TYPE_DOUBLE:
      out_.WriteBytes(8, reinterpret_cast<const char*>(value));
      break;
    case TYPE_STRING: {
      const StringValue& sv = *reinterpret_cast<const StringValue*>(value);
      out_.WriteZLong(sv.len);
      out_.WriteBytes(sv.len, sv.ptr);
      break;
    }
    case TYPE_DECIMAL: {
      int byte_size = ColumnType::GetDecimalByteSize(type.precision);
      out_.WriteZLong(byte_size);
#if __BYTE_ORDER == __LITTLE_ENDIAN
      char tmp[16];
      BitUtil::ByteSwap(tmp, value, byte_size);
      out_.WriteBytes(byte_size, tmp);
#else
      out_.WriteBytes(byte_size, reinterpret_cast<const char*>(value));
#endif
      break;
    }
    case TYPE_TIMESTAMP:
    case TYPE_BINARY:
    case INVALID_TYPE:
    case TYPE_NULL:
    case TYPE_DATE:
    case TYPE_DATETIME:
    default:
      DCHECK(false);
  }
}

Status HdfsAvroTableWriter::Init() {
  // create the Sync marker
  sync_marker_ = GenerateUUIDString();

  THdfsCompression::type codec = AVRO_DEFAULT_CODEC;
  if (state_->query_options().__isset.compression_codec) {
    codec = state_->query_options().compression_codec;
  }

  // sets codec_name_ and compressor_
  codec_type_ = codec;
  switch (codec) {
    case THdfsCompression::SNAPPY:
      codec_name_ = "snappy";
      break;
    case THdfsCompression::DEFLATE:
      codec_name_ = "deflate";
      break;
    case THdfsCompression::NONE:
      codec_name_ = "null";
      return Status::OK();
    default:
      const char* name = _THdfsCompression_VALUES_TO_NAMES.find(codec)->second;
      return Status(Substitute(
          "Avro only supports NONE, DEFLATE, and SNAPPY codecs; unsupported codec $0",
          name));
  }
  RETURN_IF_ERROR(Codec::CreateCompressor(mem_pool_.get(), true, codec, &compressor_));
  DCHECK(compressor_.get() != NULL);

  return Status::OK();
}

void HdfsAvroTableWriter::Close() {
  mem_pool_->FreeAll();
}

Status HdfsAvroTableWriter::AppendRows(
    RowBatch* batch, const vector<int32_t>& row_group_indices, bool* new_file) {
  int32_t limit;
  bool all_rows = row_group_indices.empty();
  if (all_rows) {
    limit = batch->num_rows();
  } else {
    limit = row_group_indices.size();
  }
  COUNTER_ADD(parent_->rows_inserted_counter(), limit);

  {
    SCOPED_TIMER(parent_->encode_timer());
    for (int row_idx = 0; row_idx < limit; ++row_idx) {
      TupleRow* row = all_rows ?
          batch->GetRow(row_idx) : batch->GetRow(row_group_indices[row_idx]);
      ConsumeRow(row);
    }
  }

  if (out_.Size() > DEFAULT_AVRO_BLOCK_SIZE) Flush();
  *new_file = false;
  return Status::OK();
}

Status HdfsAvroTableWriter::WriteFileHeader() {
  out_.Clear();
  out_.WriteBytes(4, reinterpret_cast<const uint8_t*>(OBJ1));

  // Write 'File Metadata' as an encoded avro map
  // number of key/value pairs in the map
  out_.WriteZLong(2);

  // Schema information
  out_.WriteZLong(strlen(AVRO_SCHEMA_STR));
  out_.WriteBytes(strlen(AVRO_SCHEMA_STR), AVRO_SCHEMA_STR);
  const string& avro_schema = table_desc_->avro_schema();
  out_.WriteZLong(avro_schema.size());
  out_.WriteBytes(avro_schema.size(), avro_schema.data());

  // codec information
  out_.WriteZLong(strlen(AVRO_CODEC_STR));
  out_.WriteBytes(strlen(AVRO_CODEC_STR), AVRO_CODEC_STR);
  out_.WriteZLong(codec_name_.size());
  out_.WriteBytes(codec_name_.size(), codec_name_.data());

  // Write end of map marker
  out_.WriteZLong(0);

  out_.WriteBytes(sync_marker_.size(), sync_marker_.data());

  const string& text = out_.String();
  RETURN_IF_ERROR(Write(reinterpret_cast<const uint8_t*>(text.c_str()),
                        text.size()));
  out_.Clear();
  return Status::OK();
}

Status HdfsAvroTableWriter::Flush() {
  if (unflushed_rows_ == 0) return Status::OK();

  WriteStream header;
  // 1. Count of objects in this block
  header.WriteZLong(unflushed_rows_);

  const uint8_t* output;
  int64_t output_length;
  // Snappy format requires a CRC after the compressed data
  uint32_t crc;
  const string& text = out_.String();

  if (codec_type_ != THdfsCompression::NONE) {
    SCOPED_TIMER(parent_->compress_timer());
    uint8_t* temp;
    RETURN_IF_ERROR(compressor_->ProcessBlock(false, text.size(),
        reinterpret_cast<const uint8_t*>(text.data()), &output_length, &temp));
    output = temp;
    if (codec_type_ == THdfsCompression::SNAPPY) {
      crc = SnappyCompressor::ComputeChecksum(
          text.size(), reinterpret_cast<const uint8_t*>(text.data()));
    }
  } else {
    output = reinterpret_cast<const uint8_t*>(text.data());
    output_length = out_.Size();
  }

  // 2. length of serialized objects
  if (codec_type_ == THdfsCompression::SNAPPY) {
    // + 4 for the CRC checksum at the end of the compressed block
    header.WriteZLong(output_length + 4);
  } else {
    header.WriteZLong(output_length);
  }

  const string& head = header.String();
  {
    SCOPED_TIMER(parent_->hdfs_write_timer());
    // Flush (1) and (2) to HDFS
    RETURN_IF_ERROR(
        Write(reinterpret_cast<const uint8_t*>(head.data()), head.size()));
    // 3. serialized objects
    RETURN_IF_ERROR(Write(output, output_length));

    // Write CRC checksum
    if (codec_type_ == THdfsCompression::SNAPPY) {
      RETURN_IF_ERROR(Write(reinterpret_cast<const uint8_t*>(&crc), sizeof(uint32_t)));
    }
  }

  // 4. sync marker
  RETURN_IF_ERROR(
      Write(reinterpret_cast<const uint8_t*>(sync_marker_.data()), sync_marker_.size()));

  out_.Clear();
  unflushed_rows_ = 0;
  return Status::OK();
}
