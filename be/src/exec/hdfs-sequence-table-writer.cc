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

#include "exec/hdfs-sequence-table-writer.h"
#include "exec/write-stream.inline.h"
#include "exec/exec-node.h"
#include "util/hdfs-util.h"
#include "util/uid-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/runtime-profile-counters.h"

#include <vector>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <stdlib.h>

#include "common/names.h"

namespace impala {

const uint8_t HdfsSequenceTableWriter::SEQ6_CODE[4] = {'S', 'E', 'Q', 6};
const char* HdfsSequenceTableWriter::VALUE_CLASS_NAME = "org.apache.hadoop.io.Text";
const char* HdfsSequenceTableWriter::KEY_CLASS_NAME =
    "org.apache.hadoop.io.BytesWritable";

HdfsSequenceTableWriter::HdfsSequenceTableWriter(HdfsTableSink* parent,
    RuntimeState* state, OutputPartition* output,
    const HdfsPartitionDescriptor* partition, const HdfsTableDescriptor* table_desc)
  : HdfsTableWriter(parent, state, output, partition, table_desc),
    mem_pool_(new MemPool(parent->mem_tracker())), compress_flag_(false),
    unflushed_rows_(0), record_compression_(false) {
  approx_block_size_ = 64 * 1024 * 1024;
  parent->mem_tracker()->Consume(approx_block_size_);
  field_delim_ = partition->field_delim();
  escape_char_ = partition->escape_char();
}

Status HdfsSequenceTableWriter::Init() {
  THdfsCompression::type codec = THdfsCompression::SNAPPY_BLOCKED;
  const TQueryOptions& query_options = state_->query_options();
  if (query_options.__isset.compression_codec) {
    codec = query_options.compression_codec;
    if (codec == THdfsCompression::SNAPPY) {
      // Seq file (and in general things that use hadoop.io.codec) always
      // mean snappy_blocked.
      codec = THdfsCompression::SNAPPY_BLOCKED;
    }
  }
  if (codec != THdfsCompression::NONE) {
    compress_flag_ = true;
    if (query_options.__isset.seq_compression_mode) {
      record_compression_ =
          query_options.seq_compression_mode == THdfsSeqCompressionMode::RECORD;
    }
    RETURN_IF_ERROR(Codec::GetHadoopCodecClassName(codec, &codec_name_));
    RETURN_IF_ERROR(Codec::CreateCompressor(
        mem_pool_.get(), true, codec_name_, &compressor_));
    DCHECK(compressor_.get() != NULL);
  }

  // create the Sync marker
  string uuid = GenerateUUIDString();
  uint8_t sync_neg1[20];

  ReadWriteUtil::PutInt(sync_neg1, static_cast<uint32_t>(-1));
  DCHECK(uuid.size() == 16);
  memcpy(sync_neg1 + sizeof(int32_t), uuid.data(), uuid.size());
  neg1_sync_marker_ = string(reinterpret_cast<char*>(sync_neg1), 20);
  sync_marker_ = uuid;

  return Status::OK();
}

Status HdfsSequenceTableWriter::AppendRows(
    RowBatch* batch, const vector<int32_t>& row_group_indices, bool* new_file) {
  int32_t limit;
  if (row_group_indices.empty()) {
    limit = batch->num_rows();
  } else {
    limit = row_group_indices.size();
  }
  COUNTER_ADD(parent_->rows_inserted_counter(), limit);

  bool all_rows = row_group_indices.empty();
  int num_non_partition_cols =
      table_desc_->num_cols() - table_desc_->num_clustering_cols();
  DCHECK_GE(output_expr_evals_.size(), num_non_partition_cols)
      << parent_->DebugString();

  {
    SCOPED_TIMER(parent_->encode_timer());
    if (all_rows) {
      for (int row_idx = 0; row_idx < limit; ++row_idx) {
        RETURN_IF_ERROR(ConsumeRow(batch->GetRow(row_idx)));
      }
    } else {
      for (int row_idx = 0; row_idx < limit; ++row_idx) {
        TupleRow* row = batch->GetRow(row_group_indices[row_idx]);
        RETURN_IF_ERROR(ConsumeRow(row));
      }
    }
  }

  if (!compress_flag_) {
    out_.WriteBytes(neg1_sync_marker_.size(), neg1_sync_marker_.data());
  }

  if (out_.Size() >= approx_block_size_) RETURN_IF_ERROR(Flush());
  *new_file = false;
  return Status::OK();
}

Status HdfsSequenceTableWriter::WriteFileHeader() {
  out_.WriteBytes(sizeof(SEQ6_CODE), SEQ6_CODE);

  // Setup to be correct key class
  out_.WriteText(strlen(KEY_CLASS_NAME),
      reinterpret_cast<const uint8_t*>(KEY_CLASS_NAME));

  // Setup to be correct value class
  out_.WriteText(strlen(VALUE_CLASS_NAME),
      reinterpret_cast<const uint8_t*>(VALUE_CLASS_NAME));

  // Flag for if compression is used
  out_.WriteBoolean(compress_flag_);
  // Only valid if compression is used. Indicates if block compression is used.
  out_.WriteBoolean(compress_flag_ && !record_compression_);

  // Output the name of our compression codec, parsed by readers
  if (compress_flag_) {
    out_.WriteText(codec_name_.size(),
        reinterpret_cast<const uint8_t*>(codec_name_.data()));
  }

  // Meta data is formated as an integer N followed by N*2 strings,
  // which are key-value pairs. Hive does not write meta data, so neither does Impala
  out_.WriteInt(0);

  // write the sync marker
  out_.WriteBytes(sync_marker_.size(), sync_marker_.data());

  string text = out_.String();
  RETURN_IF_ERROR(Write(reinterpret_cast<const uint8_t*>(text.c_str()), text.size()));
  out_.Clear();
  return Status::OK();
}

Status HdfsSequenceTableWriter::WriteCompressedBlock() {
  WriteStream record;
  uint8_t *output;
  int64_t output_length;
  DCHECK(compress_flag_);

  // Add a sync marker to start of the block
  record.WriteBytes(neg1_sync_marker_.size(), neg1_sync_marker_.data());

  // Output the number of rows in this block
  record.WriteVLong(unflushed_rows_);

  // Output compressed key-lengths block-size & compressed key-lengths block.
  // The key-lengths block contains byte value of 4 as a key length for each row (this is
  // what Hive does).
  string key_lengths_text(unflushed_rows_, '\x04');
  {
    SCOPED_TIMER(parent_->compress_timer());
    RETURN_IF_ERROR(compressor_->ProcessBlock(false, key_lengths_text.size(),
        reinterpret_cast<const uint8_t*>(key_lengths_text.data()), &output_length,
        &output));
  }
  record.WriteVInt(output_length);
  record.WriteBytes(output_length, output);

  // Output compressed keys block-size & compressed keys block.
  // The keys block contains "\0\0\0\0" byte sequence as a key for each row (this is what
  // Hive does).
  string keys_text(unflushed_rows_ * 4, '\0');
  {
    SCOPED_TIMER(parent_->compress_timer());
    RETURN_IF_ERROR(compressor_->ProcessBlock(false, keys_text.size(),
        reinterpret_cast<const uint8_t*>(keys_text.data()), &output_length, &output));
  }
  record.WriteVInt(output_length);
  record.WriteBytes(output_length, output);

  // Output compressed value-lengths block-size & compressed value-lengths block
  string value_lengths_text = out_value_lengths_block_.String();
  {
    SCOPED_TIMER(parent_->compress_timer());
    RETURN_IF_ERROR(compressor_->ProcessBlock(false, value_lengths_text.size(),
        reinterpret_cast<const uint8_t*>(value_lengths_text.data()), &output_length, &output));
  }
  record.WriteVInt(output_length);
  record.WriteBytes(output_length, output);

  // Output compressed values block-size & compressed values block
  string text = out_.String();
  {
    SCOPED_TIMER(parent_->compress_timer());
    RETURN_IF_ERROR(compressor_->ProcessBlock(false, text.size(),
        reinterpret_cast<const uint8_t*>(text.data()), &output_length, &output));
  }
  record.WriteVInt(output_length);
  record.WriteBytes(output_length, output);

  string rec = record.String();
  RETURN_IF_ERROR(Write(reinterpret_cast<const uint8_t*>(rec.data()), rec.size()));
  return Status::OK();
}

inline void HdfsSequenceTableWriter::WriteEscapedString(const StringValue* str_val,
                                                       WriteStream* buf) {
  for (int i = 0; i < str_val->len; ++i) {
    if (str_val->ptr[i] == field_delim_ || str_val->ptr[i] == escape_char_) {
      buf->WriteByte(escape_char_);
    }
    buf->WriteByte(str_val->ptr[i]);
  }
}

void HdfsSequenceTableWriter::EncodeRow(TupleRow* row, WriteStream* buf) {
  // TODO Unify with text table writer
  int num_non_partition_cols =
      table_desc_->num_cols() - table_desc_->num_clustering_cols();
  DCHECK_GE(output_expr_evals_.size(), num_non_partition_cols)
      << parent_->DebugString();
  for (int j = 0; j < num_non_partition_cols; ++j) {
    void* value = output_expr_evals_[j]->GetValue(row);
    if (value != NULL) {
      if (output_expr_evals_[j]->root().type().type == TYPE_STRING) {
        WriteEscapedString(reinterpret_cast<const StringValue*>(value), &row_buf_);
      } else {
        string str;
        output_expr_evals_[j]->PrintValue(value, &str);
        buf->WriteBytes(str.size(), str.data());
      }
    } else {
      // NULLs in hive are encoded based on the 'serialization.null.format' property.
      const string& null_val = table_desc_->null_column_value();
      buf->WriteBytes(null_val.size(), null_val.data());
    }
    // Append field delimiter.
    if (j + 1 < num_non_partition_cols) {
      buf->WriteByte(field_delim_);
    }
  }
}

inline Status HdfsSequenceTableWriter::ConsumeRow(TupleRow* row) {
  ++unflushed_rows_;
  row_buf_.Clear();
  if (compress_flag_ && !record_compression_) {
    // Output row for a block compressed sequence file.
    // Value block: Write the length as a vlong and then write the contents.
    EncodeRow(row, &row_buf_);
    out_.WriteVLong(row_buf_.Size());
    out_.WriteBytes(row_buf_.Size(), row_buf_.String().data());
    // Value-lengths block: Write the number of bytes we have just written to out_ as
    // vlong
    out_value_lengths_block_.WriteVLong(
        ReadWriteUtil::VLongRequiredBytes(row_buf_.Size()) + row_buf_.Size());
    return Status::OK();
  }

  EncodeRow(row, &row_buf_);

  const uint8_t* value_bytes;
  int64_t value_length;
  string text = row_buf_.String();
  if (compress_flag_) {
    // apply compression to row_buf_
    // the length of the buffer must be prefixed to the buffer prior to compression
    //
    // TODO this incurs copy overhead to place the length in front of the
    // buffer prior to compression. We may want to rewrite to avoid copying.
    row_buf_.Clear();
    // encoding as "Text" writes the length before the text
    row_buf_.WriteText(text.size(), reinterpret_cast<const uint8_t*>(&text.data()[0]));
    text = row_buf_.String();
    uint8_t *tmp;
    {
      SCOPED_TIMER(parent_->compress_timer());
      RETURN_IF_ERROR(compressor_->ProcessBlock(false, text.size(),
          reinterpret_cast<const uint8_t*>(text.data()), &value_length, &tmp));
    }
    value_bytes = tmp;
  } else {
    value_length = text.size();
    DCHECK_EQ(value_length, row_buf_.Size());
    value_bytes = reinterpret_cast<const uint8_t*>(text.data());
  }

  int rec_len = value_length;
  // if the record is compressed, the length is part of the compressed text
  // if not, then we need to write the length (below) and account for it's size
  if (!compress_flag_) {
    rec_len += ReadWriteUtil::VLongRequiredBytes(value_length);
  }
  // The record contains the key, account for it's size (we use "\0\0\0\0" byte sequence
  // as a key just like Hive).
  rec_len += 4;

  // Length of the record (incl. key and value length)
  out_.WriteInt(rec_len);

  // Write length of the key and the key
  out_.WriteInt(4);
  out_.WriteBytes(4, "\0\0\0\0");

  // if the record is compressed, the length is part of the compressed text
  if (!compress_flag_) out_.WriteVLong(value_length);

  // write out the value (possibly compressed)
  out_.WriteBytes(value_length, value_bytes);
  return Status::OK();
}

Status HdfsSequenceTableWriter::Flush() {
  if (unflushed_rows_ == 0) return Status::OK();

  SCOPED_TIMER(parent_->hdfs_write_timer());

  if (compress_flag_ && !record_compression_) {
    RETURN_IF_ERROR(WriteCompressedBlock());
  } else {
    string out_str = out_.String();
    RETURN_IF_ERROR(
        Write(reinterpret_cast<const uint8_t*>(out_str.data()), out_str.size()));
  }
  out_.Clear();
  out_value_lengths_block_.Clear();
  unflushed_rows_ = 0;
  return Status::OK();
}

void HdfsSequenceTableWriter::Close() {
  // TODO: double check there is no memory leak.
  parent_->mem_tracker()->Release(approx_block_size_);
  mem_pool_->FreeAll();
}

} // namespace impala
