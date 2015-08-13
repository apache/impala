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

#include "exec/hdfs-text-table-writer.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/codec.h"
#include "util/compress.h"
#include "util/hdfs-util.h"

#include <hdfs.h>
#include <stdlib.h>

#include "common/names.h"

// Hdfs block size for compressed text.
static const int64_t COMPRESSED_BLOCK_SIZE = 64 * 1024 * 1024;

// Size to buffer before compression. We want this to be less than the block size
// (compressed text is not splittable).
static const int64_t COMPRESSED_BUFFERED_SIZE = 60 * 1024 * 1024;

namespace impala {

HdfsTextTableWriter::HdfsTextTableWriter(HdfsTableSink* parent,
                                         RuntimeState* state, OutputPartition* output,
                                         const HdfsPartitionDescriptor* partition,
                                         const HdfsTableDescriptor* table_desc,
                                         const vector<ExprContext*>& output_expr_ctxs)
    : HdfsTableWriter(
        parent, state, output, partition, table_desc, output_expr_ctxs) {
  tuple_delim_ = partition->line_delim();
  field_delim_ = partition->field_delim();
  escape_char_ = partition->escape_char();
  flush_size_ = HDFS_FLUSH_WRITE_SIZE;

  // The default stringstream output precision is not very high, making it impossible
  // to properly output doubles (they get rounded to ints).  Set a more reasonable
  // precision.
  rowbatch_stringstream_.precision(RawValue::ASCII_PRECISION);
}

Status HdfsTextTableWriter::Init() {
  const TQueryOptions& query_options = state_->query_options();
  codec_ = THdfsCompression::NONE;
  if (query_options.__isset.compression_codec) {
    codec_ = query_options.compression_codec;
    if (codec_ == THdfsCompression::SNAPPY) {
      // hadoop.io.codec always means SNAPPY_BLOCKED. Alias the two.
      codec_ = THdfsCompression::SNAPPY_BLOCKED;
    }
  }

  if (codec_ != THdfsCompression::NONE) {
    mem_pool_.reset(new MemPool(parent_->mem_tracker()));
    RETURN_IF_ERROR(Codec::CreateCompressor(
        mem_pool_.get(), true, codec_, &compressor_));
    flush_size_ = COMPRESSED_BUFFERED_SIZE;
  } else {
    flush_size_ = HDFS_FLUSH_WRITE_SIZE;
  }
  parent_->mem_tracker()->Consume(flush_size_);
  return Status::OK();
}

void HdfsTextTableWriter::Close() {
  parent_->mem_tracker()->Release(flush_size_);
  if (mem_pool_.get() != NULL) mem_pool_->FreeAll();
}

uint64_t HdfsTextTableWriter::default_block_size() const {
  return compressor_.get() == NULL ? 0 : COMPRESSED_BLOCK_SIZE;
}

string HdfsTextTableWriter::file_extension() const {
  if (compressor_.get() == NULL) return "";
  return compressor_->file_extension();
}

Status HdfsTextTableWriter::AppendRowBatch(RowBatch* batch,
                                           const vector<int32_t>& row_group_indices,
                                           bool* new_file) {
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
  DCHECK_GE(output_expr_ctxs_.size(), num_non_partition_cols) << parent_->DebugString();

  {
    SCOPED_TIMER(parent_->encode_timer());
    for (int row_idx = 0; row_idx < limit; ++row_idx) {
      TupleRow* current_row = all_rows ?
          batch->GetRow(row_idx) : batch->GetRow(row_group_indices[row_idx]);

      // There might be a select expr for partition cols as well, but we shouldn't be
      // writing their values to the row. Since there must be at least
      // num_non_partition_cols select exprs, and we assume that by convention any
      // partition col exprs are the last in output exprs, it's ok to just write
      // the first num_non_partition_cols values.
      for (int j = 0; j < num_non_partition_cols; ++j) {
        void* value = output_expr_ctxs_[j]->GetValue(current_row);
        if (value != NULL) {
          const ColumnType& type = output_expr_ctxs_[j]->root()->type();
          if (type.type == TYPE_CHAR) {
            char* val_ptr = StringValue::CharSlotToPtr(value, type);
            StringValue sv(val_ptr, StringValue::UnpaddedCharLength(val_ptr, type.len));
            PrintEscaped(&sv);
          } else if (type.IsVarLenStringType()) {
            PrintEscaped(reinterpret_cast<const StringValue*>(value));
          } else {
            output_expr_ctxs_[j]->PrintValue(value, &rowbatch_stringstream_);
          }
        } else {
          // NULLs in hive are encoded based on the 'serialization.null.format' property.
          rowbatch_stringstream_ << table_desc_->null_column_value();
        }
        // Append field delimiter.
        if (j + 1 < num_non_partition_cols) {
          rowbatch_stringstream_ << field_delim_;
        }
      }
      // Append tuple delimiter.
      rowbatch_stringstream_ << tuple_delim_;
      ++output_->num_rows;
    }
  }

  *new_file = false;
  if (rowbatch_stringstream_.tellp() >= flush_size_) {
    RETURN_IF_ERROR(Flush());

    // If compressed, start a new file (compressed data is not splittable).
    *new_file = compressor_.get() != NULL;
  }

  return Status::OK();
}

Status HdfsTextTableWriter::Finalize() {
  return Flush();
}

Status HdfsTextTableWriter::Flush() {
  string rowbatch_string = rowbatch_stringstream_.str();
  rowbatch_stringstream_.str(string());
  const uint8_t* uncompressed_data =
      reinterpret_cast<const uint8_t*>(rowbatch_string.data());
  int64_t uncompressed_len = rowbatch_string.size();
  const uint8_t* data = uncompressed_data;
  int64_t len = uncompressed_len;

  if (compressor_.get() != NULL) {
    SCOPED_TIMER(parent_->compress_timer());
    uint8_t* compressed_data;
    int64_t compressed_len;
    RETURN_IF_ERROR(compressor_->ProcessBlock(false,
        uncompressed_len, uncompressed_data,
        &compressed_len, &compressed_data));
    data = compressed_data;
    len = compressed_len;
  }

  {
    SCOPED_TIMER(parent_->hdfs_write_timer());
    RETURN_IF_ERROR(Write(data, len));
  }

  return Status::OK();
}

inline void HdfsTextTableWriter::PrintEscaped(const StringValue* str_val) {
  for (int i = 0; i < str_val->len; ++i) {
    if (UNLIKELY(str_val->ptr[i] == field_delim_ || str_val->ptr[i] == escape_char_)) {
      rowbatch_stringstream_ << escape_char_;
    }
    rowbatch_stringstream_ << str_val->ptr[i];
  }
}

}
