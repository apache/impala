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

#include "exec/hdfs-text-table-writer.h"
#include "exec/exec-node.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/coding-util.h"
#include "util/hdfs-util.h"
#include "util/runtime-profile-counters.h"

#include <hdfs.h>
#include <stdlib.h>

#include "common/names.h"

namespace impala {

HdfsTextTableWriter::HdfsTextTableWriter(TableSinkBase* parent,
    RuntimeState* state, OutputPartition* output,
    const HdfsPartitionDescriptor* partition,
    const HdfsTableDescriptor* table_desc)
  : HdfsTableWriter(parent, state, output, partition, table_desc) {
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
  parent_->mem_tracker()->Consume(flush_size_);
  return Status::OK();
}

void HdfsTextTableWriter::Close() {
  parent_->mem_tracker()->Release(flush_size_);
}

uint64_t HdfsTextTableWriter::default_block_size() const { return 0; }

string HdfsTextTableWriter::file_extension() const { return "txt"; }

Status HdfsTextTableWriter::AppendRows(
    RowBatch* batch, const vector<int32_t>& row_group_indices, bool* new_file) {
  int32_t limit;
  if (row_group_indices.empty()) {
    limit = batch->num_rows();
  } else {
    limit = row_group_indices.size();
  }
  COUNTER_ADD(parent_->rows_inserted_counter(), limit);

  bool all_rows = row_group_indices.empty();
  int num_partition_cols = table_desc_->num_clustering_cols();
  int num_non_partition_cols = table_desc_->num_cols() - num_partition_cols;
  DCHECK_GE(output_expr_evals_.size(), num_non_partition_cols) << parent_->DebugString();

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
        void* value = output_expr_evals_[j]->GetValue(current_row);
        if (value != NULL) {
          const ColumnType& type = output_expr_evals_[j]->root().type();
          if (type.type == TYPE_CHAR) {
            char* val_ptr = reinterpret_cast<char*>(value);
            StringValue sv(val_ptr, StringValue::UnpaddedCharLength(val_ptr, type.len));
            PrintEscaped(&sv);
          } else if (type.IsVarLenStringType()) {
            const StringValue* string_value = reinterpret_cast<const StringValue*>(value);
            if (type.IsBinaryType()) {
              // TODO: try to find a more efficient implementation
              Base64Encode(
                  string_value->Ptr() , string_value->Len(), &rowbatch_stringstream_);
            } else {
              PrintEscaped(string_value);
            }
          } else {
            output_expr_evals_[j]->PrintValue(value, &rowbatch_stringstream_);
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
      ++output_->current_file_rows;
    }
  }

  *new_file = false;
  if (rowbatch_stringstream_.tellp() >= flush_size_) RETURN_IF_ERROR(Flush());

  RETURN_IF_ERROR(state_->CheckQueryState());
  return Status::OK();
}

Status HdfsTextTableWriter::Finalize() {
  return Flush();
}

Status HdfsTextTableWriter::InitNewFile() {
  // Write empty header lines for tables with 'skip.header.line.count' property set to
  // non-zero.
  for (int i = 0; i < parent_->skip_header_line_count(); ++i) {
    rowbatch_stringstream_ << '\n';
  }
  return Status::OK();
}

Status HdfsTextTableWriter::Flush() {
  string rowbatch_string = rowbatch_stringstream_.str();
  rowbatch_stringstream_.str(string());
  const uint8_t* data =
      reinterpret_cast<const uint8_t*>(rowbatch_string.data());
  int64_t len = rowbatch_string.size();

  {
    SCOPED_TIMER(parent_->hdfs_write_timer());
    RETURN_IF_ERROR(Write(data, len));
  }

  return Status::OK();
}

inline void HdfsTextTableWriter::PrintEscaped(const StringValue* str_val) {
  StringValue::SimpleString s = str_val->ToSimpleString();
  for (int i = 0; i < s.len; ++i) {
    if (escape_char_ == '\0') {
      rowbatch_stringstream_ << s.ptr[i];
    } else {
      if (UNLIKELY(s.ptr[i] == field_delim_ || s.ptr[i] == escape_char_)) {
        rowbatch_stringstream_ << escape_char_;
      }
      rowbatch_stringstream_ << s.ptr[i];
    }
  }
}
}
