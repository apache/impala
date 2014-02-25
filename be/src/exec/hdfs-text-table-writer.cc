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
#include "util/hdfs-util.h"
#include "exprs/expr.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"

#include <vector>
#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <stdlib.h>

using namespace std;
using namespace boost::posix_time;

namespace impala {
HdfsTextTableWriter::HdfsTextTableWriter(HdfsTableSink* parent,
                                         RuntimeState* state, OutputPartition* output,
                                         const HdfsPartitionDescriptor* partition,
                                         const HdfsTableDescriptor* table_desc,
                                         const vector<Expr*>& output_exprs)
    : HdfsTableWriter(parent, state, output, partition, table_desc, output_exprs) {
  tuple_delim_ = partition->line_delim();
  field_delim_ = partition->field_delim();
  escape_char_ = partition->escape_char();

  // The default stringstream output precision is not very high, making it impossible
  // to properly output doubles (they get rounded to ints).  Set a more reasonable
  // precision.
  rowbatch_stringstream_.precision(RawValue::ASCII_PRECISION);
}

Status HdfsTableWriter::Init() {
  parent_->mem_tracker()->Consume(HDFS_FLUSH_WRITE_SIZE);
  return Status::OK;
}

void HdfsTableWriter::Close() {
  parent_->mem_tracker()->Release(HDFS_FLUSH_WRITE_SIZE);
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
  COUNTER_UPDATE(parent_->rows_inserted_counter(), limit);

  bool all_rows = row_group_indices.empty();
  int num_non_partition_cols =
      table_desc_->num_cols() - table_desc_->num_clustering_cols();
  DCHECK_GE(output_exprs_.size(), num_non_partition_cols) << parent_->DebugString();

  {
    SCOPED_TIMER(parent_->encode_timer());
    for (int row_idx = 0; row_idx < limit; ++row_idx) {
      TupleRow* current_row = all_rows ?
          batch->GetRow(row_idx) : batch->GetRow(row_group_indices[row_idx]);

      // There might be a select expr for partition cols as well, but we shouldn't be
      // writing their values to the row. Since there must be at least
      // num_non_partition_cols select exprs, and we assume that by convention any
      // partition col exprs are the last in output_exprs_, it's ok to just write
      // the first num_non_partition_cols values.
      for (int j = 0; j < num_non_partition_cols; ++j) {
        void* value = output_exprs_[j]->GetValue(current_row);
        if (value != NULL) {
          if (output_exprs_[j]->type().type == TYPE_STRING) {
            PrintEscaped(reinterpret_cast<const StringValue*>(value));
          } else {
            output_exprs_[j]->PrintValue(value, &rowbatch_stringstream_);
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

  if (rowbatch_stringstream_.tellp() >= HDFS_FLUSH_WRITE_SIZE || true) {
    string rowbatch_string = rowbatch_stringstream_.str();
    SCOPED_TIMER(parent_->hdfs_write_timer());
    RETURN_IF_ERROR(Write(rowbatch_string.data(), rowbatch_string.size()));
    rowbatch_stringstream_.str(string());
  }

  *new_file = false;
  return Status::OK;
}

Status HdfsTextTableWriter::Finalize() {
  // Write the remaining buffered bytes to hdfs.
  string rowbatch_string = rowbatch_stringstream_.str();
  SCOPED_TIMER(parent_->hdfs_write_timer());
  RETURN_IF_ERROR(Write(rowbatch_string.data(), rowbatch_string.size()));
  rowbatch_stringstream_.str(string());
  return Status::OK;
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
