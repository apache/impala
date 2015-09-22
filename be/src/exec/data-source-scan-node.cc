// Copyright 2014 Cloudera Inc.
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

#include "exec/data-source-scan-node.h"

#include <boost/foreach.hpp>
#include <vector>
#include <gutil/strings/substitute.h>

#include "exec/parquet-common.h"
#include "exec/read-write-util.h"
#include "exprs/expr.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile.h"

#include "common/names.h"

using namespace strings;
using namespace impala::extdatasource;

DEFINE_int32(data_source_batch_size, 1024, "Batch size for calls to GetNext() on "
    "external data sources.");

namespace impala {

// $0 = num expected cols, $1 = actual num columns
const string ERROR_NUM_COLUMNS = "Data source returned unexpected number of columns. "
    "Expected $0 but received $1. This likely indicates a problem with the data source "
    "library.";
const string ERROR_MISMATCHED_COL_SIZES = "Data source returned columns containing "
    "different numbers of rows. This likely indicates a problem with the data source "
    "library.";
// $0 = column type (e.g. INT)
const string ERROR_INVALID_COL_DATA = "Data source returned inconsistent column data. "
    "Expected value of type $0 based on column metadata. This likely indicates a "
    "problem with the data source library.";
const string ERROR_INVALID_TIMESTAMP = "Data source returned invalid timestamp data. "
    "This likely indicates a problem with the data source library.";
const string ERROR_INVALID_DECIMAL = "Data source returned invalid decimal data. "
    "This likely indicates a problem with the data source library.";

// Size of an encoded TIMESTAMP
const size_t TIMESTAMP_SIZE = sizeof(int64_t) + sizeof(int32_t);

DataSourceScanNode::DataSourceScanNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      data_src_node_(tnode.data_source_node),
      tuple_idx_(0),
      num_rows_(0),
      next_row_idx_(0) {
}

DataSourceScanNode::~DataSourceScanNode() {
}

Status DataSourceScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(data_src_node_.tuple_id);
  DCHECK(tuple_desc_ != NULL);

  data_source_executor_.reset(new ExternalDataSourceExecutor());
  RETURN_IF_ERROR(data_source_executor_->Init(data_src_node_.data_source.hdfs_location,
      data_src_node_.data_source.class_name, data_src_node_.data_source.api_version,
      data_src_node_.init_string));

  // Initialize materialized_slots_ and cols_next_val_idx_.
  BOOST_FOREACH(SlotDescriptor* slot, tuple_desc_->slots()) {
    if (!slot->is_materialized()) continue;
    materialized_slots_.push_back(slot);
    cols_next_val_idx_.push_back(0);
  }
  return Status::OK();
}

Status DataSourceScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);

  // Prepare the schema for TOpenParams.row_schema
  vector<extdatasource::TColumnDesc> cols;
  BOOST_FOREACH(const SlotDescriptor* slot, materialized_slots_) {
    extdatasource::TColumnDesc col;
    int col_idx = slot->col_pos();
    col.__set_name(tuple_desc_->table_desc()->col_descs()[col_idx].name());
    col.__set_type(slot->type().ToThrift());
    cols.push_back(col);
  }
  extdatasource::TTableSchema row_schema;
  row_schema.__set_cols(cols);

  TOpenParams params;
  params.__set_query_id(state->query_id());
  params.__set_table_name(tuple_desc_->table_desc()->name());
  params.__set_init_string(data_src_node_.init_string);
  params.__set_authenticated_user_name(state->effective_user());
  params.__set_row_schema(row_schema);
  params.__set_batch_size(FLAGS_data_source_batch_size);
  params.__set_predicates(data_src_node_.accepted_predicates);
  TOpenResult result;
  RETURN_IF_ERROR(data_source_executor_->Open(params, &result));
  RETURN_IF_ERROR(Status(result.status));
  scan_handle_ = result.scan_handle;
  return GetNextInputBatch();
}

Status DataSourceScanNode::ValidateRowBatchSize() {
  if (!input_batch_->__isset.rows) return Status::OK();
  const vector<TColumnData>& cols = input_batch_->rows.cols;
  if (materialized_slots_.size() != cols.size()) {
    return Status(Substitute(ERROR_NUM_COLUMNS, materialized_slots_.size(), cols.size()));
  }

  num_rows_ = -1;
  // If num_rows was set, use that, otherwise we set it to be the number of rows in
  // the first TColumnData and then ensure the number of rows in other columns are
  // consistent.
  if (input_batch_->rows.__isset.num_rows) num_rows_ = input_batch_->rows.num_rows;
  for (int i = 0; i < materialized_slots_.size(); ++i) {
    const TColumnData& col_data = cols[i];
    if (num_rows_ < 0) num_rows_ = col_data.is_null.size();
    if (num_rows_ != col_data.is_null.size()) return Status(ERROR_MISMATCHED_COL_SIZES);
  }
  return Status::OK();
}

Status DataSourceScanNode::GetNextInputBatch() {
  input_batch_.reset(new TGetNextResult());
  next_row_idx_ = 0;
  // Reset all the indexes into the column value arrays to 0
  memset(&cols_next_val_idx_[0], 0, sizeof(int) * cols_next_val_idx_.size());
  TGetNextParams params;
  params.__set_scan_handle(scan_handle_);
  RETURN_IF_ERROR(data_source_executor_->GetNext(params, input_batch_.get()));
  RETURN_IF_ERROR(Status(input_batch_->status));
  RETURN_IF_ERROR(ValidateRowBatchSize());
  if (!InputBatchHasNext() && !input_batch_->eos) {
    // The data source should have set eos, but if it didn't we should just log a
    // warning and continue as if it had.
    VLOG_QUERY << "Data source " << data_src_node_.data_source.name << " returned no "
      << "rows but did not set 'eos'. No more rows will be fetched from the "
      << "data source.";
    input_batch_->eos = true;
  }
  return Status::OK();
}

// Sets the decimal value in the slot. Inline method to avoid nested switch statements.
inline Status SetDecimalVal(const ColumnType& type, char* bytes, int len,
    void* slot) {
  uint8_t* buffer = reinterpret_cast<uint8_t*>(bytes);
  switch (type.GetByteSize()) {
    case 4: {
      Decimal4Value* val = reinterpret_cast<Decimal4Value*>(slot);
      if (len > sizeof(Decimal4Value)) return Status(ERROR_INVALID_DECIMAL);
      // TODO: Move Decode() to a more generic utils class (here and below)
      ParquetPlainEncoder::Decode(buffer, len, val);
    }
    case 8: {
      Decimal8Value* val = reinterpret_cast<Decimal8Value*>(slot);
      if (len > sizeof(Decimal8Value)) return Status(ERROR_INVALID_DECIMAL);
      ParquetPlainEncoder::Decode(buffer, len, val);
      break;
    }
    case 16: {
      Decimal16Value* val = reinterpret_cast<Decimal16Value*>(slot);
      if (len > sizeof(Decimal16Value)) return Status(ERROR_INVALID_DECIMAL);
      ParquetPlainEncoder::Decode(buffer, len, val);
      break;
    }
    default: DCHECK(false);
  }
  return Status::OK();
}

Status DataSourceScanNode::MaterializeNextRow(MemPool* tuple_pool) {
  const vector<TColumnData>& cols = input_batch_->rows.cols;
  tuple_->Init(tuple_desc_->byte_size());

  for (int i = 0; i < materialized_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = materialized_slots_[i];
    void* slot = tuple_->GetSlot(slot_desc->tuple_offset());
    const TColumnData& col = cols[i];

    if (col.is_null[next_row_idx_]) {
      tuple_->SetNull(slot_desc->null_indicator_offset());
      continue;
    }

    // Get and increment the index into the values array (e.g. int_vals) for this col.
    int val_idx = cols_next_val_idx_[i]++;
    switch (slot_desc->type().type) {
      case TYPE_STRING: {
          if (val_idx >= col.string_vals.size()) {
            return Status(Substitute(ERROR_INVALID_COL_DATA, "STRING"));
          }
          const string& val = col.string_vals[val_idx];
          size_t val_size = val.size();
          char* buffer = reinterpret_cast<char*>(tuple_pool->Allocate(val_size));
          memcpy(buffer, val.data(), val_size);
          reinterpret_cast<StringValue*>(slot)->ptr = buffer;
          reinterpret_cast<StringValue*>(slot)->len = val_size;
          break;
        }
      case TYPE_TINYINT:
        if (val_idx >= col.byte_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "TINYINT"));
        }
        *reinterpret_cast<int8_t*>(slot) = col.byte_vals[val_idx];
        break;
      case TYPE_SMALLINT:
        if (val_idx >= col.short_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "SMALLINT"));
        }
        *reinterpret_cast<int16_t*>(slot) = col.short_vals[val_idx];
        break;
      case TYPE_INT:
        if (val_idx >= col.int_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "INT"));
        }
        *reinterpret_cast<int32_t*>(slot) = col.int_vals[val_idx];
        break;
      case TYPE_BIGINT:
        if (val_idx >= col.long_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "BIGINT"));
        }
        *reinterpret_cast<int64_t*>(slot) = col.long_vals[val_idx];
        break;
      case TYPE_DOUBLE:
        if (val_idx >= col.double_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "DOUBLE"));
        }
        *reinterpret_cast<double*>(slot) = col.double_vals[val_idx];
        break;
      case TYPE_FLOAT:
        if (val_idx >= col.double_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "FLOAT"));
        }
        *reinterpret_cast<float*>(slot) = col.double_vals[val_idx];
        break;
      case TYPE_BOOLEAN:
        if (val_idx >= col.bool_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "BOOLEAN"));
        }
        *reinterpret_cast<int8_t*>(slot) = col.bool_vals[val_idx];
        break;
      case TYPE_TIMESTAMP: {
        if (val_idx >= col.binary_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "TIMESTAMP"));
        }
        const string& val = col.binary_vals[val_idx];
        if (val.size() != TIMESTAMP_SIZE) return Status(ERROR_INVALID_TIMESTAMP);
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(val.data());
        *reinterpret_cast<TimestampValue*>(slot) = TimestampValue(
            ReadWriteUtil::GetInt<uint64_t>(bytes),
            ReadWriteUtil::GetInt<uint32_t>(bytes + sizeof(int64_t)));
        break;
      }
      case TYPE_DECIMAL: {
        if (val_idx >= col.binary_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "DECIMAL"));
        }
        const string& val = col.binary_vals[val_idx];
        RETURN_IF_ERROR(SetDecimalVal(slot_desc->type(), const_cast<char*>(val.data()),
            val.size(), slot));
        break;
      }
      default:
        DCHECK(false);
    }
  }
  return Status::OK();
}

Status DataSourceScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }
  *eos = false;

  // create new tuple buffer for row_batch
  MemPool* tuple_pool = row_batch->tuple_data_pool();
  int tuple_buffer_size = row_batch->MaxTupleBufferSize();
  void* tuple_buffer = tuple_pool->Allocate(tuple_buffer_size);
  tuple_ = reinterpret_cast<Tuple*>(tuple_buffer);
  ExprContext** ctxs = &conjunct_ctxs_[0];
  int num_ctxs = conjunct_ctxs_.size();

  while (true) {
    {
      SCOPED_TIMER(materialize_tuple_timer());
      // copy rows until we hit the limit/capacity or until we exhaust input_batch_
      while (!ReachedLimit() && !row_batch->AtCapacity(tuple_pool) &&
          InputBatchHasNext()) {
        RETURN_IF_ERROR(MaterializeNextRow(tuple_pool));
        int row_idx = row_batch->AddRow();
        TupleRow* tuple_row = row_batch->GetRow(row_idx);
        tuple_row->SetTuple(tuple_idx_, tuple_);

        if (ExecNode::EvalConjuncts(ctxs, num_ctxs, tuple_row)) {
          row_batch->CommitLastRow();
          char* new_tuple = reinterpret_cast<char*>(tuple_);
          new_tuple += tuple_desc_->byte_size();
          tuple_ = reinterpret_cast<Tuple*>(new_tuple);
          ++num_rows_returned_;
        }
        ++next_row_idx_;
      }
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);

      if (ReachedLimit() || row_batch->AtCapacity() || input_batch_->eos) {
        *eos = ReachedLimit() || input_batch_->eos;
        return Status::OK();
      }
    }

    // Need more rows
    DCHECK(!InputBatchHasNext());
    RETURN_IF_ERROR(GetNextInputBatch());
  }
}

Status DataSourceScanNode::Reset(RuntimeState* state) {
  DCHECK(false) << "NYI";
  return Status("NYI");
}

void DataSourceScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  PeriodicCounterUpdater::StopRateCounter(total_throughput_counter());
  PeriodicCounterUpdater::StopTimeSeriesCounter(bytes_read_timeseries_counter_);
  input_batch_.reset();
  TCloseParams params;
  params.__set_scan_handle(scan_handle_);
  TCloseResult result;
  Status status = data_source_executor_->Close(params, &result);
  if (!status.ok()) state->LogError(status.msg());
  ExecNode::Close(state);
}

void DataSourceScanNode::DebugString(int indentation_level, stringstream* out) const {
  string indent(indentation_level * 2, ' ');
  *out << indent << "DataSourceScanNode(tupleid=" << data_src_node_.tuple_id << ")";
}

}  // namespace impala
