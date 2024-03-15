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

#include "exec/hbase/hbase-scan-node.h"

#include <algorithm>

#include "exec/exec-node.inline.h"
#include "exec/exec-node-util.h"
#include "exec/text-converter.inline.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/jni-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"
using namespace impala;

PROFILE_DEFINE_TIMER(TotalRawHBaseReadTime, STABLE_HIGH,
    "Aggregate wall clock time spent reading from HBase.");

HBaseScanNode::HBaseScanNode(ObjectPool* pool, const ScanPlanNode& pnode,
                             const DescriptorTbl& descs)
    : ScanNode(pool, pnode, descs),
      table_name_(pnode.tnode_->hbase_scan_node.table_name),
      tuple_id_(pnode.tnode_->hbase_scan_node.tuple_id),
      tuple_desc_(nullptr),
      hbase_table_(nullptr),
      tuple_idx_(0),
      filters_(pnode.tnode_->hbase_scan_node.filters),
      hbase_scanner_(nullptr),
      row_key_slot_(nullptr),
      row_key_binary_encoded_(false),
      text_converter_(new TextConverter('\\', "", false)),
      suggested_max_caching_(0) {
  if (pnode.tnode_->hbase_scan_node.__isset.suggested_max_caching) {
    suggested_max_caching_ = pnode.tnode_->hbase_scan_node.suggested_max_caching;
  }
}

HBaseScanNode::~HBaseScanNode() {
}

Status HBaseScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  hbase_read_timer_ = PROFILE_TotalRawHBaseReadTime.Instantiate(runtime_profile());
  AddBytesReadCounters();

  hbase_scanner_.reset(
      new HBaseTableScanner(this, ExecEnv::GetInstance()->htable_factory(), state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to get tuple descriptor.");
  }
  // The data retrieved from HBase via result_.raw() is sorted by family/qualifier.
  // Here, we re-order the slots from the query by family/qualifier, exploiting the
  // known sort order of the columns retrieved from HBase, to avoid family/qualifier
  // comparisons.
  hbase_table_ = static_cast<const HBaseTableDescriptor*>(tuple_desc_->table_desc());
  const vector<HBaseTableDescriptor::HBaseColumnDescriptor>& cols = hbase_table_->cols();
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  sorted_non_key_slots_.reserve(slots.size());
  for (int i = 0; i < slots.size(); ++i) {
    const HBaseTableDescriptor::HBaseColumnDescriptor& col = cols[slots[i]->col_pos()];
    if (col.family == ":key") {
      row_key_slot_ = slots[i];
      row_key_binary_encoded_ = col.binary_encoded;
    } else {
      sorted_non_key_slots_.push_back(slots[i]);
    }
  }
  // This is not needed if flag use_hms_column_order_for_hbase_tables=false as the columns
  // will be already ordered in the FE, but sort it anyway to avoid relying on this.
  sort(sorted_non_key_slots_.begin(), sorted_non_key_slots_.end(),
      [&](const SlotDescriptor* a, const SlotDescriptor* b) -> bool {
        const HBaseTableDescriptor::HBaseColumnDescriptor& cola = cols[a->col_pos()];
        const HBaseTableDescriptor::HBaseColumnDescriptor& colb = cols[b->col_pos()];
        return cola.family == colb.family
            ? cola.qualifier < colb.qualifier : cola.family < colb.family;
      });

  // Create list of family/qualifier pointers in same sort order as sorted_non_key_slots_.
  sorted_cols_.reserve(sorted_non_key_slots_.size());
  for (int i = 0; i < sorted_non_key_slots_.size(); ++i) {
    sorted_cols_.push_back(&cols[sorted_non_key_slots_[i]->col_pos()]);
  }

  // TODO(marcel): add int tuple_idx_[] indexed by TupleId somewhere in runtime-state.h
  tuple_idx_ = 0;

  // Convert ScanRangeParamsPB to ScanRanges
  DCHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";
  for (const ScanRangeParamsPB& params : *scan_range_params_) {
    DCHECK(params.scan_range().has_hbase_key_range());
    const HBaseKeyRangePB& key_range = params.scan_range().hbase_key_range();
    scan_range_vector_.push_back(HBaseTableScanner::ScanRange());
    HBaseTableScanner::ScanRange& sr = scan_range_vector_.back();
    if (key_range.has_startkey()) {
      sr.set_start_key(key_range.startkey());
    }
    if (key_range.has_stopkey()) {
      sr.set_stop_key(key_range.stopkey());
    }
  }
  runtime_profile_->AddInfoString("Table Name", hbase_table_->fully_qualified_name());
  return Status::OK();
}

Status HBaseScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  JNIEnv* env = JniUtil::GetJNIEnv();

  // No need to initialize hbase_scanner_ if there are no scan ranges.
  if (scan_range_vector_.size() == 0) return Status::OK();
  return hbase_scanner_->StartScan(env, tuple_desc_, scan_range_vector_, filters_);
}

void HBaseScanNode::WriteTextSlot(
    const string& family, const string& qualifier,
    void* value, int value_length, SlotDescriptor* slot_desc,
    RuntimeState* state, MemPool* pool, Tuple* tuple, bool* error_in_row) {
  if (!text_converter_->WriteSlot(slot_desc, tuple, reinterpret_cast<char*>(value),
        value_length, true, false, pool)) {
    *error_in_row = true;
    if (state->LogHasSpace()) {
      stringstream ss;
      ss << "Error converting column " << family
          << ":" << qualifier << ": "
          << "'" << string(reinterpret_cast<char*>(value), value_length) << "' TO "
          << slot_desc->type();
      state->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
    }
  }
}

Status HBaseScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (scan_range_vector_.empty() || ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }
  *eos = false;

  // Create new tuple buffer for row_batch.
  int64_t tuple_buffer_size;
  uint8_t* tuple_buffer;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buffer_size, &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  tuple->Init(tuple_buffer_size);

  // Indicates whether the current row has conversion errors. Used for error reporting.
  bool error_in_row = false;

  // Indicates whether there are more rows to process. Set in hbase_scanner_.Next().
  JNIEnv* env = JniUtil::GetJNIEnv();
  bool has_next = false;
  while (true) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    if (row_batch->AtCapacity() || ReachedLimit()) {
      // hang on to last allocated chunk in pool, we'll keep writing into it in the
      // next GetNext() call
      *eos = ReachedLimit();
      return Status::OK();
    }
    RETURN_IF_ERROR(hbase_scanner_->Next(env, &has_next));
    if (!has_next) {
      *eos = true;
      return Status::OK();
    }

    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    row->SetTuple(tuple_idx_, tuple);

    {
      // Measure row key and column value materialization time
      SCOPED_TIMER(materialize_tuple_timer());

      // Write row key slot.
      if (row_key_slot_ != NULL) {
        if (row_key_binary_encoded_) {
          RETURN_IF_ERROR(hbase_scanner_->GetRowKey(env, row_key_slot_, tuple));
        } else {
          void* key;
          int key_length;
          RETURN_IF_ERROR(hbase_scanner_->GetRowKey(env, &key, &key_length));
          WriteTextSlot("key", "", key, key_length, row_key_slot_, state,
              row_batch->tuple_data_pool(), tuple, &error_in_row);
        }
      }

      // Write non-key slots.
      for (int i = 0; i < sorted_non_key_slots_.size(); ++i) {
        if (sorted_cols_[i]->binary_encoded) {
          RETURN_IF_ERROR(hbase_scanner_->GetValue(env, sorted_cols_[i]->family,
              sorted_cols_[i]->qualifier, sorted_non_key_slots_[i], tuple));
        } else {
          void* value;
          int value_length;
          RETURN_IF_ERROR(hbase_scanner_->GetValue(env, sorted_cols_[i]->family,
              sorted_cols_[i]->qualifier, &value, &value_length));
          if (value == NULL) {
            tuple->SetNull(sorted_non_key_slots_[i]->null_indicator_offset());
          } else {
            WriteTextSlot(sorted_cols_[i]->family, sorted_cols_[i]->qualifier,
                value, value_length, sorted_non_key_slots_[i], state,
                row_batch->tuple_data_pool(), tuple, &error_in_row);
          }
        }
      }
    }

    // Error logging: Flush error stream and add name of HBase table and current row key.
    if (error_in_row) {
      error_in_row = false;
      if (state->LogHasSpace()) {
        stringstream ss;
        ss << "hbase table: " << table_name_ << endl;
        void* key;
        int key_length;
        RETURN_IF_ERROR(hbase_scanner_->GetRowKey(env, &key, &key_length));
        ss << "row key: " << string(reinterpret_cast<const char*>(key), key_length);
        state->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
      }
      if (state->abort_on_error()) return Status(state->ErrorLog());
    }

    DCHECK_EQ(conjunct_evals_.size(), conjuncts_.size());
    if (EvalConjuncts(conjunct_evals_.data(), conjuncts_.size(), row)) {
      row_batch->CommitLastRow();
      IncrementNumRowsReturned(1);
      COUNTER_SET(rows_returned_counter_, rows_returned());
      tuple = reinterpret_cast<Tuple*>(
          reinterpret_cast<uint8_t*>(tuple) + tuple_desc_->byte_size());
    } else {
      // make sure to reset null indicators since we're overwriting
      // the tuple assembled for the previous row
      tuple->Init(tuple_desc_->byte_size());
    }
    COUNTER_ADD(rows_read_counter_, 1);
  }

  return Status::OK();
}

Status HBaseScanNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  DCHECK(false) << "NYI";
  return Status("NYI");
}

void HBaseScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  runtime_profile_->StopPeriodicCounters();

  if (hbase_scanner_.get() != NULL) {
    JNIEnv* env = JniUtil::GetJNIEnv();
    hbase_scanner_->Close(env);
  }
  ScanNode::Close(state);
}

void HBaseScanNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "HBaseScanNode(tupleid=" << tuple_id_ << " table=" << table_name_;
  for(int i = 0; i < scan_range_vector_.size(); i++) {
    *out << " region(" << i << "):";
    HBaseTableScanner::ScanRange scan_range = scan_range_vector_[i];
    scan_range.DebugString(0, out);
  }
  *out << ")" << endl;
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->DebugString(indentation_level + 1, out);
  }
}
