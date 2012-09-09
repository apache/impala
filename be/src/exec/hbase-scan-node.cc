// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "hbase-scan-node.h"
#include <algorithm>
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exec/text-converter.inline.h"

using namespace std;
using namespace boost;
using namespace impala;

HBaseScanNode::HBaseScanNode(ObjectPool* pool, const TPlanNode& tnode,
                             const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      table_name_(tnode.hbase_scan_node.table_name),
      tuple_id_(tnode.hbase_scan_node.tuple_id),
      tuple_desc_(NULL),
      tuple_idx_(0),
      filters_(tnode.hbase_scan_node.filters),
      num_errors_(0),
      tuple_pool_(new MemPool()),
      hbase_scanner_(NULL),
      row_key_slot_(NULL),
      text_converter_(new TextConverter('\\', tuple_pool_.get())) {
}

HBaseScanNode::~HBaseScanNode() {
}

bool HBaseScanNode::CmpColPos(const SlotDescriptor* a, const SlotDescriptor* b) {
  return a->col_pos() < b->col_pos();
}

Status HBaseScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  hbase_scanner_.reset(new HBaseTableScanner(this, state->htable_cache()));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to get tuple descriptor.");
  }
  // The data retrieved from HBase via result_.raw() is sorted by family/qualifier.
  // The corresponding HBase columns in the Impala metadata are also sorted by
  // family/qualifier.
  // Here, we re-order the slots from the query by family/qualifier, exploiting the
  // know sort order of the columns retrieved from HBase, to avoid family/qualifier
  // comparisons.
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  sorted_non_key_slots_.reserve(slots.size());
  for (int i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    if (slots[i]->col_pos() == ROW_KEY) {
      row_key_slot_ = slots[i];
    } else {
      sorted_non_key_slots_.push_back(slots[i]);
    }
  }
  sort(sorted_non_key_slots_.begin(), sorted_non_key_slots_.end(), CmpColPos);

  // Create list of family/qualifier pointers in same sort order as sorted_non_key_slots_.
  const HBaseTableDescriptor* hbase_table =
      static_cast<const HBaseTableDescriptor*>(tuple_desc_->table_desc());
  sorted_cols_.reserve(sorted_non_key_slots_.size());
  for (int i = 0; i < sorted_non_key_slots_.size(); ++i) {
    sorted_cols_.push_back(&hbase_table->cols()[sorted_non_key_slots_[i]->col_pos()]);
  }

  // TODO(marcel): add int tuple_idx_[] indexed by TupleId somewhere in runtime-state.h
  tuple_idx_ = 0;

  hbase_scanner_->set_num_requested_keyvalues(sorted_non_key_slots_.size());

  return Status::OK;
}

Status HBaseScanNode::Open(RuntimeState* state) {
  RETURN_IF_CANCELLED(state);
  COUNTER_SCOPED_TIMER(runtime_profile_->total_time_counter());
  JNIEnv* env = getJNIEnv();
  return hbase_scanner_->StartScan(env, tuple_desc_, scan_range_vector_, filters_);
}

void HBaseScanNode::WriteTextSlot(
    const string& family, const string& qualifier,
    void* value, int value_length, SlotDescriptor* slot,
    RuntimeState* state, bool* error_in_row) {
  COUNTER_SCOPED_TIMER(materialize_tuple_timer());
  if (!text_converter_->WriteSlot(slot, tuple_,
      reinterpret_cast<char*>(value), value_length, true, false)) {
    *error_in_row = true;
    if (state->LogHasSpace()) {
      state->error_stream() << "Error converting column " << family
          << ":" << qualifier << ": "
          << "'" << reinterpret_cast<char*>(value) << "' TO "
          << TypeToString(slot->type()) << endl;
    }
  }
}

Status HBaseScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_CANCELLED(state);
  // For GetNext, most of the time is spent in HBaseTableScanner::ResultScanner_next,
  // but there's still some considerable time inside here.
  // TODO: need to understand how the time is spent inside this function.
  COUNTER_SCOPED_TIMER(runtime_profile_->total_time_counter());
  COUNTER_SCOPED_TIMER(materialize_tuple_timer());
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  // create new tuple buffer for row_batch
  tuple_buffer_size_ = row_batch->capacity() * tuple_desc_->byte_size();
  tuple_buffer_ = tuple_pool_->Allocate(tuple_buffer_size_);
  bzero(tuple_buffer_, tuple_buffer_size_);
  tuple_ = reinterpret_cast<Tuple*>(tuple_buffer_);

  // Indicates whether the current row has conversion errors. Used for error reporting.
  bool error_in_row = false;

  // Indicates whether there are more rows to process. Set in hbase_scanner_.Next().
  JNIEnv* env = getJNIEnv();
  bool has_next = false;
  while (true) {
    RETURN_IF_CANCELLED(state);
    if (ReachedLimit() || row_batch->IsFull()) {
      // hang on to last allocated chunk in pool, we'll keep writing into it in the
      // next GetNext() call
      row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), !ReachedLimit());
      *eos = ReachedLimit();
      return Status::OK;
    }
    RETURN_IF_ERROR(hbase_scanner_->Next(env, &has_next));
    if (!has_next) {
      if (num_errors_ > 0) {
        const HBaseTableDescriptor* hbase_table =
            static_cast<const HBaseTableDescriptor*> (tuple_desc_->table_desc());
        state->ReportFileErrors(hbase_table->table_name(), num_errors_);
      }
      row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
      *eos = true;
      return Status::OK;
    }

    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    row->SetTuple(tuple_idx_, tuple_);

    // Write row key slot.
    if (row_key_slot_ != NULL) {
      void* key;
      int key_length;
      hbase_scanner_->GetRowKey(env, &key, &key_length);
      if (key == NULL) {
        tuple_->SetNull(row_key_slot_->null_indicator_offset());
      } else {
        WriteTextSlot("key", "",
            key, key_length, row_key_slot_, state, &error_in_row);
      }
    }

    // Write non-key slots.
    for (int i = 0; i < sorted_non_key_slots_.size(); ++i) {
      void* value;
      int value_length;
      hbase_scanner_->GetValue(env, sorted_cols_[i]->first, sorted_cols_[i]->second,
          &value, &value_length);
      if (value == NULL) {
        tuple_->SetNull(sorted_non_key_slots_[i]->null_indicator_offset());
      } else {
        WriteTextSlot(sorted_cols_[i]->first, sorted_cols_[i]->second,
            value, value_length, sorted_non_key_slots_[i], state, &error_in_row);
      }
    }

    // Error logging: Flush error stream and add name of HBase table and current row key.
    if (error_in_row) {
      error_in_row = false;
      ++num_errors_;
      if (state->LogHasSpace()) {
        state->error_stream() << "hbase table: " << table_name_ << endl;
        void* key;
        int key_length;
        hbase_scanner_->GetRowKey(env, &key, &key_length);
        state->error_stream() << "row key: "
            << string(reinterpret_cast<const char*>(key), key_length);
        state->LogErrorStream();
      }
      if (state->abort_on_error()) {
        state->ReportFileErrors(table_name_, 1);
        return Status(
            "Aborted HBaseScanNode due to conversion errors. View error log "
            "for details.");
      }
    }

    if (EvalConjuncts(&conjuncts_[0], conjuncts_.size(), row)) {
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      char* new_tuple = reinterpret_cast<char*>(tuple_);
      new_tuple += tuple_desc_->byte_size();
      tuple_ = reinterpret_cast<Tuple*>(new_tuple);
    } else {
      // make sure to reset null indicators since we're overwriting
      // the tuple assembled for the previous row
      tuple_->Init(tuple_desc_->byte_size());
    }
  }
}

Status HBaseScanNode::Close(RuntimeState* state) {
  COUNTER_SCOPED_TIMER(runtime_profile_->total_time_counter());
  COUNTER_UPDATE(memory_used_counter(), tuple_pool_->peak_allocated_bytes());

  JNIEnv* env = getJNIEnv();
  hbase_scanner_->Close(env);
  // Report total number of errors.
  if (num_errors_ > 0) {
    state->ReportFileErrors(table_name_, num_errors_);
  }
  return ExecNode::Close(state);
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

Status HBaseScanNode::SetScanRange(const TScanRange& scan_range) {
  DCHECK(scan_range.__isset.hbaseKeyRanges);
  for(int i = 0; i < scan_range.hbaseKeyRanges.size(); i++) {
    const THBaseKeyRange& key_range = scan_range.hbaseKeyRanges[i];
    scan_range_vector_.push_back(HBaseTableScanner::ScanRange());
    HBaseTableScanner::ScanRange& sr = scan_range_vector_.back();
    if (key_range.__isset.startKey) {
      sr.set_start_key(key_range.startKey);
    }
    if (key_range.__isset.stopKey) {
      sr.set_stop_key(key_range.stopKey);
    }
  }

  return Status::OK;
}
