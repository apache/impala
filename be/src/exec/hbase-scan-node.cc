// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "hbase-scan-node.h"
#include <algorithm>
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "util/jni-util.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;
using namespace boost;
using namespace impala;

HBaseScanNode::HBaseScanNode(ObjectPool* pool, const TPlanNode& tnode)
    : ExecNode(pool, tnode),
      table_name_(tnode.hbase_scan_node.table_name),
      tuple_id_(tnode.hbase_scan_node.tuple_id),
      tuple_desc_(NULL),
      tuple_idx_(0),
      start_key_(
          tnode.hbase_scan_node.__isset.start_key
            ? tnode.hbase_scan_node.start_key
            : ""),
      stop_key_(
          tnode.hbase_scan_node.__isset.stop_key
            ? tnode.hbase_scan_node.stop_key
            : ""),
      filters_(tnode.hbase_scan_node.filters),
      num_errors_(0),
      tuple_buf_pool_(new MemPool(POOL_INIT_SIZE)),
      var_len_pool_(new MemPool(POOL_INIT_SIZE)),
      hbase_scanner_(NULL),
      row_key_slot_(NULL),
      text_converter_(new TextConverter(false, '\\', var_len_pool_.get())) {
}

bool HBaseScanNode::CmpColPos(const SlotDescriptor* a, const SlotDescriptor* b) {
  return a->col_pos() < b->col_pos();
}

Status HBaseScanNode::Prepare(RuntimeState* state) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }
  hbase_scanner_.reset(new HBaseTableScanner(env));

  PrepareConjuncts(state);
  tuple_desc_ = state->descs().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to get tuple descriptor.");
  }
  // The data retrieved from HBase via result_.raw() is sorted by family/qualifier.
  // The corresponding HBase columns in the Impala metadata are also sorted by family/qualifier.
  // Here, we re-order the slots from the query by family/qualifier, exploiting the
  // know sort order of the columns retrieved from HBase, to avoid family/qualifier comparisons.
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  sorted_non_key_slots_.reserve(slots.size());
  for (int i = 0; i < slots.size(); ++i) {
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

  // Allocate tuple buffer.
  tuple_buf_size_ = state->batch_size() * tuple_desc_->byte_size();
  tuple_buf_ = tuple_buf_pool_->Allocate(tuple_buf_size_);

  // TODO(marcel): add int tuple_idx_[] indexed by TupleId somewhere in runtime-state.h
  tuple_idx_ = 0;

  hbase_scanner_->set_hbase_conf(reinterpret_cast<jobject>(RuntimeState::hbase_conf()));
  hbase_scanner_->set_num_requested_keyvalues(sorted_non_key_slots_.size());

  return Status::OK;
}

Status HBaseScanNode::Open(RuntimeState* state) {
  return hbase_scanner_->StartScan(tuple_desc_, start_key_, stop_key_, filters_);
}

void HBaseScanNode::WriteTextSlot(const string& family, const string& qualifier,
    void* value, int value_length, SlotDescriptor* slot,
    RuntimeState* state, bool* error_in_row) {
  bool parsed_ok = text_converter_->ConvertAndWriteSlotBytes(reinterpret_cast<char*>(value),
      reinterpret_cast<char*>(value) + value_length, tuple_, slot, true, false);
  if (!parsed_ok) {
    *error_in_row = true;
    if (state->LogHasSpace()) {
      state->error_stream() << "Error converting column " << family << ":" << qualifier << ": "
          << "'" << reinterpret_cast<char*>(value) << "' TO "
          << TypeToString(slot->type()) << endl;
    }
  }
}

Status HBaseScanNode::GetNext(RuntimeState* state, RowBatch* row_batch) {
  tuple_ = reinterpret_cast<Tuple*>(tuple_buf_);
  bzero(tuple_buf_, tuple_buf_size_);

  // Reset pool to reuse its already allocated memory.
  var_len_pool_->Clear();

  // Indicates whether the current row has conversion errors. Used for error reporting.
  bool error_in_row = false;

  // Indicates whether there are more rows to process. Set in hbase_scanner_.Next().
  bool has_next = false;
  while (true) {
    if (row_batch->IsFull()) return Status::OK;
    RETURN_IF_ERROR(hbase_scanner_->Next(&has_next));
    if (!has_next) {
      if (num_errors_ > 0) {
        const HBaseTableDescriptor* hbase_table =
            static_cast<const HBaseTableDescriptor*> (tuple_desc_->table_desc());
        state->ReportFileErrors(hbase_table->table_name(), num_errors_);
      }
      return Status::OK;
    }

    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    row->SetTuple(tuple_idx_, tuple_);

    // Write row key slot.
    if (row_key_slot_ != NULL) {
      void* key;
      int key_length;
      hbase_scanner_->GetRowKey(&key, &key_length);
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
      hbase_scanner_->GetValue(sorted_cols_[i]->first, sorted_cols_[i]->second,
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
        hbase_scanner_->GetRowKey(&key, &key_length);
        state->error_stream() << "row key: "
            << string(reinterpret_cast<const char*>(key), key_length);
        state->LogErrorStream();
      }
      if (state->abort_on_error()) {
        state->ReportFileErrors(table_name_, 1);
        hbase_scanner_->ReleaseBuffer();
        return Status(
            "Aborted HBaseScanNode due to conversion errors. View error log for details.");
      }
    }
    if (EvalConjuncts(row)) {
      row_batch->CommitLastRow();
      char* new_tuple = reinterpret_cast<char*>(tuple_);
      new_tuple += tuple_desc_->byte_size();
      tuple_ = reinterpret_cast<Tuple*>(new_tuple);
    } else {
      // make sure to reset null indicators since we're overwriting
      // the tuple assembled for the previous row
      tuple_->Init(tuple_desc_->byte_size());
    }
    hbase_scanner_->ReleaseBuffer();
  }
}

Status HBaseScanNode::Close(RuntimeState* state) {
  hbase_scanner_->Close();
  // Report total number of errors.
  if (num_errors_ > 0) {
    state->ReportFileErrors(table_name_, num_errors_);
  }
  return Status::OK;
}

void HBaseScanNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "HBaseScanNode(tupleid=" << tuple_id_ << " table=" << table_name_;
  if (!start_key_.empty()) {
    *out << " start_key=" << start_key_;
  }
  if (!stop_key_.empty()) {
    *out << " stop_key=" << stop_key_;
  }
  *out << ")" << endl;
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->DebugString(indentation_level + 1, out);
  }
}
