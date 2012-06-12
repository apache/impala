// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/hash-table.inline.h"
#include "exprs/expr.h"
#include "runtime/raw-value.h"
#include "util/debug-util.h"

using namespace impala;
using namespace std;

const float HashTable::MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;

HashTable::HashTable(const vector<Expr*>& build_exprs, const vector<Expr*>& probe_exprs,
    int num_build_tuples, bool stores_nulls, int64_t num_buckets) :
    build_exprs_(build_exprs),
    probe_exprs_(probe_exprs),
    num_build_tuples_(num_build_tuples),
    stores_nulls_(stores_nulls),
    num_filled_buckets_(0),
    nodes_(NULL),
    num_nodes_(0) {
  DCHECK_GT(build_exprs_.size(), 0);
  DCHECK_EQ(build_exprs_.size(), probe_exprs_.size());
  buckets_.resize(num_buckets);

  // Compute the layout and buffer size to store the evaluated expr results
  results_buffer_size_ = Expr::ComputeResultsLayout(build_exprs_, 
      &expr_values_buffer_offsets_, &var_result_begin_);
  expr_values_buffer_= new uint8_t[results_buffer_size_];
  result_null_bits_ = new bool[build_exprs_.size()];

  nodes_capacity_ = 1024;
  nodes_ = reinterpret_cast<uint8_t*>(malloc(node_byte_size() * nodes_capacity_));
}

bool HashTable::EvalRow(TupleRow* row, const vector<Expr*>& exprs) {
  // TODO: this memset is only needed for string and timestamp types, otherwise
  // the entire buffer gets written over below.
  memset(expr_values_buffer_, 0, results_buffer_size_);
  
  // Put a non-zero constant in the result location for NULL.
  // We don't want(NULL, 1) to hash to the same as (0, 1).
  int64_t null_value = HashUtil::FVN_SEED;
  
  bool has_null = false;
  for (int i = 0; i < exprs.size(); ++i) {
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    void* val = exprs[i]->GetValue(row);
    if (val == NULL) {
      result_null_bits_[i] = true;
      val = &null_value;
      has_null = true;
    } else {
      result_null_bits_[i] = false;
    }
    RawValue::Write(val, loc, build_exprs_[i]->type(), NULL);
  }
  return has_null;
}

uint32_t HashTable::HashVariableLenRow() {
  uint32_t hash = 0;
  // Hash the non-var length portions
  hash = HashUtil::Hash(expr_values_buffer_, var_result_begin_, 0);
  for (int i = 0; i < build_exprs_.size(); ++i) {
    if (build_exprs_[i]->type() != TYPE_STRING) continue;
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    if (result_null_bits_[i]) {
      // Hash with the seed value put into results_buffer_
      hash = HashUtil::Hash(loc, sizeof(StringValue), hash);
    } else {
      // Hash the string
      StringValue* str = reinterpret_cast<StringValue*>(loc);
      hash = HashUtil::Hash(str->ptr, str->len, hash);
    }
  }
  return hash;
}

bool HashTable::Equals(TupleRow* build_row) {
  for (int i = 0; i < build_exprs_.size(); ++i) {
    void* val = build_exprs_[i]->GetValue(build_row);
    if (val == NULL) {
      if (!stores_nulls_) return false;
      if (!result_null_bits_[i]) return false;
      continue;
    }
    
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    if (!RawValue::Eq(loc, val, build_exprs_[i]->type())) {
      return false;
    }
  }
  return true;
}
  
void HashTable::ResizeBuckets(int64_t num_buckets) {
  vector<Bucket> new_buckets;

  new_buckets.resize(num_buckets);
  num_filled_buckets_ = 0;

  Iterator iter = Begin();
  while (iter.HasNext()) {
    int node_idx = iter.node_idx_;
    // Advance to next node before modifying the node's next link
    ++iter;

    Node* node = GetNode(node_idx);

    // Assign it to a new bucket
    uint32_t hash = node->hash_;
    int bucket_idx = hash % num_buckets;
    AddToBucket(&new_buckets[bucket_idx], node_idx);
  }

  buckets_.swap(new_buckets);
}
  
void HashTable::GrowNodeArray() {
  nodes_capacity_ = nodes_capacity_ + nodes_capacity_ / 2;
  int64_t new_size = nodes_capacity_ * node_byte_size();
  nodes_ = reinterpret_cast<uint8_t*>(realloc(nodes_, new_size));
}

string HashTable::DebugString(bool skip_empty, const RowDescriptor* desc) {
  stringstream ss;
  ss << endl;
  for (int i = 0; i < buckets_.size(); ++i) {
    int node_idx = buckets_[i].node_idx_;
    bool first = true;
    if (skip_empty && node_idx == -1) continue;
    ss << i << ": ";
    while (node_idx != -1) {
      Node* node = GetNode(node_idx);
      if (!first) {
        ss << ",";
      }
      if (desc == NULL) {
        ss << node_idx << "(" << (void*)node->data() << ")";
      } else {
        ss << (void*)node->data() << " " << PrintRow(node->data(), *desc);
      }
      node_idx = node->next_idx_;
      first = false;
    }
    ss << endl;
  }
  return ss.str();
}

