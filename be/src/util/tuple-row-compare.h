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


#ifndef IMPALA_UTIL_TUPLE_ROW_COMPARE_H_
#define IMPALA_UTIL_TUPLE_ROW_COMPARE_H_

#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/descriptors.h"

namespace impala {

class TupleRowComparator {
 public:
  // Compares two TupleRows based on a set of exprs, in order.
  // We use is_asc to determine, for each expr, if it should be ascending or descending
  // sort order.
  // We use nulls_first to determine, for each expr, if nulls should come before
  // or after all other values.
  TupleRowComparator(
      const std::vector<ExprContext*>& key_expr_ctxs_lhs,
      const std::vector<ExprContext*>& key_expr_ctxs_rhs,
      const std::vector<bool>& is_asc,
      const std::vector<bool>& nulls_first)
      : key_expr_ctxs_lhs_(key_expr_ctxs_lhs),
        key_expr_ctxs_rhs_(key_expr_ctxs_rhs),
        is_asc_(is_asc) {
    DCHECK_EQ(key_expr_ctxs_lhs.size(), key_expr_ctxs_rhs.size());
    DCHECK_EQ(key_expr_ctxs_lhs.size(), is_asc.size());
    DCHECK_EQ(key_expr_ctxs_lhs.size(), nulls_first.size());
    nulls_first_.reserve(key_expr_ctxs_lhs.size());
    for (int i = 0; i < key_expr_ctxs_lhs.size(); ++i) {
      nulls_first_.push_back(nulls_first[i] ? -1 : 1);
    }
  }

  TupleRowComparator(
      const std::vector<ExprContext*>& key_expr_ctxs_lhs,
      const std::vector<ExprContext*>& key_expr_ctxs_rhs,
      bool is_asc, bool nulls_first)
      : key_expr_ctxs_lhs_(key_expr_ctxs_lhs),
        key_expr_ctxs_rhs_(key_expr_ctxs_rhs),
        is_asc_(key_expr_ctxs_lhs.size(), is_asc),
        nulls_first_(key_expr_ctxs_lhs.size(), nulls_first ? -1 : 1) {
    DCHECK_EQ(key_expr_ctxs_lhs.size(), key_expr_ctxs_rhs.size());
  }

  // Returns a negative value if lhs is less than rhs, a positive value if lhs is greater
  // than rhs, or 0 if they are equal. All exprs (key_exprs_lhs_ and key_exprs_rhs_)
  // must have been prepared and opened before calling this.
  int Compare(TupleRow* lhs, TupleRow* rhs) const {
    for (int i = 0; i < key_expr_ctxs_lhs_.size(); ++i) {
      void* lhs_value = key_expr_ctxs_lhs_[i]->GetValue(lhs);
      void* rhs_value = key_expr_ctxs_rhs_[i]->GetValue(rhs);

      // The sort order of NULLs is independent of asc/desc.
      if (lhs_value == NULL && rhs_value == NULL) continue;
      if (lhs_value == NULL && rhs_value != NULL) return nulls_first_[i];
      if (lhs_value != NULL && rhs_value == NULL) return -nulls_first_[i];

      int result = RawValue::Compare(lhs_value, rhs_value, 
                                     key_expr_ctxs_lhs_[i]->root()->type());
      if (!is_asc_[i]) result = -result;
      if (result != 0) return result;
      // Otherwise, try the next Expr
    }
    return 0; // fully equivalent key
  }

  // Returns true if lhs is strictly less than rhs.
  // All exprs (key_exprs_lhs_ and key_exprs_rhs_) must have been prepared and opened
  // before calling this.
  bool operator() (TupleRow* lhs, TupleRow* rhs) const {
    int result = Compare(lhs, rhs);
    if (result < 0) return true;
    return false;
  }

  bool operator() (Tuple* lhs, Tuple* rhs) const {
    TupleRow* lhs_row = reinterpret_cast<TupleRow*>(&lhs);
    TupleRow* rhs_row = reinterpret_cast<TupleRow*>(&rhs);
    return (*this)(lhs_row, rhs_row);
  }

 private:
  std::vector<ExprContext*> key_expr_ctxs_lhs_;
  std::vector<ExprContext*> key_expr_ctxs_rhs_;
  std::vector<bool> is_asc_;
  std::vector<int8_t> nulls_first_;
};

// Compares the equality of two Tuples, going slot by slot.
struct TupleEqualityChecker {
  TupleDescriptor* tuple_desc_;

  TupleEqualityChecker(TupleDescriptor* tuple_desc) : tuple_desc_(tuple_desc) {
  }

  bool operator() (Tuple* x, Tuple* y) {
    const std::vector<SlotDescriptor*>& slots = tuple_desc_->slots();
    for (int i = 0; i < slots.size(); ++i) {
      SlotDescriptor* slot = slots[i];

      if (slot->is_nullable()) {
        const NullIndicatorOffset& null_offset = slot->null_indicator_offset();
        if (x->IsNull(null_offset) || y->IsNull(null_offset)) {
          if (x->IsNull(null_offset) && y->IsNull(null_offset)) {
            continue;
          } else {
            return false;
          }
        }
      }

      int tuple_offset = slot->tuple_offset();
      const ColumnType& type = slot->type();
      if (!RawValue::Eq(x->GetSlot(tuple_offset), y->GetSlot(tuple_offset), type)) {
        return false;
      }
    }

    return true;
  }
};

// Compares the equality of two TupleRows, going tuple by tuple.
struct RowEqualityChecker {
  std::vector<TupleEqualityChecker> tuple_checkers_;

  RowEqualityChecker(const RowDescriptor& row_desc) {
    const std::vector<TupleDescriptor*>& tuple_descs = row_desc.tuple_descriptors();
    for (int i = 0; i < tuple_descs.size(); ++i) {
      tuple_checkers_.push_back(TupleEqualityChecker(tuple_descs[i]));
    }
  }

  bool operator() (TupleRow* x, TupleRow* y) {
    for (int i = 0; i < tuple_checkers_.size(); ++i) {
      Tuple* x_tuple = x->GetTuple(i);
      Tuple* y_tuple = y->GetTuple(i);
      if (!tuple_checkers_[i](x_tuple, y_tuple)) return false;
    }

    return true;
  }
};

}

#endif

