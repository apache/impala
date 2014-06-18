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
  TupleRowComparator(const std::vector<Expr*>& key_exprs_lhs,
      const std::vector<Expr*>& key_exprs_rhs,
      const std::vector<bool>& is_asc,
      const std::vector<bool>& nulls_first)
      : key_exprs_lhs_(key_exprs_lhs), key_exprs_rhs_(key_exprs_rhs) {
    DCHECK_EQ(key_exprs_lhs.size(), key_exprs_rhs.size());
    DCHECK_EQ(key_exprs_lhs.size(), is_asc.size());
    DCHECK_EQ(key_exprs_lhs.size(), nulls_first.size());
    is_asc_.reserve(key_exprs_lhs.size());
    nulls_first_.reserve(key_exprs_lhs.size());
    for (int i = 0; i < key_exprs_lhs.size(); ++i) {
      is_asc_.push_back(is_asc[i] ? 1 : 0);
      nulls_first_.push_back(nulls_first[i] ? 1 : 0);
    }
  }

  // Returns true if lhs is strictly less than rhs.
  // All exprs (key_exprs_lhs_ and key_exprs_rhs_) must have been prepared and opened
  // before calling this.
  bool operator() (TupleRow* lhs, TupleRow* rhs) const {
    std::vector<Expr*>::const_iterator lhs_expr_iter = key_exprs_lhs_.begin();
    std::vector<Expr*>::const_iterator rhs_expr_iter = key_exprs_rhs_.begin();
    std::vector<uint8_t>::const_iterator is_asc_iter = is_asc_.begin();
    std::vector<uint8_t>::const_iterator nulls_first_iter = nulls_first_.begin();

    for (;lhs_expr_iter != key_exprs_lhs_.end();
         ++lhs_expr_iter, ++rhs_expr_iter, ++is_asc_iter, ++nulls_first_iter) {
      Expr* lhs_expr = *lhs_expr_iter;
      Expr* rhs_expr = *rhs_expr_iter;
      void* lhs_value = lhs_expr->GetValue(lhs);
      void* rhs_value = rhs_expr->GetValue(rhs);
      bool less_than = static_cast<bool>(*is_asc_iter);
      bool nulls_first = static_cast<bool>(*nulls_first_iter);

      // The sort order of NULLs is independent of asc/desc.
      if (lhs_value == NULL && rhs_value == NULL) continue;
      if (lhs_value == NULL && rhs_value != NULL) return nulls_first;
      if (lhs_value != NULL && rhs_value == NULL) return !nulls_first;

      int result = RawValue::Compare(lhs_value, rhs_value, lhs_expr->type());
      if (!less_than) result = -result;
      if (result > 0) return false;
      if (result < 0) return true;
      // Otherwise, try the next Expr
    }
    return false;  // fully equivalent key -> x is not strictly before y
  }

  bool operator() (Tuple* lhs, Tuple* rhs) const {
    TupleRow* lhs_row = reinterpret_cast<TupleRow*>(&lhs);
    TupleRow* rhs_row = reinterpret_cast<TupleRow*>(&rhs);
    return (*this)(lhs_row, rhs_row);
  }

 private:
  std::vector<Expr*> key_exprs_lhs_;
  std::vector<Expr*> key_exprs_rhs_;
  std::vector<uint8_t> is_asc_;
  std::vector<uint8_t> nulls_first_;
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

