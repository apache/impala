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


#ifndef IMPALA_UTIL_TUPLE_ROW_COMPARE_H_
#define IMPALA_UTIL_TUPLE_ROW_COMPARE_H_

#include "common/compiler-util.h"
#include "exec/sort-exec-exprs.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/raw-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace impala {

/// A wrapper around types Comparator with a Less() method. This wrapper allows the use of
/// type Comparator with STL containers which expect a type like std::less<T>, which uses
/// operator() instead of Less() and is cheap to copy.
///
/// The C++ standard requires that std::priority_queue operations behave as wrappers to
/// {push,pop,make,sort}_heap, which take their comparator object by value. Therefore, it
/// is inefficient to use comparator objects that have expensive construction,
/// destruction, and copying with std::priority_queue.
///
/// ComparatorWrapper takes a reference to an object of type Comparator, rather than
/// copying that object. ComparatorWrapper<Comparator>(comp) is not safe to use beyond the
/// lifetime of comp.
template <typename Comparator>
class ComparatorWrapper {
  const Comparator& comp_;
 public:
  ComparatorWrapper(const Comparator& comp) : comp_(comp) {}

  template <typename T>
  bool operator()(const T& lhs, const T& rhs) const {
    return comp_.Less(lhs, rhs);
  }
};

/// Compares two TupleRows based on a set of exprs, in order.
class TupleRowComparator {
 public:
  /// 'sort_key_exprs' must have already been prepared.
  /// 'is_asc' determines, for each expr, if it should be ascending or descending sort
  /// order.
  /// 'nulls_first' determines, for each expr, if nulls should come before or after all
  /// other values.
  TupleRowComparator(
      const SortExecExprs& sort_key_exprs,
      const std::vector<bool>& is_asc,
      const std::vector<bool>& nulls_first)
      : key_expr_ctxs_lhs_(sort_key_exprs.lhs_ordering_expr_ctxs()),
        key_expr_ctxs_rhs_(sort_key_exprs.rhs_ordering_expr_ctxs()),
        is_asc_(is_asc),
        codegend_compare_fn_(NULL) {
    DCHECK_EQ(key_expr_ctxs_lhs_.size(), is_asc.size());
    DCHECK_EQ(key_expr_ctxs_lhs_.size(), nulls_first.size());
    nulls_first_.reserve(key_expr_ctxs_lhs_.size());
    for (int i = 0; i < key_expr_ctxs_lhs_.size(); ++i) {
      nulls_first_.push_back(nulls_first[i] ? -1 : 1);
    }
  }

  TupleRowComparator(const SortExecExprs& sort_key_exprs, bool is_asc, bool nulls_first)
      : key_expr_ctxs_lhs_(sort_key_exprs.lhs_ordering_expr_ctxs()),
        key_expr_ctxs_rhs_(sort_key_exprs.rhs_ordering_expr_ctxs()),
        is_asc_(key_expr_ctxs_lhs_.size(), is_asc),
        nulls_first_(key_expr_ctxs_lhs_.size(), nulls_first ? -1 : 1),
        codegend_compare_fn_(NULL) {}

  /// Codegens a Compare() function for this comparator that is used in Compare().
  Status Codegen(RuntimeState* state);

  /// Returns a negative value if lhs is less than rhs, a positive value if lhs is greater
  /// than rhs, or 0 if they are equal. All exprs (key_exprs_lhs_ and key_exprs_rhs_) must
  /// have been prepared and opened before calling this, i.e. 'sort_key_exprs' in the
  /// constructor must have been opened.
  int ALWAYS_INLINE Compare(const TupleRow* lhs, const TupleRow* rhs) const {
    return codegend_compare_fn_ == NULL ?
        CompareInterpreted(lhs, rhs) :
        (*codegend_compare_fn_)(&key_expr_ctxs_lhs_[0], &key_expr_ctxs_rhs_[0], lhs, rhs);
  }

  /// Returns true if lhs is strictly less than rhs.
  /// All exprs (key_exprs_lhs_ and key_exprs_rhs_) must have been prepared and opened
  /// before calling this.
  /// Force inlining because it tends not to be always inlined at callsites, even in
  /// hot loops.
  bool ALWAYS_INLINE Less(const TupleRow* lhs, const TupleRow* rhs) const {
    return Compare(lhs, rhs) < 0;
  }

  bool ALWAYS_INLINE Less(const Tuple* lhs, const Tuple* rhs) const {
    TupleRow* lhs_row = reinterpret_cast<TupleRow*>(&lhs);
    TupleRow* rhs_row = reinterpret_cast<TupleRow*>(&rhs);
    return Less(lhs_row, rhs_row);
  }

  /// Free any local allocations made during expression evaluations in Compare().
  void FreeLocalAllocations() const {
    ExprContext::FreeLocalAllocations(key_expr_ctxs_lhs_);
    ExprContext::FreeLocalAllocations(key_expr_ctxs_rhs_);
  }

 private:
  /// Interpreted implementation of Compare().
  int CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const;

  /// Codegen Compare(). Returns a non-OK status if codegen is unsuccessful.
  /// TODO: inline this at codegen'd callsites instead of indirectly calling via function
  /// pointer.
  Status CodegenCompare(LlvmCodeGen* codegen, llvm::Function** fn);

  /// References to ExprContexts managed by SortExecExprs. The lhs ExprContexts must
  /// be created and prepared before the TupleRowCompator is constructed, but the rhs
  /// ExprContexts are only created via cloning when SortExecExprs is Open()ed (which
  /// may be after the TupleRowComparator is constructed).
  const std::vector<ExprContext*>& key_expr_ctxs_lhs_;
  const std::vector<ExprContext*>& key_expr_ctxs_rhs_;

  std::vector<bool> is_asc_;
  std::vector<int8_t> nulls_first_;

  /// We store a pointer to the codegen'd function pointer (adding an extra level of
  /// indirection) so that copies of this TupleRowComparator will have the same pointer to
  /// the codegen'd function. This is necessary because the codegen'd function pointer is
  /// only set after the IR module is compiled. Without the indirection, if this
  /// TupleRowComparator is copied before the module is compiled, the copy will still have
  /// its function pointer set to NULL. The function pointer is allocated from the runtime
  /// state's object pool so that its lifetime will be >= that of any copies.
  typedef int (*CompareFn)(ExprContext* const*, ExprContext* const*, const TupleRow*,
      const TupleRow*);
  CompareFn* codegend_compare_fn_;
};

/// Compares the equality of two Tuples, going slot by slot.
struct TupleEqualityChecker {
  TupleDescriptor* tuple_desc_;

  TupleEqualityChecker(TupleDescriptor* tuple_desc) : tuple_desc_(tuple_desc) {
  }

  bool Equal(Tuple* x, Tuple* y) {
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

/// Compares the equality of two TupleRows, going tuple by tuple.
struct RowEqualityChecker {
  std::vector<TupleEqualityChecker> tuple_checkers_;

  RowEqualityChecker(const RowDescriptor& row_desc) {
    const std::vector<TupleDescriptor*>& tuple_descs = row_desc.tuple_descriptors();
    for (int i = 0; i < tuple_descs.size(); ++i) {
      tuple_checkers_.push_back(TupleEqualityChecker(tuple_descs[i]));
    }
  }

  bool Equal(const TupleRow* x, const TupleRow* y) {
    for (int i = 0; i < tuple_checkers_.size(); ++i) {
      Tuple* x_tuple = x->GetTuple(i);
      Tuple* y_tuple = y->GetTuple(i);
      if (!tuple_checkers_[i].Equal(x_tuple, y_tuple)) return false;
    }

    return true;
  }
};

}

#endif
