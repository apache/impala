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
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/raw-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace impala {

class RuntimeState;
class ScalarExprEvaluator;

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

/// Interface for comparing two TupleRows based on a set of exprs.
class TupleRowComparator {
 public:
  /// 'ordering_exprs': the ordering expressions for tuple comparison.
  TupleRowComparator(const std::vector<ScalarExpr*>& ordering_exprs)
    : ordering_exprs_(ordering_exprs),
      codegend_compare_fn_(nullptr) { }
  virtual ~TupleRowComparator() {}

  /// Create the evaluators for the ordering expressions and store them in 'pool'. The
  /// evaluators use 'expr_perm_pool' and 'expr_results_pool' for permanent and result
  /// allocations made by exprs respectively. 'state' is passed in for initialization
  /// of the evaluator.
  Status Open(ObjectPool* pool, RuntimeState* state, MemPool* expr_perm_pool,
      MemPool* expr_results_pool);

  /// Release resources held by the ordering expressions' evaluators.
  void Close(RuntimeState* state);

  /// Codegens a Compare() function for this comparator that is used in Compare().
  Status Codegen(RuntimeState* state);

  /// Returns a negative value if lhs is less than rhs, a positive value if lhs is
  /// greater than rhs, or 0 if they are equal. All exprs (ordering_exprs_lhs_ and
  /// ordering_exprs_rhs_) must have been prepared and opened before calling this,
  /// i.e. 'sort_key_exprs' in the constructor must have been opened.
  int ALWAYS_INLINE Compare(const TupleRow* lhs, const TupleRow* rhs) const {
    return codegend_compare_fn_ == NULL ?
        CompareInterpreted(lhs, rhs) :
        (*codegend_compare_fn_)(ordering_expr_evals_lhs_.data(),
            ordering_expr_evals_rhs_.data(), lhs, rhs);
  }

  /// Returns true if lhs is strictly less than rhs.
  /// All exprs (ordering_exprs_lhs_ and ordering_exprs_rhs_) must have been prepared
  /// and opened before calling this.
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

 protected:
  /// References to ordering expressions owned by the Exec node which owns this
  /// TupleRowComparator.
  const std::vector<ScalarExpr*>& ordering_exprs_;

  /// The evaluators for the LHS and RHS ordering expressions. The RHS evaluator is
  /// created via cloning the evaluator after it has been Opened().
  std::vector<ScalarExprEvaluator*> ordering_expr_evals_lhs_;
  std::vector<ScalarExprEvaluator*> ordering_expr_evals_rhs_;

  /// We store a pointer to the codegen'd function pointer (adding an extra level of
  /// indirection) so that copies of this TupleRowComparator will have the same pointer to
  /// the codegen'd function. This is necessary because the codegen'd function pointer is
  /// only set after the IR module is compiled. Without the indirection, if this
  /// TupleRowComparator is copied before the module is compiled, the copy will still have
  /// its function pointer set to NULL. The function pointer is allocated from the runtime
  /// state's object pool so that its lifetime will be >= that of any copies.
  typedef int (*CompareFn)(ScalarExprEvaluator* const*, ScalarExprEvaluator* const*,
      const TupleRow*, const TupleRow*);
  CompareFn* codegend_compare_fn_;
 private:
  /// Interpreted implementation of Compare().
  virtual int CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const = 0;

  /// Codegen Compare(). Returns a non-OK status if codegen is unsuccessful.
  /// TODO: inline this at codegen'd callsites instead of indirectly calling via function
  /// pointer.
  virtual Status CodegenCompare(LlvmCodeGen* codegen, llvm::Function** fn) = 0;
};

/// Compares two TupleRows based on a set of exprs, in lexicographical order.
class TupleRowLexicalComparator : public TupleRowComparator {
 public:
  /// 'ordering_exprs': the ordering expressions for tuple comparison.
  /// 'is_asc' determines, for each expr, if it should be ascending or descending sort
  /// order.
  /// 'nulls_first' determines, for each expr, if nulls should come before or after all
  /// other values.
  TupleRowLexicalComparator(const std::vector<ScalarExpr*>& ordering_exprs,
      const std::vector<bool>& is_asc, const std::vector<bool>& nulls_first)
    : TupleRowComparator(ordering_exprs),
      is_asc_(is_asc) {
    DCHECK_EQ(is_asc_.size(), ordering_exprs.size());
    for (bool null_first : nulls_first) nulls_first_.push_back(null_first ? -1 : 1);
  }

 private:
  const std::vector<bool>& is_asc_;
  std::vector<int8_t> nulls_first_;

  int CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const override;

  /// TODO: implement it.
  Status CodegenCompare(LlvmCodeGen* codegen, llvm::Function** fn) override;
};

/// Compares two TupleRows based on a set of exprs, in Z-order.
class TupleRowZOrderComparator : public TupleRowComparator {
 public:
  /// 'ordering_exprs': the ordering expressions for tuple comparison.
  TupleRowZOrderComparator(const std::vector<ScalarExpr*>& ordering_exprs)
    : TupleRowComparator(ordering_exprs) { }

 private:
  typedef __uint128_t uint128_t;

  int CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const override;
  Status CodegenCompare(LlvmCodeGen* codegen, llvm::Function** fn) override;

  /// Compares the rows using Z-ordering. The function does not calculate the actual
  /// Z-value, only looks for the column with the most significant dimension, and compares
  /// the values of that column. To make this possible, a unified type is necessary where
  /// all values share the same bit-representation. The three common types which all the
  /// others are converted to are uint32_t, uint64_t and uint128_t. Comparing smaller
  /// types (ie. having less bits) with bigger ones would make the bigger type much more
  /// dominant therefore the bits of these smaller types are shifted up.
  template<typename U>
  int CompareBasedOnSize(const TupleRow* lhs, const TupleRow* rhs) const;

  /// We transform the original a and b values to their "shared representation", a' and b'
  /// in a way that if a < b then a' is lexically less than b' regarding to their bits.
  /// Thus, for ints INT_MIN would be 0, INT_MIN+1 would be 1, and so on, and in the end
  /// INT_MAX would be 111..111.
  /// The basic concept of getting the shared representation is as follows:
  /// 1. Reinterpret void* as the actual type
  /// 2. Convert the number to the chosen unsigned type (U)
  /// 3. If U is bigger than the actual type, the bits of the small type are shifted up.
  /// 4. Flip the sign bit because the value was converted to unsigned.
  /// Note that floating points are represented differently, where for negative values all
  /// bits have to get flipped.
  /// Null values will be treated as the minimum value (unsigned 0).
  template <typename U>
  U GetSharedRepresentation(void* val, ColumnType type) const;
  template <typename U>
  U inline GetSharedStringRepresentation(const char* char_ptr, int length) const;
  template <typename U, typename T>
  U inline GetSharedIntRepresentation(const T val, U mask) const;
  template <typename U, typename T>
  U inline GetSharedFloatRepresentation(void* val, U mask) const;
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
