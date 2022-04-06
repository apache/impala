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

#include "codegen/codegen-fn-ptr.h"
#include "codegen/impala-ir.h"
#include "common/compiler-util.h"
#include "exprs/scalar-expr.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/raw-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace impala {

class FragmentState;
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

class TupleRowComparator;

/// TupleRowComparatorConfig contains the static state initialized from its corresponding
/// thrift structure. It serves as an input for creating instances of the
/// TupleRowComparator class.
class TupleRowComparatorConfig {
 public:
  /// 'tsort_info' : the thrift struct contains all relevant input params.
  /// 'ordering_exprs': the ordering expressions for tuple comparison created from the
  /// params in 'tsort_info'.
  TupleRowComparatorConfig(
      const TSortInfo& tsort_info, const std::vector<ScalarExpr*>& ordering_exprs);

  /// Codegens a Compare() function for this comparator that is used in Compare().
  /// The pointer to this llvm function object is returned in 'compare_fn'.
  Status Codegen(FragmentState* state, llvm::Function** compare_fn);

  /// A version of the above Cdegen() function that does not care the llvm function
  /// object.
  Status Codegen(FragmentState* state) {
    llvm::Function* compare_fn = nullptr;
    return Codegen(state, &compare_fn);
  }

  /// Indicates the sorting ordering used. Specified using the SORT BY clause.
  TSortingOrder::type sorting_order_;

  /// References to ordering expressions owned by the plan node which owns this
  /// TupleRowComparatorConfig.
  const std::vector<ScalarExpr*>& ordering_exprs_;

  /// Number of leading keys in the ordering exprs that should be sorted lexically.
  /// Only used in Z-order pre-insert sort node to sort rows lexically on partition keys.
  int num_lexical_keys_;

  /// Indicates, for each ordering expr, whether it enforces an ascending order or not.
  /// Not Owned.
  const std::vector<bool>& is_asc_;

  /// Indicates, for each ordering expr, if nulls should be listed first or last. As a
  /// optimization for the TupleRowComparator::Compare() implementation, true/false is
  /// stored as +/- 1 respectively. This is independent of is_asc_.
  std::vector<int8_t> nulls_first_;

  /// Codegened version of TupleRowComparator::Compare().
  typedef int (*CompareFn)(const TupleRowComparator*, ScalarExprEvaluator* const*,
      ScalarExprEvaluator* const*, const TupleRow*, const TupleRow*);
  CodegenFnPtr<CompareFn> codegend_compare_fn_;

 private:
  /// Codegen TupleRowLexicalComparator::Compare(). Returns a non-OK status if codegen is
  /// unsuccessful. TODO: inline this at codegen'd callsites instead of indirectly calling
  /// via function pointer.
  Status CodegenLexicalCompare(LlvmCodeGen* codegen, llvm::Function** fn);
};

/// Interface for comparing two TupleRows based on a set of exprs.
class TupleRowComparator {
 public:
  TupleRowComparator(const TupleRowComparatorConfig& config)
    : ordering_exprs_(config.ordering_exprs_),
      codegend_compare_fn_(config.codegend_compare_fn_) {
  }

  virtual ~TupleRowComparator() {}

  /// Create the evaluators for the ordering expressions and store them in 'pool'. The
  /// evaluators use 'expr_perm_pool' and 'expr_results_pool' for permanent and result
  /// allocations made by exprs respectively. 'state' is passed in for initialization
  /// of the evaluator.
  Status Open(ObjectPool* pool, RuntimeState* state, MemPool* expr_perm_pool,
      MemPool* expr_results_pool);

  /// Release resources held by the ordering expressions' evaluators.
  void Close(RuntimeState* state);

  /// 3-way comparator of lhs and rhs. Returns 0 if lhs==rhs
  /// All exprs (ordering_exprs_lhs_ and ordering_exprs_rhs_) must have been prepared
  /// and opened before calling this.
  /// Force inlining because it tends not to be always inlined at callsites, even in
  /// hot loops.
  int ALWAYS_INLINE Compare(const TupleRow* lhs, const TupleRow* rhs) const {
    return Compare(
        ordering_expr_evals_lhs_.data(), ordering_expr_evals_rhs_.data(), lhs, rhs);
  }

  int ALWAYS_INLINE Compare(const Tuple* lhs, const Tuple* rhs) const {
    TupleRow* lhs_row = reinterpret_cast<TupleRow*>(&lhs);
    TupleRow* rhs_row = reinterpret_cast<TupleRow*>(&rhs);
    return Compare(lhs_row, rhs_row);
  }

  /// Returns true if lhs is strictly less than rhs.
  /// All exprs (ordering_exprs_lhs_ and ordering_exprs_rhs_) must have been prepared
  /// and opened before calling this.
  /// Force inlining because it tends not to be always inlined at callsites, even in
  /// hot loops.
  bool ALWAYS_INLINE Less(const TupleRow* lhs, const TupleRow* rhs) const {
    return Compare(
        ordering_expr_evals_lhs_.data(), ordering_expr_evals_rhs_.data(), lhs, rhs) < 0;
  }

  bool ALWAYS_INLINE LessCodegend(const TupleRow* lhs, const TupleRow* rhs) const {
    if (codegend_compare_fn_non_atomic_ != nullptr) {
      return codegend_compare_fn_non_atomic_(this,
          ordering_expr_evals_lhs_.data(), ordering_expr_evals_rhs_.data(), lhs, rhs) < 0;
    } else {
      TupleRowComparatorConfig::CompareFn fn = codegend_compare_fn_.load();
      if (fn != nullptr) codegend_compare_fn_non_atomic_ = fn;
    }
    return Compare(
        ordering_expr_evals_lhs_.data(), ordering_expr_evals_rhs_.data(), lhs, rhs) < 0;
  }

  bool ALWAYS_INLINE Less(const Tuple* lhs, const Tuple* rhs) const {
    TupleRow* lhs_row = reinterpret_cast<TupleRow*>(&lhs);
    TupleRow* rhs_row = reinterpret_cast<TupleRow*>(&rhs);
    return Less(lhs_row, rhs_row);
  }

  /// A Symbol (or a substring of the symbol) of following.
  ///
  /// int Compare(ScalarExprEvaluator* const* evaluator_lhs,
  ///    ScalarExprEvaluator* const* evaluator_rhs, const TupleRow* lhs,
  ///    const TupleRow* rhs) const;
  ///
  /// It is passed to LlvmCodeGen::ReplaceCallSites().
  static const char* COMPARE_SYMBOL;

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 protected:
  TupleRowComparator(const std::vector<ScalarExpr*>& ordering_exprs,
      const CodegenFnPtr<TupleRowComparatorConfig::CompareFn>& codegend_compare_fn)
    : ordering_exprs_(ordering_exprs),
      codegend_compare_fn_(codegend_compare_fn) {
  }

  /// References to ordering expressions owned by the plan node which owns the
  /// TupleRowComparatorConfig used to create this instance.
  const std::vector<ScalarExpr*>& ordering_exprs_;

  /// The evaluators for the LHS and RHS ordering expressions. The RHS evaluator is
  /// created via cloning the evaluator after it has been Opened().
  std::vector<ScalarExprEvaluator*> ordering_expr_evals_lhs_;
  std::vector<ScalarExprEvaluator*> ordering_expr_evals_rhs_;

  /// Reference to the codegened function pointer owned by the TupleRowComparatorConfig
  /// object that was used to create this instance.
  const CodegenFnPtr<TupleRowComparatorConfig::CompareFn>& codegend_compare_fn_;
  mutable TupleRowComparatorConfig::CompareFn codegend_compare_fn_non_atomic_ = nullptr;

 private:
  /// Interpreted implementation of Compare().
  virtual int CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const = 0;

  /// Returns a negative value if lhs is less than rhs, a positive value if lhs is
  /// greater than rhs, or 0 if they are equal. All exprs (ordering_exprs_lhs_ and
  /// ordering_exprs_rhs_) must have been prepared and opened before calling this,
  /// i.e. 'sort_key_exprs' in the constructor must have been opened.
  ///
  /// This method calls CompareInterpreted(lhs, rhs) in non-code-gen version and
  /// morphs into the code-gen version otherwise.
  ///
  /// The presence of evaluator_lhs and evaluator_rhs in argument is to match similar
  /// arguments in the code-gen version.
  ///
  /// Mark the method IR_NO_INLINE to facilitate call site replacement during code-gen.
  /// Do not remove this attribute.
  IR_NO_INLINE int Compare(ScalarExprEvaluator* const* evaluator_lhs,
      ScalarExprEvaluator* const* evaluator_rhs, const TupleRow* lhs,
      const TupleRow* rhs) const;
};

/// Compares two TupleRows based on a set of exprs, in lexicographical order.
class TupleRowLexicalComparator : public TupleRowComparator {
 public:
  /// 'ordering_exprs': the ordering expressions for tuple comparison.
  /// 'is_asc' determines, for each expr, if it should be ascending or descending sort
  /// order.
  /// 'nulls_first' determines, for each expr, if nulls should come before or after all
  /// other values.
  TupleRowLexicalComparator(const TupleRowComparatorConfig& config)
    : TupleRowComparator(config),
      is_asc_(config.is_asc_),
      nulls_first_(config.nulls_first_) {
    DCHECK(config.sorting_order_ == TSortingOrder::LEXICAL);
    DCHECK_EQ(nulls_first_.size(), ordering_exprs_.size());
    DCHECK_EQ(is_asc_.size(), ordering_exprs_.size());
  }

 private:
  const std::vector<bool>& is_asc_;
  const std::vector<int8_t>& nulls_first_;

  int CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const override;
};

/// Compares two TupleRows based on a set of exprs. The first 'num_lexical_keys' exprs
/// are compared lexically, while the remaining exprs are compared in Z-order.
class TupleRowZOrderComparator : public TupleRowComparator {
 public:
  /// 'ordering_exprs': the ordering expressions for tuple comparison.
  TupleRowZOrderComparator(const TupleRowComparatorConfig& config)
    : TupleRowComparator(config),
      num_lexical_keys_(config.num_lexical_keys_) {
    DCHECK(config.sorting_order_ == TSortingOrder::ZORDER);
    DCHECK_GE(num_lexical_keys_, 0);
    DCHECK_LT(num_lexical_keys_, ordering_exprs_.size());

    // The algorithm requires all values having a common type, without loss of data.
    // This means we have to find the biggest type.
    max_col_size_ = ordering_exprs_[num_lexical_keys_]->type().GetByteSize();
    for (int i = num_lexical_keys_ + 1; i < ordering_exprs_.size(); ++i) {
      if (ordering_exprs_[i]->type().GetByteSize() > max_col_size_) {
        max_col_size_ = ordering_exprs_[i]->type().GetByteSize();
      }
    }
  }

 private:
  typedef __uint128_t uint128_t;

  int CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const override;

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

  /// Number of leading keys that should be sorted lexically.
  int num_lexical_keys_;
  /// The biggest type size of the ordering slots.
  int max_col_size_;
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
