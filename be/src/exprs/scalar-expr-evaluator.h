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

#ifndef IMPALA_EXPRS_SCALAR_EXPR_EVALUATOR_H
#define IMPALA_EXPRS_SCALAR_EXPR_EVALUATOR_H

#include <boost/scoped_ptr.hpp>

#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr-value.h"
#include "udf/udf-internal.h" // for CollectionVal
#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;
using impala_udf::CollectionVal;
using impala_udf::StructVal;
using impala_udf::DateVal;

class MemPool;
class RuntimeState;
class ScalarExpr;
class Status;
class TupleRow;

/// ScalarExprEvaluator is the interface for evaluating a scalar expression. It holds a
/// reference to the root of a ScalarExpr tree, runtime state (e.g. FunctionContexts)
/// needed during evaluation and also a buffer for the expression evaluation result.
/// A single evaluator is not thread-safe. It implements Get*Val() interfaces for every
/// possible return type and drives the execution by calling the Get*Val() function of
/// the root ScalarExpr with the input tuple row.
///
/// A ScalarExprEvaluator is created using the Create() interface. It must be initialized
/// by calling Open() before use and Close() must also be called to free up resources
/// owned by the evaluator.
///
/// FunctionContext is the interface for Impala to communicate with built-in functions,
/// UDF and UDAF. It is passed to UDF/UDAF to store its thread-private states, propagate
/// errors and allocate memory. An evaluator contains a vector of FunctionContext for
/// the ScalarExpr nodes in the Expr tree. The index of each node's entry is defined in
/// the its 'fn_ctx_idx_' field. The range in the vector for the sub-expression tree
/// rooted at a node is defined by [fn_ctx_idx_start_, fn_ctx_idx_end_).
///
class ScalarExprEvaluator {
 public:
  ~ScalarExprEvaluator();

  /// Creates an evaluator for the scalar expression tree rooted at 'expr' and all
  /// FunctionContexts needed during evaluation.
  ///
  /// Permanent allocations (i.e. those that must live until the evaluator is closed) come
  /// from 'expr_perm_pool'. Allocations that may contain expr results (i.e. the
  /// results of GetValue(), GetStringVal(), etc) come from 'expr_results_pool'. Lifetime
  /// of memory in 'expr_results_pool' is managed by the owner of the pool and may freed
  /// by the owner at any time except when the evaluator is in the middle of evaluating
  /// the expression. These pools can be shared between evaluators (so long as the
  /// required memory lifetimes are compatible) but cannot be shared between threads
  /// since MemPools are not thread-safe.
  ///
  /// Note that the caller is responsible to call Close() on all evaluators even if this
  /// function returns error status on initialization failure.
  static Status Create(const ScalarExpr& expr, RuntimeState* state, ObjectPool* pool,
      MemPool* expr_perm_pool, MemPool* expr_results_pool,
      ScalarExprEvaluator** eval) WARN_UNUSED_RESULT;

  /// Convenience function for creating multiple ScalarExprEvaluators. The evaluators
  /// are returned in 'evals'.
  static Status Create(const std::vector<ScalarExpr*>& exprs, RuntimeState* state,
      ObjectPool* pool, MemPool* expr_perm_pool, MemPool* expr_results_pool,
      std::vector<ScalarExprEvaluator*>* evals) WARN_UNUSED_RESULT;

  /// Initializes the ScalarExprEvaluator on all nodes in the ScalarExpr tree. This is
  /// also the location in which constant arguments to functions are computed. Does not
  /// need to be called on clones. Idempotent (this allows exprs to be opened multiple
  /// times in subplans without reinitializing function states).
  Status Open(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Convenience function for opening multiple ScalarExprEvaluators.
  static Status Open(const std::vector<ScalarExprEvaluator*>& evals,
      RuntimeState* state) WARN_UNUSED_RESULT;

  /// Free resources held by this evaluator. Must be called on every ScalarExprEvaluator,
  /// including clones. Has no effect if already closed.
  void Close(RuntimeState* state);

  /// Convenience function for closing multiple ScalarExprEvaluators.
  static void Close(const std::vector<ScalarExprEvaluator*>& evals,
      RuntimeState* state);

  /// Creates a copy of this ScalarExprEvaluator. Open() must be called first. The copy
  /// contains clones of each FunctionContext, which share the fragment-local state of the
  /// original one but have their own memory and thread-local state. This should be used
  /// to create an ScalarExprEvaluator for each execution thread that needs to evaluate
  /// 'root_'. 'expr_perm_pool' and 'expr_results_pool' are used for allocations so callers
  /// must use different MemPools for evaluators in different threads. Note that clones
  /// are considered opened. The cloned ScalarExprEvaluator cannot be used after the
  /// original ScalarExprEvaluator is destroyed because it may reference fragment-local
  /// state from the original.
  /// TODO: IMPALA-4743: Evaluate input arguments in ScalarExpr::Init() and store them
  /// in ScalarExpr.
  Status Clone(ObjectPool* pool, RuntimeState* state, MemPool* expr_perm_pool,
      MemPool* expr_results_pool, ScalarExprEvaluator** new_eval) const WARN_UNUSED_RESULT;

  /// Convenience functions for cloning multiple ScalarExprEvaluators. The newly
  /// created evaluators are appended to 'new_evals.
  static Status Clone(ObjectPool* pool, RuntimeState* state, MemPool* expr_perm_pool,
      MemPool* expr_results_pool, const std::vector<ScalarExprEvaluator*>& evals,
      std::vector<ScalarExprEvaluator*>* new_evals) WARN_UNUSED_RESULT;

  /// If 'expr' is constant, evaluates it with no input row argument and returns the
  /// result in 'const_val'. Sets 'const_val' to NULL if the argument is not constant.
  /// The returned AnyVal and associated varlen data is owned by this evaluator. This
  /// should only be called after Open() has been called on this expr. Returns an error
  /// if there was an error evaluating the expression or if memory could not be allocated
  /// for the expression result.
  Status GetConstValue(
      RuntimeState* state, const ScalarExpr& expr, AnyVal** const_val) WARN_UNUSED_RESULT;

  /// Calls the appropriate Get*Val() function on 'e' and stores the result in result_.
  /// This is used by ScalarExpr to call GetValue() on sub-expression, rather than root_.
  void* GetValue(const ScalarExpr& e, const TupleRow* row);

  /// Calls the appropriate Get*Val() function on this evaluator's root_ expr tree, stores
  /// the result in 'result_' and returns a pointer to it.
  void* GetValue(const TupleRow* row);

  /// Evaluates the expression of this evaluator on tuple row 'row' and returns
  /// the results. One function for each data type implemented.
  BooleanVal GetBooleanVal(const TupleRow* row);
  TinyIntVal GetTinyIntVal(const TupleRow* row);
  SmallIntVal GetSmallIntVal(const TupleRow* row);
  IntVal GetIntVal(const TupleRow* row);
  BigIntVal GetBigIntVal(const TupleRow* row);
  FloatVal GetFloatVal(const TupleRow* row);
  DoubleVal GetDoubleVal(const TupleRow* row);
  StringVal GetStringVal(const TupleRow* row);
  CollectionVal GetCollectionVal(const TupleRow* row);
  StructVal GetStructVal(const TupleRow* row);
  TimestampVal GetTimestampVal(const TupleRow* row);
  DecimalVal GetDecimalVal(const TupleRow* row);
  DateVal GetDateVal(const TupleRow* row);

  /// Helper to evaluate a boolean expression with predicate semantics, where NULL is
  /// equivalent to false.
  bool EvalPredicate(TupleRow* row) {
    BooleanVal v = GetBooleanVal(row);
    if (v.is_null || !v.val) return false;
    return true;
  }

  /// Helper to evaluate a boolean expression with predicate semantics, where NULL is
  /// equivalent to true.
  bool EvalPredicateAcceptNull(TupleRow* row) {
    BooleanVal v = GetBooleanVal(row);
    if (v.is_null || v.val) return true;
    return false;
  }

  /// Returns an error status if there was any error in evaluating the expression
  /// or its sub-expressions. 'start_idx' and 'end_idx' correspond to the range
  /// within the vector of FunctionContext for the sub-expressions of interest.
  /// The default parameters correspond to the entire expr 'root_'.
  Status GetError(int start_idx = 0, int end_idx = -1) const WARN_UNUSED_RESULT;

  /// Convenience functions: print value into 'str' or 'stream'. NULL turns into "NULL".
  /// The first two variants will evaluate the tuple row against 'root_'.
  void PrintValue(const TupleRow* row, std::string* str);
  void PrintValue(const TupleRow* row, std::stringstream* stream);
  void PrintValue(void* value, std::string* str);
  void PrintValue(void* value, std::stringstream* stream);

  /// Get the number of digits after the decimal that should be displayed for this value.
  /// Returns -1 if no scale has been specified (currently the scale is only set for
  /// doubles set by RoundUpTo). GetValue() must have already been called.
  /// TODO: remove this (IMPALA-4720).
  int output_scale() const { return output_scale_; }
  const ScalarExpr& root() const { return root_; }
  bool opened() const { return opened_; }
  bool closed() const { return closed_; }
  bool is_clone() const { return is_clone_; }
  MemPool* expr_perm_pool() const { return expr_perm_pool_; }

  /// The builtin functions are not called from anywhere in the code and the
  /// symbols are therefore not included in the binary. We call these functions
  /// by using dlsym. The compiler must think this function is callable to
  /// not strip these symbols.
  static void InitBuiltinsDummy();

  std::vector<ScalarExprEvaluator*>& GetChildEvaluators() { return childEvaluators_; }

  static const char* LLVM_CLASS_NAME;

 protected:
  /// Users of fn_context();
  friend class CaseExpr;
  friend class CastFormatExpr;
  friend class HiveUdfCall;
  friend class KuduPartitionExpr;
  friend class ScalarFnCall;

  /// Retrieves a registered FunctionContext. 'i' is the 'fn_context_index_' of the
  /// corresponding sub-expression in the Expr tree.
  FunctionContext* fn_context(int i) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, fn_ctxs_.size());
    return fn_ctxs_[i];
  }

 private:
  friend class ScalarExpr;
  friend class SlotRef;

  /// FunctionContexts for nodes in this Expr tree. Created by this ScalarExprEvaluator
  /// and live in the same object pool as this evaluator (i.e. same life span as the
  /// evaluator).
  std::vector<FunctionContext*> fn_ctxs_;

  /// Array access to fn_ctxs_. Used by ScalarFnCall's codegend compute function
  /// to access the correct FunctionContext.
  FunctionContext** fn_ctxs_ptr_ = nullptr;

  /// Pointer to the MemPool which all permanent allocations (including those from
  /// 'fn_ctxs_') come from. Owned by the exec node or data sink which owns this
  /// evaluator.
  MemPool* const expr_perm_pool_;

  /// The expr tree which this evaluator is for.
  const ScalarExpr& root_;

  /// Stores the result of evaluation for this expr tree (or any sub-expression).
  /// This is used in interpreted path when we need to return a void* and to store the
  /// children of a struct expression in both interpreted and codegen mode.
  ExprValue result_;

  /// For a struct scalar expression there is one evaluator created for each child of
  /// the struct. This is empty for non-struct expressions.
  std::vector<ScalarExprEvaluator*> childEvaluators_;

  /// True if this evaluator came from a Clone() call. Used to manage FunctionStateScope.
  bool is_clone_ = false;

  /// Variables keeping track of current state.
  bool initialized_ = false;
  bool opened_ = false;
  bool closed_ = false;

  /// The number of digits after the decimal that should be displayed for this value.
  /// -1 if no scale has been specified (currently the scale is only set for doubles
  /// set by RoundUpTo). This value relies on FunctionContext to be allocated first
  /// before it's derived so it lives in the evaluator instead of Expr.
  /// TODO: move this to Expr initialization after IMPALA-4743 is fixed.
  int output_scale_ = -1;

  ScalarExprEvaluator(const ScalarExpr& root, MemPool* expr_perm_pool,
      MemPool* expr_results_pool);

  /// Walks the expression tree 'expr' and fills in 'fn_ctxs_' for all Expr nodes
  /// which need FunctionContext.
  void CreateFnCtxs(RuntimeState* state, const ScalarExpr& expr, MemPool* expr_perm_pool,
      MemPool* expr_results_pool);

  // Helper functions for codegen.

  // Converts and stores 'val' to 'result_' according to its type. Intended to be called
  // from codegen code.
  void* StoreResult(const AnyVal& val, const ColumnType& type);
  static FunctionContext* GetFunctionContext(ScalarExprEvaluator* eval, int fn_ctx_idx);
  static ScalarExprEvaluator* GetChildEvaluator(ScalarExprEvaluator* eval, int idx);
};
}

#endif
