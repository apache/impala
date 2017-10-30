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


#ifndef IMPALA_EXPRS_SCALAR_EXPR_H
#define IMPALA_EXPRS_SCALAR_EXPR_H

#include <memory>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/global-types.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/types.h"
#include "udf/udf-internal.h" // for CollectionVal
#include "udf/udf.h"

namespace llvm {
  class BasicBlock;
  class Function;
  class Type;
  class Value;
};

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

class LibCacheEntry;
class LlvmCodeGen;
class MemTracker;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class ScalarExprEvaluator;
class SlotDescriptor;
class TColumnValue;
class TExpr;
class TExprNode;
class Tuple;
class TupleRow;

/// --- ScalarExpr overview
///
/// ScalarExpr is an expression which returns a value for each input tuple row.
/// Examples include built-in functions such as abs(), UDF, case-expr and literal
/// such as a string "foobar". It's a subclass of Expr which represents an expression
/// as a tree.
///
/// --- Implementation:
///
/// ScalarExpr implements compute function, which given a row, performs the computation
/// of an expr and produces scalar result. This function evaluates the necessary child
/// arguments by calling their compute functions, then performs whatever computation is
/// necessary on the arguments to generate the result. These compute functions have
/// signature Get*Val(ScalarExprEvaluator*, const TupleRow*). One is implemented for each
/// possible return type it supports (e.g. GetBooleanVal(), GetStringVal(), etc). The
/// return type is a subclass of AnyVal (e.g. StringVal). One or more of these compute
/// functions must be overridden by subclasses of ScalarExpr.
///
/// ScalarExpr contains query compile-time information about an expression (e.g.
/// sub-expressions implicitly encoded in the tree structure) and the LLVM IR compute
/// functions. ScalarExprEvaluator is the interface for evaluating a scalar expression
/// against an input TupleRow.
///
/// ScalarExpr's compute functions are codegend to replace calls to the generic compute
/// function of child expressions with the exact compute functions based on the return
/// types of the child expressions known at runtime. Subclasses should override
/// GetCodegendComputeFn() to either generate custom IR compute functions using IRBuilder,
/// which inline calls to child expressions' compute functions, or simply call
/// GetCodegendComputeFnWrapper() to generate a wrapper function to call the interpreted
/// compute function. Note that we do not need a separate GetCodegendComputeFn() for each
/// type.
///
/// TODO: Fix subclasses which call GetCodegendComputeFnWrapper() to not call interpreted
/// functions.
///
class ScalarExpr : public Expr {
 public:
  /// Create a new ScalarExpr based on thrift Expr 'texpr'. The newly created ScalarExpr
  /// is stored in ObjectPool 'pool' and returned in 'expr' on success. 'row_desc' is the
  /// tuple row descriptor of the input tuple row. On failure, 'expr' is set to NULL and
  /// the expr tree (if created) will be closed. Error status will be returned too.
  static Status Create(const TExpr& texpr, const RowDescriptor& row_desc,
      RuntimeState* state, ObjectPool* pool, ScalarExpr** expr) WARN_UNUSED_RESULT;

  /// Create a new ScalarExpr based on thrift Expr 'texpr'. The newly created ScalarExpr
  /// is stored in ObjectPool 'state->obj_pool()' and returned in 'expr'. 'row_desc' is
  /// the tuple row descriptor of the input tuple row. Returns error status on failure.
  static Status Create(const TExpr& texpr, const RowDescriptor& row_desc,
      RuntimeState* state, ScalarExpr** expr) WARN_UNUSED_RESULT;

  /// Convenience functions creating multiple ScalarExpr.
  static Status Create(const std::vector<TExpr>& texprs, const RowDescriptor& row_desc,
      RuntimeState* state, ObjectPool* pool, std::vector<ScalarExpr*>* exprs)
      WARN_UNUSED_RESULT;

  /// Convenience functions creating multiple ScalarExpr.
  static Status Create(const std::vector<TExpr>& texprs, const RowDescriptor& row_desc,
      RuntimeState* state, std::vector<ScalarExpr*>* exprs) WARN_UNUSED_RESULT;

  /// Returns true if this expression is a SlotRef. Overridden by SlotRef.
  virtual bool IsSlotRef() const { return false; }

  /// Returns true if this is a literal expression. Overridden by Literal.
  virtual bool IsLiteral() const { return false; }

  /// Returns true if this expr uses a FunctionContext to track its runtime state.
  /// Overridden by exprs which use FunctionContext.
  virtual bool HasFnCtx() const { return false; }

  /// Returns true if this expr should be treated as a constant expression.
  bool is_constant() const { return is_constant_; }

  /// Returns the number of SlotRef nodes in the expr tree. If 'slot_ids' is non-null,
  /// add the slot ids to it. Overridden by SlotRef.
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids = nullptr) const;

  /// Returns an llvm::Function* with signature:
  /// <subclass of AnyVal> ComputeFn(ScalarExprEvaluator*, const TupleRow*)
  //
  /// The function should evaluate this expr over 'row' and return the result as the
  /// appropriate type of AnyVal. Returns error status on failure.
  virtual Status GetCodegendComputeFn(
      LlvmCodeGen* codegen, llvm::Function** fn) WARN_UNUSED_RESULT = 0;

  /// Simple debug string that provides no expr subclass-specific information
  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<ScalarExpr*>& exprs);
  std::string DebugString(const std::string& expr_name) const;

  /// Computes a memory efficient layout for storing the results of evaluating 'exprs'.
  /// The results are assumed to be void* slot types (vs AnyVal types). Varlen data is
  /// not included (e.g. there will be space for a StringValue, but not the data
  /// referenced by it).
  ///
  /// Returns the number of bytes necessary to store all the results and offsets
  /// where the result for each expr should be stored.
  ///
  /// Variable length types are guaranteed to be at the end and 'var_result_begin'
  /// will be set the beginning byte offset where variable length results begin.
  /// 'var_result_begin' will be set to -1 if there are no variable len types.
  static int ComputeResultsLayout(const vector<ScalarExpr*>& exprs, vector<int>* offsets,
      int* var_result_begin);

  /// Releases cache entries to libCache for all nodes in the ScalarExpr tree.
  virtual void Close();

  /// Convenience functions for closing a list of ScalarExpr.
  static void Close(const std::vector<ScalarExpr*>& exprs);

  static const char* LLVM_CLASS_NAME;

 protected:
  friend class Expr;
  friend class AggFn;
  friend class AggFnEvaluator;
  friend class AndPredicate;
  friend class CaseExpr;
  friend class CoalesceExpr;
  friend class ConditionalFunctions;
  friend class CompoundPredicate;
  friend class DecimalFunctions;
  friend class DecimalOperators;
  friend class HiveUdfCall;
  friend class IfExpr;
  friend class InPredicate;
  friend class IsNotEmptyPredicate;
  friend class IsNullExpr;
  friend class KuduPartitionExpr;
  friend class Literal;
  friend class NullLiteral;
  friend class OrPredicate;
  friend class Predicate;
  friend class ScalarExprEvaluator;
  friend class ScalarFnCall;

  /// For BE tests
  friend class ExprTest;
  friend class ExprCodegenTest;
  friend class HashTableTest;
  friend class OldHashTableTest;

  /// Cached LLVM IR for the compute function. Set this in GetCodegendComputeFn().
  llvm::Function* ir_compute_fn_ = nullptr;

  /// Assigns indices into the FunctionContext vector 'fn_ctxs_' in an evaluator to
  /// nodes which need FunctionContext in the tree. 'next_fn_ctx_idx' is the index
  /// of the next available entry in the vector. It's updated as this function is
  /// called recursively down the tree.
  void AssignFnCtxIdx(int* next_fn_ctx_idx);

  int fn_ctx_idx() const { return fn_ctx_idx_; }

  /// Creates a single ScalarExpr node based on 'texpr_node' and returns it
  /// in 'expr'. Return error status on failure.
  static Status CreateNode(const TExprNode& texpr_node, ObjectPool* pool,
      ScalarExpr** expr) WARN_UNUSED_RESULT;

  ScalarExpr(const ColumnType& type, bool is_constant);
  ScalarExpr(const TExprNode& node);

  /// Virtual compute functions for each return type. Each subclass should override
  /// the functions for the return type(s) it supports. For example, a boolean function
  /// will only override GetBooleanVal(). Some Exprs, like Literal, have many possible
  /// return types and will override multiple Get*Val() functions. These functions should
  /// be called by other ScalarExpr and ScalarExprEvaluator only.
  virtual BooleanVal GetBooleanVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual TinyIntVal GetTinyIntVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual SmallIntVal GetSmallIntVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual IntVal GetIntVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual BigIntVal GetBigIntVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual FloatVal GetFloatVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual DoubleVal GetDoubleVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual StringVal GetStringVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual CollectionVal GetCollectionVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual TimestampVal GetTimestampVal(ScalarExprEvaluator*, const TupleRow*) const;
  virtual DecimalVal GetDecimalVal(ScalarExprEvaluator*, const TupleRow*) const;

  /// Initializes all nodes in the expr tree. Subclasses overriding this function should
  /// call ScalarExpr::Init() to recursively call Init() on the expr tree.
  virtual Status Init(const RowDescriptor& row_desc, RuntimeState* state)
      WARN_UNUSED_RESULT;

  /// Initializes 'eval' for execution. If scope if FRAGMENT_LOCAL, both
  /// fragment-local and thread-local states should be initialized. If scope is
  /// THREAD_LOCAL, only thread-local states should be initialized. THREAD_LOCAL
  /// scope is used for cloned evaluator.
  ///
  /// Subclasses overriding this function should call ScalarExpr::OpenEvaluator() to
  /// recursively call OpenEvaluator() on all nodes in the ScalarExpr tree.
  virtual Status OpenEvaluator(FunctionContext::FunctionStateScope scope,
      RuntimeState* state, ScalarExprEvaluator* eval) const
      WARN_UNUSED_RESULT;

  /// Free resources held by the 'eval' allocated during OpenEvaluator().
  /// If scope is FRAGMENT_LOCAL, both fragment-local and thread-local states should be
  /// torn down. If scope is THREAD_LOCAL, only thread-local state should be torn down.
  ///
  /// Subclasses overriding this function should call ScalarExpr::CloseEvaluator() to
  /// recursively call CloseEvaluator() on all nodes in the ScalarExpr tree.
  virtual void CloseEvaluator(FunctionContext::FunctionStateScope scope,
      RuntimeState* state, ScalarExprEvaluator* eval) const;

  /// Computes the size of the varargs buffer in bytes (0 bytes if no varargs).
  /// Overridden by ScalarFnCall.
  virtual int ComputeVarArgsBufferSize() const { return 0; }

  /// Helper function to create an empty llvm::Function* with the signature:
  /// *Val name(ScalarExprEvaluator*, TupleRow*);
  ///
  /// 'name' is the name of the returned llvm::Function*. The arguments to the IR function
  /// are returned in 'args'. The return type is determined by the return type of the expr
  /// tree.
  llvm::Function* CreateIrFunctionPrototype(const std::string& name, LlvmCodeGen* codegen,
      llvm::Value* (*args)[2]);

  /// Generates an IR compute function that calls the interpreted compute function.
  /// It doesn't provide any performance benefit over the interpreted path. This is
  /// useful for builtins (e.g. && and || operators) and UDF which don't generate
  /// custom IR code but are part of a larger expr tree. The IR compute function of
  /// the larger expr tree may still benefit from custom IR and inlining of other
  /// sub-expressions.
  ///
  /// TODO: this should be removed in the long run and replaced with cross-compilation
  /// together with constant propagation and loop unrolling.
  Status GetCodegendComputeFnWrapper(LlvmCodeGen* codegen, llvm::Function** fn)
      WARN_UNUSED_RESULT;

  /// Helper function for GetCodegendComputeFnWrapper(). Returns the cross-compiled IR
  /// function of the static Get*Val wrapper function for return type 'type'.
  llvm::Function* GetStaticGetValWrapper(ColumnType type, LlvmCodeGen* codegen);

 private:
  /// 'fn_ctx_idx_' is the index into the FunctionContext vector in ScalarExprEvaluator
  /// for storing FunctionContext needed to evaluate this ScalarExprNode. It's -1 if this
  /// ScalarExpr doesn't need a FunctionContext. The FunctionContext is managed by the
  /// evaluator and initialized by calling ScalarExpr::OpenEvaluator().
  int fn_ctx_idx_ = -1;

  /// [fn_ctx_idx_start_, fn_ctx_idx_end_) defines the range in FunctionContext vector
  /// in ScalarExpeEvaluator for the expression subtree rooted at this ScalarExpr node.
  int fn_ctx_idx_start_ = 0;
  int fn_ctx_idx_end_ = 0;

  /// True if this expr should be treated as a constant expression. True if either:
  /// * This expr was sent from the frontend and Expr.isConstant() was true.
  /// * This expr is a constant literal created in the backend.
  const bool is_constant_;

  /// Static wrappers which call the compute function of the given ScalarExpr, passing
  /// it the ScalarExprEvaluator and TupleRow. These are cross-compiled and called by
  /// the IR wrapper functions generated by GetCodegendComputeFnWrapper().
  static BooleanVal GetBooleanVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static TinyIntVal GetTinyIntVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static SmallIntVal GetSmallIntVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static IntVal GetIntVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static BigIntVal GetBigIntVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static FloatVal GetFloatVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static DoubleVal GetDoubleVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static StringVal GetStringVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static TimestampVal GetTimestampVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static DecimalVal GetDecimalVal(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
};

}

#endif
