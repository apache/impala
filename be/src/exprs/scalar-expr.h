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

#include "codegen/codegen-fn-ptr.h"
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
using impala_udf::DateVal;
using impala_udf::CollectionVal;
using impala_udf::StructVal;

class FragmentState;
struct LibCacheEntry;
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

/// Describes the memory efficient layout for storing the results of evaluating a list
/// of scalar expressions.
/// The constructor computes a memory efficient layout for storing the results of
/// evaluating 'exprs'. The results are assumed to be void* slot types (vs AnyVal types).
/// Varlen data is not included (e.g. there will be space for a StringValue, but not the
/// data referenced by it). Variable length types are guaranteed to be at the end.
struct ScalarExprsResultsRowLayout {
  ScalarExprsResultsRowLayout() = delete;
  ScalarExprsResultsRowLayout(const vector<ScalarExpr*>& exprs);

  /// The number of bytes necessary to store all the results.
  int expr_values_bytes_per_row;

  /// Maps from expression index to the byte offset into the row of expression values.
  std::vector<int> expr_values_offsets;

  /// Byte offset from where the variable length results for a row begins. If -1, there
  /// are no variable length slots.
  int var_results_begin_offset;
};

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
/// return type is a subclass of AnyVal (e.g. StringVal). Get*Val() dispatches to either
/// a codegen'd function pointer or to an interpreted implementation Get*ValInterpreted()
/// These interpreted functions must be overridden by subclasses of ScalarExpr for every
/// type that they may return.
///
/// ScalarExpr contains query compile-time information about an expression (e.g.
/// sub-expressions implicitly encoded in the tree structure) and the LLVM IR compute
/// functions. ScalarExprEvaluator is the interface for evaluating a scalar expression
/// against an input TupleRow.
///
/// ScalarExpr's compute functions are codegend to replace calls to the generic compute
/// function of child expressions with the exact compute functions based on the return
/// types of the child expressions known at runtime. Subclasses should override
/// GetCodegendComputeFnImpl() to generate custom IR compute functions using IRBuilder,
/// which inlines calls to child expressions' compute functions. Note that we do not need
/// a separate GetCodegendComputeFn() for each type.
///
/// The two main usage patterns for ScalarExpr are:
/// * The codegen'd expressions are called from other codegen'd functions, e.g. from a
///   codegen'd join implementation
/// * Get*Val() is called on the root of each expression subtree by interpreted code
///   (e.g. an operator which doesn't support codegen yet).
/// We can optimize for the second usage pattern by filling in the codegen'd function
/// pointer (codegend_compute_fn_) in root of each ScalarExpr tree. Individual callsites
/// can disable this optimisation if it's not needed. Expr subtrees can be evaluated
/// (e.g. by ScalarExprEvaluator::GetConstValue()) but may fail back to a slower
/// interpreted implementation.
///
class ScalarExpr : public Expr {
 public:
  /// Create a new ScalarExpr based on thrift Expr 'texpr'. The newly created ScalarExpr
  /// is stored in ObjectPool 'pool' and returned in 'expr' on success. 'row_desc' is the
  /// tuple row descriptor of the input tuple row. On failure, 'expr' is set to NULL and
  /// the expr tree (if created) will be closed. Error status will be returned too.
  static Status Create(const TExpr& texpr, const RowDescriptor& row_desc,
      FragmentState* state, ObjectPool* pool, ScalarExpr** expr) WARN_UNUSED_RESULT;

  /// Create a new ScalarExpr based on thrift Expr 'texpr'. The newly created ScalarExpr
  /// is stored in ObjectPool 'state->obj_pool()' and returned in 'expr'. 'row_desc' is
  /// the tuple row descriptor of the input tuple row. Returns error status on failure.
  static Status Create(const TExpr& texpr, const RowDescriptor& row_desc,
      FragmentState* state, ScalarExpr** expr) WARN_UNUSED_RESULT;

  /// Convenience functions creating multiple ScalarExpr.
  static Status Create(const std::vector<TExpr>& texprs, const RowDescriptor& row_desc,
      FragmentState* state, ObjectPool* pool,
      std::vector<ScalarExpr*>* exprs) WARN_UNUSED_RESULT;

  /// Convenience functions creating multiple ScalarExpr.
  static Status Create(const std::vector<TExpr>& texprs, const RowDescriptor& row_desc,
      FragmentState* state, std::vector<ScalarExpr*>* exprs) WARN_UNUSED_RESULT;

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
  ///
  /// The function should evaluate this expr over 'row' and return the result as the
  /// appropriate type of AnyVal. If 'is_codegen_entry_point' is true, then the
  /// appropriate setup is performed to make the Get*Val() method on this expr call into
  /// a codegen'd implementation of the expression tree. If this expr is only called from
  /// codegen'd code via 'fn', 'is_codegen_entry_point' should be false to reduce the
  /// number of entry points into codegen'd code and therefore the overhead of
  /// compilation. Returns error status on failure.
  ///
  /// This function is invoked either by other codegen functions (e.g. the codegen code
  /// of a join) or by RuntimeState::CodegenScalarExprs() which is called from
  /// FragmentInstanceState::Open() before LLVM compilation. These two call sites
  /// correspond to the two usage patterns in the class comment.
  Status GetCodegendComputeFn(LlvmCodeGen* codegen, bool is_codegen_entry_point,
      llvm::Function** fn);

  /// Simple debug string that provides no expr subclass-specific information
  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<ScalarExpr*>& exprs);
  std::string DebugString(const std::string& expr_name) const;

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
  friend class CastFormatExpr;
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
  friend class SlotRef;

  // The ORC scanner builds search arguments based on the values it gets from Literal.
  friend class HdfsOrcScanner;

  /// For BE tests
  friend class ExprTest;
  friend class ExprCodegenTest;
  friend class HashTableTest;

  /// Assigns indices into the FunctionContext vector 'fn_ctxs_' in an evaluator to
  /// nodes which need FunctionContext in the tree. 'next_fn_ctx_idx' is the index
  /// of the next available entry in the vector. It's updated as this function is
  /// called recursively down the tree.
  virtual void AssignFnCtxIdx(int* next_fn_ctx_idx);

  int fn_ctx_idx() const { return fn_ctx_idx_; }

  /// Creates a single ScalarExpr node based on 'texpr_node' and returns it
  /// in 'expr'. Return error status on failure.
  static Status CreateNode(const TExprNode& texpr_node, ObjectPool* pool,
      ScalarExpr** expr) WARN_UNUSED_RESULT;

  ScalarExpr(const ColumnType& type, bool is_constant, bool is_codegen_disabled = false);
  ScalarExpr(const TExprNode& node);

  /// Implementation of GetCodegendComputeFn() to be overridden by each subclass of
  /// ScalarExpr.
  virtual Status GetCodegendComputeFnImpl(
      LlvmCodeGen* codegen, llvm::Function** fn) WARN_UNUSED_RESULT = 0;

  /// Entry points for ScalarExprEvaluator when interpreting this ScalarExpr. These
  /// dispatch to the codegen'd function pointer if present, or otherwise use the
  /// Get*ValInterpreted() implementation.
  /// These functions should be called by other ScalarExprs and ScalarExprEvaluator only.
  BooleanVal GetBooleanVal(ScalarExprEvaluator*, const TupleRow*) const;
  TinyIntVal GetTinyIntVal(ScalarExprEvaluator*, const TupleRow*) const;
  SmallIntVal GetSmallIntVal(ScalarExprEvaluator*, const TupleRow*) const;
  IntVal GetIntVal(ScalarExprEvaluator*, const TupleRow*) const;
  BigIntVal GetBigIntVal(ScalarExprEvaluator*, const TupleRow*) const;
  FloatVal GetFloatVal(ScalarExprEvaluator*, const TupleRow*) const;
  DoubleVal GetDoubleVal(ScalarExprEvaluator*, const TupleRow*) const;
  StringVal GetStringVal(ScalarExprEvaluator*, const TupleRow*) const;
  CollectionVal GetCollectionVal(ScalarExprEvaluator*, const TupleRow*) const;
  StructVal GetStructVal(ScalarExprEvaluator*, const TupleRow*) const;
  TimestampVal GetTimestampVal(ScalarExprEvaluator*, const TupleRow*) const;
  DecimalVal GetDecimalVal(ScalarExprEvaluator*, const TupleRow*) const;
  DateVal GetDateVal(ScalarExprEvaluator*, const TupleRow*) const;

  /// Virtual compute functions for each return type. Each subclass should override
  /// the functions for the return type(s) it supports. For example, a boolean function
  /// will only override GetBooleanValInterpreted(). Some Exprs, like Literal, have many
  /// possible return types and will override multiple Get*ValInterpreted() functions.
  virtual BooleanVal GetBooleanValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;
  virtual TinyIntVal GetTinyIntValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;
  virtual SmallIntVal GetSmallIntValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;
  virtual IntVal GetIntValInterpreted(ScalarExprEvaluator*, const TupleRow*) const;
  virtual BigIntVal GetBigIntValInterpreted(ScalarExprEvaluator*, const TupleRow*) const;
  virtual FloatVal GetFloatValInterpreted(ScalarExprEvaluator*, const TupleRow*) const;
  virtual DoubleVal GetDoubleValInterpreted(ScalarExprEvaluator*, const TupleRow*) const;
  virtual StringVal GetStringValInterpreted(ScalarExprEvaluator*, const TupleRow*) const;
  virtual CollectionVal GetCollectionValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;
  virtual StructVal GetStructValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;
  virtual TimestampVal GetTimestampValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;
  virtual DecimalVal GetDecimalValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;
  virtual DateVal GetDateValInterpreted(ScalarExprEvaluator*, const TupleRow*) const;

  /// Initializes all nodes in the expr tree. Subclasses overriding this function must
  /// call ScalarExpr::Init(). If 'is_entry_point' is true, this indicates that Get*Val()
  /// may be called directly from interpreted code and that we should generate an entry
  /// point into the codegen'd code. Currently we assume all roots of ScalarExpr subtrees
  /// exprs are potential entry points.
  virtual Status Init(const RowDescriptor& row_desc, bool is_entry_point,
      FragmentState* state) WARN_UNUSED_RESULT;

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
 protected:

  /// Return true if we should codegen this expression node, based on query options
  /// and the properties of this ScalarExpr node.
  bool ShouldCodegen(const FragmentState* state) const;

  /// Return true if it is possible to evaluate this expression node without codegen.
  /// The vast majority of exprs support interpretation, so default to true. Scalars
  /// that are not always interpretable must override this function.
  virtual bool IsInterpretable() const { return true; }

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

  /// Cached LLVM IR for the compute function. Set in GetCodegendComputeFn().
  llvm::Function* ir_compute_fn_ = nullptr;

  /// Function pointer to the JIT'd function produced by GetCodegendComputeFn().
  /// Has signature *Val (ScalarExprEvaluator*, const TupleRow*), and calls the scalar
  /// function with signature like *Val (FunctionContext*, const *Val& arg1, ...)
  /// Non-NULL if this expr is codegen'd and the constructor of this Expr requested
  /// that this expr should be an entry point from interpreted to codegen'd code.
  /// (see class comment for explanation of usage patterns and motivation).
  /// The actual types of the functions differ so we use void* and reinterpret cast before
  /// calling the functions.
  CodegenFnPtr<void*> codegend_compute_fn_;

  /// True if 'codegend_compute_fn_' is registered with LlvmCodeGen as an entry point
  /// to codegen to fill in . If this is true, then 'codegend_compute_fn_' will be set
  /// to the JIT'd function produced by GetCodegendComputeFn() after LLVM compilation.
  /// Set in GetCodegendComputeFn().
  bool added_to_jit_ = false;

  /// True if codegen should be disabled for this scalar expression. Typical use case
  /// is const expressions in VALUES clause, which are evaluated only once.
  const bool is_codegen_disabled_;

  /// Static wrappers which call the compute function of the given ScalarExpr, passing
  /// it the ScalarExprEvaluator and TupleRow. These are cross-compiled and used by
  /// GetStaticGetValWrapper.
  static BooleanVal GetBooleanValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static TinyIntVal GetTinyIntValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static SmallIntVal GetSmallIntValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static IntVal GetIntValInterpreted(ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static BigIntVal GetBigIntValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static FloatVal GetFloatValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static DoubleVal GetDoubleValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static StringVal GetStringValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static TimestampVal GetTimestampValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static DecimalVal GetDecimalValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
  static DateVal GetDateValInterpreted(
      ScalarExpr*, ScalarExprEvaluator*, const TupleRow*);
};

// Helper to generate the declaration for an override of Get*ValInterpreted()
#define GET_VAL_INTERPRETED_OVERRIDE(val_type)                                       \
  virtual val_type Get##val_type##Interpreted(ScalarExprEvaluator*, const TupleRow*) \
      const override;

// Helper to stamp out Get*ValInterpreted() declarations for all scalar types.
#define GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES \
  GET_VAL_INTERPRETED_OVERRIDE(BooleanVal)                          \
  GET_VAL_INTERPRETED_OVERRIDE(TinyIntVal)                          \
  GET_VAL_INTERPRETED_OVERRIDE(SmallIntVal)                         \
  GET_VAL_INTERPRETED_OVERRIDE(IntVal)                              \
  GET_VAL_INTERPRETED_OVERRIDE(BigIntVal)                           \
  GET_VAL_INTERPRETED_OVERRIDE(FloatVal)                            \
  GET_VAL_INTERPRETED_OVERRIDE(DoubleVal)                           \
  GET_VAL_INTERPRETED_OVERRIDE(StringVal)                           \
  GET_VAL_INTERPRETED_OVERRIDE(TimestampVal)                        \
  GET_VAL_INTERPRETED_OVERRIDE(DecimalVal)                          \
  GET_VAL_INTERPRETED_OVERRIDE(DateVal)

} // namespace impala

#endif
