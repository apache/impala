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


/// --- Terminology:
//
/// Compute function: The function that, given a row, performs the computation of an expr
/// and produces a scalar result. This function evaluates the necessary child arguments by
/// calling their compute functions, then performs whatever computation is necessary on
/// the arguments (e.g. calling a UDF with the child arguments). All compute functions
/// take arguments (ExprContext*, TupleRow*). The return type is a *Val (i.e. a subclass
/// of AnyVal). Thus, a single expression will implement a compute function for every
/// return type it supports.
///
/// UDX: user-defined X. E.g., user-defined function, user-defined aggregate. Something
/// that is written by an external user.
///
/// Scalar function call: An expr that returns a single scalar value and can be
/// implemented using the UDF interface. Note that this includes builtins, which although
/// not being user-defined still use the same interface as UDFs (i.e., they are
/// implemented as functions with signature "*Val (FunctionContext*, *Val, *Val...)").
///
/// Aggregate function call: a UDA or builtin aggregate function.
///
/// --- Expr overview:
///
/// The Expr superclass defines a virtual Get*Val() compute function for each possible
/// return type (GetBooleanVal(), GetStringVal(), etc). Expr subclasses implement the
/// Get*Val() functions associated with their possible return types; for many Exprs this
/// will be a single function. These functions are generally cross-compiled to both native
/// and IR libraries. In the interpreted path, the native compute functions are run as-is.
///
/// For the codegen path, Expr defines a virtual method GetCodegendComputeFn() that
/// returns the Function* of the expr's compute function. Note that we do not need a
/// separate GetCodegendComputeFn() for each type.
///
/// Only short-circuited operators (e.g. &&, ||) and other special functions like literals
/// must implement custom Get*Val() compute functions. Scalar function calls use the
/// generic compute functions implemented by ScalarFnCall(). For cross-compiled compute
/// functions, GetCodegendComputeFn() can use ReplaceChildCallsComputeFn(), which takes a
/// cross-compiled IR Get*Val() function, pulls out any calls to the children's Get*Val()
/// functions (which we identify via the Get*Val() static wrappers), and replaces them
/// with the codegen'd version of that function. This allows us to write a single function
/// for both the interpreted and codegen paths.
///
/// Only short-circuited operators (e.g. &&, ||) and other special functions like
/// literals must implement custom Get*Val() compute functions. Scalar function calls
/// use the generic compute functions implemented by ScalarFnCall(). For cross-compiled
/// compute functions, GetCodegendComputeFn() can use ReplaceChildCallsComputeFn(), which
/// takes a cross-compiled IR Get*Val() function, pulls out any calls to the children's
/// Get*Val() functions (which we identify via the Get*Val() static wrappers), and
/// replaces them with the codegen'd version of that function. This allows us to write a
/// single function for both the interpreted and codegen paths.
///
/// --- Expr users (e.g. exec nodes):
///
/// A typical usage pattern will look something like:
/// 1. Expr::CreateExprTrees()
/// 2. Expr::Prepare()
/// 3. Expr::Open()
/// 4. Expr::CloneIfNotExists() [for multi-threaded execution]
/// 5. Evaluate exprs via Get*Val() calls
/// 6. Expr::Close() [called once per ExprContext, including clones]
///
/// Expr users should use the static Get*Val() wrapper functions to evaluate exprs,
/// cross-compile the resulting function, and use ReplaceGetValCalls() to create the
/// codegen'd function. See the comments on these functions for more details. This is a
/// similar pattern to that used by the cross-compiled compute functions.
///
/// TODO:
/// - Fix codegen compile time
/// - Fix perf regressions via extra optimization passes + patching LLVM

#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <string>
#include <vector>

#include "common/status.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/descriptors.h"
#include "runtime/decimal-value.h"
#include "runtime/lib-cache.h"
#include "runtime/raw-value.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "udf/udf.h"
#include "udf/udf-internal.h" // for CollectionVal

using namespace impala_udf;

namespace llvm {
  class BasicBlock;
  class Function;
  class Type;
  class Value;
};

namespace impala {

class Expr;
class IsNullExpr;
class LlvmCodeGen;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TColumnValue;
class TExpr;
class TExprNode;

/// This is the superclass of all expr evaluation nodes.
class Expr {
 public:
  virtual ~Expr();

  /// Virtual compute functions for each *Val type. Each Expr subclass should implement
  /// the functions for the return type(s) it supports. For example, a boolean function
  /// will only implement GetBooleanVal(). Some Exprs, like Literal, have many possible
  /// return types and will implement multiple Get*Val() functions.
  virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow*);
  virtual TinyIntVal GetTinyIntVal(ExprContext* context, TupleRow*);
  virtual SmallIntVal GetSmallIntVal(ExprContext* context, TupleRow*);
  virtual IntVal GetIntVal(ExprContext* context, TupleRow*);
  virtual BigIntVal GetBigIntVal(ExprContext* context, TupleRow*);
  virtual FloatVal GetFloatVal(ExprContext* context, TupleRow*);
  virtual DoubleVal GetDoubleVal(ExprContext* context, TupleRow*);
  virtual StringVal GetStringVal(ExprContext* context, TupleRow*);
  virtual CollectionVal GetCollectionVal(ExprContext* context, TupleRow*);
  virtual TimestampVal GetTimestampVal(ExprContext* context, TupleRow*);
  virtual DecimalVal GetDecimalVal(ExprContext* context, TupleRow*);

  /// Get the number of digits after the decimal that should be displayed for this value.
  /// Returns -1 if no scale has been specified (currently the scale is only set for
  /// doubles set by RoundUpTo). GetValue() must have already been called.
  /// TODO: is this still necessary?
  int output_scale() const { return output_scale_; }

  void AddChild(Expr* expr) { children_.push_back(expr); }
  Expr* GetChild(int i) const { return children_[i]; }
  int GetNumChildren() const { return children_.size(); }

  const ColumnType& type() const { return type_; }
  bool is_slotref() const { return is_slotref_; }

  const std::vector<Expr*>& children() const { return children_; }

  /// Returns an error status if the function context associated with the
  /// expr has an error set.
  Status GetFnContextError(ExprContext* ctx);

  /// Returns true if GetValue(NULL) can be called on this expr and always returns the same
  /// result (e.g., exprs that don't contain slotrefs). The default implementation returns
  /// true if all children are constant.
  virtual bool IsConstant() const;

  /// Returns the slots that are referenced by this expr tree in 'slot_ids'.
  /// Returns the number of slots added to the vector
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const;

  /// Create expression tree from the list of nodes contained in texpr within 'pool'.
  /// Returns the root of expression tree in 'expr' and the corresponding ExprContext in
  /// 'ctx'.
  static Status CreateExprTree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx);

  /// Creates vector of ExprContexts containing exprs from the given vector of
  /// TExprs within 'pool'.  Returns an error if any of the individual conversions caused
  /// an error, otherwise OK.
  static Status CreateExprTrees(ObjectPool* pool, const std::vector<TExpr>& texprs,
      std::vector<ExprContext*>* ctxs);

  /// Convenience function for preparing multiple expr trees.
  /// Allocations from 'ctxs' will be counted against 'tracker'.
  static Status Prepare(const std::vector<ExprContext*>& ctxs, RuntimeState* state,
                        const RowDescriptor& row_desc, MemTracker* tracker);

  /// Convenience function for opening multiple expr trees.
  static Status Open(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

  /// Clones each ExprContext for multiple expr trees. 'new_ctxs' must be non-NULL.
  /// Idempotent: if '*new_ctxs' is empty, a clone of each context in 'ctxs' will be added
  /// to it, and if non-empty, it is assumed CloneIfNotExists() was already called and the
  /// call is a no-op. The new ExprContexts are created in state->obj_pool().
  static Status CloneIfNotExists(const std::vector<ExprContext*>& ctxs,
      RuntimeState* state, std::vector<ExprContext*>* new_ctxs);

  /// Convenience function for closing multiple expr trees.
  static void Close(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

  /// Create a new literal expr of 'type' with initial 'data'.
  /// data should match the ColumnType (i.e. type == TYPE_INT, data is a int*)
  /// The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, const ColumnType& type, void* data);

  /// Create a new literal expr of 'type' by parsing the string.
  /// NULL will be returned if the string and type are not compatible.
  /// The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, const ColumnType& type,
      const std::string&);

  /// Computes a memory efficient layout for storing the results of evaluating
  /// 'exprs'. The results are assumed to be void* slot types (vs AnyVal types). Varlen
  /// data is not included (e.g. there will be space for a StringValue, but not the data
  /// referenced by it).
  ///
  /// Returns the number of bytes necessary to store all the results and offsets
  /// where the result for each expr should be stored.
  ///
  /// Variable length types are guaranteed to be at the end and 'var_result_begin'
  /// will be set the beginning byte offset where variable length results begin.
  /// 'var_result_begin' will be set to -1 if there are no variable len types.
  static int ComputeResultsLayout(const std::vector<Expr*>& exprs,
      std::vector<int>* offsets, int* var_result_begin);
  static int ComputeResultsLayout(const std::vector<ExprContext*>& ctxs,
      std::vector<int>* offsets, int* var_result_begin);

  /// Returns an llvm::Function* with signature:
  /// <subclass of AnyVal> ComputeFn(ExprContext* context, TupleRow* row)
  //
  /// The function should evaluate this expr over 'row' and return the result as the
  /// appropriate type of AnyVal.
  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) = 0;

  /// If this expr is constant, evaluates the expr with no input row argument and returns
  /// the output. Returns NULL if the argument is not constant. The returned AnyVal* is
  /// owned by this expr. This should only be called after Open() has been called on this
  /// expr.
  virtual AnyVal* GetConstVal(ExprContext* context);

  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<Expr*>& exprs);
  static std::string DebugString(const std::vector<ExprContext*>& ctxs);

  /// The builtin functions are not called from anywhere in the code and the
  /// symbols are therefore not included in the binary. We call these functions
  /// by using dlsym. The compiler must think this function is callable to
  /// not strip these symbols.
  static void InitBuiltinsDummy();

  // Any additions to this enum must be reflected in both GetConstant() and
  // GetIrConstant().
  enum ExprConstant {
    RETURN_TYPE_SIZE, // int
    ARG_TYPE_SIZE // int[]
  };

  // Static function for obtaining a runtime constant.  Expr compute functions and
  // builtins implementing the UDF interface should use this function, rather than
  // accessing runtime constants directly, so any constants can be inlined via
  // InlineConstants() in the codegen path. In the interpreted path, this function will
  // work as-is.
  //
  // 'c' determines which constant is returned. 'T' must match the type of the constant,
  // which is annotated in the ExprConstant enum above. If the constant is an array, 'i'
  // must be specified and indicates which element to return. 'T' is the element type in
  // this case. 'i' must always be an immediate integer value so InlineConstants() can
  // resolve the index, e.g., it cannot be a variable or an expression like "1 + 1".  For
  // example, if 'c' = ARG_TYPE_SIZE, then 'T' = int and 0 <= i < children_.size().
  //
  // TODO: implement a loop unroller (or use LLVM's) so we can use GetConstant() in loops
  template<typename T> static T GetConstant(
      const FunctionContext& ctx, ExprConstant c, int i = -1);

  static const char* LLVM_CLASS_NAME;

  // Prefix of Expr::GetConstant() symbols, regardless of template specialization
  static const char* GET_CONSTANT_SYMBOL_PREFIX;

 protected:
  friend class AggFnEvaluator;
  friend class CastExpr;
  friend class ComputeFunctions;
  friend class DecimalFunctions;
  friend class DecimalLliteral;
  friend class DecimalOperators;
  friend class MathFunctions;
  friend class StringFunctions;
  friend class TimestampFunctions;
  friend class ConditionalFunctions;
  friend class UtilityFunctions;
  friend class CaseExpr;
  friend class InPredicate;
  friend class FunctionCall;
  friend class ScalarFnCall;

  Expr(const ColumnType& type, bool is_slotref = false);
  Expr(const TExprNode& node, bool is_slotref = false);

  /// Initializes this expr instance for execution. This does not include initializing
  /// state in the ExprContext; 'context' should only be used to register a
  /// FunctionContext via RegisterFunctionContext(). Any IR functions must be generated
  /// here.
  ///
  /// Subclasses overriding this function should call Expr::Prepare() to recursively call
  /// Prepare() on the expr tree.
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                         ExprContext* context);

  /// Initializes 'context' for execution. If scope if FRAGMENT_LOCAL, both fragment- and
  /// thread-local state should be initialized. Otherwise, if scope is THREAD_LOCAL, only
  /// thread-local state should be initialized.
  //
  /// Subclasses overriding this function should call Expr::Open() to recursively call
  /// Open() on the expr tree.
  virtual Status Open(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);

  /// Subclasses overriding this function should call Expr::Close().
  //
  /// If scope if FRAGMENT_LOCAL, both fragment- and thread-local state should be torn
  /// down. Otherwise, if scope is THREAD_LOCAL, only thread-local state should be torn
  /// down.
  virtual void Close(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);

  /// Cache entry for the library implementing this function.
  LibCache::LibCacheEntry* cache_entry_;

  /// Function description.
  TFunction fn_;

  /// recognize if this node is a slotref in order to speed up GetValue()
  const bool is_slotref_;
  /// analysis is done, types are fixed at this point
  const ColumnType type_;
  std::vector<Expr*> children_;
  int output_scale_;

  /// Index to pass to ExprContext::fn_context() to retrieve this expr's FunctionContext.
  /// Set in RegisterFunctionContext(). -1 if this expr does not need a FunctionContext and
  /// doesn't call RegisterFunctionContext().
  int fn_context_index_;

  /// Cached codegened compute function. Exprs should set this in GetCodegendComputeFn().
  llvm::Function* ir_compute_fn_;

  /// If this expr is constant, this will store and cache the value generated by
  /// GetConstVal().
  boost::scoped_ptr<AnyVal> constant_val_;

  /// Helper function that calls ctx->Register(), sets fn_context_index_, and returns the
  /// registered FunctionContext.
  FunctionContext* RegisterFunctionContext(
      ExprContext* ctx, RuntimeState* state, int varargs_buffer_size = 0);

  /// Helper function to create an empty Function* with the appropriate signature to be
  /// returned by GetCodegendComputeFn(). 'name' is the name of the returned Function*.
  /// The arguments to the function are returned in 'args'.
  llvm::Function* CreateIrFunctionPrototype(LlvmCodeGen* codegen, const std::string& name,
                                            llvm::Value* (*args)[2]);

  /// Generates an IR compute function that calls the appropriate interpreted Get*Val()
  /// compute function.
  //
  /// This is useful for builtins that can't be implemented with the UDF interface
  /// (e.g. functions that need short-circuiting) and that don't have custom codegen
  /// functions that use the IRBuilder. It doesn't provide any performance benefit over
  /// the interpreted path.
  /// TODO: this should be replaced with fancier xcompiling infrastructure
  Status GetCodegendComputeFnWrapper(RuntimeState* state, llvm::Function** fn);

  /// Returns the IR version of the static Get*Val() wrapper function corresponding to
  /// 'type'. This is used for calling interpreted Get*Val() functions from codegen'd
  /// functions (e.g. in ScalarFnCall() when codegen is disabled).
  llvm::Function* GetStaticGetValWrapper(ColumnType type, LlvmCodeGen* codegen);

  /// Finds all calls to Expr::GetConstant() in 'fn' and replaces them with the requested
  /// runtime constant. Returns the number of calls replaced. This should be used in
  /// GetCodegendComputeFn().
  int InlineConstants(LlvmCodeGen* codegen, llvm::Function* fn);

  /// Simple debug string that provides no expr subclass-specific information
  std::string DebugString(const std::string& expr_name) const {
    std::stringstream out;
    out << expr_name << "(" << Expr::DebugString() << ")";
    return out.str();
  }

 private:
  friend class ExprContext;
  friend class ExprTest;
  friend class ExprCodegenTest;

  /// Create a new Expr based on texpr_node.node_type within 'pool'.
  static Status CreateExpr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr);

  /// Creates an expr tree for the node rooted at 'node_idx' via depth-first traversal.
  /// parameters
  ///   nodes: vector of thrift expression nodes to be translated
  ///   parent: parent of node at node_idx (or NULL for node_idx == 0)
  ///   node_idx:
  ///     in: root of TExprNode tree
  ///     out: next node in 'nodes' that isn't part of tree
  ///   root_expr: out: root of constructed expr tree
  ///   ctx: out: context of constructed expr tree
  /// return
  ///   status.ok() if successful
  ///   !status.ok() if tree is inconsistent or corrupt
  static Status CreateTreeFromThrift(ObjectPool* pool,
      const std::vector<TExprNode>& nodes, Expr* parent, int* node_idx,
      Expr** root_expr, ExprContext** ctx);

  /// Static wrappers around the virtual Get*Val() functions. Calls the appropriate
  /// Get*Val() function on expr, passing it the context and row arguments.
  //
  /// These are used to call Get*Val() functions from generated functions, since I don't
  /// know how to call virtual functions directly. GetStaticGetValWrapper() returns the
  /// IR function of the appropriate wrapper function.
  static BooleanVal GetBooleanVal(Expr* expr, ExprContext* context, TupleRow* row);
  static TinyIntVal GetTinyIntVal(Expr* expr, ExprContext* context, TupleRow* row);
  static SmallIntVal GetSmallIntVal(Expr* expr, ExprContext* context, TupleRow* row);
  static IntVal GetIntVal(Expr* expr, ExprContext* context, TupleRow* row);
  static BigIntVal GetBigIntVal(Expr* expr, ExprContext* context, TupleRow* row);
  static FloatVal GetFloatVal(Expr* expr, ExprContext* context, TupleRow* row);
  static DoubleVal GetDoubleVal(Expr* expr, ExprContext* context, TupleRow* row);
  static StringVal GetStringVal(Expr* expr, ExprContext* context, TupleRow* row);
  static TimestampVal GetTimestampVal(Expr* expr, ExprContext* context, TupleRow* row);
  static DecimalVal GetDecimalVal(Expr* expr, ExprContext* context, TupleRow* row);

  // Helper function for InlineConstants(). Returns the IR version of what GetConstant()
  // would return.
  llvm::Value* GetIrConstant(LlvmCodeGen* codegen, ExprConstant c, int i);
};

}

#endif
