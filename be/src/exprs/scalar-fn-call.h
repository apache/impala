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


#ifndef IMPALA_EXPRS_SCALAR_FN_CALL_H_
#define IMPALA_EXPRS_SCALAR_FN_CALL_H_

#include <string>

#include "exprs/scalar-expr.h"
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
using impala_udf::DateVal;

class ScalarExprEvaluator;
class TExprNode;

/// Expr for evaluating a pre-compiled native or LLVM IR function that uses the UDF
/// interface (i.e. a scalar function). This class overrides GetCodegendComputeFnImpl() to
/// return a function that calls any child exprs and passes the results as arguments to
/// the specified scalar function.
///
/// If codegen is disabled, some native functions can be called without codegen, depending
/// on the native function's signature. However, since we can't write static code to call
/// every possible function signature, codegen may be required to generate the call to the
/// function even if codegen is disabled. Codegen will also be used for IR UDFs (note that
/// there is no way to specify both a native and IR library for a single UDF).
///
/// Scalar function call: An expr that returns a single scalar value and can be
/// implemented using the UDF interface. Note that this includes builtins, which although
/// not being user-defined still use the same interface as UDFs (i.e., they are
/// implemented as functions with signature "*Val (FunctionContext*, *Val, *Val...)").
///
/// TODO:
/// - Fix error reporting, e.g. reporting leaks
/// - Testing
///    - Test cancellation
///    - Type descs in UDA test harness
///    - Allow more functions to be NULL in UDA test harness
class ScalarFnCall : public ScalarExpr {
 public:
  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn)
      override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override;

 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  virtual bool HasFnCtx() const override { return true; }

  ScalarFnCall(const TExprNode& node);
  virtual Status Init(const RowDescriptor& row_desc, bool is_entry_point,
      FragmentState* state) override WARN_UNUSED_RESULT;
  virtual Status OpenEvaluator(FunctionContext::FunctionStateScope scope,
      RuntimeState* state, ScalarExprEvaluator* eval) const override WARN_UNUSED_RESULT;
  virtual void CloseEvaluator(FunctionContext::FunctionStateScope scope,
      RuntimeState* state, ScalarExprEvaluator* eval) const override;
  virtual int ComputeVarArgsBufferSize() const override;
  /// Not all scalars functions are interpretable - see class comment.
  virtual bool IsInterpretable() const override;

  GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES

 private:
  /// If this function has var args, children()[vararg_start_idx_] is the first vararg
  /// argument.
  /// If this function does not have varargs, it is set to -1.
  const int vararg_start_idx_;

  /// Vector of all non-constant children expressions that need to be evaluated for
  /// each input row. The first element of each pair is the child expression and the
  /// second element in the value it must be evaluated into.
  std::vector<std::pair<Expr*, impala_udf::AnyVal*>> non_constant_children_;

  /// The UDF's prepare function, if specified. This is initialized in Prepare() and
  /// called in Open() (since we may have needed to codegen the function if it's from an
  /// IR module).
  CodegenFnPtr<impala_udf::UdfPrepare> prepare_fn_;

  /// THe UDF's close function, if specified. This is initialized in Prepare() and called
  /// in Close().
  CodegenFnPtr<impala_udf::UdfClose> close_fn_;

  /// A pointer to the function implementation, used by the interpreted code path. Set in
  /// Init() for BUILTIN and NATIVE functions. Not set for IR UDFs.
  void* scalar_fn_;

  /// Returns the number of non-vararg arguments
  int NumFixedArgs() const {
    return vararg_start_idx_ >= 0 ? vararg_start_idx_ : children_.size();
  }

  virtual int NumVarArgs() const { return children_.size() - NumFixedArgs(); }

  const ColumnType& VarArgsType() const {
    DCHECK_GE(NumVarArgs(), 1);
    return children_.back()->type();
  }

  /// Loads the native or IR function 'symbol' from HDFS and puts the result in *fn.
  /// If the function is loaded from an IR module, it cannot be called until the module
  /// has been JIT'd (i.e. after GetCodegendComputeFnImpl() has been called).
  Status GetFunction(LlvmCodeGen* codegen, const std::string& symbol,
      CodegenFnPtrBase* fn) WARN_UNUSED_RESULT;

  /// Loads the Prepare() and Close() functions for this ScalarFnCall. They could be
  /// native or IR functions. To load IR functions, the codegen object must have
  /// been created and any external LLVM module must have been linked already.
  Status LoadPrepareAndCloseFn(LlvmCodeGen* codegen) WARN_UNUSED_RESULT;

  /// Evaluates the non-constant children exprs. Used in the interpreted path.
  void EvaluateNonConstantChildren(
      ScalarExprEvaluator* eval, const TupleRow* row) const;

  /// Function to call scalar_fn_. Used in the interpreted path.
  template <typename RETURN_TYPE>
  RETURN_TYPE InterpretEval(ScalarExprEvaluator* eval, const TupleRow* row) const;
};
}

#endif
