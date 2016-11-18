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

#include "exprs/expr.h"
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class TExprNode;

/// Expr for evaluating a pre-compiled native or LLVM IR function that uses the UDF
/// interface (i.e. a scalar function). This class overrides GetCodegendComputeFn() to
/// return a function that calls any child exprs and passes the results as arguments to the
/// specified scalar function. If codegen is enabled, ScalarFnCall's Get*Val() compute
/// functions are wrappers around this codegen'd function.
//
/// If codegen is disabled, some native functions can be called without codegen, depending
/// on the native function's signature. However, since we can't write static code to call
/// every possible function signature, codegen may be required to generate the call to the
/// function even if codegen is disabled. Codegen will also be used for IR UDFs (note that
/// there is no way to specify both a native and IR library for a single UDF).
//
/// TODO:
/// - Fix error reporting, e.g. reporting leaks
/// - Testing
///    - Test cancellation
///    - Type descs in UDA test harness
///    - Allow more functions to be NULL in UDA test harness
class ScalarFnCall : public Expr {
 public:
  virtual std::string DebugString() const;

 protected:
  friend class Expr;
  friend class RuntimeState;

  ScalarFnCall(const TExprNode& node);
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& desc,
                         ExprContext* context);
  virtual Status Open(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);
  virtual Status GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn);
  virtual void Close(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);

  virtual BooleanVal GetBooleanVal(ExprContext* context, const TupleRow*);
  virtual TinyIntVal GetTinyIntVal(ExprContext* context, const TupleRow*);
  virtual SmallIntVal GetSmallIntVal(ExprContext* context, const TupleRow*);
  virtual IntVal GetIntVal(ExprContext* context, const TupleRow*);
  virtual BigIntVal GetBigIntVal(ExprContext* context, const TupleRow*);
  virtual FloatVal GetFloatVal(ExprContext* context, const TupleRow*);
  virtual DoubleVal GetDoubleVal(ExprContext* context, const TupleRow*);
  virtual StringVal GetStringVal(ExprContext* context, const TupleRow*);
  virtual TimestampVal GetTimestampVal(ExprContext* context, const TupleRow*);
  virtual DecimalVal GetDecimalVal(ExprContext* context, const TupleRow*);

 private:
  /// If this function has var args, children()[vararg_start_idx_] is the first vararg
  /// argument.
  /// If this function does not have varargs, it is set to -1.
  int vararg_start_idx_;

  /// Vector of all non-constant children expressions that need to be evaluated for
  /// each input row. The first element of each pair is the child expression and the
  /// second element in the value it must be evaluated into.
  std::vector<std::pair<Expr*, impala_udf::AnyVal*>> non_constant_children_;

  /// Function pointer to the JIT'd function produced by GetCodegendComputeFn().
  /// Has signature *Val (ExprContext*, const TupleRow*), and calls the scalar
  /// function with signature like *Val (FunctionContext*, const *Val& arg1, ...)
  void* scalar_fn_wrapper_;

  /// The UDF's prepare function, if specified. This is initialized in Prepare() and
  /// called in Open() (since we may have needed to codegen the function if it's from an
  /// IR module).
  UdfPrepare prepare_fn_;

  /// THe UDF's close function, if specified. This is initialized in Prepare() and called
  /// in Close().
  UdfClose close_fn_;

  /// If running with codegen disabled, scalar_fn_ will be a pointer to the non-JIT'd
  /// scalar function.
  void* scalar_fn_;

  /// Returns the number of non-vararg arguments
  int NumFixedArgs() const {
    return vararg_start_idx_ >= 0 ? vararg_start_idx_ : children_.size();
  }

  int NumVarArgs() const { return children_.size() - NumFixedArgs(); }

  const ColumnType& VarArgsType() const {
    DCHECK_GE(NumVarArgs(), 1);
    return children_.back()->type();
  }

  /// Loads the native or IR function 'symbol' from HDFS and puts the result in *fn.
  /// If the function is loaded from an IR module, it cannot be called until the module
  /// has been JIT'd (i.e. after GetCodegendComputeFn() has been called).
  Status GetFunction(LlvmCodeGen* codegen, const std::string& symbol, void** fn);

  /// Loads the Prepare() and Close() functions for this ScalarFnCall. They could be
  /// native or IR functions. To load IR functions, the codegen object must have
  /// been created and any external LLVM module must have been linked already.
  Status LoadPrepareAndCloseFn(LlvmCodeGen* codegen);

  /// Evaluates the non-constant children exprs. Used in the interpreted path.
  void EvaluateNonConstantChildren(ExprContext* context, const TupleRow* row);

  /// Function to call scalar_fn_. Used in the interpreted path.
  template <typename RETURN_TYPE>
  RETURN_TYPE InterpretEval(ExprContext* context, const TupleRow* row);

  /// Computes the size of the varargs buffer in bytes (0 bytes if no varargs).
  int ComputeVarArgsBufferSize() const;
};
}

#endif
