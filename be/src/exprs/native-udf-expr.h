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


#ifndef IMPALA_EXPRS_UDF_EXPR_H_
#define IMPALA_EXPRS_UDF_EXPR_H_

#include <string>
#include "exprs/expr.h"
#include "udf/udf.h"

namespace impala {

class TExprNode;

// Expr for evaluating a pre-compiled native or LLVM IR function that uses the UDF
// interface. We refer to this function as a UDF, even if it's not literally defined by a
// user (e.g. builtins). This class overrides GetIrComputeFn() to return a function that
// calls any child exprs and passes the results as arguments to the specified UDF. This
// codegen'd function is the kernel of the NativeUdfExpr's compute function; ComputeFn()
// is a wrapper around it that conforms to the interpreted compute function API (e.g. it
// stores the UDF's output in result_). GetIrComputeFn() is called in Prepare().
//
// Note that this class does not override Codegen(), even though it produces a mostly
// codegen'd compute function. This means that ComputeFn() is in turn wrapped in a
// codegen'd function in the non-interpreted path. We will eventually get rid of the
// current Codegen() API and everything will use GetIrComputeFn() directly instead. This
// also means that codegen is necessary to evaluate this expr even in the interpreted
// path.
//
// TODO:
// - convert other Exprs to UDFs or override GetIrComputeFn()
// - ExprContext
// - remove current Codegen/ComputeFn API
class NativeUdfExpr: public Expr {
 public:
  virtual std::string DebugString() const;

 protected:
  friend class Expr;

  NativeUdfExpr(const TExprNode& node);
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& desc);
  virtual Status GetIrComputeFn(RuntimeState* state, llvm::Function** fn);

 private:
  // Compute function that calls udf_wrapper_ and writes the result to e->result_.
  // compute_fn_ is set to this in Prepare().
  static void* ComputeFn(Expr* e, TupleRow* row);

  // FunctionContext* that is passed to the UDF
  // TODO: Get this from the to-be-implemented ExprContext instead
  boost::scoped_ptr<impala_udf::FunctionContext> udf_context_;

  // Native (.so) or IR (.ll)
  TFunctionBinaryType::type udf_type_;

  // HDFS/local path and name of the compiled UDF binary
  std::string hdfs_location_;
  std::string symbol_name_;

  // Function pointer to the JIT'd function produced by GetIrComputeFn(). Initialized and
  // called by ComputeFn().
  void* udf_wrapper_;

  // Used by ComputeFn() to create udf_wrapper_. Initialized in Prepare(). Not owned by
  // this class.
  LlvmCodeGen* codegen_;
  llvm::Function* ir_udf_wrapper_;

  // Loads the native or IR function from HDFS and puts the result in *udf.
  Status GetUdf(RuntimeState* state, llvm::Function** udf);
};

}

#endif
