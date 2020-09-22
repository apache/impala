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

#include "exprs/scalar-fn-call.h"

#include <vector>
#include <gutil/strings/substitute.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/Attributes.h>

#include <boost/preprocessor/punctuation/comma_if.hpp>
#include <boost/preprocessor/repetition/enum_params.hpp>
#include <boost/preprocessor/repetition/repeat.hpp>
#include <boost/preprocessor/repetition/repeat_from_to.hpp>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/anyval-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/fragment-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/types.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;
using namespace strings;
using std::move;
using std::pair;

// Maximum number of arguments the interpretation path supports.
#define MAX_INTERP_ARGS 20

ScalarFnCall::ScalarFnCall(const TExprNode& node)
  : ScalarExpr(node),
    vararg_start_idx_(node.__isset.vararg_start_idx ? node.vararg_start_idx : -1),
    prepare_fn_(),
    close_fn_(),
    scalar_fn_(NULL) {
  DCHECK_NE(fn_.binary_type, TFunctionBinaryType::JAVA);
}

Status ScalarFnCall::LoadPrepareAndCloseFn(LlvmCodeGen* codegen) {
  if (fn_.scalar_fn.__isset.prepare_fn_symbol) {
    RETURN_IF_ERROR(GetFunction(codegen, fn_.scalar_fn.prepare_fn_symbol,
        &prepare_fn_));
  }
  if (fn_.scalar_fn.__isset.close_fn_symbol) {
    RETURN_IF_ERROR(GetFunction(codegen, fn_.scalar_fn.close_fn_symbol,
        &close_fn_));
  }
  return Status::OK();
}

Status ScalarFnCall::Init(
    const RowDescriptor& desc, bool is_entry_point, FragmentState* state) {
  // Initialize children first.
  RETURN_IF_ERROR(ScalarExpr::Init(desc, is_entry_point, state));

  if (fn_.scalar_fn.symbol.empty()) {
    // This path is intended to only be used during development to test FE
    // code before the BE has implemented the function.
    // Having the failure in the BE (rather than during analysis) allows for
    // better FE testing.
    DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::BUILTIN);
    stringstream ss;
    ss << "Function " << fn_.name.function_name << " is not implemented.";
    return Status(ss.str());
  }

  bool is_ir_udf = fn_.binary_type == TFunctionBinaryType::IR;
  if (!ShouldCodegen(state)) {
    // The interpreted code path must be handled in different ways depending on why
    // codegen was disabled. It may not be possible to evaluate the expr without
    // codegen or we may need to prepare the function for execution.
    if (is_ir_udf) {
      DCHECK(state->CodegenDisabledByQueryOption());
      return Status(Substitute("Cannot interpret LLVM IR UDF '$0': Codegen is needed. "
                               "Please set DISABLE_CODEGEN to false.",
          fn_.name.function_name));
    }

    // The templates for builtin or native UDFs used in the interpretation path
    // support up to MAX_INTERP_ARGS number of arguments only.
    if (NumFixedArgs() > MAX_INTERP_ARGS) {
      DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::NATIVE);
      // CHAR or VARCHAR are not supported as input arguments or return values for UDFs.
      DCHECK(state->CodegenDisabledByQueryOption());
      return Status(Substitute(
          "Cannot interpret native UDF '$0': number of arguments is "
          "more than $1. Codegen is needed. Please set DISABLE_CODEGEN to false.",
          fn_.name.function_name, MAX_INTERP_ARGS));
    }
  }

  if (!is_ir_udf) {
    Status status = LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
        fn_.scalar_fn.symbol, fn_.last_modified_time, &scalar_fn_, &cache_entry_);
    if (!status.ok()) {
      if (fn_.binary_type == TFunctionBinaryType::BUILTIN) {
        // Builtins symbols should exist unless there is a version mismatch.
        return Status(TErrorCode::MISSING_BUILTIN, fn_.name.function_name,
            fn_.scalar_fn.symbol);
      } else {
        DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::NATIVE);
        return Status(Substitute("Problem loading UDF '$0':\n$1",
            fn_.name.function_name, status.GetDetail()));
      }
    }
    // For IR UDF, the loading of the Init() and CloseContext() functions is deferred
    // until the first time GetCodegendComputeFn() is invoked.
    RETURN_IF_ERROR(LoadPrepareAndCloseFn(nullptr));
  }
  return Status::OK();
}

Status ScalarFnCall::OpenEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  // Opens and inits children
  RETURN_IF_ERROR(ScalarExpr::OpenEvaluator(scope, state, eval));
  DCHECK_GE(fn_ctx_idx_, 0);
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);

  // Prepare staging_input_vals in case the interpreted evaluation path of
  // this function is invoked. staging_input_vals is preallocated here
  // so they can be reused across calls. If we have a codegen'd entry point
  // for this expression, allocating these input values may be unnecessary,
  // but they only add a small constant overhead on top of the ScalarExpr tree, so
  // we always allocate them for simplicity.
  vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
  for (int i = 0; i < NumFixedArgs(); ++i) {
    AnyVal* input_val;
    RETURN_IF_ERROR(AllocateAnyVal(state, eval->expr_perm_pool(), children_[i]->type(),
        "Could not allocate expression value", &input_val));
    input_vals->push_back(input_val);
  }

  // Only evaluate constant arguments at the top level of function contexts.
  // If 'eval' was cloned, the constant values were copied from the parent.
  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    vector<AnyVal*> constant_args;
    for (const ScalarExpr* child : children()) {
      AnyVal* const_val;
      RETURN_IF_ERROR(eval->GetConstValue(state, *child, &const_val));
      constant_args.push_back(const_val);
    }
    fn_ctx->impl()->SetConstantArgs(move(constant_args));

    // If we're calling MathFunctions::RoundUpTo(), we need to set output_scale_
    // which determines how many decimal places are printed.
    // TODO: Move this to Expr initialization when IMPALA-4743 is fixed.
    if (this == &eval->root() &&
        fn_.name.function_name == "round" &&
        type_.type == TYPE_DOUBLE &&
        children_.size() == 2) {
      BigIntVal* scale_arg = reinterpret_cast<BigIntVal*>(constant_args[1]);
      if (scale_arg != nullptr) eval->output_scale_ = scale_arg->val;
    }
  }

  // Now we have the constant values, cache them so that the interpreted path can
  // call the UDF without reevaluating the arguments. 'staging_input_vals' and
  // 'varargs_buffer' in the FunctionContext are used to pass fixed and variable-length
  // arguments respectively. 'non_constant_args()' in the FunctionContext will contain
  // pointers to the remaining (non-constant) children that are evaluated for every row.
  vector<pair<ScalarExpr*, AnyVal*>> non_constant_args;
  uint8_t* varargs_buffer = fn_ctx->impl()->varargs_buffer();
  for (int i = 0; i < children_.size(); ++i) {
    AnyVal* input_arg;
    int arg_bytes = AnyValUtil::AnyValSize(children_[i]->type());
    if (i < NumFixedArgs()) {
      input_arg = (*fn_ctx->impl()->staging_input_vals())[i];
    } else {
      input_arg = reinterpret_cast<AnyVal*>(varargs_buffer);
      varargs_buffer += arg_bytes;
    }
    // IMPALA-4586: Cache constant arguments only if the frontend has rewritten them
    // into literal expressions. This gives the frontend control over how expressions
    // are evaluated. This means that setting enable_expr_rewrites=false will also
    // disable caching of non-literal constant expressions, which gives the old
    // behaviour (before this caching optimisation was added) of repeatedly evaluating
    // exprs that are constant according to is_constant(). For exprs that are not truly
    // constant (yet is_constant() returns true for) e.g. non-deterministic UDFs, this
    // means that setting enable_expr_rewrites=false works as a safety valve to get
    // back the old behaviour, before constant expr folding or caching was added.
    // TODO: once we can annotate UDFs as non-deterministic (IMPALA-4606), we should
    // be able to trust is_constant() and switch back to that.
    if (children_[i]->IsLiteral()) {
      const AnyVal* constant_arg = fn_ctx->impl()->constant_args()[i];
      DCHECK(constant_arg != nullptr);
      memcpy(input_arg, constant_arg, arg_bytes);
    } else {
      non_constant_args.emplace_back(children_[i], input_arg);
    }
  }
  fn_ctx->impl()->SetNonConstantArgs(move(non_constant_args));

  const impala_udf::UdfPrepare prepare_fn = prepare_fn_.load();
  if (prepare_fn != nullptr) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
      prepare_fn(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
      if (fn_ctx->has_error()) return Status(fn_ctx->error_msg());
    }
    prepare_fn(fn_ctx, FunctionContext::THREAD_LOCAL);
    if (fn_ctx->has_error()) return Status(fn_ctx->error_msg());
  }

  return Status::OK();
}

void ScalarFnCall::CloseEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  DCHECK_GE(fn_ctx_idx_, 0);
  const impala_udf::UdfClose close_fn = close_fn_.load();
  if (close_fn != NULL) {
    FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
    close_fn(fn_ctx, FunctionContext::THREAD_LOCAL);
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
      close_fn(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
    }
  }
  ScalarExpr::CloseEvaluator(scope, state, eval);
}

// Dynamically loads the pre-compiled UDF and codegens a function that calls each child's
// codegen'd function, then passes those values to the UDF and returns the result.
// Example generated IR for a UDF with signature
//    create function Udf(double, int...) returns double
//    select Udf(1.0, 2, 3, 4, 5)
// define { i8, double } @UdfWrapper(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   %arg_val = call { i8, double }
//      @ExprWrapper(i8* %context, %"class.impala::TupleRow"* %row)
//   %arg_ptr = alloca { i8, double }
//   store { i8, double } %arg_val, { i8, double }* %arg_ptr
//   %arg_val1 = call i64 @ExprWrapper1(i8* %context, %"class.impala::TupleRow"* %row)
//   store i64 %arg_val1, i64* inttoptr (i64 89111072 to i64*)
//   %arg_val2 = call i64 @ExprWrapper2(i8* %context, %"class.impala::TupleRow"* %row)
//   store i64 %arg_val2, i64* inttoptr (i64 89111080 to i64*)
//   %arg_val3 = call i64 @ExprWrapper3(i8* %context, %"class.impala::TupleRow"* %row)
//   store i64 %arg_val3, i64* inttoptr (i64 89111088 to i64*)
//   %arg_val4 = call i64 @ExprWrapper4(i8* %context, %"class.impala::TupleRow"* %row)
//   store i64 %arg_val4, i64* inttoptr (i64 89111096 to i64*)
//   %result = call { i8, double }
//      @_Z14VarSumMultiplyPN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE(
//        %"class.impala_udf::FunctionContext"* inttoptr
//            (i64 37522464 to %"class.impala_udf::FunctionContext"*),
//        {i8, double }* %arg_ptr,
//        i32 4,
//        i64* inttoptr (i64 89111072 to i64*))
//   ret { i8, double } %result
Status ScalarFnCall::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
  vector<ColumnType> arg_types;
  for (ScalarExpr* child : children_) arg_types.push_back(child->type());
  llvm::Function* udf;
  RETURN_IF_ERROR(codegen->LoadFunction(fn_, fn_.scalar_fn.symbol, &type_, arg_types,
      NumFixedArgs(), vararg_start_idx_ != -1, &udf, &cache_entry_));
  // Inline constants into the function if it has an IR body.
  if (!udf->isDeclaration()) {
    codegen->InlineConstFnAttrs(AnyValUtil::ColumnTypeToTypeDesc(type_),
        AnyValUtil::ColumnTypesToTypeDescs(arg_types), udf);
    udf = codegen->FinalizeFunction(udf);
    if (udf == NULL) {
      return Status(
          TErrorCode::UDF_VERIFY_FAILED, fn_.scalar_fn.symbol, fn_.hdfs_location);
    }
  }

  if (fn_.binary_type == TFunctionBinaryType::IR) {
    // LoadFunction() should have linked the IR module into 'codegen'. Now load the
    // Prepare() and Close() functions from 'codegen'.
    RETURN_IF_ERROR(LoadPrepareAndCloseFn(codegen));
  }

  // Create wrapper that computes args and calls UDF
  stringstream fn_name;
  fn_name << udf->getName().str() << "Wrapper";

  llvm::Value* args[2];
  *fn = CreateIrFunctionPrototype(fn_name.str(), codegen, &args);
  llvm::Value* eval = args[0];
  llvm::Value* row = args[1];
  llvm::BasicBlock* block = llvm::BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmBuilder builder(block);

  // Populate UDF arguments
  vector<llvm::Value*> udf_args;

  // First argument is always FunctionContext*.
  // Index into our registered offset in the ScalarFnEvaluator.
  llvm::Value* eval_gep = builder.CreateStructGEP(NULL, eval, 1, "eval_gep");
  llvm::Value* fn_ctxs_base = builder.CreateLoad(eval_gep, "fn_ctxs_base");
  // Use GEP to add our index to the base pointer
  llvm::Value* fn_ctx_ptr =
      builder.CreateConstGEP1_32(fn_ctxs_base, fn_ctx_idx_, "fn_ctx_ptr");
  llvm::Value* fn_ctx = builder.CreateLoad(fn_ctx_ptr, "fn_ctx");
  udf_args.push_back(fn_ctx);

  // Allocate a varargs array. The array's entry type is the appropriate AnyVal subclass.
  // E.g. if the vararg type is STRING, and the function is called with 10 arguments, we
  // allocate a StringVal[10] array. We allocate the buffer with Alloca so that LLVM can
  // optimise out the buffer once the function call is inlined.
  llvm::Value* varargs_buffer = NULL;
  if (vararg_start_idx_ != -1) {
    llvm::Type* unlowered_varargs_type =
        CodegenAnyVal::GetUnloweredType(codegen, VarArgsType());
    varargs_buffer = codegen->CreateEntryBlockAlloca(builder, unlowered_varargs_type,
        NumVarArgs(), FunctionContextImpl::VARARGS_BUFFER_ALIGNMENT, "varargs_buffer");
  }

  // Call children to populate remaining arguments
  for (int i = 0; i < GetNumChildren(); ++i) {
    llvm::Function* child_fn = NULL;
    vector<llvm::Value*> child_fn_args;
    // Set 'child_fn' to the codegen'd function, sets child_fn == NULL if codegen fails
    RETURN_IF_ERROR(children_[i]->GetCodegendComputeFn(codegen, false, &child_fn));
    child_fn_args.push_back(eval);
    child_fn_args.push_back(row);

    // Call 'child_fn', adding the result to either 'udf_args' or 'varargs_buffer'
    DCHECK(child_fn != NULL);
    llvm::Type* arg_type = CodegenAnyVal::GetUnloweredType(codegen, children_[i]->type());
    llvm::Value* arg_val_ptr;
#ifdef __aarch64__
    PrimitiveType col_type = children_[i]->type().type;
#endif
    if (i < NumFixedArgs()) {
#ifndef __aarch64__
      // Allocate space to store 'child_fn's result so we can pass the pointer to the UDF.
      arg_val_ptr = codegen->CreateEntryBlockAlloca(builder, arg_type, "arg_val_ptr");
      udf_args.push_back(arg_val_ptr);
#else
      if (col_type != TYPE_BOOLEAN and col_type != TYPE_TINYINT
          and col_type != TYPE_SMALLINT) {
        arg_val_ptr = codegen->CreateEntryBlockAlloca(builder, arg_type, "arg_val_ptr");
        udf_args.push_back(arg_val_ptr);
      }
#endif
    } else {
      // Store the result of 'child_fn' in varargs_buffer[i].
      arg_val_ptr =
          builder.CreateConstGEP1_32(varargs_buffer, i - NumFixedArgs(), "arg_val_ptr");
    }
#ifndef __aarch64__
    DCHECK_EQ(arg_val_ptr->getType(), arg_type->getPointerTo());
    // The result of the call must be stored in a lowered AnyVal
    llvm::Value* lowered_arg_val_ptr = builder.CreateBitCast(arg_val_ptr,
        CodegenAnyVal::GetLoweredPtrType(codegen, children_[i]->type()),
        "lowered_arg_val_ptr");
#else
    llvm::Value* lowered_arg_val_ptr;
    if (col_type == TYPE_BOOLEAN or col_type == TYPE_TINYINT
        or col_type == TYPE_SMALLINT) {
      lowered_arg_val_ptr = codegen->CreateEntryBlockAlloca(builder,
          CodegenAnyVal::GetLoweredType(codegen, children_[i]->type()), 1,
          FunctionContextImpl::VARARGS_BUFFER_ALIGNMENT, "lowered_arg_val_ptr");
    } else {
      lowered_arg_val_ptr = builder.CreateBitCast(arg_val_ptr,
          CodegenAnyVal::GetLoweredPtrType(codegen, children_[i]->type()),
          "lowered_arg_val_ptr");
    }
#endif
    CodegenAnyVal::CreateCall(
        codegen, &builder, child_fn, child_fn_args, "arg_val", lowered_arg_val_ptr);
#ifdef __aarch64__
    if (col_type == TYPE_BOOLEAN or col_type == TYPE_TINYINT
        or col_type == TYPE_SMALLINT) {
      if (i < NumFixedArgs()) {
        arg_val_ptr = builder.CreateTruncOrBitCast(lowered_arg_val_ptr,
            CodegenAnyVal::GetUnloweredPtrType(codegen, children_[i]->type()),
            "arg_val_ptr");
        udf_args.push_back(arg_val_ptr);
      } else {
        llvm::Value* tmp_ptr = builder.CreateTruncOrBitCast(lowered_arg_val_ptr,
            CodegenAnyVal::GetUnloweredPtrType(codegen, children_[i]->type()),
            "tmp_ptr");
        builder.CreateStore(builder.CreateLoad(tmp_ptr), arg_val_ptr);
      }
    }
#endif
  }

  if (vararg_start_idx_ != -1) {
    // We've added the FunctionContext argument plus any non-variadic arguments
    DCHECK_EQ(udf_args.size(), vararg_start_idx_ + 1);
    DCHECK_GE(GetNumChildren(), 1);
    // Add the number of varargs
    udf_args.push_back(codegen->GetI32Constant(NumVarArgs()));
    // Add all the accumulated vararg inputs as one input argument.
    llvm::PointerType* vararg_type =
        CodegenAnyVal::GetUnloweredPtrType(codegen, VarArgsType());
    udf_args.push_back(builder.CreateBitCast(varargs_buffer, vararg_type, "varargs"));
  }

  // Call UDF
  llvm::Value* result_val =
      CodegenAnyVal::CreateCall(codegen, &builder, udf, udf_args, "result");
  builder.CreateRet(result_val);

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status(
        TErrorCode::UDF_VERIFY_FAILED, fn_.scalar_fn.symbol, fn_.hdfs_location);
  }
  return Status::OK();
}

Status ScalarFnCall::GetFunction(LlvmCodeGen* codegen, const string& symbol,
    CodegenFnPtrBase* fn) {
  if (fn_.binary_type == TFunctionBinaryType::NATIVE
      || fn_.binary_type == TFunctionBinaryType::BUILTIN) {
    void* raw_fn;
    const Status status = LibCache::instance()->GetSoFunctionPtr(
        fn_.hdfs_location, symbol, fn_.last_modified_time, &raw_fn, &cache_entry_);
    fn->store(raw_fn);
    return status;
  } else {
    DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::IR);
    DCHECK(codegen != NULL);
    llvm::Function* ir_fn = codegen->GetFunction(symbol, false);
    if (ir_fn == NULL) {
      stringstream ss;
      ss << "Unable to locate function " << symbol << " from LLVM module "
         << fn_.hdfs_location;
      return Status(ss.str());
    }
    ir_fn = codegen->FinalizeFunction(ir_fn);
    codegen->AddFunctionToJit(ir_fn, fn);
    return Status::OK();
  }
}

void ScalarFnCall::EvaluateNonConstantChildren(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  for (pair<ScalarExpr*, AnyVal*> child : fn_ctx->impl()->non_constant_args()) {
    void* val = eval->GetValue(*(child.first), row);
    AnyValUtil::SetAnyVal(val, child.first->type(), child.second);
  }
}

template<typename RETURN_TYPE>
RETURN_TYPE ScalarFnCall::InterpretEval(ScalarExprEvaluator* eval,
    const TupleRow* row) const {
  DCHECK(scalar_fn_ != NULL) << DebugString();
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
  EvaluateNonConstantChildren(eval, row);

  if (vararg_start_idx_ == -1) {
    switch (children_.size()) {
#define ARG_DECL_ONE(z, n, data) BOOST_PP_COMMA_IF(n) const AnyVal&
#define ARG_DECL_LIST(n) \
  FunctionContext* BOOST_PP_COMMA_IF(n) BOOST_PP_REPEAT(n, ARG_DECL_ONE, unused)
#define ARG_ONE(z, n, data) BOOST_PP_COMMA_IF(n) *(*input_vals)[n]
#define ARG_LIST(n) fn_ctx BOOST_PP_COMMA_IF(n) BOOST_PP_REPEAT(n, ARG_ONE, unused)

   // Expands to code snippet like the following for X from 0 to 20:
   // case X:
   //     typedef RETURN_TYPE (*ScalarFnX)(FunctionContext*, const AnyVal& a1, ...,
   //         const AnyVal& aX);
   //     return reinterpret_cast<ScalarFnX>(scalar_fn_)(fn_ctx, *(*input_vals)[0], ...,
   //         *(*input_vals)[X-1]);
#define SCALAR_FN_TYPE(n) BOOST_PP_CAT(ScalarFn, n)
#define INTERP_SCALAR_FN(z, n, unused)                                       \
      case n:                                                                \
        typedef RETURN_TYPE (*SCALAR_FN_TYPE(n))(ARG_DECL_LIST(n));          \
        return reinterpret_cast<SCALAR_FN_TYPE(n)>(scalar_fn_)(ARG_LIST(n));

      // Support up to MAX_INTERP_ARGS arguments in the interpretation path
      BOOST_PP_REPEAT_FROM_TO(0, BOOST_PP_ADD(MAX_INTERP_ARGS, 1),
          INTERP_SCALAR_FN, unused)

      default:
        DCHECK(false) << "Interpreted path not implemented.";
    }
  } else {
    int num_varargs = children_.size() - NumFixedArgs();
    const AnyVal* varargs = reinterpret_cast<AnyVal*>(fn_ctx->impl()->varargs_buffer());
    switch (NumFixedArgs()) {

   // Expands to code snippet like the following for X from 0 to 20:
   // case X:
   //     typedef RETURN_TYPE (*VarargFnX)(FunctionContext*, const AnyVal& a1, ...,
   //         const AnyVal& aX, int num_varargs, const AnyVal* varargs);
   //     return reinterpret_cast<VarargFnX>(scalar_fn_)(fn_ctx, *(*input_vals)[0], ...,
   //         *(*input_vals)[X-1], num_varargs, varargs);
#define SCALAR_VARARG_FN_TYPE(n) BOOST_PP_CAT(VarargFn, n)
#define INTERP_SCALAR_VARARG_FN(z, n, text)                                        \
      case n:                                                                      \
        typedef RETURN_TYPE (*SCALAR_VARARG_FN_TYPE(n))(ARG_DECL_LIST(n), int,     \
            const AnyVal*);                                                        \
        return reinterpret_cast<SCALAR_VARARG_FN_TYPE(n)>(scalar_fn_)(ARG_LIST(n), \
            num_varargs, varargs);

      BOOST_PP_REPEAT_FROM_TO(0, BOOST_PP_ADD(MAX_INTERP_ARGS, 1),
         INTERP_SCALAR_VARARG_FN, unused)

      default:
        DCHECK(false) << "Interpreted path not implemented.";
    }
  }
  return RETURN_TYPE::null();
}

// Macro to generate implementations for the below functions. 'val_type' is
// a UDF type name, e.g. IntVal and 'type_validation' is a DCHECK expression
// referencing 'type_' to assert that the function is only called on expressions
// of the appropriate type.
// * ScalarFnCall::GetBooleanValInterpreted()
// * ScalarFnCall::GetTinyIntValInterpreted()
// * ScalarFnCall::GetSmallIntValInterpreted()
// * ScalarFnCall::GetIntValInterpreted()
// * ScalarFnCall::GetBigIntValInterpreted()
// * ScalarFnCall::GetFloatValInterpreted()
// * ScalarFnCall::GetDoubleValInterpreted()
// * ScalarFnCall::GetStringValInterpreted()
// * ScalarFnCall::GetTimestampValInterpreted()
// * ScalarFnCall::GetDecimalValInterpreted()
// * ScalarFnCall::GetDateValInterpreted()
#pragma push_macro("GET_VAL_INTERPRETED")
#define GET_VAL_INTERPRETED(val_type, type_validation)        \
  val_type ScalarFnCall::Get##val_type##Interpreted(          \
      ScalarExprEvaluator* eval, const TupleRow* row) const { \
    DCHECK(type_validation) << type_.DebugString();           \
    DCHECK(eval != nullptr);                                  \
    return InterpretEval<val_type>(eval, row);                \
  }

GET_VAL_INTERPRETED(BooleanVal, type_.type == PrimitiveType::TYPE_BOOLEAN);
GET_VAL_INTERPRETED(TinyIntVal, type_.type == PrimitiveType::TYPE_TINYINT);
GET_VAL_INTERPRETED(SmallIntVal, type_.type == PrimitiveType::TYPE_SMALLINT);
GET_VAL_INTERPRETED(IntVal, type_.type == PrimitiveType::TYPE_INT);
GET_VAL_INTERPRETED(BigIntVal, type_.type == PrimitiveType::TYPE_BIGINT);
GET_VAL_INTERPRETED(FloatVal, type_.type == PrimitiveType::TYPE_FLOAT);
GET_VAL_INTERPRETED(DoubleVal, type_.type == PrimitiveType::TYPE_DOUBLE);
GET_VAL_INTERPRETED(StringVal,
    type_.IsStringType() || type_.type == PrimitiveType::TYPE_FIXED_UDA_INTERMEDIATE);
GET_VAL_INTERPRETED(TimestampVal, type_.type == PrimitiveType::TYPE_TIMESTAMP);
GET_VAL_INTERPRETED(DecimalVal, type_.type == PrimitiveType::TYPE_DECIMAL);
GET_VAL_INTERPRETED(DateVal, type_.type == PrimitiveType::TYPE_DATE);
#pragma pop_macro("GET_VAL_INTERPRETED")

string ScalarFnCall::DebugString() const {
  stringstream out;
  out << "ScalarFnCall(udf_type=" << fn_.binary_type << " location=" << fn_.hdfs_location
      << " symbol_name=" << fn_.scalar_fn.symbol <<  ScalarExpr::DebugString() << ")";
  return out.str();
}

bool ScalarFnCall::IsInterpretable() const {
  return fn_.binary_type != TFunctionBinaryType::IR && NumFixedArgs() <= MAX_INTERP_ARGS;
}

int ScalarFnCall::ComputeVarArgsBufferSize() const {
  for (int i = NumFixedArgs(); i < children_.size(); ++i) {
    // All varargs should have same type.
    DCHECK_EQ(children_[i]->type(), VarArgsType());
  }
  return NumVarArgs() == 0 ? 0 : NumVarArgs() * AnyValUtil::AnyValSize(VarArgsType());
}
