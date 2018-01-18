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
    scalar_fn_wrapper_(NULL),
    prepare_fn_(NULL),
    close_fn_(NULL),
    scalar_fn_(NULL) {
  DCHECK_NE(fn_.binary_type, TFunctionBinaryType::JAVA);
}

Status ScalarFnCall::LoadPrepareAndCloseFn(LlvmCodeGen* codegen) {
  if (fn_.scalar_fn.__isset.prepare_fn_symbol) {
    RETURN_IF_ERROR(GetFunction(codegen, fn_.scalar_fn.prepare_fn_symbol,
        reinterpret_cast<void**>(&prepare_fn_)));
  }
  if (fn_.scalar_fn.__isset.close_fn_symbol) {
    RETURN_IF_ERROR(GetFunction(codegen, fn_.scalar_fn.close_fn_symbol,
        reinterpret_cast<void**>(&close_fn_)));
  }
  return Status::OK();
}

Status ScalarFnCall::Init(const RowDescriptor& desc, RuntimeState* state) {
  // Initialize children first.
  RETURN_IF_ERROR(ScalarExpr::Init(desc, state));

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

  // Check if the function takes CHAR as input or returns CHAR.
  bool has_char_arg_or_result = type_.type == TYPE_CHAR;
  for (int i = 0; !has_char_arg_or_result && i < children_.size(); ++i) {
    has_char_arg_or_result |= children_[i]->type_.type == TYPE_CHAR;
  }

  // Use the interpreted path and call the builtin without codegen if any of the
  // followings is true:
  // 1. codegen is disabled by query option
  // 2. there are char arguments (as they aren't supported yet)
  // 3. there is an optimization hint to disable codegen and UDF can be interpreted.
  //    IR UDF or UDF with more than MAX_INTERP_ARGS number of fixed arguments
  //    cannot be interpreted.
  //
  // TODO: codegen for char arguments
  bool is_ir_udf = fn_.binary_type == TFunctionBinaryType::IR;
  bool too_many_args_to_interp = NumFixedArgs() > MAX_INTERP_ARGS;
  bool udf_interpretable = !is_ir_udf && !too_many_args_to_interp;
  if (state->CodegenDisabledByQueryOption() || has_char_arg_or_result ||
      (state->CodegenHasDisableHint() && udf_interpretable)) {
    if (is_ir_udf) {
      // CHAR or VARCHAR are not supported as input arguments or return values for UDFs.
      DCHECK(!has_char_arg_or_result && state->CodegenDisabledByQueryOption());
      return Status(Substitute("Cannot interpret LLVM IR UDF '$0': Codegen is needed. "
          "Please set DISABLE_CODEGEN to false.", fn_.name.function_name));
    }

    // The templates for builtin or native UDFs used in the interpretation path
    // support up to MAX_INTERP_ARGS number of arguments only.
    if (too_many_args_to_interp) {
      DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::NATIVE);
      // CHAR or VARCHAR are not supported as input arguments or return values for UDFs.
      DCHECK(!has_char_arg_or_result && state->CodegenDisabledByQueryOption());
      return Status(Substitute("Cannot interpret native UDF '$0': number of arguments is "
          "more than $1. Codegen is needed. Please set DISABLE_CODEGEN to false.",
          fn_.name.function_name, MAX_INTERP_ARGS));
    }

    Status status = LibCache::instance()->GetSoFunctionPtr(
        fn_.hdfs_location, fn_.scalar_fn.symbol, &scalar_fn_, &cache_entry_);
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
  } else {
    // Add the expression to the list of expressions to codegen in the codegen phase.
    state->AddScalarFnToCodegen(this);
  }

  // For IR UDF, the loading of the Init() and CloseContext() functions is deferred until
  // first time GetCodegendComputeFn() is invoked.
  if (!is_ir_udf) RETURN_IF_ERROR(LoadPrepareAndCloseFn(NULL));
  return Status::OK();
}

Status ScalarFnCall::OpenEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  // Opens and inits children
  RETURN_IF_ERROR(ScalarExpr::OpenEvaluator(scope, state, eval));
  DCHECK_GE(fn_ctx_idx_, 0);
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  bool is_interpreted = scalar_fn_wrapper_ == nullptr;

  if (is_interpreted) {
    // We're in the interpreted path (i.e. no JIT). Populate our FunctionContext's
    // staging_input_vals, which will be reused across calls to scalar_fn_.
    DCHECK(scalar_fn_ != nullptr);
    vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
    for (int i = 0; i < NumFixedArgs(); ++i) {
      AnyVal* input_val;
      RETURN_IF_ERROR(AllocateAnyVal(state, eval->expr_perm_pool(), children_[i]->type(),
          "Could not allocate expression value", &input_val));
      input_vals->push_back(input_val);
    }
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
    // TODO: Move this to Expr initialization.
    if (this == &eval->root()) {
      if (fn_.name.function_name == "round" && type_.type == TYPE_DOUBLE) {
        DCHECK_EQ(children_.size(), 2);
        IntVal* scale_arg = reinterpret_cast<IntVal*>(constant_args[1]);
        if (scale_arg != nullptr) eval->output_scale_ = scale_arg->val;
      }
    }
  }

  if (is_interpreted) {
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
  }

  if (prepare_fn_ != nullptr) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
      prepare_fn_(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
      if (fn_ctx->has_error()) return Status(fn_ctx->error_msg());
    }
    prepare_fn_(fn_ctx, FunctionContext::THREAD_LOCAL);
    if (fn_ctx->has_error()) return Status(fn_ctx->error_msg());
  }

  return Status::OK();
}

void ScalarFnCall::CloseEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  DCHECK_GE(fn_ctx_idx_, 0);
  if (close_fn_ != NULL) {
    FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
    close_fn_(fn_ctx, FunctionContext::THREAD_LOCAL);
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
      close_fn_(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
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
Status ScalarFnCall::GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }
  if (type_.type == TYPE_CHAR) {
    return Status::Expected("ScalarFnCall Codegen not supported for CHAR");
  }
  for (int i = 0; i < GetNumChildren(); ++i) {
    if (children_[i]->type().type == TYPE_CHAR) {
      *fn = NULL;
      return Status::Expected("ScalarFnCall Codegen not supported for CHAR");
    }
  }

  vector<ColumnType> arg_types;
  for (const Expr* child : children_) arg_types.push_back(child->type());
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
    Status status = children_[i]->GetCodegendComputeFn(codegen, &child_fn);
    if (UNLIKELY(!status.ok())) {
      DCHECK(child_fn == NULL);
      // Set 'child_fn' to the interpreted function
      child_fn = GetStaticGetValWrapper(children_[i]->type(), codegen);
      // First argument to interpreted function is children_[i]
      llvm::Type* expr_ptr_type = codegen->GetStructPtrType<ScalarExpr>();
      child_fn_args.push_back(codegen->CastPtrToLlvmPtr(expr_ptr_type, children_[i]));
    }
    child_fn_args.push_back(eval);
    child_fn_args.push_back(row);

    // Call 'child_fn', adding the result to either 'udf_args' or 'varargs_buffer'
    DCHECK(child_fn != NULL);
    llvm::Type* arg_type = CodegenAnyVal::GetUnloweredType(codegen, children_[i]->type());
    llvm::Value* arg_val_ptr;
    if (i < NumFixedArgs()) {
      // Allocate space to store 'child_fn's result so we can pass the pointer to the UDF.
      arg_val_ptr = codegen->CreateEntryBlockAlloca(builder, arg_type, "arg_val_ptr");
      udf_args.push_back(arg_val_ptr);
    } else {
      // Store the result of 'child_fn' in varargs_buffer[i].
      arg_val_ptr =
          builder.CreateConstGEP1_32(varargs_buffer, i - NumFixedArgs(), "arg_val_ptr");
    }
    DCHECK_EQ(arg_val_ptr->getType(), arg_type->getPointerTo());
    // The result of the call must be stored in a lowered AnyVal
    llvm::Value* lowered_arg_val_ptr = builder.CreateBitCast(arg_val_ptr,
        CodegenAnyVal::GetLoweredPtrType(codegen, children_[i]->type()),
        "lowered_arg_val_ptr");
    CodegenAnyVal::CreateCall(
        codegen, &builder, child_fn, child_fn_args, "arg_val", lowered_arg_val_ptr);
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
  ir_compute_fn_ = *fn;
  // TODO: don't do this for child exprs
  codegen->AddFunctionToJit(ir_compute_fn_, &scalar_fn_wrapper_);
  return Status::OK();
}

Status ScalarFnCall::GetFunction(LlvmCodeGen* codegen, const string& symbol, void** fn) {
  if (fn_.binary_type == TFunctionBinaryType::NATIVE
      || fn_.binary_type == TFunctionBinaryType::BUILTIN) {
    return LibCache::instance()->GetSoFunctionPtr(
        fn_.hdfs_location, symbol, fn, &cache_entry_);
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

typedef BooleanVal (*BooleanWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef TinyIntVal (*TinyIntWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef SmallIntVal (*SmallIntWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef IntVal (*IntWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef BigIntVal (*BigIntWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef FloatVal (*FloatWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef DoubleVal (*DoubleWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef StringVal (*StringWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef TimestampVal (*TimestampWrapper)(ScalarExprEvaluator*, const TupleRow*);
typedef DecimalVal (*DecimalWrapper)(ScalarExprEvaluator*, const TupleRow*);

// TODO: macroify this?
BooleanVal ScalarFnCall::GetBooleanVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<BooleanVal>(eval, row);
  BooleanWrapper fn = reinterpret_cast<BooleanWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

TinyIntVal ScalarFnCall::GetTinyIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TINYINT);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<TinyIntVal>(eval, row);
  TinyIntWrapper fn = reinterpret_cast<TinyIntWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

SmallIntVal ScalarFnCall::GetSmallIntVal(
     ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_SMALLINT);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<SmallIntVal>(eval, row);
  SmallIntWrapper fn = reinterpret_cast<SmallIntWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

IntVal ScalarFnCall::GetIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_INT);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<IntVal>(eval, row);
  IntWrapper fn = reinterpret_cast<IntWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

BigIntVal ScalarFnCall::GetBigIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BIGINT);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<BigIntVal>(eval, row);
  BigIntWrapper fn = reinterpret_cast<BigIntWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

FloatVal ScalarFnCall::GetFloatVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_FLOAT);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<FloatVal>(eval, row);
  FloatWrapper fn = reinterpret_cast<FloatWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

DoubleVal ScalarFnCall::GetDoubleVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DOUBLE);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<DoubleVal>(eval, row);
  DoubleWrapper fn = reinterpret_cast<DoubleWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

StringVal ScalarFnCall::GetStringVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsStringType());
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<StringVal>(eval, row);
  StringWrapper fn = reinterpret_cast<StringWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

TimestampVal ScalarFnCall::GetTimestampVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<TimestampVal>(eval, row);
  TimestampWrapper fn = reinterpret_cast<TimestampWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

DecimalVal ScalarFnCall::GetDecimalVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DECIMAL);
  DCHECK(eval != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<DecimalVal>(eval, row);
  DecimalWrapper fn = reinterpret_cast<DecimalWrapper>(scalar_fn_wrapper_);
  return fn(eval, row);
}

string ScalarFnCall::DebugString() const {
  stringstream out;
  out << "ScalarFnCall(udf_type=" << fn_.binary_type << " location=" << fn_.hdfs_location
      << " symbol_name=" << fn_.scalar_fn.symbol <<  ScalarExpr::DebugString() << ")";
  return out.str();
}

int ScalarFnCall::ComputeVarArgsBufferSize() const {
  for (int i = NumFixedArgs(); i < children_.size(); ++i) {
    // All varargs should have same type.
    DCHECK_EQ(children_[i]->type(), VarArgsType());
  }
  return NumVarArgs() == 0 ? 0 : NumVarArgs() * AnyValUtil::AnyValSize(VarArgsType());
}
