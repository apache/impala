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
#include "exprs/expr-context.h"
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
using llvm::ArrayType;
using llvm::BasicBlock;
using llvm::Function;
using llvm::GlobalVariable;
using llvm::PointerType;
using llvm::Type;
using llvm::Value;
using std::move;
using std::pair;

// Maximum number of arguments the interpretation path supports.
#define MAX_INTERP_ARGS 20

ScalarFnCall::ScalarFnCall(const TExprNode& node)
  : Expr(node),
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

Status ScalarFnCall::Prepare(RuntimeState* state, const RowDescriptor& desc,
    ExprContext* context) {
  RETURN_IF_ERROR(Expr::Prepare(state, desc, context));

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
  FunctionContext::TypeDesc return_type = AnyValUtil::ColumnTypeToTypeDesc(type_);
  vector<FunctionContext::TypeDesc> arg_types;
  bool has_char_arg_or_result = type_.type == TYPE_CHAR;
  for (int i = 0; i < children_.size(); ++i) {
    arg_types.push_back(AnyValUtil::ColumnTypeToTypeDesc(children_[i]->type_));
    has_char_arg_or_result |= children_[i]->type_.type == TYPE_CHAR;
  }

  fn_context_index_ =
      context->Register(state, return_type, arg_types, ComputeVarArgsBufferSize());

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

  // For IR UDF, the loading of the Prepare() and Close() functions is deferred until
  // first time GetCodegendComputeFn() is invoked.
  if (!is_ir_udf) RETURN_IF_ERROR(LoadPrepareAndCloseFn(NULL));
  return Status::OK();
}

Status ScalarFnCall::Open(RuntimeState* state, ExprContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  // Opens and inits children
  RETURN_IF_ERROR(Expr::Open(state, ctx, scope));
  FunctionContext* fn_ctx = ctx->fn_context(fn_context_index_);

  if (scalar_fn_wrapper_ == NULL) {
    // We're in the interpreted path (i.e. no JIT). Populate our FunctionContext's
    // staging_input_vals, which will be reused across calls to scalar_fn_.
    DCHECK(scalar_fn_ != NULL);
    vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
    for (int i = 0; i < NumFixedArgs(); ++i) {
      AnyVal* input_val;
      RETURN_IF_ERROR(AllocateAnyVal(state, ctx->pool_.get(), children_[i]->type(),
          "Could not allocate expression value", &input_val));
      input_vals->push_back(input_val);
    }
  }

  // Only evaluate constant arguments at the top level of function contexts.
  // If 'ctx' was cloned, the constant values were copied from the parent.
  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    vector<AnyVal*> constant_args;
    for (int i = 0; i < children_.size(); ++i) {
      AnyVal* const_val;
      RETURN_IF_ERROR(children_[i]->GetConstVal(state, ctx, &const_val));
      constant_args.push_back(const_val);
    }
    fn_ctx->impl()->SetConstantArgs(move(constant_args));
  }

  if (scalar_fn_wrapper_ == NULL) {
    // Now we have the constant values, cache them so that the interpreted path can
    // call the UDF without reevaluating the arguments. 'staging_input_vals' and
    // 'varargs_buffer' in the FunctionContext are used to pass fixed and variable-length
    // arguments respectively. 'non_constant_args()' in the FunctionContext will contain
    // pointers to the remaining (non-constant) children that are evaluated for every row.
    vector<pair<Expr*, AnyVal*>> non_constant_args;
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
        DCHECK(constant_arg != NULL);
        memcpy(input_arg, constant_arg, arg_bytes);
      } else {
        non_constant_args.emplace_back(children_[i], input_arg);
      }
    }
    fn_ctx->impl()->SetNonConstantArgs(move(non_constant_args));
  }

  if (prepare_fn_ != NULL) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
      prepare_fn_(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
      if (fn_ctx->has_error()) return Status(fn_ctx->error_msg());
    }
    prepare_fn_(fn_ctx, FunctionContext::THREAD_LOCAL);
    if (fn_ctx->has_error()) return Status(fn_ctx->error_msg());
  }

  // If we're calling MathFunctions::RoundUpTo(), we need to set output_scale_, which
  // determines how many decimal places are printed.
  // TODO: revisit this. We should be able to do this if the scale argument is
  // non-constant.
  if (fn_.name.function_name == "round" && type_.type == TYPE_DOUBLE) {
    DCHECK_EQ(children_.size(), 2);
    if (children_[1]->is_constant()) {
      IntVal scale_arg = children_[1]->GetIntVal(ctx, NULL);
      output_scale_ = scale_arg.val;
    }
  }

  return Status::OK();
}

void ScalarFnCall::Close(RuntimeState* state, ExprContext* context,
                         FunctionContext::FunctionStateScope scope) {
  if (fn_context_index_ != -1 && close_fn_ != NULL) {
    FunctionContext* fn_ctx = context->fn_context(fn_context_index_);
    close_fn_(fn_ctx, FunctionContext::THREAD_LOCAL);
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
      close_fn_(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
    }
  }
  Expr::Close(state, context, scope);
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
Status ScalarFnCall::GetCodegendComputeFn(LlvmCodeGen* codegen, Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }
  if (type_.type == TYPE_CHAR) {
    return Status("ScalarFnCall Codegen not supported for CHAR");
  }
  for (int i = 0; i < GetNumChildren(); ++i) {
    if (children_[i]->type().type == TYPE_CHAR) {
      *fn = NULL;
      return Status("ScalarFnCall Codegen not supported for CHAR");
    }
  }

  vector<ColumnType> arg_types;
  for (const Expr* child : children_) arg_types.push_back(child->type());
  Function* udf;
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

  Value* args[2];
  *fn = CreateIrFunctionPrototype(codegen, fn_name.str(), &args);
  Value* expr_ctx = args[0];
  Value* row = args[1];
  BasicBlock* block = BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmBuilder builder(block);

  // Populate UDF arguments
  vector<Value*> udf_args;

  // First argument is always FunctionContext*.
  // Index into our registered offset in the ExprContext.
  Value* expr_ctx_gep = builder.CreateStructGEP(NULL, expr_ctx, 1, "expr_ctx_gep");
  Value* fn_ctxs_base = builder.CreateLoad(expr_ctx_gep, "fn_ctxs_base");
  // Use GEP to add our index to the base pointer
  Value* fn_ctx_ptr =
      builder.CreateConstGEP1_32(fn_ctxs_base, fn_context_index_, "fn_ctx_ptr");
  Value* fn_ctx = builder.CreateLoad(fn_ctx_ptr, "fn_ctx");
  udf_args.push_back(fn_ctx);

  // Allocate a varargs array. The array's entry type is the appropriate AnyVal subclass.
  // E.g. if the vararg type is STRING, and the function is called with 10 arguments, we
  // allocate a StringVal[10] array. We allocate the buffer with Alloca so that LLVM can
  // optimise out the buffer once the function call is inlined.
  Value* varargs_buffer = NULL;
  if (vararg_start_idx_ != -1) {
    Type* unlowered_varargs_type =
        CodegenAnyVal::GetUnloweredType(codegen, VarArgsType());
    varargs_buffer = codegen->CreateEntryBlockAlloca(builder, unlowered_varargs_type,
        NumVarArgs(), FunctionContextImpl::VARARGS_BUFFER_ALIGNMENT, "varargs_buffer");
  }

  // Call children to populate remaining arguments
  for (int i = 0; i < GetNumChildren(); ++i) {
    Function* child_fn = NULL;
    vector<Value*> child_fn_args;
    // Set 'child_fn' to the codegen'd function, sets child_fn = NULL if codegen fails
    children_[i]->GetCodegendComputeFn(codegen, &child_fn);
    if (child_fn == NULL) {
      // Set 'child_fn' to the interpreted function
      child_fn = GetStaticGetValWrapper(children_[i]->type(), codegen);
      // First argument to interpreted function is children_[i]
      Type* expr_ptr_type = codegen->GetPtrType(Expr::LLVM_CLASS_NAME);
      child_fn_args.push_back(codegen->CastPtrToLlvmPtr(expr_ptr_type, children_[i]));
    }
    child_fn_args.push_back(expr_ctx);
    child_fn_args.push_back(row);

    // Call 'child_fn', adding the result to either 'udf_args' or 'varargs_buffer'
    DCHECK(child_fn != NULL);
    Type* arg_type = CodegenAnyVal::GetUnloweredType(codegen, children_[i]->type());
    Value* arg_val_ptr;
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
    Value* lowered_arg_val_ptr = builder.CreateBitCast(arg_val_ptr,
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
    udf_args.push_back(codegen->GetIntConstant(TYPE_INT, NumVarArgs()));
    // Add all the accumulated vararg inputs as one input argument.
    PointerType* vararg_type = CodegenAnyVal::GetUnloweredPtrType(codegen, VarArgsType());
    udf_args.push_back(builder.CreateBitCast(varargs_buffer, vararg_type, "varargs"));
  }

  // Call UDF
  Value* result_val =
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
    Function* ir_fn = codegen->GetFunction(symbol, false);
    if (ir_fn == NULL) {
      stringstream ss;
      ss << "Unable to locate function " << symbol << " from LLVM module "
         << fn_.hdfs_location;
      return Status(ss.str());
    }
    codegen->AddFunctionToJit(ir_fn, fn);
    return Status::OK();
  }
}

void ScalarFnCall::EvaluateNonConstantChildren(
    ExprContext* context, const TupleRow* row) {
  FunctionContext* fn_ctx = context->fn_context(fn_context_index_);
  for (pair<Expr*, AnyVal*> child : fn_ctx->impl()->non_constant_args()) {
    void* val = context->GetValue(child.first, row);
    AnyValUtil::SetAnyVal(val, child.first->type(), child.second);
  }
}

template<typename RETURN_TYPE>
RETURN_TYPE ScalarFnCall::InterpretEval(ExprContext* context, const TupleRow* row) {
  DCHECK(scalar_fn_ != NULL) << DebugString();
  FunctionContext* fn_ctx = context->fn_context(fn_context_index_);
  vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
  EvaluateNonConstantChildren(context, row);

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
   //     return reinterpret_cast<ScalarFnn>(scalar_fn_)(fn_ctx, *(*input_vals)[0], ...,
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

typedef BooleanVal (*BooleanWrapper)(ExprContext*, const TupleRow*);
typedef TinyIntVal (*TinyIntWrapper)(ExprContext*, const TupleRow*);
typedef SmallIntVal (*SmallIntWrapper)(ExprContext*, const TupleRow*);
typedef IntVal (*IntWrapper)(ExprContext*, const TupleRow*);
typedef BigIntVal (*BigIntWrapper)(ExprContext*, const TupleRow*);
typedef FloatVal (*FloatWrapper)(ExprContext*, const TupleRow*);
typedef DoubleVal (*DoubleWrapper)(ExprContext*, const TupleRow*);
typedef StringVal (*StringWrapper)(ExprContext*, const TupleRow*);
typedef TimestampVal (*TimestampWrapper)(ExprContext*, const TupleRow*);
typedef DecimalVal (*DecimalWrapper)(ExprContext*, const TupleRow*);

// TODO: macroify this?
BooleanVal ScalarFnCall::GetBooleanVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<BooleanVal>(context, row);
  BooleanWrapper fn = reinterpret_cast<BooleanWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

TinyIntVal ScalarFnCall::GetTinyIntVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TINYINT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<TinyIntVal>(context, row);
  TinyIntWrapper fn = reinterpret_cast<TinyIntWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

SmallIntVal ScalarFnCall::GetSmallIntVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_SMALLINT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<SmallIntVal>(context, row);
  SmallIntWrapper fn = reinterpret_cast<SmallIntWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

IntVal ScalarFnCall::GetIntVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_INT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<IntVal>(context, row);
  IntWrapper fn = reinterpret_cast<IntWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

BigIntVal ScalarFnCall::GetBigIntVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BIGINT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<BigIntVal>(context, row);
  BigIntWrapper fn = reinterpret_cast<BigIntWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

FloatVal ScalarFnCall::GetFloatVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_FLOAT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<FloatVal>(context, row);
  FloatWrapper fn = reinterpret_cast<FloatWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

DoubleVal ScalarFnCall::GetDoubleVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DOUBLE);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<DoubleVal>(context, row);
  DoubleWrapper fn = reinterpret_cast<DoubleWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

StringVal ScalarFnCall::GetStringVal(ExprContext* context, const TupleRow* row) {
  DCHECK(type_.IsStringType());
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<StringVal>(context, row);
  StringWrapper fn = reinterpret_cast<StringWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

TimestampVal ScalarFnCall::GetTimestampVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<TimestampVal>(context, row);
  TimestampWrapper fn = reinterpret_cast<TimestampWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

DecimalVal ScalarFnCall::GetDecimalVal(ExprContext* context, const TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DECIMAL);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<DecimalVal>(context, row);
  DecimalWrapper fn = reinterpret_cast<DecimalWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

string ScalarFnCall::DebugString() const {
  stringstream out;
  out << "ScalarFnCall(udf_type=" << fn_.binary_type << " location=" << fn_.hdfs_location
      << " symbol_name=" << fn_.scalar_fn.symbol << Expr::DebugString() << ")";
  return out.str();
}

int ScalarFnCall::ComputeVarArgsBufferSize() const {
  for (int i = NumFixedArgs(); i < children_.size(); ++i) {
    // All varargs should have same type.
    DCHECK_EQ(children_[i]->type(), VarArgsType());
  }
  return NumVarArgs() == 0 ? 0 : NumVarArgs() * AnyValUtil::AnyValSize(VarArgsType());
}
