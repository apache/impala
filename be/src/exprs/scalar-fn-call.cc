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

#include "exprs/scalar-fn-call.h"

#include <vector>
#include <gutil/strings/substitute.h>
#include <llvm/IR/Attributes.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>

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
#include "util/dynamic-util.h"
#include "util/symbols-util.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;
using namespace strings;

ScalarFnCall::ScalarFnCall(const TExprNode& node)
  : Expr(node),
    vararg_start_idx_(node.__isset.vararg_start_idx ?
        node.vararg_start_idx : -1),
    scalar_fn_wrapper_(NULL),
    prepare_fn_(NULL),
    close_fn_(NULL),
    scalar_fn_(NULL) {
  DCHECK_NE(fn_.binary_type, TFunctionBinaryType::JAVA);
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

  FunctionContext::TypeDesc return_type = AnyValUtil::ColumnTypeToTypeDesc(type_);
  vector<FunctionContext::TypeDesc> arg_types;
  bool char_arg = false;
  for (int i = 0; i < children_.size(); ++i) {
    arg_types.push_back(AnyValUtil::ColumnTypeToTypeDesc(children_[i]->type_));
    char_arg = char_arg || (children_[i]->type_.type == TYPE_CHAR);
  }

  // Compute buffer size for varargs
  int varargs_buffer_size = 0;
  if (vararg_start_idx_ != -1) {
    DCHECK_GT(GetNumChildren(), vararg_start_idx_);
    for (int i = vararg_start_idx_; i < GetNumChildren(); ++i) {
      varargs_buffer_size += AnyValUtil::AnyValSize(children_[i]->type());
    }
  }

  fn_context_index_ = context->Register(state, return_type, arg_types,
      varargs_buffer_size);

  // Use the interpreted path and call the builtin without codegen if:
  // 1. there are char arguments (as they aren't supported yet)
  // OR
  // if all of the following conditions are satisfied:
  // 2. the codegen object hasn't been created yet.
  // 3. the planner doesn't insist on using codegen.
  // 4. we're calling a builtin or native UDF with <= 8 non-variadic arguments.
  //    The templates for UDFs used in the interpretation path support up to 8
  //    arguments only.
  //
  // This saves us the overhead of creating the codegen object when it's not necessary
  // (i.e., in plan fragments with no codegen-enabled operators).
  //
  // TODO: codegen for char arguments
  // TODO: remove condition 2 above and put a flag in the RuntimeState to indicate
  // if codegen should be enabled for the entire fragment.
  bool skip_codegen = false;
  if (char_arg) {
    skip_codegen = true;
  } else if (!state->codegen_created() && !state->ShouldCodegenExpr()) {
    skip_codegen = fn_.binary_type != TFunctionBinaryType::IR && NumFixedArgs() <= 8;
  }
  if (skip_codegen) {
    // Builtins with char arguments must still have <= 8 arguments.
    // TODO: delete when we have codegen for char arguments
    if (char_arg) {
      DCHECK(NumFixedArgs() <= 8 && fn_.binary_type == TFunctionBinaryType::BUILTIN);
    }
    Status status = LibCache::instance()->GetSoFunctionPtr(
        fn_.hdfs_location, fn_.scalar_fn.symbol, &scalar_fn_, &cache_entry_);
    if (!status.ok()) {
      if (fn_.binary_type == TFunctionBinaryType::BUILTIN) {
        // Builtins symbols should exist unless there is a version mismatch.
        status.SetErrorMsg(ErrorMsg(TErrorCode::MISSING_BUILTIN,
            fn_.name.function_name, fn_.scalar_fn.symbol));
        return status;
      } else {
        DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::NATIVE);
        return Status(Substitute("Problem loading UDF '$0':\n$1",
            fn_.name.function_name, status.GetDetail()));
        return status;
      }
    }
  } else {
    // If we got here, either codegen is enabled or we need codegen to run this function.
    LlvmCodeGen* codegen;
    RETURN_IF_ERROR(state->GetCodegen(&codegen));

    if (fn_.binary_type == TFunctionBinaryType::IR) {
      string local_path;
      RETURN_IF_ERROR(LibCache::instance()->GetLocalLibPath(
          fn_.hdfs_location, LibCache::TYPE_IR, &local_path));
      // Link the UDF module into this query's main module (essentially copy the UDF
      // module into the main module) so the UDF's functions are available in the main
      // module.
      RETURN_IF_ERROR(codegen->LinkModule(local_path));
    }

    llvm::Function* ir_udf_wrapper;
    RETURN_IF_ERROR(GetCodegendComputeFn(state, &ir_udf_wrapper));
    // TODO: don't do this for child exprs
    codegen->AddFunctionToJit(ir_udf_wrapper, &scalar_fn_wrapper_);
  }

  if (fn_.scalar_fn.__isset.prepare_fn_symbol) {
    RETURN_IF_ERROR(GetFunction(state, fn_.scalar_fn.prepare_fn_symbol,
        reinterpret_cast<void**>(&prepare_fn_)));
  }
  if (fn_.scalar_fn.__isset.close_fn_symbol) {
    RETURN_IF_ERROR(GetFunction(state, fn_.scalar_fn.close_fn_symbol,
        reinterpret_cast<void**>(&close_fn_)));
  }

  return Status::OK();
}

Status ScalarFnCall::Open(RuntimeState* state, ExprContext* ctx,
                          FunctionContext::FunctionStateScope scope) {
  // Opens and inits children
  RETURN_IF_ERROR(Expr::Open(state, ctx, scope));
  FunctionContext* fn_ctx = ctx->fn_context(fn_context_index_);

  if (scalar_fn_ != NULL) {
    // We're in the interpreted path (i.e. no JIT). Populate our FunctionContext's
    // staging_input_vals, which will be reused across calls to scalar_fn_.
    DCHECK(scalar_fn_wrapper_ == NULL);
    ObjectPool* obj_pool = state->obj_pool();
    vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
    for (int i = 0; i < NumFixedArgs(); ++i) {
      input_vals->push_back(CreateAnyVal(obj_pool, children_[i]->type()));
    }
  }

  // Only evaluate constant arguments once per fragment
  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    vector<AnyVal*> constant_args;
    for (int i = 0; i < children_.size(); ++i) {
      constant_args.push_back(children_[i]->GetConstVal(ctx));
      // Check if any errors were set during the GetConstVal() call
      Status child_status = children_[i]->GetFnContextError(ctx);
      if (!child_status.ok()) return child_status;
    }
    fn_ctx->impl()->SetConstantArgs(constant_args);
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
    if (children_[1]->IsConstant()) {
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

bool ScalarFnCall::IsConstant() const {
  if (fn_.name.function_name == "rand") return false;
  return Expr::IsConstant();
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
Status ScalarFnCall::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }
  for (int i = 0; i < GetNumChildren(); ++i) {
    if (children_[i]->type().type == TYPE_CHAR) {
      *fn = NULL;
      return Status("ScalarFnCall Codegen not supported for CHAR");
    }
  }

  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));

  llvm::Function* udf;
  RETURN_IF_ERROR(GetUdf(state, &udf));

  // Create wrapper that computes args and calls UDF
  stringstream fn_name;
  fn_name << udf->getName().str() << "Wrapper";

  llvm::Value* args[2];
  *fn = CreateIrFunctionPrototype(codegen, fn_name.str(), &args);
  llvm::Value* expr_ctx = args[0];
  llvm::Value* row = args[1];
  llvm::BasicBlock* block = llvm::BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmCodeGen::LlvmBuilder builder(block);

  // Populate UDF arguments
  vector<llvm::Value*> udf_args;

  // First argument is always FunctionContext*.
  // Index into our registered offset in the ExprContext.
  llvm::Value* expr_ctx_gep = builder.CreateStructGEP(expr_ctx, 1, "expr_ctx_gep");
  llvm::Value* fn_ctxs_base = builder.CreateLoad(expr_ctx_gep, "fn_ctxs_base");
  // Use GEP to add our index to the base pointer
  llvm::Value* fn_ctx_ptr =
      builder.CreateConstGEP1_32(fn_ctxs_base, fn_context_index_, "fn_ctx_ptr");
  llvm::Value* fn_ctx = builder.CreateLoad(fn_ctx_ptr, "fn_ctx");
  udf_args.push_back(fn_ctx);

  // Get IR i8* pointer to varargs buffer from FunctionContext* argument
  // (if there are varargs)
  llvm::Value* varargs_buffer = NULL;
  if (vararg_start_idx_ != -1) {
    // FunctionContextImpl is first field of FunctionContext
    // fn_ctx_impl_ptr has type FunctionContextImpl**
    llvm::Value* fn_ctx_impl_ptr = builder.CreateStructGEP(fn_ctx, 0, "fn_ctx_impl_ptr");
    llvm::Value* fn_ctx_impl = builder.CreateLoad(fn_ctx_impl_ptr, "fn_ctx_impl");
    // varargs_buffer is first field of FunctionContextImpl
    // varargs_buffer_ptr has type i8**
    llvm::Value* varargs_buffer_ptr =
        builder.CreateStructGEP(fn_ctx_impl, 0, "varargs_buffer");
    varargs_buffer = builder.CreateLoad(varargs_buffer_ptr);
  }
  // Tracks where to write the next vararg to
  int varargs_buffer_offset = 0;

  // Call children to populate remaining arguments
  for (int i = 0; i < GetNumChildren(); ++i) {
    llvm::Function* child_fn = NULL;
    vector<llvm::Value*> child_fn_args;
    if (state->codegen_enabled()) {
      // Set 'child_fn' to the codegen'd function, sets child_fn = NULL if codegen fails
      children_[i]->GetCodegendComputeFn(state, &child_fn);
    }
    if (child_fn == NULL) {
      // Set 'child_fn' to the interpreted function
      child_fn = GetStaticGetValWrapper(children_[i]->type(), codegen);
      // First argument to interpreted function is children_[i]
      llvm::Type* expr_ptr_type = codegen->GetPtrType(Expr::LLVM_CLASS_NAME);
      child_fn_args.push_back(codegen->CastPtrToLlvmPtr(expr_ptr_type, children_[i]));
    }
    child_fn_args.push_back(expr_ctx);
    child_fn_args.push_back(row);

    // Call 'child_fn', adding the result to either 'udf_args' or 'varargs_buffer'
    DCHECK(child_fn != NULL);
    llvm::Type* arg_type = CodegenAnyVal::GetUnloweredType(codegen, children_[i]->type());
    llvm::Value* arg_val_ptr;
    if (vararg_start_idx_ == -1 || i < vararg_start_idx_) {
      // Either no varargs or arguments before varargs begin. Allocate space to store
      // 'child_fn's result so we can pass the pointer to the UDF.
      arg_val_ptr = codegen->CreateEntryBlockAlloca(builder, arg_type, "arg_val_ptr");

      if (children_[i]->type().type == TYPE_DECIMAL) {
        // UDFs may manipulate DecimalVal arguments via SIMD instructions such as 'movaps'
        // that require 16-byte memory alignment. LLVM uses 8-byte alignment by default,
        // so explicitly set the alignment for DecimalVals.
        llvm::cast<llvm::AllocaInst>(arg_val_ptr)->setAlignment(16);
      }
      udf_args.push_back(arg_val_ptr);
     } else {
      // Store the result of 'child_fn' in varargs_buffer + varargs_buffer_offset
      arg_val_ptr =
          builder.CreateConstGEP1_32(varargs_buffer, varargs_buffer_offset, "arg_val_ptr");
      varargs_buffer_offset += AnyValUtil::AnyValSize(children_[i]->type());
      // Cast arg_val_ptr from i8* to AnyVal pointer type
      arg_val_ptr =
          builder.CreateBitCast(arg_val_ptr, arg_type->getPointerTo(), "arg_val_ptr");
    }
    DCHECK_EQ(arg_val_ptr->getType(), arg_type->getPointerTo());
    // The result of the call must be stored in a lowered AnyVal
    llvm::Value* lowered_arg_val_ptr = builder.CreateBitCast(
        arg_val_ptr, CodegenAnyVal::GetLoweredPtrType(codegen, children_[i]->type()),
        "lowered_arg_val_ptr");
    CodegenAnyVal::CreateCall(
        codegen, &builder, child_fn, child_fn_args, "arg_val", lowered_arg_val_ptr);
  }

  if (vararg_start_idx_ != -1) {
    // We've added the FunctionContext argument plus any non-variadic arguments
    DCHECK_EQ(udf_args.size(), vararg_start_idx_ + 1);
    DCHECK_GE(GetNumChildren(), 1);
    // Add the number of varargs
    udf_args.push_back(codegen->GetIntConstant(
        TYPE_INT, GetNumChildren() - vararg_start_idx_));
    // Add all the accumulated vararg inputs as one input argument.
    llvm::PointerType* vararg_type = codegen->GetPtrType(
        CodegenAnyVal::GetUnloweredType(codegen, children_.back()->type()));
    udf_args.push_back(builder.CreateBitCast(varargs_buffer, vararg_type, "varargs"));
  }

  // Call UDF
  llvm::Value* result_val =
      CodegenAnyVal::CreateCall(codegen, &builder, udf, udf_args, "result");
  builder.CreateRet(result_val);

  *fn = codegen->FinalizeFunction(*fn);
  DCHECK(*fn != NULL);
  ir_compute_fn_ = *fn;
  return Status::OK();
}

Status ScalarFnCall::GetUdf(RuntimeState* state, llvm::Function** udf) {
  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));

  // from_utc_timestamp and to_utc_timestamp have inline ASM that cannot be JIT'd.
  // TimestampFunctions::AddSub() contains a try/catch which doesn't work in JIT'd
  // code.  Always use the statically compiled versions of these functions so the
  // xcompiled versions are not included in the final module to be JIT'd.
  // TODO: fix this
  bool broken_builtin = fn_.name.function_name == "from_utc_timestamp" ||
                        fn_.name.function_name == "to_utc_timestamp" ||
                        fn_.scalar_fn.symbol.find("AddSub") != string::npos;
  if (fn_.binary_type == TFunctionBinaryType::NATIVE ||
      (fn_.binary_type == TFunctionBinaryType::BUILTIN &&
       (!state->codegen_enabled() || broken_builtin))) {
    // In this path, we are code that has been statically compiled to assembly.
    // This can either be a UDF implemented in a .so or a builtin using the UDF
    // interface with the code in impalad.
    void* fn_ptr;
    Status status = LibCache::instance()->GetSoFunctionPtr(
        fn_.hdfs_location, fn_.scalar_fn.symbol, &fn_ptr, &cache_entry_);
    if (!status.ok() && fn_.binary_type == TFunctionBinaryType::BUILTIN) {
      // Builtins symbols should exist unless there is a version mismatch.
      status.AddDetail(ErrorMsg(TErrorCode::MISSING_BUILTIN,
              fn_.name.function_name, fn_.scalar_fn.symbol).msg());
    }
    RETURN_IF_ERROR(status);
    DCHECK(fn_ptr != NULL);

    // Convert UDF function pointer to llvm::Function*
    // First generate the llvm::FunctionType* corresponding to the UDF.
    llvm::Type* return_type = CodegenAnyVal::GetLoweredType(codegen, type());
    vector<llvm::Type*> arg_types;

    if (type().type == TYPE_DECIMAL) {
      // Per the x64 ABI, DecimalVals are returned via a DecmialVal* output argument
      return_type = codegen->void_type();
      arg_types.push_back(
          codegen->GetPtrType(CodegenAnyVal::GetUnloweredType(codegen, type())));
    }

    arg_types.push_back(codegen->GetPtrType("class.impala_udf::FunctionContext"));
    for (int i = 0; i < NumFixedArgs(); ++i) {
      llvm::Type* arg_type = codegen->GetPtrType(
          CodegenAnyVal::GetUnloweredType(codegen, children_[i]->type()));
      arg_types.push_back(arg_type);
    }

    if (vararg_start_idx_ >= 0) {
      llvm::Type* vararg_type = CodegenAnyVal::GetUnloweredPtrType(
          codegen, children_[vararg_start_idx_]->type());
      arg_types.push_back(codegen->GetType(TYPE_INT));
      arg_types.push_back(vararg_type);
    }
    llvm::FunctionType* udf_type = llvm::FunctionType::get(return_type, arg_types, false);

    // Create a llvm::Function* with the generated type. This is only a function
    // declaration, not a definition, since we do not create any basic blocks or
    // instructions in it.
    *udf = llvm::Function::Create(
        udf_type, llvm::GlobalValue::ExternalLinkage,
        fn_.scalar_fn.symbol, codegen->module());

    // Associate the dynamically loaded function pointer with the Function* we
    // defined. This tells LLVM where the compiled function definition is located in
    // memory.
    codegen->execution_engine()->addGlobalMapping(*udf, fn_ptr);
  } else if (fn_.binary_type == TFunctionBinaryType::BUILTIN) {
    // In this path, we're running a builtin with the UDF interface. The IR is
    // in the llvm module.
    DCHECK(state->codegen_enabled());
    *udf = codegen->module()->getFunction(fn_.scalar_fn.symbol);
    if (*udf == NULL) {
      // Builtins symbols should exist unless there is a version mismatch.
      stringstream ss;
      ss << "Builtin '" << fn_.name.function_name << "' with symbol '"
         << fn_.scalar_fn.symbol << "' does not exist. "
         << "Verify that all your impalads are the same version.";
      return Status(ss.str());
    }
    // Builtin functions may use Expr::GetConstant(). Clone the function in case we need
    // to use it again, and rename it to something more manageable than the mangled name.
    string demangled_name = SymbolsUtil::DemangleNoArgs((*udf)->getName().str());
    *udf = codegen->CloneFunction(*udf);
    (*udf)->setName(demangled_name);
    InlineConstants(codegen, *udf);
    *udf = codegen->FinalizeFunction(*udf);
    DCHECK(*udf != NULL);
  } else {
    // We're running an IR UDF.
    DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::IR);
    *udf = codegen->module()->getFunction(fn_.scalar_fn.symbol);
    if (*udf == NULL) {
      stringstream ss;
      ss << "Unable to locate function " << fn_.scalar_fn.symbol
         << " from LLVM module " << fn_.hdfs_location;
      return Status(ss.str());
    }
    *udf = codegen->FinalizeFunction(*udf);
    if (*udf == NULL) {
      return Status(
          TErrorCode::UDF_VERIFY_FAILED, fn_.scalar_fn.symbol, fn_.hdfs_location);
    }
  }
  return Status::OK();
}

Status ScalarFnCall::GetFunction(RuntimeState* state, const string& symbol, void** fn) {
  if (fn_.binary_type == TFunctionBinaryType::NATIVE ||
      fn_.binary_type == TFunctionBinaryType::BUILTIN) {
    return LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location, symbol, fn,
                                                  &cache_entry_);
  } else {
    DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::IR);
    LlvmCodeGen* codegen;
    RETURN_IF_ERROR(state->GetCodegen(&codegen));
    llvm::Function* ir_fn = codegen->module()->getFunction(symbol);
    if (ir_fn == NULL) {
      stringstream ss;
      ss << "Unable to locate function " << symbol
         << " from LLVM module " << fn_.hdfs_location;
      return Status(ss.str());
    }
    codegen->AddFunctionToJit(ir_fn, fn);
    return Status::OK();
  }
}

void ScalarFnCall::EvaluateChildren(ExprContext* context, TupleRow* row,
                                    vector<AnyVal*>* input_vals) {
  DCHECK_EQ(input_vals->size(), NumFixedArgs());
  FunctionContext* fn_ctx = context->fn_context(fn_context_index_);
  uint8_t* varargs_buffer = fn_ctx->impl()->varargs_buffer();
  for (int i = 0; i < children_.size(); ++i) {
    void* src_slot = context->GetValue(children_[i], row);
    AnyVal* dst_val;
    if (vararg_start_idx_ == -1 || i < vararg_start_idx_) {
      dst_val = (*input_vals)[i];
    } else {
      dst_val = reinterpret_cast<AnyVal*>(varargs_buffer);
      varargs_buffer += AnyValUtil::AnyValSize(children_[i]->type());
    }
    AnyValUtil::SetAnyVal(src_slot, children_[i]->type(), dst_val);
  }
}

template<typename RETURN_TYPE>
RETURN_TYPE ScalarFnCall::InterpretEval(ExprContext* context, TupleRow* row) {
  DCHECK(scalar_fn_ != NULL);
  FunctionContext* fn_ctx = context->fn_context(fn_context_index_);
  vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
  EvaluateChildren(context, row, input_vals);

  if (vararg_start_idx_ == -1) {
    switch (children_.size()) {
      case 0:
        typedef RETURN_TYPE (*ScalarFn0)(FunctionContext*);
        return reinterpret_cast<ScalarFn0>(scalar_fn_)(fn_ctx);
      case 1:
        typedef RETURN_TYPE (*ScalarFn1)(FunctionContext*, const AnyVal& a1);
        return reinterpret_cast<ScalarFn1>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0]);
      case 2:
        typedef RETURN_TYPE (*ScalarFn2)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2);
        return reinterpret_cast<ScalarFn2>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1]);
      case 3:
        typedef RETURN_TYPE (*ScalarFn3)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3);
        return reinterpret_cast<ScalarFn3>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2]);
      case 4:
        typedef RETURN_TYPE (*ScalarFn4)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4);
        return reinterpret_cast<ScalarFn4>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3]);
      case 5:
        typedef RETURN_TYPE (*ScalarFn5)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, const AnyVal& a5);
        return reinterpret_cast<ScalarFn5>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            *(*input_vals)[4]);
      case 6:
        typedef RETURN_TYPE (*ScalarFn6)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
            const AnyVal& a6);
        return reinterpret_cast<ScalarFn6>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            *(*input_vals)[4], *(*input_vals)[5]);
      case 7:
        typedef RETURN_TYPE (*ScalarFn7)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
            const AnyVal& a6, const AnyVal& a7);
        return reinterpret_cast<ScalarFn7>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            *(*input_vals)[4], *(*input_vals)[5], *(*input_vals)[6]);
      case 8:
        typedef RETURN_TYPE (*ScalarFn8)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
            const AnyVal& a6, const AnyVal& a7, const AnyVal& a8);
        return reinterpret_cast<ScalarFn8>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            *(*input_vals)[4], *(*input_vals)[5], *(*input_vals)[6], *(*input_vals)[7]);
      default:
        DCHECK(false) << "Interpreted path not implemented. We should have "
                      << "codegen'd the wrapper";
    }
   } else {
    int num_varargs = children_.size() - NumFixedArgs();
    const AnyVal* varargs = reinterpret_cast<AnyVal*>(fn_ctx->impl()->varargs_buffer());
    switch (NumFixedArgs()) {
      case 0:
        typedef RETURN_TYPE (*VarargFn0)(FunctionContext*, int num_varargs,
            const AnyVal* varargs);
        return reinterpret_cast<VarargFn0>(scalar_fn_)(fn_ctx, num_varargs, varargs);
      case 1:
        typedef RETURN_TYPE (*VarargFn1)(FunctionContext*, const AnyVal& a1,
            int num_varargs, const AnyVal* varargs);
        return reinterpret_cast<VarargFn1>(scalar_fn_)(fn_ctx, *(*input_vals)[0],
            num_varargs, varargs);
      case 2:
        typedef RETURN_TYPE (*VarargFn2)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, int num_varargs, const AnyVal* varargs);
        return reinterpret_cast<VarargFn2>(scalar_fn_)(fn_ctx, *(*input_vals)[0],
            *(*input_vals)[1], num_varargs, varargs);
      case 3:
        typedef RETURN_TYPE (*VarargFn3)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, int num_varargs, const AnyVal* varargs);
        return reinterpret_cast<VarargFn3>(scalar_fn_)(fn_ctx, *(*input_vals)[0],
            *(*input_vals)[1], *(*input_vals)[2], num_varargs, varargs);
      case 4:
        typedef RETURN_TYPE (*VarargFn4)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, int num_varargs,
            const AnyVal* varargs);
        return reinterpret_cast<VarargFn4>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            num_varargs, varargs);
      case 5:
        typedef RETURN_TYPE (*VarargFn5)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
            int num_varargs, const AnyVal* varargs);
        return reinterpret_cast<VarargFn5>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            *(*input_vals)[4], num_varargs, varargs);
      case 6:
        typedef RETURN_TYPE (*VarargFn6)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
            const AnyVal& a6, int num_varargs, const AnyVal* varargs);
        return reinterpret_cast<VarargFn6>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            *(*input_vals)[4], *(*input_vals)[5], num_varargs, varargs);
      case 7:
        typedef RETURN_TYPE (*VarargFn7)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
            const AnyVal& a6, const AnyVal& a7, int num_varargs, const AnyVal* varargs);
        return reinterpret_cast<VarargFn7>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            *(*input_vals)[4], *(*input_vals)[5], *(*input_vals)[6], num_varargs, varargs);
      case 8:
        typedef RETURN_TYPE (*VarargFn8)(FunctionContext*, const AnyVal& a1,
            const AnyVal& a2, const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
            const AnyVal& a6, const AnyVal& a7, const AnyVal& a8, int num_varargs,
            const AnyVal* varargs);
        return reinterpret_cast<VarargFn8>(scalar_fn_)(fn_ctx,
            *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2], *(*input_vals)[3],
            *(*input_vals)[4], *(*input_vals)[5], *(*input_vals)[6], *(*input_vals)[7],
            num_varargs, varargs);
      default:
        DCHECK(false) << "Interpreted path not implemented. We should have "
                      << "codegen'd the wrapper";
    }
  }
  return RETURN_TYPE::null();
}

typedef BooleanVal (*BooleanWrapper)(ExprContext*, TupleRow*);
typedef TinyIntVal (*TinyIntWrapper)(ExprContext*, TupleRow*);
typedef SmallIntVal (*SmallIntWrapper)(ExprContext*, TupleRow*);
typedef IntVal (*IntWrapper)(ExprContext*, TupleRow*);
typedef BigIntVal (*BigIntWrapper)(ExprContext*, TupleRow*);
typedef FloatVal (*FloatWrapper)(ExprContext*, TupleRow*);
typedef DoubleVal (*DoubleWrapper)(ExprContext*, TupleRow*);
typedef StringVal (*StringWrapper)(ExprContext*, TupleRow*);
typedef TimestampVal (*TimestampWrapper)(ExprContext*, TupleRow*);
typedef DecimalVal (*DecimalWrapper)(ExprContext*, TupleRow*);

// TODO: macroify this?
BooleanVal ScalarFnCall::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<BooleanVal>(context, row);
  BooleanWrapper fn = reinterpret_cast<BooleanWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

TinyIntVal ScalarFnCall::GetTinyIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TINYINT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<TinyIntVal>(context, row);
  TinyIntWrapper fn = reinterpret_cast<TinyIntWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

SmallIntVal ScalarFnCall::GetSmallIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_SMALLINT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<SmallIntVal>(context, row);
  SmallIntWrapper fn = reinterpret_cast<SmallIntWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

IntVal ScalarFnCall::GetIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_INT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<IntVal>(context, row);
  IntWrapper fn = reinterpret_cast<IntWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

BigIntVal ScalarFnCall::GetBigIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BIGINT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<BigIntVal>(context, row);
  BigIntWrapper fn = reinterpret_cast<BigIntWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

FloatVal ScalarFnCall::GetFloatVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_FLOAT);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<FloatVal>(context, row);
  FloatWrapper fn = reinterpret_cast<FloatWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

DoubleVal ScalarFnCall::GetDoubleVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DOUBLE);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<DoubleVal>(context, row);
  DoubleWrapper fn = reinterpret_cast<DoubleWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

StringVal ScalarFnCall::GetStringVal(ExprContext* context, TupleRow* row) {
  DCHECK(type_.IsStringType());
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<StringVal>(context, row);
  StringWrapper fn = reinterpret_cast<StringWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

TimestampVal ScalarFnCall::GetTimestampVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<TimestampVal>(context, row);
  TimestampWrapper fn = reinterpret_cast<TimestampWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

DecimalVal ScalarFnCall::GetDecimalVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DECIMAL);
  DCHECK(context != NULL);
  if (scalar_fn_wrapper_ == NULL) return InterpretEval<DecimalVal>(context, row);
  DecimalWrapper fn = reinterpret_cast<DecimalWrapper>(scalar_fn_wrapper_);
  return fn(context, row);
}

string ScalarFnCall::DebugString() const {
  stringstream out;
  out << "ScalarFnCall(udf_type=" << fn_.binary_type
      << " location=" << fn_.hdfs_location
      << " symbol_name=" << fn_.scalar_fn.symbol << Expr::DebugString() << ")";
  return out.str();
}
