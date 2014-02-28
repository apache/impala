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

#include "exprs/native-udf-expr.h"

#include <vector>
#include <llvm/IR/Attributes.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "codegen/llvm-codegen.h"
#include "exprs/anyval-util.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"
#include "util/dynamic-util.h"

using namespace impala;
using namespace impala_udf;
using namespace std;

NativeUdfExpr::NativeUdfExpr(const TExprNode& node)
  : Expr(node),
    vararg_start_idx_(node.__isset.vararg_start_idx ?
        node.vararg_start_idx : -1),
    udf_wrapper_(NULL),
    varargs_input_(NULL) {
  is_udf_call_ = true;
  DCHECK_EQ(node.node_type, TExprNodeType::FUNCTION_CALL);
  DCHECK_NE(fn_.binary_type, TFunctionBinaryType::HIVE);
}

NativeUdfExpr::~NativeUdfExpr() {
  delete[] varargs_input_;
}

typedef BooleanVal (*BooleanUdfWrapper)(int8_t*, TupleRow*);
typedef TinyIntVal (*TinyIntUdfWrapper)(int8_t*, TupleRow*);
typedef SmallIntVal (*SmallIntUdfWrapper)(int8_t*, TupleRow*);
typedef IntVal (*IntUdfWrapper)(int8_t*, TupleRow*);
typedef BigIntVal (*BigIntUdfWrapper)(int8_t*, TupleRow*);
typedef FloatVal (*FloatUdfWrapper)(int8_t*, TupleRow*);
typedef DoubleVal (*DoubleUdfWrapper)(int8_t*, TupleRow*);
typedef StringVal (*StringUdfWrapper)(int8_t*, TupleRow*);
typedef TimestampVal (*TimestampUdfWrapper)(int8_t*, TupleRow*);

void* NativeUdfExpr::ComputeFn(Expr* e, TupleRow* row) {
  NativeUdfExpr* udf_expr = reinterpret_cast<NativeUdfExpr*>(e);
  switch (e->type().type) {
    case TYPE_BOOLEAN: {
      BooleanUdfWrapper fn = reinterpret_cast<BooleanUdfWrapper>(udf_expr->udf_wrapper_);
      BooleanVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.bool_val = v.val;
      return &e->result_.bool_val;
    }
    case TYPE_TINYINT: {
      TinyIntUdfWrapper fn = reinterpret_cast<TinyIntUdfWrapper>(udf_expr->udf_wrapper_);
      TinyIntVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.tinyint_val = v.val;
      return &e->result_.tinyint_val;
    }
    case TYPE_SMALLINT: {
      SmallIntUdfWrapper fn =
          reinterpret_cast<SmallIntUdfWrapper>(udf_expr->udf_wrapper_);
      SmallIntVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.smallint_val = v.val;
      return &e->result_.smallint_val;
    }
    case TYPE_INT: {
      IntUdfWrapper fn = reinterpret_cast<IntUdfWrapper>(udf_expr->udf_wrapper_);
      IntVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.int_val = v.val;
      return &e->result_.int_val;
    }
    case TYPE_BIGINT: {
      BigIntUdfWrapper fn = reinterpret_cast<BigIntUdfWrapper>(udf_expr->udf_wrapper_);
      BigIntVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.bigint_val = v.val;
      return &e->result_.bigint_val;
    }
    case TYPE_FLOAT: {
      FloatUdfWrapper fn = reinterpret_cast<FloatUdfWrapper>(udf_expr->udf_wrapper_);
      FloatVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.float_val = v.val;
      return &e->result_.float_val;
    }
    case TYPE_DOUBLE: {
      DoubleUdfWrapper fn = reinterpret_cast<DoubleUdfWrapper>(udf_expr->udf_wrapper_);
      DoubleVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.double_val = v.val;
      return &e->result_.double_val;
    }
    case TYPE_STRING: {
      StringUdfWrapper fn = reinterpret_cast<StringUdfWrapper>(udf_expr->udf_wrapper_);
      StringVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.string_val.ptr = reinterpret_cast<char*>(v.ptr);
      e->result_.string_val.len = v.len;
      return &e->result_.string_val;
    }
    case TYPE_TIMESTAMP: {
      TimestampUdfWrapper fn =
          reinterpret_cast<TimestampUdfWrapper>(udf_expr->udf_wrapper_);
      TimestampVal v = fn(NULL, row);
      if (v.is_null) return NULL;
      e->result_.timestamp_val = TimestampValue::FromTimestampVal(v);
      return &e->result_.timestamp_val;
    }
    default:
      DCHECK(false) << "Type not implemented: " << e->type();
  }
  return NULL;
}

Status NativeUdfExpr::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, desc));
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

  vector<FunctionContext::TypeDesc> arg_types(children_.size());
  for (int i = 0; i < children_.size(); ++i) {
    AnyValUtil::ColumnTypeToTypeDesc(children_[i]->type_, &arg_types[i]);
  }

  // TODO: this should come from the ExprContext
  udf_context_.reset(
      FunctionContextImpl::CreateContext(state, state->udf_pool(), arg_types));

  if (vararg_start_idx_ != -1) {
    // Allocate a scratch buffer for all the variable args.
    int var_args_buffer_size = 0;
    DCHECK_GT(GetNumChildren(), vararg_start_idx_);
    for (int i = vararg_start_idx_; i < GetNumChildren(); ++i) {
      var_args_buffer_size += AnyValUtil::AnyValSize(children_[i]->type());
    }
    varargs_input_ = new uint8_t[var_args_buffer_size];
  }

  llvm::Function* ir_udf_wrapper;
  RETURN_IF_ERROR(GetIrComputeFn(state, &ir_udf_wrapper));
  DCHECK(state->codegen() != NULL);
  state->codegen()->AddFunctionToJit(ir_udf_wrapper, &udf_wrapper_);
  compute_fn_ = ComputeFn;
  return Status::OK;
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
Status NativeUdfExpr::GetIrComputeFn(RuntimeState* state, llvm::Function** fn) {
  // Udfs always require some amount of codegen.
  if (state->codegen() == NULL) state->CreateCodegen();
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);
  llvm::Function* udf;
  RETURN_IF_ERROR(GetUdf(state, &udf));

  // Create wrapper that computes args and calls UDF
  llvm::Value* args[2];
  *fn = CreateIrFunctionPrototype(codegen, "UdfWrapper", &args);
  llvm::BasicBlock* block = llvm::BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmCodeGen::LlvmBuilder builder(block);

  // First argument is always FunctionContext*
  vector<llvm::Value*> udf_args;
  udf_args.push_back(codegen->CastPtrToLlvmPtr(
      codegen->GetPtrType("class.impala_udf::FunctionContext"), udf_context_.get()));

  // Call children to populate remaining arguments
  llvm::Function::arg_iterator arg = udf->arg_begin();
  ++arg; // Skip FunctionContext* arg
  int varargs_input_offset = 0;
  for (int i = 0; i < GetNumChildren(); ++i) {
    if (vararg_start_idx_ == i) {
      // This is the start of the varargs, first add the number of args.
      udf_args.push_back(codegen->GetIntConstant(
          TYPE_INT, GetNumChildren() - vararg_start_idx_));
      ++arg;
    }

    llvm::Function* child_fn;
    if (state->codegen_enabled()) {
      // We want to do as much codegen as possible, so get the child functions
      // as IR.
      RETURN_IF_ERROR(children_[i]->GetIrComputeFn(state, &child_fn));
    } else {
      // Codegen is disabled, so use the wrapper which just calls GetValue(),
      // which goes back to the interpreted path.
      RETURN_IF_ERROR(
          children_[i]->GetWrapperIrComputeFunction(codegen, &child_fn));
    }

    DCHECK(child_fn != NULL);
    llvm::Value* arg_val = builder.CreateCall(child_fn, args, "arg_val");

    if (vararg_start_idx_ == -1 || i < vararg_start_idx_) {
      // Either no varargs or arguments before varargs begin.
      llvm::Value* arg_ptr = builder.CreateAlloca(
          child_fn->getReturnType(), 0, "arg_ptr");
      builder.CreateStore(arg_val, arg_ptr);

      // The *Val type returned by child_fn will be likely be lowered to a simpler type,
      // so we must cast arg_ptr to the actual *Val struct pointer type expected by the
      // UDF.
      llvm::Value* cast_arg_ptr =
          builder.CreateBitCast(arg_ptr, arg->getType(), "cast_arg_ptr");
      udf_args.push_back(cast_arg_ptr);
      ++arg;
    } else {
      // Store the result of child(i) in varargs_input_ + varargs_input_offset
      llvm::Type* arg_ptr_type = llvm::PointerType::get(arg_val->getType(), 0);
      llvm::Value* arg_ptr = codegen->CastPtrToLlvmPtr(
          arg_ptr_type, varargs_input_ + varargs_input_offset);
      builder.CreateStore(arg_val, arg_ptr);
      varargs_input_offset += AnyValUtil::AnyValSize(children_[i]->type());
    }
  }

  if (vararg_start_idx_ != -1) {
    // Add all the accumulated vararg inputs as one input argument.
    udf_args.push_back(codegen->CastPtrToLlvmPtr(arg->getType(), varargs_input_));
  }

  // Call UDF
  llvm::Value* result_val = builder.CreateCall(udf, udf_args, "result");
  builder.CreateRet(result_val);

  (*fn)->addFnAttr(llvm::Attribute::AlwaysInline);
  *fn = codegen->FinalizeFunction(*fn);
  DCHECK(*fn != NULL);
  return Status::OK;
}

Status NativeUdfExpr::GetUdf(RuntimeState* state, llvm::Function** udf) {
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);
  if (fn_.binary_type == TFunctionBinaryType::NATIVE ||
      (fn_.binary_type == TFunctionBinaryType::BUILTIN && !state->codegen_enabled())) {
    // In this path, we are code that has been statically compiled to assembly.
    // This can either be a UDF implemented in a .so or a builtin using the UDF
    // interface with the code in impalad.
    void* fn_ptr;
    Status status = LibCache::instance()->GetSoFunctionPtr(
        fn_.hdfs_location, fn_.scalar_fn.symbol, &fn_ptr, &cache_entry_);
    if (!status.ok() && fn_.binary_type == TFunctionBinaryType::BUILTIN) {
      // Builtins symbols should exist unless there is a version mismatch.
      stringstream ss;
      ss << "Builtin '" << fn_.name.function_name << "' with symbol '"
         << fn_.scalar_fn.symbol << "' does not exist. "
         << "Verify that all your impalads are the same version.";
      status.AddErrorMsg(ss.str());
    }
    RETURN_IF_ERROR(status);
    DCHECK(fn_ptr != NULL);

    // Convert UDF function pointer to llvm::Function*
    // First generate the llvm::FunctionType* corresponding to the UDF.
    llvm::Type* return_type = CodegenAnyVal::GetType(codegen, type());
    vector<llvm::Type*> arg_types;
    arg_types.push_back(codegen->GetPtrType("class.impala_udf::FunctionContext"));
    int num_fixed_args = vararg_start_idx_ >= 0 ? vararg_start_idx_ : children_.size();
    for (int i = 0; i < num_fixed_args; ++i) {
      llvm::Type* child_return_type =
          CodegenAnyVal::GetType(codegen, children_[i]->type());
      arg_types.push_back(llvm::PointerType::get(child_return_type, 0));
    }

    if (vararg_start_idx_ >= 0) {
      llvm::Type* vararg_return_type =
          CodegenAnyVal::GetType(codegen, children_[vararg_start_idx_]->type());
      arg_types.push_back(codegen->GetType(TYPE_INT));
      arg_types.push_back(llvm::PointerType::get(vararg_return_type, 0));
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
  } else {
    // We're running a IR UDF.
    DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::IR);
    string local_path;
    RETURN_IF_ERROR(LibCache::instance()->GetLocalLibPath(
        fn_.hdfs_location, LibCache::TYPE_IR, &local_path));

    // Link the UDF module into this query's main module (essentially copy the UDF module
    // into the main module) so the UDF function is available for inlining in the main
    // module.
    RETURN_IF_ERROR(codegen->LinkModule(local_path));
    *udf = codegen->module()->getFunction(fn_.scalar_fn.symbol);
    if (*udf == NULL) {
      stringstream ss;
      ss << "Unable to locate function " << fn_.scalar_fn.symbol
         << " from LLVM module " << fn_.hdfs_location;
      return Status(ss.str());
    }
  }
  return Status::OK;
}

string NativeUdfExpr::DebugString() const {
  stringstream out;
  out << "NativeUdfExpr(udf_type=" << fn_.binary_type
      << " location=" << fn_.hdfs_location
      << " symbol_name=" << fn_.scalar_fn.symbol << Expr::DebugString() << ")";
  return out.str();
}
