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
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "codegen/llvm-codegen.h"
#include "exprs/udf-util.h"
#include "runtime/lib-cache.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"
#include "util/dynamic-util.h"

using namespace impala;
using namespace impala_udf;
using namespace std;

NativeUdfExpr::NativeUdfExpr(const TExprNode& node)
  : Expr(node),
    udf_context_(FunctionContextImpl::CreateContext()),
    udf_type_(node.udf_call_expr.binary_type),
    hdfs_location_(node.udf_call_expr.binary_location),
    symbol_name_(node.udf_call_expr.symbol_name),
    udf_wrapper_(NULL),
    codegen_(NULL),
    ir_udf_wrapper_(NULL) {
  DCHECK(node.node_type == TExprNodeType::UDF_CALL);
  DCHECK(udf_type_ != TFunctionBinaryType::HIVE);
}

typedef BooleanVal (*BooleanUdfWrapper)(int8_t*, TupleRow*);
typedef TinyIntVal (*TinyIntUdfWrapper)(int8_t*, TupleRow*);
typedef SmallIntVal (*SmallIntUdfWrapper)(int8_t*, TupleRow*);
typedef IntVal (*IntUdfWrapper)(int8_t*, TupleRow*);
typedef BigIntVal (*BigIntUdfWrapper)(int8_t*, TupleRow*);
typedef FloatVal (*FloatUdfWrapper)(int8_t*, TupleRow*);
typedef DoubleVal (*DoubleUdfWrapper)(int8_t*, TupleRow*);
typedef StringVal (*StringUdfWrapper)(int8_t*, TupleRow*);

void* NativeUdfExpr::ComputeFn(Expr* e, TupleRow* row) {
  NativeUdfExpr* udf_expr = reinterpret_cast<NativeUdfExpr*>(e);

  // TODO: This isn't threadsafe. UDF exprs should create their own IR module which they
  // optimize and compile in Prepare() so this won't be necessary.
  if (UNLIKELY(udf_expr->udf_wrapper_ == NULL)) {
    udf_expr->udf_wrapper_ = udf_expr->codegen_->JitFunction(udf_expr->ir_udf_wrapper_);
    if (UNLIKELY(udf_expr->udf_wrapper_ == NULL)) {
      LOG(ERROR) << "Unable to JIT compile UDF wrapper function";
      return NULL;
    }
  }

  switch (e->type()) {
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
    case TYPE_TIMESTAMP: // TODO
    default:
      DCHECK(false) << "Type not implemented: " << e->type();
  }
  return NULL;
}

Status NativeUdfExpr::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  if (state->llvm_codegen() == NULL) {
    return Status("UDFs cannot be evaluated with codegen disabled");
  }
  codegen_ = state->llvm_codegen();
  RETURN_IF_ERROR(Expr::PrepareChildren(state, desc));
  RETURN_IF_ERROR(GetIrComputeFn(state, &ir_udf_wrapper_));
  compute_fn_ = NativeUdfExpr::ComputeFn;
  return Status::OK;
}

// Dynamically loads the pre-compiled UDF and codegens a function that calls each child's
// codegen'd function, then passes those values to the UDF and returns the result.
// Example generated IR for a UDF with signature
// SmallIntVal Identity(FunctionContext*, SmallIntVal*):
//
// define i32 @UdfWrapper(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   %arg_val = call i32 @ExprWrapper(i8* %context, %"class.impala::TupleRow"* %row)
//   %arg_ptr = alloca i32
//   store i32 %arg_val, i32* %arg_ptr
//   %result = call i32 @_Z8IdentityPN10impala_udf15FunctionContextERKNS_11SmallIntValE(
//      %"class.impala_udf::FunctionContext"* inttoptr
//         (i64 51760208 to %"class.impala_udf::FunctionContext"*),
//      i32* %arg_ptr)
//   ret i32 %result
// }
Status NativeUdfExpr::GetIrComputeFn(RuntimeState* state, llvm::Function** fn) {
  LlvmCodeGen* codegen = state->llvm_codegen();
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
  for (int i = 0; i < children_.size(); ++i, ++arg) {
    llvm::Function* child_fn;
    RETURN_IF_ERROR(children_[i]->GetIrComputeFn(state, &child_fn));
    DCHECK(child_fn != NULL);

    llvm::Value* arg_val = builder.CreateCall(child_fn, args, "arg_val");
    llvm::Value* arg_ptr = builder.CreateAlloca(child_fn->getReturnType(), 0, "arg_ptr");
    builder.CreateStore(arg_val, arg_ptr);

    // The *Val type returned by child_fn will be likely be lowered to a simpler type, so
    // we must cast arg_ptr to the actual *Val struct pointer type expected by the UDF.
    llvm::Value* cast_arg_ptr =
        builder.CreateBitCast(arg_ptr, arg->getType(), "cast_arg_ptr");
    udf_args.push_back(cast_arg_ptr);
  }

  // Call UDF
  llvm::Value* result_val = builder.CreateCall(udf, udf_args, "result");
  builder.CreateRet(result_val);

  *fn = codegen->FinalizeFunction(*fn);
  DCHECK(*fn != NULL);
  return Status::OK;
}

Status NativeUdfExpr::GetUdf(RuntimeState* state, llvm::Function** udf) {
  LlvmCodeGen* codegen = state->llvm_codegen();

  if (udf_type_ == TFunctionBinaryType::NATIVE) {
    void* udf_ptr;
    RETURN_IF_ERROR(state->lib_cache()->GetFunctionPtr(
        state, hdfs_location_, symbol_name_, &udf_ptr));

    // Convert UDF function pointer to llvm::Function*
    // First generate the llvm::FunctionType* corresponding to the UDF.
    llvm::Type* return_type = CodegenAnyVal::GetType(codegen, type());
    vector<llvm::Type*> arg_types;
    arg_types.push_back(codegen->GetPtrType("class.impala_udf::FunctionContext"));
    for (int i = 0; i < children_.size(); ++i) {
      llvm::Type* child_return_type =
          CodegenAnyVal::GetType(codegen, children_[i]->type());
      arg_types.push_back(llvm::PointerType::get(child_return_type, 0));
    }
    llvm::FunctionType* udf_type = llvm::FunctionType::get(return_type, arg_types, false);

    // Create a llvm::Function* with the generated type. This is only a function
    // declaration, not a definition, since we do not create any basic blocks or
    // instructions in it.
    *udf = llvm::Function::Create(
        udf_type, llvm::GlobalValue::ExternalLinkage, symbol_name_, codegen->module());

    // Associate the dynamically loaded function pointer with the Function* we
    // defined. This tells LLVM where the compiled function definition is located in
    // memory.
    codegen->execution_engine()->addGlobalMapping(*udf, udf_ptr);
  } else {
    DCHECK_EQ(udf_type_, TFunctionBinaryType::IR);

    // Link the UDF module into this query's main module (essentially copy the UDF module
    // into the main module) so the UDF function is available for inlining in the main
    // module.
    RETURN_IF_ERROR(codegen->LinkModule(hdfs_location_, state->fs_cache()));
    *udf = codegen->module()->getFunction(symbol_name_);
    if (*udf == NULL) {
      stringstream ss;
      ss << "Unable to locate function " << symbol_name_
         << " from LLVM module " << hdfs_location_;
      return Status(ss.str());
    }
  }
  return Status::OK;
}

string NativeUdfExpr::DebugString() const {
  stringstream out;
  out << "NativeUdfExpr(udf_type=" << udf_type_ << " location=" << hdfs_location_
      << " symbol_name=" << symbol_name_ << Expr::DebugString() << ")";
  return out.str();
}
