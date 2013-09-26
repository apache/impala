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

#include <dlfcn.h>
#include <vector>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "codegen/llvm-codegen.h"
#include "runtime/hdfs-fs-cache.h"
#include "udf/udf-internal.h"
#include "util/dynamic-util.h"
#include "util/hdfs-util.h"

using namespace llvm;
using namespace impala;
using namespace impala_udf;
using namespace std;

DEFINE_string(local_library_dir, "/tmp",
              "Local directory to copy UDF libraries from HDFS into");

NativeUdfExpr::NativeUdfExpr(const TExprNode& node)
  : Expr(node),
    udf_context_(FunctionContextImpl::CreateContext()),
    hdfs_location_(node.udf_call_expr.binary_location),
    symbol_name_(node.udf_call_expr.symbol_name),
    dl_handle_(NULL),
    udf_wrapper_(NULL) {
  DCHECK(node.node_type == TExprNodeType::UDF_CALL);
}

NativeUdfExpr::~NativeUdfExpr() {
  if (dl_handle_ != NULL) dlclose(dl_handle_);
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
  switch (e->type()) {
    case TYPE_BOOLEAN: {
      BooleanUdfWrapper fn = reinterpret_cast<BooleanUdfWrapper>(udf_expr->udf_wrapper_);
      BooleanVal v = fn(NULL, row);
      e->result_.bool_val = v.val;
      return &e->result_.bool_val;
    }
    case TYPE_TINYINT: {
      TinyIntUdfWrapper fn = reinterpret_cast<TinyIntUdfWrapper>(udf_expr->udf_wrapper_);
      TinyIntVal v = fn(NULL, row);
      e->result_.tinyint_val = v.val;
      return &e->result_.tinyint_val;
    }
    case TYPE_SMALLINT: {
      SmallIntUdfWrapper fn =
          reinterpret_cast<SmallIntUdfWrapper>(udf_expr->udf_wrapper_);
      SmallIntVal v = fn(NULL, row);
      e->result_.smallint_val = v.val;
      return &e->result_.smallint_val;
    }
    case TYPE_INT: {
      IntUdfWrapper fn = reinterpret_cast<IntUdfWrapper>(udf_expr->udf_wrapper_);
      IntVal v = fn(NULL, row);
      e->result_.int_val = v.val;
      return &e->result_.int_val;
    }
    case TYPE_BIGINT: {
      BigIntUdfWrapper fn = reinterpret_cast<BigIntUdfWrapper>(udf_expr->udf_wrapper_);
      BigIntVal v = fn(NULL, row);
      e->result_.bigint_val = v.val;
      return &e->result_.bigint_val;
    }
    case TYPE_FLOAT: {
      FloatUdfWrapper fn = reinterpret_cast<FloatUdfWrapper>(udf_expr->udf_wrapper_);
      FloatVal v = fn(NULL, row);
      e->result_.float_val = v.val;
      return &e->result_.float_val;
    }
    case TYPE_DOUBLE: {
      DoubleUdfWrapper fn = reinterpret_cast<DoubleUdfWrapper>(udf_expr->udf_wrapper_);
      DoubleVal v = fn(NULL, row);
      e->result_.double_val = v.val;
      return &e->result_.double_val;
    }
    case TYPE_STRING: {
      StringUdfWrapper fn = reinterpret_cast<StringUdfWrapper>(udf_expr->udf_wrapper_);
      StringVal v = fn(NULL, row);
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

  RETURN_IF_ERROR(Expr::PrepareChildren(state, desc));

  // Copy the library file from HDFS to the local filesystem
  // TODO: This needs to be moved to a cache so we don't copy the library from HDFS every
  // time the UDF is used.
  hdfsFS hdfs_conn = state->fs_cache()->GetDefaultConnection();
  hdfsFS local_conn = state->fs_cache()->GetLocalConnection();
  RETURN_IF_ERROR(CopyHdfsFile(hdfs_conn, hdfs_location_.c_str(), local_conn,
                               FLAGS_local_library_dir.c_str(), &local_location_));

  LlvmCodeGen* codegen = state->llvm_codegen();
  Function* fn;
  RETURN_IF_ERROR(GetIRComputeFn(state, &fn));
  udf_wrapper_ = codegen->JitFunction(fn);
  if (udf_wrapper_ == NULL) {
    stringstream ss;
    ss << "NativeUdfExpr::Prepare: unable to JIT compile UDF wrapper function\n" << fn;
    return Status(ss.str());
  }
  compute_fn_ = NativeUdfExpr::ComputeFn;
  return Status::OK;
}

// Dynamically loads the pre-compiled UDF and codegens a function that calls each child's
// codegen'd function, then passes those values to the UDF and returns the result.
// Example generated IR:
//
// define %BigIntVal @UdfWrapper(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   %arg_val = call %BigIntVal @ExprWrapper(
//       i8* %context, %"class.impala::TupleRow"* %row)
//   %arg = alloca %BigIntVal
//   store %BigIntVal %arg_val, %BigIntVal* %arg
//   %arg_val1 = call %BigIntVal @ExprWrapper1(
//       i8* %context, %"class.impala::TupleRow"* %row)
//   %arg2 = alloca %BigIntVal
//   store %BigIntVal %arg_val1, %BigIntVal* %arg2
//   %result = call
//       %BigIntVal @_Z6AddUdfPN10impala_udf10FunctionContextERKNS_9BigIntValES4_(
//       %"class.impala_udf::FunctionContext"* inttoptr
//           (i64 58189624 to %"class.impala_udf::FunctionContext"*),
//       %BigIntVal* %arg, %BigIntVal* %arg2)
//   ret %BigIntVal %result
// }
Status NativeUdfExpr::GetIRComputeFn(RuntimeState* state, Function** fn) {
  LlvmCodeGen* codegen = state->llvm_codegen();

  // Dynamically load the UDF
  RETURN_IF_ERROR(DynamicOpen(state, local_location_, RTLD_NOW, &dl_handle_));
  void* udf_ptr;
  RETURN_IF_ERROR(DynamicLookup(state, dl_handle_, symbol_name_.c_str(), &udf_ptr));

  // Convert UDF function pointer to llvm::Function*
  Type* return_type = GetUdfValType(codegen, type_);
  vector<Type*> arg_types;
  arg_types.push_back(codegen->GetPtrType("class.impala_udf::FunctionContext"));
  for (int i = 0; i < children_.size(); ++i) {
    Type* child_return_type = GetUdfValType(codegen, children_[i]->type());
    arg_types.push_back(PointerType::get(child_return_type, 0));
  }
  FunctionType* udf_type = FunctionType::get(return_type, arg_types, false);
  Function* udf = Function::Create(
      udf_type, GlobalValue::ExternalLinkage, symbol_name_, codegen->module());
  codegen->execution_engine()->addGlobalMapping(udf, udf_ptr);

  // Create wrapper that computes args and calls UDF
  Value* args[2];
  *fn = CreateIRFunctionPrototype(codegen, "UdfWrapper", &args);
  BasicBlock* block = BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmCodeGen::LlvmBuilder builder(block);

  // First argument is always FunctionContext*
  vector<Value*> udf_args;
  udf_args.push_back(codegen->CastPtrToLlvmPtr(
      codegen->GetPtrType("class.impala_udf::FunctionContext"), udf_context_.get()));

  // Call children to populate remaining arguments
  for (int i = 0; i < children_.size(); ++i) {
    Function* child_fn;
    RETURN_IF_ERROR(children_[i]->GetIRComputeFn(state, &child_fn));
    DCHECK(child_fn != NULL);
    Value* val = builder.CreateCall(child_fn, args, "arg_val");
    udf_args.push_back(builder.CreateAlloca(child_fn->getReturnType(), 0, "arg"));
    builder.CreateStore(val, udf_args.back());
  }

  // Call UDF
  // Note: I ran into an issue where the calling convention used by the llvm-generated
  // call to this function didn't match the expected cc (the function itself was compiled
  // with gcc). It involved how a StringVal is returned, and was fixed by reducing the
  // memory footprint of StringVal (putting 'len' before 'ptr'). This might come up again
  // when we upgrade llvm or if a *Val's memory layout is changed.
  Value* result_val = builder.CreateCall(udf, udf_args, "result");
  builder.CreateRet(result_val);

  *fn = codegen->FinalizeFunction(*fn);
  DCHECK(*fn != NULL);
  return Status::OK;
}

string NativeUdfExpr::DebugString() const {
  stringstream out;
  out << "NativeUdfExpr(location= " << hdfs_location_ << " symbol_name= "
      << symbol_name_ << " " << Expr::DebugString() << ")";
  return out.str();
}
