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

#include "exprs/hive-udf-call.h"

#include <jni.h>
#include <sstream>
#include <string>

#include "codegen/codegen-anyval.h"
#include "codegen/codegen-util.h"
#include "codegen/llvm-codegen.h"
#include "exprs/anyval-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "rpc/jni-thrift-util.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "util/bit-util.h"

#include "gen-cpp/Frontend_types.h"

#include "common/names.h"

const char* EXECUTOR_CLASS = "org/apache/impala/hive/executor/UdfExecutor";
const char* EXECUTOR_CTOR_SIGNATURE ="([B)V";
const char* EXECUTOR_EVALUATE_SIGNATURE = "()V";
const char* EXECUTOR_CLOSE_SIGNATURE = "()V";

namespace impala {

class LlvmCodeGen;

jclass HiveUdfCall::executor_cl_ = NULL;
jmethodID HiveUdfCall::executor_ctor_id_ = NULL;
jmethodID HiveUdfCall::executor_evaluate_id_ = NULL;
jmethodID HiveUdfCall::executor_close_id_ = NULL;

HiveUdfCall::HiveUdfCall(const TExprNode& node)
  : ScalarExpr(node), input_buffer_size_(0) {
  DCHECK_EQ(node.node_type, TExprNodeType::FUNCTION_CALL);
  DCHECK_EQ(node.fn.binary_type, TFunctionBinaryType::JAVA);
  DCHECK(executor_cl_ != NULL) << "Init() was not called!";
}

AnyVal* HiveUdfCall::Evaluate(ScalarExprEvaluator* eval, const TupleRow* row) const {
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  JniContext* jni_ctx = reinterpret_cast<JniContext*>(
      fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(jni_ctx != nullptr);

  JNIEnv* env = JniUtil::GetJNIEnv();
  DCHECK(env != nullptr);

  // Evaluate all the children values and put the results in input_values_buffer
  for (int i = 0; i < GetNumChildren(); ++i) {
    void* v = eval->GetValue(*GetChild(i), row);

    if (v == nullptr) {
      jni_ctx->input_nulls_buffer[i] = 1;
    } else {
      uint8_t* input_ptr = jni_ctx->input_values_buffer + input_byte_offsets_[i];
      jni_ctx->input_nulls_buffer[i] = 0;
      switch (GetChild(i)->type().type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
          // Using explicit sizes helps the compiler unroll memcpy
          memcpy(input_ptr, v, 1);
          break;
        case TYPE_SMALLINT:
          memcpy(input_ptr, v, 2);
          break;
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATE:
          memcpy(input_ptr, v, 4);
          break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
          memcpy(input_ptr, v, 8);
          break;
        case TYPE_TIMESTAMP:
          memcpy(input_ptr, v, sizeof(TimestampValue));
          break;
        case TYPE_STRING:
        case TYPE_VARCHAR:
          memcpy(input_ptr, v, sizeof(StringValue));
          break;
        default:
          DCHECK(false) << "NYI";
      }
    }
  }

  // Using this version of Call has the lowest overhead. This eliminates the
  // vtable lookup and setting up return stacks.
  env->CallNonvirtualVoidMethodA(
      jni_ctx->executor, executor_cl_, executor_evaluate_id_, nullptr);
  Status status = JniUtil::GetJniExceptionMsg(env);
  if (!status.ok()) {
    if (!jni_ctx->warning_logged) {
      stringstream ss;
      ss << "Hive UDF path=" << fn_.hdfs_location << " class=" << fn_.scalar_fn.symbol
        << " failed due to: " << status.GetDetail();
      if (fn_ctx->impl()->state()->abort_java_udf_on_exception()) {
        fn_ctx->SetError(ss.str().c_str());
      } else {
        fn_ctx->AddWarning(ss.str().c_str());
        jni_ctx->warning_logged = true;
      }
    }
    jni_ctx->output_anyval->is_null = true;
    return jni_ctx->output_anyval;
  }

  // Write output_value_buffer to output_anyval
  if (jni_ctx->output_null_value) {
    jni_ctx->output_anyval->is_null = true;
  } else {
    AnyValUtil::SetAnyVal(jni_ctx->output_value_buffer, type(), jni_ctx->output_anyval);
  }
  return jni_ctx->output_anyval;
}

Status HiveUdfCall::InitEnv() {
  DCHECK(executor_cl_ == NULL) << "Init() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) return Status("Failed to get/create JVM");
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, EXECUTOR_CLASS, &executor_cl_));
  executor_ctor_id_ = env->GetMethodID(
      executor_cl_, "<init>", EXECUTOR_CTOR_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);
  executor_evaluate_id_ = env->GetMethodID(
      executor_cl_, "evaluate", EXECUTOR_EVALUATE_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);
  executor_close_id_ = env->GetMethodID(
      executor_cl_, "close", EXECUTOR_CLOSE_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status HiveUdfCall::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  // Initialize children first.
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, is_entry_point, state));

  // Initialize input_byte_offsets_ and input_buffer_size_
  for (int i = 0; i < GetNumChildren(); ++i) {
    input_byte_offsets_.push_back(input_buffer_size_);
    input_buffer_size_ += GetChild(i)->type().GetSlotSize();
    // Align all values up to 8 bytes. We don't care about footprint since we allocate
    // one buffer for all rows and we never copy the entire buffer.
    input_buffer_size_ = BitUtil::RoundUpNumBytes(input_buffer_size_) * 8;
  }
  return Status::OK();
}

Status HiveUdfCall::OpenEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  RETURN_IF_ERROR(ScalarExpr::OpenEvaluator(scope, state, eval));

  // Create a JniContext in this thread's FunctionContext.
  DCHECK_GE(fn_ctx_idx_, 0);
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  JniContext* jni_ctx = new JniContext;
  fn_ctx->SetFunctionState(FunctionContext::THREAD_LOCAL, jni_ctx);

  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");

  // Fields used for error reporting.
  // This object and thus fn_ are alive when rows are evaluated.
  jni_ctx->hdfs_location = fn_.hdfs_location.c_str();
  jni_ctx->scalar_fn_symbol = fn_.scalar_fn.symbol.c_str();

  // Add a scoped cleanup jni reference object. This cleans up local refs made below.
  JniLocalFrame jni_frame;
  {
    // Scoped handle for libCache entry.
    LibCacheEntryHandle handle;
    string local_location;
    // Hive UDFs added as builtins are assumed to be always available in the classpath,
    // therefore no LibCache invocation is needed.
    bool builtin_udf = fn_.hdfs_location.empty();
    if (!builtin_udf) {
      RETURN_IF_ERROR(LibCache::instance()->GetLocalPath(fn_.hdfs_location,
          LibCache::TYPE_JAR, fn_.last_modified_time, &handle, &local_location));
    }
    THiveUdfExecutorCtorParams ctor_params;
    ctor_params.fn = fn_;
    ctor_params.local_location = local_location;
    ctor_params.input_byte_offsets = input_byte_offsets_;

    jni_ctx->input_values_buffer = new uint8_t[input_buffer_size_];
    jni_ctx->input_nulls_buffer = new uint8_t[GetNumChildren()];
    jni_ctx->output_value_buffer = new uint8_t[type().GetSlotSize()];

    ctor_params.input_buffer_ptr = (int64_t)jni_ctx->input_values_buffer;
    ctor_params.input_nulls_ptr = (int64_t)jni_ctx->input_nulls_buffer;
    ctor_params.output_buffer_ptr = (int64_t)jni_ctx->output_value_buffer;
    ctor_params.output_null_ptr = (int64_t)&jni_ctx->output_null_value;

    jbyteArray ctor_params_bytes;

    // Pushed frame will be popped when jni_frame goes out-of-scope.
    RETURN_IF_ERROR(jni_frame.push(env));

    RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
    // Create the java executor object. The jar referenced by the libCache handle
    // is not needed after the next call, so it is safe for the handle to go
    // out-of-scope after the java object is instantiated.
    jni_ctx->executor = env->NewObject(executor_cl_, executor_ctor_id_, ctor_params_bytes);
  }
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jni_ctx->executor, &jni_ctx->executor));

  RETURN_IF_ERROR(AllocateAnyVal(state, eval->expr_perm_pool(), type_,
      "Could not allocate JNI output value", &jni_ctx->output_anyval));
  return Status::OK();
}

void HiveUdfCall::CloseEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  if (eval->opened()) {
    FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
    JniContext* jni_ctx = reinterpret_cast<JniContext*>(
        fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));

    if (jni_ctx != NULL) {
      JNIEnv* env = JniUtil::GetJNIEnv();
      if (jni_ctx->executor != NULL) {
        env->CallNonvirtualVoidMethodA(
            jni_ctx->executor, executor_cl_, executor_close_id_, NULL);
        Status s = JniUtil::GetJniExceptionMsg(env);
        if (!s.ok()) state->LogError(s.msg());
        env->DeleteGlobalRef(jni_ctx->executor);
      }
      if (jni_ctx->input_values_buffer != NULL) {
        delete[] jni_ctx->input_values_buffer;
        jni_ctx->input_values_buffer = NULL;
      }
      if (jni_ctx->input_nulls_buffer != NULL) {
        delete[] jni_ctx->input_nulls_buffer;
        jni_ctx->input_nulls_buffer = NULL;
      }
      if (jni_ctx->output_value_buffer != NULL) {
        delete[] jni_ctx->output_value_buffer;
        jni_ctx->output_value_buffer = NULL;
      }
      jni_ctx->output_anyval = NULL;
      delete jni_ctx;
      fn_ctx->SetFunctionState(FunctionContext::THREAD_LOCAL, nullptr);
    }
  }
  ScalarExpr::CloseEvaluator(scope, state, eval);
}

Status HiveUdfCall::CodegenEvalChildren(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Function* function, llvm::Value* (*args)[2], llvm::Value* jni_ctx,
    llvm::BasicBlock* const first_block, llvm::BasicBlock** next_block) {

   llvm::Function* const set_input_null_buff_elem_fn =
       codegen->GetFunction(IRFunction::JNI_CTX_SET_INPUT_NULL_BUFF_ELEM, false);
   llvm::Function* const get_input_val_buff_at_offset_fn =
       codegen->GetFunction(IRFunction::JNI_CTX_INPUT_VAL_BUFF_AT_OFFSET, false);

   llvm::LLVMContext& context = codegen->context();
   llvm::BasicBlock* current_eval_child_block = first_block;
   const int num_children = GetNumChildren();
   for (int i = 0; i < num_children; ++i) {
     ScalarExpr* const child_expr = GetChild(i);
     llvm::Function* child_fn = nullptr;
     RETURN_IF_ERROR(child_expr->GetCodegendComputeFn(codegen, false, &child_fn));

     builder->SetInsertPoint(current_eval_child_block);

     const ColumnType& child_type = child_expr->type();
     CodegenAnyVal child_wrapped = CodegenAnyVal::CreateCallWrapped(
         codegen, builder, child_type, child_fn, *args, "child");

     CodegenAnyValReadWriteInfo rwi = child_wrapped.ToReadWriteInfo();
     rwi.entry_block().BranchTo(builder);

     llvm::BasicBlock* next_eval_child_block = llvm::BasicBlock::Create(
         context, "eval_child", function);

     // Child is null
     builder->SetInsertPoint(rwi.null_block());
     builder->CreateCall(set_input_null_buff_elem_fn,
         {jni_ctx, codegen->GetI32Constant(i), codegen->GetI8Constant(1)});
     builder->CreateBr(next_eval_child_block);

     // Child is not null.
     builder->SetInsertPoint(rwi.non_null_block());
     builder->CreateCall(set_input_null_buff_elem_fn,
         {jni_ctx, codegen->GetI32Constant(i), codegen->GetI8Constant(0)});
     llvm::Value* const input_ptr = builder->CreateCall(get_input_val_buff_at_offset_fn,
         {jni_ctx, codegen->GetI32Constant(input_byte_offsets_[i])}, "input_ptr");

     llvm::Value* const child_val_ptr =
         SlotDescriptor::CodegenStoreNonNullAnyValToNewAlloca(rwi);
     const std::size_t size = CodeGenUtil::GetTypeSize(child_type.type);
     codegen->CodegenMemcpy(builder, input_ptr, child_val_ptr, size);
     builder->CreateBr(next_eval_child_block);
     current_eval_child_block = next_eval_child_block;
   }

   *next_block = current_eval_child_block;
   return Status::OK();
}

llvm::Value* CastPtrAndLoad(LlvmCodeGen* codegen, LlvmBuilder* builder,
    const ColumnType& type, llvm::Value* ptr, const std::string& name) {
  llvm::PointerType* const ptr_type =
      CodegenAnyVal::GetLoweredType(codegen, type)->getPointerTo();
  llvm::Value* const ptr_cast =
      builder->CreateBitCast(ptr, ptr_type, name + "_ptr_cast");
  return builder->CreateLoad(ptr_cast, name);
}

/// Sample IR for calling the following Java function:
/// public String evaluate(String a, String b, String c) {
///   if (a == null || b == null || c == null) return null;
///   return a + b + c;
/// }
///
/// To reproduce, create the following function:
///
/// create function concatenate(string, string, string) returns string
/// location '/test-warehouse/impala-hive-udfs.jar'
/// symbol='org.apache.impala.TestUdf';
///
/// then run the following query:
///
/// select default.concatenate(date_string_col, string_col, cast(double_col as string))
/// from functional.alltypes;
///
/// define { i64, i8* } @HiveUdfCall(%"class.impala::ScalarExprEvaluator"* %eval,
///                                  %"class.impala::TupleRow"* %row) #47 {
/// entry:
///   %0 = alloca %"struct.impala::ColumnType"
///   %1 = alloca %"struct.impala::StringValue"
///   %2 = alloca %"struct.impala::StringValue"
///   %3 = alloca %"struct.impala::StringValue"
///   %fn_ctx = call %"class.impala_udf::FunctionContext"*
///       @_ZN6impala19ScalarExprEvaluator18GetFunctionContextEPS0_i
///           %"class.impala::ScalarExprEvaluator"* %eval,
///           i32 0)
///   %jni_ctx = call %"struct.impala::HiveUdfCall::JniContext"*
///       @_ZN6impala11HiveUdfCall13GetJniContextEPN10impala_udf15FunctionContextE(
///           %"class.impala_udf::FunctionContext"* %fn_ctx)
///   br label %eval_child
///
/// eval_child:                                       ; preds = %entry
///   %child = call { i64, i8* } @GetSlotRef(%"class.impala::ScalarExprEvaluator"* %eval,
///                                          %"class.impala::TupleRow"* %row)
///   br label %entry1
///
/// entry1:                                           ; preds = %eval_child
///   %4 = extractvalue { i64, i8* } %child, 0
///   %is_null = trunc i64 %4 to i1
///   br i1 %is_null, label %null, label %non_null
///
/// non_null:                                         ; preds = %entry1
///   %child2 = extractvalue { i64, i8* } %child, 1
///   %5 = extractvalue { i64, i8* } %child, 0
///   %6 = ashr i64 %5, 32
///   %7 = trunc i64 %6 to i32
///   call void @_ZN6impala11HiveUdfCall10JniContext26SetInputNullsBufferElementEPS1_ih(
///       %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///       i32 0,
///       i8 0)
///   %input_ptr = call i8*
///       @_ZN6impala11HiveUdfCall10JniContext28GetInputValuesBufferAtOffsetEPS1_i(
///           %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///           i32 0)
///   %8 = insertvalue %"struct.impala::StringValue" zeroinitializer, i32 %7, 1
///   %9 = insertvalue %"struct.impala::StringValue" %8, i8* %child2, 0
///   store %"struct.impala::StringValue" %9, %"struct.impala::StringValue"* %3
///   %10 = bitcast %"struct.impala::StringValue"* %3 to i8*
///   call void @llvm.memcpy.p0i8.p0i8.i64(i8* %input_ptr,
///                                        i8* %10,
///                                        i64 12,
///                                        i32 0,
///                                        i1 false)
///   br label %eval_child3
///
/// null:                                             ; preds = %entry1
///   call void @_ZN6impala11HiveUdfCall10JniContext26SetInputNullsBufferElementEPS1_ih(
///       %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///       i32 0,
///       i8 1)
///   br label %eval_child3
///
/// eval_child3:                                      ; preds = %non_null, %null
///   %child4 = call { i64, i8* } @GetSlotRef.1(
///       %"class.impala::ScalarExprEvaluator"* %eval,
///       %"class.impala::TupleRow"* %row)
///   br label %entry5
///
/// entry5:                                           ; preds = %eval_child3
///   %11 = extractvalue { i64, i8* } %child4, 0
///   %is_null8 = trunc i64 %11 to i1
///   br i1 %is_null8, label %null7, label %non_null6
///
/// non_null6:                                        ; preds = %entry5
///   %child9 = extractvalue { i64, i8* } %child4, 1
///   %12 = extractvalue { i64, i8* } %child4, 0
///   %13 = ashr i64 %12, 32
///   %14 = trunc i64 %13 to i32
///   call void @_ZN6impala11HiveUdfCall10JniContext26SetInputNullsBufferElementEPS1_ih(
///       %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///       i32 1,
///       i8 0)
///   %input_ptr11 = call i8*
///       @_ZN6impala11HiveUdfCall10JniContext28GetInputValuesBufferAtOffsetEPS1_i(
///           %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///           i32 16)
///   %15 = insertvalue %"struct.impala::StringValue" zeroinitializer, i32 %14, 1
///   %16 = insertvalue %"struct.impala::StringValue" %15, i8* %child9, 0
///   store %"struct.impala::StringValue" %16, %"struct.impala::StringValue"* %2
///   %17 = bitcast %"struct.impala::StringValue"* %2 to i8*
///   call void @llvm.memcpy.p0i8.p0i8.i64(i8* %input_ptr11,
///                                        i8* %17,
///                                        i64 12,
///                                        i32 0,
///                                        i1 false)
///   br label %eval_child10
///
/// null7:                                            ; preds = %entry5
///   call void @_ZN6impala11HiveUdfCall10JniContext26SetInputNullsBufferElementEPS1_ih(
///       %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///       i32 1,
///       i8 1)
///   br label %eval_child10
///
/// eval_child10:                                     ; preds = %non_null6, %null7
///   %child12 = call { i64, i8* } @"impala::CastFunctions::CastToStringValWrapper"(
///       %"class.impala::ScalarExprEvaluator"* %eval,
///       %"class.impala::TupleRow"* %row)
///   br label %entry13
///
/// entry13:                                          ; preds = %eval_child10
///   %18 = extractvalue { i64, i8* } %child12, 0
///   %is_null16 = trunc i64 %18 to i1
///   br i1 %is_null16, label %null15, label %non_null14
///
/// non_null14:                                       ; preds = %entry13
///   %child17 = extractvalue { i64, i8* } %child12, 1
///   %19 = extractvalue { i64, i8* } %child12, 0
///   %20 = ashr i64 %19, 32
///   %21 = trunc i64 %20 to i32
///   call void @_ZN6impala11HiveUdfCall10JniContext26SetInputNullsBufferElementEPS1_ih(
///       %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///       i32 2,
///       i8 0)
///   %input_ptr19 = call i8*
///       @_ZN6impala11HiveUdfCall10JniContext28GetInputValuesBufferAtOffsetEPS1_i(
///           %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///           i32 32)
///   %22 = insertvalue %"struct.impala::StringValue" zeroinitializer, i32 %21, 1
///   %23 = insertvalue %"struct.impala::StringValue" %22, i8* %child17, 0
///   store %"struct.impala::StringValue" %23, %"struct.impala::StringValue"* %1
///   %24 = bitcast %"struct.impala::StringValue"* %1 to i8*
///   call void @llvm.memcpy.p0i8.p0i8.i64(i8* %input_ptr19,
///                                        i8* %24,
///                                        i64 12,
///                                        i32 0,
///                                        i1 false)
///   br label %call_java
///
/// null15:                                           ; preds = %entry13
///   call void @_ZN6impala11HiveUdfCall10JniContext26SetInputNullsBufferElementEPS1_ih(
///       %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx,
///       i32 2,
///       i8 1)
///   br label %call_java
///
/// call_java:                                        ; preds = %non_null14, %null15
///   store %"struct.impala::ColumnType" {
///           i32 10, i32 -1, i32 -1, i32 -1,
///           %"class.std::vector.13" zeroinitializer,
///           %"class.std::vector.18" zeroinitializer,
///           %"class.std::vector.23" zeroinitializer },
///       %"struct.impala::ColumnType"* %0
///   %ret_ptr = call %"struct.impala_udf::AnyVal"*
///   ; The next two lines should be one line but the name of the identifier is too long.
///       @_ZN6impala11HiveUdfCall22CallJavaAndStoreResultEPKNS_10ColumnTypeEPN10
///impala_udf15FunctionContextEPNS0_10JniContextE(
///           %"struct.impala::ColumnType"* %0,
///           %"class.impala_udf::FunctionContext"* %fn_ctx,
///           %"struct.impala::HiveUdfCall::JniContext"* %jni_ctx)
///   %ret_ptr_cast = bitcast %"struct.impala_udf::AnyVal"* %ret_ptr to { i64, i8* }*
///   %ret = load { i64, i8* }, { i64, i8* }* %ret_ptr_cast
///   ret { i64, i8* } %ret
/// }
Status HiveUdfCall::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
   // Cross-compiled functions this hand-crafted function will call.
   llvm::Function* const get_func_ctx_fn =
       codegen->GetFunction(IRFunction::GET_FUNCTION_CTX, false);
   llvm::Function* const get_jni_ctx_fn =
       codegen->GetFunction(IRFunction::GET_JNI_CONTEXT, false);
   llvm::Function* const call_java_fn =
       codegen->GetFunction(IRFunction::HIVE_UDF_CALL_CALL_JAVA, false);

   // Function prototype.
   llvm::LLVMContext& context = codegen->context();
   LlvmBuilder builder(context);

   llvm::Value* args[2];
   llvm::Function* const function =
       CreateIrFunctionPrototype("HiveUdfCall", codegen, &args);

   // Codegen the initialisation of function context etc.
   llvm::BasicBlock* const entry_block =
       llvm::BasicBlock::Create(context, "entry", function);
   builder.SetInsertPoint(entry_block);

   llvm::Value* const fn_ctx = builder.CreateCall(
       get_func_ctx_fn, {args[0], codegen->GetI32Constant(fn_ctx_idx_)}, "fn_ctx");
   llvm::Value* const jni_ctx = builder.CreateCall(get_jni_ctx_fn, {fn_ctx}, "jni_ctx");

   // Codegen the evaluation of children.
   llvm::BasicBlock* const first_eval_child_block =
       llvm::BasicBlock::Create(context, "eval_child", function);
   builder.CreateBr(first_eval_child_block);

   llvm::BasicBlock* call_java_block = nullptr;
   RETURN_IF_ERROR(CodegenEvalChildren(codegen, &builder, function, &args, jni_ctx,
         first_eval_child_block, &call_java_block));

   // Codegen the call to Java and returning the result.
   call_java_block->setName("call_java");
   builder.SetInsertPoint(call_java_block);

   llvm::Value* const ir_type = type().ToIR(codegen);
   llvm::Value* const ir_type_ptr = codegen->GetPtrTo(&builder, ir_type);

   llvm::Value* const ret_ptr = builder.CreateCall(call_java_fn,
       {ir_type_ptr, fn_ctx, jni_ctx}, "ret_ptr");

   llvm::Value* ret_val = CastPtrAndLoad(codegen, &builder, type(), ret_ptr, "ret");
   builder.CreateRet(ret_val);

   *fn = codegen->FinalizeFunction(function);
   if (UNLIKELY(*fn == nullptr)) {
     return Status(TErrorCode::IR_VERIFY_FAILED, "HiveUdfCall");
   }
   return Status::OK();
}

string HiveUdfCall::DebugString() const {
  stringstream out;
  out << "HiveUdfCall(hdfs_location=" << fn_.hdfs_location
      << " classname=" << fn_.scalar_fn.symbol << " "
      << ScalarExpr::DebugString() << ")";
  return out.str();
}

BooleanVal HiveUdfCall::GetBooleanValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN);
  return *reinterpret_cast<BooleanVal*>(Evaluate(eval, row));
}

TinyIntVal HiveUdfCall::GetTinyIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TINYINT);
  return *reinterpret_cast<TinyIntVal*>(Evaluate(eval, row));
}

SmallIntVal HiveUdfCall::GetSmallIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_SMALLINT);
  return * reinterpret_cast<SmallIntVal*>(Evaluate(eval, row));
}

IntVal HiveUdfCall::GetIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_INT);
  return *reinterpret_cast<IntVal*>(Evaluate(eval, row));
}

BigIntVal HiveUdfCall::GetBigIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BIGINT);
  return *reinterpret_cast<BigIntVal*>(Evaluate(eval, row));
}

FloatVal HiveUdfCall::GetFloatValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_FLOAT);
  return *reinterpret_cast<FloatVal*>(Evaluate(eval, row));
}

DoubleVal HiveUdfCall::GetDoubleValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DOUBLE);
  return *reinterpret_cast<DoubleVal*>(Evaluate(eval, row));
}

StringVal HiveUdfCall::GetStringValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_STRING);
  StringVal result = *reinterpret_cast<StringVal*>(Evaluate(eval, row));
  if (result.is_null) return StringVal::null();
  // Copy the string into a result allocation with the usual lifetime for expr results.
  // Needed because the UDF output buffer is owned by the Java UDF executor and may be
  // freed or reused by the next call into the Java UDF executor.
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  return StringVal::CopyFrom(fn_ctx, result.ptr, result.len);
}

TimestampVal HiveUdfCall::GetTimestampValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  return *reinterpret_cast<TimestampVal*>(Evaluate(eval, row));
}

DecimalVal HiveUdfCall::GetDecimalValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DECIMAL);
  return *reinterpret_cast<DecimalVal*>(Evaluate(eval, row));
}

DateVal HiveUdfCall::GetDateValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DATE);
  return *reinterpret_cast<DateVal*>(Evaluate(eval, row));
}

JNIEnv* HiveUdfCall::GetJniEnvNotInlined() {
  return JniUtil::GetJNIEnv();
}

}
