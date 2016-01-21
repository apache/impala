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

#include "exprs/hive-udf-call.h"

#include <jni.h>
#include <sstream>
#include <string>

#include "codegen/llvm-codegen.h"
#include "exprs/anyval-util.h"
#include "exprs/expr-context.h"
#include "rpc/jni-thrift-util.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "util/bit-util.h"

#include "gen-cpp/Frontend_types.h"

#include "common/names.h"

const char* EXECUTOR_CLASS = "com/cloudera/impala/hive/executor/UdfExecutor";
const char* EXECUTOR_CTOR_SIGNATURE ="([B)V";
const char* EXECUTOR_EVALUATE_SIGNATURE = "()V";
const char* EXECUTOR_CLOSE_SIGNATURE = "()V";

namespace impala {

struct JniContext {
  jclass cl;
  jobject executor;
  jmethodID evalute_id;
  jmethodID close_id;

  uint8_t* input_values_buffer;
  uint8_t* input_nulls_buffer;
  uint8_t* output_value_buffer;
  uint8_t output_null_value;
  bool warning_logged;

  AnyVal* output_anyval;

  JniContext()
    : cl(NULL),
      executor(NULL),
      evalute_id(NULL),
      close_id(NULL),
      input_values_buffer(NULL),
      input_nulls_buffer(NULL),
      output_value_buffer(NULL),
      warning_logged(false),
      output_anyval(NULL) {
  }
};

HiveUdfCall::HiveUdfCall(const TExprNode& node)
  : Expr(node),
    input_buffer_size_(0) {
  DCHECK_EQ(node.node_type, TExprNodeType::FUNCTION_CALL);
  DCHECK_EQ(node.fn.binary_type, TFunctionBinaryType::HIVE);
}

AnyVal* HiveUdfCall::Evaluate(ExprContext* ctx, TupleRow* row) {
  FunctionContext* fn_ctx = ctx->fn_context(fn_context_index_);
  JniContext* jni_ctx = reinterpret_cast<JniContext*>(
      fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(jni_ctx != NULL);

  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    stringstream ss;
    ss << "Hive UDF path=" << fn_.hdfs_location << " class=" << fn_.scalar_fn.symbol
      << " failed due to JNI issue getting the JNIEnv object";
    fn_ctx->SetError(ss.str().c_str());
    jni_ctx->output_anyval->is_null = true;
    return jni_ctx->output_anyval;
  }

  // Evaluate all the children values and put the results in input_values_buffer
  for (int i = 0; i < GetNumChildren(); ++i) {
    void* v = ctx->GetValue(GetChild(i), row);

    if (v == NULL) {
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
          memcpy(input_ptr, v, 4);
          break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
          memcpy(input_ptr, v, 8);
          break;
        case TYPE_TIMESTAMP:
        case TYPE_STRING:
        case TYPE_VARCHAR:
          memcpy(input_ptr, v, 16);
          break;
        default:
          DCHECK(false) << "NYI";
      }
    }
  }

  // Using this version of Call has the lowest overhead. This eliminates the
  // vtable lookup and setting up return stacks.
  env->CallNonvirtualVoidMethodA(
      jni_ctx->executor, jni_ctx->cl, jni_ctx->evalute_id, NULL);
  Status status = JniUtil::GetJniExceptionMsg(env);
  if (!status.ok()) {
    if (!jni_ctx->warning_logged) {
      stringstream ss;
      ss << "Hive UDF path=" << fn_.hdfs_location << " class=" << fn_.scalar_fn.symbol
        << " failed due to: " << status.GetDetail();
      fn_ctx->AddWarning(ss.str().c_str());
      jni_ctx->warning_logged = true;
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

Status HiveUdfCall::Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                            ExprContext* ctx) {
  RETURN_IF_ERROR(Expr::Prepare(state, row_desc, ctx));

  // Copy the Hive Jar from hdfs to local file system.
  RETURN_IF_ERROR(LibCache::instance()->GetLocalLibPath(
      fn_.hdfs_location, LibCache::TYPE_JAR, &local_location_));

  // Initialize input_byte_offsets_ and input_buffer_size_
  for (int i = 0; i < GetNumChildren(); ++i) {
    input_byte_offsets_.push_back(input_buffer_size_);
    input_buffer_size_ += GetChild(i)->type().GetSlotSize();
    // Align all values up to 8 bytes. We don't care about footprint since we allocate
    // one buffer for all rows and we never copy the entire buffer.
    input_buffer_size_ = BitUtil::RoundUpNumBytes(input_buffer_size_) * 8;
  }

  // Register FunctionContext in ExprContext
  RegisterFunctionContext(ctx, state);

  return Status::OK();
}

Status HiveUdfCall::Open(RuntimeState* state, ExprContext* ctx,
                         FunctionContext::FunctionStateScope scope) {
  RETURN_IF_ERROR(Expr::Open(state, ctx, scope));

  // Create a JniContext in this thread's FunctionContext
  FunctionContext* fn_ctx = ctx->fn_context(fn_context_index_);
  JniContext* jni_ctx = new JniContext;
  fn_ctx->SetFunctionState(FunctionContext::THREAD_LOCAL, jni_ctx);

  JNIEnv* env = getJNIEnv();
  if (env == NULL) return Status("Failed to get/create JVM");

  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, EXECUTOR_CLASS, &jni_ctx->cl));
  jmethodID executor_ctor = env->GetMethodID(
      jni_ctx->cl, "<init>", EXECUTOR_CTOR_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);
  jni_ctx->evalute_id = env->GetMethodID(
      jni_ctx->cl, "evaluate", EXECUTOR_EVALUATE_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);
  jni_ctx->close_id = env->GetMethodID(
      jni_ctx->cl, "close", EXECUTOR_CLOSE_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);

  THiveUdfExecutorCtorParams ctor_params;
  ctor_params.fn = fn_;
  ctor_params.local_location = local_location_;
  ctor_params.input_byte_offsets = input_byte_offsets_;

  jni_ctx->input_values_buffer = new uint8_t[input_buffer_size_];
  jni_ctx->input_nulls_buffer = new uint8_t[GetNumChildren()];
  jni_ctx->output_value_buffer = new uint8_t[type().GetSlotSize()];

  ctor_params.input_buffer_ptr = (int64_t)jni_ctx->input_values_buffer;
  ctor_params.input_nulls_ptr = (int64_t)jni_ctx->input_nulls_buffer;
  ctor_params.output_buffer_ptr = (int64_t)jni_ctx->output_value_buffer;
  ctor_params.output_null_ptr = (int64_t)&jni_ctx->output_null_value;

  jbyteArray ctor_params_bytes;

  // Add a scoped cleanup jni reference object. This cleans up local refs made
  // below.
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));

  RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
  // Create the java executor object
  jni_ctx->executor = env->NewObject(jni_ctx->cl,
      executor_ctor, ctor_params_bytes);
  RETURN_ERROR_IF_EXC(env);
  jni_ctx->executor = env->NewGlobalRef(jni_ctx->executor);

  jni_ctx->output_anyval = CreateAnyVal(type_);

  return Status::OK();
}

void HiveUdfCall::Close(RuntimeState* state, ExprContext* ctx,
                        FunctionContext::FunctionStateScope scope) {
  if (fn_context_index_ != -1) {
    FunctionContext* fn_ctx = ctx->fn_context(fn_context_index_);
    JniContext* jni_ctx = reinterpret_cast<JniContext*>(
        fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));

    if (jni_ctx != NULL) {
      JNIEnv* env = getJNIEnv();
      if (jni_ctx->executor != NULL) {
        env->CallNonvirtualVoidMethodA(
            jni_ctx->executor, jni_ctx->cl, jni_ctx->close_id, NULL);
        env->DeleteGlobalRef(jni_ctx->executor);
        // Clear any exceptions. Not much we can do about them here.
        Status status = JniUtil::GetJniExceptionMsg(env);
        if (!status.ok()) VLOG_QUERY << status.GetDetail();
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
      if (jni_ctx->output_anyval != NULL) {
        delete jni_ctx->output_anyval;
        jni_ctx->output_anyval = NULL;
      }
    } else {
      DCHECK(!ctx->opened_);
    }
  }

  Expr::Close(state, ctx, scope);
}

Status HiveUdfCall::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  return GetCodegendComputeFnWrapper(state, fn);
}

string HiveUdfCall::DebugString() const {
  stringstream out;
  out << "HiveUdfCall(hdfs_location=" << fn_.hdfs_location
      << " classname=" << fn_.scalar_fn.symbol << " "
      << Expr::DebugString() << ")";
  return out.str();
}

BooleanVal HiveUdfCall::GetBooleanVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN);
  return *reinterpret_cast<BooleanVal*>(Evaluate(ctx, row));
}

TinyIntVal HiveUdfCall::GetTinyIntVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TINYINT);
  return *reinterpret_cast<TinyIntVal*>(Evaluate(ctx, row));
}

SmallIntVal HiveUdfCall::GetSmallIntVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_SMALLINT);
  return * reinterpret_cast<SmallIntVal*>(Evaluate(ctx, row));
}

IntVal HiveUdfCall::GetIntVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_INT);
  return *reinterpret_cast<IntVal*>(Evaluate(ctx, row));
}

BigIntVal HiveUdfCall::GetBigIntVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BIGINT);
  return *reinterpret_cast<BigIntVal*>(Evaluate(ctx, row));
}

FloatVal HiveUdfCall::GetFloatVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_FLOAT);
  return *reinterpret_cast<FloatVal*>(Evaluate(ctx, row));
}

DoubleVal HiveUdfCall::GetDoubleVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DOUBLE);
  return *reinterpret_cast<DoubleVal*>(Evaluate(ctx, row));
}

StringVal HiveUdfCall::GetStringVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_STRING);
  return *reinterpret_cast<StringVal*>(Evaluate(ctx, row));
}

TimestampVal HiveUdfCall::GetTimestampVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP);
  return *reinterpret_cast<TimestampVal*>(Evaluate(ctx, row));
}

DecimalVal HiveUdfCall::GetDecimalVal(ExprContext* ctx, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DECIMAL);
  return *reinterpret_cast<DecimalVal*>(Evaluate(ctx, row));
}

}
