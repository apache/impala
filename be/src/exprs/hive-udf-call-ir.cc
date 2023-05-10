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

#include <sstream>

#include "exprs/anyval-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/string-value.h"
#include "util/jni-util.h"

namespace impala {

HiveUdfCall::JniContext* HiveUdfCall::GetJniContext(FunctionContext* fn_ctx) {
  JniContext* jni_ctx = reinterpret_cast<JniContext*>(
      fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(jni_ctx != nullptr);
  return jni_ctx;
}

void HiveUdfCall::JniContext::SetInputNullsBufferElement(
    JniContext* jni_ctx, int index, uint8_t value) {
  jni_ctx->input_nulls_buffer[index] = value;
}

uint8_t* HiveUdfCall::JniContext::GetInputValuesBufferAtOffset(
    JniContext* jni_ctx, int offset) {
  return &jni_ctx->input_values_buffer[offset];
}

AnyVal* HiveUdfCall::CallJavaAndStoreResult(const ColumnType* type,
    FunctionContext* fn_ctx, JniContext* jni_ctx) {
  DCHECK(jni_ctx != nullptr);
  JNIEnv* env = GetJniEnvNotInlined();
  DCHECK(env != nullptr);

  // Using this version of Call has the lowest overhead. This eliminates the
  // vtable lookup and setting up return stacks.
  env->CallNonvirtualVoidMethodA(
      jni_ctx->executor, executor_cl_, executor_evaluate_id_, nullptr);
  Status status = JniUtil::GetJniExceptionMsg(env);
  if (!status.ok()) {
    if (!jni_ctx->warning_logged) {
      std::stringstream ss;
      ss << "Hive UDF path=" << jni_ctx->hdfs_location << " class="
          << jni_ctx->scalar_fn_symbol << " failed due to: " << status.GetDetail();
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
    AnyValUtil::SetAnyVal(jni_ctx->output_value_buffer, *type, jni_ctx->output_anyval);

    if (type->type == TYPE_STRING) {
      // Copy the string into a result allocation with the usual lifetime for expr
      // results. Needed because the UDF output buffer is owned by the Java UDF executor
      // and may be freed or reused by the next call into the Java UDF executor.
      StringVal* str_val = static_cast<StringVal*>(jni_ctx->output_anyval);
      const StringVal temp = StringVal::CopyFrom(fn_ctx, str_val->ptr, str_val->len);
      *str_val = temp;
    }
  }
  return jni_ctx->output_anyval;
}

} // namespace impala
