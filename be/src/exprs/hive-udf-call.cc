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
#include "rpc/thrift-util.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "util/bit-util.h"
#include "util/jni-util.h"

#include "gen-cpp/Frontend_types.h"

using namespace std;
using namespace boost;

const char* EXECUTOR_CLASS = "com/cloudera/impala/hive/executor/UdfExecutor";
const char* EXECUTOR_CTOR_SIGNATURE ="([B)V";
const char* EXECUTOR_EVALUATE_SIGNATURE = "()V";
const char* EXECUTOR_CLOSE_SIGNATURE = "()V";

namespace impala {

// TODO: we need to make one of these for each executing thread. When exprs
// in general become multi-threadable, we'll need to expose this some how.
struct HiveUdfCall::JniContext {
  jclass class_;
  jobject executor_;
  jmethodID evaluate_id_;
  jmethodID close_id_;

  uint8_t* input_values_buffer_;
  uint8_t* input_nulls_buffer_;
  uint8_t* output_value_buffer_;
  uint8_t output_null_value_;

  JniContext() {
    executor_ = NULL;
    input_values_buffer_ = NULL;
    input_nulls_buffer_ = NULL;
    output_value_buffer_ = NULL;
  }
};

HiveUdfCall::HiveUdfCall(const TExprNode& node)
  : Expr(node),
    jni_context_(new JniContext) {
  is_udf_call_ = true;
  DCHECK_EQ(node.node_type, TExprNodeType::FUNCTION_CALL);
  DCHECK_EQ(node.fn.binary_type, TFunctionBinaryType::HIVE);
}

HiveUdfCall::~HiveUdfCall() {
  JNIEnv* env = getJNIEnv();
  if (jni_context_->executor_ != NULL) {
    env->CallNonvirtualVoidMethodA(jni_context_->executor_,
        jni_context_->class_, jni_context_->close_id_, NULL);
    env->DeleteGlobalRef(jni_context_->executor_);
    // Clear any exceptions. Not much we can do about them here.
    Status status = JniUtil::GetJniExceptionMsg(env);
    if (!status.ok()) VLOG_QUERY << status.GetErrorMsg();
  }
  delete[] jni_context_->input_values_buffer_;
  delete[] jni_context_->input_nulls_buffer_;
  delete[] jni_context_->output_value_buffer_;
}

void* HiveUdfCall::Evaluate(Expr* e, TupleRow* row) {
  HiveUdfCall* udf = reinterpret_cast<HiveUdfCall*>(e);
  JniContext* ctx = udf->jni_context_.get();
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    // TODO: with new exprs structure, this should report the error to the user.
    VLOG_QUERY << "Could not get JNIEnv.";
    return NULL;
  }

  // Evaluate all the children values and put the results in input_values_buffer_
  for (int i = 0; i < e->GetNumChildren(); ++i) {
    void* v = e->GetChild(i)->GetValue(row);

    if (v == NULL) {
      ctx->input_nulls_buffer_[i] = 1;
    } else {
      uint8_t* input_ptr = ctx->input_values_buffer_ + udf->input_byte_offsets_[i];
      ctx->input_nulls_buffer_[i] = 0;
      switch (e->GetChild(i)->type()) {
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
          memcpy(input_ptr, v, 16);
          break;
        default:
          DCHECK(false) << "NYI";
      }
    }
  }

  // Using this version of Call has the lowest overhead. This eliminates the
  // vtable lookup and setting up return stacks.
  env->CallNonvirtualVoidMethodA(ctx->executor_, ctx->class_, ctx->evaluate_id_, NULL);
  Status status = JniUtil::GetJniExceptionMsg(env);
  if (!status.ok()) {
    stringstream ss;
    ss << "Hive UDF path=" << udf->fn_.hdfs_location << " class="
       << udf->fn_.scalar_fn.symbol
       << " failed due to: " << status.GetErrorMsg();
    udf->state_->LogError(ss.str());
    return NULL;
  }
  if (ctx->output_null_value_) return NULL;
  return ctx->output_value_buffer_;
}

Status HiveUdfCall::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(PrepareChildren(state, row_desc));
  state_ = state;

  // Copy the Hive Jar from hdfs to local file system.
  string local_path;
  RETURN_IF_ERROR(state->lib_cache()->GetLocalLibPath(
        state->fs_cache(), fn_.hdfs_location, LibCache::TYPE_JAR, &local_path));

  JNIEnv* env = getJNIEnv();
  if (env == NULL) return Status("Failed to get/create JVM");

  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, EXECUTOR_CLASS, &jni_context_->class_));
  jmethodID executor_ctor = env->GetMethodID(
      jni_context_->class_, "<init>", EXECUTOR_CTOR_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);
  jni_context_->evaluate_id_ = env->GetMethodID(
      jni_context_->class_, "evaluate", EXECUTOR_EVALUATE_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);
  jni_context_->close_id_ = env->GetMethodID(
      jni_context_->class_, "close", EXECUTOR_CLOSE_SIGNATURE);
  RETURN_ERROR_IF_EXC(env);

  int input_buffer_size = 0;

  THiveUdfExecutorCtorParams ctor_params;
  ctor_params.fn = fn_;
  ctor_params.local_location = local_path;
  for (int i = 0; i < GetNumChildren(); ++i) {
    ctor_params.input_byte_offsets.push_back(input_buffer_size);
    input_byte_offsets_.push_back(input_buffer_size);
    input_buffer_size += GetSlotSize(GetChild(i)->type());
    // Align all values up to 8 bytes. We don't care about footprint since we allocate
    // one buffer for all rows and we never copy the entire buffer.
    input_buffer_size = BitUtil::RoundUp(input_buffer_size, 8);
  }
  jni_context_->input_values_buffer_ = new uint8_t[input_buffer_size];
  jni_context_->input_nulls_buffer_ = new uint8_t[GetNumChildren()];
  jni_context_->output_value_buffer_ = new uint8_t[GetSlotSize(type())];

  ctor_params.input_buffer_ptr = (int64_t)jni_context_->input_values_buffer_;
  ctor_params.input_nulls_ptr = (int64_t)jni_context_->input_nulls_buffer_;
  ctor_params.output_buffer_ptr = (int64_t)jni_context_->output_value_buffer_;
  ctor_params.output_null_ptr = (int64_t)&jni_context_->output_null_value_;

  jbyteArray ctor_params_bytes;

  // Add a scoped cleanup jni reference object. This cleans up local refs made
  // below.
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));

  RETURN_IF_ERROR(SerializeThriftMsg(env, &ctor_params, &ctor_params_bytes));
  // Create the java executor object
  jni_context_->executor_ = env->NewObject(jni_context_->class_,
      executor_ctor, ctor_params_bytes);
  RETURN_ERROR_IF_EXC(env);
  jni_context_->executor_ = env->NewGlobalRef(jni_context_->executor_);

  compute_fn_ = Evaluate;
  return Status::OK;
}

string HiveUdfCall::DebugString() const {
  stringstream out;
  out << "HiveUdfCall(hdfs_location=" << fn_.hdfs_location
      << " classname=" << fn_.scalar_fn.symbol << " "
      << Expr::DebugString() << ")";
  return out.str();
}

}
