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

#include "util/jni-util.h"

#include <hdfs.h>
#include <sstream>

#include "common/status.h"
#include "rpc/jni-thrift-util.h"

#include "common/names.h"

namespace impala {

Status JniUtfCharGuard::create(JNIEnv* env, jstring jstr, JniUtfCharGuard* out) {
  DCHECK(jstr != nullptr);
  DCHECK(!env->ExceptionCheck());
  const char* utf_chars = env->GetStringUTFChars(jstr, nullptr);
  bool exception_check = static_cast<bool>(env->ExceptionCheck());
  if (utf_chars == nullptr || exception_check) {
    if (exception_check) env->ExceptionClear();
    if (utf_chars != nullptr) env->ReleaseStringUTFChars(jstr, utf_chars);
    auto fail_message = "GetStringUTFChars failed. Probable OOM on JVM side";
    LOG(ERROR) << fail_message;
    return Status(fail_message);
  }
  out->env = env;
  out->jstr = jstr;
  out->utf_chars = utf_chars;
  return Status::OK();
}

jclass JniUtil::jni_util_cl_ = NULL;
jclass JniUtil::internal_exc_cl_ = NULL;
jmethodID JniUtil::get_jvm_metrics_id_ = NULL;
jmethodID JniUtil::get_jvm_threads_id_ = NULL;
jmethodID JniUtil::throwable_to_string_id_ = NULL;
jmethodID JniUtil::throwable_to_stack_trace_id_ = NULL;

Status JniLocalFrame::push(JNIEnv* env, int max_local_ref) {
  DCHECK(env_ == NULL);
  DCHECK_GT(max_local_ref, 0);
  if (env->PushLocalFrame(max_local_ref) < 0) {
    env->ExceptionClear();
    return Status("failed to push frame");
  }
  env_ = env;
  return Status::OK();
}

bool JniUtil::ClassExists(JNIEnv* env, const char* class_str) {
  jclass local_cl = env->FindClass(class_str);
  jthrowable exc = env->ExceptionOccurred();
  if (exc != NULL) {
    env->ExceptionClear();
    return false;
  }
  env->DeleteLocalRef(local_cl);
  return true;
}

bool JniUtil::MethodExists(JNIEnv* env, jclass class_ref, const char* method_str,
    const char* method_signature) {
  env->GetMethodID(class_ref, method_str, method_signature);
  jthrowable exc = env->ExceptionOccurred();
  if (exc != nullptr) {
    env->ExceptionClear();
    return false;
  }
  return true;
}

Status JniUtil::GetGlobalClassRef(JNIEnv* env, const char* class_str, jclass* class_ref) {
  *class_ref = NULL;
  jclass local_cl = env->FindClass(class_str);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(LocalToGlobalRef(env, local_cl, class_ref));
  env->DeleteLocalRef(local_cl);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status JniUtil::LocalToGlobalRef(JNIEnv* env, jobject local_ref, jobject* global_ref) {
  *global_ref = env->NewGlobalRef(local_ref);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status JniUtil::Init() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) return Status("Failed to get/create JVM");
  // Find JniUtil class and create a global ref.
  jclass local_jni_util_cl = env->FindClass("org/apache/impala/common/JniUtil");
  if (local_jni_util_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil class.");
  }
  jni_util_cl_ = reinterpret_cast<jclass>(env->NewGlobalRef(local_jni_util_cl));
  if (jni_util_cl_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to create global reference to JniUtil class.");
  }
  env->DeleteLocalRef(local_jni_util_cl);
  if (env->ExceptionOccurred()) {
    return Status("Failed to delete local reference to JniUtil class.");
  }

  // Find InternalException class and create a global ref.
  jclass local_internal_exc_cl =
      env->FindClass("org/apache/impala/common/InternalException");
  if (local_internal_exc_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil class.");
  }
  internal_exc_cl_ = reinterpret_cast<jclass>(env->NewGlobalRef(local_internal_exc_cl));
  if (internal_exc_cl_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to create global reference to JniUtil class.");
  }
  env->DeleteLocalRef(local_internal_exc_cl);
  if (env->ExceptionOccurred()) {
    return Status("Failed to delete local reference to JniUtil class.");
  }

  // Throwable toString()
  throwable_to_string_id_ =
      env->GetStaticMethodID(jni_util_cl_, "throwableToString",
          "(Ljava/lang/Throwable;)Ljava/lang/String;");
  if (throwable_to_string_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.throwableToString method.");
  }

  // throwableToStackTrace()
  throwable_to_stack_trace_id_ =
      env->GetStaticMethodID(jni_util_cl_, "throwableToStackTrace",
          "(Ljava/lang/Throwable;)Ljava/lang/String;");
  if (throwable_to_stack_trace_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.throwableToFullStackTrace method.");
  }

  get_jvm_metrics_id_ =
      env->GetStaticMethodID(jni_util_cl_, "getJvmMetrics", "([B)[B");
  if (get_jvm_metrics_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.getJvmMetrics method.");
  }

  get_jvm_threads_id_ =
      env->GetStaticMethodID(jni_util_cl_, "getJvmThreadsInfo", "([B)[B");
  if (get_jvm_threads_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.getJvmThreadsInfo method.");
  }

  return Status::OK();
}

void JniUtil::InitLibhdfs() {
  // make random libhdfs calls to make sure that the context class loader isn't
  // null; see xxx for an explanation
  hdfsFS fs = hdfsConnect("default", 0);
  hdfsDisconnect(fs);
}

Status JniUtil::GetJniExceptionMsg(JNIEnv* env, bool log_stack, const string& prefix) {
  jthrowable exc = env->ExceptionOccurred();
  if (exc == nullptr) return Status::OK();
  env->ExceptionClear();
  DCHECK(throwable_to_string_id() != nullptr);
  const char* oom_msg_template = "$0 threw an unchecked exception. The JVM is likely out "
      "of memory (OOM).";
  jstring msg = static_cast<jstring>(env->CallStaticObjectMethod(jni_util_class(),
      throwable_to_string_id(), exc));
  if (env->ExceptionOccurred()) {
    env->ExceptionClear();
    string oom_msg = Substitute(oom_msg_template, "throwableToString");
    LOG(ERROR) << oom_msg;
    return Status(oom_msg);
  }
  JniUtfCharGuard msg_str_guard;
  RETURN_IF_ERROR(JniUtfCharGuard::create(env, msg, &msg_str_guard));
  if (log_stack) {
    jstring stack = static_cast<jstring>(env->CallStaticObjectMethod(jni_util_class(),
        throwable_to_stack_trace_id(), exc));
    if (env->ExceptionOccurred()) {
      env->ExceptionClear();
      string oom_msg = Substitute(oom_msg_template, "throwableToStackTrace");
      LOG(ERROR) << oom_msg;
      return Status(oom_msg);
    }
    JniUtfCharGuard c_stack_guard;
    RETURN_IF_ERROR(JniUtfCharGuard::create(env, stack, &c_stack_guard));
    VLOG(1) << c_stack_guard.get();
  }

  env->DeleteLocalRef(exc);
  return Status(Substitute("$0$1", prefix, msg_str_guard.get()));
}

Status JniUtil::GetJvmMetrics(const TGetJvmMetricsRequest& request,
    TGetJvmMetricsResponse* result) {
  return JniUtil::CallJniMethod(jni_util_class(), get_jvm_metrics_id_, request, result);
}

Status JniUtil::GetJvmThreadsInfo(const TGetJvmThreadsInfoRequest& request,
    TGetJvmThreadsInfoResponse* result) {
  return JniUtil::CallJniMethod(jni_util_class(), get_jvm_threads_id_, request, result);
}

Status JniUtil::LoadJniMethod(JNIEnv* env, const jclass& jni_class,
    JniMethodDescriptor* descriptor) {
  (*descriptor->method_id) = env->GetMethodID(jni_class,
      descriptor->name.c_str(), descriptor->signature.c_str());
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status JniUtil::LoadStaticJniMethod(JNIEnv* env, const jclass& jni_class,
    JniMethodDescriptor* descriptor) {
  (*descriptor->method_id) = env->GetStaticMethodID(jni_class,
      descriptor->name.c_str(), descriptor->signature.c_str());
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}
}
