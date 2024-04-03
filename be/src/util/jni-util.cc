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
#include "gutil/once.h"
#include "rpc/jni-thrift-util.h"

#include "common/names.h"

DEFINE_int64(jvm_deadlock_detector_interval_s, 60,
    "(Advanced) Interval between JVM deadlock checks. If set to 0 or a negative value, "
    "deadlock checks are disabled.");

namespace impala {

template <class T>
Status JniBufferGuard<T>::create(JNIEnv* env, T jbuffer, JniBufferGuard<T>* out) {
  DCHECK(jbuffer != nullptr);
  DCHECK(!env->ExceptionCheck());
  jboolean is_copy;
  const char* buffer = nullptr;
  uint32_t size = -1;
  if constexpr (std::is_same_v<T, jstring>) {
    size = env->GetStringLength(jbuffer);
    buffer = env->GetStringUTFChars(jbuffer, &is_copy);
  } else {
    static_assert(std::is_same_v<T, jbyteArray>);
    size = env->GetArrayLength(jbuffer);
    buffer = reinterpret_cast<char*>(env->GetByteArrayElements(jbuffer, &is_copy));
  }
  DCHECK_NE(size, -1);

  bool exception_check = static_cast<bool>(env->ExceptionCheck());
  if (buffer == nullptr || exception_check) {
    if (exception_check) env->ExceptionClear();
    if (buffer != nullptr) Release(env, jbuffer, buffer);
    std::string fail_message = Substitute("$0 $1",
        std::is_same_v<T, jstring> ? "GetStringUTFChars" : "GetByteArrayElements",
        "failed. Probable OOM on JVM side.");
    LOG(ERROR) << fail_message;
    return Status(fail_message);
  }
  out->env = env;
  out->jbuffer = jbuffer;
  out->size = size;
  out->buffer = buffer;
  return Status::OK();
}

// JniBufferGuard<jstring> is instantiated implicitly because functons in the header use
// it, but JniBufferGuard<jbyteArray> needs to be instantiated explicitly.
template class JniBufferGuard<jbyteArray>;

bool JniScopedArrayCritical::Create(JNIEnv* env, jbyteArray jarr,
    JniScopedArrayCritical* out) {
  DCHECK(env != nullptr);
  DCHECK(out != nullptr);
  DCHECK(!env->ExceptionCheck());
  int size = env->GetArrayLength(jarr);
  void* pac = env->GetPrimitiveArrayCritical(jarr, nullptr);
  if (pac == nullptr) {
    LOG(ERROR) << "GetPrimitiveArrayCritical() failed. Probable OOM on JVM side";
    return false;
  }
  out->env_ = env;
  out->jarr_ = jarr;
  out->arr_ = static_cast<uint8_t*>(pac);
  out->size_ = size;
  return true;
}

bool JniUtil::jvm_inited_ = false;
__thread JNIEnv* JniUtil::tls_env_ = nullptr;
jclass JniUtil::jni_util_cl_ = NULL;
jclass JniUtil::internal_exc_cl_ = NULL;
jmethodID JniUtil::get_jvm_metrics_id_ = NULL;
jmethodID JniUtil::get_jvm_threads_id_ = NULL;
jmethodID JniUtil::get_jmx_json_ = NULL;
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

Status JniUtil::GetMethodID(JNIEnv* env, jclass class_ref, const char* method_str,
    const char* method_signature, jmethodID* method_ref) {
  *method_ref = env->GetMethodID(class_ref, method_str, method_signature);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status JniUtil::GetStaticMethodID(JNIEnv* env, jclass class_ref, const char* method_str,
    const char* method_signature, jmethodID* method_ref) {
  *method_ref = env->GetStaticMethodID(class_ref, method_str, method_signature);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
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
  JNIEnv* env = JniUtil::GetJNIEnv();
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
      env->GetStaticMethodID(jni_util_cl_, "getJvmMemoryMetrics", "()[B");
  if (get_jvm_metrics_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.getJvmMemoryMetrics method.");
  }

  get_jvm_threads_id_ =
      env->GetStaticMethodID(jni_util_cl_, "getJvmThreadsInfo", "([B)[B");
  if (get_jvm_threads_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.getJvmThreadsInfo method.");
  }

  get_jmx_json_ =
      env->GetStaticMethodID(jni_util_cl_, "getJMXJson", "()[B");
  if (get_jmx_json_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.getJMXJson method.");
  }
  jvm_inited_ = true;
  return Status::OK();
}

void JniUtil::InitLibhdfs() {
  // make random libhdfs calls to make sure that the context class loader isn't
  // null; see xxx for an explanation
  hdfsFS fs = hdfsConnect("default", 0);
  hdfsDisconnect(fs);
}

namespace {
JavaVM* g_vm;
GoogleOnceType g_vm_once = GOOGLE_ONCE_INIT;

void FindJavaVMOrDie() {
  int num_vms;
  int rv = JNI_GetCreatedJavaVMs(&g_vm, 1, &num_vms);
  CHECK_EQ(rv, 0) << "Could not find any created Java VM. Must init libhdfs first";
  CHECK_EQ(num_vms, 1) << "No VMs returned";
}

} // anonymous namespace

JNIEnv* JniUtil::GetJNIEnvSlowPath() {
  DCHECK(!tls_env_) << "Call GetJNIEnv() fast path";

  GoogleOnceInit(&g_vm_once, &FindJavaVMOrDie);
  int rc = g_vm->GetEnv(reinterpret_cast<void**>(&tls_env_), JNI_VERSION_1_6);
  if (rc == JNI_EDETACHED) {
    // Make a dummy call to attach libhdfs.
    int junk;
    hdfsConfGetInt("x", &junk);
    rc = g_vm->GetEnv(reinterpret_cast<void**>(&tls_env_), JNI_VERSION_1_6);
  }
  CHECK_EQ(rc, 0) << "Unable to get JVM";
  return CHECK_NOTNULL(tls_env_);
}

Status JniUtil::InitJvmPauseMonitor() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (!env) return Status("Failed to get/create JVM.");
  if (!jni_util_cl_) return Status("JniUtil::Init() not called.");
  jmethodID init_jvm_pm_method;
  JniMethodDescriptor init_jvm_pm_desc = {
      "initPauseMonitor", "(J)V", &init_jvm_pm_method};
  RETURN_IF_ERROR(JniUtil::LoadStaticJniMethod(env, jni_util_cl_, &init_jvm_pm_desc));
  return JniCall::static_method(jni_util_cl_, init_jvm_pm_method)
      .with_primitive_arg(FLAGS_jvm_deadlock_detector_interval_s)
      .Call();
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
      DCHECK(msg_str_guard.get() != nullptr);
      LOG(ERROR) << msg_str_guard.get();
      return Status(oom_msg);
    }
    JniUtfCharGuard c_stack_guard;
    RETURN_IF_ERROR(JniUtfCharGuard::create(env, stack, &c_stack_guard));
    VLOG(1) << c_stack_guard.get();
    env->DeleteLocalRef(stack);
  }

  const char* msg_str = msg_str_guard.get();
  env->DeleteLocalRef(msg);
  env->DeleteLocalRef(exc);
  return Status(Substitute("$0$1", prefix, msg_str));
}

Status JniUtil::GetJvmMemoryMetrics(TGetJvmMemoryMetricsResponse* result) {
  return JniCall::static_method(jni_util_class(), get_jvm_metrics_id_).Call(result);
}

Status JniUtil::GetJvmThreadsInfo(const TGetJvmThreadsInfoRequest& request,
    TGetJvmThreadsInfoResponse* result) {
  return JniCall::static_method(jni_util_class(), get_jvm_threads_id_)
      .with_thrift_arg(request).Call(result);
}

Status JniUtil::GetJMXJson(TGetJMXJsonResponse* result) {
  return JniCall::static_method(jni_util_class(), get_jmx_json_).Call(result);
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
