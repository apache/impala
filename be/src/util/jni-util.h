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


#ifndef IMPALA_UTIL_JNI_UTIL_H
#define IMPALA_UTIL_JNI_UTIL_H

#include <jni.h>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Frontend_types.h"

#define THROW_IF_ERROR_WITH_LOGGING(stmt, env, adaptor) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      (adaptor)->WriteErrorLog(); \
      (adaptor)->WriteFileErrors(); \
      string error_msg; \
      status.GetErrorMsg(&error_msg); \
      (env)->ThrowNew((adaptor)->impala_exc_cl(), error_msg.c_str()); \
      return; \
    } \
  } while (false)

#define THROW_IF_ERROR(stmt, env, impala_exc_cl) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      string error_msg; \
      status.GetErrorMsg(&error_msg); \
      (env)->ThrowNew((impala_exc_cl), error_msg.c_str()); \
      return; \
    } \
  } while (false)

#define SET_TSTATUS_IF_ERROR(stmt, tstatus) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      (tstatus)->status_code = TStatusCode::INTERNAL_ERROR; \
      status.GetErrorMsgs(&(tstatus)->error_msgs); \
      return; \
    } \
  } while (false)

#define THROW_IF_ERROR_RET(stmt, env, impala_exc_cl, ret) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      string error_msg; \
      status.GetErrorMsg(&error_msg); \
      (env)->ThrowNew((impala_exc_cl), error_msg.c_str()); \
      return (ret); \
    } \
  } while (false)

#define THROW_IF_EXC(env, exc_class) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
      DCHECK((throwable_to_string_id_) != NULL); \
      jstring stack = (jstring) env->CallStaticObjectMethod(JniUtil::jni_util_class(), \
          (JniUtil::throwable_to_stack_trace_id()), exc); \
      jboolean is_copy; \
      const char* c_stack = \
          reinterpret_cast<const char*>((env)->GetStringUTFChars(stack, &is_copy)); \
      (env)->ExceptionClear(); \
      (env)->ThrowNew((exc_class), c_stack); \
      return; \
    } \
  } while (false)

#define RETURN_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
      jstring stack = (jstring) env->CallStaticObjectMethod(JniUtil::jni_util_class(), \
          (JniUtil::throwable_to_stack_trace_id()), exc); \
      jboolean is_copy; \
      const char* c_stack = \
          reinterpret_cast<const char*>((env)->GetStringUTFChars(stack, &is_copy)); \
      VLOG(1) << string(c_stack); \
     return; \
    } \
  } while (false)

#define EXIT_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
      jstring stack = (jstring) env->CallStaticObjectMethod(JniUtil::jni_util_class(), \
          (JniUtil::throwable_to_stack_trace_id()), exc); \
      jboolean is_copy; \
      const char* c_stack = \
          reinterpret_cast<const char*>((env)->GetStringUTFChars(stack, &is_copy)); \
      LOG(ERROR) << string(c_stack); \
     exit(1); \
    } \
  } while (false)

#define RETURN_ERROR_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) return JniUtil::GetJniExceptionMsg(env);\
  } while (false)

#define EXIT_IF_JNIERROR(stmt) \
  do { \
    if ((stmt) != JNI_OK) { \
      cerr << #stmt << " resulted in a jni error"; \
      exit(1); \
    } \
  } while (false)

#define RETURN_IF_JNIERROR(stmt) \
  do { \
    if ((stmt) != JNI_OK) { \
      stringstream out; \
      out << #stmt << " resulted in a jni error";      \
      return Status(out.str()); \
    } \
  } while (false)

// C linkage for helper functions in hdfsJniHelper.h
extern  "C" { extern JNIEnv* getJNIEnv(void); }

namespace impala {

class Status;

// Utility class to push/pop a single JNI frame. "push" will push a JNI frame and the
// d'tor will pop the JNI frame. Frames establish a scope for local references. Local
// references go out of scope when their frame is popped, which enables the GC to clean up
// the corresponding objects.
class JniLocalFrame {
 public:
  JniLocalFrame(): env_(NULL) {}
  ~JniLocalFrame() { if (env_ != NULL) env_->PopLocalFrame(NULL); }

  // Pushes a new JNI local frame. The frame can support max_local_ref local references.
  // The number of local references created inside the frame might exceed max_local_ref,
  // but there is no guarantee that memory will be available.
  // Push should be called at most once.
  Status push(JNIEnv* env, int max_local_ref=10);

 private:
  JNIEnv* env_;
};

// Describes one method to look up in a Java object
struct JniMethodDescriptor {
  // Name of the method, case must match
  const std::string name;

  // JNI-style method signature
  const std::string signature;

  // Handle to the method
  jmethodID* method_id;
};

// Utility class for JNI-related functionality.
// Init() should be called as soon as the native library is loaded.
// Creates global class references, and promotes local references to global references.
// Maintains a list of all global references for cleanup in Cleanup().
// Attention! Lifetime of JNI components and common pitfalls:
// 1. JNIEnv* cannot be shared among threads, so it should NOT be globally cached.
// 2. References created via jnienv->New*() calls are local references that go out of scope
//    at the end of a code block (and will be gc'ed by the JVM). They should NOT be cached.
// 3. Use global references for caching classes.
//    They need to be explicitly created and cleaned up (will not be gc'd up by the JVM).
//    Global references can be shared among threads.
// 4. JNI method ids and field ids are tied to the JVM that created them,
//    and can be shared among threads. They are not "references" so there is no need
//    to explicitly create a global reference to them.
class JniUtil {
 public:
  // Call this prior to any libhdfs calls.
  static void InitLibhdfs();

  // Find JniUtil class, and get JniUtil.throwableToString method id
  static Status Init();

  // Returns true if the given class could be found on the CLASSPATH in env.
  // Returns false otherwise, or if any other error occurred (e.g. a JNI exception).
  // This function does not log any errors or exceptions.
  static bool ClassExists(JNIEnv* env, const char* class_str);

  // Returns a global JNI reference to the class specified by class_str into class_ref.
  // The reference is added to global_refs_ for cleanup in Deinit().
  // Returns Status::OK if successful.
  // Catches Java exceptions and converts their message into status.
  static Status GetGlobalClassRef(JNIEnv* env, const char* class_str, jclass* class_ref);

  // Creates a global reference from a local reference returned into global_ref.
  // Adds global reference to global_refs_ for cleanup in Deinit().
  // Returns Status::OK if successful.
  // Catches Java exceptions and converts their message into status.
  static Status LocalToGlobalRef(JNIEnv* env, jobject local_ref, jobject* global_ref);

  static jmethodID throwable_to_string_id() { return throwable_to_string_id_; }
  static jmethodID throwable_to_stack_trace_id() { return throwable_to_stack_trace_id_; }

  // Global reference to java JniUtil class
  static jclass jni_util_class() { return jni_util_cl_; }

  // Global reference to InternalException class.
  static jclass internal_exc_class() { return internal_exc_cl_; }

  // Delete all global references: class members, and those stored in global_refs_.
  static Status Cleanup();

  // Returns the error message for 'e'. If no exception, returns Status::OK
  // log_stack determines if the stack trace is written to the log
  // prefix, if non-empty will be prepended to the error message.
  static Status GetJniExceptionMsg(JNIEnv* env, bool log_stack = true,
      const std::string& prefix = "");

  // Populates 'result' with a list of memory metrics from the Jvm. Returns Status::OK
  // unless there is an exception.
  static Status GetJvmMetrics(const TGetJvmMetricsRequest& request,
      TGetJvmMetricsResponse* result);

  // Loads a method whose signature is in the supplied descriptor. Returns Status::OK
  // and sets descriptor->method_id to a JNI method handle if successful, otherwise an
  // error status is returned.
  static Status LoadJniMethod(JNIEnv* jni_env, const jclass& jni_class,
      JniMethodDescriptor* descriptor);

  // Utility methods to avoid repeating lots of the JNI call boilerplate. It seems these
  // must be defined in the header to compile properly.
  template <typename T>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method, const T& arg) {
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(jni_env));
    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &arg, &request_bytes));
    jni_env->CallObjectMethod(obj, method, request_bytes);
    RETURN_ERROR_IF_EXC(jni_env);
    return Status::OK;
  }

  template <typename R>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method, R* response) {
    JNIEnv* jni_env = getJNIEnv();
    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(jni_env));
    jbyteArray result_bytes = static_cast<jbyteArray>(
        jni_env->CallObjectMethod(obj, method));
    RETURN_ERROR_IF_EXC(jni_env);
    RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, response));
    return Status::OK;
  }

  template <typename T, typename R>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method,
      const T& arg, R* response) {
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(jni_env));
    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &arg, &request_bytes));
    jbyteArray result_bytes = static_cast<jbyteArray>(
        jni_env->CallObjectMethod(obj, method, request_bytes));
    RETURN_ERROR_IF_EXC(jni_env);
    RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, response));
    return Status::OK;
  }

  template <typename T>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method,
      const T& arg, std::string* response) {
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(jni_env));
    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &arg, &request_bytes));
    jstring java_response_string = static_cast<jstring>(
        jni_env->CallObjectMethod(obj, method, request_bytes));
    RETURN_ERROR_IF_EXC(jni_env);
    jboolean is_copy;
    const char *str = jni_env->GetStringUTFChars(java_response_string, &is_copy);
    RETURN_ERROR_IF_EXC(jni_env);
    *response = str;
    jni_env->ReleaseStringUTFChars(java_response_string, str);
    RETURN_ERROR_IF_EXC(jni_env);
    return Status::OK;
  }

 private:
  static jclass jni_util_cl_;
  static jclass internal_exc_cl_;
  static jmethodID throwable_to_string_id_;
  static jmethodID throwable_to_stack_trace_id_;
  static jmethodID get_jvm_metrics_id_;
  // List of global references created with GetGlobalClassRef() or LocalToGlobalRef.
  // All global references are deleted in Cleanup().
  static std::vector<jobject> global_refs_;
};

}

#endif
