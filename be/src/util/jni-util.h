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


#ifndef IMPALA_UTIL_JNI_UTIL_H
#define IMPALA_UTIL_JNI_UTIL_H

#include <jni.h>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gutil/macros.h"

#define THROW_IF_ERROR(stmt, env, impala_exc_cl) \
  do { \
    const Status& _status = (stmt); \
    if (!_status.ok()) { \
      (env)->ThrowNew((impala_exc_cl), _status.GetDetail().c_str()); \
      return; \
    } \
  } while (false)

#define THROW_IF_ERROR_RET(stmt, env, impala_exc_cl, ret) \
  do { \
    const Status& _status = (stmt); \
    if (!_status.ok()) { \
      (env)->ThrowNew((impala_exc_cl), _status.GetDetail().c_str()); \
      return (ret); \
    } \
  } while (false)

#define RETURN_ERROR_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != nullptr) return JniUtil::GetJniExceptionMsg(env);\
  } while (false)

// If there's an exception in 'env', log the backtrace at FATAL level and abort the
// process. This will generate a core dump if core dumps are enabled, so this should
// generally only be called for internal errors where the coredump is useful for
// diagnostics.
#define ABORT_IF_EXC(env) do { ABORT_IF_ERROR(JniUtil::GetJniExceptionMsg(env)); } while (false)

// If there's an exception in 'env', log the backtrace at ERROR level and exit the process
// cleanly with status 1.
#define CLEAN_EXIT_IF_EXC(env) \
  do { \
    Status s = JniUtil::GetJniExceptionMsg(env); \
    if (!s.ok()) CLEAN_EXIT_WITH_ERROR(s.GetDetail()); \
  } while (false)

namespace impala {

class Status;

/// Utility class to push/pop a single JNI frame. "push" will push a JNI frame and the
/// d'tor will pop the JNI frame. Frames establish a scope for local references. Local
/// references go out of scope when their frame is popped, which enables the GC to clean up
/// the corresponding objects.
class JniLocalFrame {
 public:
  JniLocalFrame(): env_(nullptr) {}
  ~JniLocalFrame() { if (env_ != nullptr) env_->PopLocalFrame(nullptr); }

  JniLocalFrame(JniLocalFrame&& other) noexcept
    : env_(other.env_) {
    other.env_ = nullptr;
  }

  /// Pushes a new JNI local frame. The frame can support max_local_ref local references.
  /// The number of local references created inside the frame might exceed max_local_ref,
  /// but there is no guarantee that memory will be available.
  /// Push should be called at most once.
  Status push(JNIEnv* env, int max_local_ref = 10) WARN_UNUSED_RESULT;

 private:
  DISALLOW_COPY_AND_ASSIGN(JniLocalFrame);

  JNIEnv* env_;
};

/// Describes one method to look up in a Java object
struct JniMethodDescriptor {
  /// Name of the method, case must match
  const std::string name;

  /// JNI-style method signature
  const std::string signature;

  /// Handle to the method
  jmethodID* method_id;
};

/// Helper class for the lifetime management of JNI char or byte buffers. Releases the JNI
/// buffer when destructed. T must be either jstring or jbyteArray. If T is a jstring, the
/// string in the buffer is in utf-8 format.
///
/// See also JniScopedArrayCritical, which can also be used for byte buffers but its usage
/// is more restricted, see https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/functions.html#GetPrimitiveArrayCritical_ReleasePrimitiveArrayCritical
template <class T>
class JniBufferGuard {
  static_assert(std::is_same_v<T, jstring> || std::is_same_v<T, jbyteArray>);
 public:
  /// Construct a JniBufferGuard holding nothing.
  JniBufferGuard() : env(nullptr), jbuffer{}, size(0), buffer(nullptr)  {}

  /// Release the held JNI buffer if there is one.
  ~JniBufferGuard() {
    Release(env, jbuffer, buffer);
  }

  /// Try to get chars/bytes from jbuffer. If an error is returned, buffer and get()
  /// remain 'nullptr's, otherwise they point to a valid char/byte array. The char/byte
  /// array lives as long as this guard. jbuffer should not be null.
  static Status create(JNIEnv* env, T jbuffer, JniBufferGuard* out);

  /// Get the size of the buffer.
  uint32_t get_size() { return size; }

  /// Get the buffer. Returns nullptr if the guard does hold a buffer.
  const char* get() { return buffer; }
 private:
  JNIEnv* env;
  T jbuffer;
  uint32_t size;
  const char* buffer;
  DISALLOW_COPY_AND_ASSIGN(JniBufferGuard);

  static void Release(JNIEnv* env, T jbuffer, const char* buffer) {
    if (buffer != nullptr) {
      if constexpr (std::is_same_v<T, jstring>) {
        env->ReleaseStringUTFChars(jbuffer, buffer);
      } else {
        static_assert(std::is_same_v<T, jbyteArray>);
        /// Return buffer back. JNI_ABORT indicates to not copy contents back to java
        /// side.
        env->ReleaseByteArrayElements(jbuffer,
            const_cast<jbyte*>(reinterpret_cast<const jbyte*>(buffer)), JNI_ABORT);
      }
    }
  }
};

using JniUtfCharGuard = JniBufferGuard<jstring>;
using JniByteArrayGuard = JniBufferGuard<jbyteArray>;

class JniScopedArrayCritical {
 public:
  /// Construct a JniScopedArrayCritical holding nothing.
  JniScopedArrayCritical():  env_(nullptr), jarr_(nullptr), arr_(nullptr), size_(0) {}

  /// Release the held byte[] contents if necessary.
  ~JniScopedArrayCritical() {
    if (env_ != nullptr && jarr_ != nullptr && arr_ != nullptr) {
      env_->ReleasePrimitiveArrayCritical(jarr_, arr_, JNI_ABORT);
    }
  }

  /// Try to get the contents of 'jarr' via JNIEnv::GetPrimitiveArrayCritical() and set
  /// the results in 'out'. Returns true upon success and false otherwise. If false is
  /// returned 'out' is not modified.
  static bool Create(JNIEnv* env, jbyteArray jarr, JniScopedArrayCritical* out)
      WARN_UNUSED_RESULT;

  uint8_t* get() const { return arr_; }

  int size() const { return size_; }
 private:
  JNIEnv* env_;
  jbyteArray jarr_;
  uint8_t* arr_;
  int size_;
  DISALLOW_COPY_AND_ASSIGN(JniScopedArrayCritical);
};

/// Utility class for making JNI calls, with various types of argument
/// or response.
///
/// Example usages:
///
/// 1) Static call taking a Thrift struct and returning a string:
///
///   string s;
///   RETURN_IF_ERROR(JniCall::static_method(my_jclass, my_method)
///       .with_thrift_arg(foo).Call(&s));
///
/// 2) Non-static call taking no arguments and returning a Thrift struct:
///
///   TMyObject result;
///   RETURN_IF_ERROR(JniCall::instance_method(my_jobject, my_method).Call(&result);
class JniCall {
 public:
   JniCall(JniCall&& other) noexcept = default;

   static JniCall static_method(jclass clazz, jmethodID method) WARN_UNUSED_RESULT {
     return JniCall(method, clazz);
   }

   static JniCall instance_method(jobject obj, jmethodID method) WARN_UNUSED_RESULT {
     return JniCall(method, obj);
   }

  /// Pass a Thrift-encoded argument. The JNI method should take a byte[] for the
  /// Thrift-serialized data. Multiple arguments may be passed by repeated calls.
  template<class T>
  JniCall& with_thrift_arg(const T& arg) WARN_UNUSED_RESULT;

  /// Pass a primitive arg (eg an integer).
  /// Multiple arguments may be passed by repeated calls.
  template<class T>
  JniCall& with_primitive_arg(T arg) WARN_UNUSED_RESULT;

  /// Call the method expecting no result.
  Status Call() WARN_UNUSED_RESULT {
    return Call(static_cast<void*>(nullptr));
  }

  /// Call the method and return a result (either std::string or a Thrift struct).
  template<class T>
  Status Call(T* result) WARN_UNUSED_RESULT;

 private:
  explicit JniCall(jmethodID method);

  explicit JniCall(jmethodID method, jclass cls) : JniCall(method) {
    class_ = DCHECK_NOTNULL(cls);
  }

  explicit JniCall(jmethodID method, jobject instance) : JniCall(method) {
    instance_ = DCHECK_NOTNULL(instance);
  }

  template<class T>
  Status ObjectToResult(jobject obj, T* result) WARN_UNUSED_RESULT;

  Status ObjectToResult(jobject obj, void* no_result) WARN_UNUSED_RESULT;

  Status ObjectToResult(jobject obj, std::string* result) WARN_UNUSED_RESULT;

  Status ObjectToResult(jobject obj, jobject* result) WARN_UNUSED_RESULT;

  const jmethodID method_;
  JNIEnv* const env_;
  JniLocalFrame frame_;

  jclass class_ = nullptr;
  jobject instance_ = nullptr;
  std::vector<jvalue> args_;
  Status status_;

  DISALLOW_COPY_AND_ASSIGN(JniCall);
};


/// Utility class for JNI-related functionality.
/// Init() should be called as soon as the native library is loaded.
/// Creates global class references, and promotes local references to global references.
/// Attention! Lifetime of JNI components and common pitfalls:
/// 1. JNIEnv* cannot be shared among threads, so it should NOT be globally cached.
/// 2. References created via jnienv->New*() calls are local references that go out of scope
///    at the end of a code block (and will be gc'ed by the JVM). They should NOT be cached.
/// 3. Use global references for caching classes.
///    They need to be explicitly created and cleaned up (will not be gc'd up by the JVM).
///    Global references can be shared among threads.
/// 4. JNI method ids and field ids are tied to the JVM that created them,
///    and can be shared among threads. They are not "references" so there is no need
///    to explicitly create a global reference to them.
class JniUtil {
 public:
  /// Init JniUtil. This should be called prior to any other calls.
  static Status Init() WARN_UNUSED_RESULT;

  /// Call this prior to any libhdfs calls.
  static void InitLibhdfs();

  /// Returns the JNIEnv attached to the current thread, attaching it
  /// if necessary. Always returns a valid non-NULL value.
  static JNIEnv* GetJNIEnv() {
    if (tls_env_) return tls_env_;
    return GetJNIEnvSlowPath();
  }

  /// Initializes the JvmPauseMonitor.
  static Status InitJvmPauseMonitor() WARN_UNUSED_RESULT;

  /// Returns true if the given class could be found on the CLASSPATH in env.
  /// Returns false otherwise, or if any other error occurred (e.g. a JNI exception).
  /// This function does not log any errors or exceptions.
  static bool ClassExists(JNIEnv* env, const char* class_str);

  /// Return true if the given class has a non-static method with a specific name and
  /// signature. Returns false otherwise, or if any other error occurred
  /// (e.g. a JNI exception). This function does not log any errors or exceptions.
  static bool MethodExists(JNIEnv* env, jclass class_ref,
      const char* method_str, const char* method_signature);

  /// Wrapper method around JNI's 'GetMethodID'. Returns the method reference for the
  /// requested method.
  static Status GetMethodID(JNIEnv* env, jclass class_ref, const char* method_str,
      const char* method_signature, jmethodID* method_ref);

  /// Wrapper method around JNI's 'GetStaticMethodID'. Returns the method reference for
  /// the requested method.
  static Status GetStaticMethodID(JNIEnv* env, jclass class_ref, const char* method_str,
      const char* method_signature, jmethodID* method_ref);

  /// Returns a global JNI reference to the class specified by class_str into class_ref.
  /// The returned reference must eventually be freed by calling FreeGlobalRef() (or have
  /// the lifetime of the impalad process).
  /// Catches Java exceptions and converts their message into status.
  static Status GetGlobalClassRef(
      JNIEnv* env, const char* class_str, jclass* class_ref) WARN_UNUSED_RESULT;

  /// Creates a global reference from a local reference returned into global_ref.
  /// The returned reference must eventually be freed by calling FreeGlobalRef() (or have
  /// the lifetime of the impalad process).
  /// Catches Java exceptions and converts their message into status.
  static Status LocalToGlobalRef(JNIEnv* env, jobject local_ref,
      jobject* global_ref) WARN_UNUSED_RESULT;

  /// Templated wrapper for jobject subclasses (e.g. jclass, jarray). This is necessary
  /// because according to
  /// http://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/types.html:
  ///   class _jobject {};
  ///   class _jclass : public _jobject {};
  ///   ...
  ///   typedef _jobject *jobject;
  ///   typedef _jclass *jclass;
  /// This mean jobject* is actually _jobject**, so we need the reinterpret_cast in order
  /// to use a subclass like _jclass**. This is safe in this case because the returned
  /// subclass is known to be correct.
  template <typename jobject_subclass>
  static Status LocalToGlobalRef(
      JNIEnv* env, jobject local_ref, jobject_subclass* global_ref) {
    return LocalToGlobalRef(env, local_ref, reinterpret_cast<jobject*>(global_ref));
  }

  static jmethodID throwable_to_string_id() { return throwable_to_string_id_; }
  static jmethodID throwable_to_stack_trace_id() { return throwable_to_stack_trace_id_; }

  /// Returns true if an embedded JVM is initialized, false otherwise.
  static bool is_jvm_inited() { return jvm_inited_; }

  /// Global reference to java JniUtil class
  static jclass jni_util_class() { return jni_util_cl_; }

  /// Global reference to InternalException class.
  static jclass internal_exc_class() { return internal_exc_cl_; }

  /// Returns the error message for 'e'. If no exception, returns Status::OK
  /// log_stack determines if the stack trace is written to the log
  /// prefix, if non-empty will be prepended to the error message.
  static Status GetJniExceptionMsg(JNIEnv* env, bool log_stack = true,
      const std::string& prefix = "") WARN_UNUSED_RESULT;

  /// Populates 'result' with a list of memory metrics from the Jvm. Returns Status::OK
  /// unless there is an exception.
  static Status GetJvmMemoryMetrics(
      TGetJvmMemoryMetricsResponse* result) WARN_UNUSED_RESULT;

  /// Populates 'result' with information about live JVM threads. Returns
  /// Status::OK unless there is an exception.
  static Status GetJvmThreadsInfo(const TGetJvmThreadsInfoRequest& request,
      TGetJvmThreadsInfoResponse* result) WARN_UNUSED_RESULT;

  /// Gets JMX metrics of the JVM encoded as a JSON string.
  static Status GetJMXJson(TGetJMXJsonResponse* result) WARN_UNUSED_RESULT;

  /// Loads a method whose signature is in the supplied descriptor. Returns Status::OK
  /// and sets descriptor->method_id to a JNI method handle if successful, otherwise an
  /// error status is returned.
  static Status LoadJniMethod(JNIEnv* jni_env, const jclass& jni_class,
      JniMethodDescriptor* descriptor) WARN_UNUSED_RESULT;

  /// Same as LoadJniMethod(...), except that this loads a static method.
  static Status LoadStaticJniMethod(JNIEnv* jni_env, const jclass& jni_class,
      JniMethodDescriptor* descriptor) WARN_UNUSED_RESULT;

  /// Utility methods to avoid repeating lots of the JNI call boilerplate.
  /// New code should prefer using JniCall() directly for better clarity.
  static Status CallJniMethod(
      const jobject& obj, const jmethodID& method) WARN_UNUSED_RESULT {
    return JniCall::instance_method(obj, method).Call();
  }

  static Status CallStaticJniMethod(
      const jclass& cls, const jmethodID& method) WARN_UNUSED_RESULT {
    return JniCall::static_method(cls, method).Call();
  }

  template <typename T, typename R>
  static Status CallStaticJniMethod(const jclass& cls, const jmethodID& method,
      const T& arg, R* response) WARN_UNUSED_RESULT;

  template <typename T>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method,
      const T& arg) WARN_UNUSED_RESULT;

  template <typename T, typename R>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method,
      const T& arg, R* response) WARN_UNUSED_RESULT;

  template <typename R>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method,
      R* response) WARN_UNUSED_RESULT;

  template <typename T>
  static Status CallJniMethod(const jobject& obj, const jmethodID& method,
      const T& arg, jobject* response) WARN_UNUSED_RESULT;

 private:
  // Slow-path for GetJNIEnv, used on the first call by any thread.
  static JNIEnv* GetJNIEnvSlowPath();

  // Set in Init() once the JVM is initialized.
  static bool jvm_inited_;
  static jclass jni_util_cl_;
  static jclass internal_exc_cl_;
  static jmethodID throwable_to_string_id_;
  static jmethodID throwable_to_stack_trace_id_;
  static jmethodID get_jvm_metrics_id_;
  static jmethodID get_jvm_threads_id_;
  static jmethodID get_jmx_json_;

  // Thread-local cache of the JNIEnv for this thread.
  static __thread JNIEnv* tls_env_;
};

/// Convert a C++ primitive to a JNI 'jvalue' union.
/// See https://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/types.html
/// for reference on the union members.
template<typename T>
jvalue PrimitiveToValue(T cpp_val);

#define SPECIALIZE_PRIMITIVE_TO_VALUE(cpp_type, union_field)    \
  template<> inline jvalue PrimitiveToValue(cpp_type cpp_val) { \
    jvalue v;                                                   \
    memset(&v, 0, sizeof(v));                                    \
    v.union_field = cpp_val;                                    \
    return v;                                                   \
  }
SPECIALIZE_PRIMITIVE_TO_VALUE(bool, z);
SPECIALIZE_PRIMITIVE_TO_VALUE(int8_t, b);
SPECIALIZE_PRIMITIVE_TO_VALUE(char, c);
SPECIALIZE_PRIMITIVE_TO_VALUE(int16_t, s);
SPECIALIZE_PRIMITIVE_TO_VALUE(int32_t, i);
SPECIALIZE_PRIMITIVE_TO_VALUE(int64_t, j);
SPECIALIZE_PRIMITIVE_TO_VALUE(float, f);
SPECIALIZE_PRIMITIVE_TO_VALUE(double, d);
#undef SPECIALIZE_PRIMITIVE_TO_VALUE

template <typename T, typename R>
inline Status JniUtil::CallStaticJniMethod(const jclass& cls, const jmethodID& method,
    const T& arg, R* response) {
  return JniCall::static_method(cls, method).with_thrift_arg(arg).Call(response);
}

template <typename T>
inline Status JniUtil::CallJniMethod(const jobject& obj, const jmethodID& method,
    const T& arg) {
  return JniCall::instance_method(obj, method).with_thrift_arg(arg).Call();
}

template <>
inline Status JniUtil::CallJniMethod<int64_t>(const jobject& obj, const jmethodID& method,
    const int64_t& arg) {
  return JniCall::instance_method(obj, method).with_primitive_arg(arg).Call();
}

template <typename T, typename R>
inline Status JniUtil::CallJniMethod(const jobject& obj, const jmethodID& method,
    const T& arg, R* response) {
  return JniCall::instance_method(obj, method).with_thrift_arg(arg).Call(response);
}

template <typename R>
inline Status JniUtil::CallJniMethod(const jobject& obj, const jmethodID& method,
    R* response) {
  return JniCall::instance_method(obj, method).Call(response);
}

template <typename T>
inline Status JniUtil::CallJniMethod(const jobject& obj, const jmethodID& method,
    const T& arg, jobject* response) {
  return JniCall::instance_method(obj, method).with_thrift_arg(arg).Call(response);
}

inline JniCall::JniCall(jmethodID method)
  : method_(method),
    env_(JniUtil::GetJNIEnv()) {
  status_ = frame_.push(env_);
}

template<class T>
inline JniCall& JniCall::with_thrift_arg(const T& arg) {
  if (!status_.ok()) return *this;
  jbyteArray bytes;
  status_ = SerializeThriftMsg(env_, &arg, &bytes);
  if (status_.ok()) {
    jvalue arg;
    memset(&arg, 0, sizeof(arg));
    arg.l = bytes;
    args_.emplace_back(arg);
  }
  return *this;
}
template<class T>
inline JniCall& JniCall::with_primitive_arg(T arg) {
  if (!status_.ok()) return *this;
  args_.emplace_back(PrimitiveToValue(arg));
  return *this;
}

template<class T>
inline Status JniCall::Call(T* result) {
  RETURN_IF_ERROR(status_);
  DCHECK((instance_ != nullptr) ^ (class_ != nullptr));

  // Even if the function takes no arguments, it's OK to pass an array here.
  // The JNI API doesn't take a length and just assumes that you've passed
  // an appropriate number of elements.
  jobject ret;
  if (class_) {
    ret = env_->CallStaticObjectMethodA(class_, method_, args_.data());
  } else {
    ret = env_->CallObjectMethodA(instance_, method_, args_.data());
  }
  RETURN_ERROR_IF_EXC(env_);
  RETURN_IF_ERROR(ObjectToResult(ret, result));
  return Status::OK();
}

template<class T>
inline Status JniCall::ObjectToResult(jobject obj, T* result) {
  DCHECK(obj) << "Call returned unexpected null Thrift object";
  RETURN_IF_ERROR(DeserializeThriftMsg(env_, static_cast<jbyteArray>(obj), result));
  return Status::OK();
}

inline Status JniCall::ObjectToResult(jobject obj, void* no_result) {
  return Status::OK();
}

inline Status JniCall::ObjectToResult(jobject obj, std::string* result) {
  DCHECK(obj) << "Call returned unexpected null String instance";
  JniUtfCharGuard utf;
  RETURN_IF_ERROR(JniUtfCharGuard::create(env_, static_cast<jstring>(obj), &utf));
  *result = utf.get();;
  return Status::OK();
}

inline Status JniCall::ObjectToResult(jobject obj, jobject* result) {
  DCHECK(obj) << "Call returned unexpected null Thrift object";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, obj, result));
  return Status::OK();
}

}

#endif
