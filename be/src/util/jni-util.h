// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_JNI_UTIL_H
#define IMPALA_UTIL_JNI_UTIL_H

#include <jni.h>
#include <vector>

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

#define THROW_IF_EXC(env, exc_class, throwable_to_string_id_) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
     jstring msg = (jstring) env->CallObjectMethod(exc, (throwable_to_string_id_)); \
     jboolean is_copy; \
     const char* c_msg = reinterpret_cast<const char*>((env)->GetStringUTFChars(msg, &is_copy)); \
     (env)->ExceptionClear(); \
     (env)->ThrowNew((exc_class), c_msg); \
     return; \
    } \
  } while (false)

#define RETURN_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
      (env)->ExceptionDescribe(); \
     return; \
    } \
  } while (false)

#define EXIT_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
      (env)->ExceptionDescribe(); \
     exit(1); \
    } \
  } while (false)

#define RETURN_ERROR_IF_EXC(env, throwable_to_string_id_) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
     jstring msg = (jstring) env->CallObjectMethod(exc, (throwable_to_string_id_)); \
     jboolean is_copy; \
     const char* c_msg = \
         reinterpret_cast<const char*>((env)->GetStringUTFChars(msg, &is_copy)); \
     (env)->ExceptionClear(); \
     return Status(c_msg); \
    } \
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

  // Find Throwable class, and get Throwable toString() method id.
  static Status Init();

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

  // Delete all global references: class members, and those stored in global_refs_.
  static Status Cleanup();

 private:
  static jclass throwable_cl_;
  static jmethodID throwable_to_string_id_;
  // List of global references created with GetGlobalClassRef() or LocalToGlobalRef.
  // All global references are deleted in Cleanup().
  static std::vector<jobject> global_refs_;
};

}

#endif
