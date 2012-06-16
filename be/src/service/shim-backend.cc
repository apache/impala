// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
//
// In a situation where impalad->Java Frontend->libbackend (for predicate evaluation),
// there's a problem where several modules can get accidentally initialised twice. Where
// possible, we should make those modules 'reentrant', but for both glog and gflags it's
// not possible to do so, and loading the backend library twice causes crash failures.
//
// This file introduces a very thin shim library between Java and libbackend. The idea is
// that if any of the JNI interface symbols have already been loaded that there's no need
// to do so again, so we use RTLD_NOLOAD to query the current process and only load the
// symbol table again. If the JNI symbols can't be found, we can safely load
// libbackend. This situation occurs when we're using a standalone PlanService, which
// isn't invoked from an impalad context, so the backend symbols haven't been loaded.

#include <dlfcn.h>
#include "util/jni-util.h"
#include <errno.h>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <sstream>
#include <stdlib.h>

using namespace boost;
using namespace impala;
using namespace std;

mutex lib_initialization_lock;
bool func_ptrs_initialized = false;
void* backend_lib_handle = NULL;

jint (*backend_JNI_OnLoad)(JavaVM*, void*) = NULL;
void (*backend_JNI_OnUnload)(JavaVM*, void*) = NULL;
void (*backend_JNI_execQuery)(JNIEnv*, jclass, jbyteArray, jobject, jobject, jobject,
    jobject) = NULL;
jboolean (*backend_JNI_evalPredicate)(JNIEnv*, jclass, jbyteArray) = NULL;

// Sets up pointers to the real JNI interface by initially trying to load them from the
// current process. If that fails, tries to load libbackend.so directly. Returns false if
// function pointers can't ultimately be resolved. Is thread-safe.
bool InitJniFunctionPointers() {
  mutex::scoped_lock init_lock(lib_initialization_lock);
  if (func_ptrs_initialized) return true;

  // This reference is deliberately never freed, on the presumption
  // that we're going to use these methods for the lifetime of this
  // process
  backend_lib_handle = dlopen(NULL, RTLD_LAZY | RTLD_NOLOAD);

  backend_JNI_OnLoad =
      (jint (*)(JavaVM*, void*))dlsym(backend_lib_handle, "JNI_OnLoadImpl");

  if (backend_JNI_OnLoad == NULL) {
    // Try loading libbackend directly
    backend_lib_handle = dlopen("libbackend.so", RTLD_LAZY);
    if (backend_lib_handle == NULL) {
      return false;
    }
    backend_JNI_OnLoad = (jint (*)(JavaVM*, void*))dlsym(backend_lib_handle,
        "JNI_OnLoadImpl");
    if (backend_JNI_OnLoad == NULL) {
      return false;
    }
  }

  backend_JNI_OnUnload =
      (void (*)(JavaVM*, void*))dlsym(backend_lib_handle, "JNI_OnUnloadImpl");

  if (backend_JNI_OnUnload == NULL) return false;

  backend_JNI_execQuery = (void (*)(JNIEnv*, jclass, jbyteArray, jobject, jobject,
      jobject, jobject))dlsym(backend_lib_handle,
      "NativeBackend_ExecQueryImpl");

  if (backend_JNI_execQuery == NULL) return false;

  backend_JNI_evalPredicate =
      (jboolean (*)(JNIEnv*, jclass, jbyteArray))dlsym(backend_lib_handle,
      "NativeBackend_EvalPredicateImpl");

  if (backend_JNI_evalPredicate == NULL) return false;

  func_ptrs_initialized = true;
  return true;
}

extern "C"
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* pvt) {
  if (InitJniFunctionPointers() == false) {
    stringstream errmsg;
    errmsg << "Could not resolve function pointers: " << dlerror();
    JNIEnv* env;
    vm->GetEnv((void**)&env, JNI_VERSION_1_4);
    env->ThrowNew(env->FindClass("java/lang/UnsatisfiedLinkError"),
        errmsg.str().c_str());
    return 0;
  }
  return backend_JNI_OnLoad(vm, pvt);
}


extern "C"
JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* pvt) {
  DCHECK(func_ptrs_initialized);
  return backend_JNI_OnUnload(vm, pvt);
}


extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativeBackend_ExecQuery(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_query_exec_request,
    jobject error_log, jobject file_errors, jobject result_queue, jobject insert_result) {
  DCHECK(func_ptrs_initialized);
  return backend_JNI_execQuery(env, caller_class, thrift_query_exec_request, error_log,
      file_errors, result_queue, insert_result);
}

extern "C"
JNIEXPORT jboolean JNICALL Java_com_cloudera_impala_service_NativeBackend_EvalPredicate(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_predicate_bytes) {
  DCHECK(func_ptrs_initialized);
  return backend_JNI_evalPredicate(env, caller_class, thrift_predicate_bytes);
}
