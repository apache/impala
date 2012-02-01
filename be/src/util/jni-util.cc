// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "util/jni-util.h"
#include <hdfs.h>
#include "common/status.h"

using namespace std;

namespace impala {

jclass JniUtil::throwable_cl_ = NULL;
jmethodID JniUtil::throwable_to_string_id_ = NULL;
vector<jobject> JniUtil::global_refs_;

Status JniUtil::GetGlobalClassRef(JNIEnv* env, const char* class_str, jclass* class_ref) {
  *class_ref = NULL;
  jclass local_cl = env->FindClass(class_str);
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id_);
  RETURN_IF_ERROR(LocalToGlobalRef(env, reinterpret_cast<jobject>(local_cl),
      reinterpret_cast<jobject*>(class_ref)));
  env->DeleteLocalRef(local_cl);
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id_);
  return Status::OK;
}

Status JniUtil::LocalToGlobalRef(JNIEnv* env, jobject local_ref, jobject* global_ref) {
  *global_ref = env->NewGlobalRef(local_ref);
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id_);
  global_refs_.push_back(*global_ref);
  return Status::OK;
}

Status JniUtil::Init() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }

  // Throwable
  jclass local_throwable_cl = env->FindClass("java/lang/Throwable");
  if (local_throwable_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find Java Throwable class.");
  }
  throwable_cl_ = reinterpret_cast<jclass>(env->NewGlobalRef(local_throwable_cl));
  if (throwable_cl_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to create global reference to Java Throwable class.");
  }
  env->DeleteLocalRef(local_throwable_cl);
  if (env->ExceptionOccurred()) {
    return Status("Failed to delete local reference to Java Throwable class.");
  }

  // Throwable toString()
  throwable_to_string_id_ = env->GetMethodID(throwable_cl_, "toString", "()Ljava/lang/String;");
  if (throwable_to_string_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find Java Throwable toString() method.");
  }

  return Status::OK;
}

void JniUtil::InitLibhdfs() {
  // make random libhdfs calls to make sure that the context class loader isn't
  // null; see xxx for an explanation
  hdfsFS fs = hdfsConnect("default", 0);
  (void) hdfsOpenFile(fs, "invalid/path", O_RDONLY, 0, 0, 0);
  hdfsDisconnect(fs);
}

Status JniUtil::Cleanup() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }
  vector<jobject>::iterator it;
  for (it = global_refs_.begin(); it != global_refs_.end(); ++it) {
    env->DeleteGlobalRef(*it);
  }
  global_refs_.clear();
  return Status::OK;
}

}
