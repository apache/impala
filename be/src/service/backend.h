// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_BACKEND_H
#define IMPALA_SERVICE_BACKEND_H

#include <jni.h>

// JNI-interface method called upon loading this native library.
extern "C"
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* pvt);

// JNI-interface method called upon unloading this native library.
extern "C"
JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* pvt);

extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativeBackend_InitThread(
    JNIEnv* env, jclass caller_class);

// JNI-callable wrapper to the plan executor
// protected native static void ExecPlan(byte[] thriftExecutePlanRequest,
//      List<String> errorLog, Map<String, Integer> fileErrors,
//      BlockingQueue<TResultRow> resultQueue) throws ImpalaException;
extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativeBackend_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jobject error_log, jobject file_errors, jobject result_queue);

extern "C"
JNIEXPORT jboolean JNICALL Java_com_cloudera_impala_service_NativeBackend_EvalPredicate(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_predicate);

#endif
