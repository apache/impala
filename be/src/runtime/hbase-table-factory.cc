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

#include "runtime/hbase-table-factory.h"

#include "common/status.h"
#include "common/logging.h"
#include "util/jni-util.h"

using namespace std;

namespace impala {

jclass HBaseTableFactory::htable_cl_ = NULL;
jmethodID HBaseTableFactory::htable_ctor_ = NULL;
jmethodID HBaseTableFactory::htable_close_id_ = NULL;

jclass HBaseTableFactory::bytes_cl_ = NULL;
jmethodID HBaseTableFactory::bytes_to_bytes_id_ = NULL;

jclass HBaseTableFactory::executor_cl_ = NULL;
jmethodID HBaseTableFactory::executor_shutdown_id_ = NULL;

jobject HBaseTableFactory::conf_ = NULL;
jobject HBaseTableFactory::executor_ = NULL;

Status HBaseTableFactory::Init() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }

  // Get the Configuration and an ExecutorService.
  // These will remain the same across all threads.
  // Get o.a.h.Configuration via HBaseConfiguration
  jmethodID throwable_to_string_id = JniUtil::throwable_to_string_id();
  jclass hbase_conf_cl =
      env->FindClass("org/apache/hadoop/hbase/HBaseConfiguration");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  jmethodID hbase_conf_create_id =
      env->GetStaticMethodID(hbase_conf_cl, "create",
          "()Lorg/apache/hadoop/conf/Configuration;");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  jobject local_conf =
      env->CallStaticObjectMethod(hbase_conf_cl, hbase_conf_create_id);
  RETURN_IF_ERROR(
      JniUtil::LocalToGlobalRef(env, local_conf, &conf_));
  env->DeleteLocalRef(local_conf);
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  // ExecutorService from java.util.concurrent.Executors#newCachedThreadPool
  // Get an executor that can spin up and down threads; this should allow
  // repeated calls to HBase to be fast but still mean no extra memory or
  // threads are needed if there have been no HBase scans in a while.
  jclass executors_cl = env->FindClass("java/util/concurrent/Executors");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  jmethodID executors_cached_thread = env->GetStaticMethodID(executors_cl,
      "newCachedThreadPool", "()Ljava/util/concurrent/ExecutorService;");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  // Now create a single executor that will be used for all HTable's.
  jobject local_executor =
      env->CallStaticObjectMethod(executors_cl, executors_cached_thread);
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  // Make sure the GC doesn't clean up the executor.
  RETURN_IF_ERROR(
      JniUtil::LocalToGlobalRef(env, local_executor, &executor_));

  env->DeleteLocalRef(local_executor);
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  // Executor class and methods for shutdown.
  executor_cl_ = env->FindClass("java/util/concurrent/ExecutorService");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  executor_shutdown_id_ = env->GetMethodID(executor_cl_, "shutdownNow",
      "()Ljava/util/List;");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  // Bytes used to get String -> Java Byte Array the same way HBase will.
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/util/Bytes",
          &bytes_cl_));

  bytes_to_bytes_id_ = env->GetStaticMethodID(bytes_cl_, "toBytes",
      "(Ljava/lang/String;)[B");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  // HTable
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/HTable",
          &htable_cl_));

  htable_ctor_ = env->GetMethodID(htable_cl_, "<init>",
      "(Lorg/apache/hadoop/conf/Configuration;"
      "[BLjava/util/concurrent/ExecutorService;)V");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  htable_close_id_ = env->GetMethodID(htable_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  return Status::OK;
}

HBaseTableFactory::~HBaseTableFactory() {
  JNIEnv* env = getJNIEnv();

  // Clean up the global refs and stop the threads.
  if (conf_ != NULL) {
    env->DeleteGlobalRef(conf_);
  }

  if (executor_ != NULL && executor_shutdown_id_ != NULL) {
    env->CallObjectMethod(executor_, executor_shutdown_id_);
    env->DeleteGlobalRef(executor_);
  }
}

Status HBaseTableFactory::GetHBaseTable(const string& table_name,
                                        jobject* global_htable) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) return Status("Error creating JNIEnv");

  // Get the java string for the table name
  jstring jtable_name_string = env->NewStringUTF(table_name.c_str());
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  // Use o.a.h.hbase.util.Bytes.toBytes to convert into a byte array.
  jobject jtable_name = env->CallStaticObjectMethod(bytes_cl_,
      bytes_to_bytes_id_, jtable_name_string);
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Create the HTable.
  jobject local_htable = env->NewObject(htable_cl_,
      htable_ctor_, conf_, jtable_name, executor_);
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Make sure the GC doesn't remove the HTable until told to.
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_htable, global_htable));

  // Now clean up the un-needed refs.
  env->DeleteLocalRef(jtable_name_string);
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  env->DeleteLocalRef(jtable_name);
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  return Status::OK;
}

Status HBaseTableFactory::CloseHTable(const jobject& htable) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) return Status("Could not create JNIEnv");
  env->CallObjectMethod(htable, htable_close_id_);
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  env->DeleteGlobalRef(htable);
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  return Status::OK;
}

}  // namespace impala
