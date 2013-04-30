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
#include "runtime/hbase-table.h"
#include "util/jni-util.h"

using namespace std;
using namespace boost;

namespace impala {

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
  jclass hbase_conf_cl =
      env->FindClass("org/apache/hadoop/hbase/HBaseConfiguration");
  RETURN_ERROR_IF_EXC(env);

  jmethodID hbase_conf_create_id =
      env->GetStaticMethodID(hbase_conf_cl, "create",
          "()Lorg/apache/hadoop/conf/Configuration;");
  RETURN_ERROR_IF_EXC(env);

  jobject local_conf =
      env->CallStaticObjectMethod(hbase_conf_cl, hbase_conf_create_id);
  RETURN_IF_ERROR(
      JniUtil::LocalToGlobalRef(env, local_conf, &conf_));
  env->DeleteLocalRef(local_conf);
  RETURN_ERROR_IF_EXC(env);

  // ExecutorService from java.util.concurrent.Executors#newCachedThreadPool
  // Get an executor that can spin up and down threads; this should allow
  // repeated calls to HBase to be fast but still mean no extra memory or
  // threads are needed if there have been no HBase scans in a while.
  jclass executors_cl = env->FindClass("java/util/concurrent/Executors");
  RETURN_ERROR_IF_EXC(env);

  jmethodID executors_cached_thread = env->GetStaticMethodID(executors_cl,
      "newCachedThreadPool", "()Ljava/util/concurrent/ExecutorService;");
  RETURN_ERROR_IF_EXC(env);

  // Now create a single executor that will be used for all HTable's.
  jobject local_executor =
      env->CallStaticObjectMethod(executors_cl, executors_cached_thread);
  RETURN_ERROR_IF_EXC(env);

  // Make sure the GC doesn't clean up the executor.
  RETURN_IF_ERROR(
      JniUtil::LocalToGlobalRef(env, local_executor, &executor_));

  env->DeleteLocalRef(local_executor);
  RETURN_ERROR_IF_EXC(env);

  // Executor class and methods for shutdown.
  executor_cl_ = env->FindClass("java/util/concurrent/ExecutorService");
  RETURN_ERROR_IF_EXC(env);

  executor_shutdown_id_ = env->GetMethodID(executor_cl_, "shutdownNow",
      "()Ljava/util/List;");
  RETURN_ERROR_IF_EXC(env);

  RETURN_IF_ERROR(HBaseTable::InitJNI());
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

Status HBaseTableFactory::GetTable(const string& table_name,
                                   scoped_ptr<HBaseTable>* hbase_table) {
  hbase_table->reset(new HBaseTable(table_name, conf_, executor_));
  RETURN_IF_ERROR((*hbase_table)->Init());
  return Status::OK;
}

}  // namespace impala
