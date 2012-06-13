// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/hbase-table-cache.h"
#include "util/jni-util.h"
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

using namespace std;
using namespace boost;
using namespace impala;

jclass HBaseTableCache::htable_cl_ = NULL;
jmethodID HBaseTableCache::htable_ctor_ = NULL;
jmethodID HBaseTableCache::htable_close_id_ = NULL;

namespace impala {

void* HBaseTableCache::hbase_conf_ = NULL;

Status HBaseTableCache::Init() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }

  // TODO: Redirect all LOG4J messages to a file.
  // hbase_conf_ = HBaseConfiguration.create();
  hbase_conf_ = NULL;
  jmethodID throwable_to_string_id = JniUtil::throwable_to_string_id();
  jclass hbase_conf_cl_ = env->FindClass("org/apache/hadoop/hbase/HBaseConfiguration");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);
  jmethodID hbase_conf_create_id_ =
      env->GetStaticMethodID(hbase_conf_cl_, "create",
          "()Lorg/apache/hadoop/conf/Configuration;");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);
  jobject local_hbase_conf =
      env->CallStaticObjectMethod(hbase_conf_cl_, hbase_conf_create_id_);
  RETURN_IF_ERROR(
      JniUtil::LocalToGlobalRef(env, local_hbase_conf,
          reinterpret_cast<jobject*>(&hbase_conf_)));
  env->DeleteLocalRef(local_hbase_conf);
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);

  // Global class references:
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/HTable",
          &htable_cl_));

  htable_ctor_ = env->GetMethodID(htable_cl_, "<init>",
      "(Lorg/apache/hadoop/conf/Configuration;Ljava/lang/String;)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  htable_close_id_ = env->GetMethodID(htable_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  return Status::OK;
}

HBaseTableCache::~HBaseTableCache() {
  JNIEnv* env = getJNIEnv();
  for (HTableMap::iterator i = table_map_.begin(); i != table_map_.end(); ++i) {
    env->CallObjectMethod(i->second, htable_close_id_);
    env->DeleteGlobalRef(i->second);
  }
}

jobject HBaseTableCache::GetHBaseTable(const string& table_name) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) return NULL;

  lock_guard<mutex> l(lock_);
  HTableMap::iterator i = table_map_.find(table_name);
  if (i == table_map_.end()) {
    LOG(INFO) << "gethbasetable: name=" << table_name;
    jstring jtable_name = env->NewStringUTF(table_name.c_str());
    LOG(INFO) << "env=" << env << " htable_cl=" << htable_cl_
              << " htable_ctor_=" << htable_ctor_
              << " hbase_conf=" << hbase_conf_
              << " jtable=" << jtable_name;
    jobject htable = env->NewObject(htable_cl_, htable_ctor_, hbase_conf_, jtable_name);
    jobject global_htable = env->NewGlobalRef(htable);
    table_map_.insert(make_pair(table_name, global_htable));
    return global_htable;
  } else {
    return i->second;
  }
}

}
