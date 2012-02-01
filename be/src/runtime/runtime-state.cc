// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <sstream>

#include <boost/algorithm/string/join.hpp>
#include "common/object-pool.h"
#include "runtime/descriptors.h"
#include "common/status.h"
#include "runtime/runtime-state.h"
#include "util/jni-util.h"

#include <jni.h>
#include <iostream>

using namespace std;
using namespace boost::algorithm;

namespace impala {

void* RuntimeState::hbase_conf_ = NULL;

RuntimeState::RuntimeState(
    const TUniqueId& query_id, bool abort_on_error, int max_errors,
    DataStreamMgr* stream_mgr, HdfsFsCache* fs_cache)
  : obj_pool_(new ObjectPool()),
    batch_size_(DEFAULT_BATCH_SIZE),
    file_buffer_size_(DEFAULT_FILE_BUFFER_SIZE),
    abort_on_error_(abort_on_error),
    max_errors_(max_errors),
    query_id_(query_id),
    stream_mgr_(stream_mgr),
    fs_cache_(fs_cache) {
}

string RuntimeState::ErrorLog() const {
  return join(error_log_, "\n");
}

string RuntimeState::FileErrors() const {
  stringstream out;
  for (int i = 0; i < file_errors_.size(); ++i) {
    out << file_errors_[i].second << " errors in " << file_errors_[i].first << endl;
  }
  return out.str();
}

void RuntimeState::ReportFileErrors(const std::string& file_name, int num_errors) {
  file_errors_.push_back(make_pair(file_name, num_errors));
}

void RuntimeState::LogErrorStream() {
  error_log_.push_back(error_stream_.str());
  // Clear content of stream.
  error_stream_.str("");
  // Clear the ios error flags, if any.
  error_stream_.clear();
}

Status RuntimeState::InitHBaseConf() {
  hbase_conf_ = NULL;
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }
  // TODO: Redirect all LOG4J messages to a file.
  // hbase_conf_ = HBaseConfiguration.create();
  jmethodID throwable_to_string_id = JniUtil::throwable_to_string_id();
  jclass hbase_conf_cl_ = env->FindClass("org/apache/hadoop/hbase/HBaseConfiguration");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);
  jmethodID hbase_conf_create_id_ =
      env->GetStaticMethodID(hbase_conf_cl_, "create", "()Lorg/apache/hadoop/conf/Configuration;");
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);
  jobject local_hbase_conf = env->CallStaticObjectMethod(hbase_conf_cl_, hbase_conf_create_id_);
  RETURN_IF_ERROR(
      JniUtil::LocalToGlobalRef(env, local_hbase_conf, reinterpret_cast<jobject*>(&hbase_conf_)));
  env->DeleteLocalRef(local_hbase_conf);
  RETURN_ERROR_IF_EXC(env, throwable_to_string_id);
  return Status::OK;
}

}
