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

#include "exec/external-data-source-executor.h"

#include <list>
#include <string>

#include "common/logging.h"
#include "rpc/jni-thrift-util.h"
#include "runtime/exec-env.h"
#include "runtime/lib-cache.h"
#include "util/parse-util.h"
#include "util/metrics.h"

#include "common/names.h"

using namespace impala;
using namespace impala::extdatasource;

/// Static state shared across instances of the JNI ExternalDataSourceExecutor.
class ExternalDataSourceExecutor::JniState {
 public:
  /// Gets the singleton instance. Creation of the instance is not thread-safe
  /// (until C++11), but called once by ExternalDataSourceExecutor::Init() on startup,
  /// at which time JniState::Init() (see below) is called once.
  static ExternalDataSourceExecutor::JniState& GetInstance() {
    static ExternalDataSourceExecutor::JniState state;
    return state;
  }

  /// Initializes the JniState. Called exactly once by ExternalDataSourceExecutor::Init()
  /// process startup.
  Status Init(MetricGroup* metrics) {
    DCHECK(executor_class_ == NULL) << "JniState was already initialized.";
    JniMethodDescriptor methods[] = {
      {"<init>",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
        &ctor_},
      {"open", "([B)[B", &open_id_},
      {"getNext", "([B)[B", &get_next_id_},
      {"close", "([B)[B", &close_id_}};

    JNIEnv* env = JniUtil::GetJNIEnv();

    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(env));

    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
        "org/apache/impala/extdatasource/ExternalDataSourceExecutor",
        &executor_class_));
    uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
    for (int i = 0; i < num_methods; ++i) {
      RETURN_IF_ERROR(JniUtil::LoadJniMethod(env, executor_class_, &(methods[i])));
    }

    get_num_cache_hits_id_ = env->GetStaticMethodID(executor_class_,
        "getNumClassCacheHits", "()J");
    RETURN_ERROR_IF_EXC(env);
    get_num_cache_misses_id_ = env->GetStaticMethodID(executor_class_,
        "getNumClassCacheMisses", "()J");
    RETURN_ERROR_IF_EXC(env);

    num_class_cache_hits_ = metrics->AddCounter(
        "external-data-source.class-cache.hits", 0);
    num_class_cache_misses_ = metrics->AddCounter(
        "external-data-source.class-cache.misses", 0);
    return Status::OK();
  }


  /// Updates the class cache metrics via the static JNI methods on
  /// ExternalDataSourceExecutor.
  Status UpdateClassCacheMetrics() const {
    DCHECK(executor_class_ != NULL) << "JniState was not initialized.";
    JNIEnv* env = JniUtil::GetJNIEnv();
    int64_t num_cache_hits = env->CallStaticLongMethod(executor_class_,
        get_num_cache_hits_id_);
    RETURN_ERROR_IF_EXC(env);
    num_class_cache_hits_->SetValue(num_cache_hits);
    int64_t num_cache_misses = env->CallStaticLongMethod(executor_class_,
        get_num_cache_misses_id_);
    RETURN_ERROR_IF_EXC(env);
    num_class_cache_misses_->SetValue(num_cache_misses);
    return Status::OK();
  }

  /// Class reference for org.apache.impala.extdatasource.ExternalDataSourceExecutor
  jclass executor_class_;

  jmethodID ctor_;
  jmethodID open_id_;  // ExternalDataSourceExecutor.open()
  jmethodID get_next_id_;  // ExternalDataSourceExecutor.getNext()
  jmethodID close_id_;  // ExternalDataSourceExecutor.close()

  // Static methods for getting the number of class cache hits/misses.
  jmethodID get_num_cache_hits_id_;
  jmethodID get_num_cache_misses_id_;

  IntCounter* num_class_cache_hits_;
  IntCounter* num_class_cache_misses_;

 private:
  JniState() : executor_class_(NULL) { }

  DISALLOW_COPY_AND_ASSIGN(JniState);
};

Status ExternalDataSourceExecutor::InitJNI(MetricGroup* metrics) {
  // Initializes the JniState singleton and initializes the metric values.
  JniState& s = JniState::GetInstance();
  RETURN_IF_ERROR(s.Init(metrics));
  RETURN_IF_ERROR(s.UpdateClassCacheMetrics());
  return Status::OK();
}

ExternalDataSourceExecutor::~ExternalDataSourceExecutor() {
  DCHECK(!is_initialized_);
}

Status ExternalDataSourceExecutor::Init(const string& jar_path,
    const string& class_name, const string& api_version, const string& init_string) {
  DCHECK(!is_initialized_);
  LibCacheEntryHandle handle;
  string local_jar_path;
  // TODO(IMPALA-6727): pass the mtime from the coordinator. for now, skip the mtime
  // check (-1).
  if (!jar_path.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetLocalPath(
        jar_path, LibCache::TYPE_JAR, -1, &handle, &local_jar_path));
  }
  JNIEnv* jni_env = JniUtil::GetJNIEnv();

  // Add a scoped cleanup jni reference object. This cleans up local refs made below.
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));

  jstring jar_path_jstr = jni_env->NewStringUTF(local_jar_path.c_str());
  RETURN_ERROR_IF_EXC(jni_env);
  jstring class_name_jstr = jni_env->NewStringUTF(class_name.c_str());
  RETURN_ERROR_IF_EXC(jni_env);
  jstring api_version_jstr = jni_env->NewStringUTF(api_version.c_str());
  RETURN_ERROR_IF_EXC(jni_env);
  jstring init_string_jstr = jni_env->NewStringUTF(init_string.c_str());
  RETURN_ERROR_IF_EXC(jni_env);

  const JniState& s = JniState::GetInstance();
  jobject local_exec = jni_env->NewObject(s.executor_class_, s.ctor_, jar_path_jstr,
      class_name_jstr, api_version_jstr, init_string_jstr);
  RETURN_ERROR_IF_EXC(jni_env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, local_exec, &executor_));
  RETURN_IF_ERROR(s.UpdateClassCacheMetrics());
  is_initialized_ = true;
  return Status::OK();
}

// JniUtil::CallJniMethod() does not compile when the template parameters are in
// another namespace. The issue seems to be that SerializeThriftMsg/DeserializeThriftMsg
// are not being generated for these types.
// TODO: Understand what's happening, remove, and use JniUtil::CallJniMethod
template <typename T, typename R>
Status CallJniMethod(const jobject& obj, const jmethodID& method, const T& arg,
    R* response) {
  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  jbyteArray request_bytes;
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &arg, &request_bytes));
  jbyteArray result_bytes = static_cast<jbyteArray>(
      jni_env->CallObjectMethod(obj, method, request_bytes));
  RETURN_ERROR_IF_EXC(jni_env);
  RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, response));
  return Status::OK();
}

Status ExternalDataSourceExecutor::Open(const TOpenParams& params, TOpenResult* result) {
  DCHECK(is_initialized_);
  const JniState& s = JniState::GetInstance();
  return CallJniMethod(executor_, s.open_id_, params, result);
}

Status ExternalDataSourceExecutor::GetNext(const TGetNextParams& params,
    TGetNextResult* result) {
  DCHECK(is_initialized_);
  const JniState& s = JniState::GetInstance();
  return CallJniMethod(executor_, s.get_next_id_, params, result);
}

Status ExternalDataSourceExecutor::Close(const TCloseParams& params,
    TCloseResult* result) {
  DCHECK(is_initialized_);
  const JniState& s = JniState::GetInstance();
  Status status = CallJniMethod(executor_, s.close_id_, params,
      result);
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (executor_ != NULL) env->DeleteGlobalRef(executor_);
  is_initialized_ = false;
  return status;
}
