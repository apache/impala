// Copyright 2014 Cloudera Inc.
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

#include "exec/external-data-source-executor.h"

#include <list>
#include <string>

#include "common/logging.h"
#include "rpc/thrift-util.h"
#include "runtime/lib-cache.h"
#include "util/jni-util.h"
#include "util/parse-util.h"

using namespace std;
using namespace impala;
using namespace impala::extdatasource;

ExternalDataSourceExecutor::~ExternalDataSourceExecutor() {
  DCHECK(!is_initialized_);
}

Status ExternalDataSourceExecutor::Init(const string& jar_path,
    const string& class_name, const string& api_version) {
  DCHECK(!is_initialized_);
  string local_jar_path;
  RETURN_IF_ERROR(LibCache::instance()->GetLocalLibPath(
      jar_path, LibCache::TYPE_JAR, &local_jar_path));

  // TODO: Make finding the class and methods static, i.e. only loaded once
  JniMethodDescriptor methods[] = {
    {"<init>", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V", &ctor_},
    {"open", "([B)[B", &open_id_},
    {"getNext", "([B)[B", &get_next_id_},
    {"close", "([B)[B", &close_id_}};

  JNIEnv* jni_env = getJNIEnv();
  external_data_source_executor_class_ =
    jni_env->FindClass("com/cloudera/impala/extdatasource/ExternalDataSourceExecutor");
  RETURN_ERROR_IF_EXC(jni_env);
  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    RETURN_IF_ERROR(JniUtil::LoadJniMethod(jni_env, external_data_source_executor_class_,
        &(methods[i])));
  }

  jstring jar_path_jstr = jni_env->NewStringUTF(local_jar_path.c_str());
  RETURN_ERROR_IF_EXC(jni_env);
  jstring class_name_jstr = jni_env->NewStringUTF(class_name.c_str());
  RETURN_ERROR_IF_EXC(jni_env);
  jstring api_version_jstr = jni_env->NewStringUTF(api_version.c_str());
  RETURN_ERROR_IF_EXC(jni_env);

  jobject external_data_source_executor = jni_env->NewObject(
      external_data_source_executor_class_, ctor_, jar_path_jstr, class_name_jstr,
      api_version_jstr);
  RETURN_ERROR_IF_EXC(jni_env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, external_data_source_executor,
      &external_data_source_executor_));
  RETURN_ERROR_IF_EXC(jni_env);
  is_initialized_ = true;
  return Status::OK;
}

// JniUtil::CallJniMethod() does not compile when the template parameters are in
// another namespace. The issue seems to be that SerializeThriftMsg/DeserializeThriftMsg
// are not being generated for these types.
// TODO: Understand what's happening, remove, and use JniUtil::CallJniMethod
template <typename T, typename R>
Status CallJniMethod(const jobject& obj, const jmethodID& method, const T& arg,
    R* response) {
  JNIEnv* jni_env = getJNIEnv();
  jbyteArray request_bytes;
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &arg, &request_bytes));
  jbyteArray result_bytes = static_cast<jbyteArray>(
      jni_env->CallObjectMethod(obj, method, request_bytes));
  RETURN_ERROR_IF_EXC(jni_env);
  RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, response));
  return Status::OK;
}

Status ExternalDataSourceExecutor::Open(const TOpenParams& params, TOpenResult* result) {
  DCHECK(is_initialized_);
  return CallJniMethod(external_data_source_executor_, open_id_, params, result);
}

Status ExternalDataSourceExecutor::GetNext(const TGetNextParams& params,
    TGetNextResult* result) {
  DCHECK(is_initialized_);
  return CallJniMethod(external_data_source_executor_, get_next_id_, params, result);
}

Status ExternalDataSourceExecutor::Close(const TCloseParams& params,
    TCloseResult* result) {
  DCHECK(is_initialized_);
  Status status = CallJniMethod(external_data_source_executor_, close_id_, params,
      result);
  JNIEnv* env = getJNIEnv();
  env->DeleteGlobalRef(external_data_source_executor_);
  status.AddError(JniUtil::GetJniExceptionMsg(env)); // no-op if Status == OK
  is_initialized_ = false;
  return status;
}

