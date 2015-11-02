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

#include "catalog/catalog.h"

#include <list>
#include <string>

#include "common/logging.h"
#include "rpc/jni-thrift-util.h"
#include "util/logging-support.h"

#include "common/names.h"

using namespace impala;

DEFINE_bool(load_catalog_in_background, false,
    "If true, loads catalog metadata in the background. If false, metadata is loaded "
    "lazily (on access).");
DEFINE_int32(num_metadata_loading_threads, 16,
    "(Advanced) The number of metadata loading threads (degree of parallelism) to use "
    "when loading catalog metadata.");
DEFINE_string(sentry_config, "", "Local path to a sentry-site.xml configuration "
    "file. If set, authorization will be enabled.");

DECLARE_int32(non_impala_java_vlog);

Catalog::Catalog() {
  JniMethodDescriptor methods[] = {
    {"<init>", "(ZILjava/lang/String;II)V", &catalog_ctor_},
    {"updateCatalog", "([B)[B", &update_metastore_id_},
    {"execDdl", "([B)[B", &exec_ddl_id_},
    {"resetMetadata", "([B)[B", &reset_metadata_id_},
    {"getTableNames", "([B)[B", &get_table_names_id_},
    {"getDbs", "([B)[B", &get_dbs_id_},
    {"getFunctions", "([B)[B", &get_functions_id_},
    {"checkUserSentryAdmin", "([B)V", &sentry_admin_check_id_},
    {"getCatalogObject", "([B)[B", &get_catalog_object_id_},
    {"getCatalogObjects", "(J)[B", &get_catalog_objects_id_},
    {"getCatalogVersion", "()J", &get_catalog_version_id_},
    {"prioritizeLoad", "([B)V", &prioritize_load_id_}};

  JNIEnv* jni_env = getJNIEnv();
  // Create an instance of the java class JniCatalog
  catalog_class_ = jni_env->FindClass("com/cloudera/impala/service/JniCatalog");
  EXIT_IF_EXC(jni_env);

  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    EXIT_IF_ERROR(JniUtil::LoadJniMethod(jni_env, catalog_class_, &(methods[i])));
  }

  jboolean load_in_background = FLAGS_load_catalog_in_background;
  jint num_metadata_loading_threads = FLAGS_num_metadata_loading_threads;
  jstring sentry_config = jni_env->NewStringUTF(FLAGS_sentry_config.c_str());
  jobject catalog = jni_env->NewObject(catalog_class_, catalog_ctor_,
      load_in_background, num_metadata_loading_threads, sentry_config,
      FlagToTLogLevel(FLAGS_v), FlagToTLogLevel(FLAGS_non_impala_java_vlog));
  EXIT_IF_EXC(jni_env);
  EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, catalog, &catalog_));
}

Status Catalog::GetCatalogObject(const TCatalogObject& req,
    TCatalogObject* resp) {
  return JniUtil::CallJniMethod(catalog_, get_catalog_object_id_, req, resp);
}

Status Catalog::GetCatalogVersion(long* version) {
  JNIEnv* jni_env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  *version = jni_env->CallLongMethod(catalog_, get_catalog_version_id_);
  return Status::OK();
}

Status Catalog::GetAllCatalogObjects(long from_version,
    TGetAllCatalogObjectsResponse* resp) {
  JNIEnv* jni_env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  jvalue requested_from_version;
  requested_from_version.j = from_version;
  jbyteArray result_bytes = static_cast<jbyteArray>(
      jni_env->CallObjectMethod(catalog_, get_catalog_objects_id_,
      requested_from_version));
  RETURN_ERROR_IF_EXC(jni_env);
  RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, resp));
  return Status::OK();
}

Status Catalog::ExecDdl(const TDdlExecRequest& req, TDdlExecResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, exec_ddl_id_, req, resp);
}

Status Catalog::ResetMetadata(const TResetMetadataRequest& req,
    TResetMetadataResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, reset_metadata_id_, req, resp);
}

Status Catalog::UpdateCatalog(const TUpdateCatalogRequest& req,
    TUpdateCatalogResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, update_metastore_id_, req, resp);
}

Status Catalog::GetDbs(const string* pattern, TGetDbsResult* dbs) {
  TGetDbsParams params;
  if (pattern != NULL) params.__set_pattern(*pattern);
  return JniUtil::CallJniMethod(catalog_, get_dbs_id_, params, dbs);
}

Status Catalog::GetTableNames(const string& db, const string* pattern,
    TGetTablesResult* table_names) {
  TGetTablesParams params;
  params.__set_db(db);
  if (pattern != NULL) params.__set_pattern(*pattern);
  return JniUtil::CallJniMethod(catalog_, get_table_names_id_, params, table_names);
}

Status Catalog::GetFunctions(const TGetFunctionsRequest& request,
    TGetFunctionsResponse *response) {
  return JniUtil::CallJniMethod(catalog_, get_functions_id_, request, response);
}

Status Catalog::PrioritizeLoad(const TPrioritizeLoadRequest& req) {
  return JniUtil::CallJniMethod(catalog_, prioritize_load_id_, req);
}

Status Catalog::SentryAdminCheck(const TSentryAdminCheckRequest& req) {
  return JniUtil::CallJniMethod(catalog_, sentry_admin_check_id_, req);
}
