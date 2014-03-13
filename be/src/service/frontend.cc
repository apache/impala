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

#include "service/frontend.h"

#include <list>
#include <string>

#include "common/logging.h"
#include "rpc/thrift-util.h"
#include "util/jni-util.h"
#include "util/logging-support.h"

using namespace std;
using namespace impala;

DECLARE_int32(non_impala_java_vlog);

DEFINE_bool(load_catalog_at_startup, false, "if true, load all catalog data at startup");

// Authorization related flags. Must be set to valid values to properly configure
// authorization.
DEFINE_string(server_name, "", "The name to use for securing this impalad "
    "server during authorization. If set, authorization will be enabled.");
DEFINE_string(authorization_policy_file, "", "HDFS path to the authorization policy "
    "file. If set, authorization will be enabled.");
DEFINE_string(authorization_policy_provider_class,
    "org.apache.sentry.provider.file.HadoopGroupResourceAuthorizationProvider",
    "Advanced: The authorization policy provider class name.");
DEFINE_string(authorized_proxy_user_config, "",
    "Specifies the set of authorized proxy users (users who can impersonate other "
    "users during authorization) and whom they are allowed to impersonate. "
    "Input is a semicolon-separated list of key=value pairs of authorized proxy "
    "users to the user(s) they can impersonate. These users are specified as a comma "
    "separated list of short usernames, or '*' to indicate all users. For example: "
    "hue=user1,user2;admin=*");

Frontend::Frontend() {
  JniMethodDescriptor methods[] = {
    {"<init>", "(ZLjava/lang/String;Ljava/lang/String;Ljava/lang/String;II)V", &fe_ctor_},
    {"createExecRequest", "([B)[B", &create_exec_request_id_},
    {"getExplainPlan", "([B)Ljava/lang/String;", &get_explain_plan_id_},
    {"getHadoopConfig", "(Z)Ljava/lang/String;", &get_hadoop_config_id_},
    {"checkConfiguration", "()Ljava/lang/String;", &check_config_id_},
    {"updateCatalogCache", "([B)[B", &update_catalog_cache_id_},
    {"getTableNames", "([B)[B", &get_table_names_id_},
    {"describeTable", "([B)[B", &describe_table_id_},
    {"showCreateTable", "([B)Ljava/lang/String;", &show_create_table_id_},
    {"getDbNames", "([B)[B", &get_db_names_id_},
    {"getStats", "([B)[B", &get_stats_id_},
    {"getFunctions", "([B)[B", &get_functions_id_},
    {"getCatalogObject", "([B)[B", &get_catalog_object_id_},
    {"execHiveServer2MetadataOp", "([B)[B", &exec_hs2_metadata_op_id_},
    {"setCatalogInitialized", "()V", &set_catalog_initialized_id_},
    {"loadTableData", "([B)[B", &load_table_data_id_}};

  JNIEnv* jni_env = getJNIEnv();
  // create instance of java class JniFrontend
  fe_class_ = jni_env->FindClass("com/cloudera/impala/service/JniFrontend");
  EXIT_IF_EXC(jni_env);

  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    EXIT_IF_ERROR(JniUtil::LoadJniMethod(jni_env, fe_class_, &(methods[i])));
  };

  jboolean lazy = (FLAGS_load_catalog_at_startup ? false : true);
  jstring policy_file_path =
      jni_env->NewStringUTF(FLAGS_authorization_policy_file.c_str());
  jstring server_name =
      jni_env->NewStringUTF(FLAGS_server_name.c_str());
  jstring policy_provider_class_name =
      jni_env->NewStringUTF(FLAGS_authorization_policy_provider_class.c_str());

  jobject fe = jni_env->NewObject(fe_class_, fe_ctor_, lazy, server_name,
      policy_file_path, policy_provider_class_name, FlagToTLogLevel(FLAGS_v),
      FlagToTLogLevel(FLAGS_non_impala_java_vlog));
  EXIT_IF_EXC(jni_env);
  EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, fe, &fe_));
}

Status Frontend::UpdateCatalogCache(const TUpdateCatalogCacheRequest& req,
    TUpdateCatalogCacheResponse* resp) {
  return JniUtil::CallJniMethod(fe_, update_catalog_cache_id_, req, resp);
}

Status Frontend::DescribeTable(const TDescribeTableParams& params,
    TDescribeTableResult* response) {
  return JniUtil::CallJniMethod(fe_, describe_table_id_, params, response);
}

Status Frontend::ShowCreateTable(const TTableName& table_name, string* response) {
  return JniUtil::CallJniMethod(fe_, show_create_table_id_, table_name, response);
}

Status Frontend::GetTableNames(const string& db, const string* pattern,
    const TSessionState* session, TGetTablesResult* table_names) {
  TGetTablesParams params;
  params.__set_db(db);
  if (pattern != NULL) params.__set_pattern(*pattern);
  if (session != NULL) params.__set_session(*session);
  return JniUtil::CallJniMethod(fe_, get_table_names_id_, params, table_names);
}

Status Frontend::GetDbNames(const string* pattern, const TSessionState* session,
    TGetDbsResult* db_names) {
  TGetDbsParams params;
  if (pattern != NULL) params.__set_pattern(*pattern);
  if (session != NULL) params.__set_session(*session);
  return JniUtil::CallJniMethod(fe_, get_db_names_id_, params, db_names);
}

Status Frontend::GetStats(const TShowStatsParams& params,
    TResultSet* result) {
  return JniUtil::CallJniMethod(fe_, get_stats_id_, params, result);
}

Status Frontend::GetFunctions(TFunctionType::type fn_type, const string& db,
    const string* pattern, const TSessionState* session, TGetFunctionsResult* functions) {
  TGetFunctionsParams params;
  params.__set_type(fn_type);
  params.__set_db(db);
  if (pattern != NULL) params.__set_pattern(*pattern);
  if (session != NULL) params.__set_session(*session);
  return JniUtil::CallJniMethod(fe_, get_functions_id_, params, functions);
}

Status Frontend::GetCatalogObject(const TCatalogObject& req,
    TCatalogObject* resp) {
  return JniUtil::CallJniMethod(fe_, get_catalog_object_id_, req, resp);
}

Status Frontend::GetExecRequest(
    const TQueryContext& query_ctxt, TExecRequest* result) {
  return JniUtil::CallJniMethod(fe_, create_exec_request_id_, query_ctxt, result);
}

Status Frontend::GetExplainPlan(
    const TQueryContext& query_ctxt, string* explain_string) {
  return JniUtil::CallJniMethod(fe_, get_explain_plan_id_, query_ctxt, explain_string);
}

Status Frontend::ValidateSettings() {
  // Use FE to check Hadoop config setting
  // TODO: check OS setting
  stringstream ss;
  JNIEnv* jni_env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  jstring error_string =
      static_cast<jstring>(jni_env->CallObjectMethod(fe_, check_config_id_));
  RETURN_ERROR_IF_EXC(jni_env);
  jboolean is_copy;
  const char *str = jni_env->GetStringUTFChars(error_string, &is_copy);
  RETURN_ERROR_IF_EXC(jni_env);
  ss << str;
  jni_env->ReleaseStringUTFChars(error_string, str);
  RETURN_ERROR_IF_EXC(jni_env);

  if (ss.str().size() > 0) {
    return Status(ss.str());
  }
  return Status::OK;
}

Status Frontend::ExecHiveServer2MetadataOp(const TMetadataOpRequest& request,
    TResultSet* result) {
  return JniUtil::CallJniMethod(fe_, exec_hs2_metadata_op_id_, request, result);
}

Status Frontend::RenderHadoopConfigs(bool as_text, stringstream* output) {
  JNIEnv* jni_env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  jstring java_string = static_cast<jstring>(jni_env->CallObjectMethod(
      fe_, get_hadoop_config_id_, as_text));
  RETURN_ERROR_IF_EXC(jni_env);
  jboolean is_copy;
  const char *str = jni_env->GetStringUTFChars(java_string, &is_copy);
  RETURN_ERROR_IF_EXC(jni_env);
  (*output) << str;
  jni_env->ReleaseStringUTFChars(java_string, str);
  RETURN_ERROR_IF_EXC(jni_env);
  return Status::OK;
}

Status Frontend::LoadData(const TLoadDataReq& request, TLoadDataResp* response) {
  return JniUtil::CallJniMethod(fe_, load_table_data_id_, request, response);
}

bool Frontend::IsAuthorizationError(const Status& status) {
  return !status.ok() && status.GetErrorMsg().find("AuthorizationException") == 0;
}

Status Frontend::SetCatalogInitialized() {
  JNIEnv* jni_env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  jni_env->CallObjectMethod(fe_, set_catalog_initialized_id_);
  RETURN_ERROR_IF_EXC(jni_env);
  return Status::OK;
}
