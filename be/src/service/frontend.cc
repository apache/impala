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
#include "rpc/jni-thrift-util.h"
#include "util/jni-util.h"
#include "util/logging-support.h"

#include "common/names.h"

using namespace impala;

DECLARE_string(sentry_config);
DECLARE_int32(non_impala_java_vlog);

DEFINE_bool(load_catalog_at_startup, false, "if true, load all catalog data at startup");

// Authorization related flags. Must be set to valid values to properly configure
// authorization.
DEFINE_string(server_name, "", "The name to use for securing this impalad "
    "server during authorization. Set to enable authorization. By default, the "
    "authorization policy will be loaded from the catalog server (via the statestore)."
    "To use a file based authorization policy, set --authorization_policy_file.");
DEFINE_string(authorization_policy_file, "", "HDFS path to the authorization policy "
    "file. If set, authorization will be enabled and the authorization policy will be "
    "read from a file.");
DEFINE_string(authorization_policy_provider_class,
    "org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider",
    "Advanced: The authorization policy provider class name.");
DEFINE_string(authorized_proxy_user_config, "",
    "Specifies the set of authorized proxy users (users who can delegate to other "
    "users during authorization) and whom they are allowed to delegate. "
    "Input is a semicolon-separated list of key=value pairs of authorized proxy "
    "users to the user(s) they can delegate to. These users are specified as a list of "
    "short usernames separated by a delimiter (which defaults to comma and may be "
    "changed via --authorized_proxy_user_config_delimiter), or '*' to indicate all "
    "users. For example: hue=user1,user2;admin=*");
DEFINE_string(authorized_proxy_user_config_delimiter, ",",
    "Specifies the delimiter used in authorized_proxy_user_config. ");
Frontend::Frontend() {
  JniMethodDescriptor methods[] = {
    {"<init>", "(ZLjava/lang/String;Ljava/lang/String;Ljava/lang/String;"
        "Ljava/lang/String;II)V", &fe_ctor_},
    {"createExecRequest", "([B)[B", &create_exec_request_id_},
    {"getExplainPlan", "([B)Ljava/lang/String;", &get_explain_plan_id_},
    {"getHadoopConfig", "([B)[B", &get_hadoop_config_id_},
    {"getAllHadoopConfigs", "()[B", &get_hadoop_configs_id_},
    {"checkConfiguration", "()Ljava/lang/String;", &check_config_id_},
    {"updateCatalogCache", "([B)[B", &update_catalog_cache_id_},
    {"updateMembership", "([B)V", &update_membership_id_},
    {"getTableNames", "([B)[B", &get_table_names_id_},
    {"describeDb", "([B)[B", &describe_db_id_},
    {"describeTable", "([B)[B", &describe_table_id_},
    {"showCreateTable", "([B)Ljava/lang/String;", &show_create_table_id_},
    {"getDbNames", "([B)[B", &get_db_names_id_},
    {"getDataSrcMetadata", "([B)[B", &get_data_src_metadata_id_},
    {"getStats", "([B)[B", &get_stats_id_},
    {"getFunctions", "([B)[B", &get_functions_id_},
    {"getCatalogObject", "([B)[B", &get_catalog_object_id_},
    {"getRoles", "([B)[B", &show_roles_id_},
    {"getRolePrivileges", "([B)[B", &get_role_privileges_id_},
    {"execHiveServer2MetadataOp", "([B)[B", &exec_hs2_metadata_op_id_},
    {"setCatalogInitialized", "()V", &set_catalog_initialized_id_},
    {"loadTableData", "([B)[B", &load_table_data_id_},
    {"getTableFiles", "([B)[B", &get_table_files_id_},
    {"showCreateFunction", "([B)Ljava/lang/String;", &show_create_function_id_},
};

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
  jstring sentry_config =
      jni_env->NewStringUTF(FLAGS_sentry_config.c_str());
  jstring auth_provider_class =
      jni_env->NewStringUTF(FLAGS_authorization_policy_provider_class.c_str());
  jobject fe = jni_env->NewObject(fe_class_, fe_ctor_, lazy, server_name,
      policy_file_path, sentry_config, auth_provider_class, FlagToTLogLevel(FLAGS_v),
      FlagToTLogLevel(FLAGS_non_impala_java_vlog));
  EXIT_IF_EXC(jni_env);
  EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, fe, &fe_));
}

Status Frontend::UpdateCatalogCache(const TUpdateCatalogCacheRequest& req,
    TUpdateCatalogCacheResponse* resp) {
  return JniUtil::CallJniMethod(fe_, update_catalog_cache_id_, req, resp);
}

Status Frontend::UpdateMembership(const TUpdateMembershipRequest& req) {
  return JniUtil::CallJniMethod(fe_, update_membership_id_, req);
}

Status Frontend::DescribeDb(const TDescribeDbParams& params,
    TDescribeResult* response) {
  return JniUtil::CallJniMethod(fe_, describe_db_id_, params, response);
}

Status Frontend::DescribeTable(const TDescribeTableParams& params,
    TDescribeResult* response) {
  return JniUtil::CallJniMethod(fe_, describe_table_id_, params, response);
}

Status Frontend::ShowCreateTable(const TTableName& table_name, string* response) {
  return JniUtil::CallJniMethod(fe_, show_create_table_id_, table_name, response);
}

Status Frontend::ShowCreateFunction(const TGetFunctionsParams& params, string* response) {
  return JniUtil::CallJniMethod(fe_, show_create_function_id_, params, response);
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

Status Frontend::GetDataSrcMetadata(const string* pattern,
    TGetDataSrcsResult* result) {
  TGetDataSrcsParams params;
  if (pattern != NULL) params.__set_pattern(*pattern);
  return JniUtil::CallJniMethod(fe_, get_data_src_metadata_id_, params, result);
}

Status Frontend::GetStats(const TShowStatsParams& params,
    TResultSet* result) {
  return JniUtil::CallJniMethod(fe_, get_stats_id_, params, result);
}

Status Frontend::GetRolePrivileges(const TShowGrantRoleParams& params,
    TResultSet* result) {
  return JniUtil::CallJniMethod(fe_, get_role_privileges_id_, params, result);
}

Status Frontend::GetFunctions(TFunctionCategory::type fn_category, const string& db,
    const string* pattern, const TSessionState* session,
    TGetFunctionsResult* functions) {
  TGetFunctionsParams params;
  params.__set_category(fn_category);
  params.__set_db(db);
  if (pattern != NULL) params.__set_pattern(*pattern);
  if (session != NULL) params.__set_session(*session);
  return JniUtil::CallJniMethod(fe_, get_functions_id_, params, functions);
}

Status Frontend::ShowRoles(const TShowRolesParams& params, TShowRolesResult* result) {
  return JniUtil::CallJniMethod(fe_, show_roles_id_, params, result);
}

Status Frontend::GetCatalogObject(const TCatalogObject& req,
    TCatalogObject* resp) {
  return JniUtil::CallJniMethod(fe_, get_catalog_object_id_, req, resp);
}

Status Frontend::GetExecRequest(
    const TQueryCtx& query_ctx, TExecRequest* result) {
  return JniUtil::CallJniMethod(fe_, create_exec_request_id_, query_ctx, result);
}

Status Frontend::GetExplainPlan(
    const TQueryCtx& query_ctx, string* explain_string) {
  return JniUtil::CallJniMethod(fe_, get_explain_plan_id_, query_ctx, explain_string);
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
  return Status::OK();
}

Status Frontend::ExecHiveServer2MetadataOp(const TMetadataOpRequest& request,
    TResultSet* result) {
  return JniUtil::CallJniMethod(fe_, exec_hs2_metadata_op_id_, request, result);
}

Status Frontend::GetAllHadoopConfigs(TGetAllHadoopConfigsResponse* result) {
  return JniUtil::CallJniMethod(fe_, get_hadoop_configs_id_, result);
}

Status Frontend::GetHadoopConfig(const TGetHadoopConfigRequest& request,
    TGetHadoopConfigResponse* response) {
  return JniUtil::CallJniMethod(fe_, get_hadoop_config_id_, request, response);
}

Status Frontend::LoadData(const TLoadDataReq& request, TLoadDataResp* response) {
  return JniUtil::CallJniMethod(fe_, load_table_data_id_, request, response);
}

bool Frontend::IsAuthorizationError(const Status& status) {
  return !status.ok() && status.GetDetail().find("AuthorizationException") == 0;
}

Status Frontend::SetCatalogInitialized() {
  JNIEnv* jni_env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  jni_env->CallObjectMethod(fe_, set_catalog_initialized_id_);
  RETURN_ERROR_IF_EXC(jni_env);
  return Status::OK();
}

Status Frontend::GetTableFiles(const TShowFilesParams& params, TResultSet* result) {
  return JniUtil::CallJniMethod(fe_, get_table_files_id_, params, result);
}
