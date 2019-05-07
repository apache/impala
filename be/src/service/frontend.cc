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

#include "service/frontend.h"

#include <jni.h>
#include <list>
#include <string>

#include "common/logging.h"
#include "rpc/jni-thrift-util.h"
#include "util/backend-gflag-util.h"
#include "util/jni-util.h"
#include "util/time.h"

#include "common/names.h"

#ifndef NDEBUG
DECLARE_int32(stress_catalog_init_delay_ms);
#endif

using namespace impala;

// Authorization related flags. Must be set to valid values to properly configure
// authorization.
DEFINE_string(authorization_provider,
    "sentry",
    "Specifies the type of internally-provided authorization provider to use. "
    "['ranger', 'sentry' (default)]");
DEFINE_string(authorization_factory_class,
    "",
    "Specifies the class name that implements the authorization provider. "
    "This will override the authorization_provider flag if both are specified.");
DEFINE_string(ranger_service_type, "hive", "Specifies the Ranger service type.");
DEFINE_string(ranger_app_id, "",
    "Specifies the Ranger application ID. Ranger application ID is an ID to "
    "uniquely identify the application that communicates with Ranger. This flag is "
    "required when authorization with Ranger is enabled.");
DEFINE_string(server_name, "", "The name to use for securing this impalad "
    "server during authorization. Set to enable authorization.");
DEFINE_string(authorization_policy_provider_class,
    "org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider",
    "Advanced: The authorization policy provider class name for Sentry.");
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
DEFINE_string(authorized_proxy_group_config, "",
    "Specifies the set of authorized proxy groups (users who can delegate to other "
    "users belonging to the specified groups during authorization) and whom they are "
    "allowed to delegate. Input is a semicolon-separated list of key=value pairs of "
    "authorized proxy users to the group(s) they can delegate to. These groups are "
    "specified as a list of groups separated by a delimiter (which defaults to comma and "
    "may be changed via --authorized_proxy_group_config_delimiter), or '*' to indicate "
    "all users. For example: hue=group1,group2;admin=*");
DEFINE_string(authorized_proxy_group_config_delimiter, ",",
    "Specifies the delimiter used in authorized_proxy_group_config. ");
DEFINE_string(kudu_master_hosts, "", "Specifies the default Kudu master(s). The given "
    "value should be a comma separated list of hostnames or IP addresses; ports are "
    "optional.");

Frontend::Frontend() {
  JniMethodDescriptor methods[] = {
    {"<init>", "([B)V", &fe_ctor_},
    {"createExecRequest", "([B)[B", &create_exec_request_id_},
    {"getExplainPlan", "([B)Ljava/lang/String;", &get_explain_plan_id_},
    {"getHadoopConfig", "([B)[B", &get_hadoop_config_id_},
    {"getAllHadoopConfigs", "()[B", &get_hadoop_configs_id_},
    {"getHadoopGroups", "([B)[B", &get_hadoop_groups_id_},
    {"checkConfiguration", "()Ljava/lang/String;", &check_config_id_},
    {"updateCatalogCache", "([B)[B", &update_catalog_cache_id_},
    {"updateExecutorMembership", "([B)V", &update_membership_id_},
    {"getCatalogMetrics", "()[B", &get_catalog_metrics_id_},
    {"getTableNames", "([B)[B", &get_table_names_id_},
    {"describeDb", "([B)[B", &describe_db_id_},
    {"describeTable", "([B)[B", &describe_table_id_},
    {"showCreateTable", "([B)Ljava/lang/String;", &show_create_table_id_},
    {"getDbs", "([B)[B", &get_dbs_id_},
    {"getDataSrcMetadata", "([B)[B", &get_data_src_metadata_id_},
    {"getStats", "([B)[B", &get_stats_id_},
    {"getFunctions", "([B)[B", &get_functions_id_},
    {"getCatalogObject", "([B)[B", &get_catalog_object_id_},
    {"getRoles", "([B)[B", &show_roles_id_},
    {"getPrincipalPrivileges", "([B)[B", &get_principal_privileges_id_},
    {"execHiveServer2MetadataOp", "([B)[B", &exec_hs2_metadata_op_id_},
    {"setCatalogIsReady", "()V", &set_catalog_is_ready_id_},
    {"waitForCatalog", "()V", &wait_for_catalog_id_},
    {"loadTableData", "([B)[B", &load_table_data_id_},
    {"getTableFiles", "([B)[B", &get_table_files_id_},
    {"showCreateFunction", "([B)Ljava/lang/String;", &show_create_function_id_},
    {"buildTestDescriptorTable", "([B)[B", &build_test_descriptor_table_id_},
  };

  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  JniLocalFrame jni_frame;
  ABORT_IF_ERROR(jni_frame.push(jni_env));

  // create instance of java class JniFrontend
  jclass fe_class = jni_env->FindClass("org/apache/impala/service/JniFrontend");
  ABORT_IF_EXC(jni_env);

  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    ABORT_IF_ERROR(JniUtil::LoadJniMethod(jni_env, fe_class, &(methods[i])));
  };

  jbyteArray cfg_bytes;
  ABORT_IF_ERROR(GetThriftBackendGflags(jni_env, &cfg_bytes));

  jobject fe = jni_env->NewObject(fe_class, fe_ctor_, cfg_bytes);
  ABORT_IF_EXC(jni_env);
  ABORT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, fe, &fe_));
}

Status Frontend::UpdateCatalogCache(const TUpdateCatalogCacheRequest& req,
    TUpdateCatalogCacheResponse* resp) {
  return JniUtil::CallJniMethod(fe_, update_catalog_cache_id_, req, resp);
}

Status Frontend::UpdateExecutorMembership(const TUpdateExecutorMembershipRequest& req) {
  return JniUtil::CallJniMethod(fe_, update_membership_id_, req);
}

Status Frontend::DescribeDb(const TDescribeDbParams& params,
    TDescribeResult* response) {
  return JniUtil::CallJniMethod(fe_, describe_db_id_, params, response);
}

Status Frontend::DescribeTable(const TDescribeTableParams& params,
    const TSessionState& session, TDescribeResult* response) {
  TDescribeTableParams tparams;
  tparams.__set_output_style(params.output_style);
  if (params.__isset.table_name) tparams.__set_table_name(params.table_name);
  if (params.__isset.result_struct) tparams.__set_result_struct(params.result_struct);
  tparams.__set_session(session);
  return JniUtil::CallJniMethod(fe_, describe_table_id_, tparams, response);
}

Status Frontend::ShowCreateTable(const TTableName& table_name, string* response) {
  return JniUtil::CallJniMethod(fe_, show_create_table_id_, table_name, response);
}

Status Frontend::ShowCreateFunction(const TGetFunctionsParams& params, string* response) {
  return JniUtil::CallJniMethod(fe_, show_create_function_id_, params, response);
}

Status Frontend::GetCatalogMetrics(TGetCatalogMetricsResult* resp) {
  return JniUtil::CallJniMethod(fe_, get_catalog_metrics_id_, resp);
}

Status Frontend::GetTableNames(const string& db, const string* pattern,
    const TSessionState* session, TGetTablesResult* table_names) {
  TGetTablesParams params;
  params.__set_db(db);
  if (pattern != NULL) params.__set_pattern(*pattern);
  if (session != NULL) params.__set_session(*session);
  return JniUtil::CallJniMethod(fe_, get_table_names_id_, params, table_names);
}

Status Frontend::GetDbs(const string* pattern, const TSessionState* session,
    TGetDbsResult* dbs) {
  TGetDbsParams params;
  if (pattern != NULL) params.__set_pattern(*pattern);
  if (session != NULL) params.__set_session(*session);
  return JniUtil::CallJniMethod(fe_, get_dbs_id_, params, dbs);
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

Status Frontend::GetPrincipalPrivileges(const TShowGrantPrincipalParams& params,
    TResultSet* result) {
  return JniUtil::CallJniMethod(fe_, get_principal_privileges_id_, params, result);
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
  string err;
  RETURN_IF_ERROR(JniCall::instance_method(fe_, check_config_id_).Call(&err));
  if (!err.empty()) return Status(err);
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

Status Frontend::GetHadoopGroups(const TGetHadoopGroupsRequest& request,
    TGetHadoopGroupsResponse* response) {
  return JniUtil::CallJniMethod(fe_, get_hadoop_groups_id_, request, response);
}

Status Frontend::LoadData(const TLoadDataReq& request, TLoadDataResp* response) {
  return JniUtil::CallJniMethod(fe_, load_table_data_id_, request, response);
}

bool Frontend::IsAuthorizationError(const Status& status) {
  return !status.ok() && status.GetDetail().find("AuthorizationException") == 0;
}

void Frontend::SetCatalogIsReady() {
  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  jni_env->CallVoidMethod(fe_, set_catalog_is_ready_id_);
  ABORT_IF_EXC(jni_env);
}

void Frontend::WaitForCatalog() {
#ifndef NDEBUG
  if (FLAGS_stress_catalog_init_delay_ms > 0) {
    SleepForMs(FLAGS_stress_catalog_init_delay_ms);
  }
#endif
  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  jni_env->CallVoidMethod(fe_, wait_for_catalog_id_);
  ABORT_IF_EXC(jni_env);
}

Status Frontend::GetTableFiles(const TShowFilesParams& params, TResultSet* result) {
  return JniUtil::CallJniMethod(fe_, get_table_files_id_, params, result);
}

Status Frontend::BuildTestDescriptorTable(const TBuildTestDescriptorTableParams& params,
    TDescriptorTable* result) {
  return JniUtil::CallJniMethod(fe_, build_test_descriptor_table_id_, params, result);
}
