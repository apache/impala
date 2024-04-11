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
#include "util/test-info.h"
#include "util/time.h"

#include "common/names.h"

#ifndef NDEBUG
DECLARE_int32(stress_catalog_init_delay_ms);
#endif

using namespace impala;

// Authorization related flags. Must be set to valid values to properly configure
// authorization.
DEFINE_string(authorization_provider,
    "",
    "Specifies the type of internally-provided authorization provider to use. "
    "Defaults to unset, which disables authorization. To enable authorization, "
    "set to one of the following: ['ranger']");
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
DEFINE_bool(enable_shell_based_groups_mapping_support, false,
    "Enables support for Hadoop groups mapping "
    "org.apache.hadoop.security.ShellBasedUnixGroupsMapping. By default this support "
    "is not enabled as it can lead to many process getting spawned to fetch groups for "
    "user using shell command.");
DEFINE_string(kudu_master_hosts, "", "Specifies the default Kudu master(s). The given "
    "value should be a comma separated list of hostnames or IP addresses; ports are "
    "optional.");
DEFINE_bool(enable_kudu_impala_hms_check, true, "By default this flag is true. If "
    "enabled checks that Kudu and Impala are using the same HMS instance(s).");
DEFINE_string(jni_frontend_class, "org/apache/impala/service/JniFrontend", "By default "
    "the JniFrontend class included in the repository is used as the frontend interface. "
    "This option allows the class to be overridden by a third party module. The "
    "overridden class needs to contain all the methods in the methods[] variable, so "
    "most implementations should make their class a child of JniFrontend and "
    "override only relevant methods.");
DEFINE_bool(allow_catalog_cache_op_from_masked_users, false, "Whether to allow table "
    "level catalog-cache operations, i.e. REFRESH/INVALIDATE METADATA <table>, from users"
    " that have associate Ranger masking policies on the table. By default, such "
    "operations are blocked since such users are considered read-only users. Note that "
    "checking column masking policies requires loading column info of the table, which "
    "could slow down simple commands like INVALIDATE METADATA <table>");
DEFINE_int32(dbcp_max_conn_pool_size, 8,
    "The maximum number of active connections that can be allocated from a DBCP "
    "connection pool at the same time, or -1 for no limit. DBCP connection pools are "
    "created when accessing remote RDBMS for external JDBC tables. This setting applies "
    "to all DBCP connection pools created on the coordinator.");
DEFINE_int32(dbcp_max_wait_millis_for_conn, -1,
    "The maximum number of milliseconds that DBCP connection pool will wait (when "
    "there are no available connections) for a connection to be returned before "
    "throwing an exception, or -1 to wait indefinitely. 0 means immediately throwing "
    "exception if there are no available connections in the pool.");
DEFINE_int32(dbcp_data_source_idle_timeout_s, 300,
    "Timeout value in seconds for idle DBCP DataSource objects in cache. It only takes "
    "effect when query option 'clean_dbcp_ds_cache' is set as false.");

Frontend::Frontend() {
  JniMethodDescriptor methods[] = {
    {"<init>", "([BZ)V", &fe_ctor_},
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
    {"getMetadataTableNames", "([B)[B", &get_metadata_table_names_id_},
    {"describeDb", "([B)[B", &describe_db_id_},
    {"describeTable", "([B)[B", &describe_table_id_},
    {"showCreateTable", "([B)Ljava/lang/String;", &show_create_table_id_},
    {"getDbs", "([B)[B", &get_dbs_id_},
    {"getDataSrcMetadata", "([B)[B", &get_data_src_metadata_id_},
    {"getStats", "([B)[B", &get_stats_id_},
    {"getFunctions", "([B)[B", &get_functions_id_},
    {"getTableHistory", "([B)[B", &get_table_history_id_},
    {"getCatalogObject", "([B)[B", &get_catalog_object_id_},
    {"getCatalogTable", "([B)Lorg/apache/impala/catalog/FeTable;",
        &get_catalog_table_id_},
    {"getRoles", "([B)[B", &show_roles_id_},
    {"getPrincipalPrivileges", "([B)[B", &get_principal_privileges_id_},
    {"execHiveServer2MetadataOp", "([B)[B", &exec_hs2_metadata_op_id_},
    {"setCatalogIsReady", "()V", &set_catalog_is_ready_id_},
    {"waitForCatalog", "()V", &wait_for_catalog_id_},
    {"loadTableData", "([B)[B", &load_table_data_id_},
    {"convertTable", "([B)V", &convertTable},
    {"getTableFiles", "([B)[B", &get_table_files_id_},
    {"showCreateFunction", "([B)Ljava/lang/String;", &show_create_function_id_},
    {"buildTestDescriptorTable", "([B)[B", &build_test_descriptor_table_id_},
    {"callQueryCompleteHooks", "([B)V", &call_query_complete_hooks_id_},
    {"abortTransaction", "(J)V", &abort_txn_},
    {"addTransaction", "([B)V", &add_txn_},
    {"unregisterTransaction", "(J)V", &unregister_txn_},
    {"getSaml2Redirect", "([B)[B", &get_saml2_redirect_id_},
    {"validateSaml2Response", "([B)[B", &validate_saml2_response_id_},
    {"validateSaml2Bearer", "([B)Ljava/lang/String;", &validate_saml2_bearer_id_},
    {"abortKuduTransaction", "([B)V", &abort_kudu_txn_},
    {"commitKuduTransaction", "([B)V", &commit_kudu_txn_}
  };

  JniMethodDescriptor staticMethods[] = {
    {"getSecretFromKeyStore", "([B)Ljava/lang/String;", &get_secret_from_key_store_}
  };

  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  JniLocalFrame jni_frame;
  ABORT_IF_ERROR(jni_frame.push(jni_env));

  // create instance of java class JniFrontend
  fe_class_ = jni_env->FindClass(FLAGS_jni_frontend_class.c_str());
  ABORT_IF_EXC(jni_env);

  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    ABORT_IF_ERROR(JniUtil::LoadJniMethod(jni_env, fe_class_, &(methods[i])));
  };

  num_methods = sizeof(staticMethods) / sizeof(staticMethods[0]);
  for (int i = 0; i < num_methods; ++i) {
    ABORT_IF_ERROR(JniUtil::LoadStaticJniMethod(jni_env, fe_class_, &(staticMethods[i])));
  };

  jbyteArray cfg_bytes;
  ABORT_IF_ERROR(GetThriftBackendGFlagsForJNI(jni_env, &cfg_bytes));

  // Pass in whether this is a backend test, so that the Frontend can avoid certain
  // unnecessary initialization that introduces dependencies on a running minicluster.
  jboolean is_be_test = TestInfo::is_be_test();
  jobject fe = jni_env->NewObject(fe_class_, fe_ctor_, cfg_bytes, is_be_test);
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
  if (params.__isset.metadata_table_name) {
    tparams.__set_metadata_table_name(params.metadata_table_name);
  }
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
    set<TImpalaTableType::type> table_types = set<TImpalaTableType::type>();
    return GetTableNames(db, pattern, session, table_types, table_names);
}

Status Frontend::GetTableNames(const string& db, const string* pattern,
    const TSessionState* session, const set<TImpalaTableType::type>& table_types,
    TGetTablesResult* table_names) {
  TGetTablesParams params;
  params.__set_db(db);
  params.__set_table_types(table_types);
  if (pattern != nullptr) params.__set_pattern(*pattern);
  if (session != nullptr) params.__set_session(*session);
  return JniUtil::CallJniMethod(fe_, get_table_names_id_, params, table_names);
}

Status Frontend::GetMetadataTableNames(const string& db, const string& table_name,
    const string* pattern, const TSessionState* session,
    TGetTablesResult* metadata_table_names) {
  TGetMetadataTablesParams params;
  params.__set_db(db);
  params.__set_tbl(table_name);
  if (pattern != nullptr) params.__set_pattern(*pattern);
  if (session != nullptr) params.__set_session(*session);
  return JniUtil::CallJniMethod(fe_, get_metadata_table_names_id_, params,
      metadata_table_names);
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

Status Frontend::GetTableHistory(const TDescribeHistoryParams& params,
      TGetTableHistoryResult* result) {
  return JniUtil::CallJniMethod(fe_, get_table_history_id_, params, result);
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

Status Frontend::GetCatalogTable(const TTableName& table_name, jobject* result) {
  return JniUtil::CallJniMethod(fe_, get_catalog_table_id_, table_name, result);
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

Status Frontend::addTransaction(const TQueryCtx& query_ctx) {
  return JniUtil::CallJniMethod(fe_, add_txn_, query_ctx);
}

Status Frontend::AbortTransaction(int64_t transaction_id) {
  return JniUtil::CallJniMethod(fe_, abort_txn_, transaction_id);
}

Status Frontend::UnregisterTransaction(int64_t transaction_id) {
  return JniUtil::CallJniMethod(fe_, unregister_txn_, transaction_id);
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

// Call FE post-query execution hook
Status Frontend::CallQueryCompleteHooks(const TQueryCompleteContext& context) {
  return JniUtil::CallJniMethod(fe_, call_query_complete_hooks_id_, context);
}

Status Frontend::GetSaml2Redirect( const TWrappedHttpRequest& request,
    TWrappedHttpResponse* response)  {
  return JniUtil::CallJniMethod(
      fe_, get_saml2_redirect_id_, request, response);
}

Status Frontend::ValidateSaml2Response(
    const TWrappedHttpRequest& request, TWrappedHttpResponse* response) {
  return JniUtil::CallJniMethod(
      fe_, validate_saml2_response_id_, request, response);
}

Status Frontend::ValidateSaml2Bearer(
  const TWrappedHttpRequest& request, string* user) {
  return JniUtil::CallJniMethod(
      fe_, validate_saml2_bearer_id_, request, user);
}

Status Frontend::AbortKuduTransaction(const TUniqueId& query_id) {
  return JniUtil::CallJniMethod(fe_, abort_kudu_txn_, query_id);
}

Status Frontend::CommitKuduTransaction(const TUniqueId& query_id) {
  return JniUtil::CallJniMethod(fe_, commit_kudu_txn_, query_id);
}

Status Frontend::Convert(const TExecRequest& request) {
  return JniUtil::CallJniMethod(fe_, convertTable, request);
}

Status Frontend::GetSecretFromKeyStore(const string& secret_key, string* secret) {
  TStringLiteral secret_key_t;
  secret_key_t.__set_value(secret_key);
  return JniUtil::CallStaticJniMethod(fe_class_, get_secret_from_key_store_, secret_key_t,
      secret);
}
