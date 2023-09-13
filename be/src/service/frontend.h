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

#ifndef IMPALA_SERVICE_FRONTEND_H
#define IMPALA_SERVICE_FRONTEND_H

#include <jni.h>

#include "common/status.h"
#include "gen-cpp/CatalogService_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/LineageGraph_types.h"
#include "gen-cpp/Types_types.h"

namespace impala {

/// The Frontend is a proxy for the Java-side JniFrontend class. The interface is a set of
/// wrapper methods for methods called over JNI.
/// TODO: Consider changing all methods to accept and return only Thrift structures so that
/// all go through exactly the same calling code.
class Frontend {
 public:
  /// Does all the work of initialising the JNI method stubs. If any method can't be found,
  /// or if there is any further exception, the constructor will terminate the process.
  Frontend();

  /// Request to update the Impalad catalog cache. The 'req' argument contains a pointer
  /// to a CatalogServer used for the FE to call NativeGetNextCatalogTopicItem() back to
  /// get the catalog objects iteratively. Returns a response that contains details such
  /// as the new max catalog version.
  Status UpdateCatalogCache(const TUpdateCatalogCacheRequest& req,
      TUpdateCatalogCacheResponse *resp);

  /// Request to update the Impalad frontend cluster membership snapshot of executors.
  /// The TUpdateExecutorMembershipRequest contains the latest set of executor nodes.
  Status UpdateExecutorMembership(const TUpdateExecutorMembershipRequest& req);

  /// Call FE to get explain plan
  Status GetExplainPlan(const TQueryCtx& query_ctx, std::string* explain_string);

  /// Call FE to get TExecRequest.
  Status GetExecRequest(const TQueryCtx& query_ctx, TExecRequest* result);

  /// Get the metrics from the catalog used by this frontend.
  Status GetCatalogMetrics(TGetCatalogMetricsResult* resp);

  /// Returns all matching table names, per Hive's "SHOW TABLES <pattern>" regardless of
  /// the table type.
  Status GetTableNames(const std::string& db, const std::string* pattern,
      const TSessionState* session, TGetTablesResult* table_names);

  /// Returns all matching table names, per Hive's "SHOW TABLES <pattern>" such that each
  /// corresponds to a table whose type is in table_types for a non-empty table_types.
  /// Each table name returned is unqualified. If table_types is empty, then all types of
  /// tables will be considered when their table names are matched against the pattern.
  ///
  /// If pattern is NULL, match all tables otherwise match only those tables that
  /// match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  /// and each pN may contain wildcards denoted by '*' which match all strings.
  /// The TSessionState parameter is used to filter results of metadata operations when
  /// authorization is enabled. If this is a user initiated request, it should
  /// be set to the user's current session. If this is an Impala internal request,
  /// the session should be set to NULL which will skip privilege checks returning all
  /// results.
  Status GetTableNames(const std::string& db, const std::string* pattern,
      const TSessionState* session, const std::set<TImpalaTableType::type>& table_types,
      TGetTablesResult* table_names);

  /// Returns the list of metadata tables for the given table that match the pattern.
  /// 'pattern' and 'session' are used as in GetTableNames(). Currently only Iceberg
  /// metadata tables are supported.
  Status GetMetadataTableNames(const string& db, const string& table_name,
      const string* pattern, const TSessionState* session,
      TGetTablesResult* metadata_table_names);

  /// Return all databases matching the optional argument 'pattern'.
  /// If pattern is NULL, match all databases otherwise match only those databases that
  /// match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  /// and each pN may contain wildcards denoted by '*' which match all strings.
  /// The TSessionState parameter is used to filter results of metadata operations when
  /// authorization is enabled. If this is a user initiated request, it should
  /// be set to the user's current session. If this is an Impala internal request,
  /// the session should be set to NULL which will skip privilege checks returning all
  /// results.
  Status GetDbs(const std::string* pattern, const TSessionState* session,
      TGetDbsResult* dbs);

  /// Return all data sources matching the optional argument 'pattern'.
  /// If pattern is NULL, match all data source names otherwise match only those that
  /// match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  /// and each pN may contain wildcards denoted by '*' which match all strings.
  Status GetDataSrcMetadata(const std::string* pattern, TGetDataSrcsResult* result);

  /// Call FE to get the table/column stats.
  Status GetStats(const TShowStatsParams& params, TResultSet* result);

  /// Call FE to get the privileges granted to a principal.
  Status GetPrincipalPrivileges(const TShowGrantPrincipalParams& params,
      TResultSet* result);

  /// Call FE to get table history for an Iceberg table.
  Status GetTableHistory(const TDescribeHistoryParams& params,
      TGetTableHistoryResult* result);

  /// Return all functions of 'category' that match the optional argument 'pattern'.
  /// If pattern is NULL match all functions, otherwise match only those functions that
  /// match the pattern string.
  /// The TSessionState parameter is used to filter results of metadata operations when
  /// authorization is enabled. If this is a user initiated request, it should
  /// be set to the user's current session. If this is an Impala internal request,
  /// the session should be set to NULL which will skip privilege checks returning all
  /// results.
  Status GetFunctions(TFunctionCategory::type fn_category, const std::string& db,
      const std::string* pattern, const TSessionState* session,
      TGetFunctionsResult* functions);

  /// Gets the Thrift representation of a Catalog object. The request is a TCatalogObject
  /// which has the desired TCatalogObjectType and name properly set.
  /// Returns OK if the operation was successful, otherwise a Status object with
  /// information on the error will be returned.
  Status GetCatalogObject(const TCatalogObject& request, TCatalogObject* response);

  /// Gets the Java object of a Catalog table object. It can be used to call Java methods
  /// of the Catalog Table object.
  Status GetCatalogTable(const TTableName& table_name, jobject *result);

  /// Call FE to get the roles.
  Status ShowRoles(const TShowRolesParams& params, TShowRolesResult* result);

  /// Returns (in the output parameter) the result of a DESCRIBE DATABASE db command.
  /// This command retrieves db metadata, such as db location and comment.
  /// The metadata that is returned is controlled by setting the 'output_style' field.
  /// If set to FORMATTED|EXTENDED, all the database's properties are returned.
  Status DescribeDb(const TDescribeDbParams& params, TDescribeResult* response);

  /// Returns (in the output parameter) the result of a DESCRIBE table command. This
  /// command retrieves table metadata, such as the column definitions. The metadata
  /// that is returned is controlled by setting the 'output_style' field. If this
  /// field is set to MINIMAL, only the column definitions are returned. If set to
  /// FORMATTED|EXTENDED, extended metadata is returned (in addition to the column defs).
  /// This includes info about the table properties, SerDe properties, StorageDescriptor
  /// properties, and more.  The current user session is needed for privileges checks.
  Status DescribeTable(const TDescribeTableParams& params, const TSessionState& session,
      TDescribeResult* response);

  /// Returns (in the output parameter) a string containing the CREATE TABLE command that
  /// creates the table specified in the params.
  Status ShowCreateTable(const TTableName& table_name, std::string* response);

  /// Returns (in the output parameter) a string containing the CREATE FUNCTION command that
  /// creates the function specified in the params.
  Status ShowCreateFunction(const TGetFunctionsParams& params, std::string* response);

  /// Validate Hadoop config; requires FE
  Status ValidateSettings();

  /// Calls FE to execute HiveServer2 metadata operation.
  Status ExecHiveServer2MetadataOp(const TMetadataOpRequest& request,
      TResultSet* result);

  /// Returns all Hadoop configurations in key, value form in result.
  Status GetAllHadoopConfigs(TGetAllHadoopConfigsResponse* result);

  /// Returns (in the output parameter) the value for the given config. The returned Thrift
  /// struct will indicate if the value was null or not found by not setting its 'value'
  /// field.
  Status GetHadoopConfig(const TGetHadoopConfigRequest& request,
      TGetHadoopConfigResponse* response);

  /// Returns (in the output parameter) the list of groups for the given user.
  Status GetHadoopGroups(const TGetHadoopGroupsRequest& request,
      TGetHadoopGroupsResponse* response);

  /// Loads a single file or set of files into a table or partition. Saves the RPC
  /// response in the TLoadDataResp output parameter. Returns OK if the operation
  /// completed successfully.
  Status LoadData(const TLoadDataReq& load_data_request, TLoadDataResp* response);

  /// Adds a transaction that was started externally
  Status addTransaction(const TQueryCtx& queryCtx);

  /// Aborts transaction with the given transaction id.
  Status AbortTransaction(int64_t transaction_id);

  /// Unregisters an already committed transaction.
  Status UnregisterTransaction(int64_t transaction_id);

  /// Returns true if the error returned by the FE was due to an AuthorizationException.
  static bool IsAuthorizationError(const Status& status);

  /// Sets the frontend's catalog in the ready state. This is only used for testing in
  /// conjunction with InProcessImpalaServer. This sets the frontend's catalog as
  /// ready, so can receive queries without needing a catalog server.
  void SetCatalogIsReady();

  /// Waits for the FE catalog to be initialized and ready to receive queries.
  /// There is no bound on the wait time.
  void WaitForCatalog();

  /// Call FE to get files info for a table or partition.
  Status GetTableFiles(const TShowFilesParams& params, TResultSet* result);

  /// Creates a thrift descriptor table for testing.
  Status BuildTestDescriptorTable(const TBuildTestDescriptorTableParams& params,
      TDescriptorTable* result);

  // Call FE post-query execution hook
  Status CallQueryCompleteHooks(const TQueryCompleteContext& context);

  // Call FE to create a http response that redirects to the SSO service.
  Status GetSaml2Redirect(const TWrappedHttpRequest& request,
      TWrappedHttpResponse* response);

  // Call FE to validate the SAML2 AuthNResponse.
  // The response is an HTML form that contains a bearer token or an error
  // message.
  Status ValidateSaml2Response(
      const TWrappedHttpRequest& request, TWrappedHttpResponse* response);

  // Call FE to validate the bearer token.
  // Fills "user" if the validation was successful.
  Status ValidateSaml2Bearer(const TWrappedHttpRequest& request, string* user);

  /// Aborts Kudu transaction with the given query id.
  Status AbortKuduTransaction(const TUniqueId& query_id);

  /// Commits Kudu transaction with the given query id.
  Status CommitKuduTransaction(const TUniqueId& query_id);

  /// Convert external Hdfs tables to Iceberg tables
  Status Convert(const TExecRequest& request);

  /// Get secret from jceks key store for the input secret_key.
  Status GetSecretFromKeyStore(const string& secret_key, string* secret);

 private:
  jclass fe_class_; // org.apache.impala.service.JniFrontend class
  jobject fe_;  // instance of org.apache.impala.service.JniFrontend
  jmethodID create_exec_request_id_;  // JniFrontend.createExecRequest()
  jmethodID get_explain_plan_id_;  // JniFrontend.getExplainPlan()
  jmethodID get_hadoop_config_id_;  // JniFrontend.getHadoopConfig(byte[])
  jmethodID get_hadoop_configs_id_;  // JniFrontend.getAllHadoopConfigs()
  jmethodID get_hadoop_groups_id_;  // JniFrontend.getHadoopGroups()
  jmethodID check_config_id_; // JniFrontend.checkConfiguration()
  jmethodID update_catalog_cache_id_; // JniFrontend.updateCatalogCache(byte[][])
  jmethodID update_membership_id_; // JniFrontend.updateExecutorMembership()
  jmethodID get_catalog_metrics_id_; // JniFrontend.getCatalogMetrics()
  jmethodID get_table_names_id_; // JniFrontend.getTableNames
  jmethodID get_metadata_table_names_id_; // JniFrontend.getMetadataTableNames
  jmethodID describe_db_id_; // JniFrontend.describeDb
  jmethodID describe_table_id_; // JniFrontend.describeTable
  jmethodID show_create_table_id_; // JniFrontend.showCreateTable
  jmethodID get_dbs_id_; // JniFrontend.getDbs
  jmethodID get_data_src_metadata_id_; // JniFrontend.getDataSrcMetadata
  jmethodID get_stats_id_; // JniFrontend.getTableStats
  jmethodID get_functions_id_; // JniFrontend.getFunctions
  jmethodID get_table_history_id_; // JniFrontend.getTableHistory
  jmethodID get_catalog_object_id_; // JniFrontend.getCatalogObject
  jmethodID get_catalog_table_id_; // JniFrontend.getCatalogTable
  jmethodID show_roles_id_; // JniFrontend.getRoles
  jmethodID get_principal_privileges_id_; // JniFrontend.getPrincipalPrivileges
  jmethodID exec_hs2_metadata_op_id_; // JniFrontend.execHiveServer2MetadataOp
  jmethodID load_table_data_id_; // JniFrontend.loadTableData
  jmethodID set_catalog_is_ready_id_; // JniFrontend.setCatalogIsReady
  jmethodID wait_for_catalog_id_; // JniFrontend.waitForCatalog
  jmethodID get_table_files_id_; // JniFrontend.getTableFiles
  jmethodID show_create_function_id_; // JniFrontend.showCreateFunction
  jmethodID call_query_complete_hooks_id_; // JniFrontend.callQueryCompleteHooks
  jmethodID add_txn_; // JniFrontend.addTransaction()
  jmethodID abort_txn_; // JniFrontend.abortTransaction()
  jmethodID unregister_txn_; // JniFrontend.unregisterTransaction()
  jmethodID get_saml2_redirect_id_; // JniFrontend.getSaml2Redirect()
  jmethodID validate_saml2_response_id_; // JniFrontend.validateSaml2Response()
  jmethodID validate_saml2_bearer_id_; // JniFrontend.validateSaml2Bearer()
  jmethodID abort_kudu_txn_; // JniFrontend.abortKuduTransaction()
  jmethodID commit_kudu_txn_; // JniFrontend.commitKuduTransaction()
  jmethodID convertTable; // JniFrontend.convertTable
  jmethodID get_secret_from_key_store_; // JniFrontend.getSecretFromKeyStore()

  // Only used for testing.
  jmethodID build_test_descriptor_table_id_; // JniFrontend.buildTestDescriptorTable()

  jmethodID fe_ctor_;
};

}

#endif
