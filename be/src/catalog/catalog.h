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

#ifndef IMPALA_CATALOG_CATALOG_H
#define IMPALA_CATALOG_CATALOG_H

#include <jni.h>

#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/CatalogInternalService_types.h"
#include "gen-cpp/CatalogService_types.h"
#include "common/status.h"

namespace impala {

class CatalogServer;

/// The Catalog is a proxy for the Java-side JniCatalog class. The interface is a set of
/// wrapper functions for methods called over JNI.
class Catalog {
 public:
  /// Does all the work of initialising the JNI method stubs. If any method can't be found,
  /// or if there is any further exception, the constructor will terminate the process.
  Catalog();

  /// Executes the given TDdlExecRequest and returns a response with details on the
  /// result of the operation. Returns OK if the operation was successful,
  /// otherwise a Status object with information on the error will be returned.
  Status ExecDdl(const TDdlExecRequest& req, TDdlExecResponse* resp);

  /// Executes the given TUpdateCatalogRequest and returns a response with details on
  /// the result of the operation. Returns OK if the operation was successful,
  /// otherwise a Status object with information on the error will be returned.
  Status UpdateCatalog(const TUpdateCatalogRequest& req,
      TUpdateCatalogResponse* resp);

  /// Resets the metadata of a single table or the entire catalog, based on the
  /// given TResetMetadataRequest. Returns OK if the operation was successful, otherwise
  /// a Status object with information on the error will be returned.
  Status ResetMetadata(const TResetMetadataRequest& req, TResetMetadataResponse* resp);

  /// Queries the catalog to get the current version and sets the 'version' output
  /// parameter to this value. Returns OK if the operation was successful, otherwise a
  /// Status object with information on the error will be returned.
  Status GetCatalogVersion(long* version);

  /// Retrieves the catalog objects that were added/modified/deleted since version
  /// 'from_version'. Returns OK if the operation was successful, otherwise a Status
  /// object with information on the error will be returned. 'caller' is a pointer to
  /// the caller CatalogServer object. caller->AddTopicUpdate() will be repeatedly
  /// called by the frontend.
  Status GetCatalogDelta(CatalogServer* caller, int64_t from_version,
      TGetCatalogDeltaResponse* resp);

  /// Gets the Thrift representation of a Catalog object. The request is a TCatalogObject
  /// which has the desired TCatalogObjectType and name properly set.
  /// Returns OK if the operation was successful, otherwise a Status object with
  /// information on the error will be returned.
  Status GetCatalogObject(const TCatalogObject& request, TCatalogObject* response);

  /// Like the above method but get the json string of the catalog object. The json
  /// string can't be deserialized to Thrift objects so can only be used in showing debug
  /// infos.
  Status GetJsonCatalogObject(const TCatalogObject& req, std::string* res);

  /// Return partial information about a Catalog object.
  /// Returns OK if the operation was successful, otherwise a Status object with
  /// information on the error will be returned.
  Status GetPartialCatalogObject(const TGetPartialCatalogObjectRequest& request,
      TGetPartialCatalogObjectResponse* response);

  /// Return all databases matching the optional argument 'pattern'.
  /// If pattern is NULL, match all databases otherwise match only those databases that
  /// match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  /// and each pN may contain wildcards denoted by '*' which match all strings.
  /// TODO: GetDbs() and GetTableNames() can probably be scrapped in favor of
  /// GetCatalogDelta(). Consider removing them and moving everything to use
  /// that.
  Status GetDbs(const std::string* pattern, TGetDbsResult* dbs);

  /// Returns all matching table names, per Hive's "SHOW TABLES <pattern>". Each
  /// table name returned is unqualified.
  /// If pattern is NULL, match all tables otherwise match only those tables that
  /// match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  /// and each pN may contain wildcards denoted by '*' which match all strings.
  Status GetTableNames(const std::string& db, const std::string* pattern,
      TGetTablesResult* table_names);

  /// Returns the collected metrics of a table. The response contains a
  /// pretty-printed string representation of table metrics.
  Status GetTableMetrics(const std::string& db, const std::string& tbl,
      std::string* metrics);

  /// Returns the Catalog server metrics in the response object. Refer to
  /// TGetCatalogServerMetricsResponse definition for details.
  Status GetCatalogServerMetrics(TGetCatalogServerMetricsResponse* response);

  /// Returns the current catalog usage that includes the most frequently accessed
  /// tables as well as the tables with the highest memory requirements.
  Status GetCatalogUsage(TGetCatalogUsageResponse* response);

  /// Returns the running catalog operation metrics
  Status GetOperationUsage(TGetOperationUsageResponse* response);

  /// Returns the metastore event processor summary view. The summary string
  /// in the response can contain detailed metrics along with status
  Status GetEventProcessorSummary(TEventProcessorMetricsSummaryResponse* response);

  /// Gets all functions in the catalog matching the parameters in the given
  /// TFunctionsRequest.
  Status GetFunctions(const TGetFunctionsRequest& request,
      TGetFunctionsResponse *response);

  /// Prioritizes the loading of metadata for the catalog objects specified in the
  /// TPrioritizeLoadRequest.
  Status PrioritizeLoad(const TPrioritizeLoadRequest& req);

  /// Get partition statistics for the partitions specified in TGetPartitionStatsRequest.
  Status GetPartitionStats(
      const TGetPartitionStatsRequest& req, TGetPartitionStatsResponse* resp);

  /// Update recently used table names and their use counts in an impalad since the last
  /// report.
  Status UpdateTableUsage(const TUpdateTableUsageRequest& req);

  /// Gets the null partition name.
  Status GetNullPartitionName(TGetNullPartitionNameResponse* resp);

  /// Gets the latest compactions for the request.
  Status GetLatestCompactions(
      const TGetLatestCompactionsRequest& req, TGetLatestCompactionsResponse* resp);

  /// Regenerate Catalog Service ID.
  /// The function should be called when the CatalogD becomes active.
  void RegenerateServiceId();

  /// Refresh the data sources from metadata store.
  /// Returns OK if the refreshing was successful, otherwise a Status object with
  /// information on the error will be returned.
  Status RefreshDataSources();
  /// Returns all Hadoop configurations in key, value form in result.
  Status GetAllHadoopConfigs(TGetAllHadoopConfigsResponse* result);

 private:
  jobject catalog_;  // instance of org.apache.impala.service.JniCatalog
  jmethodID update_metastore_id_;  // JniCatalog.updateMetaastore()
  jmethodID exec_ddl_id_;  // JniCatalog.execDdl()
  jmethodID reset_metadata_id_;  // JniCatalog.resetMetdata()
  jmethodID get_catalog_object_id_;  // JniCatalog.getCatalogObject()
  jmethodID get_json_catalog_object_id_;  // JniCatalog.getJsonCatalogObject()
  jmethodID get_partial_catalog_object_id_;  // JniCatalog.getPartialCatalogObject()
  jmethodID get_catalog_delta_id_;  // JniCatalog.getCatalogDelta()
  jmethodID get_catalog_version_id_;  // JniCatalog.getCatalogVersion()
  jmethodID get_catalog_usage_id_; // JniCatalog.getCatalogUsage()
  jmethodID get_operation_usage_id_; // JniCatalog.getOperationUsage()
  jmethodID get_catalog_server_metrics_; // JniCatalog.getCatalogServerMetrics()
  jmethodID get_event_processor_summary_; // JniCatalog.getEventProcessorMetrics()
  jmethodID get_dbs_id_; // JniCatalog.getDbs()
  jmethodID get_table_names_id_; // JniCatalog.getTableNames()
  jmethodID get_table_metrics_id_; // JniCatalog.getTableMetrics()
  jmethodID get_functions_id_; // JniCatalog.getFunctions()
  jmethodID get_partition_stats_id_; // JniCatalog.getPartitionStats()
  jmethodID prioritize_load_id_; // JniCatalog.prioritizeLoad()
  jmethodID catalog_ctor_;
  jmethodID update_table_usage_id_;
  jmethodID regenerate_service_id_; // JniCatalog.regenerateServiceId()
  jmethodID refresh_data_sources_; // JniCatalog.refreshDataSources()
  jmethodID get_null_partition_name_id_; // JniCatalog.getNullPartitionName()
  jmethodID get_latest_compactions_id_; // JniCatalog.getLatestCompactions()
  jmethodID get_hadoop_configs_id_;  // JniCatalog.getAllHadoopConfigs()
};

}
#endif
