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

#include "catalog/catalog.h"

#include <list>
#include <string>

#include "common/logging.h"
#include "common/thread-debug-info.h"
#include "rpc/jni-thrift-util.h"
#include "util/backend-gflag-util.h"

#include "common/names.h"

using namespace impala;


DEFINE_bool(load_catalog_in_background, false,
    "If true, loads catalog metadata in the background. If false, metadata is loaded "
    "lazily (on access).");
DEFINE_int32(num_metadata_loading_threads, 16,
    "(Advanced) The number of metadata loading threads (degree of parallelism) to use "
    "when loading catalog metadata.");
DEFINE_int32(max_hdfs_partitions_parallel_load, 5,
    "(Advanced) Number of threads used to load block metadata for HDFS based partitioned "
    "tables. Due to HDFS architectural limitations, it is unlikely to get a linear "
    "speed up beyond 5 threads.");
DEFINE_int32(max_nonhdfs_partitions_parallel_load, 20,
    "(Advanced) Number of threads used to load block metadata for tables that do not "
    "support the notion of blocks/storage IDs. Currently supported for S3/ADLS.");
DEFINE_int32(initial_hms_cnxn_timeout_s, 120,
    "Number of seconds catalogd will wait to establish an initial connection to the HMS "
    "before exiting.");

Catalog::Catalog() {
  JniMethodDescriptor methods[] = {
    {"<init>", "([B)V", &catalog_ctor_},
    {"updateCatalog", "([B)[B", &update_metastore_id_},
    {"execDdl", "([B)[B", &exec_ddl_id_},
    {"resetMetadata", "([B)[B", &reset_metadata_id_},
    {"getTableNames", "([B)[B", &get_table_names_id_},
    {"getTableMetrics", "([B)Ljava/lang/String;", &get_table_metrics_id_},
    {"getDbs", "([B)[B", &get_dbs_id_},
    {"getFunctions", "([B)[B", &get_functions_id_},
    {"getCatalogObject", "([B)[B", &get_catalog_object_id_},
    {"getJsonCatalogObject", "([B)Ljava/lang/String;", &get_json_catalog_object_id_},
    {"getPartialCatalogObject", "([B)[B", &get_partial_catalog_object_id_},
    {"getCatalogDelta", "([B)[B", &get_catalog_delta_id_},
    {"getCatalogUsage", "()[B", &get_catalog_usage_id_},
    {"getOperationUsage", "()[B", &get_operation_usage_id_},
    {"getCatalogVersion", "()J", &get_catalog_version_id_},
    {"getCatalogServerMetrics", "()[B", &get_catalog_server_metrics_},
    {"getEventProcessorSummary", "()[B", &get_event_processor_summary_},
    {"prioritizeLoad", "([B)V", &prioritize_load_id_},
    {"getPartitionStats", "([B)[B", &get_partition_stats_id_},
    {"updateTableUsage", "([B)V", &update_table_usage_id_},
    {"regenerateServiceId", "()V", &regenerate_service_id_},
    {"refreshDataSources", "()V", &refresh_data_sources_},
    {"getNullPartitionName", "()[B", &get_null_partition_name_id_},
    {"getLatestCompactions", "([B)[B", &get_latest_compactions_id_},
    {"getAllHadoopConfigs", "()[B", &get_hadoop_configs_id_},
  };

  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  // Used to release local references promptly
  JniLocalFrame jni_frame;
  ABORT_IF_ERROR(jni_frame.push(jni_env));

  // Create an instance of the java class JniCatalog
  jclass catalog_class = jni_env->FindClass("org/apache/impala/service/JniCatalog");
  ABORT_IF_EXC(jni_env);

  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    ABORT_IF_ERROR(JniUtil::LoadJniMethod(jni_env, catalog_class, &(methods[i])));
  }

  jbyteArray cfg_bytes;
  ABORT_IF_ERROR(GetThriftBackendGFlagsForJNI(jni_env, &cfg_bytes));

  jobject catalog = jni_env->NewObject(catalog_class, catalog_ctor_, cfg_bytes);
  CLEAN_EXIT_IF_EXC(jni_env);
  ABORT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, catalog, &catalog_));
}

Status Catalog::GetCatalogObject(const TCatalogObject& req,
    TCatalogObject* resp) {
  return JniUtil::CallJniMethod(catalog_, get_catalog_object_id_, req, resp);
}

Status Catalog::GetJsonCatalogObject(const TCatalogObject& req, string* res) {
  return JniUtil::CallJniMethod(catalog_, get_json_catalog_object_id_, req, res);
}

Status Catalog::GetPartialCatalogObject(const TGetPartialCatalogObjectRequest& req,
    TGetPartialCatalogObjectResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, get_partial_catalog_object_id_, req, resp);
}

Status Catalog::GetCatalogVersion(long* version) {
  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  *version = jni_env->CallLongMethod(catalog_, get_catalog_version_id_);
  RETURN_ERROR_IF_EXC(jni_env);
  return Status::OK();
}

Status Catalog::GetCatalogDelta(CatalogServer* caller, int64_t from_version,
    TGetCatalogDeltaResponse* resp) {
  TGetCatalogDeltaRequest request;
  request.__set_native_catalog_server_ptr(reinterpret_cast<int64_t>(caller));
  request.__set_from_version(from_version);
  return JniUtil::CallJniMethod(catalog_, get_catalog_delta_id_, request, resp);
}

Status Catalog::ExecDdl(const TDdlExecRequest& req, TDdlExecResponse* resp) {
  if (req.__isset.header && req.header.__isset.query_id) {
    GetThreadDebugInfo()->SetQueryId(req.header.query_id);
  }
  return JniUtil::CallJniMethod(catalog_, exec_ddl_id_, req, resp);
}

Status Catalog::ResetMetadata(const TResetMetadataRequest& req,
    TResetMetadataResponse* resp) {
  if (req.__isset.header && req.header.__isset.query_id) {
    GetThreadDebugInfo()->SetQueryId(req.header.query_id);
  }
  return JniUtil::CallJniMethod(catalog_, reset_metadata_id_, req, resp);
}

Status Catalog::UpdateCatalog(const TUpdateCatalogRequest& req,
    TUpdateCatalogResponse* resp) {
  if (req.__isset.header && req.header.__isset.query_id) {
    GetThreadDebugInfo()->SetQueryId(req.header.query_id);
  }
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

Status Catalog::GetTableMetrics(const string& db, const string& tbl,
    string* table_metrics) {
  TGetTableMetricsParams params;
  TTableName tblName;
  tblName.__set_db_name(db);
  tblName.__set_table_name(tbl);
  params.__set_table_name(tblName);
  return JniUtil::CallJniMethod(catalog_, get_table_metrics_id_, params, table_metrics);
}

Status Catalog::GetCatalogUsage(TGetCatalogUsageResponse* response) {
  return JniUtil::CallJniMethod(catalog_, get_catalog_usage_id_, response);
}

Status Catalog::GetOperationUsage(TGetOperationUsageResponse* response) {
  return JniUtil::CallJniMethod(catalog_, get_operation_usage_id_, response);
}

Status Catalog::GetEventProcessorSummary(
    TEventProcessorMetricsSummaryResponse* response) {
  return JniUtil::CallJniMethod(catalog_, get_event_processor_summary_, response);
}

Status Catalog::GetCatalogServerMetrics(TGetCatalogServerMetricsResponse* response) {
  return JniUtil::CallJniMethod(catalog_, get_catalog_server_metrics_, response);
}

Status Catalog::GetFunctions(const TGetFunctionsRequest& request,
    TGetFunctionsResponse *response) {
  return JniUtil::CallJniMethod(catalog_, get_functions_id_, request, response);
}

Status Catalog::PrioritizeLoad(const TPrioritizeLoadRequest& req) {
  if (req.__isset.header && req.header.__isset.query_id) {
    GetThreadDebugInfo()->SetQueryId(req.header.query_id);
  }
  return JniUtil::CallJniMethod(catalog_, prioritize_load_id_, req);
}

Status Catalog::GetPartitionStats(
    const TGetPartitionStatsRequest& req, TGetPartitionStatsResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, get_partition_stats_id_, req, resp);
}

Status Catalog::UpdateTableUsage(const TUpdateTableUsageRequest& req) {
  return JniUtil::CallJniMethod(catalog_, update_table_usage_id_, req);
}

Status Catalog::GetAllHadoopConfigs(TGetAllHadoopConfigsResponse* result) {
  return JniUtil::CallJniMethod(catalog_, get_hadoop_configs_id_, result);
}

void Catalog::RegenerateServiceId() {
  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  jni_env->CallVoidMethod(catalog_, regenerate_service_id_);
  ABORT_IF_EXC(jni_env);
}

Status Catalog::RefreshDataSources() {
  return JniUtil::CallJniMethod(catalog_, refresh_data_sources_);
}

Status Catalog::GetNullPartitionName(TGetNullPartitionNameResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, get_null_partition_name_id_, resp);
}

Status Catalog::GetLatestCompactions(
    const TGetLatestCompactionsRequest& req, TGetLatestCompactionsResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, get_latest_compactions_id_, req, resp);
}
