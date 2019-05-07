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
DEFINE_int64(sentry_catalog_polling_frequency_s, 60,
    "Frequency (in seconds) at which the the catalogd polls the sentry service to update "
    "any policy changes.");
DEFINE_string(sentry_config, "", "Local path to a sentry-site.xml configuration "
    "file. If set, authorization will be enabled.");

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
    {"checkUserSentryAdmin", "([B)[B", &sentry_admin_check_id_},
    {"getCatalogObject", "([B)[B", &get_catalog_object_id_},
    {"getPartialCatalogObject", "([B)[B", &get_partial_catalog_object_id_},
    {"getCatalogDelta", "([B)[B", &get_catalog_delta_id_},
    {"getCatalogUsage", "()[B", &get_catalog_usage_id_},
    {"getCatalogVersion", "()J", &get_catalog_version_id_},
    {"getCatalogServerMetrics", "()[B", &get_catalog_server_metrics_},
    {"getEventProcessorSummary", "()[B", &get_event_processor_summary_},
    {"prioritizeLoad", "([B)V", &prioritize_load_id_},
    {"getPartitionStats", "([B)[B", &get_partition_stats_id_},
    {"updateTableUsage", "([B)V", &update_table_usage_id_},
  };

  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  // Create an instance of the java class JniCatalog
  catalog_class_ = jni_env->FindClass("org/apache/impala/service/JniCatalog");
  ABORT_IF_EXC(jni_env);

  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    ABORT_IF_ERROR(JniUtil::LoadJniMethod(jni_env, catalog_class_, &(methods[i])));
  }

  jbyteArray cfg_bytes;
  ABORT_IF_ERROR(GetThriftBackendGflags(jni_env, &cfg_bytes));

  jobject catalog = jni_env->NewObject(catalog_class_, catalog_ctor_, cfg_bytes);
  CLEAN_EXIT_IF_EXC(jni_env);
  ABORT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, catalog, &catalog_));
}

Status Catalog::GetCatalogObject(const TCatalogObject& req,
    TCatalogObject* resp) {
  return JniUtil::CallJniMethod(catalog_, get_catalog_object_id_, req, resp);
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
  return JniUtil::CallJniMethod(catalog_, prioritize_load_id_, req);
}

Status Catalog::GetPartitionStats(
    const TGetPartitionStatsRequest& req, TGetPartitionStatsResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, get_partition_stats_id_, req, resp);
}

Status Catalog::SentryAdminCheck(const TSentryAdminCheckRequest& req,
    TSentryAdminCheckResponse* resp) {
  return JniUtil::CallJniMethod(catalog_, sentry_admin_check_id_, req, resp);
}

Status Catalog::UpdateTableUsage(const TUpdateTableUsageRequest& req) {
  return JniUtil::CallJniMethod(catalog_, update_table_usage_id_, req);
}
