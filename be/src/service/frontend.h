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

#ifndef IMPALA_SERVICE_FRONTEND_H
#define IMPALA_SERVICE_FRONTEND_H

#include <jni.h>

#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Frontend_types.h"
#include "common/status.h"

namespace impala {

// The Frontend is a proxy for the Java-side JniFrontend class. The interface is a set of
// wrapper methods for methods called over JNI.
// TODO: Consider changing all methods to accept and return only Thrift structures so that
// all go through exactly the same calling code.
class Frontend {
 public:
  // Does all the work of initialising the JNI method stubs. If any method can't be found,
  // or if there is any further exception, the constructor will terminate the process.
  Frontend();

  // Make any changes required to the metastore as a result of an INSERT query, e.g. newly
  // created partitions.
  Status UpdateMetastore(const TCatalogUpdate& catalog_update);

  // Call FE to get explain plan
  Status GetExplainPlan(const TClientRequest& query_request, std::string* explain_string);

  // Call FE to get TClientRequestResult.
  Status GetExecRequest(const TClientRequest& request, TExecRequest* result);

  // Returns all matching table names, per Hive's "SHOW TABLES <pattern>". Each
  // table name returned is unqualified.
  // If pattern is NULL, match all tables otherwise match only those tables that
  // match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  // and each pN may contain wildcards denoted by '*' which match all strings.
  // The TSessionState parameter is used to filter results of metadata operations when
  // authorization is enabled. If this is a user initiated request, it should
  // be set to the user's current session. If this is an Impala internal request,
  // the session should be set to NULL which will skip privilege checks returning all
  // results.
  Status GetTableNames(const std::string& db, const std::string* pattern,
      const TSessionState* session, TGetTablesResult* table_names);

  // Return all databases matching the optional argument 'pattern'.
  // If pattern is NULL, match all databases otherwise match only those databases that
  // match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  // and each pN may contain wildcards denoted by '*' which match all strings.
  // The TSessionState parameter is used to filter results of metadata operations when
  // authorization is enabled. If this is a user initiated request, it should
  // be set to the user's current session. If this is an Impala internal request,
  // the session should be set to NULL which will skip privilege checks returning all
  // results.
  Status GetDbNames(const std::string* pattern, const TSessionState* session,
      TGetDbsResult* table_names);

  // Return all functions matching the optional argument 'pattern'.
  // If pattern is NULL match all functions, otherwise match only those functions that
  // match the pattern string.
  // The TSessionState parameter is used to filter results of metadata operations when
  // authorization is enabled. If this is a user initiated request, it should
  // be set to the user's current session. If this is an Impala internal request,
  // the session should be set to NULL which will skip privilege checks returning all
  // results.
  Status GetFunctions(const std::string& db, const std::string* pattern,
      const TSessionState* session, TGetFunctionsResult* functions);

  // Returns (in the output parameter) the result of a DESCRIBE table command. This
  // command retrieves table metadata, such as the column definitions. The metadata
  // that is returned is controlled by setting the 'output_style' field. If this
  // field is set to MINIMAL, only the column definitions are returned. If set to
  // FORMATTED, extended metadata is returned (in addition to the column defs).
  // This includes info about the table properties, SerDe properties, StorageDescriptor
  // properties, and more.
  Status DescribeTable(const TDescribeTableParams& params,
      TDescribeTableResult* response);

  // Executes the given TDdlExecRequest and returns a response with details on the
  // result of the operation. Returns OK if the operation was successfull,
  // otherwise a Status object with information on the error will be returned. Only
  // supports true DDL operations (CREATE/ALTER/DROP), pseudo-DDL operations such as
  // SHOW/RESET/USE should be executed using their appropriate executor functions.
  Status ExecDdlRequest(const TDdlExecRequest& params, TDdlExecResponse* resp);

  // Reset the metadata
  Status ResetMetadata(const TResetMetadataParams& reset_metadata_params);

  // Validate Hadoop config; requires FE
  Status ValidateSettings();

  // Calls FE to execute HiveServer2 metadata operation.
  Status ExecHiveServer2MetadataOp(const TMetadataOpRequest& request,
                                   TMetadataOpResponse* result);

  // Writes a table of all Hadoop configurations, either in text or as HTML per the
  // as_text parameter, to the output stringstream.
  Status RenderHadoopConfigs(bool as_text, std::stringstream* output);

  // Loads a single file or set of files into a table or partition. Saves the RPC
  // response in the TLoadDataResp output parameter. Returns OK if the operation
  // completed successfully.
  Status LoadData(const TLoadDataReq& load_data_request, TLoadDataResp* response);

  // Returns true if the error returned by the FE was due to an AuthorizationException.
  static bool IsAuthorizationError(const Status& status);

 private:
  // Descriptor of Java Frontend class itself, used to create a new instance.
  jclass fe_class_;

  jobject fe_;  // instance of com.cloudera.impala.service.JniFrontend
  jmethodID create_exec_request_id_;  // JniFrontend.createExecRequest()
  jmethodID get_explain_plan_id_;  // JniFrontend.getExplainPlan()
  jmethodID get_hadoop_config_id_;  // JniFrontend.getHadoopConfig()
  jmethodID check_config_id_; // JniFrontend.checkConfiguration()
  jmethodID update_metastore_id_; // JniFrontend.updateMetastore()
  jmethodID get_table_names_id_; // JniFrontend.getTableNames
  jmethodID describe_table_id_; // JniFrontend.describeTable
  jmethodID get_db_names_id_; // JniFrontend.getDbNames
  jmethodID get_functions_id_; // JniFrontend.getFunctions
  jmethodID exec_hs2_metadata_op_id_; // JniFrontend.execHiveServer2MetadataOp
  jmethodID exec_ddl_request_id_; // JniFrontend.execDdlRequest
  jmethodID reset_metadata_id_; // JniFrontend.resetMetadata
  jmethodID load_table_data_id_; // JniFrontend.loadTableData
  jmethodID fe_ctor_;

  struct FrontendMethodDescriptor;

  // Utility method to load a method whose signature is in the supplied descriptor; if
  // successful descriptor->method_id is set to a JNI method handle.
  void LoadJniFrontendMethod(JNIEnv* jni_env, FrontendMethodDescriptor* descriptor);

  // Utility methods to avoid repeating lots of the JNI call boilerplate.
  template <typename T>
  Status CallJniMethod(const jmethodID& method, const T& arg);
  template <typename T, typename R>
  Status CallJniMethod(
      const jmethodID& method, const T& arg, R* response);
  template <typename T>
  Status CallJniMethod(
      const jmethodID& method, const T& arg, std::string* response);
};

}

#endif
