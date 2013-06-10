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

#include "util/jni-util.h"
#include "common/logging.h"
#include "util/thrift-util.h"

using namespace std;
using namespace impala;

DEFINE_bool(load_catalog_at_startup, false, "if true, load all catalog data at startup");

// Authorization related flags. Must be set to valid values to properly configure
// authorization.
DEFINE_string(server_name, "", "The name to use for securing this impalad "
              "server during authorization. If set, authorization will be enabled.");
DEFINE_string(authorization_policy_file, "", "HDFS path to the authorization policy "
              "file. If set, authorization will be enabled.");
DEFINE_string(authorization_policy_provider_class,
    "org.apache.access.provider.file.HadoopGroupResourceAuthorizationProvider",
    "Advanced: The authorization policy provider class name.");

// Describes one method to look up in a Frontend object
struct Frontend::FrontendMethodDescriptor {
  // Name of the method, case must match
  const string name;

  // JNI-style method signature
  const string signature;

  // Handle to the method, set by LoadJNIFrontendMethod
  jmethodID* method_id;
};

Frontend::Frontend() {
  FrontendMethodDescriptor methods[] = {
    {"<init>", "(ZLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)V", &fe_ctor_},
    {"createExecRequest", "([B)[B", &create_exec_request_id_},
    {"getExplainPlan", "([B)Ljava/lang/String;", &get_explain_plan_id_},
    {"getHadoopConfig", "(Z)Ljava/lang/String;", &get_hadoop_config_id_},
    {"getHadoopConfigValue", "(Ljava/lang/String;)Ljava/lang/String;",
         &get_hadoop_config_value_id_},
    {"checkConfiguration", "()Ljava/lang/String;", &check_config_id_},
    {"updateMetastore", "([B)V", &update_metastore_id_},
    {"getTableNames", "([B)[B", &get_table_names_id_},
    {"describeTable", "([B)[B", &describe_table_id_},
    {"getDbNames", "([B)[B", &get_db_names_id_},
    {"execHiveServer2MetadataOp", "([B)[B", &exec_hs2_metadata_op_id_},
    {"alterTable", "([B)V", &alter_table_id_},
    {"alterView", "([B)V", &alter_view_id_},
    {"createTable", "([B)V", &create_table_id_},
    {"createTableLike", "([B)V", &create_table_like_id_},
    {"createView", "([B)V", &create_view_id_},
    {"createDatabase", "([B)V", &create_database_id_},
    {"dropTableOrView", "([B)V", &drop_table_or_view_id_},
    {"dropDatabase", "([B)V", &drop_database_id_},
    {"resetMetadata", "([B)V", &reset_metadata_id_},
    {"loadTableData", "([B)[B", &load_table_data_id_}};

  JNIEnv* jni_env = getJNIEnv();
  // create instance of java class JniFrontend
  fe_class_ = jni_env->FindClass("com/cloudera/impala/service/JniFrontend");

  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    LoadJniFrontendMethod(jni_env, &(methods[i]));
  };

  jboolean lazy = (FLAGS_load_catalog_at_startup ? false : true);
  jstring policy_file_path =
      jni_env->NewStringUTF(FLAGS_authorization_policy_file.c_str());
  jstring server_name =
      jni_env->NewStringUTF(FLAGS_server_name.c_str());
  jstring policy_provider_class_name =
      jni_env->NewStringUTF(FLAGS_authorization_policy_provider_class.c_str());

  jobject fe = jni_env->NewObject(fe_class_, fe_ctor_, lazy, server_name,
      policy_file_path, policy_provider_class_name);
  EXIT_IF_EXC(jni_env);
  EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, fe, &fe_));
}

void Frontend::LoadJniFrontendMethod(JNIEnv* jni_env,
    FrontendMethodDescriptor* descriptor) {
  (*descriptor->method_id) = jni_env->GetMethodID(fe_class_, descriptor->name.c_str(),
      descriptor->signature.c_str());
  EXIT_IF_EXC(jni_env);
}

template <typename T>
Status Frontend::CallJniMethodWithThriftArgs(const jmethodID& method, const T& arg) {
  JNIEnv* jni_env = getJNIEnv();
  jbyteArray request_bytes;
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &arg, &request_bytes));

  jni_env->CallObjectMethod(fe_, method, request_bytes);
  RETURN_ERROR_IF_EXC(jni_env);

  return Status::OK;
}

template <typename T, typename R>
Status Frontend::CallJniMethodWithThriftArgs(const jmethodID& method, const T& arg,
    R* response) {
  JNIEnv* jni_env = getJNIEnv();
  jbyteArray request_bytes;
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &arg, &request_bytes));

  jbyteArray result_bytes = static_cast<jbyteArray>(
      jni_env->CallObjectMethod(fe_, method, request_bytes));
  RETURN_ERROR_IF_EXC(jni_env);
  RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, response));

  return Status::OK;
}

template <typename T>
Status Frontend::CallJniMethodWithThriftArgs(const jmethodID& method, const T& arg,
    string* response) {
  JNIEnv* jni_env = getJNIEnv();
  jbyteArray request_bytes;
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &arg, &request_bytes));
  jstring java_response_string = static_cast<jstring>(
      jni_env->CallObjectMethod(fe_, method, request_bytes));
  RETURN_ERROR_IF_EXC(jni_env);
  jboolean is_copy;
  const char *str = jni_env->GetStringUTFChars(java_response_string, &is_copy);
  RETURN_ERROR_IF_EXC(jni_env);
  *response = str;
  jni_env->ReleaseStringUTFChars(java_response_string, str);
  RETURN_ERROR_IF_EXC(jni_env);
  return Status::OK;
}

Status Frontend::UpdateMetastore(const TCatalogUpdate& catalog_update) {
  VLOG_QUERY << "UpdateMetastore()";
  return CallJniMethodWithThriftArgs(update_metastore_id_, catalog_update);
}

Status Frontend::AlterTable(const TAlterTableParams& params) {
  return CallJniMethodWithThriftArgs(alter_table_id_, params);
}

Status Frontend::AlterView(const TCreateOrAlterViewParams& params) {
  return CallJniMethodWithThriftArgs(alter_view_id_, params);
}

Status Frontend::CreateDatabase(const TCreateDbParams& params) {
  return CallJniMethodWithThriftArgs(create_database_id_, params);
}

Status Frontend::CreateTableLike(const TCreateTableLikeParams& params) {
  return CallJniMethodWithThriftArgs(create_table_like_id_, params);
}

Status Frontend::CreateTable(const TCreateTableParams& params) {
  return CallJniMethodWithThriftArgs(create_table_id_, params);
}

Status Frontend::CreateView(const TCreateOrAlterViewParams& params) {
  return CallJniMethodWithThriftArgs(create_view_id_, params);
}

Status Frontend::DropDatabase(const TDropDbParams& params) {
  return CallJniMethodWithThriftArgs(drop_database_id_, params);
}

Status Frontend::DropTableOrView(const TDropTableOrViewParams& params) {
  return CallJniMethodWithThriftArgs(drop_table_or_view_id_, params);
}

Status Frontend::ResetMetadata(const TResetMetadataParams& params) {
  return CallJniMethodWithThriftArgs(reset_metadata_id_, params);
}

Status Frontend::DescribeTable(const TDescribeTableParams& params,
    TDescribeTableResult* response) {
  return CallJniMethodWithThriftArgs(describe_table_id_, params, response);
}

Status Frontend::GetTableNames(const string& db, const string* pattern,
    const TSessionState* session, TGetTablesResult* table_names) {
  TGetTablesParams params;
  params.__set_db(db);

  if (pattern != NULL) {
    params.__set_pattern(*pattern);
  }
  if (session != NULL) {
    params.__set_session(*session);
  }

  return CallJniMethodWithThriftArgs(get_table_names_id_, params, table_names);
}

Status Frontend::GetDbNames(const string* pattern, const TSessionState* session,
    TGetDbsResult* db_names) {
  TGetDbsParams params;
  if (pattern != NULL) {
    params.__set_pattern(*pattern);
  }
  if (session != NULL) {
    params.__set_session(*session);
  }

  return CallJniMethodWithThriftArgs(get_db_names_id_, params, db_names);
}

Status Frontend::GetExecRequest(
    const TClientRequest& request, TExecRequest* result) {
  return CallJniMethodWithThriftArgs(create_exec_request_id_, request, result);
}

Status Frontend::GetExplainPlan(
    const TClientRequest& query_request, string* explain_string) {
  return CallJniMethodWithThriftArgs(
      get_explain_plan_id_, query_request, explain_string);
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

Status Frontend::GetHadoopConfigValue(const string& key, string* output) {
  JNIEnv* jni_env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  jstring value_arg = jni_env->NewStringUTF(key.c_str());
  RETURN_ERROR_IF_EXC(jni_env);
  jstring java_config_value = static_cast<jstring>(
      jni_env->CallObjectMethod(fe_, get_hadoop_config_value_id_, value_arg));
  RETURN_ERROR_IF_EXC(jni_env);
  const char *str = jni_env->GetStringUTFChars(java_config_value, NULL);
  RETURN_ERROR_IF_EXC(jni_env);
  *output = str;
  jni_env->ReleaseStringUTFChars(java_config_value, str);
  RETURN_ERROR_IF_EXC(jni_env);

  return Status::OK;
}

Status Frontend::ExecHiveServer2MetadataOp(const TMetadataOpRequest& request,
    TMetadataOpResponse* result) {
  return CallJniMethodWithThriftArgs(exec_hs2_metadata_op_id_, request, result);
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
  return CallJniMethodWithThriftArgs(load_table_data_id_, request, response);
}
