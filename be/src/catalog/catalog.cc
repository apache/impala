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

#include "catalog/catalog.h"

#include <list>
#include <string>

#include "common/logging.h"
#include "rpc/thrift-util.h"
#include "util/jni-util.h"
#include "util/logging-support.h"

using namespace std;
using namespace impala;

DECLARE_int32(non_impala_java_vlog);

// Describes one method to look up in a Catalog object
struct Catalog::MethodDescriptor {
  // Name of the method, case must match
  const string name;

  // JNI-style method signature
  const string signature;

  // Handle to the method, set by LoadJNIMethod
  jmethodID* method_id;
};

Catalog::Catalog() {
  MethodDescriptor methods[] = {
    {"<init>", "(II)V", &catalog_ctor_},
    {"updateCatalog", "([B)[B", &update_metastore_id_},
    {"execDdl", "([B)[B", &exec_ddl_id_},
    {"resetMetadata", "([B)[B", &reset_metadata_id_},
    {"getTableNames", "([B)[B", &get_table_names_id_},
    {"getDbNames", "([B)[B", &get_db_names_id_},
    {"getCatalogObject", "([B)[B", &get_catalog_object_id_},
    {"getCatalogObjects", "(J)[B", &get_catalog_objects_id_}};

  JNIEnv* jni_env = getJNIEnv();
  // Create an instance of the java class JniCatalog
  catalog_class_ = jni_env->FindClass("com/cloudera/impala/service/JniCatalog");
  EXIT_IF_EXC(jni_env);

  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    LoadJniMethod(jni_env, &(methods[i]));
  }

  jobject catalog = jni_env->NewObject(catalog_class_, catalog_ctor_,
      FlagToTLogLevel(FLAGS_v), FlagToTLogLevel(FLAGS_non_impala_java_vlog));
  EXIT_IF_EXC(jni_env);
  EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, catalog, &catalog_));
}

void Catalog::LoadJniMethod(JNIEnv* jni_env, MethodDescriptor* descriptor) {
  (*descriptor->method_id) = jni_env->GetMethodID(catalog_class_,
      descriptor->name.c_str(), descriptor->signature.c_str());
  EXIT_IF_EXC(jni_env);
}

Status Catalog::GetCatalogObject(const TCatalogObject& req,
    TCatalogObject* resp) {
  return JniUtil::CallJniMethod(catalog_, get_catalog_object_id_, req, resp);
}

Status Catalog::GetAllCatalogObjects(long from_version,
    TGetAllCatalogObjectsResponse* resp) {
  JNIEnv* jni_env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(jni_env));
  jvalue requested_from_version;
  requested_from_version.j = from_version;
  jbyteArray result_bytes = static_cast<jbyteArray>(
      jni_env->CallObjectMethod(catalog_, get_catalog_objects_id_,
      requested_from_version));
  RETURN_ERROR_IF_EXC(jni_env);
  RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, resp));
  return Status::OK;
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

Status Catalog::GetDbNames(const string* pattern, TGetDbsResult* db_names) {
  TGetDbsParams params;
  if (pattern != NULL) params.__set_pattern(*pattern);
  return JniUtil::CallJniMethod(catalog_, get_db_names_id_, params, db_names);
}

Status Catalog::GetTableNames(const string& db, const string* pattern,
    TGetTablesResult* table_names) {
  TGetTablesParams params;
  params.__set_db(db);
  if (pattern != NULL) params.__set_pattern(*pattern);
  return JniUtil::CallJniMethod(catalog_, get_table_names_id_, params, table_names);
}
