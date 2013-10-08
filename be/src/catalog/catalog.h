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

#ifndef IMPALA_CATALOG_CATALOG_H
#define IMPALA_CATALOG_CATALOG_H

#include <jni.h>

#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/CatalogService_types.h"
#include "common/status.h"

namespace impala {

// The Catalog is a proxy for the Java-side JniCatalog class. The interface is a set of
// wrapper functions for methods called over JNI.
class Catalog {
 public:
  // Does all the work of initialising the JNI method stubs. If any method can't be found,
  // or if there is any further exception, the constructor will terminate the process.
  Catalog();

  // Executes the given TDdlExecRequest and returns a response with details on the
  // result of the operation. Returns OK if the operation was successful,
  // otherwise a Status object with information on the error will be returned.
  Status ExecDdl(const TDdlExecRequest& req, TDdlExecResponse* resp);

  // Executes the given TUpdateMetastoreRequest and returns a response with details on
  // the result of the operation. Returns OK if the operation was successful,
  // otherwise a Status object with information on the error will be returned.
  Status UpdateMetastore(const TUpdateMetastoreRequest& req,
      TUpdateMetastoreResponse* resp);

  // Resets the metadata of a single table or the entire catalog, based on the
  // given TResetMetadataRequest. Returns OK if the operation was successful, otherwise
  // a Status object with information on the error will be returned.
  Status ResetMetadata(const TResetMetadataRequest& req, TResetMetadataResponse* resp);

  // Gets all Catalog objects and the metadata that is applicable applicable for
  // the given request. Always returns all object names that exist in the Catalog, but
  // allows for extended metadata for objects that were modified after a specific version.
  // Returns OK if the operation was successful, otherwise a Status object with
  // information on the error will be returned.
  Status GetAllCatalogObjects(const TGetAllCatalogObjectsRequest& req,
      TGetAllCatalogObjectsResponse* resp);

  // Gets the Thrift representation of a Catalog object. The request is a TCatalogObject
  // which has the desired TCatalogObjectType and name properly set.
  // Returns OK if the operation was successful, otherwise a Status object with
  // information on the error will be returned.
  Status GetCatalogObject(const TCatalogObject& request, TCatalogObject* response);

  // Return all databases matching the optional argument 'pattern'.
  // If pattern is NULL, match all databases otherwise match only those databases that
  // match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  // and each pN may contain wildcards denoted by '*' which match all strings.
  // TODO: GetDbNames() and GetTableNames() can probably be scraped in favor of
  // GetAllCatalogObjects(). Consider removing them and moving everything to use
  // that.
  Status GetDbNames(const std::string* pattern, TGetDbsResult* table_names);

  // Returns all matching table names, per Hive's "SHOW TABLES <pattern>". Each
  // table name returned is unqualified.
  // If pattern is NULL, match all tables otherwise match only those tables that
  // match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  // and each pN may contain wildcards denoted by '*' which match all strings.
  Status GetTableNames(const std::string& db, const std::string* pattern,
      TGetTablesResult* table_names);

 private:
  // Descriptor of Java Catalog class itself, used to create a new instance.
  jclass catalog_class_;

  jobject catalog_;  // instance of com.cloudera.impala.service.JniCatalog
  jmethodID update_metastore_id_;  // CatalogServiceFrontend.updateMetaastore()
  jmethodID exec_ddl_id_;  // CatalogServiceFrontend.execDdl()
  jmethodID reset_metadata_id_;  // CatalogServiceFrontend.resetMetdata()
  jmethodID get_catalog_object_id_;  // CatalogServiceFrontend.getCatalogObject()
  jmethodID get_catalog_objects_id_;  // CatalogServiceFrontend.getCatalogObjects()
  jmethodID get_db_names_id_; // CatalogServiceFrontend.getDbNames()
  jmethodID get_table_names_id_; // CatalogServiceFrontend.getTableNames()
  jmethodID catalog_ctor_;

  struct MethodDescriptor;

  // Utility method to load a method whose signature is in the supplied descriptor; if
  // successful descriptor->method_id is set to a JNI method handle.
  void LoadJniMethod(JNIEnv* jni_env, MethodDescriptor* descriptor);
};

}
#endif
