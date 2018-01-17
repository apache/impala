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

namespace cpp impala
namespace java org.apache.impala.thrift

include "CatalogObjects.thrift"

// Contains structures used internally by the Catalog Server.

// Arguments to a GetCatalogDelta call.
struct TGetCatalogDeltaRequest {
  // The base catalog version from which the delta is computed.
  1: required i64 from_version

  // The native caller ptr for calling back NativeAddPendingTopicItem().
  2: required i64 native_catalog_server_ptr
}

// Response from a call to GetCatalogDelta. The catalog object updates are passed
// separately via NativeAddPendingTopicItem() callback.
struct TGetCatalogDeltaResponse {
  // The maximum catalog version of all objects in this response or 0 if the Catalog
  // contained no objects.
  1: required i64 max_catalog_version

  // List of updated (new and modified) catalog objects whose catalog verion is
  // larger than TGetCatalotDeltaRequest.from_version. Deprecated after IMPALA-5990.
  2: optional list<CatalogObjects.TCatalogObject> updated_objects_deprecated

  // List of deleted catalog objects whose catalog version is larger than
  // TGetCatalogDelta.from_version. Deprecated after IMPALA-5990.
  3: optional list<CatalogObjects.TCatalogObject> deleted_objects_deprecated
}
