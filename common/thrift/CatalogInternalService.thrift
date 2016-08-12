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

// Response from a call to GetAllCatalogObjects. Contains all known Catalog objects
// (databases, tables/views, and functions) from the CatalogService's cache.
// What metadata is included for each object is based on the parameters used in
// the request.
struct TGetAllCatalogObjectsResponse {
  // The maximum catalog version of all objects in this response or 0 if the Catalog
  // contained no objects.
  1: required i64 max_catalog_version

  // List of catalog objects (empty list if no objects detected in the Catalog).
  2: required list<CatalogObjects.TCatalogObject> objects
}
