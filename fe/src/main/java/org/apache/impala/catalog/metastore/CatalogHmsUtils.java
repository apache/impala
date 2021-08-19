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

package org.apache.impala.catalog.metastore;

/**
 * Contains constants used for CatalogD HMS metrics, which are migrated from
 * CatalogMetastoreServer.java. The purpose is to ignore the compilation of Catalog MS
 * (using Hive 4 APIs) when using Apache Hive 3 without affecting other classes.
 */
public class CatalogHmsUtils {

  // Metrics for CatalogD HMS cache
  public static final String CATALOGD_CACHE_MISS_METRIC = "catalogd-hms-cache.miss";
  public static final String CATALOGD_CACHE_HIT_METRIC = "catalogd-hms-cache.hit";
  public static final String CATALOGD_CACHE_API_REQUESTS_METRIC =
      "catalogd-hms-cache.api-requests";

  // CatalogD HMS Cache - API specific metrics
  public static final String CATALOGD_CACHE_API_MISS_METRIC =
      "catalogd-hms-cache.cache-miss.api.%s";
  public static final String CATALOGD_CACHE_API_HIT_METRIC =
      "catalogd-hms-cache.cache-hit.api.%s";
}
