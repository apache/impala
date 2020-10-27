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
 * HmsApiNameEnum has list of names of HMS APIs that will be served from CatalogD HMS
 * cache, if CatalogD caching is enabled.
 */
public enum HmsApiNameEnum {
  GET_TABLE_REQ("get_table_req"),
  GET_PARTITION_BY_EXPR("get_partitions_by_expr"),
  GET_PARTITION_BY_NAMES("get_partitions_by_names_req");

  private final String apiName;

  HmsApiNameEnum(String apiName) {
    this.apiName = apiName;
  }

  public String apiName() {
    return apiName;
  }

  public static boolean contains(String apiName) {
    for (HmsApiNameEnum api : HmsApiNameEnum.values()) {
      if (api.name().equals(apiName)) {
        return true;
      }
    }
    return false;
  }
}
