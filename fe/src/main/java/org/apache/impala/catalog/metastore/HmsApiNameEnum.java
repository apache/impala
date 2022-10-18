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
  GET_PARTITION_BY_NAMES("get_partitions_by_names_req"),
  CREATE_DATABASE("create_database"),
  DROP_DATABASE("drop_database"),
  DROP_DATABASE_REQ("drop_database_req"),
  ALTER_DATABASE("alter_database"),
  CREATE_TABLE("create_table"),
  CREATE_TABLE_REQ("create_table_req"),
  CREATE_TABLE_WITH_CONSTRAINTS("create_table_with_constraints"),
  CREATE_TABLE_WITH_ENVIRONMENT_CONTEXT("create_table_with_environment_context"),
  ALTER_TABLE("alter_table"),
  ALTER_TABLE_WITH_ENVIRONMENT_CONTEXT("alter_table_with_environment_context"),
  ALTER_TABLE_WITH_CASCADE("alter_table_with_cascade"),
  ALTER_TABLE_REQ("alter_table_req"),
  ADD_PARTITION("add_partition"),
  ADD_PARTITION_WITH_ENVIRONMENT_CONTEXT("add_partition_with_environment_context"),
  ADD_PARTITIONS("add_partitions"),
  ADD_PARTITIONS_PSPEC("add_partitions_pspec"),
  ADD_PARTITIONS_REQ("add_partitions_req"),
  APPEND_PARTITION("append_partition"),
  APPEND_PARTITION_WITH_ENVIRONMENT_CONTEXT("append_partition_with_environment_context"),
  APPEND_PARTITION_BY_NAME("append_partition_by_name"),
  APPEND_PARTITION_BY_NAME_WITH_ENVIRONMENT_CONTEXT(
      "append_partition_by_name_with_environment_context"),
  DROP_PARTITION("drop_partition"),
  DROP_PARTITION_BY_NAME("drop_partition_by_name"),
  DROP_PARTITION_WITH_ENVIRONMENT_CONTEXT("drop_partition_with_environment_context"),
  DROP_PARTITION_BY_NAME_WITH_ENVIRONMENT_CONTEXT(
      "drop_partition_by_name_with_environment_context"),
  DROP_PARTITIONS_REQ("drop_partitions_req"),
  ALTER_PARTITION("alter_partition"),
  ALTER_PARTITIONS("alter_partitions"),
  ALTER_PARTITIONS_WITH_ENVIRONMENT_CONTEXT("alter_partitions_with_environment_context"),
  ALTER_PARTITIONS_REQ("alter_partitions_req"),
  ALTER_PARTITION_WITH_ENVIRONMENT_CONTEXT("alter_partition_with_environment_context"),
  RENAME_PARTITION("rename_partition"),
  RENAME_PARTITION_REQ("rename_partition_req"),
  EXCHANGE_PARTITION("exchange_partition"),
  EXCHANGE_PARTITIONS("exchange_partitions"),
  DROP_TABLE("drop_table"),
  DROP_TABLE_WITH_ENVIRONMENT_CONTEXT("drop_table_with_environment_context"),
  TRUNCATE_TABLE("truncate_table"),
  TRUNCATE_TABLE_REQ("truncate_tale_req");

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
