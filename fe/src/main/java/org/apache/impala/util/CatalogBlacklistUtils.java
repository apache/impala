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

package org.apache.impala.util;

import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Db;

import java.util.Set;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

public class CatalogBlacklistUtils {
  private final static Set<String> BLACKLISTED_DBS =
      CatalogBlacklistUtils.parseBlacklistedDbsFromConfigs();
  private final static Set<TableName> BLACKLISTED_TABLES =
      CatalogBlacklistUtils.parseBlacklistedTablesFromConfigs();

  /**
   * Parse blacklisted databases from backend configs.
   */
  public static Set<String> parseBlacklistedDbsFromConfigs() {
    return parseBlacklistedDbs(
        BackendConfig.INSTANCE == null ? "" : BackendConfig.INSTANCE.getBlacklistedDbs(),
        null);
  }

  /**
   * Prase blacklisted tables from backend configs.
   */
  public static Set<TableName> parseBlacklistedTablesFromConfigs() {
    return parseBlacklistedTables(
        BackendConfig.INSTANCE == null ? "" :
            BackendConfig.INSTANCE.getBlacklistedTables(),
        null);
  }

  /**
   * Parse blacklisted databases from given configs string. Pass Logger if logging is
   * necessary.
   */
  public static Set<String> parseBlacklistedDbs(String blacklistedDbsConfig,
      Logger logger) {
    Preconditions.checkNotNull(blacklistedDbsConfig);
    Set<String> blacklistedDbs = Sets.newHashSet();
    for (String db: Splitter.on(',').trimResults().omitEmptyStrings().split(
        blacklistedDbsConfig)) {
      blacklistedDbs.add(db.toLowerCase());
      if (logger != null) logger.info("Blacklist db: " + db);
    }
    return blacklistedDbs;
  }

  /**
   * Parse blacklisted tables from configs string. Pass Logger if logging is necessary.
   */
  public static Set<TableName> parseBlacklistedTables(String blacklistedTablesConfig,
      Logger logger) {
    Preconditions.checkNotNull(blacklistedTablesConfig);
    Set<TableName> blacklistedTables = Sets.newHashSet();
    for (String tblName: Splitter.on(',').trimResults().omitEmptyStrings().split(
        blacklistedTablesConfig)) {
      TableName tbl = TableName.parse(tblName);
      if (tbl == null) {
        if (logger != null) {
          logger.warn(String.format("Illegal blacklisted table name: '%s'",
              tblName));
        }
        continue;
      }
      blacklistedTables.add(tbl);
      if (logger != null) logger.info("Blacklist table: " + tbl);
    }
    return blacklistedTables;
  }

  public static void verifyDbName(String dbName) throws AnalysisException {
    if (BackendConfig.INSTANCE.enableWorkloadMgmt() && dbName.equalsIgnoreCase(Db.SYS)) {
      // Override system DB for Impala system tables.
      return;
    }
    if (BLACKLISTED_DBS.contains(dbName)) {
      throw new AnalysisException("Invalid db name: " + dbName
          + ". It has been blacklisted using --blacklisted_dbs");
    }
  }

  public static void verifyTableName(TableName table) throws AnalysisException {
    if (BLACKLISTED_TABLES.contains(table)) {
      throw new AnalysisException("Invalid table/view name: " + table
          + ". It has been blacklisted using --blacklisted_tables");
    }
  }
}
