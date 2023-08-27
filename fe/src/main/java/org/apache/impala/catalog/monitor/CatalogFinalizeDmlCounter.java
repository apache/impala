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

package org.apache.impala.catalog.monitor;

import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUpdateCatalogRequest;

import java.util.HashMap;
import java.util.Optional;

/**
 * Extends the CatalogOperationCounter to count the finalize DML requests.
 * These DML requests are called when the query is finished to finalize the
 * metadata change.
 */
public class CatalogFinalizeDmlCounter extends CatalogOperationCounter {
  /**
   * Holds the possible DML types that can trigger finalize DML request.
   */
  enum FinalizeDmlType {
    FINALIZE_INSERT_INTO,
    FINALIZE_UPDATE,
    FINALIZE_CREATE_TABLE_AS_SELECT,
    FINALIZE_DML;
  }

  /**
   * Initialize the super.counters_ with the FinalizeDmlType enum values.
   */
  public CatalogFinalizeDmlCounter() {
    this.counters_ = new HashMap<>();
    for (FinalizeDmlType dmlType : FinalizeDmlType.values()) {
      counters_.put(dmlType.toString(), new HashMap<>());
    }
  }

  public void incrementOperation(TUpdateCatalogRequest req) {
    Optional<TTableName> tTableName =
        Optional.of(new TTableName(req.db_name, req.target_table));
    FinalizeDmlType dmlType = getDmlType(req.getHeader().redacted_sql_stmt);
    incrementCounter(dmlType.toString(), getTableName(tTableName));
  }

  public void decrementOperation(TUpdateCatalogRequest req) {
    Optional<TTableName> tTableName =
        Optional.of(new TTableName(req.db_name, req.target_table));
    FinalizeDmlType dmlType = getDmlType(req.getHeader().redacted_sql_stmt);
    decrementCounter(dmlType.toString(), getTableName(tTableName));
  }

  /**
   * The DML type of the query is stored as class types and it is not exposed
   * after analysis. Therefore, we need to guess it if the SQL statement is
   * available.
   */
  public static FinalizeDmlType getDmlType(String sql_stmt) {
    sql_stmt = sql_stmt.toUpperCase();
    if (sql_stmt.contains("INSERT INTO")) {
      return FinalizeDmlType.FINALIZE_INSERT_INTO;
    } else if (sql_stmt.contains("CREATE TABLE")) {
      return FinalizeDmlType.FINALIZE_CREATE_TABLE_AS_SELECT;
    } else if (sql_stmt.contains("UPDATE")) {
      return FinalizeDmlType.FINALIZE_UPDATE;
    }
    return FinalizeDmlType.FINALIZE_DML;
  }
}