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

package org.apache.impala.extdatasource.jdbc.dao;

/**
 * Postgres specific data accessor. Postgres JDBC drivers do not support generic LIMIT and
 * OFFSET escape functions
 */
public class PostgresDatabaseAccessor extends GenericJdbcDatabaseAccessor {

  @Override
  public String getCaseSensitiveName(String name) {
    if (!name.isEmpty() && name.charAt(0) != '\"') {
      StringBuilder sb = new StringBuilder("\"");
      sb.append(name);
      sb.append("\"");
      return sb.toString();
    } else {
      return name;
    }
  }

  @Override
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else {
      if (limit == -1) {
        return sql;
      }
      return sql + " LIMIT " + limit + " OFFSET " + offset;
    }
  }

  @Override
  protected String addLimitToQuery(String sql, int limit) {
    if (limit == -1) {
      return sql;
    }
    return sql + " LIMIT " + limit;
  }

  @Override
  protected boolean isAdditionalPropertiesSupported() {
    return true;
  }

  @Override
  protected String getPropertiesDelimiter(boolean precededDelimiter) {
    /* Additional properties can be set in optional, ampersand-separated key/value string
     * in JDBC connection string in following format:
     *     jdbc:postgresql://[host][:port]/[database][?propertyName1=propertyValue1]
     *     [&propertyName2=propertyValue2]...
     */
    return precededDelimiter ? "?" : "&";
  }
}
