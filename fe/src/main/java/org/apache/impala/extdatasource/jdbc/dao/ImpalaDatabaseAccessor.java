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
 * Impala specific data accessor. This is needed because Impala JDBC drivers do not
 * support generic LIMIT and OFFSET escape functions
 */
public class ImpalaDatabaseAccessor extends GenericJdbcDatabaseAccessor {

  @Override
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else {
      if (limit != -1) {
        return sql + " LIMIT " + limit + " OFFSET " + offset;
      } else {
        return sql;
      }
    }
  }

  @Override
  protected String addLimitToQuery(String sql, int limit) {
    if (limit != -1) {
      return sql + " LIMIT " + limit;
    } else {
      return sql;
    }
  }

  @Override
  protected boolean isAdditionalPropertiesSupported() {
    return true;
  }

  @Override
  protected String getPropertiesDelimiter(boolean precededDelimiter) {
    /* Query options can be set in optional, semicolon-separated key/value string
     * in JDBC connection string in following format:
     *     jdbc:impala://[host][:port]/[database][;optionName1=optionValue1]
     *     [;optionName2=optionValue2]...
     */
    return ";";
  }
}
