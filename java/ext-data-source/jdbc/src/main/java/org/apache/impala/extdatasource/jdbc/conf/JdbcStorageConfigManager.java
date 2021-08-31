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

package org.apache.impala.extdatasource.jdbc.conf;


import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main configuration handler class
 */
public class JdbcStorageConfigManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(JdbcStorageConfigManager.class);

  public static Configuration convertMapToConfiguration(Map<String, String> props) {
    checkRequiredPropertiesAreDefined(props);
    Configuration conf = new Configuration();

    for (Entry<String, String> entry : props.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    return conf;
  }

  private static void checkRequiredPropertiesAreDefined(Map<String, String> props) {

    try {
      String dbTypeName = props.get(JdbcStorageConfig.DATABASE_TYPE.getPropertyName());
      DatabaseType.valueOf(dbTypeName);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unknown database type.", e);
    }
    // Check the required parameters
    for (JdbcStorageConfig config : JdbcStorageConfig.values()) {
      if (config.isRequired() && !props.containsKey(config.getPropertyName())) {
        throw new IllegalArgumentException(String.format("Required config '%s' was not "
            + "present!", config.getPropertyName()));
      }
    }
  }

  public static String getConfigValue(JdbcStorageConfig key, Configuration config) {
    return config.get(key.getPropertyName());
  }

  public static String getOrigQueryToExecute(Configuration config) {
    String query;
    String tableName = config.get(JdbcStorageConfig.TABLE.getPropertyName());
    if (tableName != null) {
      // We generate query as 'select * from tbl'
      query = "select * from " + tableName;
    } else {
      query = config.get(JdbcStorageConfig.QUERY.getPropertyName());
    }

    return query;
  }

  public static String getQueryToExecute(Configuration config) {
    String query = config.get(JdbcStorageConfig.QUERY.getPropertyName());
    if (query != null) {
      // Query has been defined, return it
      return query;
    }

    // We generate query as 'select * from tbl'
    String tableName = config.get(JdbcStorageConfig.TABLE.getPropertyName());
    query = "select * from " + tableName;

    return query;
  }

  private static boolean isEmptyString(String value) {
    return ((value == null) || (value.trim().isEmpty()));
  }
}
