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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfig;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfigManager;
import org.apache.impala.extdatasource.jdbc.exception.JdbcDatabaseAccessException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TCacheJarResult;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 */
public class GenericJdbcDatabaseAccessor implements DatabaseAccessor {

  protected static final Logger LOG = LoggerFactory
      .getLogger(GenericJdbcDatabaseAccessor.class);

  protected static final String DBCP_CONFIG_PREFIX = "dbcp";
  protected static final int DEFAULT_FETCH_SIZE = 1000;
  protected static final long MILLI_SECONDS_PER_DAY = 86400000;

  protected DataSource dbcpDataSource = null;
  protected String dataSourceCacheKey = null;

  // Cache datasource for sharing
  private static final DataSourceObjectCache dataSourceCache =
      new DataSourceObjectCache();

  @Override
  public long getTotalNumberOfRecords(Configuration conf)
      throws JdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseSource(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      // TODO: If a target database cannot flatten this view query, try to text
      // replace the generated "select *".
      String countQuery = "SELECT COUNT(*) FROM (" + sql + ") tmptable";
      LOG.info("Query to execute is [{}]", countQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(countQuery);
      rs = ps.executeQuery();
      if (rs.next()) {
        return rs.getLong(1);
      } else {
        LOG.warn("The count query '{}' did not return any results.", countQuery);
        throw new JdbcDatabaseAccessException(
            "Count query did not return any results.");
      }
    } catch (JdbcDatabaseAccessException he) {
      throw he;
    } catch (Exception e) {
      LOG.error("Caught exception while trying to get the number of records", e);
      throw new JdbcDatabaseAccessException(e);
    } finally {
      cleanupResources(conn, ps, rs);
    }
  }


  @Override
  public JdbcRecordIterator getRecordIterator(Configuration conf, int limit, int offset)
      throws JdbcDatabaseAccessException {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseSource(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String partitionQuery = addLimitAndOffsetToQuery(sql, limit, offset);

      LOG.info("Query to execute is [{}]", partitionQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(partitionQuery, ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);
      ps.setFetchSize(getFetchSize(conf));
      rs = ps.executeQuery();

      return new JdbcRecordIterator(conn, ps, rs, conf);
    } catch (Exception e) {
      LOG.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new JdbcDatabaseAccessException(
          "Caught exception while trying to execute query:" + e.getMessage(), e);
    }
  }


  @Override
  public void close(Connection connToBeClosed, boolean cleanDbcpDSCache) {
    if (connToBeClosed != null) {
      Preconditions.checkNotNull(dbcpDataSource);
      invalidateConnection(connToBeClosed);
    }
    Preconditions.checkNotNull(dataSourceCache);
    dataSourceCache.remove(dataSourceCacheKey, cleanDbcpDSCache);
    dbcpDataSource = null;
  }

  @Override
  public String getCaseSensitiveName(String name) {
    return name;
  }

  protected boolean isAdditionalPropertiesSupported() {
    return false;
  }

  protected String getPropertiesDelimiter(boolean precededDelimiter) {
    return null;
  }

  protected String getAdditionalProperties(String configProperties) {
    if (Strings.isNullOrEmpty(configProperties)) return null;
    String delimiter = getPropertiesDelimiter(/* precededDelimiter */ false);
    Preconditions.checkState(!Strings.isNullOrEmpty(delimiter));
    // Extract valid query options.
    Pattern pattern = Pattern.compile("(\\w*\\s*)=(\\s*\"[^\"]*\"|[^,]*)");
    Matcher matcher = pattern.matcher(configProperties);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      Preconditions.checkState(!Strings.isNullOrEmpty(matcher.group(1)));
      if (Strings.isNullOrEmpty(matcher.group(2))) {
        LOG.info("Ignore invalid query option '{}'", matcher.group(1));
        continue;
      }
      if (sb.length() > 0) sb.append(delimiter);
      sb.append(matcher.group(1).trim());
      sb.append("=");
      sb.append(matcher.group(2).trim());
    }
    return sb.toString();
  }

  /**
   * This function converts the date represented in epoch days to
   * a string format of "yyyy-MM-dd"
  */
  @Override
  public String getDateString(int dateVal) {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateToString = formatter.format(new Date(((long)dateVal) *
        MILLI_SECONDS_PER_DAY));
    return dateToString;
  }

  /**
   * Uses generic JDBC escape functions to add a limit and offset clause to a query
   * string
   *
   * @param sql
   * @param limit
   * @param offset
   * @return
   */
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else if (limit != -1) {
      return sql + " {LIMIT " + limit + " OFFSET " + offset + "}";
    } else {
      return sql + " {OFFSET " + offset + "}";
    }
  }

  /*
   * Uses generic JDBC escape functions to add a limit clause to a query string
   */
  protected String addLimitToQuery(String sql, int limit) {
    if (limit == -1) {
      return sql;
    }
    return sql + " {LIMIT " + limit + "}";
  }

  protected void cleanupResources(Connection conn, PreparedStatement ps, ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      LOG.warn("Caught exception during resultset cleanup.", e);
    }

    try {
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      LOG.warn("Caught exception during statement cleanup.", e);
    }

    if (conn != null) {
      Preconditions.checkNotNull(dbcpDataSource);
      // Call BasicDataSource.invalidateConnection() to close a connection since this API
      // is more effective than Connection.close().
      invalidateConnection(conn);
    }
  }

  /*
   * Manually invalidates a connection, effectively requesting the pool to try to close
   * it, and reclaim pool capacity.
   */
  private void invalidateConnection(Connection conn) {
    try {
      Preconditions.checkState(dbcpDataSource instanceof BasicDataSource);
      BasicDataSource basicDataSource = (BasicDataSource) dbcpDataSource;
      basicDataSource.invalidateConnection(conn);
    } catch (Exception e) {
      LOG.warn("Caught exception during connection cleanup.", e);
    }
  }

  protected void initializeDatabaseSource(Configuration conf)
      throws JdbcDatabaseAccessException {
    if (dbcpDataSource == null) {
      synchronized (this) {
        if (dbcpDataSource == null) {
          Properties props = getConnectionPoolProperties(conf);
          String jdbcUrl = props.getProperty("url");
          String username = props.getProperty("username", "-");
          dataSourceCacheKey = String.format("%s.%s", jdbcUrl, username);
          Preconditions.checkNotNull(dataSourceCache);
          dbcpDataSource = dataSourceCache.get(dataSourceCacheKey, props);
        }
      }
    }
  }

  protected Properties getConnectionPoolProperties(Configuration conf) {
    // Create the default properties object
    Properties dbProperties = getDefaultDBCPProperties();

    // user properties
    Map<String, String> userProperties = conf.getValByRegex(DBCP_CONFIG_PREFIX + "\\.*");
    if ((userProperties != null) && (!userProperties.isEmpty())) {
      for (Entry<String, String> entry : userProperties.entrySet()) {
        dbProperties.put(entry.getKey().replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""),
            entry.getValue());
      }
    }

    String passwd = JdbcStorageConfigManager.getPasswordFromProperties(conf);
    if (passwd != null) {
      dbProperties.put(JdbcStorageConfig.DBCP_PASSWORD.getPropertyName().replaceFirst(
          DBCP_CONFIG_PREFIX + "\\.", ""), passwd);
    }

    // essential properties
    String jdbcUrl = conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName());
    boolean precededDelimiter = true;
    String jdbcAuth = conf.get(JdbcStorageConfig.JDBC_AUTH.getPropertyName());
    if (!Strings.isNullOrEmpty(jdbcAuth)) {
      jdbcUrl += getPropertiesDelimiter(precededDelimiter) + jdbcAuth;
      precededDelimiter = false;
    }
    if (isAdditionalPropertiesSupported()) {
      String additionalProperties = getAdditionalProperties(
          conf.get(JdbcStorageConfig.JDBC_OPTIONS.getPropertyName()));
      if (!Strings.isNullOrEmpty(additionalProperties)) {
        jdbcUrl += getPropertiesDelimiter(precededDelimiter) + additionalProperties;
        if (precededDelimiter) precededDelimiter = false;
      }
    }
    LOG.trace("JDBC URL: {}", jdbcUrl);
    dbProperties.put("url", jdbcUrl);
    dbProperties.put("driverClassName",
        conf.get(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName()));
    dbProperties.put("driverUrl",
        conf.get(JdbcStorageConfig.JDBC_DRIVER_URL.getPropertyName()));
    dbProperties.put("type", "javax.sql.DataSource");
    return dbProperties;
  }

  protected Properties getDefaultDBCPProperties() {
    Properties props = new Properties();
    // Don't set 'initialSize', otherwise the driver class will be loaded in
    // BasicDataSourceFactory.createDataSource() before the class loader is set
    // by calling BasicDataSource.setDriverClassLoader.
    // props.put("initialSize", "1");
    // 'maxActive' and 'maxWait' properties are renamed as 'maxTotal' and 'maxWaitMillis'
    // respectively in org.apache.commons.dbcp2.
    props.put("maxTotal",
        String.valueOf(BackendConfig.INSTANCE.getDbcpMaxConnPoolSize()));
    props.put("maxIdle",
        String.valueOf(BackendConfig.INSTANCE.getDbcpMaxConnPoolSize()));
    props.put("minIdle", "0");
    props.put("maxWaitMillis",
        String.valueOf(BackendConfig.INSTANCE.getDbcpMaxWaitMillisForConn()));
    return props;
  }

  protected int getFetchSize(Configuration conf) {
    return conf
        .getInt(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
  }

}
