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


import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.impala.common.InternalException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfig;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfigManager;
import org.apache.impala.extdatasource.jdbc.exception.JdbcDatabaseAccessException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TCacheJarResult;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 */
public class GenericJdbcDatabaseAccessor implements DatabaseAccessor {

  protected static final Logger LOG = LoggerFactory
      .getLogger(GenericJdbcDatabaseAccessor.class);

  protected static final String DBCP_CONFIG_PREFIX = "dbcp";
  protected static final int DEFAULT_FETCH_SIZE = 1000;
  protected static final int CACHE_EXPIRE_TIMEOUT_S = 1800;
  protected static final int CACHE_SIZE = 100;

  protected DataSource dbcpDataSource = null;
  // Cache datasource for sharing
  public static Cache<String, DataSource> dataSourceCache = CacheBuilder
      .newBuilder()
      .removalListener((RemovalListener<String, DataSource>) notification -> {
        DataSource ds = notification.getValue();
        if (ds instanceof BasicDataSource) {
          BasicDataSource dbcpDs = (BasicDataSource) ds;
          try {
            dbcpDs.close();
            LOG.info("Close datasource for '{}'.", notification.getKey());
          } catch (SQLException e) {
            LOG.warn("Caught exception during datasource cleanup.", e);
          }
        }
      })
      .expireAfterAccess(CACHE_EXPIRE_TIMEOUT_S, TimeUnit.SECONDS)
      .maximumSize(CACHE_SIZE)
      .build();

  @Override
  public int getTotalNumberOfRecords(Configuration conf)
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
        return rs.getInt(1);
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
  public void close(boolean cleanCache) {
    dbcpDataSource = null;
    if (cleanCache && dataSourceCache != null) {
      dataSourceCache.invalidateAll();
      dataSourceCache = null;
    }
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

    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      LOG.warn("Caught exception during connection cleanup.", e);
    }
  }

  protected void initializeDatabaseSource(Configuration conf)
      throws ExecutionException {
    if (dbcpDataSource == null) {
      synchronized (this) {
        if (dbcpDataSource == null) {
          Properties props = getConnectionPoolProperties(conf);
          String jdbcUrl = props.getProperty("url");
          String username = props.getProperty("username", "-");
          String cacheMapKey = String.format("%s.%s", jdbcUrl, username);
          dbcpDataSource = dataSourceCache.get(cacheMapKey,
              () -> {
                LOG.info("Datasource for '{}' was not cached. "
                    + "Loading now.", cacheMapKey);
                BasicDataSource basicDataSource =
                    BasicDataSourceFactory.createDataSource(props);
                // Put jdbc driver to cache
                String driverUrl = props.getProperty("driverUrl");
                String localPath = BackendConfig.INSTANCE.getBackendCfg().local_library_path;
                
                // TCacheJarResult cacheResult = FeSupport.CacheJar(driverUrl);
                String driverLocalPath = getJdbcDriverFromUri(driverUrl, localPath);
               // TStatus cacheJarStatus = cacheResult.getStatus();
              //  if (cacheJarStatus.getStatus_code() != TErrorCode.OK) {
                if (driverLocalPath == null) {
                  throw new JdbcDatabaseAccessException(String.format(
                      "Unable to fetch jdbc driver jar from location '%s'. ",
                      driverUrl));
                }
                // String driverLocalPath = cacheResult.getLocal_path();
                // Create class loader for jdbc driver and set it for the
                // BasicDataSource object so that the driver class could be loaded
                // from jar file without searching classpath.
                URL driverJarUrl = new File(driverLocalPath).toURI().toURL();
                URLClassLoader driverLoader =
                    URLClassLoader.newInstance( new URL[] { driverJarUrl },
                        getClass().getClassLoader());
                basicDataSource.setDriverClassLoader(driverLoader);
                // Delete the jar file once its loaded
                Path localJarPath = new Path(driverLocalPath);
                FileSystemUtil.deleteIfExists(localJarPath);
                return basicDataSource;
              });
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

    // essential properties
    dbProperties.put("url", conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()));
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
    props.put("maxActive", "3");
    props.put("maxIdle", "0");
    props.put("maxWait", "10000");
    props.put("timeBetweenEvictionRunsMillis", "30000");
    return props;
  }

  protected int getFetchSize(Configuration conf) {
    return conf
        .getInt(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
  }

  protected String getJdbcDriverFromUri(String driverUrl, String localLibPath)
 // protected TCacheJarResult createWithLocalPath(String localLibPath, Function fn)
      throws InternalException {
    Path localJarPath = null;
    String uri = driverUrl;
      String localJarPathString = null;
      if (uri != null) {
        localJarPath = new Path("file://" + localLibPath,
            UUID.randomUUID().toString() + ".jar");
        Preconditions.checkNotNull(localJarPath);
        try {
          FileSystemUtil.copyToLocal(new Path(uri), localJarPath);
        } catch (IOException e) {
          String errorMsg = "Couldn't copy " + uri + " to local path: " +
              localJarPath.toString();
          LOG.error(errorMsg, e);
          throw new InternalException(errorMsg);
        }
        localJarPathString = localJarPath.toString();
      }
   //   return new HiveUdfLoader(localJarPathString, fn.getClassName());
       return localJarPathString;
  }

}
