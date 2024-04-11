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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.hadoop.fs.Path;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.extdatasource.jdbc.exception.JdbcDatabaseAccessException;
import org.apache.impala.service.BackendConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Thread safe cache for storing SQL DataSource object with reference count.
 */
public class DataSourceObjectCache {

  private static final Logger LOG = LoggerFactory.getLogger(DataSourceObjectCache.class);
  private static final long CLEANUP_INTERVAL_MS = 10000;
  private static final long DEFAULT_IDLE_DATA_SOURCE_TIMEOUT_MS = 300000;

  /**
   * Map entry object with reference count.
   */
  class Entry {
    private DataSource dbcpDataSource_;
    private String driverFilePath_;
    private int referenceCount_;
    private long idleTimeStamp_;

    public Entry(DataSource dataSource, String driverFilePath) {
      dbcpDataSource_ = dataSource;
      driverFilePath_ = driverFilePath;
      referenceCount_ = 1;
      idleTimeStamp_ = 0;
    }
    public DataSource getDataSource() { return dbcpDataSource_; }
    public int getReference() { return referenceCount_; }
    public String getDriverFilePath() { return driverFilePath_; }
    public void incrementReference() { ++referenceCount_; }
    public int decrementReference() { return --referenceCount_; }
    public long getIdleTimeStamp() { return idleTimeStamp_; }
    public void setIdleTimeStamp(long timeStamp) { idleTimeStamp_ = timeStamp; }
  }

  private final Map<String, Entry> cacheMap_ = new HashMap<>();
  private Thread cleanupThread_;

  public DataSourceObjectCache() {
    cleanupThread_ = new Thread(new Runnable() {
      @Override
      public void run() {
        cleanup();
      }
    });
    cleanupThread_.setDaemon(true);
    cleanupThread_.setName("DataSourceObjectCache daemon thread");
    cleanupThread_.start();
  }

  /*
   * Return DataSource for the given cache-key. The cache-key is generated as
   * jdbc-url + username, like 'jdbc:impala://10.96.132.138:21050/tpcds.impala_user'.
   */
  public DataSource get(String cacheKey, Properties props)
      throws JdbcDatabaseAccessException {
    synchronized (this) {
      Entry entry = cacheMap_.get(cacheKey);
      if (entry != null) {
        entry.incrementReference();
        return entry.getDataSource();
      }

      LOG.info("Datasource for '{}' was not cached. Loading now.", cacheKey);
      String driverUrl = props.getProperty("driverUrl");
      try {
        BasicDataSource dbcpDs = BasicDataSourceFactory.createDataSource(props);
        // Copy jdbc driver to local file system.
        String driverLocalPath = FileSystemUtil.copyFileFromUriToLocal(driverUrl);
        // Create class loader for jdbc driver and set it for the
        // BasicDataSource object so that the driver class could be loaded
        // from jar file without searching classpath.
        URL driverJarUrl = new File(driverLocalPath).toURI().toURL();
        URLClassLoader driverLoader = URLClassLoader.newInstance(
            new URL[] { driverJarUrl }, getClass().getClassLoader());
        dbcpDs.setDriverClassLoader(driverLoader);
        entry = new Entry(dbcpDs, driverLocalPath);
        cacheMap_.put(cacheKey, entry);
        return dbcpDs;
      } catch (Exception e) {
        throw new JdbcDatabaseAccessException(String.format(
            "Unable to fetch jdbc driver jar from location '%s'. ", driverUrl));
      }
    }
  }

  /*
   * Release DataSource object for the given cache-key.
   */
  public void remove(String cacheKey, boolean cleanDbcpDSCache) {
    if (Strings.isNullOrEmpty(cacheKey)) return;
    Entry entry = null;
    synchronized (this) {
      entry = cacheMap_.get(cacheKey);
      if (entry == null) return;
      Preconditions.checkState(entry.getReference() > 0);
      entry.decrementReference();
      if (entry.getReference() > 0) {
        return;
      } else if (!cleanDbcpDSCache) {
        entry.setIdleTimeStamp(System.currentTimeMillis());
        return;
      }
      // Remove SQL DataSource from cache when reference count reaches 0
      // and cleanDbcpDSCache is set as true.
      cacheMap_.remove(cacheKey);
    }
    closeDataSource(cacheKey, entry);
  }

  /*
   * This function is running on a working thread in the coordinator daemon.
   * It cleans up idle DataSource objects in 10 seconds interval.
   */
  private void cleanup() {
    long idleTimeoutInMS =
        BackendConfig.INSTANCE.getDbcpDataSourceIdleTimeoutInSeconds() * 1000;
    if (idleTimeoutInMS < 0) {
      idleTimeoutInMS = DEFAULT_IDLE_DATA_SOURCE_TIMEOUT_MS;
    }
    while (true) {
      try {
        Thread.sleep((long) CLEANUP_INTERVAL_MS);
      } catch (InterruptedException e) {
        LOG.info("Thread.sleep failed, ", e);
      }
      long currTime = System.currentTimeMillis();
      Map<String, Entry> entriesToBeClosed = new HashMap<>();
      synchronized (this) {
        Iterator<Map.Entry<String, Entry>> iter = cacheMap_.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<String, Entry> mapEntry = iter.next();
          String cacheKey = mapEntry.getKey();
          Entry entry = mapEntry.getValue();
          if (entry.getReference() <= 0 &&
              currTime - entry.getIdleTimeStamp() > idleTimeoutInMS) {
            iter.remove();
            entriesToBeClosed.put(cacheKey, entry);
          }
        }
      }
      for (Map.Entry<String, Entry> mapEntry : entriesToBeClosed.entrySet()) {
        closeDataSource(mapEntry.getKey(), mapEntry.getValue());
      }
      entriesToBeClosed.clear();
    }
  }

  /*
   * Close DataSource object referenced by the given entry.
   */
  private void closeDataSource(String cacheKey, Entry entry) {
    if (Strings.isNullOrEmpty(cacheKey) || entry == null) return;
    DataSource ds = entry.getDataSource();
    String driverFilePath = entry.getDriverFilePath();
    Preconditions.checkState(ds instanceof BasicDataSource);
    BasicDataSource dbcpDs = (BasicDataSource) ds;
    try {
      dbcpDs.close();
      LOG.info("Close datasource for '{}'.", cacheKey);
      if (driverFilePath != null) {
        // Delete the jar file of jdbc driver from local file system.
        Path localJarPath = new Path("file://" + driverFilePath);
        FileSystemUtil.deleteIfExists(localJarPath);
      }
    } catch (SQLException e) {
      LOG.warn("Caught exception during datasource cleanup.", e);
    }
  }
}
