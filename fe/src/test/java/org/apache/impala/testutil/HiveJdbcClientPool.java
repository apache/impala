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

package org.apache.impala.testutil;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple connection pooling implementation which opens a given number of connections
 * to HS2 and reuses the connections to submit the queries. It exposes a autocloseable
 * HiveJdbcClient class which can be use to submit hive queries. When the close is
 * called on the HiveJdbcClient it releases the connections back to the pool so that it
 * can reused by another query
 */
public class HiveJdbcClientPool implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaJdbcClient.class);
  private static AtomicInteger clientIdGenerator = new AtomicInteger(0);

  private final static String HIVE_SERVER2_DRIVER_NAME =
      "org.apache.hive.jdbc.HiveDriver";
  private final int poolSize_;
  private final BlockingQueue<HiveJdbcClient> freeClients_;
  private final long timeoutInSeconds_;
  private final static int DEFAULT_PORT_NUMBER = 11050;

  public class HiveJdbcClient implements AutoCloseable {

    private final Connection conn_;
    private Statement stmt_;
    private final int clientId;

    private HiveJdbcClient(String connString) throws SQLException {
      conn_ = DriverManager.getConnection(connString);
      stmt_ = conn_.createStatement();
      clientId = clientIdGenerator.getAndIncrement();
    }

    public int getClientId() { return clientId; }

    @Override
    public void close() throws SQLException {
      if (stmt_ != null) {
        stmt_.close();
      }
      freeClients_.add(this);
    }

    private void validateConnection() throws SQLException {
      Preconditions.checkNotNull(conn_,"Connection not initialized.");
      Preconditions.checkState(!conn_.isClosed(), "Connection is not open");
      Preconditions.checkNotNull(stmt_);

      // Re-open if the statement if it has been closed.
      if (stmt_.isClosed()) {
        stmt_ = conn_.createStatement();
      }
    }

    /*
     * Executes the given query and returns the ResultSet. Will re-open the Statement
     * if needed.
     */
    public ResultSet execQuery(String query) throws SQLException {
      validateConnection();
      LOG.info("Executing: " + query);
      return stmt_.executeQuery(query);
    }

    /**
     * Executes a given query and returns true if the query is successful
     */
    public boolean executeSql(String sql) throws SQLException {
      validateConnection();
      LOG.info("Executing sql : " + sql);
      return stmt_.execute(sql);
    }
  }

  public HiveJdbcClient getClient() throws TimeoutException, InterruptedException {
    try {
      HiveJdbcClient client = freeClients_.poll(timeoutInSeconds_, TimeUnit.SECONDS);
      if (client == null) {
        throw new TimeoutException("Timed out while waiting to get a "
            + "new client. Consider increasing the pool size");
      }
      return client;
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting to a Hive JDBC client", e);
      throw e;
    }
  }

  private HiveJdbcClientPool(int poolsize, long timeoutInSeconds)
      throws ClassNotFoundException, SQLException {
    Preconditions.checkArgument(poolsize > 0);
    this.poolSize_ = poolsize;
    this.timeoutInSeconds_ = timeoutInSeconds;
    this.freeClients_ = new LinkedBlockingQueue<>(poolsize);
    LOG.info("Using JDBC Driver Name: " + HIVE_SERVER2_DRIVER_NAME);
    // Make sure the driver can be found, throws a ClassNotFoundException if
    // it is not available.
    Class.forName(HIVE_SERVER2_DRIVER_NAME);
    String connString = String.format(TestUtils.HS2_CONNECTION_TEMPLATE,
        DEFAULT_PORT_NUMBER,
        "default");
    LOG.info("Using connection string: " + connString);
    for (int i = 0; i < poolSize_; i++) {
      freeClients_.add(new HiveJdbcClient(connString));
    }
  }

  public static synchronized HiveJdbcClientPool create(int poolSize)
      throws SQLException, ClassNotFoundException {
    return new HiveJdbcClientPool(poolSize, 5 * 60);
  }

  /*
   * Closes the internal Statement and Connection objects. If they are already closed
   * this is a no-op.
   */
  @Override
  public void close() {
    int closedCount = poolSize_;
    while (closedCount > 0) {
      try {
        HiveJdbcClient client = freeClients_.poll(5 * 60, TimeUnit.SECONDS);
        if (client != null) {
          if (client.stmt_ != null) { client.stmt_.close(); }
          if (client.conn_ != null) { client.conn_.close(); }
        }
        closedCount--;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
