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

package org.apache.impala.catalog.events;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.EventProcessorStatus;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.impala.util.RandomHiveQueryRunner;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stress test for EventsProcessor generates a lot of events by running many Hive
 * queries in parallel. At the same time it creates impala clients which continuously
 * keeps refreshing the tables in the test database. While events are being generated,
 * the event processor is triggered at random intervals to simulate a variable sized
 * event batches. As of July 2019 this test generates about 2110 events on Hive-2 with
 * the default number of Clients and number of queries per client. The defaults can be
 * overridden by providing system properties for "numClients" and "numQueriesPerClient"
 * to the test
 */
public class EventsProcessorStressTest {

  // use a fixed seed value to make the test repeatable
  private static final Random random = new Random(117);

  private static CatalogServiceTestCatalog catalog_;
  private static MetastoreEventsProcessor eventsProcessor_;
  private static final String testDbPrefix_ = "events_stress_db_";
  private static final String testTblPrefix_ = "stress_test_tbl_";
  // number of concurrent hive and impala clients
  private static final int numClients_;
  // total number of random queries per client
  private static final int numQueriesPerClient_;
  private ExecutorService impalaRefreshExecutorService_;
  private static final Logger LOG =
      LoggerFactory.getLogger(EventsProcessorStressTest.class);

  static {
    Pair<Integer, Integer> configs = getConcurrencyConfigs();
    numClients_ = configs.first;
    numQueriesPerClient_ = configs.second;
  }

  /**
   * Gets the concurrency factor (number of clients nad number of queries per client) for
   * the test
   */
  private static Pair<Integer, Integer> getConcurrencyConfigs() {
    int numClients = 4;
    if (System.getProperty("numClients") != null) {
      numClients = Integer.parseInt(System.getProperty("numClients"));
    } else if (MetastoreShim.getMajorVersion() >= 3) {
      // in CDP environment, we run hive queries on Tez using Yarn applications
      // currently there are limitations due to which we can only run one session at time
      // without affecting the performance
      numClients = 1;
    }
    int numQueriesPerClient = 50;
    if (System.getProperty("numQueriesPerClient") != null) {
      numQueriesPerClient = Integer.parseInt(System.getProperty("numQueriesPerClient"));
    } else if (MetastoreShim.getMajorVersion() >= 3) {
      // in CDP we use only 1 client, so increase the number of queries to have
      // reasonable number of events generated
      numQueriesPerClient = 200;
    }
    return new Pair<>(numClients, numQueriesPerClient);
  }

  /**
   * Initialize the test catalog and start a synchronous events processor
   */
  @BeforeClass
  public static void setupTestEnv() throws Exception {
    catalog_ = CatalogServiceTestCatalog.create();
    CatalogOpExecutor catalogOpExecutor = catalog_.getCatalogOpExecutor();
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      CurrentNotificationEventId currentNotificationId =
          metaStoreClient.getHiveClient().getCurrentNotificationEventId();
      eventsProcessor_ = new SynchronousHMSEventProcessorForTests(
          catalogOpExecutor, currentNotificationId.getEventId(), 10L);
      eventsProcessor_.start();
    }
    catalog_.setMetastoreEventProcessor(eventsProcessor_);
  }

  /**
   * Cleans up the test databases and shuts down the event processor
   */
  @AfterClass
  public static void destroyTestEnv() {
    try {
      for (int i = 0; i < numClients_; i++) {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
          msClient.getHiveClient().dropDatabase(testDbPrefix_ + i, true, true, true);
        }
        // remove database from catalog as well to clean up catalog state
        catalog_.removeDb(testDbPrefix_ + i);
      }
    } catch (Exception ex) {
      // ignored
    } finally {
      if (eventsProcessor_ != null) {
        eventsProcessor_.shutdown();
      }
    }
  }

  /**
   * Creates Impala clients which issue reload on table in the test data base in a loop to
   * simulate a actual load in a real system. Having the tables loaded is important so
   * that events processor is doing some real work rather than just invalidating
   * incomplete tables.
   */
  private void startImpalaRefreshClients() {
    impalaRefreshExecutorService_ = Executors.newFixedThreadPool(numClients_,
        new ThreadFactoryBuilder().setNameFormat("impala-refresh-client-%d")
            .setDaemon(true).build());
    for (int i = 0; i < numClients_; i++) {
      impalaRefreshExecutorService_.submit((Runnable) () -> {
        final int clientId =
            Integer.parseInt(Thread.currentThread().getName().substring(
                "impala-refresh-client-".length()));
        final String dbName = testDbPrefix_ + clientId;
        while (true) {
          try {
            List<String> tablenames = catalog_.getTableNames(dbName,
                PatternMatcher.MATCHER_MATCH_ALL);
            for (String tbl : tablenames) {
              catalog_.reloadTable(catalog_.getTable(dbName, tbl), "test refresh "
                  + "operation for events stress test",
                  NoOpEventSequence.INSTANCE);
            }
            // wait for a random duration between 0 and 3 seconds. We want the refresh
            // clients to aggressive than the event polling threads
            Thread.sleep(random.nextInt(3000));
          } catch (CatalogException | InterruptedException e) {
            // ignore exceptions since it is possible that hive has dropped the
            // database or table
          }
        }
      });
    }
  }

  /**
   * Stop the impala refresh clients
   */
  private void stopImpalaRefreshClients() {
    impalaRefreshExecutorService_.shutdownNow();
  }

  /**
   * Gets the current notification event id
   */
  private long getCurrentNotificationId() throws TException {
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      return metaStoreClient.getHiveClient().getCurrentNotificationEventId().getEventId();
    }
  }

  @Test
  public void testUsingRandomHiveQueries() throws Exception {
    LOG.info("Using number of clients: {} number of queries per client: {}", numClients_,
        numQueriesPerClient_);
    final RandomHiveQueryRunner queryRunner = new RandomHiveQueryRunner(random,
        testDbPrefix_, testTblPrefix_, numClients_, numQueriesPerClient_, null);
    long eventIdBefore = getCurrentNotificationId();
    queryRunner.start();
    startImpalaRefreshClients();
    try {
      while (!queryRunner.isTerminated()) {
        // randomly wait between 0 and 10 seconds
        Thread.sleep(random.nextInt(10000));
        eventsProcessor_.processEvents();
        // make sure that events processor is in ACTIVE state after every batch which
        // is processed
        Assert.assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      }
      queryRunner.checkForErrors();
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      Assert.fail(unwrapCause(ex));
    } finally {
      stopImpalaRefreshClients();
      queryRunner.shutdownNow();
      LOG.info("Total number of events generated {}",
          getCurrentNotificationId() - eventIdBefore);
    }
  }

  /**
   * Unwraps the exception cause from the trace
   */
  private String unwrapCause(Throwable ex) {
    String cause = ex.getMessage();
    while (ex.getCause() != null) {
      cause = ex.getCause().getMessage();
      ex = ex.getCause();
    }
    return cause;
  }
}
