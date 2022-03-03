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

package org.apache.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbSetOwnerParams;
import org.apache.impala.thrift.TAlterDbType;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TOwnerType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test issues concurrent alter database operations to reproduce the race described in
 * IMPALA-8312
 */
public class AlterDatabaseTest {
  private static final String TEST_OWNER_1 = "user_1";
  private static final PrincipalType TEST_TYPE_1 = PrincipalType.USER;

  private static final String TEST_OWNER_2 = "user_2";
  private static final PrincipalType TEST_TYPE_2 = PrincipalType.ROLE;

  private static ImpaladTestCatalog catalog_;
  private static CatalogOpExecutor catalogOpExecutor_;
  private static final String TEST_ALTER_DB = "testAlterdb";
  // number of reader threads which query the test database
  private static final int NUM_READERS = 10;
  // number of writer threads which change the database. We only need one currently
  // since all the alterDatabase calls are serialized by metastoreDdlLock_ in
  // CatalogOpExecutor
  private static final int NUM_WRITERS = 1;

  // barrier to make sure that readers and writers start at the same time
  private final CyclicBarrier barrier_ =
      new CyclicBarrier(NUM_READERS + NUM_WRITERS);
  // toggle switch to change the database owner from user_1 to user_2 in each
  // consecutive alter database call
  private final AtomicBoolean toggler_ = new AtomicBoolean(false);

  /**
   * Sets up the test class by instantiating the catalog service
   * @throws ImpalaException
   */
  @BeforeClass
  public static void setUpTest() throws ImpalaException {
    CatalogServiceTestCatalog testSrcCatalog = CatalogServiceTestCatalog.create();
    catalog_ = new ImpaladTestCatalog(testSrcCatalog);
    catalogOpExecutor_ = testSrcCatalog.getCatalogOpExecutor();
  }

  /**
   * Clean-up the database once the test completes
   * @throws ImpalaException
   */
  @After
  public void cleanUp() throws ImpalaException {
    catalogOpExecutor_.execDdlRequest(dropDbRequest());
  }

  @Before
  public void setUpDatabase() throws ImpalaException {
    // cleanup and recreate any pre-existing testdb
    catalogOpExecutor_.execDdlRequest(dropDbRequest());
    catalogOpExecutor_.execDdlRequest(createDbRequest());
    Db db = catalog_.getDb(TEST_ALTER_DB);
    assertNotNull(db);
    catalogOpExecutor_.execDdlRequest(getNextDdlRequest());
    assertNotNull(catalog_.getDb(TEST_ALTER_DB));
    String owner = db.getMetaStoreDb().getOwnerName();
    assertTrue(owner.equals(TEST_OWNER_1) || owner.equals(TEST_OWNER_2));
  }

  /**
   * Drops the test db from the test catalog
   */
  private static TDdlExecRequest dropDbRequest() {
    TDdlExecRequest request = new TDdlExecRequest();
    request.setQuery_options(new TDdlQueryOptions());
    request.setDdl_type(TDdlType.DROP_DATABASE);
    TDropDbParams dropDbParams = new TDropDbParams();
    dropDbParams.setDb(TEST_ALTER_DB);
    dropDbParams.setIf_exists(true);
    dropDbParams.setCascade(true);
    request.setDrop_db_params(dropDbParams);
    return request;
  }

  /**
   * Creates the test db in the catalog. Sets the owner to <code>TEST_OWNER_1</code>
   */
  private static TDdlExecRequest createDbRequest() {
    TDdlExecRequest request = new TDdlExecRequest();
    request.setQuery_options(new TDdlQueryOptions());
    request.setDdl_type(TDdlType.CREATE_DATABASE);
    TCreateDbParams createDbParams = new TCreateDbParams();
    createDbParams.setDb(TEST_ALTER_DB);
    createDbParams.setComment("test comment");
    createDbParams.setOwner(TEST_OWNER_1);
    request.setCreate_db_params(createDbParams);
    return request;
  }

  /**
   * Reader task to be used by the read threads. Calls into Catalog and validates if the
   * owner and ownerType is valid
   */
  private class ValidateDbOwnerTask implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      barrier_.await();
      for (int i = 0; i < 100; i++) {
        Db testDb = catalog_.getDb(TEST_ALTER_DB);
        validateOwner(testDb.getMetaStoreDb());
      }
      return null;
    }
  }

  /**
   * Writer task to be used by the write threads. The task loops and issues many alter
   * database set owner requests. Each call flips the owner and owner type from user_1
   * (PrincipleType.USER) to user_2 (PrincipleType.ROLE) and vice versa.
   */
  private class SetOwnerTask implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      barrier_.await();
      for (int i = 0; i < 100; i++) {
        catalogOpExecutor_.execDdlRequest(getNextDdlRequest());
      }
      return null;
    }
  }

  /**
   * Test creates multiple reader and writer threads which operate on the test database.
   * The readers fetch the Db object and validate its owner information while the writer
   * thread changes the owner of the database concurrently
   */
  @Test
  public void testConcurrentAlterDbOps() throws Exception {
    ExecutorService threadPool = Executors.newFixedThreadPool(NUM_READERS + NUM_WRITERS);
    List<Future<Void>> results = new ArrayList<>(NUM_READERS + NUM_WRITERS);
    for (int i = 0; i < NUM_WRITERS; i++) {
      results.add(threadPool.submit(new SetOwnerTask()));
    }
    for (int i = 0; i < NUM_READERS; i++) {
      results.add(threadPool.submit(new ValidateDbOwnerTask()));
    }
    try {
      for (Future<Void> result : results) {
        result.get(100, TimeUnit.SECONDS);
      }
    } finally {
      threadPool.shutdownNow();
    }
  }

  /**
   * Creates ddl request to alter database set owner. Each invocation changes the owner
   * from user_1 to user_2 and vice-versa.
   */
  private TDdlExecRequest getNextDdlRequest() {
    TAlterDbSetOwnerParams alterDbSetOwnerParams = new TAlterDbSetOwnerParams();
    if (toggler_.get()) {
      alterDbSetOwnerParams.setOwner_name(TEST_OWNER_1);
      alterDbSetOwnerParams.setOwner_type(TOwnerType.findByValue(0));
      assertTrue(toggler_.compareAndSet(true, false));
    } else {
      alterDbSetOwnerParams.setOwner_name(TEST_OWNER_2);
      alterDbSetOwnerParams.setOwner_type(TOwnerType.findByValue(1));
      assertTrue(toggler_.compareAndSet(false, true));
    }
    TAlterDbParams alterDbParams = new TAlterDbParams();
    alterDbParams.setDb(TEST_ALTER_DB);
    alterDbParams.setAlter_type(TAlterDbType.SET_OWNER);
    alterDbParams.setSet_owner_params(alterDbSetOwnerParams);
    TDdlExecRequest request = new TDdlExecRequest();
    request.setQuery_options(new TDdlQueryOptions());
    request.setDdl_type(TDdlType.ALTER_DATABASE);
    request.setAlter_db_params(alterDbParams);
    return request;
  }

  /**
   * Validates the owner information of the database. Makes sure that if the owner is
   * user_1 its type is USER and if the owner is user_2 its type is ROLE
   */
  private void validateOwner(Database msDb) {
    assertNotNull(msDb.getOwnerName());
    assertNotNull(msDb.getOwnerType());
    if (TEST_OWNER_1.equals(msDb.getOwnerName())) {
      assertEquals("Owner " + TEST_OWNER_1 + " should have the type " + TEST_TYPE_1,
          msDb.getOwnerType(), TEST_TYPE_1);
    } else if (TEST_OWNER_2.equals(msDb.getOwnerName())) {
      assertEquals("Owner " + TEST_OWNER_2 + " should have the type " + TEST_TYPE_2,
          msDb.getOwnerType(), TEST_TYPE_2);
    } else {
      fail("Unknown owner for the database " + msDb.getOwnerName());
    }
  }
}
