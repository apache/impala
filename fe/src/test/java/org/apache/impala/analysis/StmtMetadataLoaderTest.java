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

package org.apache.impala.analysis;

import java.util.Arrays;

import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.local.CatalogdMetaProvider;
import org.apache.impala.catalog.local.FailedLoadLocalTable;
import org.apache.impala.catalog.local.LocalCatalog;
import org.apache.impala.catalog.local.LocalDb;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.util.EventSequence;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class StmtMetadataLoaderTest {

  private void testLoadTables(String stmtStr,
      int expectedNumLoadRequests, int expectedNumCatalogUpdates,
      String[] expectedDbs, String[] expectedTables)
      throws ImpalaException {
    try (ImpaladTestCatalog catalog = new ImpaladTestCatalog()) {
      Frontend fe = new Frontend(new NoopAuthorizationFactory(), catalog);
      StatementBase stmt = Parser.parse(stmtStr);
      // Catalog is fresh and no tables are cached.
      validateUncached(stmt, fe, expectedNumLoadRequests, expectedNumCatalogUpdates,
          expectedDbs, expectedTables);
      // All relevant tables should be cached now.
      validateCached(stmt, fe, expectedDbs, expectedTables);
    }
  }

  private void testNoLoad(String stmtStr) throws ImpalaException {
    try (ImpaladTestCatalog catalog = new ImpaladTestCatalog()) {
      Frontend fe = new Frontend(new NoopAuthorizationFactory(), catalog);
      StatementBase stmt = Parser.parse(stmtStr);
      validateCached(stmt, fe, new String[]{}, new String[]{});
    }
  }

  private void testLoadAcidTables(String stmtStr)
      throws ImpalaException {
    try (ImpaladTestCatalog catalog = new ImpaladTestCatalog()) {
      Frontend fe = new Frontend(new NoopAuthorizationFactory(), catalog);
      StatementBase stmt = Parser.parse(stmtStr);
      EventSequence timeline = new EventSequence("Test Timeline");
      StmtMetadataLoader mdLoader =
          new StmtMetadataLoader(fe, Catalog.DEFAULT_DB, timeline);
      StmtTableCache stmtTableCache = mdLoader.loadTables(stmt);
      validateTablesWriteIds(stmtTableCache);
    }
  }

  private void validateDbs(StmtTableCache stmtTableCache, String[] expectedDbs) {
    String[] actualDbs = new String[stmtTableCache.dbs.size()];
    actualDbs = stmtTableCache.dbs.toArray(actualDbs);
    Arrays.sort(expectedDbs);
    Arrays.sort(actualDbs);
    Assert.assertArrayEquals(expectedDbs, actualDbs);
  }

  private void validateTables(StmtTableCache stmtTableCache, String[] expectedTables) {
    String[] actualTables = new String[stmtTableCache.tables.size()];
    int idx = 0;
    for (FeTable t: stmtTableCache.tables.values()) {
      Assert.assertTrue(t.isLoaded());
      actualTables[idx++] = t.getFullName();
    }
    Arrays.sort(expectedTables);
    Arrays.sort(actualTables);
    Assert.assertArrayEquals(expectedTables, actualTables);
  }

  private void validateTablesWriteIds(StmtTableCache stmtTableCache) {
    Assert.assertTrue(stmtTableCache.tables.size() > 0);
    for (FeTable t: stmtTableCache.tables.values()) {
      Assert.assertTrue(t.isLoaded());
      Assert.assertTrue(t.getValidWriteIds() != null);
      Assert.assertTrue(t.getValidWriteIds().isWriteIdValid(t.getWriteId()));
    }
  }

  // Assume tables in the stmt are not acid tables.
  private void validateUncached(StatementBase stmt, Frontend fe,
      int expectedNumLoadRequests, int expectedNumCatalogUpdates,
      String[] expectedDbs, String[] expectedTables) throws InternalException {
    EventSequence timeline = new EventSequence("Test Timeline");
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(fe, Catalog.DEFAULT_DB, timeline);
    StmtTableCache stmtTableCache = mdLoader.loadTables(stmt);
    // Validate metrics.
    Assert.assertEquals(expectedNumLoadRequests,
        mdLoader.getNumLoadRequestsSent());
    Assert.assertEquals(expectedNumCatalogUpdates,
        mdLoader.getNumCatalogUpdatesReceived());
    // Validate timeline.
    Assert.assertEquals(2, mdLoader.getTimeline().getNumEvents());
    // Validate dbs and tables.
    validateDbs(stmtTableCache, expectedDbs);
    validateTables(stmtTableCache, expectedTables);
  }

  private void validateCached(StatementBase stmt, Frontend fe,
      String[] expectedDbs, String[] expectedTables) throws InternalException {
    EventSequence timeline = new EventSequence("Test Timeline");
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(fe, Catalog.DEFAULT_DB, timeline);
    StmtTableCache stmtTableCache = mdLoader.loadTables(stmt);
    // Validate metrics. Expect all tables to already be in the cache.
    Assert.assertEquals(0, mdLoader.getNumLoadRequestsSent());
    Assert.assertEquals(0, mdLoader.getNumCatalogUpdatesReceived());
    // Validate timeline. Expect a single "everything is cached" event.
    Assert.assertEquals(1, mdLoader.getTimeline().getNumEvents());
    // Validate dbs and tables.
    validateDbs(stmtTableCache, expectedDbs);
    validateTables(stmtTableCache, expectedTables);
  }

  @Test
  public void testSingleLoadRequest() throws ImpalaException {
    // Single query block.
    testLoadTables("select * from functional.alltypes", 1, 1,
        new String[] {"default", "functional"},
        new String[] {"functional.alltypes"});
    // Single query block, multiple dbs and tables.
    testLoadTables("select * from functional.alltypes, functional_parquet.alltypes, " +
        "functional_avro.alltypes", 1, 1,
        new String[] {"default", "functional", "functional_parquet", "functional_avro"},
        new String[] {"functional.alltypes", "functional_parquet.alltypes",
            "functional_avro.alltypes"});
    // Single query block, test deduplication.
    testLoadTables("select * from functional.alltypes, functional.alltypes, " +
        "functional.alltypes", 1, 1,
        new String[] {"default", "functional"},
        new String[] {"functional.alltypes"});
    // Multiple query blocks, multiple dbs and tables.
    testLoadTables("with w as (select id from functional.alltypes) " +
        "select * from w, (select id from functional.alltypessmall) v " +
        "where v.id in (select id from functional.alltypestiny)", 1, 1,
        new String[] {"default", "functional"},
        new String[] {"functional.alltypes", "functional.alltypessmall",
            "functional.alltypestiny"});
    testLoadTables("select * from functional.alltypes union distinct " +
        "select * from functional.alltypessmall union all " +
        "select * from functional.alltypestiny", 1, 1,
        new String[] {"default", "functional"},
        new String[] {"functional.alltypes", "functional.alltypessmall",
            "functional.alltypestiny"});
    // Multiple query blocks, test deduplication.
    testLoadTables("with w as (select id from functional.alltypes) " +
        "select * from w, (select id from functional.alltypes) v " +
        "where v.id in (select id from functional.alltypes)", 1, 1,
        new String[] {"default", "functional"},
        new String[] {"functional.alltypes"});
    testLoadTables("select * from functional.alltypes union distinct " +
        "select * from functional.alltypes union all " +
        "select * from functional.alltypes", 1, 1,
        new String[] {"default", "functional"},
        new String[] {"functional.alltypes"});
  }

  @Test
  public void testViewExpansion() throws ImpalaException {
    // Test views:
    // functional.alltypes_view references functional.alltypes
    // functional.view_view references functional.alltypes_view
    testLoadTables("select * from functional.alltypes_view", 2, 2,
        new String[] {"default", "functional"},
        new String[] {"functional.alltypes_view", "functional.alltypes"});
    testLoadTables("select * from functional.view_view", 3, 3,
        new String[] {"default", "functional"},
        new String[] {"functional.view_view", "functional.alltypes_view",
            "functional.alltypes"});
    // Test deduplication.
    testLoadTables("select * from functional.view_view, functional.view_view", 3, 3,
        new String[] {"default", "functional"},
        new String[] {"functional.view_view", "functional.alltypes_view",
            "functional.alltypes"});
    testLoadTables("select * from functional.alltypes, functional.view_view", 2, 2,
        new String[] {"default", "functional"},
        new String[] {"functional.view_view", "functional.alltypes_view",
            "functional.alltypes"});
    testLoadTables("select * from functional.alltypes_view, functional.view_view", 2, 2,
        new String[] {"default", "functional"},
        new String[] {"functional.view_view", "functional.alltypes_view",
            "functional.alltypes"});
    // All tables nested in views are also referenced at top level.
    testLoadTables("select * from functional.alltypes, functional.alltypes_view, " +
            "functional.view_view", 1, 1,
        new String[] {"default", "functional"},
        new String[] {"functional.view_view", "functional.alltypes_view",
            "functional.alltypes"});
  }

  @Test
  public void testResetMetadataStmts() throws ImpalaException {
    // These stmts should not request any table loads.
    testNoLoad("invalidate metadata");
    testNoLoad("invalidate metadata functional.alltypes");
    testNoLoad("refresh functional.alltypes");
    testNoLoad("refresh functions functional");
    testNoLoad("refresh authorization");

    // This stmt requires the table to be loaded.
    testLoadTables("refresh functional.alltypes partition (year=2009, month=1)", 1, 1,
        new String[] {"default", "functional"}, new String[] {"functional.alltypes"});
  }

  @Test
  public void testTableWriteID() throws ImpalaException {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    testLoadAcidTables("select * from functional.insert_only_transactional_table");
  }

  @Test
  public void testCollectPolicyTablesOnFailedTables() throws ImpalaException {
    FeSupport.loadLibrary();
    CatalogdMetaProvider provider = new CatalogdMetaProvider(
        BackendConfig.INSTANCE.getBackendCfg());
    LocalCatalog catalog = new LocalCatalog(provider, /*defaultKuduMasterHosts=*/null);
    Frontend fe = new Frontend(new NoopAuthorizationFactory(), catalog);
    EventSequence timeline = new EventSequence("Test Timeline");
    User user = new User("user");
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(fe, Catalog.DEFAULT_DB, timeline, user, null);
    LocalDb db = new LocalDb(catalog, "default");
    mdLoader.collectPolicyTables(new FailedLoadLocalTable(
        db, "tbl", TImpalaTableType.TABLE, "comment",
        new TableLoadingException("error", /*cause=*/new Exception())));
  }
}
