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

package org.apache.impala.catalog.local;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.impala.catalog.CatalogRef;
import org.apache.impala.thrift.CatalogLookupStatus;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.thrift.TException;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests that MultiMetaProvider knows which provider serves which table of a database
 * merged from several providers, from listing table names only, and rejects a table
 * whose provider cannot be decided.
 */
public class MultiMetaProviderTest {
  private static final String DB_NAME = "db";
  private static final String TBL_NAME = "tbl";
  private static final CatalogRef REST_1 = CatalogRef.rest("rest-1", "rest-1-uri");
  private static final CatalogRef REST_2 = CatalogRef.rest("rest-2", "rest-2-uri");
  private static final CatalogRef REST_3 = CatalogRef.rest("rest-3", "rest-3-uri");

  @Test
  public void testTableIsServedByProviderThatListsIt() throws Exception {
    MetaProvider catalogd = provider(CatalogRef.CATALOGD, "hms_tbl");
    MetaProvider first = provider(REST_1, "other_tbl");
    MetaProvider second = provider(REST_2, TBL_NAME);
    MultiMetaProvider provider =
        new MultiMetaProvider(catalogd, Arrays.asList(first, second));
    assertSame(REST_2, provider.getCatalogRef(DB_NAME, TBL_NAME));
    assertSame(REST_2, provider.getCatalogRef(DB_NAME, TBL_NAME.toUpperCase()));
    assertSame(REST_1, provider.getCatalogRef(DB_NAME, "other_tbl"));
    assertSame(CatalogRef.CATALOGD, provider.getCatalogRef(DB_NAME, "hms_tbl"));
    // Every call lists the database afresh, and no table is loaded.
    for (MetaProvider p : Arrays.asList(catalogd, first, second)) {
      verify(p, times(4)).loadTableList(DB_NAME);
      verify(p, never()).loadTable(anyString(), anyString());
      verify(p, never()).getTableIfPresent(anyString(), anyString());
    }
  }

  @Test
  public void testTableFollowsConcurrentChanges() throws Exception {
    MetaProvider catalogd = provider(CatalogRef.CATALOGD);
    when(catalogd.loadTableList(DB_NAME))
        .thenReturn(ImmutableList.of())
        .thenReturn(ImmutableList.of(new TBriefTableMeta(TBL_NAME)));
    MetaProvider rest = provider(REST_1);
    when(rest.loadTableList(DB_NAME))
        .thenReturn(ImmutableList.of(new TBriefTableMeta(TBL_NAME)))
        .thenReturn(ImmutableList.of());
    MultiMetaProvider provider = new MultiMetaProvider(catalogd, Arrays.asList(rest));
    assertSame(REST_1, provider.getCatalogRef(DB_NAME, TBL_NAME));
    // The table was dropped from the REST catalog and created in catalogd.
    assertSame(CatalogRef.CATALOGD, provider.getCatalogRef(DB_NAME, TBL_NAME));
  }

  @Test
  public void testMissingTableIsServedByFirstProviderOfDatabase() throws Exception {
    MetaProvider catalogd = provider(CatalogRef.CATALOGD);
    when(catalogd.loadTableList(DB_NAME)).thenThrow(
        new InconsistentMetadataFetchException(
            CatalogLookupStatus.DB_NOT_FOUND, "no such db"));
    MetaProvider first = provider(REST_1);
    when(first.loadTableList(DB_NAME)).thenThrow(new NoSuchNamespaceException("no db"));
    MetaProvider second = provider(REST_2, "other_tbl");
    MetaProvider third = provider(REST_3);
    MultiMetaProvider provider =
        new MultiMetaProvider(catalogd, Arrays.asList(first, second, third));
    assertSame(REST_2, provider.getCatalogRef(DB_NAME, TBL_NAME));

    // With the database in catalogd, catalogd answers for its missing tables.
    MultiMetaProvider withCatalogd = new MultiMetaProvider(provider(CatalogRef.CATALOGD),
        Arrays.asList(provider(REST_1, "other_tbl")));
    assertSame(CatalogRef.CATALOGD, withCatalogd.getCatalogRef(DB_NAME, TBL_NAME));
  }

  @Test
  public void testMissingDatabaseIsServedByPrimary() throws Exception {
    MetaProvider first = provider(REST_1);
    when(first.loadTableList(DB_NAME)).thenThrow(new NoSuchNamespaceException("no db"));
    MetaProvider second = provider(REST_2);
    when(second.loadTableList(DB_NAME)).thenThrow(
        new TException(new UnknownDBException("no db")));
    MultiMetaProvider provider = new MultiMetaProvider(first, Arrays.asList(second));
    assertSame(REST_1, provider.getCatalogRef(DB_NAME, TBL_NAME));
  }

  @Test
  public void testFailingProviderMakesEveryTableUndecidable() throws Exception {
    // A provider that fails to list the database may hold the table, so no table of
    // the database can be resolved, not even one that another provider lists.
    MetaProvider catalogd = provider(CatalogRef.CATALOGD, "hms_tbl");
    MetaProvider failing = provider(REST_1);
    when(failing.loadTableList(DB_NAME)).thenThrow(new TException("unreachable"));
    MultiMetaProvider provider =
        new MultiMetaProvider(catalogd, Arrays.asList(failing));
    assertUndecidable(provider, TBL_NAME);
    assertUndecidable(provider, "hms_tbl");
  }

  @Test
  public void testTableListedBySeveralProvidersIsAmbiguous() throws Exception {
    MetaProvider first = provider(REST_1, TBL_NAME);
    MetaProvider second = provider(REST_2, TBL_NAME.toUpperCase(), "other_tbl");
    MultiMetaProvider provider = new MultiMetaProvider(first, Arrays.asList(second));
    assertAmbiguous(provider, TBL_NAME);
    assertSame(REST_2, provider.getCatalogRef(DB_NAME, "other_tbl"));
  }

  @Test
  public void testLocalCatalogAsksItsProvider() throws Exception {
    MetaProvider first = provider(REST_1, "other_tbl");
    MetaProvider second = provider(REST_2, TBL_NAME);
    LocalCatalog catalog =
        new LocalCatalog(new MultiMetaProvider(first, Arrays.asList(second)));
    assertSame(REST_2, catalog.getCatalogRef(DB_NAME.toUpperCase(), TBL_NAME));
    // Without MultiMetaProvider, every table belongs to the one catalog.
    assertSame(REST_1,
        new LocalCatalog(first).getCatalogRef("no_such_db", "no_such_table"));
  }

  private static void assertAmbiguous(MultiMetaProvider provider, String tblName) {
    try {
      provider.getCatalogRef(DB_NAME, tblName);
      fail("Expected the table to be ambiguous");
    } catch (TException e) {
      assertTrue(e.getMessage().contains("Ambiguous table name: " + tblName));
    }
  }

  private static void assertUndecidable(MultiMetaProvider provider, String tblName) {
    try {
      provider.getCatalogRef(DB_NAME, tblName);
      fail("Expected the catalog of the table to be undecidable");
    } catch (TException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith(
          "Cannot decide which catalog serves " + DB_NAME + "." + tblName + ": "));
      assertTrue(e.getMessage(), e.getMessage().contains("unreachable"));
    }
  }

  /**
   * Returns a provider of catalog 'catalogRef' that lists the tables 'tblNames' in
   * database DB_NAME and, like the real providers, answers its own catalog for every
   * table.
   */
  private static MetaProvider provider(CatalogRef catalogRef, String... tblNames)
      throws TException {
    MetaProvider provider = mock(MetaProvider.class);
    when(provider.getURI()).thenReturn(catalogRef.toString());
    when(provider.getCatalogRef(anyString(), anyString())).thenReturn(catalogRef);
    ImmutableList.Builder<TBriefTableMeta> tables = ImmutableList.builder();
    for (String tblName : tblNames) tables.add(new TBriefTableMeta(tblName));
    when(provider.loadTableList(DB_NAME)).thenReturn(tables.build());
    return provider;
  }
}
