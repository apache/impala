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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.util.CatalogBlacklistUtilsTest;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

public class BlacklistingMetaProviderTest {

  private static TBackendGflags origFlags;

  @BeforeClass
  public static void setup() {
    // The original BackendConfig need to be saved so they can be restored and not break
    // other tests.
    if (BackendConfig.INSTANCE == null) {
      BackendConfig.create(new TBackendGflags());
    }
    origFlags = BackendConfig.INSTANCE.getBackendCfg();
  }

  @AfterClass
  public static void teardown() {
    BackendConfig.create(origFlags);
  }

  @Test
  public void testLoadDbList() throws TException {
    // Configure backend with blacklisted databases.
    CatalogBlacklistUtilsTest.setBlacklist("blacklisted_db1,blacklisted_db2", "");

    // Create mock provider delegate that returns a list including both blacklisted and
    // non-blacklisted databases.
    MetaProvider mockDelegate = Mockito.mock(MetaProvider.class);
    Mockito.when(mockDelegate.loadDbList()).thenReturn(ImmutableList.of("allowed_db1",
        "blacklisted_db1", "allowed_db2", "blacklisted_db2", "allowed_db3"));

    // Create the blacklisting provider
    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockDelegate);

    // Call loadDbList and verify blacklisted databases are filtered out.
    ImmutableList<String> result = fixture.loadDbList();

    // Should have 3 databases: allowed_db1, allowed_db2, allowed_db3.
    assertThat(result.size(), equalTo(3));
    assertTrue(result.contains("allowed_db1"));
    assertTrue(result.contains("allowed_db2"));
    assertTrue(result.contains("allowed_db3"));

    Mockito.verify(mockDelegate).loadDbList();
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testLoadDbListWithNonBlacklistedDbs() throws TException {
    // Configure backend with empty blacklist.
    CatalogBlacklistUtilsTest.setBlacklist("", "");

    // Create mock provider delegate that returns a list of databases.
    MetaProvider mockDelegate = Mockito.mock(MetaProvider.class);
    Mockito.when(mockDelegate.loadDbList()).thenReturn(ImmutableList.of("regular_db1",
        "regular_db2", "regular_db3"));

    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockDelegate);
    ImmutableList<String> result = fixture.loadDbList();

    // Verify that all databases are returned.
    assertThat(result.size(), equalTo(3));
    assertTrue(result.contains("regular_db1"));
    assertTrue(result.contains("regular_db2"));
    assertTrue(result.contains("regular_db3"));

    Mockito.verify(mockDelegate).loadDbList();
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testLoadDbListNoBlacklistedDbs() throws TException {
    // Configure backend with no blacklisted databases.
    CatalogBlacklistUtilsTest.setBlacklist("", "");

    // Create list of databases.
    ImmutableList<String> dbList = ImmutableList.of("db1", "db2");

    // Create mock provider delegate that returns a list of databases.
    MetaProvider mockDelegate = Mockito.mock(MetaProvider.class);
    Mockito.when(mockDelegate.loadDbList()).thenReturn(dbList);

    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockDelegate);
    ImmutableList<String> result = fixture.loadDbList();

    // Verify that all databases are returned.
    assertThat(result, sameInstance(dbList));

    Mockito.verify(mockDelegate).loadDbList();
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLoadTableList() throws TException {
    // Configure backend with blacklisted tables.
    CatalogBlacklistUtilsTest.setBlacklist("", "db1.foo,db2.bar");

    // Create mock provider delegate that returns a list including both blacklisted and
    // non-blacklisted tables.
    MetaProvider mockDelegate = Mockito.mock(MetaProvider.class);
    Mockito.when(mockDelegate.loadTableList("db1")).thenReturn(ImmutableList.of(
        new TBriefTableMeta("foo"), new TBriefTableMeta("bar")));
    Mockito.when(mockDelegate.loadTableList("db2")).thenReturn(ImmutableList.of(
        new TBriefTableMeta("foo"), new TBriefTableMeta("bar")));
    Mockito.when(mockDelegate.loadTableList("db3")).thenReturn(ImmutableList.of(
        new TBriefTableMeta("foo"), new TBriefTableMeta("bar")));

    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockDelegate);

    assertThat(fixture.loadTableList("db1").toArray(),
        arrayContainingInAnyOrder(equalTo(new TBriefTableMeta("bar"))));

    assertThat(fixture.loadTableList("db2").toArray(),
        arrayContainingInAnyOrder(equalTo(new TBriefTableMeta("foo"))));

    assertThat(fixture.loadTableList("db3").toArray(), arrayContainingInAnyOrder(
        equalTo(new TBriefTableMeta("foo")), equalTo(new TBriefTableMeta("bar"))));

    Mockito.verify(mockDelegate).loadTableList("db1");
    Mockito.verify(mockDelegate).loadTableList("db2");
    Mockito.verify(mockDelegate).loadTableList("db3");
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testLoadTableListWithNoBlacklistedTables() throws TException {
    // Configure backend with no blacklisted tables.
    CatalogBlacklistUtilsTest.setBlacklist("", "");

    // Create list of tables.
    ImmutableCollection<TBriefTableMeta> tablesList =
        ImmutableList.of(new TBriefTableMeta("tbl1"), new TBriefTableMeta("tbl2"));

    // Create mock provider delegate that returns a list of databases.
    MetaProvider mockDelegate = Mockito.mock(MetaProvider.class);
    Mockito.when(mockDelegate.loadTableList("db1")).thenReturn(tablesList);

    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockDelegate);
    ImmutableCollection<TBriefTableMeta> result = fixture.loadTableList("db1");

    // Verify that all databases are returned.
    assertThat(result, sameInstance(tablesList));

    Mockito.verify(mockDelegate).loadTableList("db1");
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

}
