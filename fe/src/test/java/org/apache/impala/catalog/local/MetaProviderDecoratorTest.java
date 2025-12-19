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
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.local.LocalIcebergTable.TableParams;
import org.apache.impala.catalog.local.MetaProvider.PartitionMetadata;
import org.apache.impala.catalog.local.MetaProvider.PartitionRef;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;
import org.junit.Test;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

public class MetaProviderDecoratorTest {

  // Expected number of public methods in MetaProvider interface
  // Update this constant when adding new methods to MetaProvider
  private static final int EXPECTED_METHOD_COUNT = 23;

  @Test
  public void testMetaProviderMethodCount() {
    // Use reflection to count public methods in MetaProvider interface
    assertThat("Number of public methods in MetaProvider interface has changed. " +
        "Update EXPECTED_METHOD_COUNT and add tests for new methods.",
        MetaProvider.class.getDeclaredMethods().length, equalTo(EXPECTED_METHOD_COUNT));

    assertThat("Abstract class MetaProviderDecorator is missing an implementation for " +
        "one or more methods on the MetaProvider interface",
        MetaProvider.class.getDeclaredMethods().length,
        equalTo(MetaProviderDecorator.class.getDeclaredMethods().length));
  }

  @Test
  public void testAllMethodsDelegation() throws TException, CatalogException {
    // Create a mock MetaProvider
    MetaProvider mockDecorated = mock(MetaProvider.class);

    // Create a concrete implementation of MetaProviderDecorator for testing
    MetaProviderDecorator fixture = new MetaProviderDecorator(mockDecorated) {};

    // Setup mock return values
    String expectedUri = "test-uri";
    AuthorizationPolicy expectedAuthPolicy = mock(AuthorizationPolicy.class);
    boolean expectedIsReady = true;
    ImmutableList<String> expectedDbList = ImmutableList.of("db1", "db2");
    Database expectedDatabase = new Database();
    ImmutableCollection<TBriefTableMeta> expectedTableList =
        ImmutableList.of(new TBriefTableMeta());
    Table expectedTable = new Table();
    TableMetaRef expectedTableRef = mock(TableMetaRef.class);
    Pair<Table, TableMetaRef> expectedTablePair =
        new Pair<>(expectedTable, expectedTableRef);
    String expectedNullPartitionKey = "__HIVE_DEFAULT_PARTITION__";
    List<PartitionRef> expectedPartitionRefs = new ArrayList<>();
    SqlConstraints expectedConstraints = mock(SqlConstraints.class);
    List<String> expectedFunctionNames = ImmutableList.of("func1", "func2");
    ImmutableList<Function> expectedFunctions = ImmutableList.of();
    ImmutableList<DataSource> expectedDataSources = ImmutableList.of();
    DataSource expectedDataSource = mock(DataSource.class);
    Map<String, PartitionMetadata> expectedPartitionMetadata = new HashMap<>();
    List<ColumnStatisticsObj> expectedColStats = new ArrayList<>();
    TPartialTableInfo expectedIcebergTableInfo = new TPartialTableInfo();
    org.apache.iceberg.Table expectedIcebergApiTable =
        mock(org.apache.iceberg.Table.class);
    TValidWriteIdList expectedValidWriteIdList = new TValidWriteIdList();
    Iterable<HdfsCachePool> expectedCachePools = ImmutableList.of();

    // Configure mock behavior
    when(mockDecorated.getURI()).thenReturn(expectedUri);
    when(mockDecorated.getAuthPolicy()).thenReturn(expectedAuthPolicy);
    when(mockDecorated.isReady()).thenReturn(expectedIsReady);
    when(mockDecorated.loadDbList()).thenReturn(expectedDbList);
    when(mockDecorated.loadDb(anyString())).thenReturn(expectedDatabase);
    when(mockDecorated.loadTableList(anyString())).thenReturn(expectedTableList);
    when(mockDecorated.loadTable(anyString(), anyString()))
        .thenReturn(expectedTablePair);
    when(mockDecorated.getTableIfPresent(anyString(), anyString()))
        .thenReturn(expectedTablePair);
    when(mockDecorated.loadNullPartitionKeyValue()).thenReturn(expectedNullPartitionKey);
    when(mockDecorated.loadPartitionList(any(TableMetaRef.class)))
        .thenReturn(expectedPartitionRefs);
    when(mockDecorated.loadConstraints(any(TableMetaRef.class), any(Table.class)))
        .thenReturn(expectedConstraints);
    when(mockDecorated.loadFunctionNames(anyString())).thenReturn(expectedFunctionNames);
    when(mockDecorated.loadFunction(anyString(), anyString()))
        .thenReturn(expectedFunctions);
    when(mockDecorated.loadDataSources()).thenReturn(expectedDataSources);
    when(mockDecorated.loadDataSource(anyString())).thenReturn(expectedDataSource);
    when(mockDecorated.loadPartitionsByRefs(any(TableMetaRef.class), anyList(),
        any(ListMap.class), anyList())).thenReturn(expectedPartitionMetadata);
    when(mockDecorated.loadTableColumnStatistics(any(TableMetaRef.class), anyList()))
        .thenReturn(expectedColStats);
    when(mockDecorated.loadIcebergTable(any(TableMetaRef.class)))
        .thenReturn(expectedIcebergTableInfo);
    when(mockDecorated.loadIcebergApiTable(any(TableMetaRef.class),
        any(TableParams.class), any(Table.class))).thenReturn(expectedIcebergApiTable);
    when(mockDecorated.getValidWriteIdList(any(TableMetaRef.class)))
        .thenReturn(expectedValidWriteIdList);
    when(mockDecorated.getHdfsCachePools()).thenReturn(expectedCachePools);

    // Test getURI()
    String actualUri = fixture.getURI();
    assertThat(actualUri, equalTo(expectedUri));
    verify(mockDecorated).getURI();

    // Test getAuthPolicy()
    AuthorizationPolicy actualAuthPolicy = fixture.getAuthPolicy();
    assertThat(actualAuthPolicy, sameInstance(expectedAuthPolicy));
    verify(mockDecorated).getAuthPolicy();

    // Test isReady()
    boolean actualIsReady = fixture.isReady();
    assertThat(actualIsReady, equalTo(expectedIsReady));
    verify(mockDecorated).isReady();

    // Test waitForIsReady(long)
    long timeout = 1000L;
    fixture.waitForIsReady(timeout);
    verify(mockDecorated).waitForIsReady(eq(timeout));

    // Test setIsReady(boolean)
    fixture.setIsReady(false);
    verify(mockDecorated).setIsReady(eq(false));

    // Test loadDbList()
    ImmutableList<String> actualDbList = fixture.loadDbList();
    assertThat(actualDbList, equalTo(expectedDbList));
    verify(mockDecorated).loadDbList();

    // Test loadDb(String)
    Database actualDatabase = fixture.loadDb("testDb");
    assertThat(actualDatabase, sameInstance(expectedDatabase));
    verify(mockDecorated).loadDb(eq("testDb"));

    // Test loadTableList(String)
    ImmutableCollection<TBriefTableMeta> actualTableList =
        fixture.loadTableList("testDb");
    assertThat(actualTableList, sameInstance(expectedTableList));
    verify(mockDecorated).loadTableList(eq("testDb"));

    // Test loadTable(String, String)
    Pair<Table, TableMetaRef> actualTablePair =
        fixture.loadTable("testDb", "testTable");
    assertThat(actualTablePair, sameInstance(expectedTablePair));
    verify(mockDecorated).loadTable(eq("testDb"), eq("testTable"));

    // Test getTableIfPresent(String, String)
    Pair<Table, TableMetaRef> actualTableIfPresent =
        fixture.getTableIfPresent("testDb", "testTable");
    assertThat(actualTableIfPresent, sameInstance(expectedTablePair));
    verify(mockDecorated).getTableIfPresent(eq("testDb"), eq("testTable"));

    // Test loadNullPartitionKeyValue()
    String actualNullPartitionKey = fixture.loadNullPartitionKeyValue();
    assertThat(actualNullPartitionKey, equalTo(expectedNullPartitionKey));
    verify(mockDecorated).loadNullPartitionKeyValue();

    // Test loadPartitionList(TableMetaRef)
    List<PartitionRef> actualPartitionRefs =
        fixture.loadPartitionList(expectedTableRef);
    assertThat(actualPartitionRefs, sameInstance(expectedPartitionRefs));
    verify(mockDecorated).loadPartitionList(eq(expectedTableRef));

    // Test loadConstraints(TableMetaRef, Table)
    SqlConstraints actualConstraints =
        fixture.loadConstraints(expectedTableRef, expectedTable);
    assertThat(actualConstraints, sameInstance(expectedConstraints));
    verify(mockDecorated).loadConstraints(eq(expectedTableRef), eq(expectedTable));

    // Test loadFunctionNames(String)
    List<String> actualFunctionNames = fixture.loadFunctionNames("testDb");
    assertThat(actualFunctionNames, equalTo(expectedFunctionNames));
    verify(mockDecorated).loadFunctionNames(eq("testDb"));

    // Test loadFunction(String, String)
    ImmutableList<Function> actualFunctions =
        fixture.loadFunction("testDb", "testFunc");
    assertThat(actualFunctions, equalTo(expectedFunctions));
    verify(mockDecorated).loadFunction(eq("testDb"), eq("testFunc"));

    // Test loadDataSources()
    ImmutableList<DataSource> actualDataSources = fixture.loadDataSources();
    assertThat(actualDataSources, equalTo(expectedDataSources));
    verify(mockDecorated).loadDataSources();

    // Test loadDataSource(String)
    DataSource actualDataSource = fixture.loadDataSource("testDs");
    assertThat(actualDataSource, sameInstance(expectedDataSource));
    verify(mockDecorated).loadDataSource(eq("testDs"));

    // Test loadPartitionsByRefs(...)
    List<String> partCols = ImmutableList.of("col1");
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    List<PartitionRef> partRefs = new ArrayList<>();
    Map<String, PartitionMetadata> actualPartitionMetadata =
        fixture.loadPartitionsByRefs(expectedTableRef, partCols, hostIndex, partRefs);
    assertThat(actualPartitionMetadata, sameInstance(expectedPartitionMetadata));
    verify(mockDecorated).loadPartitionsByRefs(
        eq(expectedTableRef), eq(partCols), eq(hostIndex), eq(partRefs));

    // Test loadTableColumnStatistics(TableMetaRef, List<String>)
    List<String> colNames = ImmutableList.of("col1", "col2");
    List<ColumnStatisticsObj> actualColStats =
        fixture.loadTableColumnStatistics(expectedTableRef, colNames);
    assertThat(actualColStats, sameInstance(expectedColStats));
    verify(mockDecorated).loadTableColumnStatistics(eq(expectedTableRef), eq(colNames));

    // Test loadIcebergTable(TableMetaRef)
    TPartialTableInfo actualIcebergTableInfo =
        fixture.loadIcebergTable(expectedTableRef);
    assertThat(actualIcebergTableInfo, sameInstance(expectedIcebergTableInfo));
    verify(mockDecorated).loadIcebergTable(eq(expectedTableRef));

    // Test loadIcebergApiTable(TableMetaRef, TableParams, Table)
    TableParams tableParams = mock(TableParams.class);
    org.apache.iceberg.Table actualIcebergApiTable =
        fixture.loadIcebergApiTable(expectedTableRef, tableParams, expectedTable);
    assertThat(actualIcebergApiTable, sameInstance(expectedIcebergApiTable));
    verify(mockDecorated).loadIcebergApiTable(
        eq(expectedTableRef), eq(tableParams), eq(expectedTable));

    // Test getValidWriteIdList(TableMetaRef)
    TValidWriteIdList actualValidWriteIdList =
        fixture.getValidWriteIdList(expectedTableRef);
    assertThat(actualValidWriteIdList, sameInstance(expectedValidWriteIdList));
    verify(mockDecorated).getValidWriteIdList(eq(expectedTableRef));

    // Test getHdfsCachePools()
    Iterable<HdfsCachePool> actualCachePools = fixture.getHdfsCachePools();
    assertThat(actualCachePools, sameInstance(expectedCachePools));
    verify(mockDecorated).getHdfsCachePools();

    verifyNoMoreInteractions(mockDecorated);
  }

}
