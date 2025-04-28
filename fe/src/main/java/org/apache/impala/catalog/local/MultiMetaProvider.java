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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;
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
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;

/**
 * MetaProvider implementation that proxies calls through a chain of MetaProviders. First,
 * the primary provider is visited, if it yields exception, secondary providers are
 * visited one-by-one. Some methods resort to the primary provider to keep the behavior
 * concise, for example: authorization policy checking, readiness probe, and null
 * partition key-value fetching is directed to the primary provider.
 */
public class MultiMetaProvider implements MetaProvider {

  private final MetaProvider primaryProvider_;
  private final List<MetaProvider> secondaryProviders_;

  public MultiMetaProvider(MetaProvider primaryProvider,
      List<MetaProvider> secondaryProviders) {
    primaryProvider_ = primaryProvider;
    secondaryProviders_ = secondaryProviders;
  }

  public MetaProvider getPrimaryProvider() {
    return primaryProvider_;
  }

  @Override
  public String getURI() {
    StringJoiner joiner = new StringJoiner(", ");
    joiner.add(primaryProvider_.getURI());
    for (MetaProvider provider : secondaryProviders_) {
      joiner.add(provider.getURI());
    }
    return joiner.toString();
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    return primaryProvider_.getAuthPolicy();
  }

  @Override
  public boolean isReady() {
    return primaryProvider_.isReady();
  }

  @Override
  public void waitForIsReady(long timeoutMs) {
    primaryProvider_.waitForIsReady(timeoutMs);
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    return collectFromAllProviders(
        unchecked(MetaProvider::loadDbList)).stream().flatMap(
        Collection::stream).distinct().collect(ImmutableList.toImmutableList());
  }

  @Override
  public Database loadDb(String dbName) throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadDb(dbName)));
  }

  @Override
  public ImmutableCollection<TBriefTableMeta> loadTableList(String dbName)
      throws TException {
    ImmutableList<TBriefTableMeta> combinedTableList = collectFromAllProviders(
        unchecked(provider -> provider.loadTableList(dbName))).stream().flatMap(
        Collection::stream).collect(ImmutableList.toImmutableList());
    Optional<Entry<String, Integer>> firstDuplicate = combinedTableList.stream()
        .map(tableMeta -> tableMeta.name)
        .collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).entrySet().stream()
        .filter(stringIntegerEntry -> stringIntegerEntry.getValue() > 1).findFirst();
    if (firstDuplicate.isPresent()) {
      throw new TException("Ambiguous table name: " + firstDuplicate.get().getKey());
    }
    return combinedTableList;
  }

  @Override
  public Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadTable(dbName, tableName)));
  }

  @Override
  public Pair<Table, TableMetaRef> getTableIfPresent(String dbName, String tableName) {
    try {
      return tryAllProviders(
          unchecked(provider -> provider.getTableIfPresent(dbName, tableName)));
    } catch (TException e) {
      return null;
    }
  }

  @Override
  public String loadNullPartitionKeyValue() throws TException {
    return primaryProvider_.loadNullPartitionKeyValue();
  }

  @Override
  public List<PartitionRef> loadPartitionList(TableMetaRef table)
      throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadPartitionList(table)));
  }

  @Override
  public SqlConstraints loadConstraints(TableMetaRef table, Table msTbl)
      throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadConstraints(table, msTbl)));
  }

  @Override
  public List<String> loadFunctionNames(String dbName) throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadFunctionNames(dbName)));
  }

  @Override
  public ImmutableList<Function> loadFunction(String dbName, String functionName)
      throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadFunction(dbName, functionName)));
  }

  @Override
  public ImmutableList<DataSource> loadDataSources() throws TException {
    return tryAllProviders(unchecked(MetaProvider::loadDataSources));
  }

  @Override
  public DataSource loadDataSource(String dsName) throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadDataSource(dsName)));
  }

  @Override
  public Map<String, PartitionMetadata> loadPartitionsByRefs(TableMetaRef table,
      List<String> partitionColumnNames, ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs)
      throws TException, CatalogException {
    return tryAllProviders(unchecked(
        provider -> provider.loadPartitionsByRefs(table, partitionColumnNames,
            hostIndex, partitionRefs)));
  }

  @Override
  public List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadTableColumnStatistics(table, colNames)));
  }

  @Override
  public TPartialTableInfo loadIcebergTable(TableMetaRef table) throws TException {
    return tryAllProviders(
        unchecked( provider -> provider.loadIcebergTable(table)));
  }

  @Override
  public org.apache.iceberg.Table loadIcebergApiTable(TableMetaRef table,
      TableParams param, Table msTable) throws TException {
    return tryAllProviders(
        unchecked(provider -> provider.loadIcebergApiTable(table, param, msTable)));
  }

  @Override
  public TValidWriteIdList getValidWriteIdList(TableMetaRef ref) {
    try {
      return tryAllProviders(unchecked(
          provider -> provider.getValidWriteIdList(ref)));
    } catch (TException e) {
      return null;
    }
  }

  @Override
  public Iterable<HdfsCachePool> getHdfsCachePools() {
    try {
      return tryAllProviders(unchecked(MetaProvider::getHdfsCachePools));
    } catch (TException e) {
      return null;
    }
  }

  private ImmutableList<MetaProvider> getAllProviders() {
    return ImmutableList.<MetaProvider>builder()
        .add(primaryProvider_)
        .addAll(secondaryProviders_).build();
  }

  private <R> R tryAllProviders(java.util.function.Function<MetaProvider, R> function)
      throws TException {
    Map<String, Exception> exceptions = new HashMap<>();

    ImmutableList<MetaProvider> providers = getAllProviders();

    for (MetaProvider provider : providers) {
      try {
        return function.apply(provider);
      } catch (MetaProviderException e) {
        exceptions.put(provider.getURI(), e);
      }
    }

    handleExceptions(exceptions);
    return null;
  }

  private <R> Collection<R> collectFromAllProviders(
      java.util.function.Function<MetaProvider, R> function)
      throws TException {
    Map<String, Exception> exceptions = new HashMap<>();

    ImmutableList<MetaProvider> providers = getAllProviders();

    Collection<R> results = new ArrayList<>();

    for (MetaProvider provider : providers) {
      try {
        results.add(function.apply(provider));
      } catch (MetaProviderException e) {
        exceptions.put(provider.getURI(), e);
      }
    }

    if (!results.isEmpty()) {
      return results;
    }

    handleExceptions(exceptions);
    return Collections.emptyList();
  }

  private void handleExceptions(Map<String, Exception> exceptions) throws TException {
    if (!exceptions.isEmpty()) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      for (Entry<String, Exception> e : exceptions.entrySet()) {
        printWriter.print(String.format("%s: ", e.getKey()));
        e.getValue().printStackTrace(printWriter);
      }
      String message = String.format(
          "Every MetaProvider failed with the following exceptions: %s",
          stringWriter);
      throw new TException(message);
    }
  }


  private static <T, R> java.util.function.Function<T, R> unchecked(
      ThrowingFunction<T, R> tf) {
    return t -> {
      try {
        return tf.apply(t);
      } catch (Exception e) {
        throw new MetaProviderException(e);
      }
    };
  }

  /**
   * Exception class to make exception wrapping/unwrapping clear in
   * 'collectFromAllProviders' and 'tryAllProviders'.
   */
  public static class MetaProviderException extends RuntimeException {

    MetaProviderException(Throwable cause) {
      super(cause);
    }
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R> {

    R apply(T t) throws Exception;
  }
}
