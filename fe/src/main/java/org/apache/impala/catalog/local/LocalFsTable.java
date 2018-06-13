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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.util.ListMap;

public class LocalFsTable extends LocalTable implements FeFsTable {
  public LocalFsTable(LocalDb db, String tblName, SchemaInfo schemaInfo) {
    super(db, tblName, schemaInfo);
  }

  @Override
  public boolean isCacheable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isLocationCacheable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isMarkedCached() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getLocation() {
    return getMetaStoreTable().getSd().getLocation();
  }

  @Override
  public String getNullPartitionKeyValue() {
    return db_.getCatalog().getNullPartitionKeyValue();
  }

  @Override
  public TResultSet getFiles(List<List<TPartitionKeyValue>> partitionSet)
      throws CatalogException {
    // TODO(todd): implement fetching files from HDFS
    return null;
  }

  @Override
  public String getHdfsBaseDir() {
    // TODO(todd): this is redundant with getLocation, it seems.
    return getLocation();
  }

  @Override
  public long getTotalHdfsBytes() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getTotalNumFiles() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isAvroTable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long getExtrapolatedNumRows(long totalBytes) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isStatsExtrapolationEnabled() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public TResultSet getTableStats() {
    return HdfsTable.getTableStats(this);
  }

  @Override
  public Collection<? extends PrunablePartition> getPartitions() {
    // TODO(todd): implement me
    return Collections.emptyList();
  }

  @Override
  public Set<Long> getPartitionIds() {
    // TODO(todd): implement me
    return Collections.emptySet();
  }

  @Override
  public Map<Long, ? extends PrunablePartition> getPartitionMap() {
    // TODO(todd): implement me
    return Collections.emptyMap();
  }

  @Override
  public TreeMap<LiteralExpr, HashSet<Long>> getPartitionValueMap(int col) {
    // TODO(todd): implement me
    return new TreeMap<>();
  }

  @Override
  public Set<Long> getNullPartitionIds(int colIdx) {
    // TODO(todd): implement me
    return Collections.emptySet();
  }

  @Override
  public List<? extends FeFsPartition> loadPartitions(Collection<Long> ids) {
    // TODO(todd): implement me
    return Collections.emptyList();
  }

  @Override
  public int parseSkipHeaderLineCount(StringBuilder error) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Map<Long, List<FileDescriptor>> getFilesSample(
      Collection<? extends FeFsPartition> inputParts,
      long percentBytes, long minSampleBytes, long randomSeed) {
    // TODO(todd): implement me
    return Collections.emptyMap();
  }

  @Override
  public ListMap<TNetworkAddress> getHostIndex() {
    // TODO(todd): implement me
    return new ListMap<>();
  }
}
