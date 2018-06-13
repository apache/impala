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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import org.apache.impala.catalog.PartitionStatsUtil;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TPartitionStats;

import com.google.common.base.Preconditions;

public class LocalFsPartition implements FeFsPartition {
  private final LocalFsTable table_;
  private final LocalPartitionSpec spec_;
  private final Partition msPartition_;

  public LocalFsPartition(LocalFsTable table, LocalPartitionSpec spec,
      Partition msPartition) {
    table_ = Preconditions.checkNotNull(table);
    spec_ = Preconditions.checkNotNull(spec);
    msPartition_ = Preconditions.checkNotNull(msPartition);
  }

  @Override
  public String getPartitionName() {
    return FeCatalogUtils.getPartitionName(this);
  }

  @Override
  public long getId() {
    return spec_.getId();
  }

  @Override
  public FeFsTable getTable() {
    return table_;
  }

  @Override
  public List<FileDescriptor> getFileDescriptors() {
    // TODO(todd): implement me
    return Collections.emptyList();
  }

  @Override
  public boolean hasFileDescriptors() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getNumFileDescriptors() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getLocation() {
    return msPartition_.getSd().getLocation();
  }

  @Override
  public Path getLocationPath() {
    return new Path(getLocation());
  }

  @Override
  public TAccessLevel getAccessLevel() {
    // TODO(todd): implement me
    return TAccessLevel.READ_ONLY;
  }

  @Override
  public boolean isCacheable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isMarkedCached() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public HdfsStorageDescriptor getInputFormatDescriptor() {
    try {
      // TODO(todd): should we do this in the constructor to avoid having to worry
      // about throwing an exception from this getter? The downside is that then
      // the object would end up long-lived, whereas its actual usage is short-lived.
      return HdfsStorageDescriptor.fromStorageDescriptor(table_.getName(),
              msPartition_.getSd());
    } catch (InvalidStorageDescriptorException e) {
      throw new LocalCatalogException(String.format(
          "Invalid input format descriptor for partition %s of table %s",
          getPartitionName(), table_.getFullName()), e);
    }
  }

  @Override
  public HdfsFileFormat getFileFormat() {
    return getInputFormatDescriptor().getFileFormat();
  }

  @Override
  public TPartitionStats getPartitionStats() {
    // TODO(todd): after HIVE-19715 is complete, we should defer loading these
    // stats until requested. Even prior to that, we should consider caching them
    // separately to increase effective cache capacity.
    return PartitionStatsUtil.getPartStatsOrWarn(this);
  }

  @Override
  public boolean hasIncrementalStats() {
    // TODO(todd): copy-paste from HdfsPartition
    // TODO(todd): as in the equivalent method in HdfsPartition, this is
    // expensive because it deserializes the stats completely.
    TPartitionStats partStats = getPartitionStats();
    return partStats != null && partStats.intermediate_col_stats != null;
  }

  @Override
  public long getSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getNumRows() {
    return FeCatalogUtils.getRowCount(msPartition_.getParameters());
  }

  @Override
  public String getConjunctSql() {
    return FeCatalogUtils.getConjunctSqlForPartition(this);
  }

  @Override
  public List<String> getPartitionValuesAsStrings(boolean mapNullsToHiveKey) {
    return FeCatalogUtils.getPartitionValuesAsStrings(this, mapNullsToHiveKey);
  }

  @Override
  public List<LiteralExpr> getPartitionValues() {
    return spec_.getPartitionValues();
  }

  @Override
  public LiteralExpr getPartitionValue(int pos) {
    return spec_.getPartitionValues().get(pos);
  }

  @Override
  public Map<String, String> getParameters() {
    return msPartition_.getParameters();
  }
}
