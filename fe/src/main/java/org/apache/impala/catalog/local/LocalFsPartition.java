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

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

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
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.TPartitionStats;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class LocalFsPartition implements FeFsPartition {
  private final LocalFsTable table_;
  private final LocalPartitionSpec spec_;
  private final Partition msPartition_;

  /**
   * Null in the case of a 'prototype partition'.
   */
  @Nullable
  private final ImmutableList<FileDescriptor> fileDescriptors_;

  @Nullable
  private final byte[] partitionStats_;

  // True if partitionStats_ has intermediate_col_stats populated.
  private final boolean hasIncrementalStats_;

  public LocalFsPartition(LocalFsTable table, LocalPartitionSpec spec,
      Partition msPartition, ImmutableList<FileDescriptor> fileDescriptors,
      byte [] partitionStats, boolean hasIncrementalStats) {
    table_ = Preconditions.checkNotNull(table);
    spec_ = Preconditions.checkNotNull(spec);
    msPartition_ = Preconditions.checkNotNull(msPartition);
    fileDescriptors_ = fileDescriptors;
    partitionStats_ = partitionStats;
    hasIncrementalStats_ = hasIncrementalStats;
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
  public FileSystemUtil.FsType getFsType() {
    Preconditions.checkNotNull(getLocationPath().toUri().getScheme(),
        "Cannot get scheme from path " + getLocationPath());
    return FileSystemUtil.FsType.getFsType(getLocationPath().toUri().getScheme());
  }

  @Override
  public List<FileDescriptor> getFileDescriptors() {
    return fileDescriptors_;
  }

  @Override
  public boolean hasFileDescriptors() {
    return !fileDescriptors_.isEmpty();
  }

  @Override
  public int getNumFileDescriptors() {
    return fileDescriptors_.size();
  }

  @Override
  public String getLocation() {
    return msPartition_.getSd().getLocation();
  }

  @Override
  public THdfsPartitionLocation getLocationAsThrift() {
    String loc = getLocation();
    // The special "prototype partition" has a null location.
    if (loc == null) return null;
    // TODO(todd): support prefix-compressed partition locations. For now,
    // using -1 indicates that the location is a full path string.
    return new THdfsPartitionLocation(/*prefix_index=*/-1, loc);
  }

  @Override
  public Path getLocationPath() {
    Preconditions.checkNotNull(getLocation(),
            "LocalFsPartition location is null");
    return new Path(getLocation());
  }

  @Override
  public TAccessLevel getAccessLevel() {
    // TODO(todd): implement me
    return TAccessLevel.READ_WRITE;
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
  public boolean hasIncrementalStats() { return hasIncrementalStats_; }

  @Override // FeFsPartition
  public byte[] getPartitionStatsCompressed() { return partitionStats_; }

  @Override
  public long getSize() {
    long size = 0;
    for (FileDescriptor fd : fileDescriptors_) {
      size += fd.getFileLength();
    }
    return size;
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

  @Override
  public long getWriteId() {
    return MetastoreShim.getWriteIdFromMSPartition(msPartition_);
  }
}
