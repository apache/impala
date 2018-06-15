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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.PartitionStatsUtil;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.TPartitionStats;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class LocalFsPartition implements FeFsPartition {
  private static final Configuration CONF = new Configuration();
  private final LocalFsTable table_;
  private final LocalPartitionSpec spec_;
  private final Partition msPartition_;
  private ImmutableList<FileDescriptor> fileDescriptors_;

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
    loadFileDescriptors();
    return fileDescriptors_;
  }

  @Override
  public boolean hasFileDescriptors() {
    loadFileDescriptors();
    return !fileDescriptors_.isEmpty();
  }

  @Override
  public int getNumFileDescriptors() {
    loadFileDescriptors();
    return fileDescriptors_.size();
  }

  @Override
  public String getLocation() {
    return msPartition_.getSd().getLocation();
  }

  @Override
  public THdfsPartitionLocation getLocationAsThrift() {
    // TODO(todd): support prefix-compressed partition locations. For now,
    // using -1 indicates that the location is a full path string.
    return new THdfsPartitionLocation(/*prefix_index=*/-1, getLocation());
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
    loadFileDescriptors();
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
  public Map<String, String> getFilteredHmsParameters() {
    // TODO(todd): for now, copied from HdfsPartition. Eventually we would want to
    // lazy-fetch these parameters separately for only the cases that require them,
    // since they are quite large.
    return Maps.filterKeys(getParameters(),
        HdfsPartition.IS_NOT_INCREMENTAL_STATS_KEY);
  }


  private void loadFileDescriptors() {
    if (fileDescriptors_ != null) return;
    Path partDir = getLocationPath();
    List<LocatedFileStatus> stats;
    try {
      stats = table_.db_.getCatalog().getMetaProvider().loadFileMetadata(partDir);
    } catch (FileNotFoundException fnf) {
      // If the partition directory isn't found, this is treated as having no
      // files.
      fileDescriptors_ = ImmutableList.of();
      return;
    } catch (IOException ioe) {
      throw new LocalCatalogException(String.format(
          "Could not load files for partition %s of table %s",
          spec_.getName(), table_.getFullName()), ioe);
    }

    HdfsTable.FileMetadataLoadStats loadStats =
        new HdfsTable.FileMetadataLoadStats(partDir);

    try {
      FileSystem fs = partDir.getFileSystem(CONF);
      fileDescriptors_ = ImmutableList.copyOf(
          HdfsTable.createFileDescriptors(fs, new FakeRemoteIterator<>(stats),
              table_.getHostIndex(), loadStats));
    } catch (IOException e) {
        throw new LocalCatalogException(String.format(
            "Could not convert files to descriptors for partition %s of table %s",
            spec_.getName(), table_.getFullName()), e);
    }
  }

  /**
   * Wrapper for a normal Iterable<T> to appear like a Hadoop RemoteIterator<T>.
   * This is necessary because the existing code to convert file statuses to
   * descriptors consumes the remote iterator directly and thus avoids materializing
   * all of the LocatedFileStatus objects in memory at the same time.
   */
  private static class FakeRemoteIterator<T> implements RemoteIterator<T> {
    private final Iterator<T> it_;

    FakeRemoteIterator(Iterable<T> it) {
      this.it_ = it.iterator();
    }
    @Override
    public boolean hasNext() throws IOException {
      return it_.hasNext();
    }

    @Override
    public T next() throws IOException {
      return it_.next();
    }
  }
}
