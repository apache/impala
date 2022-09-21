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

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.fb.FbIcebergDataFileFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Scan of a single iceberg table.
 */
public class IcebergScanNode extends HdfsScanNode {

  private List<FileDescriptor> fileDescs_;

  public IcebergScanNode(PlanNodeId id, TableRef tblRef, List<Expr> conjuncts,
      MultiAggregateInfo aggInfo, List<FileDescriptor> fileDescs)
      throws ImpalaRuntimeException {
    super(id, tblRef.getDesc(), conjuncts,
        getIcebergPartition(((FeIcebergTable)tblRef.getTable()).getFeFsTable()), tblRef,
        aggInfo, null, false);
    // Hdfs table transformed from iceberg table only has one partition
    Preconditions.checkState(partitions_.size() == 1);

    fileDescs_ = fileDescs;
    //TODO IMPALA-11577: optimize file format counting
    boolean hasParquet = false;
    boolean hasOrc = false;
    boolean hasAvro = false;
    for (FileDescriptor fileDesc : fileDescs_) {
      byte fileFormat = fileDesc.getFbFileMetadata().icebergMetadata().fileFormat();
      if (fileFormat == FbIcebergDataFileFormat.PARQUET) {
        hasParquet = true;
      } else if (fileFormat == FbIcebergDataFileFormat.ORC) {
        hasOrc = true;
      } else if (fileFormat == FbIcebergDataFileFormat.AVRO) {
        hasAvro = true;
      } else {
        throw new ImpalaRuntimeException(String.format(
            "Invalid Iceberg file format of file: %s", fileDesc.getAbsolutePath()));
      }
    }
    if (hasParquet) fileFormats_.add(HdfsFileFormat.PARQUET);
    if (hasOrc) fileFormats_.add(HdfsFileFormat.ORC);

    //TODO IMPALA-11708: Currently mixed file format Iceberg tables containing AVRO files
    // cannot be read.
    if (hasAvro) {
      fileFormats_.add(HdfsFileFormat.AVRO);
      if (hasOrc || hasParquet) {
        throw new ImpalaRuntimeException("Iceberg tables containing multiple file "
            + "formats are only supported if they do not contain AVRO files.");
      }
    }
  }

  /**
   * In some cases we exactly know the cardinality, e.g. POSITION DELETE scan node.
   */
  public void setCardinality(long cardinality) {
    cardinality_ = cardinality;
  }

  /**
   * Get partition info from FeFsTable, we treat iceberg table as an
   * unpartitioned hdfs table
   */
  private static List<? extends FeFsPartition> getIcebergPartition(FeFsTable feFsTable) {
    Collection<? extends FeFsPartition> partitions =
        FeCatalogUtils.loadAllPartitions(feFsTable);
    return new ArrayList<>(partitions);
  }

  @Override
  protected List<FileDescriptor> getFileDescriptorsWithLimit(
      FeFsPartition partition, boolean fsHasBlocks, long limit) {
    if (limit != -1) {
      long cnt = 0;
      List<FileDescriptor> ret = new ArrayList<>();
      for (FileDescriptor fd : fileDescs_) {
        if (cnt == limit) break;
        ret.add(fd);
        ++cnt;
      }
      return ret;
    } else {
      return fileDescs_;
    }
  }

  /**
   * Returns a sample of file descriptors associated to this scan node.
   * The algorithm is based on FeFsTable.Utils.getFilesSample()
   */
  @Override
  protected Map<SampledPartitionMetadata, List<FileDescriptor>> getFilesSample(
      long percentBytes, long minSampleBytes, long randomSeed) {
    Preconditions.checkState(percentBytes >= 0 && percentBytes <= 100);
    Preconditions.checkState(minSampleBytes >= 0);

    // Ensure a consistent ordering of files for repeatable runs.
    List<FileDescriptor> orderedFds = Lists.newArrayList(fileDescs_);
    Collections.sort(orderedFds);

    Preconditions.checkState(partitions_.size() == 1);
    FeFsPartition part = partitions_.get(0);
    SampledPartitionMetadata sampledPartitionMetadata =
        new SampledPartitionMetadata(part.getId(), part.getFsType());

    long totalBytes = 0;
    for (FileDescriptor fd : orderedFds) {
      totalBytes += fd.getFileLength();
    }

    int numFilesRemaining = orderedFds.size();
    double fracPercentBytes = (double) percentBytes / 100;
    long targetBytes = (long) Math.round(totalBytes * fracPercentBytes);
    targetBytes = Math.max(targetBytes, minSampleBytes);

    // Randomly select files until targetBytes has been reached or all files have been
    // selected.
    Random rnd = new Random(randomSeed);
    long selectedBytes = 0;
    List<FileDescriptor> sampleFiles = Lists.newArrayList();
    while (selectedBytes < targetBytes && numFilesRemaining > 0) {
      int selectedIdx = rnd.nextInt(numFilesRemaining);
      FileDescriptor fd = orderedFds.get(selectedIdx);
      sampleFiles.add(fd);
      selectedBytes += fd.getFileLength();
      // Avoid selecting the same file multiple times.
      orderedFds.set(selectedIdx, orderedFds.get(numFilesRemaining - 1));
      --numFilesRemaining;
    }
    Map<SampledPartitionMetadata, List<FileDescriptor>> result = new HashMap<>();
    result.put(sampledPartitionMetadata, sampleFiles);
    return result;
  }
}
