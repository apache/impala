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

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.fb.FbIcebergDataFileFormat;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Scan of a single iceberg table.
 */
public class IcebergScanNode extends HdfsScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(IcebergScanNode.class);

  // List of files needed to be scanned by this scan node. The list is sorted in case of
  // partitioned tables, so partition range scans are scheduled more evenly.
  // See IMPALA-12765 for details.
  private List<FileDescriptor> fileDescs_;

  // Indicates that the files in 'fileDescs_' are sorted.
  private boolean filesAreSorted_ = false;

  // Conjuncts on columns not involved in IDENTITY-partitioning. Subset of 'conjuncts_',
  // but this does not include conjuncts on IDENTITY-partitioned columns, because such
  // conjuncts have already been pushed to Iceberg to filter out partitions/files, so
  // they don't have further selectivity on the surviving files.
  private List<Expr> nonIdentityConjuncts_;

  // Conjuncts that will be skipped from pushing down to the scan node because Iceberg
  // already applied them and they won't filter any further rows.
  private List<Expr> skippedConjuncts_;

  // The Iceberg snapshot id used for this scan.
  private final long snapshotId_;

  // This member is set when this scan node is the left child of an IcebergDeleteNode or
  // in other words when this scan node reads data files that have delete files
  // associated. Holds the scan node ID of the right child of the IcebergDeleteNode
  // responsible for reading the delete files of the corresponding table.
  private final PlanNodeId deleteFileScanNodeId;

  public IcebergScanNode(PlanNodeId id, TableRef tblRef, List<Expr> conjuncts,
      MultiAggregateInfo aggInfo, List<FileDescriptor> fileDescs,
      List<Expr> nonIdentityConjuncts, List<Expr> skippedConjuncts, long snapshotId)
      throws ImpalaRuntimeException {
    this(id, tblRef, conjuncts, aggInfo, fileDescs, nonIdentityConjuncts,
        skippedConjuncts, null, snapshotId);
  }

  public IcebergScanNode(PlanNodeId id, TableRef tblRef, List<Expr> conjuncts,
      MultiAggregateInfo aggInfo, List<FileDescriptor> fileDescs,
      List<Expr> nonIdentityConjuncts, List<Expr> skippedConjuncts, PlanNodeId deleteId,
      long snapshotId)
      throws ImpalaRuntimeException {
    super(id, tblRef.getDesc(), conjuncts,
        getIcebergPartition(((FeIcebergTable)tblRef.getTable()).getFeFsTable()), tblRef,
        aggInfo, null, false);
    // Hdfs table transformed from iceberg table only has one partition
    Preconditions.checkState(partitions_.size() == 1);

    fileDescs_ = fileDescs;
    if (((FeIcebergTable)tblRef.getTable()).isPartitioned()) {
      // Let's order the file descriptors for better scheduling.
      // See IMPALA-12765 for details.
      // Create a clone of the original file descriptor list to avoid getting
      // ConcurrentModificationException when sorting.
      fileDescs_ = new ArrayList<>(fileDescs_);
      Collections.sort(fileDescs_);
      filesAreSorted_ = true;
    }
    nonIdentityConjuncts_ = nonIdentityConjuncts;
    snapshotId_ = snapshotId;
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
    if (hasAvro) fileFormats_.add(HdfsFileFormat.AVRO);
    this.skippedConjuncts_ = skippedConjuncts;
    this.deleteFileScanNodeId = deleteId;
  }

  /**
   * Computes cardinalities of the Iceberg scan node. Implemented based on
   * HdfsScanNode.computeCardinalities with some modifications:
   *   - we exactly know the record counts of the data files
   *   - IDENTITY-based partition conjuncts already filtered out the files, so
   *     we don't need their selectivity
   */
  @Override
  protected void computeCardinalities(Analyzer analyzer) {
    cardinality_ = 0;

    if (sampledFiles_ != null) {
      for (List<FileDescriptor> sampledFileDescs : sampledFiles_.values()) {
        for (FileDescriptor fd : sampledFileDescs) {
          cardinality_ += fd.getFbFileMetadata().icebergMetadata().recordCount();
        }
      }
    } else {
      for (FileDescriptor fd : fileDescs_) {
        cardinality_ += fd.getFbFileMetadata().icebergMetadata().recordCount();
      }
    }

    // Adjust cardinality for all collections referenced along the tuple's path.
    for (Type t: desc_.getPath().getMatchedTypes()) {
      if (t.isCollectionType()) cardinality_ *= PlannerContext.AVG_COLLECTION_SIZE;
    }
    inputCardinality_ = cardinality_;

    if (cardinality_ > 0) {
      double selectivity = computeCombinedSelectivity(nonIdentityConjuncts_);
      if (LOG.isTraceEnabled()) {
        LOG.trace("cardinality_=" + Long.toString(cardinality_) +
                  " sel=" + Double.toString(selectivity));
      }
      cardinality_ = applySelectivity(cardinality_, selectivity);
    }

    cardinality_ = capCardinalityAtLimit(cardinality_);

    if (countStarSlot_ != null) {
      // We are doing optimized count star. Override cardinality with total num files.
      inputCardinality_ = fileDescs_.size();
      cardinality_ = fileDescs_.size();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("IcebergScanNode: cardinality_=" + Long.toString(cardinality_));
    }
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
  protected Map<Long, List<FileDescriptor>> getFilesSample(
      long percentBytes, long minSampleBytes, long randomSeed) {
    Preconditions.checkState(percentBytes >= 0 && percentBytes <= 100);
    Preconditions.checkState(minSampleBytes >= 0);

    // Ensure a consistent ordering of files for repeatable runs.
    List<FileDescriptor> orderedFds = Lists.newArrayList(fileDescs_);
    if (!filesAreSorted_) {
      Collections.sort(orderedFds);
    }

    Preconditions.checkState(partitions_.size() == 1);
    FeFsPartition part = partitions_.get(0);

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
    Map<Long, List<FileDescriptor>> result = new HashMap<>();
    result.put(part.getId(), sampleFiles);
    return result;
  }

  @Override
  protected void toThrift(TPlanNode msg, ThriftSerializationCtx serialCtx) {
    super.toThrift(msg, serialCtx);
    Preconditions.checkNotNull(msg.hdfs_scan_node);
    if (deleteFileScanNodeId != null) {
      msg.hdfs_scan_node.setDeleteFileScanNodeId(deleteFileScanNodeId.asInt());
    }
  }

  @Override
  protected String getDerivedExplainString(
      String indentPrefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(
        indentPrefix + "Iceberg snapshot id: " + String.valueOf(snapshotId_) + "\n");
    if (!skippedConjuncts_.isEmpty()) {
      output.append(indentPrefix +
          String.format("skipped Iceberg predicates: %s\n",
              Expr.getExplainString(skippedConjuncts_, detailLevel)));
    }
    return output.toString();
  }
}
