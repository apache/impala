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
import java.util.List;

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;

import com.google.common.base.Preconditions;

/**
 * Scan of a single iceberg table.
 */
public class IcebergScanNode extends HdfsScanNode {

  private List<FileDescriptor> fileDescs_;

  public IcebergScanNode(PlanNodeId id, TableRef tblRef, List<Expr> conjuncts,
      MultiAggregateInfo aggInfo, List<FileDescriptor> fileDescs) {
    super(id, tblRef.getDesc(), conjuncts,
        getIcebergPartition(((FeIcebergTable)tblRef.getTable()).getFeFsTable()), tblRef,
        aggInfo, null, false);
    // Hdfs table transformed from iceberg table only has one partition
    Preconditions.checkState(partitions_.size() == 1);
    fileDescs_ = fileDescs;
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
}
