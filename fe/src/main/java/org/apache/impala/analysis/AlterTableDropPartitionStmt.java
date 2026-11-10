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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableDropPartitionParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TIcebergDropPartitionRequest;
import org.apache.impala.util.IcebergUtil;

/**
 * Represents an ALTER TABLE DROP PARTITION statement.
 */
public class AlterTableDropPartitionStmt extends AlterTableStmt {
  private final boolean ifExists_;
  private final PartitionSet partitionSet_;

  // Setting this value causes dropped partition(s) to be permanently
  // deleted. For example, for HDFS tables it skips the trash mechanism
  private final boolean purgePartition_;

  // Data file paths selected by Iceberg's partition filtering. The scan tasks return
  // each data file once, so the paths are unique.
  private List<String> icebergFilePaths_;

  // Statistics for selected Iceberg partitions
  private long numberOfIcebergPartitions_;

  public AlterTableDropPartitionStmt(TableName tableName,
      PartitionSet partitionSet, boolean ifExists, boolean purgePartition) {
    super(tableName);
    Preconditions.checkNotNull(partitionSet);
    partitionSet_ = partitionSet;
    partitionSet_.setTableName(tableName);
    ifExists_ = ifExists;
    purgePartition_ = purgePartition;
  }

  public boolean getIfNotExists() { return ifExists_; }

  @Override
  public String getOperation() {
    StringBuilder sb = new StringBuilder("DROP ");
    if (ifExists_) sb.append("IF EXISTS ");
    sb.append("PARTITION");
    return sb.toString();
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("ALTER TABLE " + getTbl());
    sb.append(" DROP ");
    if (ifExists_) sb.append("IF EXISTS ");
    sb.append(partitionSet_.toSql(options));
    if (purgePartition_) sb.append(" PURGE");
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.DROP_PARTITION);
    TAlterTableDropPartitionParams dropPartParams = new TAlterTableDropPartitionParams();
    dropPartParams.setIf_exists(!partitionSet_.getPartitionShouldExist());
    dropPartParams.setPurge(purgePartition_);
    params.setDrop_partition_params(dropPartParams);
    if (table_ instanceof FeIcebergTable) {
      TIcebergDropPartitionRequest request = new TIcebergDropPartitionRequest();
      request.setPaths(icebergFilePaths_);
      request.num_partitions = numberOfIcebergPartitions_;
      dropPartParams.setIceberg_drop_partition_request(request);
      dropPartParams.setPartition_set(Collections.emptyList());
    } else {
      dropPartParams.setPartition_set(partitionSet_.toThrift());
    }
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable table = getTargetTable();
    if (table instanceof FeKuduTable) {
      throw new AnalysisException("ALTER TABLE DROP PARTITION is not supported for " +
          "Kudu tables: " + partitionSet_.toSql());
    }
    if (!ifExists_) partitionSet_.setPartitionShouldExist();
    partitionSet_.setPrivilegeRequirement(Privilege.ALTER);
    partitionSet_.analyze(analyzer);

    if (table instanceof FeIcebergTable) { analyzeIceberg(analyzer); }
  }

  public void analyzeIceberg(Analyzer analyzer) throws AnalysisException {
    if (purgePartition_) {
      throw new AnalysisException(
          "Partition purge is not supported for Iceberg tables");
    }

    FeIcebergTable table = (FeIcebergTable) table_;
    // Resolve the partition predicate(s) to the files that fully belong to the selected
    // partition(s).
    IcebergUtil.PartitionFilesResult result = IcebergUtil.planPartitionFiles(
        table, partitionSet_.getPartitionExprs(), analyzer);
    numberOfIcebergPartitions_ = result.numPartitions;
    // Only the data files are dropped, also if every partition is selected: truncating
    // the table instead would also drop the files written since the analysis. Iceberg's
    // DeleteFiles drops data files only, and it would fail to find delete files when it
    // validates that the files to drop still exist.
    icebergFilePaths_ = new ArrayList<>();
    for (FileScanTask fileScanTask : result.fileScanTasks) {
      icebergFilePaths_.add(fileScanTask.file().location());
    }
    if (numberOfIcebergPartitions_ == 0 && !ifExists_) {
      throw new AnalysisException(
          "No matching partition(s) found");
    }
  }
}
