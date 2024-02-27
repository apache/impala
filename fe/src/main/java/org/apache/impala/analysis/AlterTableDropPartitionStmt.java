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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IcebergPartitionPredicateConverter;
import org.apache.impala.common.IcebergPredicateConverter;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
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

  // File paths selected by Iceberg's partition filtering
  private List<String> icebergFilePaths_;

  // Statistics for selected Iceberg partitions
  private long numberOfIcebergPartitions_;

  // If every partition is selected by Iceberg's partition filtering, this flag signals
  // that a truncate should be executed instead of deleting every file from the metadata.
  private boolean isIcebergTruncate_ = false;

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
      request.setIs_truncate(isIcebergTruncate_);
      if (isIcebergTruncate_) {
        request.setPaths(Collections.emptyList());
      } else {
        request.setPaths(icebergFilePaths_);
      }
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
    // To rewrite transforms and column references
    IcebergPartitionExpressionRewriter rewriter =
        new IcebergPartitionExpressionRewriter(analyzer,
            table.getIcebergApiTable().spec());
    // For Impala expression to Iceberg expression conversion
    IcebergPredicateConverter converter =
        new IcebergPartitionPredicateConverter(table.getIcebergSchema(), analyzer);

    List<Expression> icebergPartitionExprs = new ArrayList<>();
    for (Expr expr : partitionSet_.getPartitionExprs()) {
      expr = rewriter.rewrite(expr);
      expr.analyze(analyzer);
      analyzer.getConstantFolder().rewrite(expr, analyzer);
      try {
        icebergPartitionExprs.add(converter.convert(expr));
      } catch (ImpalaException e) {
        throw new AnalysisException(
            "Invalid partition filtering expression: " + expr.toSql());
      }
    }

    try (CloseableIterable<FileScanTask> fileScanTasks = IcebergUtil.planFiles(table,
        icebergPartitionExprs, null)) {
      icebergFilePaths_ = new ArrayList<>();
      Set<String> icebergPartitionSummary = new HashSet<>();
      for (FileScanTask fileScanTask : fileScanTasks) {
        if (fileScanTask.residual().isEquivalentTo(Expressions.alwaysTrue())) {
          icebergPartitionSummary.add(fileScanTask.file().partition().toString());
          List<DeleteFile> deleteFiles = fileScanTask.deletes();
          if (!deleteFiles.isEmpty()) {
            icebergFilePaths_.addAll(deleteFiles.stream()
                .map(deleteFile -> deleteFile.path().toString()).collect(
                    Collectors.toSet()));
          }
          icebergFilePaths_.add(fileScanTask.file().path().toString());
        }
      }
      numberOfIcebergPartitions_ = icebergPartitionSummary.size();
      if (icebergFilePaths_.size() == FeIcebergTable.Utils.getTotalNumberOfFiles(table,
          null)) {
        isIcebergTruncate_ = true;
        icebergFilePaths_ = Collections.emptyList();
      }
    } catch (IOException | TableLoadingException | ImpalaRuntimeException e) {
      throw new AnalysisException("Error loading metadata for Iceberg table", e);
    }
    if (numberOfIcebergPartitions_ == 0 && !ifExists_) {
      throw new AnalysisException(
          "No matching partition(s) found");
    }
  }
}
