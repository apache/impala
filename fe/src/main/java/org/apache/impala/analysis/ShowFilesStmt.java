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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.paimon.FeShowFileStmtSupport;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IcebergPartitionPredicateConverter;
import org.apache.impala.common.IcebergPredicateConverter;
import org.apache.impala.common.ImpalaException;

import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW FILES statement.
 * Acceptable syntax:
 *
 * SHOW FILES IN [dbName.]tableName [PARTITION(key=value,...)]
 *
 */
public class ShowFilesStmt extends StatementBase implements SingleTableStmt {
  private final TableName tableName_;

  // Show files for all the partitions if this is null.
  private final PartitionSet partitionSet_;

  // Set during analysis.
  protected FeTable table_;

  // File paths selected by Iceberg's partition filtering
  private List<String> icebergFilePaths_;

  public ShowFilesStmt(TableName tableName, PartitionSet partitionSet) {
    tableName_ = Preconditions.checkNotNull(tableName);
    partitionSet_ = partitionSet;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("SHOW FILES IN " + tableName_.toString());
    if (partitionSet_ != null) strBuilder.append(" " + partitionSet_.toSql(options));
    return strBuilder.toString();
  }

  @Override
  public TableName getTableName() { return tableName_; }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Resolve and analyze table ref to register privilege and audit events
    // and to allow us to evaluate partition predicates.
    TableRef tableRef = new TableRef(tableName_.toPath(), null, Privilege.VIEW_METADATA);
    tableRef = analyzer.resolveTableRef(tableRef);
    if (tableRef instanceof InlineViewRef) {
      throw new AnalysisException(String.format(
          "SHOW FILES not allowed on a view: %s", tableName_));
    }
    if (tableRef instanceof CollectionTableRef) {
      throw new AnalysisException(String.format(
          "SHOW FILES not allowed on a nested collection: %s", tableName_));
    }
    table_ = tableRef.getTable();
    Preconditions.checkNotNull(table_);
    if (!(table_ instanceof FeFsTable) && !(table_ instanceof FeShowFileStmtSupport)) {
      throw new AnalysisException("SHOW FILES is applicable only to file-based tables.");
    }
    tableRef.analyze(analyzer);

    // Analyze the partition spec, if one was specified.
    if (partitionSet_ != null) {
        if (table_ instanceof FeShowFileStmtSupport) {
            FeShowFileStmtSupport showFileStmtSupport = (FeShowFileStmtSupport) table_;
            if (!showFileStmtSupport.supportPartitionFilter()) {
                throw new AnalysisException(
                        "SHOW FILES with partition filter is not applicable to" +
                        " table type:" +
                        showFileStmtSupport.getTableFormat().name()
                );
            }
        }
      partitionSet_.setTableName(table_.getTableName());
      partitionSet_.setPartitionShouldExist();
      partitionSet_.setPrivilegeRequirement(Privilege.VIEW_METADATA);
      partitionSet_.analyze(analyzer);
    }

    if (table_ instanceof FeIcebergTable) { analyzeIceberg(analyzer); }
  }

  public void analyzeIceberg(Analyzer analyzer) throws AnalysisException {
    if (partitionSet_ == null) {
      icebergFilePaths_ = null;
      return;
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
        icebergPartitionExprs, null, null)) {
      // Collect file paths without sorting - sorting will be done in execution phase
      List<String> filePaths = new ArrayList<>();
      Set<String> uniquePaths = new HashSet<>();

      for (FileScanTask fileScanTask : fileScanTasks) {
        if (fileScanTask.residual().isEquivalentTo(Expressions.alwaysTrue())) {
          // Add delete files
          for (DeleteFile deleteFile : fileScanTask.deletes()) {
            String path = deleteFile.path().toString();
            if (uniquePaths.add(path)) {
              filePaths.add(path);
            }
          }

          // Add data file
          String dataFilePath = fileScanTask.file().path().toString();
          if (uniquePaths.add(dataFilePath)) {
            filePaths.add(dataFilePath);
          }
        }
      }

      // Store unsorted file paths - lexicographic sorting will be applied in execution
      icebergFilePaths_ = filePaths;

      if (icebergFilePaths_.isEmpty()) {
        throw new AnalysisException("No matching partition(s) found.");
      }
    } catch (IOException | TableLoadingException e) {
      throw new AnalysisException("Error loading metadata for Iceberg table", e);
    }
  }

  public TShowFilesParams toThrift() {
    TShowFilesParams params = new TShowFilesParams();
    params.setTable_name(new TTableName(table_.getDb().getName(), table_.getName()));
    // For Iceberg tables, we use selected_files instead of partition_set
    if (partitionSet_ != null && !(table_ instanceof FeIcebergTable)) {
      params.setPartition_set(partitionSet_.toThrift());
    }
    // For Iceberg tables: icebergFilePaths_ is null for unfiltered queries (no PARTITION
    // clause) or contains the list of matching file paths for filtered queries.
    if (table_ instanceof FeIcebergTable && icebergFilePaths_ != null) {
      params.setSelected_files(icebergFilePaths_);
    }
    return params;
  }
}
