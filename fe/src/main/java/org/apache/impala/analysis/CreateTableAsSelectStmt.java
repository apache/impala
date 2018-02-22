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

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.THdfsFileFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a CREATE TABLE AS SELECT (CTAS) statement
 *
 * The statement supports an optional PARTITIONED BY clause. Its syntax and semantics
 * follow the PARTITION feature of INSERT FROM SELECT statements: inside the PARTITIONED
 * BY (...) column list the user must specify names of the columns to partition by. These
 * column names must appear in the specified order at the end of the select statement. A
 * remapping between columns of the source and destination tables is not possible, because
 * the destination table does not yet exist. Specifying static values for the partition
 * columns is also not possible, as their type needs to be deduced from columns in the
 * select statement.
 */
public class CreateTableAsSelectStmt extends StatementBase {
  // List of partition columns from the PARTITIONED BY (...) clause. Set to null if no
  // partition was given.
  private final List<String> partitionKeys_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  private final CreateTableStmt createStmt_;
  private final InsertStmt insertStmt_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  private final static EnumSet<THdfsFileFormat> SUPPORTED_INSERT_FORMATS =
      EnumSet.of(THdfsFileFormat.PARQUET, THdfsFileFormat.TEXT, THdfsFileFormat.KUDU);

  /**
   * Helper class for parsing.
   * Contains every parameter of the constructor with the exception of hints. This is
   * needed to keep the production rules that check for optional hints separate from the
   * rules that check for optional partition info. Merging these independent rules would
   * make it necessary to create rules for every combination of them.
   */
  public static class CtasParams {
    public CreateTableStmt createStmt;
    public QueryStmt queryStmt;
    public List<String> partitionKeys;

    public CtasParams(CreateTableStmt createStmt, QueryStmt queryStmt,
        List<String> partitionKeys) {
      this.createStmt = Preconditions.checkNotNull(createStmt);
      this.queryStmt = Preconditions.checkNotNull(queryStmt);
      this.partitionKeys = partitionKeys;
    }
  }

  /**
   * Builds a CREATE TABLE AS SELECT statement
   */
  public CreateTableAsSelectStmt(CtasParams params, List<PlanHint> planHints) {
    createStmt_ = params.createStmt;
    partitionKeys_ = params.partitionKeys;
    List<PartitionKeyValue> pkvs = null;
    if (partitionKeys_ != null) {
      pkvs = Lists.newArrayList();
      for (String key: partitionKeys_) {
        pkvs.add(new PartitionKeyValue(key, null));
      }
    }
    insertStmt_ = InsertStmt.createInsert(null, createStmt_.getTblName(), false, pkvs,
        planHints, null, params.queryStmt, null);
  }

  public QueryStmt getQueryStmt() { return insertStmt_.getQueryStmt(); }
  public InsertStmt getInsertStmt() { return insertStmt_; }
  public CreateTableStmt getCreateStmt() { return createStmt_; }
  @Override
  public String toSql() { return ToSqlUtils.getCreateTableSql(this); }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    createStmt_.collectTableRefs(tblRefs);
    insertStmt_.collectTableRefs(tblRefs);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);

    if (!SUPPORTED_INSERT_FORMATS.contains(createStmt_.getFileFormat())) {
      throw new AnalysisException(String.format("CREATE TABLE AS SELECT " +
          "does not support the (%s) file format. Supported formats are: (%s)",
          createStmt_.getFileFormat().toString().replace("_", ""),
          "PARQUET, TEXTFILE, KUDU"));
    }
    if (createStmt_.getFileFormat() == THdfsFileFormat.KUDU && createStmt_.isExternal()) {
      // TODO: Add support for CTAS on external Kudu tables (see IMPALA-4318)
      throw new AnalysisException(String.format("CREATE TABLE AS SELECT is not " +
          "supported for external Kudu tables."));
    }

    // The analysis for CTAS happens in two phases - the first phase happens before
    // the target table exists and we want to validate the CREATE statement and the
    // query portion of the insert statement. If this passes, analysis will be run
    // over the full INSERT statement. To avoid duplicate registrations of table/colRefs,
    // create a new root analyzer and clone the query statement for this initial pass.
    Analyzer dummyRootAnalyzer = new Analyzer(analyzer.getStmtTableCache(),
        analyzer.getQueryCtx(), analyzer.getAuthzConfig());
    QueryStmt tmpQueryStmt = insertStmt_.getQueryStmt().clone();
    Analyzer tmpAnalyzer = new Analyzer(dummyRootAnalyzer);
    tmpAnalyzer.setUseHiveColLabels(true);
    tmpQueryStmt.analyze(tmpAnalyzer);
    // Subqueries need to be rewritten by the StmtRewriter first.
    if (analyzer.containsSubquery()) return;

    // Add the columns from the partition clause to the create statement.
    if (partitionKeys_ != null) {
      int colCnt = tmpQueryStmt.getColLabels().size();
      int partColCnt = partitionKeys_.size();
      if (partColCnt >= colCnt) {
        throw new AnalysisException(String.format("Number of partition columns (%s) " +
            "must be smaller than the number of columns in the select statement (%s).",
            partColCnt, colCnt));
      }
      int firstCol = colCnt - partColCnt;
      for (int i = firstCol, j = 0; i < colCnt; ++i, ++j) {
        String partitionLabel = partitionKeys_.get(j);
        String colLabel = tmpQueryStmt.getColLabels().get(i);

        // Ensure that partition columns are named and positioned at end of
        // input column list.
        if (!partitionLabel.equals(colLabel)) {
          throw new AnalysisException(String.format("Partition column name " +
              "mismatch: %s != %s", partitionLabel, colLabel));
        }

        ColumnDef colDef = new ColumnDef(colLabel, null);
        colDef.setType(tmpQueryStmt.getBaseTblResultExprs().get(i).getType());
        createStmt_.getPartitionColumnDefs().add(colDef);
      }
      // Remove partition columns from table column list.
      tmpQueryStmt.getColLabels().subList(firstCol, colCnt).clear();
    }

    // Add the columns from the select statement to the create statement.
    int colCnt = tmpQueryStmt.getColLabels().size();
    for (int i = 0; i < colCnt; ++i) {
      ColumnDef colDef = new ColumnDef(tmpQueryStmt.getColLabels().get(i), null,
          Collections.<ColumnDef.Option, Object>emptyMap());
      colDef.setType(tmpQueryStmt.getBaseTblResultExprs().get(i).getType());
      createStmt_.getColumnDefs().add(colDef);
    }
    createStmt_.analyze(analyzer);


    // The full privilege check for the database will be done as part of the INSERT
    // analysis.
    Db db = analyzer.getDb(createStmt_.getDb(), Privilege.ANY);
    if (db == null) {
      throw new AnalysisException(
          Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + createStmt_.getDb());
    }

    // Running analysis on the INSERT portion of the CTAS requires the target INSERT
    // table to "exist". For CTAS the table does not exist yet, so create a "temp"
    // table to run analysis against. The schema of this temp table should exactly
    // match the schema of the table that will be created by running the CREATE
    // statement.
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        CatalogOpExecutor.createMetaStoreTable(createStmt_.toThrift());

    try (MetaStoreClient client = analyzer.getCatalog().getMetaStoreClient()) {
      // Set a valid location of this table using the same rules as the metastore. If the
      // user specified a location for the table this will be a no-op.
      msTbl.getSd().setLocation(analyzer.getCatalog().getTablePath(msTbl).toString());

      Table tmpTable = null;
      if (KuduTable.isKuduTable(msTbl)) {
        tmpTable = KuduTable.createCtasTarget(db, msTbl, createStmt_.getColumnDefs(),
            createStmt_.getPrimaryKeyColumnDefs(),
            createStmt_.getKuduPartitionParams());
      } else if (HdfsFileFormat.isHdfsInputFormatClass(msTbl.getSd().getInputFormat())) {
        tmpTable = HdfsTable.createCtasTarget(db, msTbl);
      }
      Preconditions.checkState(tmpTable != null &&
          (tmpTable instanceof HdfsTable || tmpTable instanceof KuduTable));

      insertStmt_.setTargetTable(tmpTable);
    } catch (Exception e) {
      throw new AnalysisException(e.getMessage(), e);
    }

    // Finally, run analysis on the insert statement.
    insertStmt_.analyze(analyzer);
  }

  @Override
  public List<Expr> getResultExprs() { return insertStmt_.getResultExprs(); }

  @Override
  public void castResultExprs(List<Type> types) throws AnalysisException {
    super.castResultExprs(types);
    // Set types of column definitions.
    List<ColumnDef> colDefs = createStmt_.getColumnDefs();
    List<ColumnDef> partitionColDefs = createStmt_.getPartitionColumnDefs();
    Preconditions.checkState(colDefs.size() + partitionColDefs.size() == types.size());
    for (int i = 0; i < colDefs.size(); ++i) colDefs.get(i).setType(types.get(i));
    for (int i = 0; i < partitionColDefs.size(); ++i) {
      partitionColDefs.get(i).setType(types.get(i + colDefs.size()));
    }
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    insertStmt_.rewriteExprs(rewriter);
  }

  @Override
  public void reset() {
    super.reset();
    createStmt_.reset();
    insertStmt_.reset();
  }
}
