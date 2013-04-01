// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.util.EnumSet;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.service.DdlExecutor;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Represents a CREATE TABLE AS SELECT (CTAS) statement
 */
public class CreateTableAsSelectStmt extends StatementBase {
  private final CreateTableStmt createStmt_;
  private final InsertStmt insertStmt_;
  private final static EnumSet<FileFormat> SUPPORTED_INSERT_FORMATS =
      EnumSet.of(FileFormat.PARQUETFILE, FileFormat.TEXTFILE);

  /**
   * Builds a CREATE TABLE AS SELECT statement
   */
  public CreateTableAsSelectStmt(CreateTableStmt createStmt, QueryStmt queryStmt) {
    Preconditions.checkNotNull(queryStmt);
    Preconditions.checkNotNull(createStmt);
    this.createStmt_ = createStmt;
    this.insertStmt_ = new InsertStmt(null, createStmt.getTblName(), false,
        null, queryStmt, null);
  }

  public QueryStmt getQueryStmt() { return insertStmt_.getQueryStmt(); }
  public InsertStmt getInsertStmt() { return insertStmt_; }
  public CreateTableStmt getCreateStmt() { return createStmt_; }
  @Override
  public String debugString() { return toSql(); }
  @Override
  public String toSql() { return createStmt_.toSql() + " AS " + getQueryStmt().toSql(); }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {

    // The analysis for CTAS happens in two phases - the first phase happens before
    // the target table exists and we want to validate the CREATE statement and the
    // query portion of the insert statement. If this passes, analysis will be run
    // over the full INSERT statement. To avoid duplicate registrations of table/colRefs,
    // create a new analyzer and clone the query statement for this initial pass.
    QueryStmt tmpQueryStmt = insertStmt_.getQueryStmt().clone();
    Analyzer tmpAnalyzer = new Analyzer(analyzer, analyzer.getUser());
    tmpAnalyzer.setUseHiveColLabels(true);
    tmpQueryStmt.analyze(tmpAnalyzer);

    // Add the columns from the select statement to the create statement.
    int colCnt = tmpQueryStmt.getColLabels().size();
    for (int i = 0; i < colCnt; ++i) {
      createStmt_.getColumnDefs().add(new ColumnDef(
          tmpQueryStmt.getColLabels().get(i),
          tmpQueryStmt.getResultExprs().get(i).getType(), null));
    }
    createStmt_.analyze(analyzer);

    if (!SUPPORTED_INSERT_FORMATS.contains(createStmt_.getFileFormat())) {
      throw new AnalysisException(String.format("CREATE TABLE AS SELECT " +
          "does not support (%s) file format. Supported formats are: (%s)",
          createStmt_.getFileFormat(), Joiner.on(", ").join(SUPPORTED_INSERT_FORMATS)));
    }

    // The full privilege check for the database will be done as part of the INSERT
    // analysis.
    Db db = analyzer.getCatalog().getDb(createStmt_.getDb(),
        analyzer.getUser(), Privilege.ANY);
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
        DdlExecutor.createMetaStoreTable(createStmt_.toThrift());

    MetaStoreClient client = analyzer.getCatalog().getMetaStoreClient();
    try {
      // Set a valid location of this table using the same rules as the metastore. If the
      // user specified a location for the table this will be a no-op.
      msTbl.getSd().setLocation(analyzer.getCatalog().getTablePath(msTbl).toString());

      // If the user didn't specify a table location for the CREATE statement, inject the
      // location that was calculated in the getTablePath() call. Since this will be the
      // target location for the INSERT statement, it is important the two match.
      if (createStmt_.getLocation() == null) {
        createStmt_.setLocation(new HdfsURI(msTbl.getSd().getLocation()));
      }

      // Create a "temp" table based off the given metastore.api.Table object.
      Table table = Table.fromMetastoreTable(analyzer.getCatalog().getNextTableId(),
          client.getHiveClient(), db, msTbl);
      Preconditions.checkState(table != null && table instanceof HdfsTable);

      HdfsTable hdfsTable = (HdfsTable) table;
      hdfsTable.load(hdfsTable, client.getHiveClient(), msTbl);
      insertStmt_.setTargetTable(table);
    } catch (TableLoadingException e) {
      throw new AnalysisException(e.getMessage(), e);
    } catch (Exception e) {
      throw new AnalysisException(e.getMessage(), e);
    } finally {
      client.release();
    }

    // Finally, run analysis on the insert statement.
    insertStmt_.analyze(analyzer);
  }
}