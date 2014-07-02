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

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.log4j.Logger;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TComputeStatsParams;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents an COMPUTE STATS <table> statement for statistics collection. The
 * statement gathers all table and column stats for a given table and stores them in
 * the Metastore via the CatalogService. All existing stats for that table are replaced
 * and no existing stats are reused.
 *
 * TODO: Allow more coarse/fine grained (db, column) and/or incremental stats collection.
 */
public class ComputeStatsStmt extends StatementBase {
  private static final Logger LOG = Logger.getLogger(ComputeStatsStmt.class);

  private static String AVRO_SCHEMA_MSG_PREFIX = "Cannot COMPUTE STATS on Avro table " +
      "'%s' because its column definitions do not match those in the Avro schema.";
  private static String AVRO_SCHEMA_MSG_SUFFIX = "Please re-create the table with " +
          "column definitions, e.g., using the result of 'SHOW CREATE TABLE'";

  protected final TableName tableName_;

  // Set during analysis.
  protected Table table_;

  // The Null count is not currently being used in optimization or run-time,
  // and compute stats runs 2x faster in many cases when not counting NULLs.
  private static final boolean COUNT_NULLS = false;

  // Query for getting the per-partition row count and the total row count.
  // Set during analysis.
  protected String tableStatsQueryStr_;

  // Query for getting the per-column NDVs and number of NULLs.
  // Set during analysis.
  protected String columnStatsQueryStr_;

  protected ComputeStatsStmt(TableName tableName) {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    this.tableName_ = tableName;
    this.table_ = null;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    table_ = analyzer.getTable(tableName_, Privilege.ALTER);
    String sqlTableName = table_.getTableName().toSql();
    if (table_ instanceof View) {
      throw new AnalysisException(String.format(
          "COMPUTE STATS not allowed on a view: %s", sqlTableName));
    }

    // Query for getting the per-partition row count and the total row count.
    StringBuilder tableStatsQueryBuilder = new StringBuilder("SELECT ");
    List<String> tableStatsSelectList = Lists.newArrayList();
    tableStatsSelectList.add("COUNT(*)");
    List<String> groupByCols = Lists.newArrayList();
    // Only add group by clause for HdfsTables.
    if (table_ instanceof HdfsTable) {
      HdfsTable hdfsTable = (HdfsTable) table_;
      if (hdfsTable.isAvroTable()) checkIncompleteAvroSchema(hdfsTable);
      for (int i = 0; i < table_.getNumClusteringCols(); ++i) {
        String colRefSql = ToSqlUtils.getIdentSql(table_.getColumns().get(i).getName());
        groupByCols.add(colRefSql);
        // For the select list, wrap the group by columns in a cast to string because
        // the Metastore stores them as strings.
        tableStatsSelectList.add("CAST(" + colRefSql + " AS STRING)");
      }
    }
    tableStatsQueryBuilder.append(Joiner.on(", ").join(tableStatsSelectList));
    tableStatsQueryBuilder.append(" FROM " + sqlTableName);
    if (!groupByCols.isEmpty()) {
      tableStatsQueryBuilder.append(" GROUP BY ");
      tableStatsQueryBuilder.append(Joiner.on(", ").join(groupByCols));
    }
    tableStatsQueryStr_ = tableStatsQueryBuilder.toString();
    LOG.debug(tableStatsQueryStr_);

    // Query for getting the per-column NDVs and number of NULLs.
    StringBuilder columnStatsQueryBuilder = new StringBuilder("SELECT ");
    List<String> columnStatsSelectList = Lists.newArrayList();
    // For Hdfs tables, exclude partition columns from stats gathering because Hive
    // cannot store them as part of the non-partition column stats. For HBase tables,
    // include the single clustering column (the row key).
    int startColIdx = (table_ instanceof HBaseTable) ? 0 : table_.getNumClusteringCols();
    for (int i = startColIdx; i < table_.getColumns().size(); ++i) {
      Column c = table_.getColumns().get(i);
      ColumnType ctype = c.getType();

      // Ignore columns with an invalid/unsupported type. For example, complex types in
      // an HBase-backed table will appear as invalid types.
      if (!ctype.isValid() || !ctype.isSupported()) continue;
      // NDV approximation function. Add explicit alias for later identification when
      // updating the Metastore.
      String colRefSql = ToSqlUtils.getIdentSql(c.getName());
      columnStatsSelectList.add("NDV(" + colRefSql + ") AS " + colRefSql);

      if (COUNT_NULLS) {
        // Count the number of NULL values.
        columnStatsSelectList.add("COUNT(IF(" + colRefSql + " IS NULL, 1, NULL))");
      } else {
        // Using -1 to indicate "unknown". We need cast to BIGINT because backend expects
        // an i64Val as the number of NULLs returned by the COMPUTE STATS column stats
        // child query. See CatalogOpExecutor::SetColumnStats(). If we do not cast, then
        // the -1 will be treated as TINYINT resulting a 0 to be placed in the #NULLs
        // column (see IMPALA-1068).
        columnStatsSelectList.add("CAST(-1 as BIGINT)");
      }

      // For STRING columns also compute the max and avg string length.
      if (ctype.isStringType()) {
        columnStatsSelectList.add("MAX(length(" + colRefSql + "))");
        columnStatsSelectList.add("AVG(length(" + colRefSql + "))");
      } else {
        // For non-STRING columns we use the fixed size of the type.
        // We store the same information for all types to avoid having to
        // treat STRING columns specially in the BE CatalogOpExecutor.
        Integer typeSize = ctype.getPrimitiveType().getSlotSize();
        columnStatsSelectList.add(typeSize.toString());
        columnStatsSelectList.add("CAST(" + typeSize.toString() + " as DOUBLE)");
      }
    }

    if (columnStatsSelectList.size() == 0) {
      // Table doesn't have any columns that we can compute stats for
      columnStatsQueryStr_ = null;
      return;
    }

    columnStatsQueryBuilder.append(Joiner.on(", ").join(columnStatsSelectList));
    columnStatsQueryBuilder.append(" FROM " + sqlTableName);
    columnStatsQueryStr_ = columnStatsQueryBuilder.toString();
    LOG.debug(columnStatsQueryStr_);
  }

  /**
   * Checks whether the column definitions from the CREATE TABLE stmt match the columns
   * in the Avro schema. If there is a mismatch, then COMPUTE STATS cannot update the
   * statistics in the Metastore's backend DB due to HIVE-6308. Throws an
   * AnalysisException for such ill-created Avro tables. Does nothing if
   * the column definitions match the Avro schema exactly.
   */
  private void checkIncompleteAvroSchema(HdfsTable table) throws AnalysisException {
    Preconditions.checkState(table.isAvroTable());
    org.apache.hadoop.hive.metastore.api.Table msTable = table.getMetaStoreTable();
    // The column definitions from 'CREATE TABLE (column definitions) ...'
    Iterator<FieldSchema> colDefs = msTable.getSd().getCols().iterator();
    // The columns derived from the Avro schema file or literal schema.
    // Inconsistencies between the Avro-schema columns and the column definitions
    // are sometimes resolved in the CREATE TABLE, and sometimes not (see below).
    Iterator<Column> avroSchemaCols = table.getColumns().iterator();
    // Skip partition columns from 'table' since those are not present in
    // the msTable field schemas.
    for (int i = 0; i < table.getNumClusteringCols(); ++i) {
      if (avroSchemaCols.hasNext()) avroSchemaCols.next();
    }
    int pos = 0;
    while (colDefs.hasNext() || avroSchemaCols.hasNext()) {
      if (colDefs.hasNext() && avroSchemaCols.hasNext()) {
        FieldSchema colDef = colDefs.next();
        Column avroSchemaCol = avroSchemaCols.next();
        // Check that the column names are identical. Ignore mismatched types
        // as those will either fail in the scan or succeed.
        if (!colDef.getName().equalsIgnoreCase(avroSchemaCol.getName())) {
          throw new AnalysisException(
              String.format(AVRO_SCHEMA_MSG_PREFIX +
                  "\nDefinition of column '%s' of type '%s' does not match " +
                  "the Avro-schema column '%s' of type '%s' at position '%s'.\n" +
                  AVRO_SCHEMA_MSG_SUFFIX,
                  table.getName(), colDef.getName(), colDef.getType(),
                  avroSchemaCol.getName(), avroSchemaCol.getType(), pos));
        }
      }
      // The following two cases are typically not possible because Hive resolves
      // inconsistencies between the column-definition list and the Avro schema if a
      // column-definition list was given in the CREATE TABLE (having no column
      // definitions at all results in HIVE-6308). Even so, we check these cases for
      // extra safety. COMPUTE STATS could be made to succeed in special instances of
      // the cases below but we chose to throw an AnalysisException to avoid confusion
      // because this scenario "should" never arise as mentioned above.
      if (colDefs.hasNext() && !avroSchemaCols.hasNext()) {
        FieldSchema colDef = colDefs.next();
        throw new AnalysisException(
            String.format(AVRO_SCHEMA_MSG_PREFIX +
                "\nMissing Avro-schema column corresponding to column " +
                "definition '%s' of type '%s' at position '%s'.\n" +
                AVRO_SCHEMA_MSG_SUFFIX,
                table.getName(), colDef.getName(), colDef.getType(), pos));
      }
      if (!colDefs.hasNext() && avroSchemaCols.hasNext()) {
        Column avroSchemaCol = avroSchemaCols.next();
        throw new AnalysisException(
            String.format(AVRO_SCHEMA_MSG_PREFIX +
                "\nMissing column definition corresponding to Avro-schema " +
                "column '%s' of type '%s' at position '%s'.\n" +
                AVRO_SCHEMA_MSG_SUFFIX,
                table.getName(), avroSchemaCol.getName(), avroSchemaCol.getType(), pos));
      }
      ++pos;
    }
  }

  public String getTblStatsQuery() { return tableStatsQueryStr_; }
  public String getColStatsQuery() { return columnStatsQueryStr_; }

  @Override
  public String toSql() { return "COMPUTE STATS " + tableName_.toString(); }

  public TComputeStatsParams toThrift() {
    TComputeStatsParams params = new TComputeStatsParams();
    params.setTable_name(new TTableName(table_.getDb().getName(), table_.getName()));
    params.setTbl_stats_query(tableStatsQueryStr_);
    if (columnStatsQueryStr_ != null) {
      params.setCol_stats_query(columnStatsQueryStr_);
    } else {
      params.setCol_stats_queryIsSet(false);
    }
    return params;
  }
}
