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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.Token;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.parse.HiveLexer;

import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsCompression;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.View;
import org.apache.impala.util.KuduUtil;

/**
 * Contains utility methods for creating SQL strings, for example,
 * for creating identifier strings that are compatible with Hive or Impala.
 */
public class ToSqlUtils {
  // Table properties to hide when generating the toSql() statement
  // EXTERNAL, SORT BY, and comment are hidden because they are part of the toSql result,
  // e.g., "CREATE EXTERNAL TABLE <name> ... SORT BY (...) ... COMMENT <comment> ..."
  private static final ImmutableSet<String> HIDDEN_TABLE_PROPERTIES = ImmutableSet.of(
      "EXTERNAL", "comment", AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS);

  /**
   * Removes all hidden properties from the given 'tblProperties' map.
   */
  private static void removeHiddenTableProperties(Map<String, String> tblProperties) {
    for (String key: HIDDEN_TABLE_PROPERTIES) tblProperties.remove(key);
  }

  /**
   * Returns the list of sort columns from 'properties' or 'null' if 'properties' doesn't
   * contain 'sort.columns'.
   */
  private static List<String> getSortColumns(Map<String, String> properties) {
    String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
    if (!properties.containsKey(sortByKey)) return null;
    return Lists.newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().split(
        properties.get(sortByKey)));
  }

  /**
   * Returns a comma-delimited string of Kudu 'partition by' parameters on a
   * CreateTableStmt, or null if this isn't a CreateTableStmt for a Kudu table.
   */
  private static String getKuduPartitionByParams(CreateTableStmt stmt) {
    List<KuduPartitionParam> partitionParams = stmt.getKuduPartitionParams();
    Preconditions.checkNotNull(partitionParams);
    if (partitionParams.isEmpty()) return null;
    List<String> paramStrings = Lists.newArrayList();
    for (KuduPartitionParam p : partitionParams) {
      paramStrings.add(p.toSql());
    }
    return Joiner.on(", ").join(paramStrings);
  }

  /**
   * Given an unquoted identifier string, returns an identifier lexable by
   * Impala and Hive, possibly by enclosing the original identifier in "`" quotes.
   * For example, Hive cannot parse its own auto-generated column
   * names "_c0", "_c1" etc. unless they are quoted. Impala and Hive keywords
   * must also be quoted.
   *
   * Impala's lexer recognizes a superset of the unquoted identifiers that Hive can.
   * At the same time, Impala's and Hive's list of keywords differ.
   * This method always returns an identifier that Impala and Hive can recognize,
   * although for some identifiers the quotes may not be strictly necessary for
   * one or the other system.
   */
  public static String getIdentSql(String ident) {
    boolean hiveNeedsQuotes = true;
    HiveLexer hiveLexer = new HiveLexer(new ANTLRStringStream(ident));
    try {
      Token t = hiveLexer.nextToken();
      // Check that the lexer recognizes an identifier and then EOF.
      boolean identFound = t.getType() == HiveLexer.Identifier;
      t = hiveLexer.nextToken();
      // No enclosing quotes are necessary for Hive.
      hiveNeedsQuotes = !(identFound && t.getType() == HiveLexer.EOF);
    } catch (Exception e) {
      // Ignore exception and just quote the identifier to be safe.
    }
    boolean isImpalaKeyword = SqlScanner.isKeyword(ident.toUpperCase());
    // Impala's scanner recognizes the ".123" portion of "db.123_tbl" as a decimal,
    // so while the quoting is not necessary for the given identifier itself, the quotes
    // are needed if this identifier will be preceded by a ".".
    boolean startsWithNumber = false;
    if (!hiveNeedsQuotes && !isImpalaKeyword) {
      startsWithNumber = Character.isDigit(ident.charAt(0));
    }
    if (hiveNeedsQuotes || isImpalaKeyword || startsWithNumber) return "`" + ident + "`";
    return ident;
  }

  public static List<String> getIdentSqlList(List<String> identList) {
    List<String> identSqlList = Lists.newArrayList();
    for (String ident: identList) {
      identSqlList.add(getIdentSql(ident));
    }
    return identSqlList;
  }

  public static String getPathSql(List<String> path) {
    StringBuilder result = new StringBuilder();
    for (String p: path) {
      if (result.length() > 0) result.append(".");
      result.append(getIdentSql(p));
    }
    return result.toString();
  }

  /**
   * Returns the "CREATE TABLE" SQL string corresponding to the given CreateTableStmt
   * statement.
   */
  public static String getCreateTableSql(CreateTableStmt stmt) {
    ArrayList<String> colsSql = Lists.newArrayList();
    for (ColumnDef col: stmt.getColumnDefs()) {
      colsSql.add(col.toString());
    }
    ArrayList<String> partitionColsSql = Lists.newArrayList();
    for (ColumnDef col: stmt.getPartitionColumnDefs()) {
      partitionColsSql.add(col.toString());
    }
    String kuduParamsSql = getKuduPartitionByParams(stmt);
    // TODO: Pass the correct compression, if applicable.
    return getCreateTableSql(stmt.getDb(), stmt.getTbl(), stmt.getComment(), colsSql,
        partitionColsSql, stmt.getTblPrimaryKeyColumnNames(), kuduParamsSql,
        stmt.getSortColumns(), stmt.getTblProperties(), stmt.getSerdeProperties(),
        stmt.isExternal(), stmt.getIfNotExists(), stmt.getRowFormat(),
        HdfsFileFormat.fromThrift(stmt.getFileFormat()), HdfsCompression.NONE, null,
        stmt.getLocation());
  }

  /**
   * Returns the "CREATE TABLE" SQL string corresponding to the given
   * CreateTableAsSelectStmt statement.
   */
  public static String getCreateTableSql(CreateTableAsSelectStmt stmt) {
    CreateTableStmt innerStmt = stmt.getCreateStmt();
    // Only add partition column labels to output. Table columns must not be specified as
    // they are deduced from the select statement.
    ArrayList<String> partitionColsSql = Lists.newArrayList();
    for (ColumnDef col: innerStmt.getPartitionColumnDefs()) {
      partitionColsSql.add(col.getColName());
    }
    // Use a LinkedHashMap to preserve the ordering of the table properties.
    LinkedHashMap<String, String> properties =
        Maps.newLinkedHashMap(innerStmt.getTblProperties());
    removeHiddenTableProperties(properties);
    String kuduParamsSql = getKuduPartitionByParams(innerStmt);
    // TODO: Pass the correct compression, if applicable.
    String createTableSql = getCreateTableSql(innerStmt.getDb(), innerStmt.getTbl(),
        innerStmt.getComment(), null, partitionColsSql,
        innerStmt.getTblPrimaryKeyColumnNames(), kuduParamsSql,
        innerStmt.getSortColumns(), properties, innerStmt.getSerdeProperties(),
        innerStmt.isExternal(), innerStmt.getIfNotExists(), innerStmt.getRowFormat(),
        HdfsFileFormat.fromThrift(innerStmt.getFileFormat()), HdfsCompression.NONE, null,
        innerStmt.getLocation());
    return createTableSql + " AS " + stmt.getQueryStmt().toSql();
  }

  /**
   * Returns a "CREATE TABLE" or "CREATE VIEW" statement that creates the specified
   * table.
   */
  public static String getCreateTableSql(Table table) throws CatalogException {
    Preconditions.checkNotNull(table);
    if (table instanceof View) return getCreateViewSql((View)table);
    org.apache.hadoop.hive.metastore.api.Table msTable = table.getMetaStoreTable();
    // Use a LinkedHashMap to preserve the ordering of the table properties.
    LinkedHashMap<String, String> properties = Maps.newLinkedHashMap(msTable.getParameters());
    if (properties.containsKey("transient_lastDdlTime")) {
      properties.remove("transient_lastDdlTime");
    }
    boolean isExternal = msTable.getTableType() != null &&
        msTable.getTableType().equals(TableType.EXTERNAL_TABLE.toString());
    List<String> sortColsSql = getSortColumns(properties);
    String comment = properties.get("comment");
    removeHiddenTableProperties(properties);
    ArrayList<String> colsSql = Lists.newArrayList();
    ArrayList<String> partitionColsSql = Lists.newArrayList();
    boolean isHbaseTable = table instanceof HBaseTable;
    for (int i = 0; i < table.getColumns().size(); i++) {
      if (!isHbaseTable && i < table.getNumClusteringCols()) {
        partitionColsSql.add(columnToSql(table.getColumns().get(i)));
      } else {
        colsSql.add(columnToSql(table.getColumns().get(i)));
      }
    }
    RowFormat rowFormat = RowFormat.fromStorageDescriptor(msTable.getSd());
    HdfsFileFormat format = HdfsFileFormat.fromHdfsInputFormatClass(
        msTable.getSd().getInputFormat());
    HdfsCompression compression = HdfsCompression.fromHdfsInputFormatClass(
        msTable.getSd().getInputFormat());
    String location = isHbaseTable ? null : msTable.getSd().getLocation();
    Map<String, String> serdeParameters = msTable.getSd().getSerdeInfo().getParameters();

    String storageHandlerClassName = table.getStorageHandlerClassName();
    List<String> primaryKeySql = Lists.newArrayList();
    String kuduPartitionByParams = null;
    if (table instanceof KuduTable) {
      KuduTable kuduTable = (KuduTable) table;
      // Kudu tables don't use LOCATION syntax
      location = null;
      format = HdfsFileFormat.KUDU;
      // Kudu tables cannot use the Hive DDL syntax for the storage handler
      storageHandlerClassName = null;
      properties.remove(KuduTable.KEY_STORAGE_HANDLER);
      String kuduTableName = properties.get(KuduTable.KEY_TABLE_NAME);
      Preconditions.checkNotNull(kuduTableName);
      if (kuduTableName.equals(KuduUtil.getDefaultCreateKuduTableName(
          table.getDb().getName(), table.getName()))) {
        properties.remove(KuduTable.KEY_TABLE_NAME);
      }
      // Internal property, should not be exposed to the user.
      properties.remove(StatsSetupConst.DO_NOT_UPDATE_STATS);

      if (!isExternal) {
        primaryKeySql.addAll(kuduTable.getPrimaryKeyColumnNames());

        List<String> paramsSql = Lists.newArrayList();
        for (KuduPartitionParam param: kuduTable.getPartitionBy()) {
          paramsSql.add(param.toSql());
        }
        kuduPartitionByParams = Joiner.on(", ").join(paramsSql);
      } else {
        // We shouldn't output the columns for external tables
        colsSql = null;
      }
    }
    HdfsUri tableLocation = location == null ? null : new HdfsUri(location);
    return getCreateTableSql(table.getDb().getName(), table.getName(), comment, colsSql,
        partitionColsSql, primaryKeySql, kuduPartitionByParams, sortColsSql, properties,
        serdeParameters, isExternal, false, rowFormat, format, compression,
        storageHandlerClassName, tableLocation);
  }

  /**
   * Returns a "CREATE TABLE" string that creates the table with the specified properties.
   * The tableName must not be null. If columnsSql is null, the schema syntax will
   * not be generated.
   */
  public static String getCreateTableSql(String dbName, String tableName,
      String tableComment, List<String> columnsSql, List<String> partitionColumnsSql,
      List<String> primaryKeysSql, String kuduPartitionByParams,
      List<String> sortColsSql, Map<String, String> tblProperties,
      Map<String, String> serdeParameters, boolean isExternal, boolean ifNotExists,
      RowFormat rowFormat, HdfsFileFormat fileFormat, HdfsCompression compression,
      String storageHandlerClass, HdfsUri location) {
    Preconditions.checkNotNull(tableName);
    StringBuilder sb = new StringBuilder("CREATE ");
    if (isExternal) sb.append("EXTERNAL ");
    sb.append("TABLE ");
    if (ifNotExists) sb.append("IF NOT EXISTS ");
    if (dbName != null) sb.append(dbName + ".");
    sb.append(tableName);
    if (columnsSql != null && !columnsSql.isEmpty()) {
      sb.append(" (\n  ");
      sb.append(Joiner.on(",\n  ").join(columnsSql));
      if (primaryKeysSql != null && !primaryKeysSql.isEmpty()) {
        sb.append(",\n  PRIMARY KEY (");
        Joiner.on(", ").appendTo(sb, primaryKeysSql).append(")");
      }
      sb.append("\n)");
    } else {
      // CTAS for Kudu tables still print the primary key
      if (primaryKeysSql != null && !primaryKeysSql.isEmpty()) {
        sb.append("\n PRIMARY KEY (");
        Joiner.on(", ").appendTo(sb, primaryKeysSql).append(")");
      }
    }
    sb.append("\n");

    if (partitionColumnsSql != null && partitionColumnsSql.size() > 0) {
      sb.append(String.format("PARTITIONED BY (\n  %s\n)\n",
          Joiner.on(", \n  ").join(partitionColumnsSql)));
    }

    if (kuduPartitionByParams != null) {
      sb.append("PARTITION BY " + kuduPartitionByParams + "\n");
    }

    if (sortColsSql != null) {
      sb.append(String.format("SORT BY (\n  %s\n)\n",
          Joiner.on(", \n  ").join(sortColsSql)));
    }

    if (tableComment != null) sb.append(" COMMENT '" + tableComment + "'\n");

    if (rowFormat != null && !rowFormat.isDefault()) {
      sb.append("ROW FORMAT DELIMITED");
      if (rowFormat.getFieldDelimiter() != null) {
        String fieldDelim = StringEscapeUtils.escapeJava(rowFormat.getFieldDelimiter());
        sb.append(" FIELDS TERMINATED BY '" + fieldDelim + "'");
      }
      if (rowFormat.getEscapeChar() != null) {
        String escapeChar = StringEscapeUtils.escapeJava(rowFormat.getEscapeChar());
        sb.append(" ESCAPED BY '" + escapeChar + "'");
      }
      if (rowFormat.getLineDelimiter() != null) {
        String lineDelim = StringEscapeUtils.escapeJava(rowFormat.getLineDelimiter());
        sb.append(" LINES TERMINATED BY '" + lineDelim + "'");
      }
      sb.append("\n");
    }

    if (storageHandlerClass == null) {
      // TODO: Remove this special case when we have the LZO_TEXT writer
      // We must handle LZO_TEXT specially because Impala does not yet support creating
      // tables with this row format. In this case, we cannot output "WITH
      // SERDEPROPERTIES" because Hive does not support it with "STORED AS". For any
      // other HdfsFileFormat we want to output the serdeproperties because it is
      // supported by Impala.
      if (compression != HdfsCompression.LZO &&
          compression != HdfsCompression.LZO_INDEX &&
          serdeParameters != null && !serdeParameters.isEmpty()) {
        sb.append(
            "WITH SERDEPROPERTIES " + propertyMapToSql(serdeParameters) + "\n");
      }

      if (fileFormat != null) {
        sb.append("STORED AS " + fileFormat.toSql(compression) + "\n");
      }
    } else {
      // If the storageHandlerClass is set, then we will generate the proper Hive DDL
      // because we do not yet support creating HBase tables via Impala.
      sb.append("STORED BY '" + storageHandlerClass + "'\n");
      if (serdeParameters != null && !serdeParameters.isEmpty()) {
        sb.append(
            "WITH SERDEPROPERTIES " + propertyMapToSql(serdeParameters) + "\n");
      }
    }
    if (location != null) {
      sb.append("LOCATION '" + location.toString() + "'\n");
    }
    if (tblProperties != null && !tblProperties.isEmpty()) {
      sb.append("TBLPROPERTIES " + propertyMapToSql(tblProperties));
    }
    return sb.toString();
  }

  public static String getCreateFunctionSql(List<Function> functions) {
    Preconditions.checkNotNull(functions);
    StringBuilder sb = new StringBuilder();
    for (Function fn: functions) {
      sb.append(fn.toSql(false));
    }
    return sb.toString();
  }

  public static String getCreateViewSql(View view) {
    StringBuffer sb = new StringBuffer();
    sb.append("CREATE VIEW ");
    // Use toSql() to ensure that the table name and query statement are normalized
    // and identifiers are quoted.
    sb.append(view.getTableName().toSql());
    sb.append(" AS\n");
    sb.append(view.getQueryStmt().toSql());
    return sb.toString();
  }

  private static String columnToSql(Column col) {
    StringBuilder sb = new StringBuilder(col.getName());
    if (col.getType() != null) sb.append(" " + col.getType().toSql());
    if (col instanceof KuduColumn) {
      KuduColumn kuduCol = (KuduColumn) col;
      Boolean isNullable = kuduCol.isNullable();
      if (isNullable != null) sb.append(isNullable ? " NULL" : " NOT NULL");
      if (kuduCol.getEncoding() != null) sb.append(" ENCODING " + kuduCol.getEncoding());
      if (kuduCol.getCompression() != null) {
        sb.append(" COMPRESSION " + kuduCol.getCompression());
      }
      if (kuduCol.hasDefaultValue()) {
        sb.append(" DEFAULT " + kuduCol.getDefaultValueSql());
      }
      if (kuduCol.getBlockSize() != 0) {
        sb.append(String.format(" BLOCK_SIZE %d", kuduCol.getBlockSize()));
      }
    }
    if (!Strings.isNullOrEmpty(col.getComment())) {
      sb.append(String.format(" COMMENT '%s'", col.getComment()));
    }
    return sb.toString();
  }

  private static String propertyMapToSql(Map<String, String> propertyMap) {
    // Sort entries on the key to ensure output is deterministic for tests (IMPALA-5757).
    List<Entry<String, String>> mapEntries = Lists.newArrayList(propertyMap.entrySet());
    Collections.sort(mapEntries, new Comparator<Entry<String, String>>() {
      public int compare(Entry<String, String> o1, Entry<String, String> o2) {
        return ObjectUtils.compare(o1.getKey(), o2.getKey());
      } });

    List<String> properties = Lists.newArrayList();
    for (Map.Entry<String, String> entry: mapEntries) {
      properties.add(String.format("'%s'='%s'", entry.getKey(),
          // Properties may contain characters that need to be escaped.
          // e.g. If the row format escape delimiter is '\', the map of serde properties
          // from the metastore table will contain 'escape.delim' => '\', which is not
          // properly escaped.
          StringEscapeUtils.escapeJava(entry.getValue())));
    }
    return "(" + Joiner.on(", ").join(properties) + ")";
  }

  /**
   * Returns a SQL representation of the given list of hints. Uses the end-of-line
   * commented plan hint style such that hinted views created by Impala are readable by
   * Hive (parsed as a comment by Hive).
   */
  public static String getPlanHintsSql(List<PlanHint> hints) {
    Preconditions.checkNotNull(hints);
    if (hints.isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    sb.append("\n-- +");
    sb.append(Joiner.on(",").join(hints));
    sb.append("\n");
    return sb.toString();
  }
}
