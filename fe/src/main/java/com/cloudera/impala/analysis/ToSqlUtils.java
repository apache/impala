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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.Token;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.parse.HiveLexer;

import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsCompression;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.Table;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Contains utility methods for creating SQL strings, for example,
 * for creating identifier strings that are compatible with Hive or Impala.
 */
public class ToSqlUtils {
  // Table properties to hide when generating the toSql() statement
  // EXTERNAL and comment are hidden because they are part of the toSql result, e.g.,
  // "CREATE EXTERNAL TABLE <name> ... COMMENT <comment> ..."
  private static final ImmutableSet<String> HIDDEN_TABLE_PROPERTIES =
      ImmutableSet.of("EXTERNAL", "comment");

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
      try {
        Integer.parseInt(ident.substring(0, 1));
        startsWithNumber = true;
      } catch (NumberFormatException e) {
        // Ignore exception, identifier does not start with number.
      }
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
   * Returns the "CREATE TABLE" SQL string corresponding to the given CreateTableStmt.
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
    // TODO: Pass the correct compression, if applicable.
    return getCreateTableSql(stmt.getDb(), stmt.getTbl(), stmt.getComment(), colsSql,
        partitionColsSql, stmt.getTblProperties(), stmt.getSerdeProperties(),
        stmt.isExternal(), stmt.getIfNotExists(), stmt.getRowFormat(),
        HdfsFileFormat.fromThrift(stmt.getFileFormat()), HdfsCompression.NONE, null,
        stmt.getLocation().toString());
  }

  /**
   * Returns a "CREATE TABLE" statement that creates the specified table.
   */
  public static String getCreateTableSql(Table table) throws CatalogException {
    Preconditions.checkNotNull(table);
    org.apache.hadoop.hive.metastore.api.Table msTable = table.getMetaStoreTable();
    HashMap<String, String> properties = Maps.newHashMap(msTable.getParameters());
    boolean isExternal = msTable.getTableType() != null &&
        msTable.getTableType().equals(TableType.EXTERNAL_TABLE.toString());
    String comment = properties.get("comment");
    for (String hiddenProperty: HIDDEN_TABLE_PROPERTIES) {
      properties.remove(hiddenProperty);
    }
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
    return getCreateTableSql(table.getDb().getName(), table.getName(), comment, colsSql,
        partitionColsSql, properties, serdeParameters, isExternal, false, rowFormat,
        format, compression, table.getStorageHandlerClassName(), location);
  }

  /**
   * Returns a "CREATE TABLE" string that creates the table with the specified properties.
   * The tableName must not be null. If columnsSql is null, the schema syntax will
   * not be generated.
   */
  public static String getCreateTableSql(String dbName, String tableName,
      String tableComment, List<String> columnsSql, List<String> partitionColumnsSql,
      Map<String, String> tblProperties, Map<String, String> serdeParameters,
      boolean isExternal, boolean ifNotExists, RowFormat rowFormat,
      HdfsFileFormat fileFormat, HdfsCompression compression, String storageHandlerClass,
      String location) {
    Preconditions.checkNotNull(tableName);
    StringBuilder sb = new StringBuilder("CREATE ");
    if (isExternal) sb.append("EXTERNAL ");
    sb.append("TABLE ");
    if (ifNotExists) sb.append("IF NOT EXISTS ");
    if (dbName != null) sb.append(dbName + ".");
    if (columnsSql != null) {
      sb.append(tableName + " (\n  ");
      sb.append(Joiner.on(", \n  ").join(columnsSql));
      sb.append("\n)");
    }
    sb.append("\n");
    if (tableComment != null) sb.append(" COMMENT '" + tableComment + "'\n");

    if (partitionColumnsSql != null && partitionColumnsSql.size() > 0) {
      sb.append(String.format("PARTITIONED BY (\n  %s\n)\n",
          Joiner.on(", \n  ").join(partitionColumnsSql)));
    }

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
      sb.append("LOCATION '" + location + "'\n");
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

  private static String columnToSql(Column col) {
    StringBuilder sb = new StringBuilder(col.getName());
    if (col.getType() != null) sb.append(" " + col.getType().toSql());
    if (!Strings.isNullOrEmpty(col.getComment())) {
      sb.append(String.format(" COMMENT '%s'", col.getComment()));
    }
    return sb.toString();
  }

  private static String propertyMapToSql(Map<String, String> propertyMap) {
    List<String> properties = Lists.newArrayList();
    for (Map.Entry<String, String> entry: propertyMap.entrySet()) {
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
  public static String getPlanHintsSql(List<String> hints) {
    if (hints == null || hints.isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    sb.append("\n-- +");
    sb.append(Joiner.on(",").join(hints));
    sb.append("\n");
    return sb.toString();
  }
}
