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
import java.util.Arrays;
import java.util.List;

import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TShowTablesParams;

import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW METADATA TABLES [pattern] statement.
 *
 * This statement queries the list of metadata tables available for a table. This is
 * currently only supported for Iceberg tables:
 *
 * Acceptable syntax:
 *
 * SHOW METADATA TABLES IN [database.]table
 * SHOW METADATA TABLES IN [database.]table "pattern"
 * SHOW METADATA TABLES IN [database.]table LIKE "pattern"
 *
 * As in Hive, the 'LIKE' is optional. In Hive, also SHOW TABLES unquotedpattern is
 * accepted by the parser but returns no results. We don't support that syntax.
 */
public class ShowMetadataTablesStmt extends ShowTablesOrViewsStmt {
  private final String tbl_;

  public ShowMetadataTablesStmt(String database, String tbl, String pattern) {
    super(database, pattern);
    Preconditions.checkNotNull(tbl);
    tbl_ = tbl;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    List<String> rawPath = createRawPath();
    tblRefs.add(new TableRef(rawPath, null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(tbl_);
    super.analyze(analyzer);

    String db = getDb();
    analyzer.registerPrivReq(builder ->
        builder.onTableUnknownOwner(db, tbl_)
        .any()
        .build());

    Path resolvedPath;
    try {
      final List<String> rawPath = createRawPath();
      resolvedPath = analyzer.resolvePath(rawPath, PathType.ANY);
    } catch (TableLoadingException tle) {
      throw new AnalysisException(tle.getMessage(), tle);
    }

    FeTable table = resolvedPath.getRootTable();
    if (!(table instanceof FeIcebergTable)) {
      throw new AnalysisException(
          "The SHOW METADATA TABLES statement is only valid for Iceberg tables: '" + db +
          "." + tbl_ + "' is not an Iceberg table.");
    }
  }

  @Override
  public TShowTablesParams toThrift() {
    TShowTablesParams params = super.toThrift();
    params.setTbl(tbl_);
    return params;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("SHOW METADATA TABLES IN ");

    String parsedDb = getParsedDb();
    if (parsedDb != null) sb.append(parsedDb).append(".");

    sb.append(tbl_);

    String pattern = getPattern();
    if (pattern != null) sb.append(" LIKE '").append(pattern).append("'");

    return sb.toString();
  }

  private List<String> createRawPath() {
    List<String> res = new ArrayList<>();
    final String dbName = isAnalyzed() ? getDb() : getParsedDb();
    if (dbName != null) res.add(dbName);
    res.add(tbl_);
    return res;
  }
}
