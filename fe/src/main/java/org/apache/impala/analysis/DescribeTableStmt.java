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

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TDescribeTableParams;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Representation of a DESCRIBE statement which returns metadata on a specified
 * table or path:
 * Syntax: DESCRIBE <path>
 *         DESCRIBE FORMATTED|EXTENDED <table>
 *
 * If FORMATTED|EXTENDED is not specified and the path refers to a table, the statement
 * only returns info on the given table's column definitions (column name, data type,
 * comment, and table-type-specific info like nullability, etc.). If the path refers to
 * a complex typed field within a column, the statement returns the field names, types,
 * and comments.
 * If FORMATTED|EXTENDED is specified, extended metadata on the table is returned
 * (in addition to the column definitions). This metadata includes info about the table
 * properties, SerDe properties, StorageDescriptor properties, and more.
 */
public class DescribeTableStmt extends StatementBase {
  private final TDescribeOutputStyle outputStyle_;

  /// "."-separated path from the describe statement.
  private final List<String> rawPath_;

  /// The resolved path to describe, set after analysis.
  private Path path_;

  /// The fully qualified name of the root table, set after analysis.
  private FeTable table_;

  /// Struct type with the fields to display for the described path.
  /// Only set when describing a path to a nested collection.
  private StructType resultStruct_;

  public DescribeTableStmt(List<String> rawPath, TDescribeOutputStyle outputStyle) {
    Preconditions.checkNotNull(rawPath);
    Preconditions.checkArgument(!rawPath.isEmpty());
    rawPath_ = rawPath;
    outputStyle_ = outputStyle;
    path_ = null;
    resultStruct_ = null;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("DESCRIBE ");
    if (outputStyle_ != TDescribeOutputStyle.MINIMAL) {
      sb.append(outputStyle_.toString() + " ");
    }
    return sb.toString() + StringUtils.join(rawPath_, ".");
  }

  public FeTable getTable() { return table_; }
  public TDescribeOutputStyle getOutputStyle() { return outputStyle_; }


  /**
   * Returns true if this statement is to describe a table and not a complex type.
   */
  public boolean targetsTable() {
    Preconditions.checkState(isAnalyzed());
    return path_.destTable() != null;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(rawPath_, null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);

    try {
      path_ = analyzer.resolvePath(rawPath_, PathType.ANY);
    } catch (AnalysisException ae) {
      // Register privilege requests to prefer reporting an authorization error over
      // an analysis error. We should not accidentally reveal the non-existence of a
      // table/database if the user is not authorized.
      if (rawPath_.size() > 1) {
        analyzer.registerPrivReq(builder ->
            builder.onTableUnknownOwner(rawPath_.get(0), rawPath_.get(1))
                .any()
                .build());
      }
      analyzer.registerPrivReq(builder ->
          builder.onTableUnknownOwner(analyzer.getDefaultDb(), rawPath_.get(0))
              .any()
              .build());
      throw ae;
    } catch (TableLoadingException tle) {
      throw new AnalysisException(tle.getMessage(), tle);
    }

    table_ = path_.getRootTable();

    // Register authorization and audit events.
    // The ANY privilege here is used as a first-level check to see if there exists any
    // privilege in the table or columns of the given table. A further check for
    // column-level filtering will be done in Frontend.doDescribeTable() to filter out
    // unauthorized columns against the actual required privileges.
    // What this essentially means is if we have DROP a privilege on a particular table,
    // this check will succeed, but the column-level filtering logic will filter out
    // all columns returning an empty result due to insufficient VIEW_METADATA privilege.
    analyzer.getTable(table_.getTableName(), /* add column-level privilege */ true,
        Privilege.ANY);
    checkMinimalForIcebergMetadataTable();

    if (!targetsTable()) analyzeComplexType(analyzer);
  }

  private void analyzeComplexType(Analyzer analyzer) throws AnalysisException {
    analyzer.registerPrivReq(builder ->
        builder.onColumn(path_.getRootTable().getDb().getName(),
          path_.getRootTable().getName(),
          path_.getRawPath().get(0), path_.getRootTable().getOwnerUser())
        .any()
        .build());

    if (!path_.destType().isComplexType()) {
      throw new AnalysisException("Cannot describe path '" +
          Joiner.on('.').join(rawPath_) + "' targeting scalar type: " +
          path_.destType().toSql());
    }

    if (outputStyle_ == TDescribeOutputStyle.FORMATTED ||
        outputStyle_ == TDescribeOutputStyle.EXTENDED) {
      throw new AnalysisException("DESCRIBE FORMATTED|EXTENDED must refer to a table");
    }

    // Describing a complex type.
    Preconditions.checkState(outputStyle_ == TDescribeOutputStyle.MINIMAL);
    resultStruct_ = Path.getTypeAsStruct(path_.destType());
  }

  private void checkMinimalForIcebergMetadataTable() throws AnalysisException {
    if (table_ instanceof IcebergMetadataTable &&
        outputStyle_ != TDescribeOutputStyle.MINIMAL) {
      throw new AnalysisException("DESCRIBE FORMATTED|EXTENDED cannot refer to a "
          + "metadata table.");
    }
  }

  public TDescribeTableParams toThrift() {
    TDescribeTableParams params = new TDescribeTableParams();
    params.setOutput_style(outputStyle_);
    if (resultStruct_ != null) {
      params.setResult_struct(resultStruct_.toThrift());
    } else {
      Preconditions.checkNotNull(table_);
      params.setTable_name(table_.getTableName().toThrift());
      if (table_ instanceof IcebergMetadataTable) {
        params.setMetadata_table_name(((IcebergMetadataTable)table_).
            getMetadataTableName());
      }
    }
    return params;
  }
}
