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
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
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
  private final ArrayList<String> rawPath_;

  /// The resolved path to describe, set after analysis.
  private Path path_;

  /// The fully qualified name of the root table, set after analysis.
  private Table table_;

  /// Struct type with the fields to display for the described path.
  /// Only set when describing a path to a nested collection.
  private StructType resultStruct_;

  public DescribeTableStmt(ArrayList<String> rawPath, TDescribeOutputStyle outputStyle) {
    Preconditions.checkNotNull(rawPath);
    Preconditions.checkArgument(!rawPath.isEmpty());
    rawPath_ = rawPath;
    outputStyle_ = outputStyle;
    path_ = null;
    resultStruct_ = null;
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("DESCRIBE ");
    if (outputStyle_ != TDescribeOutputStyle.MINIMAL) {
      sb.append(outputStyle_.toString() + " ");
    }
    return sb.toString() + StringUtils.join(rawPath_, ".");
  }

  public Table getTable() { return table_; }
  public TDescribeOutputStyle getOutputStyle() { return outputStyle_; }

  /**
   * Get the privilege requirement, which depends on the output style.
   */
  private Privilege getPrivilegeRequirement() {
    switch (outputStyle_) {
      case MINIMAL: return Privilege.ANY;
      case FORMATTED:
      case EXTENDED:
        return Privilege.VIEW_METADATA;
      default:
        Preconditions.checkArgument(false);
        return null;
    }
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(rawPath_, null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    try {
      path_ = analyzer.resolvePath(rawPath_, PathType.ANY);
    } catch (AnalysisException ae) {
      // Register privilege requests to prefer reporting an authorization error over
      // an analysis error. We should not accidentally reveal the non-existence of a
      // table/database if the user is not authorized.
      if (rawPath_.size() > 1) {
        analyzer.registerPrivReq(new PrivilegeRequestBuilder()
            .onTable(rawPath_.get(0), rawPath_.get(1))
            .allOf(getPrivilegeRequirement()).toRequest());
      }
      analyzer.registerPrivReq(new PrivilegeRequestBuilder()
          .onTable(analyzer.getDefaultDb(), rawPath_.get(0))
          .allOf(getPrivilegeRequirement()).toRequest());
      throw ae;
    } catch (TableLoadingException tle) {
      throw new AnalysisException(tle.getMessage(), tle);
    }

    table_ = path_.getRootTable();
    // Register authorization and audit events.
    analyzer.getTable(table_.getTableName(), getPrivilegeRequirement());

    // Describing a table.
    if (path_.destTable() != null) return;

    if (path_.destType().isComplexType()) {
      if (outputStyle_ == TDescribeOutputStyle.FORMATTED ||
          outputStyle_ == TDescribeOutputStyle.EXTENDED) {
        throw new AnalysisException("DESCRIBE FORMATTED|EXTENDED must refer to a table");
      }
      // Describing a nested collection.
      Preconditions.checkState(outputStyle_ == TDescribeOutputStyle.MINIMAL);
      resultStruct_ = Path.getTypeAsStruct(path_.destType());
    } else {
      throw new AnalysisException("Cannot describe path '" +
          Joiner.on('.').join(rawPath_) + "' targeting scalar type: " +
          path_.destType().toSql());
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
    }
    return params;
  }
}
