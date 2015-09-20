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

import org.apache.commons.lang3.StringUtils;

import parquet.Strings;

import com.cloudera.impala.analysis.Path.PathType;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequestBuilder;
import com.cloudera.impala.catalog.StructType;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDescribeTableOutputStyle;
import com.cloudera.impala.thrift.TDescribeTableParams;
import com.google.common.base.Preconditions;

/**
 * Representation of a DESCRIBE table statement which returns metadata on
 * a specified table:
 * Syntax: DESCRIBE <path>
 *         DESCRIBE FORMATTED <table>
 *
 * If FORMATTED is not specified and the path refers to a table, the statement only
 * returns info on the given table's column definition (column name, data type, and
 * comment). If the path refers to a complex typed field within a column, the statement
 * returns the field names, types, and comments.
 * If FORMATTED is specified, extended metadata on the table is returned
 * (in addition to the column definitions). This metadata includes info about the table
 * properties, SerDe properties, StorageDescriptor properties, and more.
 */
public class DescribeStmt extends StatementBase {
  private final TDescribeTableOutputStyle outputStyle_;

  /// "."-separated path from the describe statement.
  private ArrayList<String> rawPath_;

  /// The resolved path to describe, set after analysis.
  private Path path_;

  /// The fully qualified name of the root table, set after analysis.
  private TableName tableName_;

  /// Struct type with the fields to display for the described path.
  private StructType resultStruct_;

  public DescribeStmt(ArrayList<String> rawPath, TDescribeTableOutputStyle outputStyle) {
    Preconditions.checkNotNull(rawPath);
    Preconditions.checkArgument(!rawPath.isEmpty());
    rawPath_ = rawPath;
    outputStyle_ = outputStyle;
    path_ = null;
    tableName_ = null;
    resultStruct_ = null;
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("DESCRIBE ");
    if (outputStyle_ != TDescribeTableOutputStyle.MINIMAL) {
      sb.append(outputStyle_.toString());
    }
    return sb.toString() + StringUtils.join(rawPath_, ".");
  }

  public TableName getTableName() { return tableName_; }
  public TDescribeTableOutputStyle getOutputStyle() { return outputStyle_; }


  /**
   * Get the privilege requirement, which depends on the output style.
   */
  public Privilege getPrivilegeRequirement() {
    switch (outputStyle_) {
      case MINIMAL: return Privilege.ANY;
      case FORMATTED: return Privilege.VIEW_METADATA;
      default:
        Preconditions.checkArgument(false);
        return null;
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    try {
      path_ = analyzer.resolvePath(rawPath_, PathType.ANY);
    } catch (AnalysisException ae) {
      // Register privilege requests to prefer reporting an authorization error over
      // an analysis error. We should not accidentally reveal the non-existence of a
      // table/database if the user is not authorized.
      if (analyzer.hasMissingTbls()) throw ae;
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

    tableName_ = analyzer.getFqTableName(path_.getRootTable().getTableName());
    analyzer.getTable(tableName_, getPrivilegeRequirement());

    if (path_.destTable() != null) {
      resultStruct_ = path_.getRootTable().getHiveColumnsAsStruct();
    } else if (path_.destType().isComplexType()) {
      if (outputStyle_ == TDescribeTableOutputStyle.FORMATTED) {
        throw new AnalysisException("DESCRIBE FORMATTED must refer to a table");
      }
      Preconditions.checkState(outputStyle_ == TDescribeTableOutputStyle.MINIMAL);
      resultStruct_ = Path.getTypeAsStruct(path_.destType());
    } else {
      throw new AnalysisException("Cannot describe path '" +
          Strings.join(rawPath_, ".") + "' targeting scalar type: " +
          path_.destType().toSql());
    }
  }

  public TDescribeTableParams toThrift() {
    TDescribeTableParams params = new TDescribeTableParams();
    params.setTable_name(getTableName().getTbl());
    params.setDb(getTableName().getDb());
    params.setOutput_style(outputStyle_);
    params.setResult_struct(resultStruct_.toThrift());
    return params;
  }
}
