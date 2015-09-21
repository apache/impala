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

import java.util.List;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.util.MetaStoreUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a column definition in a CREATE/ALTER TABLE/VIEW statement.
 * Column definitions in CREATE/ALTER TABLE statements require a column type,
 * whereas column definitions in CREATE/ALTER VIEW statements infer the column type from
 * the corresponding view definition. All column definitions have an optional comment.
 * Since a column definition refers a column stored in the Metastore, the column name
 * must be valid according to the Metastore's rules (see @MetaStoreUtils).
 */
public class ColumnDef {
  private final String colName_;
  private String comment_;

  // Required in CREATE/ALTER TABLE stmts. Set to NULL in CREATE/ALTER VIEW stmts,
  // for which we setType() after analyzing the defining view definition stmt.
  private final TypeDef typeDef_;
  private Type type_;

  public ColumnDef(String colName, TypeDef typeDef, String comment) {
    colName_ = colName.toLowerCase();
    typeDef_ = typeDef;
    comment_ = comment;
  }

  /**
   * Creates an analyzed ColumnDef from a Hive FieldSchema. Throws if the FieldSchema's
   * type is not supported.
   */
  private ColumnDef(FieldSchema fs) throws AnalysisException {
    Type type = Type.parseColumnType(fs.getType());
    if (type == null) {
      throw new AnalysisException(String.format(
          "Unsupported type '%s' in Hive field schema '%s'",
          fs.getType(), fs.getName()));
    }
    colName_ = fs.getName();
    typeDef_ = new TypeDef(type);
    comment_ = fs.getComment();
    analyze();
  }

  public void setType(Type type) { type_ = type; }
  public Type getType() { return type_; }
  public TypeDef getTypeDef() { return typeDef_; }
  public String getColName() { return colName_; }
  public void setComment(String comment) { comment_ = comment; }
  public String getComment() { return comment_; }

  public void analyze() throws AnalysisException {
    // Check whether the column name meets the Metastore's requirements.
    if (!MetaStoreUtils.validateName(colName_)) {
      throw new AnalysisException("Invalid column/field name: " + colName_);
    }
    if (typeDef_ != null) {
      typeDef_.analyze(null);
      type_ = typeDef_.getType();
    }
    Preconditions.checkNotNull(type_);
    Preconditions.checkState(type_.isValid());
    // Check HMS constraints of type and comment.
    String typeSql = type_.toSql();
    if (typeSql.length() > MetaStoreUtil.MAX_TYPE_NAME_LENGTH) {
      throw new AnalysisException(String.format(
          "Type of column '%s' exceeds maximum type length of %d characters:\n" +
          "%s has %d characters.", colName_, MetaStoreUtil.MAX_TYPE_NAME_LENGTH,
          typeSql, typeSql.length()));
    }
    if (comment_ != null &&
        comment_.length() > MetaStoreUtil.CREATE_MAX_COMMENT_LENGTH) {
      throw new AnalysisException(String.format(
          "Comment of column '%s' exceeds maximum length of %d characters:\n" +
          "%s has %d characters.", colName_, MetaStoreUtil.CREATE_MAX_COMMENT_LENGTH,
          comment_, comment_.length()));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(colName_);
    if (type_ != null) {
      sb.append(" " + type_.toString());
    } else {
      sb.append(" " + typeDef_.toString());
    }
    if (comment_ != null) sb.append(String.format(" COMMENT '%s'", comment_));
    return sb.toString();
  }

  public TColumn toThrift() {
    TColumn col = new TColumn(new TColumn(getColName(), type_.toThrift()));
    col.setComment(getComment());
    return col;
  }

  public static List<ColumnDef> createFromFieldSchemas(List<FieldSchema> fieldSchemas)
      throws AnalysisException {
    List<ColumnDef> result = Lists.newArrayListWithCapacity(fieldSchemas.size());
    for (FieldSchema fs: fieldSchemas) result.add(new ColumnDef(fs));
    return result;
  }

  public static List<FieldSchema> toFieldSchemas(List<ColumnDef> colDefs) {
    return Lists.transform(colDefs, new Function<ColumnDef, FieldSchema>() {
      public FieldSchema apply(ColumnDef colDef) {
        Preconditions.checkNotNull(colDef.getType());
        return new FieldSchema(colDef.getColName(), colDef.getType().toSql(),
            colDef.getComment());
      }
    });
  }

}
