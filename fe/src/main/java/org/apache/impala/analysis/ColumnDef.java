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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;

/**
 * Represents a column definition in a CREATE/ALTER TABLE/VIEW statement.
 * Column definitions in CREATE/ALTER TABLE statements require a column type,
 * whereas column definitions in CREATE/ALTER VIEW statements infer the column type from
 * the corresponding view definition. All column definitions have an optional comment.
 * Since a column definition refers a column stored in the Metastore, the column name
 * must be valid according to the Metastore's rules (see @MetaStoreUtils). A number of
 * additional column options may be specified for Kudu tables.
 */
public class ColumnDef {
  private final String colName_;
  // Required in CREATE/ALTER TABLE stmts. Set to NULL in CREATE/ALTER VIEW stmts,
  // for which we setType() after analyzing the defining view definition stmt.
  private final TypeDef typeDef_;
  private Type type_;
  private String comment_;

  // Available column options
  public enum Option {
    IS_PRIMARY_KEY,
    IS_NULLABLE,
    ENCODING,
    COMPRESSION,
    DEFAULT,
    BLOCK_SIZE,
    COMMENT
  }

  // Kudu-specific column options
  //
  // Set to true if the user specified "PRIMARY KEY" in the column definition.
  private boolean isPrimaryKey_;
  // Set to true if this column may contain null values. Can be NULL if
  // not specified.
  private Boolean isNullable_;
  private String encodingVal_;
  // Encoding for this column; set in analysis.
  private Encoding encoding_;
  private String compressionVal_;
  // Compression algorithm for this column; set in analysis.
  private CompressionAlgorithm compression_;
  // Default value for this column.
  private Expr defaultValue_;
  // Desired block size for this column.
  private LiteralExpr blockSize_;

  public ColumnDef(String colName, TypeDef typeDef, Map<Option, Object> options) {
    Preconditions.checkNotNull(options);
    colName_ = colName.toLowerCase();
    typeDef_ = typeDef;
    for (Map.Entry<Option, Object> option: options.entrySet()) {
      switch (option.getKey()) {
        case IS_PRIMARY_KEY:
          Preconditions.checkState(option.getValue() instanceof Boolean);
          isPrimaryKey_ = (Boolean) option.getValue();
          break;
        case IS_NULLABLE:
          Preconditions.checkState(option.getValue() instanceof Boolean);
          isNullable_ = (Boolean) option.getValue();
          break;
        case ENCODING:
          Preconditions.checkState(option.getValue() instanceof String);
          encodingVal_ = ((String) option.getValue()).toUpperCase();
          break;
        case COMPRESSION:
          Preconditions.checkState(option.getValue() instanceof String);
          compressionVal_ = ((String) option.getValue()).toUpperCase();
          break;
        case DEFAULT:
          Preconditions.checkState(option.getValue() instanceof Expr);
          defaultValue_ = (Expr) option.getValue();
          break;
        case BLOCK_SIZE:
          Preconditions.checkState(option.getValue() instanceof LiteralExpr);
          blockSize_ = (LiteralExpr) option.getValue();
          break;
        case COMMENT:
          Preconditions.checkState(option.getValue() instanceof String);
          comment_ = (String) option.getValue();
          break;
        default:
          throw new IllegalStateException(String.format("Illegal option %s",
              option.getKey()));
      }
    }
  }

  public ColumnDef(String colName, TypeDef typeDef) {
    this(colName, typeDef, Collections.<Option, Object>emptyMap());
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
    analyze(null);
  }

  public String getColName() { return colName_; }
  public void setType(Type type) { type_ = type; }
  public Type getType() { return type_; }
  public TypeDef getTypeDef() { return typeDef_; }
  boolean isPrimaryKey() { return isPrimaryKey_; }
  public void setComment(String comment) { comment_ = comment; }
  public String getComment() { return comment_; }
  public boolean hasKuduOptions() {
    return isPrimaryKey() || isNullabilitySet() || hasEncoding() || hasCompression()
        || hasDefaultValue() || hasBlockSize();
  }
  public boolean hasEncoding() { return encodingVal_ != null; }
  public boolean hasCompression() { return compressionVal_ != null; }
  public boolean hasBlockSize() { return blockSize_ != null; }
  public boolean isNullabilitySet() { return isNullable_ != null; }

  // True if the column was explicitly set to be nullable (may differ from the default
  // behavior if not explicitly set).
  public boolean isExplicitNullable() { return isNullabilitySet() && isNullable_; }

  // True if the column was explicitly set to be not nullable (may differ from the default
  // behavior if not explicitly set).
  public boolean isExplicitNotNullable() { return isNullabilitySet() && !isNullable_; }

  public boolean hasDefaultValue() { return defaultValue_ != null; }

  public void analyze(Analyzer analyzer) throws AnalysisException {
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
    if (hasKuduOptions()) {
      Preconditions.checkNotNull(analyzer);
      analyzeKuduOptions(analyzer);
    }
    // Check HMS constraints on comment.
    if (comment_ != null &&
        comment_.length() > MetaStoreUtil.CREATE_MAX_COMMENT_LENGTH) {
      throw new AnalysisException(String.format(
          "Comment of column '%s' exceeds maximum length of %d characters:\n" +
          "%s has %d characters.", colName_, MetaStoreUtil.CREATE_MAX_COMMENT_LENGTH,
          comment_, comment_.length()));
    }
  }

  private void analyzeKuduOptions(Analyzer analyzer) throws AnalysisException {
    if (isPrimaryKey_ && isNullable_ != null && isNullable_) {
      throw new AnalysisException("Primary key columns cannot be nullable: " +
          toString());
    }
    // Encoding value
    if (encodingVal_ != null) {
      try {
        encoding_ = Encoding.valueOf(encodingVal_);
      } catch (IllegalArgumentException e) {
        throw new AnalysisException(String.format("Unsupported encoding value '%s'. " +
            "Supported encoding values are: %s", encodingVal_,
            Joiner.on(", ").join(Encoding.values())));
      }
    }
    // Compression algorithm
    if (compressionVal_ != null) {
      try {
        compression_ = CompressionAlgorithm.valueOf(compressionVal_);
      } catch (IllegalArgumentException e) {
        throw new AnalysisException(String.format("Unsupported compression " +
            "algorithm '%s'. Supported compression algorithms are: %s", compressionVal_,
            Joiner.on(", ").join(CompressionAlgorithm.values())));
      }
    }
    // Analyze the default value, if any.
    // TODO: Similar checks are applied for range partition values in
    // RangePartition.analyzeBoundaryValue(). Consider consolidating the logic into a
    // single function.
    if (defaultValue_ != null) {
      try {
        defaultValue_.analyze(analyzer);
      } catch (AnalysisException e) {
        throw new AnalysisException(String.format("Only constant values are allowed " +
            "for default values: %s", defaultValue_.toSql()), e);
      }
      if (!defaultValue_.isConstant()) {
        throw new AnalysisException(String.format("Only constant values are allowed " +
            "for default values: %s", defaultValue_.toSql()));
      }
      defaultValue_ = LiteralExpr.create(defaultValue_, analyzer.getQueryCtx());
      if (defaultValue_ == null) {
        throw new AnalysisException(String.format("Only constant values are allowed " +
            "for default values: %s", defaultValue_.toSql()));
      }
      if (defaultValue_.getType().isNull() && ((isNullable_ != null && !isNullable_)
          || isPrimaryKey_)) {
        throw new AnalysisException(String.format("Default value of NULL not allowed " +
            "on non-nullable column: '%s'", getColName()));
      }
      if (!Type.isImplicitlyCastable(defaultValue_.getType(), type_, true)) {
        throw new AnalysisException(String.format("Default value %s (type: %s) " +
            "is not compatible with column '%s' (type: %s).", defaultValue_.toSql(),
            defaultValue_.getType().toSql(), colName_, type_.toSql()));
      }
      if (!defaultValue_.getType().equals(type_)) {
        Expr castLiteral = defaultValue_.uncheckedCastTo(type_);
        Preconditions.checkNotNull(castLiteral);
        defaultValue_ = LiteralExpr.create(castLiteral, analyzer.getQueryCtx());
      }
      Preconditions.checkNotNull(defaultValue_);
    }

    // Analyze the block size value, if any.
    if (blockSize_ != null) {
      blockSize_.analyze(null);
      if (!blockSize_.getType().isIntegerType()) {
        throw new AnalysisException(String.format("Invalid value for BLOCK_SIZE: %s. " +
            "A positive INTEGER value is expected.", blockSize_.toSql()));
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(colName_).append(" ");
    if (type_ != null) {
      sb.append(type_.toSql());
    } else {
      sb.append(typeDef_.toSql());
    }
    if (isPrimaryKey_) sb.append(" PRIMARY KEY");
    if (isNullable_ != null) sb.append(isNullable_ ? " NULL" : " NOT NULL");
    if (encoding_ != null) sb.append(" ENCODING " + encoding_.toString());
    if (compression_ != null) sb.append(" COMPRESSION " + compression_.toString());
    if (defaultValue_ != null) sb.append(" DEFAULT " + defaultValue_.toSql());
    if (blockSize_ != null) sb.append(" BLOCK_SIZE " + blockSize_.toSql());
    if (comment_ != null) sb.append(String.format(" COMMENT '%s'", comment_));
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj == this) return true;
    if (obj.getClass() != getClass()) return false;
    ColumnDef rhs = (ColumnDef) obj;
    return new EqualsBuilder()
        .append(colName_, rhs.colName_)
        .append(comment_, rhs.comment_)
        .append(isPrimaryKey_, rhs.isPrimaryKey_)
        .append(typeDef_, rhs.typeDef_)
        .append(type_, rhs.type_)
        .append(isNullable_, rhs.isNullable_)
        .append(encoding_, rhs.encoding_)
        .append(compression_, rhs.compression_)
        .append(defaultValue_, rhs.defaultValue_)
        .append(blockSize_, rhs.blockSize_)
        .isEquals();
  }

  public TColumn toThrift() {
    TColumn col = new TColumn(getColName(), type_.toThrift());
    Integer blockSize =
        blockSize_ == null ? null : (int) ((NumericLiteral) blockSize_).getIntValue();
    KuduUtil.setColumnOptions(col, isPrimaryKey_, isNullable_, encoding_,
        compression_, defaultValue_, blockSize);
    if (comment_ != null) col.setComment(comment_);
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

  static List<String> toColumnNames(Collection<ColumnDef> colDefs) {
    List<String> colNames = Lists.newArrayList();
    for (ColumnDef colDef: colDefs) {
      colNames.add(colDef.getColName());
    }
    return colNames;
  }

  /**
   * Generates and returns a map of column names to column definitions. Assumes that
   * the column names are unique. It guarantees that the iteration order of the map
   * is the same as the iteration order of 'colDefs'.
   */
  static Map<String, ColumnDef> mapByColumnNames(Collection<ColumnDef> colDefs) {
    Map<String, ColumnDef> colDefsByColName = new LinkedHashMap<String, ColumnDef>();
    for (ColumnDef colDef: colDefs) {
      ColumnDef def = colDefsByColName.put(colDef.getColName(), colDef);
      Preconditions.checkState(def == null);
    }
    return colDefsByColName;
  }
}
