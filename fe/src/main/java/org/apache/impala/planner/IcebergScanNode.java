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

package org.apache.impala.planner;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ExprUtil;

import com.google.common.base.Preconditions;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scan of a single iceberg table
 */
public class IcebergScanNode extends HdfsScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(TimeTravelSpec.class);

  private final FeIcebergTable icebergTable_;

  // Exprs in icebergConjuncts_ converted to Expression.
  private final List<Expression> icebergPredicates_ = new ArrayList<>();

  private TimeTravelSpec timeTravelSpec_;

  public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, List<Expr> conjuncts,
      TableRef tblRef, FeFsTable feFsTable, MultiAggregateInfo aggInfo) {
    super(id, desc, conjuncts, getIcebergPartition(feFsTable), tblRef, aggInfo,
        null, false);
    icebergTable_ = (FeIcebergTable) desc_.getTable();
    timeTravelSpec_ = tblRef.getTimeTravelSpec();
    // Hdfs table transformed from iceberg table only has one partition
    Preconditions.checkState(partitions_.size() == 1);
  }

  /**
   * Get partition info from FeFsTable, we treat iceberg table as an
   * unpartitioned hdfs table
   */
  private static List<? extends FeFsPartition> getIcebergPartition(FeFsTable feFsTable) {
    Collection<? extends FeFsPartition> partitions =
        FeCatalogUtils.loadAllPartitions(feFsTable);
    return new ArrayList<>(partitions);
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    extractIcebergConjuncts(analyzer);
    super.init(analyzer);
  }

  /**
   * We need prune hdfs partition FileDescriptor by iceberg predicates
   */
  public List<FileDescriptor> getFileDescriptorByIcebergPredicates()
      throws ImpalaRuntimeException {
    List<DataFile> dataFileList;
    try {
      Pair<List<DataFile>, Boolean> dataFileListAndDeletePair =
          IcebergUtil.getIcebergDataFiles(icebergTable_, icebergPredicates_,
          timeTravelSpec_);
      dataFileList = dataFileListAndDeletePair.first;
      Boolean hasDeleteFile = dataFileListAndDeletePair.second;
      if (hasDeleteFile) {
        throw new TableLoadingException(String.format("Unsupported Iceberg V2 feature, "
            + "table '%s' with snapshot id '%s' contains delete files.",
            icebergTable_.getFullName(), icebergTable_.snapshotId()));
      }
    } catch (TableLoadingException e) {
      throw new ImpalaRuntimeException(String.format(
          "Failed to load data files for Iceberg table: %s", icebergTable_.getFullName()),
          e);
    }
    long dataFilesCacheMisses = 0;
    List<FileDescriptor> fileDescList = new ArrayList<>();
    for (DataFile dataFile : dataFileList) {
      FileDescriptor fileDesc = icebergTable_.getPathHashToFileDescMap()
          .get(IcebergUtil.getDataFilePathHash(dataFile));
      if (fileDesc == null) {
        if (timeTravelSpec_ == null) {
          // We should always find the data files in the cache when not doing time travel.
          throw new ImpalaRuntimeException("Cannot find file in cache: " + dataFile.path()
              + " with snapshot id: " + String.valueOf(icebergTable_.snapshotId()));
        }
        ++dataFilesCacheMisses;
        try {
          fileDesc = FeIcebergTable.Utils.getFileDescriptor(
              new Path(dataFile.path().toString()),
              new Path(icebergTable_.getIcebergTableLocation()),
              icebergTable_.getHostIndex());
        } catch (IOException ex) {
          throw new ImpalaRuntimeException(
              "Cannot load file descriptor for " + dataFile.path(), ex);
        }
        if (fileDesc == null) {
          throw new ImpalaRuntimeException(
              "Cannot load file descriptor for: " + dataFile.path());
        }
        // Add file descriptor to the cache.
        fileDesc = fileDesc.cloneWithFileMetadata(
              IcebergUtil.createIcebergMetadata(icebergTable_, dataFile));
        icebergTable_.getPathHashToFileDescMap().put(
            IcebergUtil.getDataFilePathHash(dataFile), fileDesc);
      }
      fileDescList.add(fileDesc);
    }

    if (dataFilesCacheMisses > 0) {
      Preconditions.checkState(timeTravelSpec_ != null);
      LOG.info("File descriptors had to be loaded on demand during time travel: " +
          String.valueOf(dataFilesCacheMisses));
    }

    return fileDescList;
  }

  /**
   * Extracts predicates from conjuncts_ that can be pushed down to Iceberg.
   *
   * Since Iceberg will filter data files by metadata instead of scan data files,
   * we pushdown all predicates to Iceberg to get the minimum data files to scan.
   * Here are three cases for predicate pushdown:
   * 1.The column is not part of any Iceberg partition expression
   * 2.The column is part of all partition keys without any transformation (i.e. IDENTITY)
   * 3.The column is part of all partition keys with transformation (i.e. MONTH/DAY/HOUR)
   * We can use case 1 and 3 to filter data files, but also need to evaluate it in the
   * scan, for case 2 we don't need to evaluate it in the scan. So we evaluate all
   * predicates in the scan to keep consistency. More details about Iceberg scanning,
   * please refer: https://iceberg.apache.org/spec/#scan-planning
   */
  private void extractIcebergConjuncts(Analyzer analyzer) throws ImpalaException {
    for (Expr expr : conjuncts_) {
      tryConvertIcebergPredicate(analyzer, expr);
    }
  }

  /**
   * Returns Iceberg operator by BinaryPredicate operator, or null if the operation
   * is not supported by Iceberg.
   */
  private Operation getIcebergOperator(BinaryPredicate.Operator op) {
    switch (op) {
      case EQ: return Operation.EQ;
      case NE: return Operation.NOT_EQ;
      case LE: return Operation.LT_EQ;
      case GE: return Operation.GT_EQ;
      case LT: return Operation.LT;
      case GT: return Operation.GT;
      default: return null;
    }
  }

  /**
   * Returns Iceberg operator by CompoundPredicate operator, or null if the operation
   * is not supported by Iceberg.
   */
  private Operation getIcebergOperator(CompoundPredicate.Operator op) {
    switch (op) {
      case AND: return Operation.AND;
      case OR: return Operation.OR;
      case NOT: return Operation.NOT;
      default: return null;
    }
  }

  /**
   * Transform impala predicate to iceberg predicate
   */
  private void tryConvertIcebergPredicate(Analyzer analyzer, Expr expr)
      throws ImpalaException {
    Expression predicate = convertIcebergPredicate(analyzer, expr);
    if (predicate != null) {
      icebergPredicates_.add(predicate);
      LOG.debug("Push down the predicate: " + predicate + " to iceberg");
    }
  }

  private Expression convertIcebergPredicate(Analyzer analyzer, Expr expr)
      throws ImpalaException {
    if (expr instanceof BinaryPredicate) {
      return convertIcebergPredicate(analyzer, (BinaryPredicate) expr);
    } else if (expr instanceof InPredicate) {
      return convertIcebergPredicate(analyzer, (InPredicate) expr);
    } else if (expr instanceof IsNullPredicate) {
      return convertIcebergPredicate((IsNullPredicate) expr);
    } else if (expr instanceof CompoundPredicate) {
      return convertIcebergPredicate(analyzer, (CompoundPredicate) expr);
    } else {
      return null;
    }
  }

  private UnboundPredicate<Object> convertIcebergPredicate(Analyzer analyzer,
      BinaryPredicate predicate) throws ImpalaException {
    Operation op = getIcebergOperator(predicate.getOp());
    if (op == null) {
      return null;
    }

    // Do not convert if there is an implicit cast
    if (!(predicate.getChild(0) instanceof SlotRef)) {
      return null;
    }
    SlotRef ref = (SlotRef) predicate.getChild(0);

    if (!(predicate.getChild(1) instanceof LiteralExpr)) {
      return null;
    }
    LiteralExpr literal = (LiteralExpr) predicate.getChild(1);

    // If predicate contains map/struct, this column would be null
    Column col = ref.getDesc().getColumn();
    if (col == null) {
      return null;
    }

    Object value = getIcebergValue(analyzer, ref, literal);
    if (value == null) {
      return null;
    }

    return Expressions.predicate(op, col.getName(), value);
  }

  private UnboundPredicate<Object> convertIcebergPredicate(Analyzer analyzer,
      InPredicate predicate) throws ImpalaException {
    // TODO: convert NOT_IN predicate
    if (predicate.isNotIn()) {
      return null;
    }

    // Do not convert if there is an implicit cast
    if (!(predicate.getChild(0) instanceof SlotRef)) {
      return null;
    }
    SlotRef ref = (SlotRef) predicate.getChild(0);

    // If predicate contains map/struct, this column would be null
    Column col = ref.getDesc().getColumn();
    if (col == null) {
      return null;
    }

    // Expressions takes a list of values as Objects
    List<Object> values = new ArrayList<>();
    for (int i = 1; i < predicate.getChildren().size(); ++i) {
      if (!Expr.IS_LITERAL.apply(predicate.getChild(i))) {
        return null;
      }
      LiteralExpr literal = (LiteralExpr) predicate.getChild(i);

      // Cannot push IN or NOT_IN predicate with null literal values
      if (Expr.IS_NULL_LITERAL.apply(literal)) {
        return null;
      }

      Object value = getIcebergValue(analyzer, ref, literal);
      if (value == null) {
        return null;
      }

      values.add(value);
    }

    return Expressions.in(col.getName(), values);
  }

  private UnboundPredicate<Object> convertIcebergPredicate(IsNullPredicate predicate) {
    // Do not convert if there is an implicit cast
    if (!(predicate.getChild(0) instanceof SlotRef)) {
      return null;
    }
    SlotRef ref = (SlotRef) predicate.getChild(0);

    // If predicate contains map/struct, this column would be null
    Column col = ref.getDesc().getColumn();
    if (col == null) {
      return null;
    }

    if (predicate.isNotNull()) {
      return Expressions.notNull(col.getName());
    } else{
      return Expressions.isNull(col.getName());
    }
  }

  private Expression convertIcebergPredicate(Analyzer analyzer,
      CompoundPredicate predicate) throws ImpalaException {
    Operation op = getIcebergOperator(predicate.getOp());
    if (op == null) {
      return null;
    }

    Expression left = convertIcebergPredicate(analyzer, predicate.getChild(0));
    if (left == null) {
      return null;
    }
    if (op.equals(Operation.NOT)) {
      return Expressions.not(left);
    }

    Expression right = convertIcebergPredicate(analyzer, predicate.getChild(1));
    if (right == null) {
      return null;
    }
    return op.equals(Operation.AND) ? Expressions.and(left, right)
        : Expressions.or(left, right);
  }

  private Object getIcebergValue(Analyzer analyzer, SlotRef ref, LiteralExpr literal)
      throws ImpalaException {
    IcebergColumn iceCol = (IcebergColumn) ref.getDesc().getColumn();
    Schema iceSchema = icebergTable_.getIcebergSchema();
    switch (literal.getType().getPrimitiveType()) {
      case BOOLEAN: return ((BoolLiteral) literal).getValue();
      case TINYINT:
      case SMALLINT:
      case INT: return ((NumericLiteral) literal).getIntValue();
      case BIGINT: return ((NumericLiteral) literal).getLongValue();
      case FLOAT: return (float) ((NumericLiteral) literal).getDoubleValue();
      case DOUBLE: return ((NumericLiteral) literal).getDoubleValue();
      case STRING:
      case DATETIME:
      case CHAR: return ((StringLiteral) literal).getUnescapedValue();
      case TIMESTAMP: return getIcebergTsValue(analyzer, literal, iceCol, iceSchema);
      case DATE: return ((DateLiteral) literal).getValue();
      case DECIMAL: return getIcebergDecimalValue(ref, (NumericLiteral) literal);
      default: {
        Preconditions.checkState(false,
            "Unsupported iceberg type considered for predicate: %s",
            literal.getType().toSql());
      }
    }
    return null;
  }

  private BigDecimal getIcebergDecimalValue(SlotRef ref, NumericLiteral literal) {
    Type colType = ref.getDesc().getColumn().getType();
    int scale = colType.getDecimalDigits();
    BigDecimal literalValue = literal.getValue();

    if (literalValue.scale() > scale) return null;
    // Iceberg DecimalLiteral needs to have the exact same scale.
    if (literalValue.scale() < scale) return literalValue.setScale(scale);
    return literalValue;
  }

  private Object getIcebergTsValue(Analyzer analyzer, LiteralExpr literal,
      IcebergColumn iceCol, Schema iceSchema) throws AnalysisException {
    try {
      org.apache.iceberg.types.Type iceType = iceSchema.findType(iceCol.getFieldId());
      Preconditions.checkState(iceType instanceof Types.TimestampType);
      Types.TimestampType tsType = (Types.TimestampType) iceType;
      if (tsType.shouldAdjustToUTC()) {
        return ExprUtil.localTimestampToUnixTimeMicros(analyzer, literal);
      } else {
        return ExprUtil.utcTimestampToUnixTimeMicros(analyzer, literal);
      }
    } catch (InternalException ex) {
      // We cannot interpret the timestamp literal. Maybe the timestamp is invalid,
      // or the local timestamp ambigously converts to UTC due to daylight saving
      // time backward turn. E.g. '2021-10-31 02:15:00 Europe/Budapest' converts to
      // either '2021-10-31 00:15:00 UTC' or '2021-10-31 01:15:00 UTC'.
      LOG.warn("Exception occurred during timestamp conversion: " + ex.toString() +
          "\nThis means timestamp predicate is not pushed to Iceberg, let Impala " +
          "backend handle it.");
    }
    return null;
  }
}
