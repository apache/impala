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
import java.util.ListIterator;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;
import org.apache.impala.util.ExprUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scan of a single iceberg table
 */
public class IcebergScanNode extends HdfsScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(TimeTravelSpec.class);

  private final FeIcebergTable icebergTable_;

  // Exprs in icebergConjuncts_ converted to UnboundPredicate.
  private final List<UnboundPredicate> icebergPredicates_ = new ArrayList<>();

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
      dataFileList = IcebergUtil.getIcebergDataFiles(icebergTable_, icebergPredicates_,
          timeTravelSpec_);
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
        try {
          fileDesc = fileDesc.cloneWithFileMetadata(
              IcebergUtil.createIcebergMetadata(icebergTable_, dataFile));
        } catch (TableLoadingException e) {
          // TODO: get rid of try-catch TableLoadingException once we have IMPALA-10737.
          throw new ImpalaRuntimeException(String.format(
              "Failed to load Iceberg table: %s", icebergTable_.getFullName()), e);
        }
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
    ListIterator<Expr> it = conjuncts_.listIterator();
    while (it.hasNext()) {
      tryConvertBinaryIcebergPredicate(analyzer, it.next());
    }
  }

  /**
   * Transform impala binary predicate to iceberg predicate
   */
  private boolean tryConvertBinaryIcebergPredicate(Analyzer analyzer, Expr expr)
      throws ImpalaException {
    if (! (expr instanceof BinaryPredicate)) return false;

    BinaryPredicate predicate = (BinaryPredicate) expr;
    Operation op = getIcebergOperator(predicate.getOp());
    if (op == null) return false;

    if (!(predicate.getChild(0) instanceof SlotRef)) return false;
    SlotRef ref = (SlotRef) predicate.getChild(0);

    if (!(predicate.getChild(1) instanceof LiteralExpr)) return false;
    LiteralExpr literal = (LiteralExpr) predicate.getChild(1);

    // If predicate contains map/struct, this column would be null
    if (ref.getDesc().getColumn() == null) return false;

    String colName = ref.getDesc().getColumn().getName();
    UnboundPredicate unboundPredicate = null;
    switch (literal.getType().getPrimitiveType()) {
      case BOOLEAN: {
        unboundPredicate = Expressions.predicate(op, colName,
            ((BoolLiteral) literal).getValue());
        break;
      }
      case TINYINT:
      case SMALLINT:
      case INT: {
        unboundPredicate = Expressions.predicate(op, colName,
            ((NumericLiteral) literal).getIntValue());
        break;
      }
      case BIGINT: {
        unboundPredicate = Expressions.predicate(op, colName,
            ((NumericLiteral) literal).getLongValue());
        break;
      }
      case FLOAT: {
        unboundPredicate = Expressions.predicate(op, colName,
            (float)((NumericLiteral) literal).getDoubleValue());
        break;
      }
      case DOUBLE: {
        unboundPredicate = Expressions.predicate(op, colName,
            ((NumericLiteral) literal).getDoubleValue());
        break;
      }
      case STRING:
      case DATETIME:
      case CHAR: {
        unboundPredicate = Expressions.predicate(op, colName,
            ((StringLiteral) literal).getUnescapedValue());
        break;
      }
      case TIMESTAMP: {
        // TODO(IMPALA-10850): interpret timestamps in local timezone.
        long unixMicros = ExprUtil.utcTimestampToUnixTimeMicros(analyzer, literal);
        unboundPredicate = Expressions.predicate(op, colName, unixMicros);
        break;
      }
      case DATE: {
        int daysSinceEpoch = ((DateLiteral) literal).getValue();
        unboundPredicate = Expressions.predicate(op, colName, daysSinceEpoch);
        break;
      }
      case DECIMAL: {
        Type colType = ref.getDesc().getColumn().getType();
        int scale = colType.getDecimalDigits();
        BigDecimal literalValue = ((NumericLiteral) literal).getValue();
        if (literalValue.scale() <= scale) {
          // Iceberg DecimalLiteral needs to have the exact same scale.
          if (literalValue.scale() < scale) literalValue = literalValue.setScale(scale);
          unboundPredicate = Expressions.predicate(op, colName, literalValue);
        }
        break;
      }
      default: break;
    }
    if (unboundPredicate == null) return false;

    icebergPredicates_.add(unboundPredicate);

    return true;
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
}
