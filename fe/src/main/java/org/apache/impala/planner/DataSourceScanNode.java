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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.extdatasource.ExternalDataSourceExecutor;
import org.apache.impala.extdatasource.thrift.TBinaryPredicate;
import org.apache.impala.extdatasource.thrift.TColumnDesc;
import org.apache.impala.extdatasource.thrift.TComparisonOp;
import org.apache.impala.extdatasource.thrift.TPrepareParams;
import org.apache.impala.extdatasource.thrift.TPrepareResult;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TCacheJarResult;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TDataSourceScanNode;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TScanRange;
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TStatus;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Scan of a table provided by an external data source.
 */
public class DataSourceScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(DataSourceScanNode.class);
  private final TupleDescriptor desc_;
  private final DataSourceTable table_;

  // The converted conjuncts_ that were accepted by the data source. A conjunct can
  // be converted if it contains only disjunctive predicates of the form
  // <slotref> <op> <constant>.
  private List<List<TBinaryPredicate>> acceptedPredicates_;

  // The conjuncts that were accepted by the data source and removed from conjuncts_ in
  // removeAcceptedConjuncts(). Only used in getNodeExplainString() to print the
  // conjuncts applied by the data source.
  private List<Expr> acceptedConjuncts_;

  // The number of rows estimate as returned by prepare().
  private long numRowsEstimate_;

  public DataSourceScanNode(PlanNodeId id, TupleDescriptor desc, List<Expr> conjuncts) {
    super(id, desc, "SCAN DATA SOURCE");
    desc_ = desc;
    table_ = (DataSourceTable) desc_.getTable();
    conjuncts_ = conjuncts;
    acceptedPredicates_ = null;
    acceptedConjuncts_ = null;
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    checkForSupportedFileFormats();
    prepareDataSource();
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    computeStats(analyzer);
    // materialize slots in remaining conjuncts_
    analyzer.materializeSlots(conjuncts_);
    computeMemLayout(analyzer);
    computeScanRangeLocations(analyzer);
  }

  /**
   * Returns a thrift TColumnValue representing the literal from a binary
   * predicate, or null if the type cannot be represented.
   */
  public static TColumnValue literalToColumnValue(LiteralExpr expr) {
    switch (expr.getType().getPrimitiveType()) {
      case BOOLEAN:
        return new TColumnValue().setBool_val(((BoolLiteral) expr).getValue());
      case TINYINT:
        return new TColumnValue().setByte_val(
            (byte) ((NumericLiteral) expr).getLongValue());
      case SMALLINT:
        return new TColumnValue().setShort_val(
            (short) ((NumericLiteral) expr).getLongValue());
      case INT:
        return new TColumnValue().setInt_val(
            (int) ((NumericLiteral) expr).getLongValue());
      case BIGINT:
        return new TColumnValue().setLong_val(((NumericLiteral) expr).getLongValue());
      case FLOAT:
      case DOUBLE:
        return new TColumnValue().setDouble_val(
            ((NumericLiteral) expr).getDoubleValue());
      case STRING:
        return new TColumnValue().setString_val(((StringLiteral) expr).getValue());
      case DECIMAL:
      case DATE:
      case DATETIME:
      case TIMESTAMP:
        // TODO: we support DECIMAL and TIMESTAMP but no way to specify it in SQL.
        return null;
      default:
        Preconditions.checkState(false);
        return null;
    }
  }

  /**
   * Calls prepare() on the data source to determine accepted predicates and get
   * stats. The accepted predicates are moved from conjuncts_ into acceptedConjuncts_
   * and the associated TBinaryPredicates are set in acceptedPredicates_.
   */
  private void prepareDataSource() throws InternalException {
    // Binary predicates that will be offered to the data source.
    List<List<TBinaryPredicate>> offeredPredicates = Lists.newArrayList();
    // The index into conjuncts_ for each element in offeredPredicates.
    List<Integer> conjunctsIdx = Lists.newArrayList();
    for (int i = 0; i < conjuncts_.size(); ++i) {
      Expr conjunct = conjuncts_.get(i);
      List<TBinaryPredicate> disjuncts = getDisjuncts(conjunct);
      if (disjuncts != null) {
        offeredPredicates.add(disjuncts);
        conjunctsIdx.add(i);
      }
    }

    String hdfsLocation = table_.getDataSource().getHdfs_location();
    TCacheJarResult cacheResult = FeSupport.CacheJar(hdfsLocation);
    TStatus cacheJarStatus = cacheResult.getStatus();
    if (cacheJarStatus.getStatus_code() != TErrorCode.OK) {
      throw new InternalException(String.format(
          "Unable to cache data source library at location '%s'. Check that the file " +
          "exists and is readable. Message: %s",
          hdfsLocation, Joiner.on("\n").join(cacheJarStatus.getError_msgs())));
    }
    String localPath = cacheResult.getLocal_path();
    String className = table_.getDataSource().getClass_name();
    String apiVersion = table_.getDataSource().getApi_version();
    TPrepareResult prepareResult;
    TStatus prepareStatus;
    try {
      ExternalDataSourceExecutor executor = new ExternalDataSourceExecutor(
          localPath, className, apiVersion, table_.getInitString());
      TPrepareParams prepareParams = new TPrepareParams();
      prepareParams.setInit_string(table_.getInitString());
      prepareParams.setPredicates(offeredPredicates);
      // TODO: Include DB (i.e. getFullName())?
      prepareParams.setTable_name(table_.getName());
      prepareResult = executor.prepare(prepareParams);
      prepareStatus = prepareResult.getStatus();
    } catch (Exception e) {
      throw new InternalException(String.format(
          "Error calling prepare() on data source %s",
          DataSource.debugString(table_.getDataSource())), e);
    }
    if (prepareStatus.getStatus_code() != TErrorCode.OK) {
      throw new InternalException(String.format(
          "Data source %s returned an error from prepare(): %s",
          DataSource.debugString(table_.getDataSource()),
          Joiner.on("\n").join(prepareStatus.getError_msgs())));
    }

    numRowsEstimate_ = prepareResult.getNum_rows_estimate();
    acceptedPredicates_ = Lists.newArrayList();
    List<Integer> acceptedPredicatesIdx = prepareResult.isSetAccepted_conjuncts() ?
        prepareResult.getAccepted_conjuncts() : ImmutableList.<Integer>of();
    for (Integer acceptedIdx: acceptedPredicatesIdx) {
      acceptedPredicates_.add(offeredPredicates.get(acceptedIdx));
    }
    removeAcceptedConjuncts(acceptedPredicatesIdx, conjunctsIdx);
  }

  /**
   * Converts the conjunct to a list of TBinaryPredicates if it contains only
   * disjunctive predicates of the form {slotref} {op} {constant} that can be represented
   * by TBinaryPredicates. If the Expr cannot be converted, null is returned.
   * TODO: Move this to Expr.
   */
  private List<TBinaryPredicate> getDisjuncts(Expr conjunct) {
    List<TBinaryPredicate> disjuncts = Lists.newArrayList();
    if (getDisjunctsHelper(conjunct, disjuncts)) return disjuncts;
    return null;
  }

  // Recursive helper method for getDisjuncts().
  private boolean getDisjunctsHelper(Expr conjunct,
      List<TBinaryPredicate> predicates) {
    if (conjunct instanceof BinaryPredicate) {
      if (conjunct.getChildren().size() != 2) return false;
      SlotRef slotRef = null;
      LiteralExpr literalExpr = null;
      TComparisonOp op = null;
      if ((conjunct.getChild(0).unwrapSlotRef(true) instanceof SlotRef) &&
          (conjunct.getChild(1) instanceof LiteralExpr)) {
        slotRef = conjunct.getChild(0).unwrapSlotRef(true);
        literalExpr = (LiteralExpr) conjunct.getChild(1);
        op = ((BinaryPredicate) conjunct).getOp().getThriftOp();
      } else if ((conjunct.getChild(1).unwrapSlotRef(true) instanceof SlotRef) &&
                 (conjunct.getChild(0) instanceof LiteralExpr)) {
        slotRef = conjunct.getChild(1).unwrapSlotRef(true);
        literalExpr = (LiteralExpr) conjunct.getChild(0);
        op = ((BinaryPredicate) conjunct).getOp().converse().getThriftOp();
      } else {
        return false;
      }

      TColumnValue val = literalToColumnValue(literalExpr);
      if (val == null) return false; // false if unsupported type, e.g.

      String colName = Joiner.on(".").join(slotRef.getResolvedPath().getRawPath());
      TColumnDesc col = new TColumnDesc().setName(colName).setType(
          slotRef.getType().toThrift());
      predicates.add(new TBinaryPredicate().setCol(col).setOp(op).setValue(val));
      return true;
    } else if (conjunct instanceof CompoundPredicate) {
      CompoundPredicate compoundPredicate = ((CompoundPredicate) conjunct);
      if (compoundPredicate.getOp() != CompoundPredicate.Operator.OR) return false;
      if (!getDisjunctsHelper(conjunct.getChild(0), predicates)) return false;
      if (!getDisjunctsHelper(conjunct.getChild(1), predicates)) return false;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    inputCardinality_ = numRowsEstimate_;
    cardinality_ = numRowsEstimate_;
    cardinality_ *= computeSelectivity();
    cardinality_ = Math.max(1, cardinality_);
    cardinality_ = capAtLimit(cardinality_);

    if (LOG.isTraceEnabled()) {
      LOG.trace("computeStats DataSourceScan: cardinality=" + Long.toString(cardinality_));
    }

    numNodes_ = table_.getNumNodes();
    if (LOG.isTraceEnabled()) {
      LOG.trace("computeStats DataSourceScan: #nodes=" + Integer.toString(numNodes_));
    }
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("tid", desc_.getId().asInt())
        .add("tblName", table_.getFullName())
        .add("dataSource", DataSource.debugString(table_.getDataSource()))
        .add("initString", table_.getInitString())
        .addValue(super.debugString())
        .toString();
  }

  /**
   * Removes the predicates from conjuncts_ that were accepted by the data source.
   * Stores the accepted conjuncts in acceptedConjuncts_.
   */
  private void removeAcceptedConjuncts(List<Integer> acceptedPredicatesIdx,
      List<Integer> conjunctsIdx) {
    acceptedConjuncts_ = Lists.newArrayList();
    // Because conjuncts_ is modified in place using positional indexes from
    // conjunctsIdx, we remove the accepted predicates in reverse order.
    for (int i = acceptedPredicatesIdx.size() - 1; i >= 0; --i) {
      int acceptedPredIdx = acceptedPredicatesIdx.get(i);
      int conjunctIdx = conjunctsIdx.get(acceptedPredIdx);
      acceptedConjuncts_.add(conjuncts_.remove(conjunctIdx));
    }
    // Returns a view of the list in the original order as we will print these
    // in the explain string and it's convenient to have predicates printed
    // in the same order that they're specified.
    acceptedConjuncts_ = Lists.reverse(acceptedConjuncts_);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    Preconditions.checkNotNull(acceptedPredicates_);
    msg.node_type = TPlanNodeType.DATA_SOURCE_NODE;
    msg.data_source_node = new TDataSourceScanNode(desc_.getId().asInt(),
        table_.getDataSource(), table_.getInitString(), acceptedPredicates_);
  }

  /**
   * Create a single scan range for the localhost.
   */
  private void computeScanRangeLocations(Analyzer analyzer) {
    // TODO: Does the port matter?
    TNetworkAddress networkAddress = addressToTNetworkAddress("localhost:12345");
    Integer hostIndex = analyzer.getHostIndex().getIndex(networkAddress);
    scanRanges_ = Lists.newArrayList(
        new TScanRangeLocationList(
            new TScanRange(), Lists.newArrayList(new TScanRangeLocation(hostIndex))));
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // TODO: What's a good estimate of memory consumption?
    nodeResourceProfile_ = ResourceProfile.noReservation(1024L * 1024L * 1024L);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    String aliasStr = "";
    if (!table_.getFullName().equalsIgnoreCase(desc_.getAlias()) &&
        !table_.getName().equalsIgnoreCase(desc_.getAlias())) {
      aliasStr = " " + desc_.getAlias();
    }
    output.append(String.format("%s%s:%s [%s%s]\n", prefix, id_.toString(),
        displayName_, table_.getFullName(), aliasStr));

    if (!acceptedConjuncts_.isEmpty()) {
      output.append(prefix + "data source predicates: " +
          getExplainString(acceptedConjuncts_) + "\n");
    }
    if (!conjuncts_.isEmpty()) {
      output.append(prefix + "predicates: " + getExplainString(conjuncts_) + "\n");
    }

    // Add table and column stats in verbose mode.
    if (detailLevel == TExplainLevel.VERBOSE) {
      output.append(getStatsExplainString(prefix, detailLevel));
      output.append("\n");
    }
    return output.toString();
  }

  @Override
  public boolean hasStorageLayerConjuncts() { return !acceptedConjuncts_.isEmpty(); }
}
