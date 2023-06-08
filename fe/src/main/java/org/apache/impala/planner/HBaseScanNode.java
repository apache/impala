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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HBaseColumn;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.THBaseFilter;
import org.apache.impala.thrift.THBaseKeyRange;
import org.apache.impala.thrift.THBaseScanNode;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TScanRange;
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.util.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Full scan of an HBase table.
 * Only families/qualifiers specified in TupleDescriptor will be retrieved in the backend.
 */
public class HBaseScanNode extends ScanNode {
  // The suggested value for "hbase.client.scan.setCaching", which batches maxCaching
  // rows per fetch request to the HBase region server. If the value is too high,
  // then the hbase region server will have a hard time (GC pressure and long response
  // times). If the value is too small, then there will be extra trips to the hbase
  // region server.
  // Default to 1024 and update it based on row size estimate such that each batch size
  // won't exceed 500MB.
  private final static int MAX_HBASE_FETCH_BATCH_SIZE = 500 * 1024 * 1024;
  private final static int DEFAULT_SUGGESTED_CACHING = 1024;

  // Used for memory estimation when the column max size stat is missing (happens only
  // in case of string type columns).
  private final static int DEFAULT_STRING_COL_BYTES = 32 * 1024;

  // Used for memory estimation to clamp the max estimate to 128 MB in case of
  // missing stats.
  private final static int DEFAULT_MAX_ESTIMATE_BYTES = 128 * 1024 * 1024;

  // Used for memory estimation to clamp the min estimate to 4 KB which is min
  // block size that can be allocated by the mem-pool.
  private final static int DEFAULT_MIN_ESTIMATE_BYTES = 4 * 1024;

  private final static Logger LOG = LoggerFactory.getLogger(HBaseScanNode.class);
  private final TupleDescriptor desc_;

  // One range per clustering column. The range bounds are expected to be constants.
  // A null entry means there's no range restriction for that particular key.
  // If keyRanges is non-null it always contains as many entries as there are clustering
  // cols. Don't replace this variable after init().
  private List<ValueRange> keyRanges_ = new ArrayList<>();

  // The list of conjuncts used to create the key ranges. Used if we must estimate
  // cardinality based on row count stats. Don't replace this variable after init().
  private List<Expr> keyConjuncts_ = new ArrayList<>();

  // derived from keyRanges_; empty means unbounded;
  // initialize start/stopKey_ to be unbounded.
  private byte[] startKey_ = HConstants.EMPTY_START_ROW;
  private byte[] stopKey_ = HConstants.EMPTY_END_ROW;

  // True if this scan node is not going to scan anything. If the row key filter
  // evaluates to null, or if the lower bound > upper bound, then this scan node won't
  // scan at all.
  private boolean isEmpty_ = false;

  // List of HBase Filters for generating thrift message. Filled in finalize().
  private final List<THBaseFilter> filters_ = new ArrayList<>();

  private int suggestedCaching_ = DEFAULT_SUGGESTED_CACHING;

  public HBaseScanNode(PlanNodeId id, TupleDescriptor desc) {
    super(id, desc, "SCAN HBASE");
    desc_ = desc;
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    FeTable table = desc_.getTable();
    // determine scan predicates for clustering cols
    List<Column> columns = table.getColumns();
    for (int i = 0; i < columns.size(); ++i) {
      HBaseColumn col = (HBaseColumn) columns.get(i);
      if (!col.isKeyColumn()) continue;
      SlotDescriptor slotDesc = analyzer.getColumnSlot(desc_, col);
      if (slotDesc == null || !slotDesc.getType().isStringType()) {
        // the hbase row key is mapped to a non-string type
        // (since it's stored in ASCII it will be lexicographically ordered,
        // and non-string comparisons won't work)
        keyRanges_.add(null);
      } else {
        keyRanges_.add(createHBaseValueRange(slotDesc));
      }
    }

    checkForSupportedFileFormats();
    assignConjuncts(analyzer);
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    setStartStopKey(analyzer);
    // Convert predicates to HBase filters_.
    createHBaseFilters(analyzer);

    // materialize slots in remaining conjuncts_
    analyzer.materializeSlots(conjuncts_);
    computeMemLayout(analyzer);
    computeScanRangeLocations(analyzer);
    Preconditions.checkState(!scanRangeSpecs_.isSetSplit_specs());

    // Make sure key ranges are not changed any more. So startKey_ and stopKey_ are
    // stable too. These invariants make it safe to reuse row estimation results in
    // computeStats.
    keyRanges_ = Collections.unmodifiableList(keyRanges_);
    keyConjuncts_ = Collections.unmodifiableList(keyConjuncts_);

    // Call computeStats() after materializing slots and computing the mem layout.
    computeStats(analyzer);
  }

  /**
   * Transform '=', '<[=]' and '>[=]' comparisons for given slot into
   * ValueRange. Also removes those predicates which were used for the construction
   * of ValueRange from 'conjuncts_'. Only looks at comparisons w/ string constants
   * (ie, the bounds of the result can be evaluated with Expr::GetValue(NULL)).
   * HBase row key filtering works only if the row key is mapped to a string column and
   * the expression is a string constant expression.
   * If there are multiple competing comparison predicates that could be used
   * to construct a ValueRange, only the first one from each category is chosen.
   */
  private ValueRange createHBaseValueRange(SlotDescriptor d) {
    ListIterator<Expr> i = conjuncts_.listIterator();
    ValueRange result = null;
    while (i.hasNext()) {
      Expr e = i.next();
      if (!(e instanceof BinaryPredicate)) continue;
      BinaryPredicate comp = (BinaryPredicate) e;
      if ((comp.getOp() == BinaryPredicate.Operator.NE)
          || (comp.getOp() == BinaryPredicate.Operator.DISTINCT_FROM)
          || (comp.getOp() == BinaryPredicate.Operator.NOT_DISTINCT)) {
        continue;
      }
      Expr slotBinding = comp.getSlotBinding(d.getId());
      if (slotBinding == null || !slotBinding.isConstant() ||
          !slotBinding.getType().equals(Type.STRING)) {
        continue;
      }

      if (comp.getOp() == BinaryPredicate.Operator.EQ) {
        i.remove();
        keyConjuncts_.add(e);
        return ValueRange.createEqRange(slotBinding);
      }

      if (result == null) result = new ValueRange();

      // TODO: do we need copies here?
      if (comp.getOp() == BinaryPredicate.Operator.GT
          || comp.getOp() == BinaryPredicate.Operator.GE) {
        if (result.getLowerBound() == null) {
          result.setLowerBound(slotBinding);
          result.setLowerBoundInclusive(comp.getOp() == BinaryPredicate.Operator.GE);
          i.remove();
          keyConjuncts_.add(e);
        }
      } else {
        if (result.getUpperBound() == null) {
          result.setUpperBound(slotBinding);
          result.setUpperBoundInclusive(comp.getOp() == BinaryPredicate.Operator.LE);
          i.remove();
          keyConjuncts_.add(e);
       }
      }
    }
    return result;
  }

  /**
   * Convert keyRanges_ to startKey_ and stopKey_.
   * If ValueRange is not null, transform it into start/stopKey_ by evaluating the
   * expression. Analysis has checked that the expression is string type. If the
   * expression evaluates to null, then there's nothing to scan because Hbase row key
   * cannot be null.
   * At present, we only do row key filtering for string-mapped keys. String-mapped keys
   * are always encoded as ASCII.
   * ValueRange is null if there is no predicate on the row-key.
   */
  private void setStartStopKey(Analyzer analyzer) throws ImpalaException {
    Preconditions.checkNotNull(keyRanges_);
    Preconditions.checkState(keyRanges_.size() == 1);

    ValueRange rowRange = keyRanges_.get(0);
    if (rowRange != null) {
      if (rowRange.getLowerBound() != null) {
        Preconditions.checkState(rowRange.getLowerBound().isConstant());
        Preconditions.checkState(
            rowRange.getLowerBound().getType().equals(Type.STRING));
        LiteralExpr val = LiteralExpr.createBounded(rowRange.getLowerBound(),
            analyzer.getQueryCtx(), StringLiteral.MAX_STRING_LEN);
        // TODO: Make this a Preconditions.checkState(). If we get here,
        // and the value is not a string literal, then we've got a predicate
        // that we removed from the conjunct list, but which we won't evaluate
        // as a key. That is, we'll produce wrong query results.
        if (val instanceof StringLiteral) {
          StringLiteral litVal = (StringLiteral) val;
          startKey_ = convertToBytes(litVal.getUnescapedValue(),
              !rowRange.getLowerBoundInclusive());
        } else {
          // lower bound is null.
          isEmpty_ = true;
          return;
        }
      }
      if (rowRange.getUpperBound() != null) {
        Preconditions.checkState(rowRange.getUpperBound().isConstant());
        Preconditions.checkState(
            rowRange.getUpperBound().getType().equals(Type.STRING));
        LiteralExpr val = LiteralExpr.createBounded(rowRange.getUpperBound(),
            analyzer.getQueryCtx(), StringLiteral.MAX_STRING_LEN);
        if (val instanceof StringLiteral) {
          StringLiteral litVal = (StringLiteral) val;
          stopKey_ = convertToBytes(litVal.getUnescapedValue(),
              rowRange.getUpperBoundInclusive());
        } else {
          // lower bound is null.
          isEmpty_ = true;
          return;
        }
      }
    }

    boolean endKeyIsEndOfTable = Bytes.equals(stopKey_, HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey_, stopKey_) > 0) && !endKeyIsEndOfTable) {
      // Lower bound is greater than upper bound.
      isEmpty_ = true;
    }
  }

  /**
   * Also sets suggestedCaching_.
   */
  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    FeHBaseTable tbl = (FeHBaseTable) desc_.getTable();
    if (LOG.isTraceEnabled()) {
      LOG.trace("computing stats for HbaseScan on " + tbl.getHBaseTableName());
    }
    ValueRange rowRange = keyRanges_.get(0);
    if (isEmpty_) {
      cardinality_ = 0;
    } else if (rowRange != null && rowRange.isEqRange()) {
      cardinality_ = 1;
    } else if (inputCardinality_ >= 0) {
      // We have run computeStats successfully. Don't need to estimate cardinality again
      // (IMPALA-8912). Check some invariants if computeStats has been called.
      Preconditions.checkState(numNodes_ > 0);
      Preconditions.checkState(numInstances_ > 0);
      Preconditions.checkState(cardinality_ >= 0);
      cardinality_ = inputCardinality_;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Reuse last stats: inputCardinality_=" + inputCardinality_);
      }
    } else {
      Pair<Long, Long> estimate;
      if (analyzer.getQueryOptions().isDisable_hbase_num_rows_estimate()) {
        estimate = new Pair<>(-1L, -1L);
      } else {
        // Set maxCaching so that each fetch from hbase won't return a batch of more than
        // MAX_HBASE_FETCH_BATCH_SIZE bytes.
        // May return -1 for the estimate if insufficient data is available.
        estimate = tbl.getEstimatedRowStats(startKey_, stopKey_);
      }
      long rowsFromHms = tbl.getTTableStats().getNum_rows();
      if (estimate.first == -1) {
        // No useful estimate. Rely on HMS row count stats.
        // This works only if HBase stats are available in HMS. This is true
        // for the Impala tests, and may be true for some applications.
        cardinality_ = rowsFromHms;
        if (LOG.isTraceEnabled()) {
          LOG.trace("Fallback to use table stats in HMS: num_rows=" + cardinality_);
        }
        // TODO: What do do if neither HBase nor HMS provide a row count estimate?
        // Is there some third, ulitimate fallback?
        // Apply estimated key range selectivity from original key conjuncts
        if (cardinality_ > 0 && keyConjuncts_ != null) {
          cardinality_ =
            applySelectivity(cardinality_, computeCombinedSelectivity(keyConjuncts_));
        }
      } else {
        // Use the HBase sampling scan to estimate cardinality. Note that,
        // in tests, this estimate has proven to be very rough: off by 2x or more.
        // Cap the cardinality estimation by the row count from HMS when available.
        cardinality_ = (rowsFromHms >= 0) ? Long.min(estimate.first, rowsFromHms) :
                                              estimate.first;
        if (estimate.second > 0) {
          suggestedCaching_ = (int)
              Math.max(MAX_HBASE_FETCH_BATCH_SIZE / estimate.second, 1);
        }
      }
    }
    inputCardinality_ = cardinality_;

    if (cardinality_ > 0) {
      cardinality_ = applyConjunctsSelectivity(cardinality_);
    } else {
      // Safe guard for cardinality_ < -1, e.g. when hbase sampling fails and numRows
      // in HMS is abnormally set to be < -1.
      cardinality_ = Math.max(-1, cardinality_);
    }
    cardinality_ = capCardinalityAtLimit(cardinality_);
    if (LOG.isTraceEnabled()) {
      LOG.trace("computeStats HbaseScan: cardinality=" + cardinality_);
    }

    // Assume that each node/instance in the cluster gets a scan range, unless there are
    // fewer scan ranges than nodes/instances.
    int numExecutors = analyzer.numExecutorsForPlanning();
    numNodes_ =
        Math.max(1, Math.min(scanRangeSpecs_.getConcrete_rangesSize(), numExecutors));
    int maxInstances = numNodes_ * getMaxInstancesPerNode(analyzer);
    numInstances_ =
        Math.max(1, Math.min(scanRangeSpecs_.getConcrete_rangesSize(), maxInstances));
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "computeStats HbaseScan: #nodes=" + numNodes_ + " #instances=" + numInstances_);
    }
  }

  @Override
  protected String debugString() {
    FeHBaseTable tbl = (FeHBaseTable) desc_.getTable();
    return MoreObjects.toStringHelper(this)
        .add("tid", desc_.getId().asInt())
        .add("hiveTblName", tbl.getFullName())
        .add("hbaseTblName", tbl.getHBaseTableName())
        .add("startKey", ByteBuffer.wrap(startKey_).toString())
        .add("stopKey", ByteBuffer.wrap(stopKey_).toString())
        .add("isEmpty", isEmpty_)
        .addValue(super.debugString())
        .toString();
  }

  // We convert predicates of the form <slotref> op <constant> where slotref is of
  // type string to HBase filters. All these predicates are also evaluated at
  // the HBaseScanNode. To properly filter out NULL values HBaseScanNode treats all
  // predicates as disjunctive, thereby requiring re-evaluation when there are multiple
  // attributes. We explicitly materialize the referenced slots, otherwise our hbase
  // scans don't return correct data.
  // TODO: expand this to generate nested filter lists for arbitrary conjunctions
  // and disjunctions.
  private void createHBaseFilters(Analyzer analyzer) {
    for (Expr e: conjuncts_) {
      // We only consider binary predicates
      if (!(e instanceof BinaryPredicate)) continue;
      BinaryPredicate bp = (BinaryPredicate) e;
      CompareFilter.CompareOp hbaseOp = impalaOpToHBaseOp(bp.getOp());
      // Ignore unsupported ops
      if (hbaseOp == null) continue;

      for (SlotDescriptor slot: desc_.getSlots()) {
        // Only push down predicates on string columns
        if (slot.getType().getPrimitiveType() != PrimitiveType.STRING) continue;

        Expr bindingExpr = bp.getSlotBinding(slot.getId());
        if (bindingExpr == null || !(bindingExpr instanceof StringLiteral)) continue;

        StringLiteral literal = (StringLiteral) bindingExpr;
        HBaseColumn col = (HBaseColumn) slot.getColumn();
        // IMPALA-7929: Since the qualifier can be null (e.g. for the key column of an
        // HBase table), the qualifier field must be optional in order to express the
        // null value. Constructors in Thrift do not set optional fields, so the qualifier
        // must be set separately.
        THBaseFilter thbf = new THBaseFilter(col.getColumnFamily(),
            (byte) hbaseOp.ordinal(), literal.getUnescapedValue());
        thbf.setQualifier(col.getColumnQualifier());
        filters_.add(thbf);
        analyzer.materializeSlots(Lists.newArrayList(e));
      }
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HBASE_SCAN_NODE;
    FeHBaseTable tbl = (FeHBaseTable) desc_.getTable();
    msg.hbase_scan_node =
      new THBaseScanNode(desc_.getId().asInt(), tbl.getHBaseTableName());
    if (!filters_.isEmpty()) {
      msg.hbase_scan_node.setFilters(filters_);
    }
    msg.hbase_scan_node.setSuggested_max_caching(suggestedCaching_);
  }

  /**
   * We create a TScanRange for each region server that contains at least one
   * relevant region, and the created TScanRange will contain all the relevant regions
   * of that region server.
   */
  private void computeScanRangeLocations(Analyzer analyzer) {
    scanRangeSpecs_ = new TScanRangeSpec();

    // For empty scan node, return an empty list.
    if (isEmpty_) return;

    // Retrieve relevant HBase regions and their region servers
    FeHBaseTable tbl = (FeHBaseTable) desc_.getTable();
    List<HRegionLocation> regionsLoc;
    try {
      regionsLoc = FeHBaseTable.Util.getRegionsInRange(tbl, startKey_, stopKey_);
    } catch (IOException e) {
      throw new RuntimeException(
          "couldn't retrieve HBase table (" + tbl.getHBaseTableName() + ") info:\n"
          + e.getMessage(), e);
    }

    // Convert list of HRegionLocation to Map<hostport, List<HRegionLocation>>.
    // The List<HRegionLocations>'s end up being sorted by start key/end key, because
    // regionsLoc is sorted that way.
    Map<String, List<HRegionLocation>> locationMap = new HashMap<>();
    for (HRegionLocation regionLoc: regionsLoc) {
      String locHostPort = regionLoc.getHostnamePort();
      if (locationMap.containsKey(locHostPort)) {
        locationMap.get(locHostPort).add(regionLoc);
      } else {
        locationMap.put(locHostPort, Lists.newArrayList(regionLoc));
      }
    }

    for (Map.Entry<String, List<HRegionLocation>> locEntry: locationMap.entrySet()) {
      // HBaseTableScanner(backend) initializes a result scanner for each key range.
      // To minimize # of result scanner re-init, create only a single HBaseKeyRange
      // for all adjacent regions on this server.
      THBaseKeyRange keyRange = null;
      byte[] prevEndKey = null;
      for (HRegionLocation regionLoc: locEntry.getValue()) {
        byte[] curRegStartKey = regionLoc.getRegionInfo().getStartKey();
        byte[] curRegEndKey   = regionLoc.getRegionInfo().getEndKey();
        if (prevEndKey != null &&
            Bytes.compareTo(prevEndKey, curRegStartKey) == 0) {
          // the current region starts where the previous one left off;
          // extend the key range
          setKeyRangeEnd(keyRange, curRegEndKey);
        } else {
          // create a new HBaseKeyRange (and TScanRange2/TScanRangeLocationList to go
          // with it).
          keyRange = new THBaseKeyRange();
          setKeyRangeStart(keyRange, curRegStartKey);
          setKeyRangeEnd(keyRange, curRegEndKey);

          TScanRangeLocationList scanRangeLocation = new TScanRangeLocationList();
          TNetworkAddress networkAddress = addressToTNetworkAddress(locEntry.getKey());
          scanRangeLocation.addToLocations(
              new TScanRangeLocation(
                  analyzer.getHostIndex().getOrAddIndex(networkAddress)));

          TScanRange scanRange = new TScanRange();
          scanRange.setHbase_key_range(keyRange);
          scanRangeLocation.setScan_range(scanRange);

          scanRangeSpecs_.addToConcrete_ranges(scanRangeLocation);
        }
        prevEndKey = curRegEndKey;
      }
    }
  }

  /**
   * Set the start key of keyRange using the provided key, bounded by startKey_
   * @param keyRange the keyRange to be updated
   * @param rangeStartKey the start key value to be set to
   */
  private void setKeyRangeStart(THBaseKeyRange keyRange, byte[] rangeStartKey) {
    keyRange.unsetStartKey();
    // use the max(startKey, rangeStartKey) for scan start
    if (!Bytes.equals(rangeStartKey, HConstants.EMPTY_START_ROW) ||
        !Bytes.equals(startKey_, HConstants.EMPTY_START_ROW)) {
      byte[] partStart = (Bytes.compareTo(rangeStartKey, startKey_) < 0) ?
          startKey_ : rangeStartKey;
      keyRange.setStartKey(Bytes.toString(partStart));
    }
  }

  /**
   * Set the end key of keyRange using the provided key, bounded by stopKey_
   * @param keyRange the keyRange to be updated
   * @param rangeEndKey the end key value to be set to
   */
  private void setKeyRangeEnd(THBaseKeyRange keyRange, byte[] rangeEndKey) {
    keyRange.unsetStopKey();
    // use the min(stopkey, regionStopKey) for scan stop
    if (!Bytes.equals(rangeEndKey, HConstants.EMPTY_END_ROW) ||
        !Bytes.equals(stopKey_, HConstants.EMPTY_END_ROW)) {
      if (Bytes.equals(stopKey_, HConstants.EMPTY_END_ROW)) {
        keyRange.setStopKey(Bytes.toString(rangeEndKey));
      } else if (Bytes.equals(rangeEndKey, HConstants.EMPTY_END_ROW)) {
        keyRange.setStopKey(Bytes.toString(stopKey_));
      } else {
        byte[] partEnd = (Bytes.compareTo(rangeEndKey, stopKey_) < 0) ?
            rangeEndKey : stopKey_;
        keyRange.setStopKey(Bytes.toString(partEnd));
      }
    }
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    FeHBaseTable table = (FeHBaseTable) desc_.getTable();
    StringBuilder output = new StringBuilder();
    if (isEmpty_) {
      output.append(prefix + "empty scan node\n");
      return output.toString();
    }
    String aliasStr = "";
    if (!table.getFullName().equalsIgnoreCase(desc_.getAlias()) &&
        !table.getName().equalsIgnoreCase(desc_.getAlias())) {
      aliasStr = " " + desc_.getAlias();
    }
    output.append(String.format("%s%s:%s [%s%s]\n", prefix, id_.toString(),
        displayName_, table.getFullName(), aliasStr));
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (!keyConjuncts_.isEmpty()) {
        output.append(detailPrefix + "key predicates: " +
            Expr.getExplainString(keyConjuncts_, detailLevel) + "\n");
      }
      if (!Bytes.equals(startKey_, HConstants.EMPTY_START_ROW)) {
        output.append(detailPrefix + "start key: " + printKey(startKey_) + "\n");
      }
      if (!Bytes.equals(stopKey_, HConstants.EMPTY_END_ROW)) {
        output.append(detailPrefix + "stop key: " + printKey(stopKey_) + "\n");
      }
      if (!filters_.isEmpty()) {
        output.append(detailPrefix + "hbase filters:");
        if (filters_.size() == 1) {
          THBaseFilter filter = filters_.get(0);
          output.append(" " + filter.family + ":" + filter.qualifier + " " +
              CompareFilter.CompareOp.values()[filter.op_ordinal].toString() + " " +
              "'" + filter.filter_constant + "'");
        } else {
          for (int i = 0; i < filters_.size(); ++i) {
            THBaseFilter filter = filters_.get(i);
            output.append("\n" + detailPrefix + filter.family + ":" + filter.qualifier
                + " " + CompareFilter.CompareOp.values()[filter.op_ordinal].toString()
                + " " + "'" + filter.filter_constant + "'");
          }
        }
        output.append('\n');
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix
            + "predicates: " + Expr.getExplainString(conjuncts_, detailLevel) + "\n");
      }
    }
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(getStatsExplainString(detailPrefix));
      output.append("\n");
    }
    return output.toString();
  }

  /**
   * Convert key into byte array and append a '\0' if 'nextKey' is true.
   */
  private byte[] convertToBytes(String rowKey, boolean nextKey) {
    byte[] keyBytes = Bytes.toBytes(rowKey);
    if (!nextKey) {
      return keyBytes;
    } else {
      // append \0
      return Arrays.copyOf(keyBytes, keyBytes.length + 1);
    }
  }

  /**
   * Prints non-printable characters in escaped octal, otherwise outputs
   * the characters.
   */
  public static String printKey(byte[] key) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < key.length; ++i) {
      if (!Character.isISOControl(key[i])) {
        result.append((char) key[i]);
      } else {
        result.append("\\");
        result.append(Integer.toOctalString(key[i]));
      }
    }
    return result.toString();
  }

  private static CompareFilter.CompareOp impalaOpToHBaseOp(
      BinaryPredicate.Operator impalaOp) {
    switch(impalaOp) {
      case EQ: return CompareFilter.CompareOp.EQUAL;
      case NE: return CompareFilter.CompareOp.NOT_EQUAL;
      case GT: return CompareFilter.CompareOp.GREATER;
      case GE: return CompareFilter.CompareOp.GREATER_OR_EQUAL;
      case LT: return CompareFilter.CompareOp.LESS;
      case LE: return CompareFilter.CompareOp.LESS_OR_EQUAL;
      // TODO: Add support for pushing LIKE/REGEX down to HBase with a different Filter.
      default: throw new IllegalArgumentException(
          "HBase: Unsupported Impala compare operator: " + impalaOp);
    }
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = computeScanProcessingCost(queryOptions);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    FeHBaseTable tbl = (FeHBaseTable) desc_.getTable();
    // The first column in an HBase table is always the key column.
    HBaseColumn keyCol = (HBaseColumn) tbl.getColumns().get(0);

    List<HBaseColumn> colsToFetchFromHBase = new ArrayList<>();
    for (SlotDescriptor slot : desc_.getSlots()) {
      HBaseColumn col = (HBaseColumn) tbl.getColumn(slot.getLabel());
      // Will add key column separately, since its always fetched.
      if (col.getColumnFamily().equals(FeHBaseTable.Util.ROW_KEY_COLUMN_FAMILY)) continue;
      colsToFetchFromHBase.add(col);
    }
    // Add the key column.
    colsToFetchFromHBase.add(keyCol);
    long mem_estimate = memoryEstimateForFetchingColumns(colsToFetchFromHBase);
    mem_estimate = Math.max(mem_estimate, DEFAULT_MIN_ESTIMATE_BYTES);
    nodeResourceProfile_ = ResourceProfile.noReservation(mem_estimate);
  }

  /**
   * Returns an estimate of memory required by the HBase scan node for fetching
   * the given list of HBase columns. Primarily used as a helper function but
   * also exposed at the package level for testing.
   */
  protected static long memoryEstimateForFetchingColumns(List<HBaseColumn> columns) {
    // In HBase, every column value is stored in the following format:
    // http://hbase.apache.org/0.94/book/regions.arch.html#keyvalue
    // and out of this only rowKey per row and (columnFamily, columnQualifier and
    // columnValue) per column per row are allocated. The following estimations are based
    // on that and its interaction with the mem-pool. Currently, we do an
    // allocate-clear cycle on mem-pool for each row fetched from HBase (See
    // HBaseTableScanner::Next()). To get an approx upper limit on mem
    // allocation, we take the max row size possible and assume all possible
    // chunk sizes below it have been allocated in previous row iterations. So,
    // for a value of n bytes the max possible mem allocation for a mem pool
    // that only uses power of 2 chunk sizes will be:
    // (2^(ceil((log(n))+1) - 1) ~ 2 * BitUtil.roundUpToPowerOf2(n)
    long maxRowSize = 0;
    boolean isMissingStats = false;
    for (HBaseColumn col : columns) {
      long colMaxSize = col.getStats().getMaxSize();
      if (col.getType().isStringType()) {
        if (colMaxSize == -1) {
          colMaxSize = DEFAULT_STRING_COL_BYTES;
          isMissingStats = true;
        }
        // Round off string col size to next power of 2. For strings less than
        // 512 KB (MemPool::MAX_CHUNK_SIZE) this ensures enough contribution to
        // the max row size so that the final round off can accommodate any
        // fluctuations in size. For larger strings, this approximates that it
        // completely takes up the new chunk allocated for it.
        colMaxSize = BitUtil.roundUpToPowerOf2(colMaxSize);
      }
      Preconditions.checkState(colMaxSize != -1);
      maxRowSize += colMaxSize;
      if (!col.getColumnFamily().equals(FeHBaseTable.Util.ROW_KEY_COLUMN_FAMILY)) {
        // For non-key columns, their respective column family and column qualifier
        // strings need to be fetched and mem needs to be allocated for that too.
        maxRowSize += col.getColumnFamily().length() + col.getColumnQualifier().length();
      }
    }
    // Use the max allocation assuming all possible chunk sizes below it have
    // been allocated.
    long mem_estimate = BitUtil.roundUpToPowerOf2(maxRowSize) * 2;
    // We use a default of 32 KB for string cols that dont have max length set.
    // Assuming such large cols are uncommon we set an upper limit to avoid extreme
    // overestimation.
    if (isMissingStats) mem_estimate = Math.min(mem_estimate, DEFAULT_MAX_ESTIMATE_BYTES);
    return mem_estimate;
  }

  @Override
  public boolean hasStorageLayerConjuncts() { return !filters_.isEmpty(); }
}
