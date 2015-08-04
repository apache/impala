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

package com.cloudera.impala.planner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HBaseColumn;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THBaseFilter;
import com.cloudera.impala.thrift.THBaseKeyRange;
import com.cloudera.impala.thrift.THBaseScanNode;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.cloudera.impala.thrift.TScanRange;
import com.cloudera.impala.thrift.TScanRangeLocation;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Full scan of an HBase table.
 * Only families/qualifiers specified in TupleDescriptor will be retrieved in the backend.
 */
public class HBaseScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(HBaseScanNode.class);
  private final TupleDescriptor desc_;

  // One range per clustering column. The range bounds are expected to be constants.
  // A null entry means there's no range restriction for that particular key.
  // If keyRanges is non-null it always contains as many entries as there are clustering
  // cols.
  private List<ValueRange> keyRanges_;

  // derived from keyRanges_; empty means unbounded;
  // initialize start/stopKey_ to be unbounded.
  private byte[] startKey_ = HConstants.EMPTY_START_ROW;
  private byte[] stopKey_ = HConstants.EMPTY_END_ROW;

  // True if this scan node is not going to scan anything. If the row key filter
  // evaluates to null, or if the lower bound > upper bound, then this scan node won't
  // scan at all.
  private boolean isEmpty_ = false;

  // List of HBase Filters for generating thrift message. Filled in finalize().
  private final List<THBaseFilter> filters_ = new ArrayList<THBaseFilter>();

  // The suggested value for "hbase.client.scan.setCaching", which batches maxCaching
  // rows per fetch request to the HBase region server. If the value is too high,
  // then the hbase region server will have a hard time (GC pressure and long response
  // times). If the value is too small, then there will be extra trips to the hbase
  // region server.
  // Default to 1024 and update it based on row size estimate such that each batch size
  // won't exceed 500MB.
  private final static int MAX_HBASE_FETCH_BATCH_SIZE = 500 * 1024 * 1024;
  private final static int DEFAULT_SUGGESTED_CACHING = 1024;
  private int suggestedCaching_ = DEFAULT_SUGGESTED_CACHING;

  // HBase config; Common across all object instance.
  private static Configuration hbaseConf_ = HBaseConfiguration.create();

  public HBaseScanNode(PlanNodeId id, TupleDescriptor desc) {
    super(id, desc, "SCAN HBASE");
    desc_ = desc;
  }

  public void setKeyRanges(List<ValueRange> keyRanges) {
    Preconditions.checkNotNull(keyRanges);
    keyRanges_ = keyRanges;
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    checkForSupportedFileFormats();
    assignConjuncts(analyzer);
    setStartStopKey(analyzer);
    // Convert predicates to HBase filters_.
    createHBaseFilters(analyzer);

    // materialize slots in remaining conjuncts_
    analyzer.materializeSlots(conjuncts_);
    computeMemLayout(analyzer);
    computeScanRangeLocations(analyzer);

    // Call computeStats() after materializing slots and computing the mem layout.
    computeStats(analyzer);
  }

  /**
   * Convert keyRanges_ to startKey_ and stopKey_.
   * If ValueRange is not null, transform it into start/stopKey_ by evaluating the
   * expression. Analysis has checked that the expression is string type. If the
   * expression evaluates to null, then there's nothing to scan because Hbase row key
   * cannot be null.
   * At present, we only do row key filtering for string-mapped keys. String-mapped keys
   * are always encded as ascii.
   * ValueRange is null if there is no predicate on the row-key.
   */
  private void setStartStopKey(Analyzer analyzer) throws InternalException {
    Preconditions.checkNotNull(keyRanges_);
    Preconditions.checkState(keyRanges_.size() == 1);

    ValueRange rowRange = keyRanges_.get(0);
    if (rowRange != null) {
      if (rowRange.getLowerBound() != null) {
        Preconditions.checkState(rowRange.getLowerBound().isConstant());
        Preconditions.checkState(
            rowRange.getLowerBound().getType().equals(Type.STRING));
        TColumnValue val = FeSupport.EvalConstExpr(rowRange.getLowerBound(),
            analyzer.getQueryCtx());
        if (!val.isSetString_val()) {
          // lower bound is null.
          isEmpty_ = true;
          return;
        } else {
          startKey_ = convertToBytes(val.getString_val(),
              !rowRange.getLowerBoundInclusive());
        }
      }
      if (rowRange.getUpperBound() != null) {
        Preconditions.checkState(rowRange.getUpperBound().isConstant());
        Preconditions.checkState(
            rowRange.getUpperBound().getType().equals(Type.STRING));
        TColumnValue val = FeSupport.EvalConstExpr(rowRange.getUpperBound(),
            analyzer.getQueryCtx());
        if (!val.isSetString_val()) {
          // upper bound is null.
          isEmpty_ = true;
          return;
        } else {
          stopKey_ = convertToBytes(val.getString_val(),
              rowRange.getUpperBoundInclusive());
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
    HBaseTable tbl = (HBaseTable) desc_.getTable();

    ValueRange rowRange = keyRanges_.get(0);
    if (isEmpty_) {
      cardinality_ = 0;
    } else if (rowRange != null && rowRange.isEqRange()) {
      cardinality_ = 1;
    } else {
      // Set maxCaching so that each fetch from hbase won't return a batch of more than
      // MAX_HBASE_FETCH_BATCH_SIZE bytes.
      Pair<Long, Long> estimate = tbl.getEstimatedRowStats(startKey_, stopKey_);
      cardinality_ = estimate.first.longValue();
      if (estimate.second.longValue() > 0) {
        suggestedCaching_ = (int)
            Math.max(MAX_HBASE_FETCH_BATCH_SIZE / estimate.second.longValue(), 1);
      }
    }
    inputCardinality_ = cardinality_;

    cardinality_ *= computeSelectivity();
    cardinality_ = Math.max(1, cardinality_);
    cardinality_ = capAtLimit(cardinality_);
    LOG.debug("computeStats HbaseScan: cardinality=" + Long.toString(cardinality_));

    // TODO: take actual regions into account
    numNodes_ = tbl.getNumNodes();
    LOG.debug("computeStats HbaseScan: #nodes=" + Integer.toString(numNodes_));
  }

  @Override
  protected String debugString() {
    HBaseTable tbl = (HBaseTable) desc_.getTable();
    return Objects.toStringHelper(this)
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
        filters_.add(new THBaseFilter(
            col.getColumnFamily(), col.getColumnQualifier(),
            (byte) hbaseOp.ordinal(), literal.getUnescapedValue()));
        analyzer.materializeSlots(Lists.newArrayList(e));
      }
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HBASE_SCAN_NODE;
    HBaseTable tbl = (HBaseTable) desc_.getTable();
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
    scanRanges_ = Lists.newArrayList();

    // For empty scan node, return an empty list.
    if (isEmpty_) return;

    // Retrieve relevant HBase regions and their region servers
    HBaseTable tbl = (HBaseTable) desc_.getTable();
    HTable hbaseTbl = null;
    List<HRegionLocation> regionsLoc;
    try {
      hbaseTbl = new HTable(hbaseConf_, tbl.getHBaseTableName());
      regionsLoc = HBaseTable.getRegionsInRange(hbaseTbl, startKey_, stopKey_);
    } catch (IOException e) {
      throw new RuntimeException(
          "couldn't retrieve HBase table (" + tbl.getHBaseTableName() + ") info:\n"
          + e.getMessage());
    }

    // Convert list of HRegionLocation to Map<hostport, List<HRegionLocation>>.
    // The List<HRegionLocations>'s end up being sorted by start key/end key, because
    // regionsLoc is sorted that way.
    Map<String, List<HRegionLocation>> locationMap = Maps.newHashMap();
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
          // create a new HBaseKeyRange (and TScanRange2/TScanRangeLocations to go
          // with it).
          keyRange = new THBaseKeyRange();
          setKeyRangeStart(keyRange, curRegStartKey);
          setKeyRangeEnd(keyRange, curRegEndKey);

          TScanRangeLocations scanRangeLocation = new TScanRangeLocations();
          TNetworkAddress networkAddress = addressToTNetworkAddress(locEntry.getKey());
          scanRangeLocation.addToLocations(
              new TScanRangeLocation(analyzer.getHostIndex().getIndex(networkAddress)));
          scanRanges_.add(scanRangeLocation);

          TScanRange scanRange = new TScanRange();
          scanRange.setHbase_key_range(keyRange);
          scanRangeLocation.setScan_range(scanRange);
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
    HBaseTable table = (HBaseTable) desc_.getTable();
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
            output.append("\n  " + filter.family + ":" + filter.qualifier + " " +
                CompareFilter.CompareOp.values()[filter.op_ordinal].toString() + " " +
                "'" + filter.filter_constant + "'");
          }
        }
        output.append('\n');
      }
      if (!conjuncts_.isEmpty()) {
        output.append(
            detailPrefix + "predicates: " + getExplainString(conjuncts_) + "\n");
      }
    }
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(getStatsExplainString(detailPrefix, detailLevel));
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
  public void computeCosts(TQueryOptions queryOptions) {
    // TODO: What's a good estimate of memory consumption?
    perHostMemCost_ = 1024L * 1024L * 1024L;
  }

  /**
   * Returns the per-host upper bound of memory that any number of concurrent scan nodes
   * will use. Used for estimating the per-host memory requirement of queries.
   */
  public static long getPerHostMemUpperBound() {
    // TODO: What's a good estimate of memory consumption?
    return 1024L * 1024L * 1024L;
  }
}
