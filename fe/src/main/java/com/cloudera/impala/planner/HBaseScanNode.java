// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HBaseColumn;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.THBaseFilter;
import com.cloudera.impala.thrift.THBaseKeyRange;
import com.cloudera.impala.thrift.THBaseScanNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TScanRange;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;


/**
 * Full scan of an HBase table.
 * Only families/qualifiers specified in TupleDescriptor will be retrieved in the backend.
 */
public class HBaseScanNode extends ScanNode {
  private final TupleDescriptor desc;

  // derived from keyRanges
  private byte[] startKey;
  private byte[] stopKey;

  // List of HBase Filters for generating thrift message. Filled in finalize().
  private final List<THBaseFilter> filters = new ArrayList<THBaseFilter>();

  public HBaseScanNode(TupleDescriptor desc) {
    super(desc);
    this.desc = desc;
  }

  @Override
  public void finalize(Analyzer analyzer) throws InternalException {
    Preconditions.checkState(keyRanges == null || keyRanges.size() == 1);
    if (keyRanges != null) {
      // Transform ValueRange into start-/stoprow by printing the values.
      // At present, we only do that for string-mapped keys because the hbase
      // data is stored as text.
      ValueRange rowRange = keyRanges.get(0);
      if (rowRange.lowerBound != null) {
        Preconditions.checkState(rowRange.lowerBound.isConstant());
        Preconditions.checkState(rowRange.lowerBound instanceof StringLiteral);
        startKey = convertToBytes(((StringLiteral) rowRange.lowerBound).getValue(),
                                  !rowRange.lowerBoundInclusive);
      }
      if (rowRange.upperBound != null) {
        Preconditions.checkState(rowRange.upperBound.isConstant());
        Preconditions.checkState(rowRange.upperBound instanceof StringLiteral);
        stopKey = convertToBytes(((StringLiteral) rowRange.upperBound).getValue(),
                                  rowRange.upperBoundInclusive);
      }
    }
    // Convert predicates to HBase filters.
    createHBaseFilters(analyzer);
  }

  @Override
  protected String debugString() {
    HBaseTable tbl = (HBaseTable) desc.getTable();
    return Objects.toStringHelper(this)
        .add("tid", desc.getId().asInt())
        .add("hiveTblName", tbl.getFullName())
        .add("hbaseTblName", tbl.getHBaseTableName())
        .add("startKey", ByteBuffer.wrap(startKey).toString())
        .add("stopKey", ByteBuffer.wrap(stopKey).toString())
        .addValue(super.debugString())
        .toString();
  }

  // We convert predicates of the form <slotref> op <constant> where slotref is of type string
  // to HBase filters. We remove the corresponding predicate from the conjuncts.
  // TODO: expand this to generate nested filter lists for arbitrary conjunctions and disjunctions.
  private void createHBaseFilters(Analyzer analyzer) {
    for (SlotDescriptor slot: desc.getSlots()) {
      // TODO: Currently we can only push down predicates on string columns.
      if (slot.getType() != PrimitiveType.STRING) {
        continue;
      }
      // List of predicates that cannot be pushed down as an HBase Filter.
      List<Predicate> remainingPreds = new ArrayList<Predicate>();
      for (Predicate p: conjuncts) {
        if (!(p instanceof BinaryPredicate)) {
          remainingPreds.add(p);
          continue;
        }
        BinaryPredicate bp = (BinaryPredicate) p;
        Expr bindingExpr = bp.getSlotBinding(slot.getId());
        if (bindingExpr == null || !(bindingExpr instanceof StringLiteral)) {
          remainingPreds.add(p);
          continue;
        }
        CompareFilter.CompareOp hbaseOp = impalaOpToHBaseOp(bp.getOp());
        // Currently unsupported op, leave it as a predicate.
        if (hbaseOp == null) {
          remainingPreds.add(p);
          continue;
        }
        StringLiteral literal = (StringLiteral) bindingExpr;
        HBaseColumn col = (HBaseColumn) slot.getColumn();
        filters.add(new THBaseFilter(col.getColumnFamily(), col.getColumnQualifier(),
              (byte) hbaseOp.ordinal(), literal.getValue()));
      }
      conjuncts = remainingPreds;
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HBASE_SCAN_NODE;
    HBaseTable tbl = (HBaseTable) desc.getTable();
    msg.hbase_scan_node = new THBaseScanNode(desc.getId().asInt(), tbl.getHBaseTableName());
    if (!filters.isEmpty()) {
      msg.hbase_scan_node.setFilters(filters);
    }
  }

  @Override
  public void getScanParams(
      int numPartitions, List<TScanRange> scanRanges, List<String> hosts) {
    TScanRange scanRange = new TScanRange(id);
    if (startKey != null || stopKey != null) {
      THBaseKeyRange keyRange = new THBaseKeyRange();
      if (startKey != null) {
        keyRange.setStartKey(Bytes.toString(startKey));
      }
      if (stopKey != null) {
        keyRange.setStopKey(Bytes.toString(stopKey));
      }
      scanRange.setHbaseKeyRange(keyRange);
    }
    scanRanges.add(scanRange);
  }

  @Override
  protected String getExplainString(String prefix) {
    HBaseTable tbl = (HBaseTable) desc.getTable();
    StringBuilder output = new StringBuilder()
        .append(prefix + "SCAN HBASE table=" + tbl.getName() + "\n");
    if (startKey != null) {
      output.append(prefix + "  START KEY: " + printKey(startKey) + "\n");
    }
    if (stopKey != null) {
      output.append(prefix + "  STOP KEY: " + printKey(stopKey) + "\n");
    }
    if (!filters.isEmpty()) {
      output.append(prefix + "  HBASE FILTERS: ");
      if (filters.size() == 1) {
        THBaseFilter filter = filters.get(0);
        output.append(filter.family + ":" + filter.qualifier + " " +
            CompareFilter.CompareOp.values()[filter.op_ordinal].toString() + " " +
            "'" + filter.filter_constant + "'");
      } else {
        for (int i = 0; i < filters.size(); ++i) {
          THBaseFilter filter = filters.get(i);
          output.append("\n  " + filter.family + ":" + filter.qualifier + " " +
              CompareFilter.CompareOp.values()[filter.op_ordinal].toString() + " " +
              "'" + filter.filter_constant + "'");
        }
      }
      output.append('\n');
    }
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts));
    }
    output.append(super.getExplainString(prefix));
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
  private String printKey(byte[] key) {
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

  private static CompareFilter.CompareOp impalaOpToHBaseOp(BinaryPredicate.Operator impalaOp) {
    switch(impalaOp) {
      case EQ: return CompareFilter.CompareOp.EQUAL;
      case NE: return CompareFilter.CompareOp.NOT_EQUAL;
      case GT: return CompareFilter.CompareOp.GREATER;
      case GE: return CompareFilter.CompareOp.GREATER_OR_EQUAL;
      case LT: return CompareFilter.CompareOp.LESS;
      case LE: return CompareFilter.CompareOp.LESS_OR_EQUAL;
      // TODO: Add support for pushing LIKE/REGEX down to HBase with a different Filter.
      default: throw null;
    }
  }
}
