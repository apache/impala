// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.THBaseScanNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
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

  public HBaseScanNode(TupleDescriptor desc) {
    super(desc);
    this.desc = desc;
  }

  @Override
  public void finalize(Analyzer analyzer) throws InternalException {
    Preconditions.checkState(keyRanges == null || keyRanges.size() == 1);
    if (keyRanges == null) {
      return;
    }

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

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HBASE_SCAN_NODE;
    HBaseTable tbl = (HBaseTable) desc.getTable();
    msg.hbase_scan_node = new THBaseScanNode(desc.getId().asInt(), tbl.getHBaseTableName());
    if (startKey != null) {
      msg.hbase_scan_node.setStart_key(Bytes.toString(startKey));
    }
    if (stopKey != null) {
      msg.hbase_scan_node.setStop_key(Bytes.toString(stopKey));
    }
  }

  @Override
  protected String getExplainString(String prefix) {
    HBaseTable tbl = (HBaseTable) desc.getTable();
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN HBASE table=" + tbl.getName() + "\n");
    if (startKey != null) {
      output.append(prefix + "  START KEY: " + printKey(startKey) + "\n");
    }
    if (stopKey != null) {
      output.append(prefix + "  STOP KEY: " + printKey(stopKey) + "\n");
    }
    output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts) + "\n");
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
}
