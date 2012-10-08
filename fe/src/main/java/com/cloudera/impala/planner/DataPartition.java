// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.thrift.TDataPartition;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPartitionType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Specification of the partition of a single stream of data.
 * Examples of those streams of data are: the scan of a table; the output
 * of a plan fragment; etc. (ie, this is not restricted to direct exchanges
 * between two fragments, which in the backend is facilitated by the classes
 * DataStreamSender/DataStreamMgr/DataStreamRecvr).
 * TODO: better name? just Partitioning?
 */
public class DataPartition {
  private final static Logger LOG = LoggerFactory.getLogger(DataPartition.class);

  private final TPartitionType type;

  // for hash partition: exprs used to compute hash value
  private final ImmutableList<Expr> partitionExprs;

  public DataPartition(TPartitionType type, List<Expr> exprs) {
    Preconditions.checkNotNull(exprs);
    Preconditions.checkState(!exprs.isEmpty());
    Preconditions.checkState(type == TPartitionType.HASH_PARTITIONED
        || type == TPartitionType.RANGE_PARTITIONED);
    this.type = type;
    this.partitionExprs = ImmutableList.copyOf(exprs);
  }

  public DataPartition(TPartitionType type) {
    Preconditions.checkState(type == TPartitionType.UNPARTITIONED
        || type == TPartitionType.RANDOM);
    this.type = type;
    this.partitionExprs = ImmutableList.of();
  }

  public final static DataPartition UNPARTITIONED =
      new DataPartition(TPartitionType.UNPARTITIONED);

  public final static DataPartition RANDOM =
      new DataPartition(TPartitionType.RANDOM);

  public boolean isPartitioned() {
    return type != TPartitionType.UNPARTITIONED;
  }

  public TPartitionType getType() {
    return type;
  }

  public TDataPartition toThrift() {
    TDataPartition result = new TDataPartition(type);
    if (partitionExprs != null) {
      result.setPartitioning_exprs(Expr.treesToThrift(partitionExprs));
    }
    return result;
  }

  /**
   * Returns true if 'this' is a partition that is compatible with the
   * requirements of 's'.
   * TODO: specify more clearly and implement
   */
  public boolean isCompatible(DataPartition s) {
    // TODO: implement
    return true;
  }

  public String getExplainString(TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    str.append(type.toString());
    if (!partitionExprs.isEmpty()) {
      List<String> strings = Lists.newArrayList();
      for (Expr expr: partitionExprs) {
        strings.add(expr.toSql());
      }
      str.append(": " + Joiner.on(", ").join(strings));
    }
    str.append("\n");
    return str.toString();
  }
}
