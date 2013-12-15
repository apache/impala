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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.thrift.TDataPartition;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPartitionType;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
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

  private final TPartitionType type_;

  // for hash partition: exprs used to compute hash value
  private final ImmutableList<Expr> partitionExprs_;

  public DataPartition(TPartitionType type, List<Expr> exprs) {
    Preconditions.checkNotNull(exprs);
    Preconditions.checkState(!exprs.isEmpty());
    Preconditions.checkState(type == TPartitionType.HASH_PARTITIONED
        || type == TPartitionType.RANGE_PARTITIONED);
    type_ = type;
    partitionExprs_ = ImmutableList.copyOf(exprs);
  }

  public DataPartition(TPartitionType type) {
    Preconditions.checkState(type == TPartitionType.UNPARTITIONED
        || type == TPartitionType.RANDOM);
    type_ = type;
    partitionExprs_ = ImmutableList.of();
  }

  public final static DataPartition UNPARTITIONED =
      new DataPartition(TPartitionType.UNPARTITIONED);

  public final static DataPartition RANDOM =
      new DataPartition(TPartitionType.RANDOM);

  public boolean isPartitioned() { return type_ != TPartitionType.UNPARTITIONED; }
  public TPartitionType getType() { return type_; }
  public ImmutableList<Expr> getPartitionExprs() { return partitionExprs_; }

  public TDataPartition toThrift() {
    TDataPartition result = new TDataPartition(type_);
    if (partitionExprs_ != null) {
      result.setPartition_exprs(Expr.treesToThrift(partitionExprs_));
    }
    return result;
  }

  public String debugString() {
    return Objects.toStringHelper(this)
        .add("type_", type_)
        .addValue(Expr.debugString(partitionExprs_))
        .toString();
  }

  public String getExplainString(TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    str.append(type_.toString());
    if (!partitionExprs_.isEmpty()) {
      List<String> strings = Lists.newArrayList();
      for (Expr expr: partitionExprs_) {
        strings.add(expr.toSql());
      }
      str.append(": " + Joiner.on(", ").join(strings));
    }
    str.append("\n");
    return str.toString();
  }
}
