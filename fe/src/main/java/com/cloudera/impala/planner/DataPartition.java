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

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.thrift.TDataPartition;
import com.cloudera.impala.thrift.TPartitionType;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Specification of the partition of a single stream of data.
 * Examples of those streams of data are: the scan of a table; the output
 * of a plan fragment; etc. (ie, this is not restricted to direct exchanges
 * between two fragments, which in the backend is facilitated by the classes
 * DataStreamSender/DataStreamMgr/DataStreamRecvr).
 */
public class DataPartition {
  private final static Logger LOG = LoggerFactory.getLogger(DataPartition.class);

  private final TPartitionType type_;

  // for hash partition: exprs used to compute hash value
  private List<Expr> partitionExprs_;

  public DataPartition(TPartitionType type, List<Expr> exprs) {
    Preconditions.checkNotNull(exprs);
    Preconditions.checkState(!exprs.isEmpty());
    Preconditions.checkState(type == TPartitionType.HASH_PARTITIONED
        || type == TPartitionType.RANGE_PARTITIONED);
    type_ = type;
    partitionExprs_ = exprs;
  }

  public DataPartition(TPartitionType type) {
    Preconditions.checkState(type == TPartitionType.UNPARTITIONED
        || type == TPartitionType.RANDOM);
    type_ = type;
    partitionExprs_ = Lists.newArrayList();
  }

  public final static DataPartition UNPARTITIONED =
      new DataPartition(TPartitionType.UNPARTITIONED);

  public final static DataPartition RANDOM =
      new DataPartition(TPartitionType.RANDOM);

  public boolean isPartitioned() { return type_ != TPartitionType.UNPARTITIONED; }
  public boolean isHashPartitioned() { return type_ == TPartitionType.HASH_PARTITIONED; }
  public TPartitionType getType() { return type_; }
  public List<Expr> getPartitionExprs() { return partitionExprs_; }

  public void substitute(ExprSubstitutionMap smap, Analyzer analyzer) {
    partitionExprs_ = Expr.substituteList(partitionExprs_, smap, analyzer, false);
  }

  public TDataPartition toThrift() {
    TDataPartition result = new TDataPartition(type_);
    if (partitionExprs_ != null) {
      result.setPartition_exprs(Expr.treesToThrift(partitionExprs_));
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    DataPartition other = (DataPartition) obj;
    if (type_ != other.type_) return false;
    return Expr.equalLists(partitionExprs_, other.partitionExprs_);
  }

  public String debugString() {
    return Objects.toStringHelper(this)
        .add("type_", type_)
        .addValue(Expr.debugString(partitionExprs_))
        .toString();
  }

  public String getExplainString() {
    StringBuilder str = new StringBuilder();
    str.append(getPartitionShortName(type_));
    if (!partitionExprs_.isEmpty()) {
      List<String> strings = Lists.newArrayList();
      for (Expr expr: partitionExprs_) {
        strings.add(expr.toSql());
      }
      str.append("(" + Joiner.on(",").join(strings) +")");
    }
    return str.toString();
  }

  private String getPartitionShortName(TPartitionType partition) {
    switch (partition) {
      case RANDOM: return "RANDOM";
      case HASH_PARTITIONED: return "HASH";
      case RANGE_PARTITIONED: return "RANGE";
      case UNPARTITIONED: return "UNPARTITIONED";
      default: return "";
    }
  }
}
