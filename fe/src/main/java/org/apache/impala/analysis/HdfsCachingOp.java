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

package org.apache.impala.analysis;

import java.math.BigDecimal;

import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.THdfsCachingOp;
import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

/**
 * Represents the partial SQL statement of specifying whether a table/partition
 * should or should not be marked as cached.
 */
public class HdfsCachingOp extends StmtNode {
  private final THdfsCachingOp cacheOp_;
  private final BigDecimal parsedReplication_;

  /**
   * Creates an HdfsCachingOp that specifies the target should be uncached
   */
  public HdfsCachingOp() {
    cacheOp_ = new THdfsCachingOp(false);
    parsedReplication_ = null;
  }

  /**
   * Creates an HdfsCachingOp that specifies the target should be cached in cachePoolName
   * with an optional replication factor
   */
  public HdfsCachingOp(String cachePoolName, BigDecimal replication) {
    cacheOp_ = new THdfsCachingOp(true);
    cacheOp_.setCache_pool_name(cachePoolName);
    parsedReplication_ = replication;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (cacheOp_.isSet_cached()) {
      String poolName = cacheOp_.getCache_pool_name();
      Preconditions.checkNotNull(poolName);
      if (poolName.isEmpty()) {
        throw new AnalysisException("Cache pool name cannot be empty.");
      }

      HdfsCachePool cachePool = analyzer.getCatalog().getHdfsCachePool(poolName);
      if (cachePool == null) {
        throw new AnalysisException(
            "The specified cache pool does not exist: " + poolName);
      }

      if (parsedReplication_ != null && (parsedReplication_.longValue() <= 0 ||
            parsedReplication_.longValue() > Short.MAX_VALUE)) {
          throw new AnalysisException(
              "Cache replication factor must be between 0 and Short.MAX_VALUE");
      }

      if (parsedReplication_ != null) {
        cacheOp_.setReplication(parsedReplication_.shortValue());
      }
    }
  }

  @Override
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    if (!shouldCache()) return "UNCACHED";
    StringBuilder sb = new StringBuilder();
    sb.append("CACHED IN '" + getCachePoolName() + "'");
    if (parsedReplication_ != null) {
      sb.append(" WITH REPLICATION = " + parsedReplication_.longValue());
    }
    return sb.toString();
  }

  public THdfsCachingOp toThrift() { return cacheOp_; }

  public boolean shouldCache() { return cacheOp_.isSet_cached(); }

  public String getCachePoolName() {
    return shouldCache() ? cacheOp_.getCache_pool_name() : null;
  }
}
