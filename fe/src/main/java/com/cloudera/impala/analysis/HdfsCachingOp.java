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

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.HdfsCachePool;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.THdfsCachingOp;
import com.google.common.base.Preconditions;

/**
 * Represents the partial SQL statement of specifying whether a table/partition
 * should or should not be marked as cached.
 */
public class HdfsCachingOp implements ParseNode {
  private final THdfsCachingOp cacheOp_;

  /**
   * Creates an HdfsCachingOp that specifies the target should be uncached
   */
  public HdfsCachingOp() {
    cacheOp_ = new THdfsCachingOp(false);
  }

  /**
   * Creates an HdfsCachingOp that specifies the target should be cached in cachePoolName
   */
  public HdfsCachingOp(String cachePoolName) {
    cacheOp_ = new THdfsCachingOp(true);
    cacheOp_.setCache_pool_name(cachePoolName);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
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
    }
  }

  @Override
  public String toSql() {
    return shouldCache() ? "CACHED IN '" + getCachePoolName() + "'" : "UNCACHED";
  }

  public THdfsCachingOp toThrift() { return cacheOp_; }

  public boolean shouldCache() { return cacheOp_.isSet_cached(); }
  public String getCachePoolName() {
    return shouldCache() ? cacheOp_.getCache_pool_name() : null;
  }
}