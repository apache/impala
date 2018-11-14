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

package org.apache.impala.catalog;

import org.apache.hadoop.hdfs.protocol.CachePoolInfo;

import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.THdfsCachePool;
import com.google.common.base.Preconditions;

/**
 * Represents an HDFS cache pool (CachePoolInfo class). Currently, the only metadata we
 * care about for cache pools is the cache pool name. In the future it may be desirable
 * to track additional metadata such as the owner, size, and current usage of the pool.
 */
public class HdfsCachePool extends CatalogObjectImpl {
  private final THdfsCachePool cachePool_;

  public HdfsCachePool(CachePoolInfo cachePoolInfo) {
    cachePool_ = new THdfsCachePool(cachePoolInfo.getPoolName());
  }

  public HdfsCachePool(THdfsCachePool cachePool) {
    Preconditions.checkNotNull(cachePool);
    cachePool_ = cachePool;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.HDFS_CACHE_POOL;
  }

  public THdfsCachePool toThrift() {
    return cachePool_;
  }

  public static HdfsCachePool fromThrift(THdfsCachePool cachePool) {
    return new HdfsCachePool(cachePool);
  }

  @Override
  public String getName() { return cachePool_.getPool_name(); }

  @Override
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setCache_pool(toThrift());
  }
}
