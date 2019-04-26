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

import com.google.common.base.Preconditions;
import org.apache.impala.thrift.TAuthzCacheInvalidation;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;

/**
 * This catalog object is used for authorization cache invalidation notification.
 */
public class AuthzCacheInvalidation extends CatalogObjectImpl {
  private final TAuthzCacheInvalidation authzCacheInvalidation_;

  public AuthzCacheInvalidation(String markerName) {
    this(new TAuthzCacheInvalidation(markerName));
  }

  public AuthzCacheInvalidation(TAuthzCacheInvalidation authzCacheInvalidation) {
    authzCacheInvalidation_ = Preconditions.checkNotNull(authzCacheInvalidation);
  }

  @Override
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setAuthz_cache_invalidation(authzCacheInvalidation_);
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.AUTHZ_CACHE_INVALIDATION;
  }

  @Override
  public String getName() { return authzCacheInvalidation_.getMarker_name(); }

  public TAuthzCacheInvalidation toThrift() { return authzCacheInvalidation_; }
}
