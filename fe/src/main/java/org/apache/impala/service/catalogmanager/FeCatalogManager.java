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
package org.apache.impala.service.catalogmanager;

import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateCatalogCacheResponse;
import org.apache.thrift.TException;

/**
 * Manages the Catalog implementation used by the frontend.
 *
 * This class abstracts away the different lifecycles used by the LocalCatalog
 * and the ImpaladCatalog. The former creates a new instance for each request or
 * query, whereas the latter only creates a new instance upon receiving a full update
 * from the catalogd via the statestore.
 */
public abstract class FeCatalogManager {

  protected AtomicReference<? extends AuthorizationChecker> authzChecker_;

  /**
   * @return the appropriate implementation based on the current backend
   * configuration.
   */
  public static FeCatalogManager createFromBackendConfig() throws ImpalaRuntimeException {
    TBackendGflags cfg = BackendConfig.INSTANCE.getBackendCfg();
    if (cfg.use_local_catalog) {
      return new LocalImpl();
    } else {
      return new CatalogdImpl();
    }
  }

  /**
   * Create a manager which always returns the same instance and does not permit
   * updates from the statestore.
   */
  public static FeCatalogManager createForTests(FeCatalog testCatalog) {
    return new TestImpl(testCatalog);
  }

  public void setAuthzChecker(
      AtomicReference<? extends AuthorizationChecker> authzChecker) {
    authzChecker_ = Preconditions.checkNotNull(authzChecker);
  }

  /**
   * @return a Catalog instance to be used for a request or query. Depending
   * on the catalog implementation this may either be a reused instance or a
   * fresh one for each query.
   */
  public abstract FeCatalog getOrCreateCatalog();

  /**
   * Update the Catalog based on an update from the state store.
   *
   * This can be called either in response to a DDL statement (in which case the update
   * may include just the changed objects related to that DDL) or due to data being
   * published by the state store.
   *
   * In the case of the DDL-triggered update, the return value is ignored. In the case
   * of the statestore update, the return value is passed back to the C++ code to
   * indicate the last applied catalog update and used to implement SYNC_DDL.
   */
  public abstract TUpdateCatalogCacheResponse updateCatalogCache(
      TUpdateCatalogCacheRequest req) throws CatalogException, TException;
}
