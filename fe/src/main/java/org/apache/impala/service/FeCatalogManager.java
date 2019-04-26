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
package org.apache.impala.service;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.local.CatalogdMetaProvider;
import org.apache.impala.catalog.local.LocalCatalog;
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
  private static String DEFAULT_KUDU_MASTER_HOSTS =
      BackendConfig.INSTANCE.getBackendCfg().kudu_master_hosts;

  protected AtomicReference<? extends AuthorizationChecker> authzChecker_;

  /**
   * @return the appropriate implementation based on the current backend
   * configuration.
   */
  public static FeCatalogManager createFromBackendConfig() {
    if (BackendConfig.INSTANCE.getBackendCfg().use_local_catalog) {
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
  abstract TUpdateCatalogCacheResponse updateCatalogCache(
      TUpdateCatalogCacheRequest req) throws CatalogException, TException;

  /**
   * Implementation which creates ImpaladCatalog instances and expects to receive
   * updates via the statestore. New instances are created only when full updates
   * are received.
   */
  private static class CatalogdImpl extends FeCatalogManager {
    private final AtomicReference<ImpaladCatalog> catalog_ =
        new AtomicReference<>();

    private CatalogdImpl() {
      catalog_.set(createNewCatalog());
    }

    @Override
    public FeCatalog getOrCreateCatalog() {
      return catalog_.get();
    }

    @Override
    TUpdateCatalogCacheResponse updateCatalogCache(TUpdateCatalogCacheRequest req)
        throws CatalogException, TException {
      ImpaladCatalog catalog = catalog_.get();
      if (req.is_delta) return catalog.updateCatalog(req);

      // If this is not a delta, this update should replace the current
      // Catalog contents so create a new catalog and populate it.
      catalog = createNewCatalog();

      TUpdateCatalogCacheResponse response = catalog.updateCatalog(req);

      // Now that the catalog has been updated, replace the reference to
      // catalog_. This ensures that clients don't see the catalog
      // disappear. The catalog is guaranteed to be ready since updateCatalog() has a
      // postcondition of isReady() == true.
      catalog_.set(catalog);
      return response;
    }

    private ImpaladCatalog createNewCatalog() {
      return new ImpaladCatalog(DEFAULT_KUDU_MASTER_HOSTS, authzChecker_);
    }
  }

  /**
   * Implementation which creates LocalCatalog instances. A new instance is
   * created for each request or query.
   */
  private static class LocalImpl extends FeCatalogManager {
    private static CatalogdMetaProvider PROVIDER = new CatalogdMetaProvider(
        BackendConfig.INSTANCE.getBackendCfg());

    @Override
    public FeCatalog getOrCreateCatalog() {
      PROVIDER.setAuthzChecker(authzChecker_);
      return new LocalCatalog(PROVIDER, DEFAULT_KUDU_MASTER_HOSTS);
    }

    @Override
    TUpdateCatalogCacheResponse updateCatalogCache(TUpdateCatalogCacheRequest req) {
      return PROVIDER.updateCatalogCache(req);
    }
  }

  /**
   * Implementation which returns a provided catalog instance, used by tests.
   * No updates from the statestore are permitted.
   */
  private static class TestImpl extends FeCatalogManager {
    private final FeCatalog catalog_;

    TestImpl(FeCatalog catalog) {
      catalog_ = catalog;
    }

    @Override
    public FeCatalog getOrCreateCatalog() {
      return catalog_;
    }

    @Override
    TUpdateCatalogCacheResponse updateCatalogCache(TUpdateCatalogCacheRequest req) {
      throw new IllegalStateException(
          "Unexpected call to updateCatalogCache() with a test catalog instance");
    }
  }
}
