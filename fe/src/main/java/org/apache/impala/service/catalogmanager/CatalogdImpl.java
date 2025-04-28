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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateCatalogCacheResponse;
import org.apache.thrift.TException;

/**
 * Implementation which creates ImpaladCatalog instances and expects to receive updates
 * via the statestore. New instances are created only when full updates are received.
 */
class CatalogdImpl extends FeCatalogManager {

  private final AtomicReference<ImpaladCatalog> catalog_ =
      new AtomicReference<>();

  CatalogdImpl() {
    catalog_.set(createNewCatalog());
  }

  @Override
  public FeCatalog getOrCreateCatalog() {
    return catalog_.get();
  }

  @Override
  public TUpdateCatalogCacheResponse updateCatalogCache(TUpdateCatalogCacheRequest req)
      throws CatalogException, TException {
    ImpaladCatalog catalog = catalog_.get();
    if (req.is_delta) return catalog.updateCatalog(req);

    // If this is not a delta, this update should replace the current
    // Catalog contents so create a new catalog and populate it.
    ImpaladCatalog oldCatalog = catalog;
    catalog = createNewCatalog();

    TUpdateCatalogCacheResponse response = catalog.updateCatalog(req);

    // Now that the catalog has been updated, replace the reference to
    // catalog_. This ensures that clients don't see the catalog
    // disappear. The catalog is guaranteed to be ready since updateCatalog() has a
    // postcondition of isReady() == true.
    catalog_.set(catalog);
    if (oldCatalog != null) oldCatalog.release();

    return response;
  }

  private ImpaladCatalog createNewCatalog() {
    return new ImpaladCatalog(authzChecker_);
  }
}
