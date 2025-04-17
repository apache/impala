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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.local.CatalogdMetaProvider;
import org.apache.impala.catalog.local.IcebergMetaProvider;
import org.apache.impala.catalog.local.LocalCatalog;
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
  public static FeCatalogManager createFromBackendConfig() {
    TBackendGflags cfg = BackendConfig.INSTANCE.getBackendCfg();
    if (cfg.use_local_catalog) {
      if (!cfg.catalogd_deployed) {
        // Currently Iceberg REST Catalog is the only implementation.
        return new IcebergRestCatalogImpl();
      } else {
        return new LocalImpl();
      }
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
      return new LocalCatalog(PROVIDER);
    }

    @Override
    TUpdateCatalogCacheResponse updateCatalogCache(TUpdateCatalogCacheRequest req) {
      return PROVIDER.updateCatalogCache(req);
    }
  }

  /**
   * Implementation which creates LocalCatalog instances and uses an Iceberg REST
   * Catalog.
   * TODO(boroknagyz): merge with LocalImpl
   */
  private static class IcebergRestCatalogImpl extends FeCatalogManager {
    private static IcebergMetaProvider PROVIDER;

    @Override
    public synchronized FeCatalog getOrCreateCatalog() {
      if (PROVIDER == null) {
        try {
          PROVIDER = initProvider();
        } catch (IOException e) {
          throw new IllegalStateException("Create IcebergMetaProvider failed", e);
        }
      }
      return new LocalCatalog(PROVIDER);
    }

    IcebergMetaProvider initProvider() throws IOException {
      TBackendGflags flags = BackendConfig.INSTANCE.getBackendCfg();
      String catalogConfigDir = flags.catalog_config_dir;
      Preconditions.checkState(catalogConfigDir != null &&
          !catalogConfigDir.isEmpty());
      List<String> files = listFiles(catalogConfigDir);
      Preconditions.checkState(files.size() == 1,
          String.format("Expected number of files in directory %s is one, found %d files",
              catalogConfigDir, files.size()));
      String configFile = catalogConfigDir + Path.SEPARATOR + files.get(0);
      Properties props = readPropertiesFile(configFile);
      // In the future we can expect different catalog types, but currently we only
      // support Iceberg REST Catalogs.
      checkPropertyValue(configFile, props, "connector.name", "iceberg");
      checkPropertyValue(configFile, props, "iceberg.catalog.type", "rest");
      return new IcebergMetaProvider(props);
    }

    private List<String> listFiles(String dirPath) {
      File dir = new File(dirPath);
      Preconditions.checkState(dir.exists() && dir.isDirectory());
      return Stream.of(dir.listFiles())
          .filter(file -> !file.isDirectory())
          .map(File::getName)
          .collect(Collectors.toList());
    }

    private Properties readPropertiesFile(String file) throws IOException {
      Properties props = new Properties();
      props.load(new FileInputStream(file));
      return props;
    }

    private void checkPropertyValue(String configFile, Properties props, String key,
        String expectedValue) {
      if (!props.containsKey(key)) {
        throw new IllegalStateException(String.format(
            "Expected property %s was not specified in config file %s.", key,
            configFile));
      }
      String actualValue = props.getProperty(key);
      if (!Objects.equals(actualValue, expectedValue)) {
        throw new IllegalStateException(String.format(
            "Expected value of '%s' is '%s', but found '%s' in config file %s",
            key, expectedValue, actualValue, configFile));
      }
    }

    @Override
    TUpdateCatalogCacheResponse updateCatalogCache(TUpdateCatalogCacheRequest req) {
      return null;
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
