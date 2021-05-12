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

package org.apache.impala.testutil;

import java.net.ServerSocket;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.metastore.CatalogMetastoreServer;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TUniqueId;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Test class of the Catalog Server's catalog that exposes internal state that is useful
 * for testing.
 */
public class CatalogServiceTestCatalog extends CatalogServiceCatalog {
  public CatalogServiceTestCatalog(boolean loadInBackground, int numLoadingThreads,
      TUniqueId catalogServiceId, MetaStoreClientPool metaStoreClientPool)
      throws ImpalaException {
    super(loadInBackground, numLoadingThreads, catalogServiceId,
        System.getProperty("java.io.tmpdir"), metaStoreClientPool);

    // Cache pools are typically loaded asynchronously, but as there is no fixed execution
    // order for tests, the cache pools are loaded synchronously before the tests are
    // executed.
    CachePoolReader rd = new CachePoolReader(false);
    rd.run();
  }

  public static CatalogServiceCatalog create() {
    return createWithAuth(new NoopAuthorizationFactory());
  }

  public static CatalogServiceCatalog createWithAuth(AuthorizationFactory authzFactory) {
    return createWithAuth(authzFactory, false);
  }

  /**
   * Creates a catalog server that reads authorization policy metadata from the
   * authorization config.
   */
  public static CatalogServiceCatalog createWithAuth(AuthorizationFactory authzFactory,
      boolean startCatalogHms) {
    FeSupport.loadLibrary();
    CatalogServiceCatalog cs;
    try {
      if (MetastoreShim.getMajorVersion() > 2) {
        MetastoreShim.setHiveClientCapabilities();
      }
      if (startCatalogHms) {
        cs = new CatalogServiceTestHMSCatalog(false, 16, new TUniqueId(),
            new MetaStoreClientPool(0, 0));
      } else {
        cs = new CatalogServiceTestCatalog(false, 16, new TUniqueId(),
            new MetaStoreClientPool(0, 0));
      }
      cs.setAuthzManager(authzFactory.newAuthorizationManager(cs));
      cs.reset();
    } catch (ImpalaException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
    return cs;
  }

  /**
   * Creates a transient test catalog instance backed by an embedded HMS derby database on
   * the local filesystem. The derby database is created from scratch and has no table
   * metadata.
   */
  public static CatalogServiceCatalog createTransientTestCatalog() throws
      ImpalaException {
    FeSupport.loadLibrary();
    Path derbyPath = Paths.get(System.getProperty("java.io.tmpdir"),
        UUID.randomUUID().toString());
    if (MetastoreShim.getMajorVersion() > 2) {
      MetastoreShim.setHiveClientCapabilities();
    }
    CatalogServiceCatalog cs = new CatalogServiceTestCatalog(false, 16,
        new TUniqueId(), new EmbeddedMetastoreClientPool(0, derbyPath));
    cs.setAuthzManager(new NoopAuthorizationManager());
    cs.reset();
    return cs;
  }

  private static class CatalogTestMetastoreServer extends CatalogMetastoreServer {
    private final int port;
    public CatalogTestMetastoreServer(
        CatalogServiceCatalog catalogServiceCatalog) throws ImpalaException {
      super(catalogServiceCatalog);
      try {
        port = getRandomPort();
      } catch (Exception e) {
        throw new CatalogException(e.getMessage());
      }
    }

    @Override
    public int getPort() {
      return port;
    }

    private static int getRandomPort() throws Exception {
      for (int i=0; i<5; i++) {
        ServerSocket serverSocket = null;
        try {
          serverSocket = new ServerSocket(0);
          return serverSocket.getLocalPort();
        } finally {
          if (serverSocket != null) serverSocket.close();
        }
      }
      throw new Exception("Could not find a free port");
    }
  }

  public static class CatalogServiceTestHMSCatalog extends CatalogServiceTestCatalog {
    private CatalogTestMetastoreServer metastoreServer;
    public CatalogServiceTestHMSCatalog(boolean loadInBackground, int numLoadingThreads,
        TUniqueId catalogServiceId,
        MetaStoreClientPool metaStoreClientPool) throws ImpalaException {
      super(loadInBackground, numLoadingThreads, catalogServiceId, metaStoreClientPool);
    }

    @Override
    protected CatalogMetastoreServer getCatalogMetastoreServer() {
      synchronized (this) {
        if (metastoreServer != null) return metastoreServer;
        try {
          metastoreServer = new CatalogTestMetastoreServer(this);
        } catch (ImpalaException e) {
          return null;
        }
        return metastoreServer;
      }
    }

    public int getPort() { return metastoreServer.getPort(); }

    @Override
    public void close() {
      super.close();
      try {
        metastoreServer.stop();
      } catch (CatalogException e) {
        // ignored
      }
    }
  }

  public static CatalogServiceCatalog createTestCatalogMetastoreServer()
      throws ImpalaException {
    return createWithAuth(new NoopAuthorizationFactory(), true);
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }
}
