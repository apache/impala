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

import com.google.common.base.Preconditions;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.TableLoadingMgr;
import org.apache.impala.catalog.events.MetastoreEvents.EventFactoryForSyncToLatestEvent;
import org.apache.impala.catalog.events.NoOpEventProcessor;
import org.apache.impala.catalog.metastore.NoOpCatalogMetastoreServer;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.hive.executor.TestHiveJavaFunctionFactory;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.FeSupport;
import org.apache.impala.util.NoOpEventSequence;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Test class of the Catalog Server's catalog that exposes internal state that is useful
 * for testing.
 */
public class CatalogServiceTestCatalog extends CatalogServiceCatalog {
  private CatalogOpExecutor opExecutor_;
  protected CatalogServiceTestCatalog(boolean loadInBackground, int numLoadingThreads,
      MetaStoreClientPool metaStoreClientPool) throws ImpalaException {
    super(loadInBackground, numLoadingThreads, System.getProperty("java.io.tmpdir"),
        metaStoreClientPool);

    // Cache pools are typically loaded asynchronously, but as there is no fixed execution
    // order for tests, the cache pools are loaded synchronously before the tests are
    // executed.
    CachePoolReader rd = new CachePoolReader(false);
    rd.run();
  }

  public interface BaseTestCatalogSupplier {
    public abstract CatalogServiceTestCatalog get() throws ImpalaException;
  }

  public static CatalogServiceTestCatalog create() {
    return createWithAuth(new NoopAuthorizationFactory());
  }

  /**
   * Creates a catalog server that reads authorization policy metadata from the
   * authorization config.
   */
  public static CatalogServiceTestCatalog createWithAuth(AuthorizationFactory factory) {
    return createWithAuth(factory,
        () -> new CatalogServiceTestCatalog(false, 16, new MetaStoreClientPool(0, 0)));
  }

  public static CatalogServiceTestCatalog createWithAuth(
      AuthorizationFactory factory, BaseTestCatalogSupplier catalogSupplier) {
    FeSupport.loadLibrary();
    CatalogServiceTestCatalog cs;
    try {
      if (MetastoreShim.getMajorVersion() > 2) {
        MetastoreShim.setHiveClientCapabilities();
      }
      cs = catalogSupplier.get();
      cs.setAuthzManager(factory.newAuthorizationManager(cs));
      cs.setMetastoreEventProcessor(NoOpEventProcessor.getInstance());
      cs.setCatalogMetastoreServer(NoOpCatalogMetastoreServer.INSTANCE);
      cs.setCatalogOpExecutor(new CatalogOpExecutor(cs,
          new NoopAuthorizationFactory().getAuthorizationConfig(),
          new NoopAuthorizationFactory.NoopAuthorizationManager(),
          new TestHiveJavaFunctionFactory()));
      cs.setEventFactoryForSyncToLatestEvent(
          new EventFactoryForSyncToLatestEvent(cs.getCatalogOpExecutor()));
      cs.reset(NoOpEventSequence.INSTANCE);
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
    CatalogServiceTestCatalog cs = new CatalogServiceTestCatalog(false, 16,
        new EmbeddedMetastoreClientPool(0, derbyPath));
    cs.setAuthzManager(new NoopAuthorizationManager());
    cs.setMetastoreEventProcessor(NoOpEventProcessor.getInstance());
    cs.setCatalogOpExecutor(new CatalogOpExecutor(cs,
        new NoopAuthorizationFactory().getAuthorizationConfig(),
        new NoopAuthorizationFactory.NoopAuthorizationManager(),
        new TestHiveJavaFunctionFactory()));
    cs.setEventFactoryForSyncToLatestEvent(
        new EventFactoryForSyncToLatestEvent(cs.getCatalogOpExecutor()));
    cs.reset(NoOpEventSequence.INSTANCE);
    return cs;
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }

  protected void setCatalogOpExecutor(CatalogOpExecutor opExecutor) {
    opExecutor_ = opExecutor;
  }

  public CatalogOpExecutor getCatalogOpExecutor() {
    Preconditions.checkNotNull(opExecutor_, "returning null opExecutor_");
    return opExecutor_;
  }
}
