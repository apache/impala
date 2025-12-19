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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.iceberg.exceptions.RESTException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.local.BlacklistingMetaProvider;
import org.apache.impala.catalog.local.CatalogdMetaProvider;
import org.apache.impala.catalog.local.IcebergMetaProvider;
import org.apache.impala.catalog.local.LocalCatalog;
import org.apache.impala.catalog.local.MetaProvider;
import org.apache.impala.catalog.local.MultiMetaProvider;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateCatalogCacheResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation which creates LocalCatalog instances. A new instance is created for each
 * request or query.
 */
class LocalImpl extends FeCatalogManager {

  private static final Logger LOG = LoggerFactory.getLogger(LocalImpl.class);
  private final MetaProvider provider_;

  public LocalImpl() throws ImpalaRuntimeException {
    provider_ = getMetaProvider();
  }

  private MetaProvider getMetaProvider() throws ImpalaRuntimeException {
    TBackendGflags backendCfg = BackendConfig.INSTANCE.getBackendCfg();
    String catalogConfigDir = backendCfg.catalog_config_dir;
    List<MetaProvider> providers = new ArrayList<>();
    if (backendCfg.catalogd_deployed) {
      providers.add(new CatalogdMetaProvider(backendCfg));
    }
    if (catalogConfigDir != null && !catalogConfigDir.isEmpty()) {
      File configDir = new File(catalogConfigDir);
      try {
        LOG.info("Loading catalog config from {}", configDir);
        List<MetaProvider> secondaryProviders = getSecondaryProviders(configDir);
        providers.addAll(secondaryProviders);
      } catch (ImpalaRuntimeException e) {
        LOG.warn("Unable to load secondary providers from catalog config file", e);
      }
    }
    if (providers.isEmpty()) {
      throw new ImpalaRuntimeException("No metadata providers available");
    }
    if (providers.size() == 1) {
      return providers.get(0);
    }
    return new MultiMetaProvider(providers.get(0),
        providers.subList(1, providers.size()));
  }

  @Override
  public FeCatalog getOrCreateCatalog() {
    if (provider_ instanceof CatalogdMetaProvider) {
      ((CatalogdMetaProvider) provider_).setAuthzChecker(authzChecker_);
    }
    return new LocalCatalog(provider_);
  }

  @Override
  public TUpdateCatalogCacheResponse updateCatalogCache(TUpdateCatalogCacheRequest req) {
    if (provider_ instanceof CatalogdMetaProvider) {
      return ((CatalogdMetaProvider) provider_).updateCatalogCache(req);
    }
    if (provider_ instanceof MultiMetaProvider) {
      MetaProvider primaryProvider = ((MultiMetaProvider) provider_).getPrimaryProvider();
      if (primaryProvider instanceof CatalogdMetaProvider) {
        return ((CatalogdMetaProvider) primaryProvider).updateCatalogCache(req);
      }
    }
    return null;
  }

  private static List<MetaProvider> getSecondaryProviders(File catalogConfigDir)
      throws ImpalaRuntimeException {
    ConfigLoader loader = new ConfigLoader(catalogConfigDir);
    List<MetaProvider> list = new ArrayList<>();
    for (Properties properties : loader.loadConfigs()) {
      try {
        MetaProvider icebergMetaProvider =
            new BlacklistingMetaProvider(new IcebergMetaProvider(properties));
        list.add(icebergMetaProvider);
      } catch (RESTException e) {
        LOG.error(String.format(
            "Unable to instantiate IcebergMetaProvider from the following "
                + "properties: %s", properties), e);
      }
    }
    return list;
  }
}
