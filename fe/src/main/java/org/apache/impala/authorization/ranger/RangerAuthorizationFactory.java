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

package org.apache.impala.authorization.ranger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeCatalogManager;

import java.util.function.Supplier;

/**
 * An implementation of {@link AuthorizationFactory} that uses Ranger.
 */
public class RangerAuthorizationFactory implements AuthorizationFactory {
  private final AuthorizationConfig authzConfig_;
  private final RangerAuthorizationChecker authzChecker_;

  public RangerAuthorizationFactory(BackendConfig backendConfig) {
    Preconditions.checkNotNull(backendConfig);
    authzConfig_ = newAuthorizationConfig(backendConfig);
    authzChecker_ = new RangerAuthorizationChecker(authzConfig_);
  }

  /**
   * This is for testing.
   */
  @VisibleForTesting
  public RangerAuthorizationFactory(AuthorizationConfig authzConfig) {
    Preconditions.checkNotNull(authzConfig);
    authzConfig_ = authzConfig;
    authzChecker_ = new RangerAuthorizationChecker(authzConfig_);
  }

  private static AuthorizationConfig newAuthorizationConfig(BackendConfig backendConfig) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(
        backendConfig.getBackendCfg().getServer_name()),
        "Authorization is enabled but the server name is empty. " +
            "Set the server name using impalad and catalogd --server_name flag.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(
        backendConfig.getRangerServiceType()),
        "Ranger service type is empty. Set the Ranger service type using " +
            "impalad and catalogd --ranger_service_type flag.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(backendConfig.getRangerAppId()),
        "Ranger application ID is empty. Set the Ranger application ID using " +
            "impalad and catalogd --ranger_app_id flag.");
    return new RangerAuthorizationConfig(backendConfig.getRangerServiceType(),
        backendConfig.getRangerAppId(), backendConfig.getBackendCfg().getServer_name(),
        null, null, null);
  }

  @Override
  public AuthorizationConfig getAuthorizationConfig() { return authzConfig_; }

  @Override
  public AuthorizationChecker newAuthorizationChecker(AuthorizationPolicy authzPolicy) {
    // Do not create a new instance of RangerAuthorizationChecker. It is unnecessary
    // since RangerAuthorizationChecker is stateless and creating a new instance of
    // RangerAuthorizationChecker can be expensive due to the need to re-initialize the
    // Ranger plugin.
    return authzChecker_;
  }

  @Override
  public AuthorizationManager newAuthorizationManager(FeCatalogManager catalog,
      Supplier<? extends AuthorizationChecker> authzChecker) {
    Preconditions.checkArgument(authzChecker.get() instanceof RangerAuthorizationChecker);

    return new RangerImpaladAuthorizationManager(() ->
        ((RangerAuthorizationChecker) authzChecker.get()).getRangerImpalaPlugin());
  }

  @Override
  public AuthorizationManager newAuthorizationManager(CatalogServiceCatalog catalog) {
    RangerAuthorizationConfig config = (RangerAuthorizationConfig) authzConfig_;
    RangerImpalaPlugin plugin = RangerImpalaPlugin.getInstance(config.getServiceType(),
        config.getAppId());
    return new RangerCatalogdAuthorizationManager(() -> plugin, catalog);
  }

  @Override
  public boolean supportsTableMasking() {
    return BackendConfig.INSTANCE.isColumnMaskingEnabled()
        || BackendConfig.INSTANCE.isRowFilteringEnabled();
  }
}
