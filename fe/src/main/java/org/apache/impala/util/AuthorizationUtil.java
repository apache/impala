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

package org.apache.impala.util;

import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.AuthorizationProvider;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthorizationUtil {
  private final static Logger LOG = LoggerFactory.getLogger(AuthorizationUtil.class);

  private AuthorizationUtil() {}

  /**
   * Returns the {@link AuthorizationFactory} class name specified by
   * the given {@link BackendConfig}
   *
   * @param beCfg
   * @return the AuthorizationFactory class name for the given BackendConfig
   * @throws InternalException if the class name could not be determined from
   *                           the given backend config values
   */
  public static String authzFactoryClassNameFrom(BackendConfig beCfg)
      throws InternalException {
    final String authzFactoryClassOption = beCfg.getAuthorizationFactoryClass();
    final String authzFactoryClassName;
    if (authzFactoryClassOption != null) {
      // authorization_factory_class takes precedence
      authzFactoryClassName = authzFactoryClassOption;
    } else {
      // use authorization_provider flag
      String authzProvider = beCfg.getAuthorizationProvider();
      // If authorization_provider is empty, use the Noop policy that disables
      // authorization.
      if (authzProvider.equals("")) authzProvider = "noop";
      try {
        final AuthorizationProvider provider = AuthorizationProvider.valueOf(
            authzProvider.toUpperCase().trim());
        authzFactoryClassName = provider.getAuthorizationFactoryClassName();
      } catch (Exception e) {
        throw new InternalException(
            "Could not parse authorization_provider flag: " + authzProvider);
      }
    }
    return authzFactoryClassName;
  }

  /**
   * Creates the appropriate {@link AuthorizationFactory} based on the
   * given {@link BackendConfig}.
   *
   * @param beCfg the BackendConfig
   * @return the correctly-chosen AuthorizationFactory
   *
   * @throws InternalException if the correct factory could not be determined
   *                           e.g. if config flags are invalid
   */
  public static AuthorizationFactory authzFactoryFrom(BackendConfig beCfg)
      throws InternalException {
    final AuthorizationFactory authzFactory;
    final String authzFactoryClassName = authzFactoryClassNameFrom(beCfg);
    try {
      authzFactory = (AuthorizationFactory)
          Class.forName(authzFactoryClassName)
              .getConstructor(BackendConfig.class)
              .newInstance(beCfg);
    } catch (Exception e) {
      throw new InternalException(
          "Unable to instantiate authorization provider: " + authzFactoryClassName, e);
    }
    if (!beCfg.isColumnMaskingEnabled() && beCfg.isRowFilteringEnabled()) {
      throw new InternalException("Unable to enable row-filtering without column-masking."
          + " Please set --enable_column_masking to true as well");
    }
    final AuthorizationConfig authzConfig = authzFactory.getAuthorizationConfig();

    if (!authzConfig.isEnabled()) {
      // For backward compatibility to keep the existing behavior, when authorization
      // is not enabled, we need to use a dummy authorization config.
      LOG.info("Authorization is 'DISABLED'.");
      return new NoopAuthorizationFactory(beCfg);
    }

    LOG.info(String.format("Authorization is 'ENABLED' using %s.",
        authzConfig.getProviderName()));
    return authzFactory;
  }
}
