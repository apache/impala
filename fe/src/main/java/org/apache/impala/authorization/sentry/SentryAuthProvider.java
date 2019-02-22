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

package org.apache.impala.authorization.sentry;

import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.sentry.policy.engine.common.CommonPolicyEngine;
import org.apache.sentry.provider.cache.PrivilegeCache;
import org.apache.sentry.provider.cache.SimpleCacheProviderBackend;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;

import com.google.common.base.Preconditions;

/**
 * Wrapper to facilitate differences in Sentry APIs across Sentry versions.
 */
class SentryAuthProvider {
  /*
   * Creates a new ResourceAuthorizationProvider based on the given configuration.
   */
  static ResourceAuthorizationProvider createProvider(AuthorizationConfig config,
      SentryAuthorizationPolicy policy) {
    Preconditions.checkArgument(policy instanceof PrivilegeCache);
    Preconditions.checkArgument(config instanceof SentryAuthorizationConfig);
    SentryAuthorizationConfig sentryAuthzConfig = (SentryAuthorizationConfig) config;
    try {
      // Note: The second parameter to the ProviderBackend is a "resourceFile" path
      // which is not used by Impala. We cannot pass 'null' so instead pass an empty
      // string.
      ProviderBackend providerBe = new SimpleCacheProviderBackend(
          sentryAuthzConfig.getSentryConfig().getConfig(), "");
      Preconditions.checkNotNull(policy);
      ProviderBackendContext context = new ProviderBackendContext();
      context.setBindingHandle(policy);
      providerBe.initialize(context);

      CommonPolicyEngine engine =
          new CommonPolicyEngine(providerBe);

      // Try to create an instance of the specified policy provider class.
      // Re-throw any exceptions that are encountered.

      return (ResourceAuthorizationProvider) ConstructorUtils.invokeConstructor(
          Class.forName(sentryAuthzConfig.getPolicyProviderClassName()),
          new Object[] {"", engine, ImpalaPrivilegeModel.INSTANCE});
    } catch (Exception e) {
      // Re-throw as unchecked exception.
      throw new IllegalStateException(
          "Error creating ResourceAuthorizationProvider: ", e);
    }
  }
}
