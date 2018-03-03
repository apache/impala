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

package org.apache.impala.authorization;

import com.google.common.base.Preconditions;
import org.apache.sentry.policy.db.SimpleDBPolicyEngine;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.impala.catalog.AuthorizationPolicy;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.cache.SimpleCacheProviderBackend;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;

/**
 * Wrapper to facilitate differences in Sentry APIs across
 * Sentry versions.
 */
class SentryAuthProvider {
  /*
   * Creates a new ResourceAuthorizationProvider based on the given configuration.
   */
  static ResourceAuthorizationProvider createProvider(AuthorizationConfig config,
      AuthorizationPolicy policy) {
    try {
      ProviderBackend providerBe;
      // Create the appropriate backend provider.
      if (config.isFileBasedPolicy()) {
        providerBe = new SimpleFileProviderBackend(config.getSentryConfig().getConfig(),
            config.getPolicyFile());
      } else {
        // Note: The second parameter to the ProviderBackend is a "resourceFile" path
        // which is not used by Impala. We cannot pass 'null' so instead pass an empty
        // string.
        providerBe = new SimpleCacheProviderBackend(config.getSentryConfig().getConfig(),
            "");
        Preconditions.checkNotNull(policy);
        ProviderBackendContext context = new ProviderBackendContext();
        context.setBindingHandle(policy);
        providerBe.initialize(context);
      }

      SimpleDBPolicyEngine engine =
          new SimpleDBPolicyEngine(config.getServerName(), providerBe);

      // Try to create an instance of the specified policy provider class.
      // Re-throw any exceptions that are encountered.
      String policyFile = config.getPolicyFile() == null ? "" : config.getPolicyFile();
      return (ResourceAuthorizationProvider) ConstructorUtils.invokeConstructor(
          Class.forName(config.getPolicyProviderClassName()),
          new Object[] {policyFile, engine});
    } catch (Exception e) {
      // Re-throw as unchecked exception.
      throw new IllegalStateException(
          "Error creating ResourceAuthorizationProvider: ", e);
    }
  }
}
