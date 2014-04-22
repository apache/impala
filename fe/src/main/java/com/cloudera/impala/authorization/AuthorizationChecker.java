// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.authorization;

import java.util.EnumSet;
import java.util.List;

import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.policy.db.SimpleDBPolicyEngine;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/*
 * Class used to check whether a user has access to a given resource.
 */
public class AuthorizationChecker {
  private final ResourceAuthorizationProvider provider_;
  private final AuthorizationConfig config_;

  /*
   * Creates a new AuthorizationChecker based on the config values.
   */
  public AuthorizationChecker(AuthorizationConfig config) {
    Preconditions.checkNotNull(config);
    config_ = config;
    if (config.isEnabled()) {
      provider_ = createAuthorizationProvider(config);
      Preconditions.checkNotNull(provider_);
    } else {
      provider_ = null;
    }
  }

  /*
   * Creates a new ResourceAuthorizationProvider based on the given configuration.
   */
  private static ResourceAuthorizationProvider
      createAuthorizationProvider(AuthorizationConfig config) {
    try {
      SimpleFileProviderBackend providerBackend = new SimpleFileProviderBackend(
          config.getSentryConfig(), config.getPolicyFile());
      SimpleDBPolicyEngine engine =
          new SimpleDBPolicyEngine(config.getServerName(), providerBackend);

      // Try to create an instance of the specified policy provider class.
      // Re-throw any exceptions that are encountered.
      return (ResourceAuthorizationProvider) ConstructorUtils.invokeConstructor(
          Class.forName(config.getPolicyProviderClassName()),
          new Object[] {config.getPolicyFile(), engine});

    } catch (Exception e) {
      // Re-throw as unchecked exception.
      throw new IllegalStateException(
          "Error creating ResourceAuthorizationProvider: ", e);
    }
  }

  /*
   * Returns the configuration used to create this AuthorizationProvider.
   */
  public AuthorizationConfig getConfig() {
    return config_;
  }

  /*
   * Returns true if the given user has permission to execute the given
   * request, false otherwise. Always returns true if authorization is disabled.
   */
  public boolean hasAccess(User user, PrivilegeRequest request) {
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(request);

    // If authorization is not enabled the user will always have access. If this is
    // an internal request, the user will always have permission.
    if (!config_.isEnabled() || user instanceof ImpalaInternalAdminUser) {
      return true;
    }

    EnumSet<DBModelAction> actions = request.getPrivilege().getHiveActions();

    List<Authorizable> authorizeables = Lists.newArrayList();
    authorizeables.add(new org.apache.sentry.core.model.db.Server(config_.getServerName()));
    // If request.getAuthorizeable() is null, the request is for server-level permission.
    if (request.getAuthorizeable() != null) {
      authorizeables.addAll(request.getAuthorizeable().getHiveAuthorizeableHierarchy());
    }

    // The Hive Access API does not currently provide a way to check if the user
    // has any privileges on a given resource.
    if (request.getPrivilege().getAnyOf()) {
      for (DBModelAction action: actions) {
        if (provider_.hasAccess(new Subject(user.getShortName()), authorizeables,
            EnumSet.of(action), ActiveRoleSet.ALL)) {
          return true;
        }
      }
      return false;
    }
    return provider_.hasAccess(new Subject(user.getShortName()), authorizeables, actions,
        ActiveRoleSet.ALL);
  }
}
