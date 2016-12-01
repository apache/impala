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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.impala.catalog.AuthorizationException;
import org.apache.impala.catalog.AuthorizationPolicy;
import org.apache.impala.common.InternalException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.policy.db.SimpleDBPolicyEngine;
import org.apache.sentry.provider.cache.SimpleCacheProviderBackend;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class used to check whether a user has access to a given resource.
 */
public class AuthorizationChecker {
  private final ResourceAuthorizationProvider provider_;
  private final AuthorizationConfig config_;
  private final AuthorizeableServer server_;

  /*
   * Creates a new AuthorizationChecker based on the config values.
   */
  public AuthorizationChecker(AuthorizationConfig config, AuthorizationPolicy policy) {
    Preconditions.checkNotNull(config);
    config_ = config;
    if (config.isEnabled()) {
      server_ = new AuthorizeableServer(config.getServerName());
      provider_ = createProvider(config, policy);
      Preconditions.checkNotNull(provider_);
    } else {
      provider_ = null;
      server_ = null;
    }
  }

  /*
   * Creates a new ResourceAuthorizationProvider based on the given configuration.
   */
  private static ResourceAuthorizationProvider createProvider(AuthorizationConfig config,
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

  /*
   * Returns the configuration used to create this AuthorizationProvider.
   */
  public AuthorizationConfig getConfig() { return config_; }

  /**
   * Returns the set of groups this user belongs to. Uses the GroupMappingService
   * that is in the AuthorizationProvider to properly resolve Hadoop groups or
   * local group mappings.
   */
  public Set<String> getUserGroups(User user) throws InternalException {
    return provider_.getGroupMapping().getGroups(user.getShortName());
  }

  /**
   * Authorizes the PrivilegeRequest, throwing an Authorization exception if
   * the user does not have sufficient privileges.
   */
  public void checkAccess(User user, PrivilegeRequest privilegeRequest)
      throws AuthorizationException, InternalException {
    Preconditions.checkNotNull(privilegeRequest);

    if (!hasAccess(user, privilegeRequest)) {
      if (privilegeRequest.getAuthorizeable() instanceof AuthorizeableFn) {
        throw new AuthorizationException(String.format(
            "User '%s' does not have privileges to CREATE/DROP functions.",
            user.getName()));
      }

      Privilege privilege = privilegeRequest.getPrivilege();
      if (EnumSet.of(Privilege.ANY, Privilege.ALL, Privilege.VIEW_METADATA)
          .contains(privilege)) {
        throw new AuthorizationException(String.format(
            "User '%s' does not have privileges to access: %s",
            user.getName(), privilegeRequest.getName()));
      } else {
        throw new AuthorizationException(String.format(
            "User '%s' does not have privileges to execute '%s' on: %s",
            user.getName(), privilege, privilegeRequest.getName()));
      }
    }
  }

  /*
   * Returns true if the given user has permission to execute the given
   * request, false otherwise. Always returns true if authorization is disabled.
   */
  public boolean hasAccess(User user, PrivilegeRequest request)
      throws InternalException {
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(request);

    // If authorization is not enabled the user will always have access. If this is
    // an internal request, the user will always have permission.
    if (!config_.isEnabled() || user instanceof ImpalaInternalAdminUser) {
      return true;
    }

    EnumSet<DBModelAction> actions = request.getPrivilege().getHiveActions();

    List<DBModelAuthorizable> authorizeables = Lists.newArrayList(
        server_.getHiveAuthorizeableHierarchy());
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
    } else if (request.getPrivilege() == Privilege.CREATE && authorizeables.size() > 1) {
      // CREATE on an object requires CREATE on the parent,
      // so don't check access on the object we're creating.
      authorizeables.remove(authorizeables.size() - 1);
    }
    return provider_.hasAccess(new Subject(user.getShortName()), authorizeables, actions,
        ActiveRoleSet.ALL);
  }
}
