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

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.apache.access.core.Authorizable;
import org.apache.access.provider.file.HadoopGroupResourceAuthorizationProvider;
import org.apache.access.provider.file.ResourceAuthorizationProvider;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/*
 * Class used to check whether a user has access to a given resource.
 */
public class AuthorizationChecker {
  private final ResourceAuthorizationProvider provider;
  private final AuthorizationConfig config;

  /*
   * Creates a new AuthorizationChecker from the given config. If authorization is
   * enabled, it will authorize based on the Hadoop Group the user belongs to.
   * TODO: Support a LocalGroupAuthorizationProvider as well.
   */
  public AuthorizationChecker(AuthorizationConfig config) throws IOException {
    Preconditions.checkNotNull(config);
    this.config = config;
    if (config.isEnabled()) {
      this.provider = new HadoopGroupResourceAuthorizationProvider(
          config.getPolicyFile(), config.getServerName());
    } else {
      this.provider = null;
    }
  }

  /*
   * Returns the configuration used to create this AuthorizationProvider.
   */
  public AuthorizationConfig getConfig() {
    return config;
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
    if (!config.isEnabled() || user instanceof ImpalaInternalAdminUser) {
      return true;
    }

    EnumSet<org.apache.access.core.Action> actions =
        request.getPrivilege().getHiveActions();

    List<Authorizable> authorizeables = Lists.newArrayList();
    authorizeables.add(new org.apache.access.core.Server(config.getServerName()));
    // If request.getAuthorizeable() is null, the request is for server-level permission.
    if (request.getAuthorizeable() != null) {
      authorizeables.addAll(request.getAuthorizeable().getHiveAuthorizeableHierarchy());
    }

    // The Hive Access API does not currently provide a way to check if the user
    // has any privileges on a given resource.
    if (request.getPrivilege().getAnyOf()) {
      for (org.apache.access.core.Action action: actions) {
        if (provider.hasAccess(new org.apache.access.core.Subject(user.getName()),
            authorizeables, EnumSet.of(action))) {
          return true;
        }
      }
      return false;
    }
    return provider.hasAccess(new org.apache.access.core.Subject(user.getName()),
        authorizeables, actions);
  }
}