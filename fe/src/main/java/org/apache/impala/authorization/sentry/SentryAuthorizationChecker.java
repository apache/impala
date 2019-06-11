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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.impala.authorization.Authorizable.Type;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.authorization.BaseAuthorizationChecker;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.User;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.common.InternalException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * An implementation of AuthorizationChecker that uses Sentry.
 */
public class SentryAuthorizationChecker extends BaseAuthorizationChecker {
  private final ResourceAuthorizationProvider provider_;
  private final SentryAuthorizableServer server_;

  public SentryAuthorizationChecker(AuthorizationConfig config,
      AuthorizationPolicy policy) {
    super(config);
    if (config.isEnabled()) {
      server_ = new SentryAuthorizableServer(config.getServerName());
      provider_ = createProvider(config, new SentryAuthorizationPolicy(policy));
      Preconditions.checkNotNull(provider_);
    } else {
      provider_ = null;
      server_ = null;
    }
  }

  /**
   * Returns the set of groups this user belongs to.
   */
  public Set<String> getUserGroups(User user) throws InternalException {
    try {
      return provider_.getGroupMapping().getGroups(user.getShortName());
    } catch (Exception e) {
      if (SentryUtil.isSentryGroupNotFound(e)) {
        // Sentry 2.1+ throws exceptions when user does not exist; swallow the
        // exception and just return an empty set for this case.
        return Collections.emptySet();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void authorizeRowFilterAndColumnMask(User user,
      List<PrivilegeRequest> privilegeRequests)
      throws AuthorizationException, InternalException {
  }

  @Override
  public void invalidateAuthorizationCache() {
    // Authorization refresh in Sentry is done by updating {@link AuthorizationPolicy}.
  }

  @Override
  public AuthorizationContext createAuthorizationContext(boolean doAudits,
      String sqlStmt) {
    return new AuthorizationContext();
  }

  /*
   * Creates a new ResourceAuthorizationProvider based on the given configuration.
   */
  private static ResourceAuthorizationProvider createProvider(AuthorizationConfig config,
      SentryAuthorizationPolicy policy) {
    return SentryAuthProvider.createProvider(config, policy);
  }

  @Override
  public boolean authorizeResource(AuthorizationContext authzCtx, User user,
      PrivilegeRequest request) throws InternalException {
    EnumSet<ImpalaAction> actions = ImpalaAction.from(request.getPrivilege());

    List<DBModelAuthorizable> authorizables = Lists.newArrayList(
        server_.getDBModelAuthorizableHierarchy());
    // If request.getAuthorizable() is null, the request is for server-level permission.
    if (request.getAuthorizable() != null) {
      Preconditions.checkState(request.getAuthorizable() instanceof SentryAuthorizable);
      authorizables.addAll(((SentryAuthorizable) request.getAuthorizable())
          .getDBModelAuthorizableHierarchy());
    }

    // The Hive Access API does not currently provide a way to check if the user
    // has any privileges on a given resource.
    if (request.getPrivilege().hasAnyOf()) {
      for (ImpalaAction action: actions) {
        if (provider_.hasAccess(new Subject(user.getShortName()), authorizables,
            EnumSet.of(action), request.hasGrantOption(), ActiveRoleSet.ALL)) {
          return true;
        }
      }
      return false;
      // AuthorizableFn is special due to Sentry not having the concept of a function in
      // DBModelAuthorizable.AuthorizableType. As a result, the list of authorizables for
      // an AuthorizableFn only contains the server and database, but not the function
      // itself. So there is no need to remove the last authorizable here.
    } else if (request.getPrivilege() == Privilege.CREATE && authorizables.size() > 1 &&
        !(request.getAuthorizable().getType() == Type.FUNCTION)) {
      // CREATE on an object requires CREATE on the parent,
      // so don't check access on the object we're creating.
      authorizables.remove(authorizables.size() - 1);
    }
    return provider_.hasAccess(new Subject(user.getShortName()), authorizables, actions,
        request.hasGrantOption(), ActiveRoleSet.ALL);
  }
}
