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
import com.google.common.collect.Sets;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.PrincipalPrivilege;
import org.apache.impala.catalog.PrincipalPrivilegeTree;
import org.apache.impala.catalog.Role;
import org.apache.impala.catalog.User;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.policy.common.PrivilegeFactory;
import org.apache.sentry.policy.engine.common.CommonPrivilegeFactory;
import org.apache.sentry.provider.cache.FilteredPrivilegeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The source data this cache is backing is read from the Sentry Policy Service.
 * Writing to the cache will replace any matching items, but will not write back to the
 * Sentry Policy Service. Acts as the backing cache for the Sentry cached based provider.
 * TODO: Instead of calling into Sentry to perform final authorization checks, we
 * should parse/validate the privileges in Impala.
 */
public class SentryAuthorizationPolicy implements FilteredPrivilegeCache {
  private static final Logger LOG = LoggerFactory.getLogger(
      SentryAuthorizationPolicy.class);

  private final AuthorizationPolicy authzPolicy_;
  private final PrivilegeFactory privilegeFactory;

  public SentryAuthorizationPolicy(AuthorizationPolicy authzPolicy) {
    Preconditions.checkNotNull(authzPolicy);
    authzPolicy_ = authzPolicy;
    this.privilegeFactory = new CommonPrivilegeFactory();
  }

  /**
   * Returns a set of privilege strings in Sentry format.
   */
  @Override
  public Set<String> listPrivileges(Set<String> groups,
      ActiveRoleSet roleSet) {
    return listPrivilegesForGroups(groups, roleSet, null);
  }


  private Set<String> listPrivilegesForGroups(Set<String> groups,
      ActiveRoleSet roleSet, PrincipalPrivilegeTree.Filter filter) {
    Set<String> privileges = Sets.newHashSet();
    if (roleSet != ActiveRoleSet.ALL) {
      throw new UnsupportedOperationException("Impala does not support role subsets.");
    }

    // Collect all privileges granted to all roles.
    for (String groupName: groups) {
      List<Role> grantedRoles = authzPolicy_.getGrantedRoles(groupName);
      for (Role role: grantedRoles) {
        privileges.addAll(role.getFilteredPrivilegeNames(filter));
      }
    }
    return privileges;
  }

  /**
   * Returns a set of privilege strings in Sentry format.
   */
  @Override
  public Set<String> listPrivileges(Set<String> groups, Set<String> users,
      ActiveRoleSet roleSet) {
    return listPrivilegesForGroupsAndUsers(groups, users, roleSet, null);
  }

  private Set<String> listPrivilegesForGroupsAndUsers(Set<String> groups,
      Set<String> users, ActiveRoleSet roleSet, PrincipalPrivilegeTree.Filter filter) {
    Set<String> privileges = listPrivilegesForGroups(groups, roleSet, filter);
    for (String userName: users) {
      User user = authzPolicy_.getUser(userName);
      if (user != null) {
        privileges.addAll(user.getFilteredPrivilegeNames(filter));
      }
    }
    return privileges;
  }

  @Override
  public Set<String> listPrivileges(Set<String> groups, Set<String> users,
      ActiveRoleSet roleSet, Authorizable... authorizationHierarchy) {
    PrincipalPrivilegeTree.Filter filter = createPrivilegeFilter(authorizationHierarchy);
    return listPrivilegesForGroupsAndUsers(groups, users, roleSet, filter);
  }

  @Override
  public Set<Privilege> listPrivilegeObjects(Set<String> groups, Set<String> users,
      ActiveRoleSet roleSet, Authorizable... authorizationHierarchy) {
    Set<String> privilegeStrings =
        listPrivileges(groups, users, roleSet, authorizationHierarchy);

    return privilegeStrings.stream()
      .filter(priString -> priString != null)
      .map(priString -> getPrivilegeObject(priString))
      .collect(Collectors.toSet());
  }

  private Privilege getPrivilegeObject(String priString) {
    return privilegeFactory.createPrivilege(priString);
  }

  private PrincipalPrivilegeTree.Filter createPrivilegeFilter(
      Authorizable... authorizationHierarchy) {
    PrincipalPrivilegeTree.Filter filter = new PrincipalPrivilegeTree.Filter();
    for (Authorizable auth : authorizationHierarchy) {
      String name = auth.getName().toLowerCase();
      if (name.equals(SentryConstants.RESOURCE_WILDCARD_VALUE) ||
        name.equals(SentryConstants.RESOURCE_WILDCARD_VALUE_SOME)||
        name.equals(SentryConstants.RESOURCE_WILDCARD_VALUE_ALL)) {
        name = null; // null will match with everything
      }
      if (!(auth instanceof DBModelAuthorizable)) continue;
      DBModelAuthorizable dbAuth = (DBModelAuthorizable) auth;
      switch (dbAuth.getAuthzType()) {
        case Server:
          filter.setServer(name);
          break;
        case Db:
          filter.setDb(name);
          break;
        case Table:
          filter.setTable(name);
          break;
        case URI:
          filter.setIsUri(true);
        // Do not do anything for Column and View
      }
    }
    return filter;
  }

  @Override
  public void close() {
    // Nothing to do, but required by PrivilegeCache.
  }
}
