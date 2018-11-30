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

import com.google.common.collect.Sets;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.PrincipalPrivilege;
import org.apache.impala.catalog.Role;
import org.apache.impala.catalog.User;
import org.apache.log4j.Logger;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.provider.cache.PrivilegeCache;

import java.util.List;
import java.util.Set;

/**
 * The source data this cache is backing is read from the Sentry Policy Service.
 * Writing to the cache will replace any matching items, but will not write back to the
 * Sentry Policy Service. Acts as the backing cache for the Sentry cached based provider.
 * TODO: Instead of calling into Sentry to perform final authorization checks, we
 * should parse/validate the privileges in Impala.
 */
public class SentryAuthorizationPolicy extends AuthorizationPolicy
    implements PrivilegeCache {
  private static final Logger LOG = Logger.getLogger(SentryAuthorizationPolicy.class);

  /**
   * Returns a set of privilege strings in Sentry format.
   */
  @Override
  public synchronized Set<String> listPrivileges(Set<String> groups,
      ActiveRoleSet roleSet) {
    Set<String> privileges = Sets.newHashSet();
    if (roleSet != ActiveRoleSet.ALL) {
      throw new UnsupportedOperationException("Impala does not support role subsets.");
    }

    // Collect all privileges granted to all roles.
    for (String groupName: groups) {
      List<Role> grantedRoles = getGrantedRoles(groupName);
      for (Role role: grantedRoles) {
        for (PrincipalPrivilege privilege: role.getPrivileges()) {
          String authorizable = privilege.getName();
          if (authorizable == null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Ignoring invalid privilege: " + privilege.getName());
            }
            continue;
          }
          privileges.add(authorizable);
        }
      }
    }
    return privileges;
  }

  /**
   * Returns a set of privilege strings in Sentry format.
   */
  @Override
  public synchronized Set<String> listPrivileges(Set<String> groups, Set<String> users,
      ActiveRoleSet roleSet) {
    Set<String> privileges = listPrivileges(groups, roleSet);
    for (String userName: users) {
      User user = getUser(userName);
      if (user != null) {
        for (PrincipalPrivilege privilege: user.getPrivileges()) {
          String authorizable = privilege.getName();
          if (authorizable == null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Ignoring invalid privilege: " + privilege.getName());
            }
            continue;
          }
          privileges.add(authorizable);
        }
      }
    }
    return privileges;
  }

  @Override
  public void close() {
    // Nothing to do, but required by PrivilegeCache.
  }
}
