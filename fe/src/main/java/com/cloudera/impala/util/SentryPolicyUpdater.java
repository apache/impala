// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.util;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;

import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.catalog.CatalogServiceCatalog;
import com.cloudera.impala.catalog.Role;
import com.cloudera.impala.catalog.RolePrivilege;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.thrift.TPrivilege;
import com.google.common.collect.Sets;

/**
 * This class acts as a link between the Sentry Service and the Catalog to ensure
 * both places are updated consistently (once Impala supports GRANT/REVOKE). More
 * specifically, this class will synchronize updates to the Sentry Service and
 * the Impala catalog to ensure they are applied atomically (in Impala's view) and
 * only if reading the most recent policy from the Sentry Service succeeds.
 * It also periodically refreshes the authorization policy metadata and updates the
 * catalog with any changes. Because any catalog updates will need to be synchronized with
 * updates from GRANT/REVOKE statements, it makes sense for this class to
 * synchronize all modifications.
 */
public class SentryPolicyUpdater {
  private static final Logger LOG = Logger.getLogger(SentryPolicyUpdater.class);

  // Used to periodically poll the Sentry service and updates the catalog with any
  // changes.
  private final ScheduledExecutorService policyReader_ =
      Executors.newScheduledThreadPool(1);

  // The Catalog the SentryServiceExecutor is associated with.
  private final CatalogServiceCatalog catalog_;

  // The interface to access the Sentry Policy Service to read/update policy metadata.
  private final SentryPolicyService sentryPolicyService_;

  public SentryPolicyUpdater(SentryConfig sentryConfig, CatalogServiceCatalog catalog) {
    catalog_ = catalog;
    sentryPolicyService_ = new SentryPolicyService(sentryConfig, null);
    // TODO: Make this configurable
    policyReader_.scheduleAtFixedRate(new AuthorizationPolicyReader(), 0, 60,
        TimeUnit.SECONDS);
  }

  /**
   * Refreshes the authorization policy metadata by querying the Sentry Policy Service.
   * There is currently no way to get a snapshot of the policy from the Sentry Service,
   * so it is possible that Impala will end up in a state that is not consistent with a
   * state the Sentry Service has ever been in. For example, consider the case where a
   * refresh is running and all privileges for Role A have been processed. Before moving
   * to Role B, the user revokes a privilege from Role A and grants it to Role B.
   * Impala will temporarily (until the next refresh) think the privilege is granted to
   * Role A AND to Role B.
   * TODO: Think more about consistency as well as how to recover from errors that leave
   * the policy in a potentially inconsistent state (an RPC fails part-way through a
   * refresh). We should also consider applying this entire update to the catalog
   * atomically.
   */
  private class AuthorizationPolicyReader implements Runnable {
    public void run() {
      // Assume all roles should be removed. Then query the Policy Service and remove
      // roles from this set that actually exist.
      Set<String> rolesToRemove = catalog_.getAuthPolicy().getAllRoleNames();
      try {
        // Read the full policy, adding new/modified roles to "updatedRoles".
        for (TSentryRole sentryRole: sentryPolicyService_.listAllRoles()) {
          // This role exists and should not be removed, delete it from the rolesToRemove
          // set.
          rolesToRemove.remove(sentryRole.getRoleName().toLowerCase());

          Set<String> grantGroups = Sets.newHashSet();
          for (TSentryGroup group: sentryRole.getGroups()) {
            grantGroups.add(group.getGroupName());
          }
          Role existingRole = catalog_.getAuthPolicy().getRole(sentryRole.getRoleName());
          Role role;
          // These roles are the same, use the current role.
          if (existingRole != null &&
              existingRole.getGrantGroups().equals(grantGroups)) {
            role = existingRole;
          } else {
            role = catalog_.addRole(sentryRole.getRoleName(), grantGroups);
          }

          // Assume all privileges should be removed. Privileges that still exist are
          // deleted from this set and we are left with the set of privileges that need
          // to be removed.
          Set<String> privilegesToRemove = role.getPrivilegeNames();

          // Check all the privileges that are part of this role.
          for (TSentryPrivilege sentryPriv:
              sentryPolicyService_.listRolePrivileges(role.getName())) {
            TPrivilege thriftPriv =
                SentryPolicyService.sentryPrivilegeToTPrivilege(sentryPriv);
            thriftPriv.setRole_id(role.getId());
            privilegesToRemove.remove(thriftPriv.getPrivilege_name().toLowerCase());

            RolePrivilege existingPriv =
                role.getPrivilege(thriftPriv.getPrivilege_name());
            // We already know about this privilege (privileges cannot be modified).
            if (existingPriv != null &&
                existingPriv.getCreateTimeMs() == sentryPriv.getCreateTime()) {
              continue;
            }
            catalog_.addRolePrivilege(role.getName(), thriftPriv);
          }

          // Remove the privileges that no longer exist.
          for (String privilegeName: privilegesToRemove) {
            TPrivilege privilege = new TPrivilege();
            privilege.setPrivilege_name(privilegeName);
            catalog_.removeRolePrivilege(role.getName(), privilege);
          }
        }
      } catch (Exception e) {
        LOG.error("Error refreshing Sentry policy: ", e);
        return;
      }

      // Remove all the roles, incrementing the catalog version to indicate
      // a change.
      for (String roleName: rolesToRemove) {
        catalog_.removeRole(roleName);
      }
    }
  }

  /**
   * Perfoms a synchronous refresh of all authorization policy metadata and updates
   * the Catalog with any changes. Throws an ImpalaRuntimeException if there are any
   * errors executing the refresh job.
   */
  public void refresh() throws ImpalaRuntimeException {
    try {
      policyReader_.submit(new AuthorizationPolicyReader()).get();
    } catch (Exception e) {
      // We shouldn't make it here. It means an exception leaked from the
      // AuthorizationPolicyReader.
      throw new ImpalaRuntimeException("Error refreshing authorization policy, " +
          "current policy state may be inconsistent. Running 'invalidate metadata' " +
          "may resolve this problem: ", e);
    }
  }
}
