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

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;

import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.CatalogServiceCatalog;
import com.cloudera.impala.catalog.Role;
import com.cloudera.impala.catalog.RolePrivilege;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.thrift.TPrivilege;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Thread safe class that acts as a link between the Sentry Service and the Catalog
 * to ensure both places are updated consistently. More specifically, this class
 * synchronizes updates to the Sentry Service and the Impala catalog to ensure
 * they are applied atomically (in Impala's view) and only if reading/writing the
 * policy via the Sentry Service succeeds. Note that there may be external updates
 * to the Sentry Service that cannot be protected against.
 * It also periodically refreshes the authorization policy metadata and updates the
 * catalog with any changes. Because any catalog updates need to be synchronized with
 * updates from GRANT/REVOKE statements, it makes sense for this class to
 * synchronize all modifications.
 */
public class SentryProxy {
  private static final Logger LOG = Logger.getLogger(SentryProxy.class);

  // Used to periodically poll the Sentry Service and updates the catalog with any
  // changes.
  private final ScheduledExecutorService policyReader_ =
      Executors.newScheduledThreadPool(1);

  // The Catalog the SentryPolicyUpdater is associated with.
  private final CatalogServiceCatalog catalog_;

  // The interface to access the Sentry Policy Service to read policy metadata.
  private final SentryPolicyService sentryPolicyService_;

  // This is user that the Catalog Service is running as. This user should always be a
  // Sentry Service admin => have full rights to read/update the Sentry Service.
  private final User processUser_ = new User(System.getProperty("user.name"));

  public SentryProxy(SentryConfig sentryConfig, CatalogServiceCatalog catalog) {
    Preconditions.checkNotNull(catalog);
    Preconditions.checkNotNull(sentryConfig);
    catalog_ = catalog;
    sentryPolicyService_ = new SentryPolicyService(sentryConfig);
    // Sentry Service is enabled.
    // TODO: Make this configurable
    policyReader_.scheduleAtFixedRate(new PolicyReader(), 0, 60,
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
  private class PolicyReader implements Runnable {
    public void run() {
      synchronized (SentryProxy.this) {
        // Assume all roles should be removed. Then query the Policy Service and remove
        // roles from this set that actually exist.
        Set<String> rolesToRemove = catalog_.getAuthPolicy().getAllRoleNames();
        try {
          // Read the full policy, adding new/modified roles to "updatedRoles".
          for (TSentryRole sentryRole:
              sentryPolicyService_.listAllRoles(processUser_)) {
            // This role exists and should not be removed, delete it from the
            // rolesToRemove set.
            rolesToRemove.remove(sentryRole.getRoleName().toLowerCase());

            Set<String> grantGroups = Sets.newHashSet();
            for (TSentryGroup group: sentryRole.getGroups()) {
              grantGroups.add(group.getGroupName());
            }
            Role existingRole =
                catalog_.getAuthPolicy().getRole(sentryRole.getRoleName());
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
                sentryPolicyService_.listRolePrivileges(processUser_, role.getName())) {
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
  }

  /**
   * Checks whether this user is an admin on the Sentry Service. Throws an
   * AuthorizationException if the user does not have admin privileges or if there are
   * any issues communicating with the Sentry Service..
   * @param requestingUser - The requesting user.
   */
  public void checkUserSentryAdmin(User requestingUser)
      throws AuthorizationException {
    // Check if the user has access by issuing a read-only RPC.
    // TODO: This is not an elegant way to verify whether the user has privileges to
    // access Sentry. This should be modified in the future when Sentry has
    // a more robust mechanism to perform these checks.
    try {
      sentryPolicyService_.listAllRoles(requestingUser);
    } catch (ImpalaException e) {
      throw new AuthorizationException(String.format("User '%s' does not have " +
          "privileges to access the requested policy metadata or Sentry Service is " +
          "unavailable.", requestingUser.getName()));
    }
  }

  /**
   * Creates a new role using the Sentry Service and updates the Impala catalog.
   * If the RPC to the Sentry Service fails the Impala catalog will not
   * be modified. Returns the new Role.
   * Throws exception if there was any error updating the Sentry Service or
   * if a role with the same name already exists in the catalog. This includes
   * the case where a role was added externally (eg. via Hive). If the role was added
   * externally, Impala will load it during the next refresh of the policy.
   * TODO: Consider adding the role to the policy if we find it was created
   * externally.
   */
  public synchronized Role createRole(User user, String roleName)
      throws ImpalaException {
    Role role = null;
    if (catalog_.getAuthPolicy().getRole(roleName) != null) {
      throw new CatalogException("Role already exists: " + roleName);
    }
    sentryPolicyService_.createRole(user, roleName, false);
    // Initially the role has no grant groups (empty set).
    role = catalog_.addRole(roleName, Sets.<String>newHashSet());
    return role;
  }

  /**
   * Drops the given role using the Sentry Service and updates the Impala catalog.
   * If the RPC to the Sentry Service fails the Impala catalog will not
   * be modified. Returns the removed Role or null if the role did not exist in the
   * Catalog.
   * Throws exception if there was any error updating the Sentry Service.
   */
  public synchronized Role dropRole(User user, String roleName) throws ImpalaException {
    sentryPolicyService_.dropRole(user, roleName, false);
    return catalog_.removeRole(roleName);
  }

  /**
   * Removes the role grant group using the Sentry Service and updates the Impala
   * catalog. If the RPC to the Sentry Service fails the Impala catalog will not
   * be modified. Returns the updated Role.
   * Throws exception if there was any error updating the Sentry Service or if the Impala
   * catalog does not contain the given role name.
   */
  public synchronized Role grantRoleGroup(User user, String roleName, String groupName)
      throws ImpalaException {
    sentryPolicyService_.grantRoleToGroup(user, roleName, groupName);
    return catalog_.addRoleGrantGroup(roleName, groupName);
  }

  /**
   * Removes the role grant group using the Sentry Service and updates the Impala
   * catalog. If the RPC to the Sentry Service fails the Impala catalog will not
   * be modified. Returns the updated Role.
   * Throws exception if there was any error updating the Sentry Service or if the Impala
   * catalog does not contain the given role name.
   */
  public synchronized Role revokeRoleGroup(User user, String roleName, String groupName)
      throws ImpalaException {
    sentryPolicyService_.revokeRoleFromGroup(user, roleName, groupName);
    return catalog_.removeRoleGrantGroup(roleName, groupName);
  }

  /**
   * Grants privileges to a role in the Sentry Service and updates the Impala
   * catalog. If the RPC to the Sentry Service fails, the Impala catalog will not
   * be modified. Returns the granted privileges.
   * Throws exception if there was any error updating the Sentry Service or if the Impala
   * catalog does not contain the given role name.
   */
  public synchronized List<RolePrivilege> grantRolePrivileges(User user,
      String roleName, List<TPrivilege> privileges) throws ImpalaException {
    sentryPolicyService_.grantRolePrivileges(user, roleName, privileges);
    // Update the catalog
    List<RolePrivilege> rolePrivileges = Lists.newArrayList();
    for (TPrivilege privilege: privileges) {
      rolePrivileges.add(catalog_.addRolePrivilege(roleName, privilege));
    }
    return rolePrivileges;
  }

  /**
   * Revokes privileges from a role in the Sentry Service and updates the Impala
   * catalog. If the RPC to the Sentry Service fails the Impala catalog will not be
   * modified. Returns the removed privileges. Throws an exception if there was any error
   * updating the Sentry Service or if the Impala catalog does not contain the given role
   * name.
   */
  public synchronized List<RolePrivilege> revokeRolePrivileges(User user,
      String roleName, List<TPrivilege> privileges, boolean hasGrantOption)
      throws ImpalaException {
    List<RolePrivilege> rolePrivileges = Lists.newArrayList();
    if (!hasGrantOption) {
      sentryPolicyService_.revokeRolePrivileges(user, roleName, privileges);
      // Update the catalog
      for (TPrivilege privilege: privileges) {
        RolePrivilege rolePriv = catalog_.removeRolePrivilege(roleName, privilege);
        if (rolePriv == null) {
          rolePriv = RolePrivilege.fromThrift(privilege);
          rolePriv.setCatalogVersion(catalog_.getCatalogVersion());
        }
        rolePrivileges.add(rolePriv);
      }
    } else {
      // If the REVOKE GRANT OPTION has been specified, the privileges should not be
      // removed, they should just be updated to clear the GRANT OPTION flag. Sentry
      // does not yet provide an "alter privilege" API so we need to revoke the
      // privileges and re-grant them.
      sentryPolicyService_.revokeRolePrivileges(user, roleName, privileges);
      List<TPrivilege> updatedPrivileges = Lists.newArrayList();
      for (TPrivilege privilege: privileges) {
        RolePrivilege existingPriv = catalog_.getRolePrivilege(roleName, privilege);
        if (existingPriv == null) {
          RolePrivilege rolePriv = RolePrivilege.fromThrift(privilege);
          rolePriv.setCatalogVersion(catalog_.getCatalogVersion());
          rolePrivileges.add(rolePriv);
          continue;
        }
        TPrivilege updatedPriv = existingPriv.toThrift();
        updatedPriv.setHas_grant_opt(false);
        updatedPrivileges.add(updatedPriv);
      }
      // Re-grant the updated privileges.
      sentryPolicyService_.grantRolePrivileges(user, roleName, updatedPrivileges);
      // Update the catalog
      for (TPrivilege updatedPriv: updatedPrivileges) {
        rolePrivileges.add(catalog_.addRolePrivilege(roleName, updatedPriv));
      }
    }
    return rolePrivileges;
  }

  /**
   * Perfoms a synchronous refresh of all authorization policy metadata and updates
   * the Catalog with any changes. Throws an ImpalaRuntimeException if there are any
   * errors executing the refresh job.
   */
  public void refresh() throws ImpalaRuntimeException {
    try {
      policyReader_.submit(new PolicyReader()).get();
    } catch (Exception e) {
      // We shouldn't make it here. It means an exception leaked from the
      // AuthorizationPolicyReader.
      throw new ImpalaRuntimeException("Error refreshing authorization policy, " +
          "current policy state may be inconsistent. Running 'invalidate metadata' " +
          "may resolve this problem: ", e);
    }
  }
}
