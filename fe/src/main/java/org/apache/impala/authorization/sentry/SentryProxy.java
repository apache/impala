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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Principal;
import org.apache.impala.catalog.PrincipalPrivilege;
import org.apache.impala.catalog.Role;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.log4j.Logger;
import org.apache.sentry.api.service.thrift.TSentryGroup;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.api.service.thrift.TSentryRole;

import org.apache.impala.authorization.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TPrivilege;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.service.common.SentryOwnerPrivilegeType;
import org.apache.sentry.service.common.ServiceConstants;
import org.apache.thrift.transport.TTransportException;

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
  /**
   * This class keeps track of catalog objects added and removed.
   */
  @VisibleForTesting
  protected static class AuthorizationDelta {
    private final List<TCatalogObject> added_;
    private final List<TCatalogObject> removed_;

    public AuthorizationDelta() {
      this(new ArrayList<>(), new ArrayList<>());
    }

    public AuthorizationDelta(List<TCatalogObject> added,
        List<TCatalogObject> removed) {
      added_ = added;
      removed_ = removed;
    }

    public List<TCatalogObject> getAddedCatalogObjects() { return added_; }
    public List<TCatalogObject> getRemovedCatalogObjects() { return removed_; }
  }

  private static final Logger LOG = Logger.getLogger(SentryProxy.class);

  // Used to periodically poll the Sentry Service and updates the catalog with any
  // changes.
  private final ScheduledExecutorService policyReader_ =
      Executors.newScheduledThreadPool(1);

  // The Catalog the SentryPolicyUpdater is associated with.
  private final CatalogServiceCatalog catalog_;

  // The interface to access the Sentry Policy Service to read policy metadata.
  private final SentryPolicyService sentryPolicyService_;

  // This is the user that the Catalog Service is running as. For kerberized clusters,
  // this is set to the Kerberos principal of Catalog. This user should always be a
  // Sentry Service admin => have full rights to read/update the Sentry Service.
  private final User processUser_;

  // The value for the object ownership config.
  private final String objectOwnershipConfigValue_;

  public SentryProxy(SentryAuthorizationConfig authzConfig, CatalogServiceCatalog catalog)
      throws ImpalaException {
    Preconditions.checkNotNull(authzConfig);
    SentryConfig sentryConfig = authzConfig.getSentryConfig();
    Preconditions.checkNotNull(catalog);
    Preconditions.checkNotNull(sentryConfig);
    catalog_ = catalog;
    String kerberosPrincipal = BackendConfig.INSTANCE.getBackendCfg().getPrincipal();
    if (Strings.isNullOrEmpty(kerberosPrincipal)) {
      processUser_ = new User(System.getProperty("user.name"));
    } else {
      processUser_ = new User(kerberosPrincipal);
    }
    sentryPolicyService_ = new SentryPolicyService(sentryConfig);

    // For some tests, we create a config but may not have a config file.
    if (sentryConfig.getConfigFile() != null && !sentryConfig.getConfigFile().isEmpty()) {
      objectOwnershipConfigValue_ = sentryPolicyService_
          .getConfigValue(ServiceConstants.ServerConfig
          .SENTRY_DB_POLICY_STORE_OWNER_AS_PRIVILEGE);
    } else {
      objectOwnershipConfigValue_ = SentryOwnerPrivilegeType.NONE.toString();
    }
    // We configure the PolicyReader to swallow any exception because we do not want to
    // kill the PolicyReader background thread on exception.
    policyReader_.scheduleAtFixedRate(new PolicyReader(/*reset versions*/ false,
        /*swallow exception*/ true, new AuthorizationDelta()), 0,
        BackendConfig.INSTANCE.getSentryCatalogPollingFrequency(), TimeUnit.SECONDS);
  }

  /**
   * Refreshes the authorization policy metadata by querying the Sentry Policy Service.
   * There is currently no way to get a snapshot of the policy from the Sentry Service,
   * so it is possible that Impala will end up in a state that is not consistent with a
   * state the Sentry Service has ever been in. For example, consider the case where a
   * refresh is running and all privileges for Principal A have been processed. Before
   * moving to Principal B, the user revokes a privilege from Principal A and grants it to
   * Principal B. Impala will temporarily (until the next refresh) think the privilege is
   * granted to Principal A AND to Principal B.
   * TODO: Think more about consistency as well as how to recover from errors that leave
   * the policy in a potentially inconsistent state (an RPC fails part-way through a
   * refresh). We should also consider applying this entire update to the catalog
   * atomically.
   */
  private class PolicyReader implements Runnable {
    private final boolean resetVersions_;
    // A flag to indicate whether or not to swallow any exception thrown.
    private final boolean swallowException_;
    // Keep track of catalog objects added/removed.
    private final AuthorizationDelta authzDelta_;

    public PolicyReader(boolean resetVersions, boolean swallowException,
        AuthorizationDelta authzDelta) {
      resetVersions_ = resetVersions;
      swallowException_ = swallowException;
      authzDelta_ = authzDelta;
    }

    public void run() {
      synchronized (SentryProxy.this) {
        refreshSentryAuthorization(catalog_, sentryPolicyService_, processUser_,
            resetVersions_, swallowException_, authzDelta_);
      }
    }
  }

  /**
   * Refreshes Sentry authorization and updates the catalog.
   */
  @VisibleForTesting
  static void refreshSentryAuthorization(CatalogServiceCatalog catalog,
      SentryPolicyService sentryPolicyService, User processUser, boolean resetVersions,
      boolean swallowException, AuthorizationDelta authzDelta) {
    long startTime = System.currentTimeMillis();
    try {
      refreshRolePrivileges(catalog, sentryPolicyService, processUser, resetVersions,
          authzDelta);
      refreshUserPrivileges(catalog, sentryPolicyService, processUser, resetVersions,
          authzDelta);
    } catch (Exception e) {
      LOG.error("Error refreshing Sentry policy: ", e);
      if (swallowException) return;
      // We need to differentiate between Sentry not available exception and
      // any other exceptions.
      if (e.getCause() != null && e.getCause() instanceof SentryUserException) {
        Throwable sentryException = e.getCause();
        if (sentryException.getCause() != null &&
            sentryException.getCause() instanceof TTransportException) {
          throw new SentryUnavailableException(e);
        }
      }
      throw new SentryPolicyReaderException(e);
    } finally {
      LOG.debug("Refreshing Sentry policy took " +
          (System.currentTimeMillis() - startTime) + "ms");
    }
  }

  /**
   * Updates all roles and their associated privileges in the catalog by adding,
   * removing, and replacing the catalog objects to match those in Sentry since
   * the last sentry sync update.
   */
  private static void refreshRolePrivileges(CatalogServiceCatalog catalog,
      SentryPolicyService sentryPolicyService, User processUser, boolean resetVersions,
      AuthorizationDelta authzDelta) throws ImpalaException {
    // Assume all roles should be removed. Then query the Policy Service and remove
    // roles from this set that actually exist.
    Set<String> rolesToRemove = catalog.getAuthPolicy().getAllRoleNames();
    // The keys (role names) in listAllRolesPrivileges here are always in lower case.
    Map<String, Set<TSentryPrivilege>> allRolesPrivileges =
        sentryPolicyService.listAllRolesPrivileges(processUser);
    // Read the full policy, adding new/modified roles to "updatedRoles".
    for (TSentryRole sentryRole:
        sentryPolicyService.listAllRoles(processUser)) {
      // This role exists and should not be removed, delete it from the
      // rolesToRemove set.
      rolesToRemove.remove(sentryRole.getRoleName().toLowerCase());

      Set<String> grantGroups = Sets.newHashSet();
      for (TSentryGroup group: sentryRole.getGroups()) {
        grantGroups.add(group.getGroupName());
      }
      Role existingRole =
          catalog.getAuthPolicy().getRole(sentryRole.getRoleName());
      Role role;
      // These roles are the same, use the current role.
      if (existingRole != null &&
          existingRole.getGrantGroups().equals(grantGroups)) {
        role = existingRole;
        if (resetVersions) {
          role.setCatalogVersion(catalog.incrementAndGetCatalogVersion());
        }
      } else {
        LOG.debug("Adding role: " + sentryRole.getRoleName());
        role = catalog.addRole(sentryRole.getRoleName(), grantGroups);
        authzDelta.getAddedCatalogObjects().add(role.toTCatalogObject());
      }
      // allRolesPrivileges keys and sentryRole.getName() are used here since they both
      // come from Sentry so they agree in case.
      refreshPrivilegesInCatalog(catalog, resetVersions, sentryRole.getRoleName(), role,
          allRolesPrivileges, authzDelta);
    }
    // Remove all the roles, incrementing the catalog version to indicate
    // a change.
    for (String roleName: rolesToRemove) {
      LOG.debug("Removing role: " + roleName);
      authzDelta.getRemovedCatalogObjects().add(
          catalog.removeRole(roleName).toTCatalogObject());
    }
  }

  /**
   * Updates all users and their associated privileges in the catalog by adding,
   * removing, and replacing the catalog objects to match those in Sentry since the
   * last Sentry sync update. Take note that we only store the users with privileges
   * stored in Sentry and not all available users in the system. User privileges do not
   * support grant groups.
   */
  private static void refreshUserPrivileges(CatalogServiceCatalog catalog,
      SentryPolicyService sentryPolicyService, User processUser, boolean resetVersions,
      AuthorizationDelta authzDelta) throws ImpalaException {
    // Assume all users should be removed. Then query the Policy Service and remove
    // users from this set that actually exist.
    Set<String> usersToRemove = catalog.getAuthPolicy().getAllUserNames();
    // The keys (user names) in listAllUsersPrivileges here are always in lower case.
    Map<String, Set<TSentryPrivilege>> allUsersPrivileges =
        sentryPolicyService.listAllUsersPrivileges(processUser);
    for (Map.Entry<String, Set<TSentryPrivilege>> userPrivilegesEntry:
        allUsersPrivileges.entrySet()) {
      String userName = userPrivilegesEntry.getKey();
      // This user exists and should not be removed so remove it from the
      // usersToRemove set.
      usersToRemove.remove(userName);

      Reference<Boolean> existingUser = new Reference<>();
      org.apache.impala.catalog.User user = catalog.addUserIfNotExists(userName,
          existingUser);
      if (existingUser.getRef() && resetVersions) {
        user.setCatalogVersion(catalog.incrementAndGetCatalogVersion());
      } else if (!existingUser.getRef()) {
        LOG.debug("Adding user: " + user.getName());
        authzDelta.getAddedCatalogObjects().add(user.toTCatalogObject());
      }
      // allUsersPrivileges keys and userPrivilegesEntry.getKey() are used here since
      // they both come from Sentry so they agree in case.
      refreshPrivilegesInCatalog(catalog, resetVersions, userPrivilegesEntry.getKey(),
          user, allUsersPrivileges, authzDelta);
    }
    // Remove all the users, incrementing the catalog version to indicate
    // a change.
    for (String userName: usersToRemove) {
      LOG.debug("Removing user: " + userName);
      authzDelta.getRemovedCatalogObjects().add(
          catalog.removeUser(userName).toTCatalogObject());
    }
  }

  /**
   * Updates the privileges for a given principal in the catalog since the last Sentry
   * sync update. The sentryPrincipalName is used to match against the key in
   * allPrincipalPrivileges, which both come from Sentry, so they should have the
   * same case sensitivity.
   */
  private static void refreshPrivilegesInCatalog(CatalogServiceCatalog catalog,
      boolean resetVersions, String sentryPrincipalName, Principal principal,
      Map<String, Set<TSentryPrivilege>> allPrincipalPrivileges,
      AuthorizationDelta authzDelta) throws CatalogException {
    // Assume all privileges should be removed. Privileges that still exist are
    // deleted from this set and we are left with the set of privileges that need
    // to be removed.
    Set<String> privilegesToRemove = principal.getPrivilegeNames();
    // It is important to get a set of privileges using sentryPrincipalName
    // and not principal.getName() because principal.getName() may return a
    // principal name with a different case than the principal names stored
    // in allPrincipalPrivileges. See IMPALA-7729 for more information.
    Set<TSentryPrivilege> sentryPrivileges = allPrincipalPrivileges.get(
        sentryPrincipalName);
    if (sentryPrivileges == null) {
      sentryPrivileges = Sets.newHashSet();
    }
    // Check all the privileges that are part of this principal.
    for (TSentryPrivilege sentryPriv: sentryPrivileges) {
      TPrivilege thriftPriv =
          SentryPolicyService.sentryPrivilegeToTPrivilege(sentryPriv, principal);
      String privilegeName = PrincipalPrivilege.buildPrivilegeName(thriftPriv);
      privilegesToRemove.remove(privilegeName);
      PrincipalPrivilege existingPrincipalPriv = principal.getPrivilege(privilegeName);
      // We already know about this privilege (privileges cannot be modified).
      if (existingPrincipalPriv != null &&
          existingPrincipalPriv.getCreateTimeMs() == sentryPriv.getCreateTime()) {
        if (resetVersions) {
          existingPrincipalPriv.setCatalogVersion(
              catalog.incrementAndGetCatalogVersion());
        }
        continue;
      }
      if (principal.getPrincipalType() == TPrincipalType.ROLE) {
        LOG.debug("Adding role privilege: " + privilegeName);
        authzDelta.getAddedCatalogObjects().add(
            catalog.addRolePrivilege(principal.getName(), thriftPriv).toTCatalogObject());
      } else {
        LOG.debug("Adding user privilege: " + privilegeName);
        authzDelta.getAddedCatalogObjects().add(
            catalog.addUserPrivilege(principal.getName(), thriftPriv).toTCatalogObject());
      }
    }

    // Remove the privileges that no longer exist.
    for (String privilegeName: privilegesToRemove) {
      if (principal.getPrincipalType() == TPrincipalType.ROLE) {
        LOG.debug("Removing role privilege: " + privilegeName);
        authzDelta.getRemovedCatalogObjects().add(
            catalog.removeRolePrivilege(principal.getName(), privilegeName)
            .toTCatalogObject());
      } else {
        LOG.debug("Removing user privilege: " + privilegeName);
        authzDelta.getRemovedCatalogObjects().add(
            catalog.removeUserPrivilege(principal.getName(), privilegeName)
            .toTCatalogObject());
      }
    }
  }

  /**
   * Checks whether this user is an admin on the Sentry Service.
   */
  public boolean isSentryAdmin(User requestingUser)
      throws AuthorizationException {
    // Check if the user has access by issuing a read-only RPC.
    // TODO: This is not an elegant way to verify whether the user has privileges to
    // access Sentry. This should be modified in the future when Sentry has
    // a more robust mechanism to perform these checks.
    try {
      sentryPolicyService_.listAllRoles(requestingUser);
    } catch (ImpalaException e) {
      return false;
    }
    return true;
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
   * This code is odd because we need to avoid duplicate privileges in Sentry because
   * the same privilege with and without grant option are two different privileges.
   * https://issues.apache.org/jira/browse/SENTRY-2408
   * If the current privilege and the requested privilege have the same grant option
   * state, then just execute the grant. This is necessary so that if the user does not
   * have the ability to grant, Sentry will throw an exception. We don't want to
   * expose data by skipping the grant if it already exists.
   * For the case when the existing privilege does not have the grant option, but the
   * request does, we need to first add the new privilege, then revoke the old one.
   * If this is done in the wrong order, and an exception is thrown, the user will get
   * a "REVOKE_PRIVILEGE" error on a grant.
   * For the case when the existing privilege does have the grant option, but the
   * request does not, we add the grant option to the request because the privilege
   * should not be "downgraded", we don't want to have duplicates, and we still need
   * Sentry to perform the "has grant ability" check. Downgraded indicates changing a
   * privilege from one that has a grant option to one that does not.
   */
  public synchronized List<PrincipalPrivilege> grantRolePrivileges(User user,
      String roleName, List<TPrivilege> privileges, boolean hasGrantOption,
      List<PrincipalPrivilege> removedPrivileges) throws ImpalaException {
    // First find out what's in the catalog. All privileges will have the same grant
    // option set. The only case there will be more than one privilege is in the case
    // of multiple column privileges.
    Preconditions.checkArgument(!privileges.isEmpty());
    TPrivilege tWithGrant = PrincipalPrivilege.copyPrivilegeWithGrant(privileges.get(0),
        true);
    PrincipalPrivilege catWithGrant = catalog_.getPrincipalPrivilege(roleName,
        tWithGrant);
    TPrivilege tNoGrant = PrincipalPrivilege.copyPrivilegeWithGrant(privileges.get(0),
        false);
    PrincipalPrivilege catNoGrant = catalog_.getPrincipalPrivilege(roleName, tNoGrant);

    // List of privileges that should be removed. If removed, they will be added to
    // the removedPrivileges list.
    List<TPrivilege> toRemove = null;

    if (catNoGrant != null && hasGrantOption) {
      toRemove = privileges.stream().map(p ->
          PrincipalPrivilege.copyPrivilegeWithGrant(p, false))
          .collect(Collectors.toList());
    } else if (catWithGrant != null && !hasGrantOption) {
      // Elevate the requested privileges.
      privileges = privileges.stream().map(p -> p.setHas_grant_opt(true))
          .collect(Collectors.toList());
    }

    // This is a list of privileges that were added, to be returned.
    List<PrincipalPrivilege> rolePrivileges = Lists.newArrayList();
    // Do the grants first
    sentryPolicyService_.grantRolePrivileges(user, roleName, privileges);
    // Update the catalog
    for (TPrivilege privilege: privileges) {
      rolePrivileges.add(catalog_.addRolePrivilege(roleName, privilege));
    }

    // Then the revokes
    if (toRemove != null && !toRemove.isEmpty()) {
      sentryPolicyService_.revokeRolePrivileges(user, roleName, toRemove);
      for (TPrivilege privilege : toRemove) {
        PrincipalPrivilege rolePriv = catalog_.removeRolePrivilege(roleName,
            PrincipalPrivilege.buildPrivilegeName(privilege));
        if (rolePriv == null) continue;
        removedPrivileges.add(rolePriv);
      }
      // If we removed anything, it might have removed the grants, so redo the grants.
      // TODO: https://issues.apache.org/jira/browse/SENTRY-2408
      // When Sentry adds API to modify privileges, this code can be refactored.
      sentryPolicyService_.grantRolePrivileges(user, roleName, privileges);
    }
    return rolePrivileges;
  }

  /**
   * Revokes privileges from a role in the Sentry Service and updates the Impala
   * catalog. If the RPC to the Sentry Service fails the Impala catalog will not be
   * modified. Returns the removed privileges. Throws an exception if there was any error
   * updating the Sentry Service or if the Impala catalog does not contain the given role
   * name.  The addedPrivileges parameter will be populated with any new privileges that
   * are granted, in the case where the revoke involves a grant option.  In this case
   * privileges that contain the grant option are removed and the new privileges without
   * the grant option are added.
   *
   * The revoke code is confusing because of the various usages of "grant option". The
   * parameter "hasGrantOption" indicates that the revoke should just remove the grant
   * option from an existing privilege. The grant option on the privilege indicates
   * whether the existing privilege has the grant option set which for the case of
   * revokes, there's currently no SQL statement that will result in the grant option
   * being set on the request.
   */
  public synchronized List<PrincipalPrivilege> revokeRolePrivileges(User user,
      String roleName, List<TPrivilege> privileges, boolean hasGrantOption,
      List<PrincipalPrivilege> addedPrivileges) throws ImpalaException {
    List<PrincipalPrivilege> rolePrivileges = Lists.newArrayList();
    if (!hasGrantOption) {
      sentryPolicyService_.revokeRolePrivileges(user, roleName, privileges);
      // Update the catalog. The catalog should only have one privilege whether it has
      // the grant option or not. We need to remove it which ever one is set. Since the
      // catalog object name for privileges contains the grantoption value, we need to
      // check both.
      for (TPrivilege privilege: privileges) {
        TPrivilege privNotGrant = privilege.deepCopy();
        privNotGrant.setHas_grant_opt(!privilege.has_grant_opt);
        PrincipalPrivilege rolePrivNotGrant = catalog_.removeRolePrivilege(roleName,
            PrincipalPrivilege.buildPrivilegeName(privNotGrant));
        PrincipalPrivilege rolePriv = catalog_.removeRolePrivilege(roleName,
            PrincipalPrivilege.buildPrivilegeName(privilege));
        if (rolePrivNotGrant != null) {
          rolePrivileges.add(rolePrivNotGrant);
        }
        if (rolePriv != null) {
          rolePrivileges.add(rolePriv);
        }
      }
    } else {
      // If the REVOKE GRANT OPTION has been specified, the privileges with grant must be
      // revoked and these same privileges are added back to the catalog with the grant
      // option set to false. They can not simply be updated because "grantoption" is part
      // of the name of the catalog object. Sentry does not yet provide an
      // "alter privilege" API so we need to revoke the privileges and re-grant them.
      sentryPolicyService_.revokeRolePrivileges(user, roleName, privileges);
      List<TPrivilege> newPrivs = Lists.newArrayList();
      for (TPrivilege privilege: privileges) {
        PrincipalPrivilege existingPriv = catalog_.getPrincipalPrivilege(roleName,
            privilege);
        if (existingPriv == null) continue;
        rolePrivileges.add(catalog_.removeRolePrivilege(roleName,
            PrincipalPrivilege.buildPrivilegeName(privilege)));

        TPrivilege addedPriv = new TPrivilege(existingPriv.toThrift());
        addedPriv.setHas_grant_opt(false);
        newPrivs.add(addedPriv);
      }
      // Re-grant the updated privileges.
      if (!newPrivs.isEmpty()) {
        sentryPolicyService_.grantRolePrivileges(user, roleName, newPrivs);
      }
      // Update the catalog
      for (TPrivilege newPriv: newPrivs) {
        addedPrivileges.add(catalog_.addRolePrivilege(roleName, newPriv));
      }
    }
    return rolePrivileges;
  }

  /**
   * Performs a synchronous refresh of all authorization policy metadata and updates
   * the Catalog with any changes. Throws an ImpalaRuntimeException if there are any
   * errors executing the refresh job.
   */
  public void refresh(boolean resetVersions, List<TCatalogObject> added,
      List<TCatalogObject> removed) throws ImpalaRuntimeException {
    try {
      // Since this is a synchronous refresh, any exception thrown while running the
      // refresh should be thrown here instead of silently swallowing it.
      policyReader_.submit(new PolicyReader(resetVersions, /*swallow exception*/ false,
          new AuthorizationDelta(added, removed))).get();
    } catch (Exception e) {
      if (e.getCause() != null && e.getCause() instanceof SentryUnavailableException) {
        throw new ImpalaRuntimeException("Error refreshing authorization policy. " +
            "Sentry is unavailable. Ensure Sentry is up: ", e);
      }
      throw new ImpalaRuntimeException("Error refreshing authorization policy, " +
          "current policy state may be inconsistent. Running 'invalidate metadata' " +
          "may resolve this problem: ", e);
    }
  }

  /**
   * Checks if object ownership is enabled in Sentry.
   */
  public boolean isObjectOwnershipEnabled() {
    if (objectOwnershipConfigValue_.length() == 0 ||
        objectOwnershipConfigValue_.equalsIgnoreCase(
            SentryOwnerPrivilegeType.NONE.toString())) {
      return false;
    }
    return true;
  }

  /**
   * Checks if 'with grant' is enabled for object ownership in Sentry.
   */
  public boolean isObjectOwnershipGrantEnabled() {
    if (objectOwnershipConfigValue_.equalsIgnoreCase(
        SentryOwnerPrivilegeType.ALL_WITH_GRANT.toString())) {
      return true;
    }
    return false;
  }
}
