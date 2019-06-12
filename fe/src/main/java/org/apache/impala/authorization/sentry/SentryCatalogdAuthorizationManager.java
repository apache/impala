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
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.authorization.AuthorizationDelta;
import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogObject;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Principal;
import org.apache.impala.catalog.PrincipalPrivilege;
import org.apache.impala.catalog.Role;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Reference;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowGrantPrincipalParams;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowRolesResult;
import org.apache.impala.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An implementation of {@link AuthorizationManager} for Catalogd that uses Sentry.
 *
 * The methods here manage the authorization metadata stored in the Catalogd catalog via
 * {@link org.apache.impala.authorization.AuthorizationPolicy} through operations
 * performed by {@link SentryProxy}. Any update to authorization metadata will then be
 * broadcasted to all Impalads.
 *
 * Operations not supported by Sentry will throw an {@link UnsupportedFeatureException}.
 */
public class SentryCatalogdAuthorizationManager implements AuthorizationManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SentryCatalogdAuthorizationManager.class);

  private final CatalogServiceCatalog catalog_;
  // Proxy to access the Sentry Service and also periodically refreshes the
  // policy metadata. Null if Sentry Service is not enabled.
  private final SentryProxy sentryProxy_;

  public SentryCatalogdAuthorizationManager(SentryAuthorizationConfig authzConfig,
      CatalogServiceCatalog catalog) throws ImpalaException {
    sentryProxy_ = new SentryProxy(Preconditions.checkNotNull(authzConfig), catalog);
    catalog_ = Preconditions.checkNotNull(catalog);
  }

  /**
   * Checks if the given user is a Sentry admin.
   */
  public boolean isSentryAdmin(User user) throws ImpalaException {
    return sentryProxy_.isSentryAdmin(user);
  }

  @Override
  public void createRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    verifySentryServiceEnabled();

    Role role = catalog_.getAuthPolicy().getRole(params.getRole_name());
    if (role != null) {
      throw new AuthorizationException(String.format("Role '%s' already exists.",
          params.getRole_name()));
    }

    role = sentryProxy_.createRole(requestingUser, params.getRole_name());
    Preconditions.checkNotNull(role);

    TCatalogObject catalogObject = new TCatalogObject();
    catalogObject.setType(role.getCatalogObjectType());
    catalogObject.setPrincipal(role.toThrift());
    catalogObject.setCatalog_version(role.getCatalogVersion());
    response.result.addToUpdated_catalog_objects(catalogObject);
    response.result.setVersion(role.getCatalogVersion());
  }

  @Override
  public void dropRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    verifySentryServiceEnabled();

    Role role = catalog_.getAuthPolicy().getRole(params.getRole_name());
    if (role == null) {
      throw new AuthorizationException(String.format("Role '%s' does not exist.",
          params.getRole_name()));
    }

    role = sentryProxy_.dropRole(requestingUser, params.getRole_name());
    if (role == null) {
      // Nothing was removed from the catalogd's cache.
      response.result.setVersion(catalog_.getCatalogVersion());
      return;
    }
    Preconditions.checkNotNull(role);

    TCatalogObject catalogObject = new TCatalogObject();
    catalogObject.setType(role.getCatalogObjectType());
    catalogObject.setPrincipal(role.toThrift());
    catalogObject.setCatalog_version(role.getCatalogVersion());
    response.result.addToRemoved_catalog_objects(catalogObject);
    response.result.setVersion(role.getCatalogVersion());
  }

  @Override
  public TShowRolesResult getRoles(TShowRolesParams params) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void grantRoleToGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkArgument(!params.getRole_names().isEmpty());
    Preconditions.checkArgument(!params.getGroup_names().isEmpty());
    verifySentryServiceEnabled();

    if (catalog_.getAuthPolicy().getRole(params.getRole_names().get(0)) == null) {
      throw new AuthorizationException(String.format("Role '%s' does not exist.",
          params.getRole_names().get(0)));
    }

    String roleName = params.getRole_names().get(0);
    String groupName = params.getGroup_names().get(0);
    Role role = sentryProxy_.grantRoleGroup(requestingUser, roleName, groupName);
    Preconditions.checkNotNull(role);
    response.result.addToUpdated_catalog_objects(createRoleObject(role));
    response.result.setVersion(role.getCatalogVersion());
  }

  @Override
  public void revokeRoleFromGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkArgument(!params.getRole_names().isEmpty());
    Preconditions.checkArgument(!params.getGroup_names().isEmpty());
    verifySentryServiceEnabled();

    if (catalog_.getAuthPolicy().getRole(params.getRole_names().get(0)) == null) {
      throw new AuthorizationException(String.format("Role '%s' does not exist.",
          params.getRole_names().get(0)));
    }

    String roleName = params.getRole_names().get(0);
    String groupName = params.getGroup_names().get(0);
    Role role = sentryProxy_.revokeRoleGroup(requestingUser, roleName, groupName);
    response.result.addToUpdated_catalog_objects(createRoleObject(role));
    response.result.setVersion(role.getCatalogVersion());
  }

  private static TCatalogObject createRoleObject(Role role) {
    Preconditions.checkNotNull(role);

    TCatalogObject catalogObject = new TCatalogObject();
    catalogObject.setType(role.getCatalogObjectType());
    catalogObject.setPrincipal(role.toThrift());
    catalogObject.setCatalog_version(role.getCatalogVersion());
    return catalogObject;
  }

  @Override
  public void grantPrivilegeToRole(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    verifySentryServiceEnabled();

    String roleName = params.getPrincipal_name();
    Role role = catalog_.getAuthPolicy().getRole(roleName);
    if (role == null) {
      throw new AuthorizationException(String.format("Role '%s' does not exist.",
          roleName));
    }

    List<TPrivilege> privileges = params.getPrivileges().stream()
        .peek(p -> p.setPrincipal_id(role.getId()))
        .collect(Collectors.toList());
    List<PrincipalPrivilege> removedGrantOptPrivileges =
        Lists.newArrayListWithExpectedSize(privileges.size());
    List<PrincipalPrivilege> addedRolePrivileges =
        sentryProxy_.grantRolePrivileges(new User(header.getRequesting_user()), roleName,
            privileges, params.isHas_grant_opt(), removedGrantOptPrivileges);

    Preconditions.checkNotNull(addedRolePrivileges);
    List<TCatalogObject> updatedPrivs =
        Lists.newArrayListWithExpectedSize(addedRolePrivileges.size());
    for (PrincipalPrivilege rolePriv: addedRolePrivileges) {
      updatedPrivs.add(rolePriv.toTCatalogObject());
    }

    List<TCatalogObject> removedPrivs =
        Lists.newArrayListWithExpectedSize(removedGrantOptPrivileges.size());
    for (PrincipalPrivilege rolePriv: removedGrantOptPrivileges) {
      removedPrivs.add(rolePriv.toTCatalogObject());
    }

    if (!updatedPrivs.isEmpty()) {
      response.result.setUpdated_catalog_objects(updatedPrivs);
      response.result.setVersion(
          updatedPrivs.get(updatedPrivs.size() - 1).getCatalog_version());
      if (!removedPrivs.isEmpty()) {
        response.result.setRemoved_catalog_objects(removedPrivs);
        response.result.setVersion(
            Math.max(getLastItemVersion(updatedPrivs), getLastItemVersion(removedPrivs)));
      }
    }
  }

  @Override
  public void revokePrivilegeFromRole(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    verifySentryServiceEnabled();
    Preconditions.checkArgument(!params.getPrivileges().isEmpty());

    Role role = catalog_.getAuthPolicy().getRole(params.principal_name);
    if (role == null) {
      throw new AuthorizationException(String.format("Role '%s' does not exist.",
          params.getPrincipal_name()));
    }

    String roleName = params.getPrincipal_name();
    List<TPrivilege> privileges = params.getPrivileges().stream()
        .peek(p -> {
          if (role != null) p.setPrincipal_id(role.getId());
        }).collect(Collectors.toList());

    // If this is a revoke of a privilege that contains the grant option, the privileges
    // with the grant option will be revoked and new privileges without the grant option
    // will be added.  The privilege in the catalog cannot simply be updated since the
    // name of the catalog object now contains the grantoption.

    // If privileges contain the grant option and are revoked, this api will return a
    // list of the revoked privileges that contain the grant option. The
    // addedRolePrivileges parameter will contain a list of new privileges without the
    // grant option that are granted. If this is simply a revoke of a privilege without
    // grant options, the api will still return revoked privileges, but the
    // addedRolePrivileges will be empty since there will be no newly granted
    // privileges.
    List<PrincipalPrivilege> addedRolePrivileges =
        Lists.newArrayListWithExpectedSize(privileges.size());
    List<PrincipalPrivilege> removedGrantOptPrivileges =
        sentryProxy_.revokeRolePrivileges(new User(header.getRequesting_user()), roleName,
            privileges, params.isHas_grant_opt(), addedRolePrivileges);
    Preconditions.checkNotNull(addedRolePrivileges);

    List<TCatalogObject> updatedPrivs =
        Lists.newArrayListWithExpectedSize(addedRolePrivileges.size());
    for (PrincipalPrivilege rolePriv : addedRolePrivileges) {
      updatedPrivs.add(rolePriv.toTCatalogObject());
    }

    List<TCatalogObject> removedPrivs =
        Lists.newArrayListWithExpectedSize(removedGrantOptPrivileges.size());
    for (PrincipalPrivilege rolePriv : removedGrantOptPrivileges) {
      removedPrivs.add(rolePriv.toTCatalogObject());
    }

    // If this is a REVOKE statement with hasGrantOpt, only the GRANT OPTION is removed
    // from the privileges. Otherwise the privileges are removed from the catalog.
    if (privileges.get(0).isHas_grant_opt()) {
      if (!updatedPrivs.isEmpty() && !removedPrivs.isEmpty()) {
        response.result.setUpdated_catalog_objects(updatedPrivs);
        response.result.setRemoved_catalog_objects(removedPrivs);
        response.result.setVersion(
            Math.max(getLastItemVersion(updatedPrivs), getLastItemVersion(removedPrivs)));
      }
    } else if (!removedPrivs.isEmpty()) {
      response.result.setRemoved_catalog_objects(removedPrivs);
      response.result.setVersion(
          removedPrivs.get(removedPrivs.size() - 1).getCatalog_version());
    }
  }

  @Override
  public void grantPrivilegeToUser(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException(
        "GRANT <privilege> TO USER is not supported by Sentry.");
  }

  @Override
  public void revokePrivilegeFromUser(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException(
        "REVOKE <privilege> FROM USER is not supported by Sentry.");
  }

  @Override
  public void grantPrivilegeToGroup(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException(
        "GRANT <privilege> TO GROUP is not supported by Sentry.");
  }

  @Override
  public void revokePrivilegeFromGroup(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException(
        "REVOKE <privilege> FROM GROUP is not supported by Sentry.");
  }

  @Override
  public TResultSet getPrivileges(TShowGrantPrincipalParams params)
      throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void updateDatabaseOwnerPrivilege(String serverName, String databaseName,
      String oldOwner, PrincipalType oldOwnerType, String newOwner,
      PrincipalType newOwnerType, TDdlExecResponse response) throws ImpalaException {
    verifySentryServiceEnabled();
    if (!sentryProxy_.isObjectOwnershipEnabled()) return;

    Preconditions.checkNotNull(serverName);
    Preconditions.checkNotNull(databaseName);
    TPrivilege filter = createDatabaseOwnerPrivilegeFilter(databaseName, serverName);
    updateOwnerPrivilege(oldOwner, oldOwnerType, newOwner, newOwnerType, response,
        filter);
  }

  /**
   * Update the owner privileges for an object.
   * If object ownership is enabled in Sentry, we need to update the owner privilege
   * in the catalog so that any subsequent statements that rely on that privilege, or
   * the absence, will function correctly without waiting for the next refresh.
   * If oldOwner is not null, the privilege will be removed. If newOwner is not null,
   * the privilege will be added.
   * The catalog will correctly reflect the owner in HMS, however because the owner
   * privileges are created by HMS in Sentry, Impala does not have visibility on
   * whether or not that create was successful. If Sentry failed to properly update the
   * owner privilege, Impala will have a different view of privileges until the next
   * Sentry refresh.
   * e.g. For create, the privileges should be available to immediately create a table.
   * Additionally, if the metadata operation is successful, but sentry fails to add
   * the privilege, it will be removed on the next refresh. ALTER DATABASE SET OWNER
   * can be used to try adding the owner privilege again.
   * This method should be called from within a DDLLock or table lock (in the case of
   * alter table statements.) to ensure that the privileges are in sync with the metadata
   * operations.
   */
  @Override
  public void updateTableOwnerPrivilege(String serverName, String databaseName,
      String tableName, String oldOwner, PrincipalType oldOwnerType, String newOwner,
      PrincipalType newOwnerType, TDdlExecResponse response) throws ImpalaException {
    verifySentryServiceEnabled();
    if (!sentryProxy_.isObjectOwnershipEnabled()) return;

    Preconditions.checkNotNull(serverName);
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    TPrivilege filter = createTableOwnerPrivilegeFilter(databaseName, tableName,
        serverName);
    updateOwnerPrivilege(oldOwner, oldOwnerType, newOwner, newOwnerType, response,
        filter);
  }

  @Override
  public AuthorizationDelta refreshAuthorization(boolean resetVersions)
      throws ImpalaException {
    List<TCatalogObject> catalogObjectsAdded = new ArrayList<>();
    List<TCatalogObject> catalogObjectsRemoved = new ArrayList<>();
    sentryProxy_.refresh(resetVersions, catalogObjectsAdded, catalogObjectsRemoved);
    return new AuthorizationDelta(catalogObjectsAdded, catalogObjectsRemoved);
  }

  private void updateOwnerPrivilege(String oldOwner, PrincipalType oldOwnerType,
      String newOwner, PrincipalType newOwnerType, TDdlExecResponse response,
      TPrivilege filter) {
    if (oldOwner != null && !oldOwner.isEmpty()) {
      removePrivilegeFromCatalog(oldOwner, oldOwnerType, filter, response);
    }
    if (newOwner != null && !newOwner.isEmpty()) {
      addPrivilegeToCatalog(newOwner, newOwnerType, filter, response);
    }
  }

  /**
   * Throws a CatalogException if the Sentry Service is not enabled.
   */
  private void verifySentryServiceEnabled() throws CatalogException {
    if (sentryProxy_ == null) {
      throw new CatalogException("Sentry Service is not enabled on the " +
          "CatalogServer.");
    }
  }

  /**
   * Checks if with grant is enabled for object ownership in Sentry.
   */
  private boolean isObjectOwnershipGrantEnabled() throws ImpalaException {
    return sentryProxy_ == null ? false : sentryProxy_.isObjectOwnershipGrantEnabled();
  }

  /**
   * Create a TPrivilege for an owner of a table for use as a filter.
   */
  private TPrivilege createTableOwnerPrivilegeFilter(String databaseName,
      String tableName, String serverName) throws ImpalaException {
    TPrivilege privilege = createDatabaseOwnerPrivilegeFilter(databaseName, serverName);
    privilege.setScope(TPrivilegeScope.TABLE);
    privilege.setTable_name(tableName);
    return privilege;
  }

  /**
   * Create a TPrivilege for an owner of a database for use as a filter.
   */
  private TPrivilege createDatabaseOwnerPrivilegeFilter(String databaseName,
      String serverName) throws ImpalaException {
    TPrivilege privilege = new TPrivilege();
    privilege.setScope(TPrivilegeScope.DATABASE).setServer_name(serverName)
        .setPrivilege_level(TPrivilegeLevel.OWNER)
        .setDb_name(databaseName).setCreate_time_ms(-1)
        .setHas_grant_opt(isObjectOwnershipGrantEnabled());
    return privilege;
  }

  /**
   * This is a helper method to take care of catalog related updates when removing
   * a privilege.
   */
  private void removePrivilegeFromCatalog(String ownerString, PrincipalType ownerType,
      TPrivilege filter, TDdlExecResponse response) {
    Preconditions.checkNotNull(ownerString);
    Preconditions.checkNotNull(ownerType);
    Preconditions.checkNotNull(filter);
    try {
      PrincipalPrivilege removedPrivilege = null;
      switch (ownerType) {
        case ROLE:
          removedPrivilege = catalog_.removeRolePrivilege(ownerString,
              PrincipalPrivilege.buildPrivilegeName(filter));
          break;
        case USER:
          removedPrivilege = catalog_.removeUserPrivilege(ownerString,
              PrincipalPrivilege.buildPrivilegeName(filter));
          break;
        default:
          Preconditions.checkArgument(false,
              "Unexpected PrincipalType: " + ownerType.name());
      }
      if (removedPrivilege != null) {
        response.result.addToRemoved_catalog_objects(removedPrivilege
            .toTCatalogObject());
      }
    } catch (CatalogException e) {
      // Failure removing an owner privilege is not an issue because it could be
      // that Sentry refresh occurred while executing this method and this method
      // is used as a a best-effort to do what Sentry refresh does to make the
      // owner privilege available right away without having to wait for a Sentry
      // refresh.
      LOG.warn("Unable to remove owner privilege: " +
          PrincipalPrivilege.buildPrivilegeName(filter), e);
    }
  }

  /**
   * This is a helper method to take care of catalog related updates when adding
   * a privilege. This will also add a user to the catalog if it doesn't exist.
   */
  private void addPrivilegeToCatalog(String ownerString, PrincipalType ownerType,
      TPrivilege filter, TDdlExecResponse response) {
    Preconditions.checkNotNull(ownerString);
    Preconditions.checkNotNull(ownerType);
    Preconditions.checkNotNull(filter);
    try {
      Principal owner;
      PrincipalPrivilege cPrivilege = null;
      if (ownerType == PrincipalType.USER) {
        Reference<Boolean> existingUser = new Reference<>();
        owner = catalog_.addUserIfNotExists(ownerString, existingUser);
        filter.setPrincipal_id(owner.getId());
        filter.setPrincipal_type(TPrincipalType.USER);
        cPrivilege = catalog_.addUserPrivilege(ownerString, filter);
        if (!existingUser.getRef()) {
          response.result.addToUpdated_catalog_objects(owner.toTCatalogObject());
        }
      } else if (ownerType == PrincipalType.ROLE) {
        owner = catalog_.getAuthPolicy().getRole(ownerString);
        Preconditions.checkNotNull(owner);
        filter.setPrincipal_id(owner.getId());
        filter.setPrincipal_type(TPrincipalType.ROLE);
        cPrivilege = catalog_.addRolePrivilege(ownerString, filter);
      } else {
        Preconditions.checkArgument(false, "Unexpected PrincipalType: " +
            ownerType.name());
      }
      response.result.addToUpdated_catalog_objects(cPrivilege.toTCatalogObject());
    } catch (CatalogException e) {
      // Failure adding an owner privilege is not an issue because it could be
      // that Sentry refresh occurred while executing this method and this method
      // is used as a a best-effort to do what Sentry refresh does to make the
      // owner privilege available right away without having to wait for a Sentry
      // refresh.
      LOG.warn("Unable to add owner privilege: " +
          PrincipalPrivilege.buildPrivilegeName(filter), e);
    }
  }

  /**
   * Returns the version from the last item in the list.  This assumes that the items
   * are added in version order.
   */
  private long getLastItemVersion(List<TCatalogObject> items) {
    Preconditions.checkState(items != null && !items.isEmpty());
    return items.get(items.size() - 1).getCatalog_version();
  }
}
