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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.catalog.Principal;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.sentry.api.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.api.service.thrift.TSentryGrantOption;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.api.service.thrift.TSentryRole;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.analysis.PrivilegeSpec;
import org.apache.impala.authorization.User;
import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 *  Wrapper around the SentryService APIs that are used by Impala and Impala tests.
 */
public class SentryPolicyService {
  private final static Logger LOG = LoggerFactory.getLogger(SentryPolicyService.class);
  private final String ACCESS_DENIED_ERROR_MSG =
      "User '%s' does not have privileges to execute: %s";
  private final SentryConfig config_;

  /**
   * Wrapper around a SentryPolicyServiceClient.
   * TODO: When SENTRY-296 is resolved we can more easily cache connections instead of
   * opening a new connection for each request.
   */
  class SentryServiceClient implements AutoCloseable {
    private final SentryPolicyServiceClient client_;

    /**
     * Creates and opens a new Sentry Service thrift client.
     */
    public SentryServiceClient() throws InternalException {
      client_ = createClient();
    }

    /**
     * Get the underlying SentryPolicyServiceClient.
     */
    public SentryPolicyServiceClient get() {
      return client_;
    }

    /**
     * Returns this client back to the connection pool. Can be called multiple times.
     */
    public void close() throws InternalException {
      try {
        client_.close();
      } catch (Exception e) {
        throw new InternalException("Error closing client: ", e);
      }
    }

    /**
     * Creates a new client to the SentryService.
     */
    private SentryPolicyServiceClient createClient() throws InternalException {
      SentryPolicyServiceClient client;
      try {
        client = SentryServiceClientFactory.create(config_.getConfig());
      } catch (Exception e) {
        throw new InternalException("Error creating Sentry Service client: ", e);
      }
      return client;
    }
  }

  public SentryPolicyService(SentryConfig config) {
    config_ = config;
  }

  /**
   * Drops a role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to drop.
   * @param ifExists - If true, no error is thrown if the role does not exist.
   * @throws ImpalaException - On any error dropping the role.
   */
  public void dropRole(User requestingUser, String roleName, boolean ifExists)
      throws ImpalaException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Dropping role: %s on behalf of: %s", roleName,
          requestingUser.getName()));
    }
    SentryServiceClient client = new SentryServiceClient();
    try {
      if (ifExists) {
        client.get().dropRoleIfExists(requestingUser.getShortName(), roleName);
      } else {
        client.get().dropRole(requestingUser.getShortName(), roleName);
      }
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "DROP_ROLE"));
      }
      throw new InternalException("Error dropping role: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Creates a new role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to create.
   * @param ifNotExists - If true, no error is thrown if the role already exists.
   * @throws ImpalaException - On any error creating the role.
   */
  public void createRole(User requestingUser, String roleName, boolean ifNotExists)
      throws ImpalaException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Creating role: %s on behalf of: %s", roleName,
          requestingUser.getName()));
    }
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().createRole(requestingUser.getShortName(), roleName);
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
              requestingUser.getName(), "CREATE_ROLE"));
      }
      if (SentryUtil.isSentryAlreadyExists(e)) {
        if (ifNotExists) return;
        throw new InternalException("Error creating role: " + e.getMessage(), e);
      }
      throw new InternalException("Error creating role: " + e.getMessage(), e);
    } finally {
      client.close();
    }
  }

  /**
   * Grants a role to a group.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to grant to a group. Role must already exist.
   * @param groupName - The group to grant the role to.
   * @throws ImpalaException - On any error.
   */
  public void grantRoleToGroup(User requestingUser, String roleName, String groupName)
      throws ImpalaException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Granting role '%s' to group '%s' on behalf of: %s",
          roleName, groupName, requestingUser.getName()));
    }
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().grantRoleToGroup(requestingUser.getShortName(), groupName, roleName);
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
              requestingUser.getName(), "GRANT_ROLE"));
      }
      throw new InternalException(
          "Error making 'grantRoleToGroup' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Grants a role to the groups.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to grant to the groups. Role must already exist.
   * @param groupNames - The groups to grant the role to.
   * @throws ImpalaException - On any error.
   */
  public void grantRoleToGroups(User requestingUser, String roleName,
      Set<String> groupNames) throws ImpalaException {
    for (String groupName : groupNames) {
      grantRoleToGroup(requestingUser, roleName, groupName);
    }
  }

  /**
   * Removes a role from a group.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role name to remove.
   * @param groupName - The group to remove the role from.
   * @throws InternalException - On any error.
   */
  public void revokeRoleFromGroup(User requestingUser, String roleName, String groupName)
      throws ImpalaException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Revoking role '%s' from group '%s' on behalf of: %s",
          roleName, groupName, requestingUser.getName()));
    }
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().revokeRoleFromGroup(requestingUser.getShortName(),
          groupName, roleName);
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
              requestingUser.getName(), "REVOKE_ROLE"));
      }
      throw new InternalException(
          "Error making 'revokeRoleFromGroup' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Grants a privilege to an existing role.
   */
  public void grantRolePrivilege(User requestingUser, String roleName,
      TPrivilege privilege) throws ImpalaException {
    grantRolePrivileges(requestingUser, roleName, Lists.newArrayList(privilege));
  }

  /**
   * Grants privileges to an existing role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to grant privileges to (case insensitive).
   * @param privileges - The privileges to grant.
   * @throws ImpalaException - On any error
   */
  public void grantRolePrivileges(User requestingUser, String roleName,
      List<TPrivilege> privileges) throws ImpalaException {
    Preconditions.checkState(!privileges.isEmpty());
    TPrivilege privilege = privileges.get(0);
    TPrivilegeScope scope = privilege.getScope();
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format(
          "Granting role '%s' '%s' privilege on '%s' on behalf of: %s",
          roleName, privilege.getPrivilege_level().toString(), scope.toString(),
          requestingUser.getName()));
    }
    // Verify that all privileges have the same scope.
    for (int i = 1; i < privileges.size(); ++i) {
      Preconditions.checkState(privileges.get(i).getScope() == scope, "All the " +
          "privileges must have the same scope.");
    }
    Preconditions.checkState(scope == TPrivilegeScope.COLUMN || privileges.size() == 1,
        "Cannot grant multiple " + scope + " privileges with a singe RPC to the " +
        "Sentry Service.");
    SentryServiceClient client = new SentryServiceClient();
    try {
      switch (scope) {
        case SERVER:
          client.get().grantServerPrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getPrivilege_level().toString(),
              privilege.isHas_grant_opt());
          break;
        case DATABASE:
          client.get().grantDatabasePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getPrivilege_level().toString(),
              privilege.isHas_grant_opt());
          break;
        case TABLE:
          client.get().grantTablePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getTable_name(), privilege.getPrivilege_level().toString(),
              privilege.isHas_grant_opt());
          break;
        case COLUMN:
          client.get().grantColumnsPrivileges(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getTable_name(), getColumnNames(privileges),
              privilege.getPrivilege_level().toString(), privilege.isHas_grant_opt());
          break;
        case URI:
          client.get().grantURIPrivilege(requestingUser.getShortName(),
              roleName, privilege.getServer_name(), privilege.getUri(),
              privilege.isHas_grant_opt());
          break;
      }
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
            requestingUser.getName(), "GRANT_PRIVILEGE"));
      }
      throw new InternalException(
          "Error making 'grantPrivilege*' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Revokes privileges from an existing role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to revoke privileges from (case insensitive).
   * @param privileges - The privileges to revoke.
   * @throws ImpalaException - On any error
   */
  public void revokeRolePrivileges(User requestingUser, String roleName,
      List<TPrivilege> privileges) throws ImpalaException {
    Preconditions.checkState(!privileges.isEmpty());
    TPrivilege privilege = privileges.get(0);
    TPrivilegeScope scope = privilege.getScope();
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Revoking from role '%s' '%s' privilege on '%s' on " +
          "behalf of: %s", roleName, privilege.getPrivilege_level().toString(),
          scope.toString(), requestingUser.getName()));
    }
    // Verify that all privileges have the same scope.
    for (int i = 1; i < privileges.size(); ++i) {
      Preconditions.checkState(privileges.get(i).getScope() == scope, "All the " +
          "privileges must have the same scope.");
    }
    Preconditions.checkState(scope == TPrivilegeScope.COLUMN || privileges.size() == 1,
        "Cannot revoke multiple " + scope + " privileges with a singe RPC to the " +
        "Sentry Service.");
    SentryServiceClient client = new SentryServiceClient();
    try {
      switch (scope) {
        case SERVER:
          client.get().revokeServerPrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getPrivilege_level().toString(),
              /* grant option */ null);
          break;
        case DATABASE:
          client.get().revokeDatabasePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getPrivilege_level().toString(), null);
          break;
        case TABLE:
          client.get().revokeTablePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getTable_name(), privilege.getPrivilege_level().toString(),
              null);
          break;
        case COLUMN:
          client.get().revokeColumnsPrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getTable_name(), getColumnNames(privileges),
              privilege.getPrivilege_level().toString(), null);
          break;
        case URI:
          client.get().revokeURIPrivilege(requestingUser.getShortName(),
              roleName, privilege.getServer_name(), privilege.getUri(),
              null);
          break;
      }
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
            requestingUser.getName(), "REVOKE_PRIVILEGE"));
      }
      throw new InternalException(
          "Error making 'revokePrivilege*' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Returns the column names referenced in a list of column-level privileges.
   * Verifies that all column-level privileges refer to the same table.
   */
  private List<String> getColumnNames(List<TPrivilege> privileges) {
    List<String> columnNames = Lists.newArrayList();
    String tablePath = PrivilegeSpec.getTablePath(privileges.get(0));
    columnNames.add(privileges.get(0).getColumn_name());
    // Collect all column names and verify that they belong to the same table.
    for (int i = 1; i < privileges.size(); ++i) {
      TPrivilege privilege = privileges.get(i);
      Preconditions.checkState(tablePath.equals(PrivilegeSpec.getTablePath(privilege))
          && privilege.getScope() == TPrivilegeScope.COLUMN);
      columnNames.add(privileges.get(i).getColumn_name());
    }
    return columnNames;
  }

  /**
   * Lists all roles.
   */
  public List<TSentryRole> listAllRoles(User requestingUser) throws ImpalaException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listAllRoles(requestingUser.getShortName()));
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
            requestingUser.getName(), "LIST_ROLES"));
      }
      throw new InternalException("Error making 'listRoles' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Lists all privileges granted to a role.
   */
  public List<TSentryPrivilege> listRolePrivileges(User requestingUser, String roleName)
      throws ImpalaException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listAllPrivilegesByRoleName(
          requestingUser.getShortName(), roleName));
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
            requestingUser.getName(), "LIST_ROLE_PRIVILEGES"));
      }
      throw new InternalException("Error making 'listAllPrivilegesByRoleName' RPC to " +
          "Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Returns a map of all roles with their associated privileges.
   */
  public Map<String, Set<TSentryPrivilege>> listAllRolesPrivileges(User requestingUser)
      throws ImpalaException {
    return listAllPrincipalsPrivileges(requestingUser, TPrincipalType.ROLE);
  }

  /**
   * Returns a map of all users with their associated privileges.
   */
  public Map<String, Set<TSentryPrivilege>> listAllUsersPrivileges(User requestingUser)
      throws ImpalaException {
    return listAllPrincipalsPrivileges(requestingUser, TPrincipalType.USER);
  }

  private Map<String, Set<TSentryPrivilege>> listAllPrincipalsPrivileges(
      User requestingUser, TPrincipalType type) throws ImpalaException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return type == TPrincipalType.ROLE ?
          client.get().listAllRolesPrivileges(requestingUser.getShortName()) :
          client.get().listAllUsersPrivileges(requestingUser.getShortName());
    } catch (Exception e) {
      if (SentryUtil.isSentryAccessDenied(e)) {
        throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
            requestingUser.getName(),
            type == TPrincipalType.ROLE ?
                "LIST_ALL_ROLES_PRIVILEGES" : "LIST_ALL_USERS_PRIVILEGES"));
      }
      throw new InternalException(String.format("Error making '%s' RPC to " +
          "Sentry Service: ",
          type == TPrincipalType.ROLE ?
              "listAllRolesPrivileges" :
              "listAllUsersPrivileges"), e);
    } finally {
      client.close();
    }
  }

  /**
   * Returns the configuration value for the specified key.  Will return an empty string
   * if no value is set.
   */
  public String getConfigValue(String key) throws ImpalaException {
    try (SentryServiceClient client = new SentryServiceClient()) {
      return client.get().getConfigValue(key, "");
    } catch (SentryUserException e) {
      throw new InternalException("Error making 'getConfigValue' RPC to Sentry Service: ",
          e);
    }
  }

  /**
   * Utility function that converts a TSentryPrivilege to an Impala TPrivilege object.
   */
  public static TPrivilege sentryPrivilegeToTPrivilege(TSentryPrivilege sentryPriv,
      Principal principal) {
    TPrivilege privilege = new TPrivilege();
    privilege.setServer_name(sentryPriv.getServerName());
    if (sentryPriv.isSetDbName()) privilege.setDb_name(sentryPriv.getDbName());
    if (sentryPriv.isSetTableName()) privilege.setTable_name(sentryPriv.getTableName());
    if (sentryPriv.isSetColumnName()) {
      privilege.setColumn_name(sentryPriv.getColumnName());
    }
    if (sentryPriv.isSetURI()) privilege.setUri(sentryPriv.getURI());
    privilege.setScope(Enum.valueOf(TPrivilegeScope.class,
        sentryPriv.getPrivilegeScope().toUpperCase()));
    if (sentryPriv.getAction().equals("*")) {
      privilege.setPrivilege_level(TPrivilegeLevel.ALL);
    } else {
      privilege.setPrivilege_level(Enum.valueOf(TPrivilegeLevel.class,
          sentryPriv.getAction().toUpperCase()));
    }
    privilege.setCreate_time_ms(sentryPriv.getCreateTime());
    if (sentryPriv.isSetGrantOption() &&
        sentryPriv.getGrantOption() == TSentryGrantOption.TRUE) {
      privilege.setHas_grant_opt(true);
    } else {
      privilege.setHas_grant_opt(false);
    }
    privilege.setPrincipal_id(principal.getId());
    privilege.setPrincipal_type(principal.getPrincipalType());
    return privilege;
  }

  /**
   * Checks if the given user is a Sentry admin.
   */
  public boolean isSentryAdmin(User user)
      throws InternalException, SentryUserException {
    try (SentryServiceClient client = new SentryServiceClient()) {
      return client.get().isAdmin(user.getName());
    }
  }
}
