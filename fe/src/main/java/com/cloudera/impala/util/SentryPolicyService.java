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

import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.PrivilegeSpec;
import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.RolePrivilege;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TPrivilegeLevel;
import com.cloudera.impala.thrift.TPrivilegeScope;
import com.google.common.base.Joiner;
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
  class SentryServiceClient {
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
    public void close() {
      client_.close();
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
    LOG.trace(String.format("Dropping role: %s on behalf of: %s", roleName,
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      if (ifExists) {
        client.get().dropRoleIfExists(requestingUser.getShortName(), roleName);
      } else {
        client.get().dropRole(requestingUser.getShortName(), roleName);
      }
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "DROP_ROLE"));
    } catch (SentryUserException e) {
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
    LOG.trace(String.format("Creating role: %s on behalf of: %s", roleName,
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().createRole(requestingUser.getShortName(), roleName);
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "CREATE_ROLE"));
    } catch (SentryAlreadyExistsException e) {
      if (ifNotExists) return;
      throw new InternalException("Error creating role: ", e);
    } catch (SentryUserException e) {
      throw new InternalException("Error creating role: ", e);
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
    LOG.trace(String.format("Granting role '%s' to group '%s' on behalf of: %s",
        roleName, groupName, requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().grantRoleToGroup(requestingUser.getShortName(), groupName, roleName);
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "GRANT_ROLE"));
    } catch (SentryUserException e) {
      throw new InternalException(
          "Error making 'grantRoleToGroup' RPC to Sentry Service: ", e);
    } finally {
      client.close();
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
    LOG.trace(String.format("Revoking role '%s' from group '%s' on behalf of: %s",
        roleName, groupName, requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().revokeRoleFromGroup(requestingUser.getShortName(),
          groupName, roleName);
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "REVOKE_ROLE"));
    } catch (SentryUserException e) {
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
   * @param privilege - The privilege to grant.
   * @throws ImpalaException - On any error
   */
  public void grantRolePrivileges(User requestingUser, String roleName,
      List<TPrivilege> privileges) throws ImpalaException {
    Preconditions.checkState(!privileges.isEmpty());
    TPrivilege privilege = privileges.get(0);
    TPrivilegeScope scope = privilege.getScope();
    LOG.trace(String.format("Granting role '%s' '%s' privilege on '%s' on behalf of: %s",
        roleName, privilege.getPrivilege_level().toString(), scope.toString(),
        requestingUser.getName()));
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
              privilege.getServer_name(), privilege.isHas_grant_opt());
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
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "GRANT_PRIVILEGE"));
    } catch (SentryUserException e) {
      throw new InternalException(
          "Error making 'grantPrivilege*' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Revokes a privilege from an existing role.
   */
  public void revokeRolePrivilege(User requestingUser, String roleName,
      TPrivilege privilege) throws ImpalaException {
    revokeRolePrivileges(requestingUser, roleName, Lists.newArrayList(privilege));
  }

  /**
   * Revokes privileges from an existing role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to revoke privileges from (case insensitive).
   * @param privilege - The privilege to revoke.
   * @throws ImpalaException - On any error
   */
  public void revokeRolePrivileges(User requestingUser, String roleName,
      List<TPrivilege> privileges) throws ImpalaException {
    Preconditions.checkState(!privileges.isEmpty());
    TPrivilege privilege = privileges.get(0);
    TPrivilegeScope scope = privilege.getScope();
    LOG.trace(String.format("Revoking from role '%s' '%s' privilege on '%s' on " +
        "behalf of: %s", roleName, privilege.getPrivilege_level().toString(),
        scope.toString(), requestingUser.getName()));
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
              privilege.getServer_name(), privilege.getPrivilege_level().toString());
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
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "REVOKE_PRIVILEGE"));
    } catch (SentryUserException e) {
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
   * Lists all roles granted to all groups a user belongs to.
   */
  public List<TSentryRole> listUserRoles(User requestingUser)
      throws ImpalaException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listUserRoles(
          requestingUser.getShortName()));
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "LIST_USER_ROLES"));
    } catch (SentryUserException e) {
      throw new InternalException(
          "Error making 'listUserRoles' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Lists all roles.
   */
  public List<TSentryRole> listAllRoles(User requestingUser) throws ImpalaException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listRoles(requestingUser.getShortName()));
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "LIST_ROLES"));
    } catch (SentryUserException e) {
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
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "LIST_ROLE_PRIVILEGES"));
    } catch (SentryUserException e) {
      throw new InternalException("Error making 'listAllPrivilegesByRoleName' RPC to " +
          "Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Utility function that converts a TSentryPrivilege to an Impala TPrivilege object.
   */
  public static TPrivilege sentryPrivilegeToTPrivilege(TSentryPrivilege sentryPriv) {
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
    privilege.setPrivilege_name(RolePrivilege.buildRolePrivilegeName(privilege));
    privilege.setCreate_time_ms(sentryPriv.getCreateTime());
    if (sentryPriv.isSetGrantOption() &&
        sentryPriv.getGrantOption() == TSentryGrantOption.TRUE) {
      privilege.setHas_grant_opt(true);
    } else {
      privilege.setHas_grant_opt(false);
    }
    return privilege;
  }
}
