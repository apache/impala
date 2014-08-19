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

import java.io.IOException;
import java.util.List;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.RolePrivilege;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TPrivilegeLevel;
import com.cloudera.impala.thrift.TPrivilegeScope;
import com.google.common.collect.Lists;

/**
 *  Wrapper around the SentryService APIs that are used by Impala and Impala tests.
 */
public class SentryPolicyService {
  private final static Logger LOG = LoggerFactory.getLogger(SentryPolicyService.class);
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
        client = new SentryPolicyServiceClient(config_.getConfig());
      } catch (IOException e) {
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
   * @throws InternalException - On any error dropping the role.
   */
  public void dropRole(User requestingUser, String roleName, boolean ifExists)
      throws InternalException {
    LOG.trace(String.format("Dropping role: %s on behalf of: %s", roleName,
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      if (ifExists) {
        client.get().dropRoleIfExists(requestingUser.getShortName(), roleName);
      } else {
        client.get().dropRole(requestingUser.getShortName(), roleName);
      }
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
   * @throws InternalException - On any error creating the role.
   */
  public void createRole(User requestingUser, String roleName, boolean ifNotExists)
      throws InternalException {
    LOG.trace(String.format("Creating role: %s on behalf of: %s", roleName,
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().createRole(requestingUser.getShortName(), roleName);
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
   * @throws InternalException - On any error.
   */
  public void grantRoleToGroup(User requestingUser, String roleName, String groupName)
      throws InternalException {
    LOG.trace(String.format("Granting role '%s' to group '%s' on behalf of: %s",
        roleName, groupName, requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().grantRoleToGroup(requestingUser.getShortName(), groupName, roleName);
    } catch (SentryUserException e) {
      throw new InternalException("Error granting role to group: ", e);
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
      throws InternalException {
    LOG.trace(String.format("Revoking role '%s' from group '%s' on behalf of: %s",
        roleName, groupName, requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().revokeRoleFromGroup(requestingUser.getShortName(),
          groupName, roleName);
    } catch (SentryUserException e) {
      throw new InternalException("Error revoking role from group: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Grants privileges to an existing role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to grant privileges to (case insensitive).
   * @param privilege - The privilege to grant.
   * @throws InternalException - On any error
   */
  public void grantRolePrivilege(User requestingUser, String roleName,
      TPrivilege privilege) throws InternalException {
    LOG.trace(String.format("Granting role '%s' privilege '%s' on '%s' on behalf of: %s",
        roleName, privilege.toString(), privilege.getScope().toString(),
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      switch (privilege.getScope()) {
        case SERVER:
          client.get().grantServerPrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name());
          break;
        case DATABASE:
          client.get().grantDatabasePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getPrivilege_level().toString());
          break;
        case TABLE:
          String tblName = privilege.getTable_name();
          String dbName = privilege.getDb_name();
          client.get().grantTablePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(),
              dbName, tblName, privilege.getPrivilege_level().toString());
          break;
        case URI:
          client.get().grantURIPrivilege(requestingUser.getShortName(),
              roleName, privilege.getServer_name(), privilege.getUri());
          break;
      }
    } catch (SentryUserException e) {
      throw new InternalException("Error granting privilege: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Revokes privileges from an existing role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to grant privileges to (case insensitive).
   * @param privilege - The privilege to grant to the object.
   * @throws InternalException - On any error
   */
  public void revokeRolePrivilege(User requestingUser, String roleName,
      TPrivilege privilege) throws InternalException {
    LOG.trace(String.format("Revoking role '%s' privilege '%s' on '%s' on behalf of: %s",
        roleName, privilege.toString(), privilege.getScope().toString(),
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      switch (privilege.getScope()) {
        case SERVER:
          client.get().revokeServerPrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name());
          break;
        case DATABASE:
          client.get().revokeDatabasePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getPrivilege_level().toString());
          break;
        case TABLE:
          String tblName = privilege.getTable_name();
          String dbName = privilege.getDb_name();
          client.get().revokeTablePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), dbName, tblName,
              privilege.getPrivilege_level().toString());
          break;
        case URI:
          client.get().revokeURIPrivilege(requestingUser.getShortName(),
              roleName, privilege.getServer_name(), privilege.getUri());
          break;
      }
    } catch (SentryUserException e) {
      throw new InternalException("Error revoking privilege: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Lists all roles.
   */
  public List<TSentryRole> listAllRoles(User requestingUser) throws InternalException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listRoles(requestingUser.getShortName()));
    } catch (SentryUserException e) {
      throw new InternalException("Error listing roles: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Lists all privileges granted to a role.
   */
  public List<TSentryPrivilege> listRolePrivileges(User requestingUser, String roleName)
      throws InternalException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listAllPrivilegesByRoleName(
          requestingUser.getShortName(), roleName));
    } catch (SentryUserException e) {
      throw new InternalException("Error listing privileges by role name: ", e);
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
    return privilege;
  }
}
