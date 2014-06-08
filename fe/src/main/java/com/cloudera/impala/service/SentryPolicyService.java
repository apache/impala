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

package com.cloudera.impala.service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.Authorizeable;
import com.cloudera.impala.authorization.AuthorizeableDb;
import com.cloudera.impala.authorization.AuthorizeableTable;
import com.cloudera.impala.authorization.AuthorizeableUri;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 *  Wrapper around the SentryService APIs that are used by Impala and Impala tests.
 *  NOTE: Currently the RPCs require passing user/group information. This will be
 *  fixed in SENTRY-191, but for now we pass bogus values.
 */
public class SentryPolicyService {
  private final static Logger LOG = LoggerFactory.getLogger(SentryPolicyService.class);

  // Queue of clients.
  private final LinkedBlockingQueue<SentryServiceClient> clientPool_ =
      new LinkedBlockingQueue<SentryServiceClient>();

  private final SentryConfig config_;
  private final String serverName_;
  private final User user_ = new User(System.getProperty("user.name"));

  /**
   * Wrapper around a SentryPolicyServiceClient. Handles creating clients and releasing
   * them back to the clientPool_.
   */
  class SentryServiceClient {
    private final SentryPolicyServiceClient client_;

    /**
     * Creates a new Sentry Service thrift client.
     */
    public SentryServiceClient() throws InternalException { client_ = createClient(); }

    /**
     * Get the underlying SentryPolicyServiceClient.
     */
    public SentryPolicyServiceClient get() { return client_; }

    /**
     * Returns this client back to the connection pool.
     */
    public void release() { clientPool_.add(this); }

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

  public SentryPolicyService(SentryConfig config, String serverName) {
    config_ = config;
    serverName_ = serverName;
  }

  /**
   * Drops a role. Currently only used by authorization tests.
   *
   * @param roleName - The role to drop.
   * @param ifExists - If true, no error is thrown if the role does not exist.
   * @throws InternalException - On any error dropping the role.
   */
  public void dropRole(String roleName, boolean ifExists) throws InternalException {
    LOG.trace("Dropping role: " + roleName);
    SentryServiceClient client = getClient();
    try {
      if (ifExists) {
        client.get().dropRoleIfExists(user_.getName(), roleName);
      } else {
        client.get().dropRole(user_.getName(), roleName);
      }
    } catch (SentryUserException e) {
      throw new InternalException("Error dropping role: ", e);
    } finally {
      client.release();
    }
  }

  /**
   * Creates a new role. Currently only used by authorization tests.
   *
   * @param roleName - The role to create.
   * @param ifNotExists - If true, no error is thrown if the role already exists.
   * @throws InternalException - On any error creating the role.
   */
  public void createRole(String roleName, boolean ifNotExists)
      throws InternalException {
    LOG.trace("Creating role: " + roleName);
    SentryServiceClient client = getClient();
    try {
      client.get().createRole(user_.getName(), roleName);
    } catch (SentryAlreadyExistsException e) {
      if (ifNotExists) return;
      throw new InternalException("Error creating role: ", e);
    } catch (SentryUserException e) {
      throw new InternalException("Error creating role: ", e);
    } finally {
      client.release();
    }
  }

  /**
   * Grants a role to a group. Currently only used by authorization tests.
   *
   * @param roleName - The role to grant to a group. Role must already exist.
   * @param groupName - The group to grant the role to.
   * @throws InternalException - On any error.
   */
  public void grantRoleToGroup(String roleName, String groupName)
      throws InternalException {
    LOG.trace(String.format("Granting role '%s' to group '%s'", roleName, groupName));

    SentryServiceClient client = getClient();
    try {
      client.get().grantRoleToGroup(user_.getName(), groupName, roleName);
    } catch (SentryUserException e) {
      throw new InternalException("Error granting role to group: ", e);
    } finally {
      client.release();
    }
  }

  /**
   * Grants privileges to an existing role. Currently only used by authorization tests.
   *
   * @param roleName - The role to grant privileges to (case insensitive).
   * @param authorizeable - The object to secure (Table, Database, Uri, etc...)
   * @param privilege - The privilege to grant to the object.
   * @throws InternalException - On any error
   */
  public void grantRolePrivilege(String roleName, Authorizeable authorizeable,
      Privilege privilege) throws InternalException {
    LOG.trace(String.format("Granting role '%s' privilege '%s' on '%s'", roleName,
        privilege.toString(), authorizeable.getName()));

    SentryServiceClient client = getClient();
    try {
      if (authorizeable instanceof AuthorizeableDb) {
        AuthorizeableDb db = (AuthorizeableDb) authorizeable;
        try {
          client.get().grantDatabasePrivilege(user_.getName(), roleName,
              serverName_, db.getName());
        } catch (SentryUserException e) {
          throw new InternalException("Error granting privilege: ", e);
        } catch (RuntimeException e) {
          if (e.getMessage().contains("duplicate key value")) return;
          throw e;
        }
      } else if (authorizeable instanceof AuthorizeableUri) {
        AuthorizeableUri uri = (AuthorizeableUri) authorizeable;
        try {
          client.get().grantURIPrivilege(user_.getName(),
              roleName, serverName_, uri.getName());
        } catch (SentryUserException e) {
          throw new InternalException("Error granting privilege: ", e);
        } catch (RuntimeException e) {
          if (e.getMessage().contains("duplicate key value")) return;
          throw e;
        }
      } else if (authorizeable instanceof AuthorizeableTable) {
        AuthorizeableTable tbl = (AuthorizeableTable) authorizeable;
        String tblName = tbl.getTblName();
        String dbName = tbl.getDbName();
        try {
          client.get().grantTablePrivilege(user_.getName(), roleName, serverName_,
              dbName, tblName, privilege.toString());
        } catch (SentryUserException e) {
          throw new InternalException("Error granting privilege: ", e);
        } catch (RuntimeException e) {
          // TODO: Fix this once Sentry has better error codes.
          if (e.getMessage().contains("duplicate key value")) return;
          throw e;
        }
      } else {
        Preconditions.checkState(false, "Unexpected Authorizeable type: %s",
            authorizeable.getClass().getName());
      }
    } finally {
      client.release();
    }
  }

  /**
   * Removes a roles from a group. Currently only used by authorization tests.
   *
   * @param roleName - The role name to remove.
   * @param groupName - The group to remove the role from.
   * @throws InternalException - On any error.
   */
  public void revokeRoleFromGroup(String roleName, String groupName)
      throws InternalException {
    LOG.trace(String.format("Revoking role '%s' from group '%s'", roleName, groupName));

    SentryServiceClient client = getClient();
    try {
      client.get().revokeRoleFromGroup(user_.getName(), groupName, roleName);
    } catch (SentryUserException e) {
      throw new InternalException("Error revoking role from group: ", e);
    } finally {
      client.release();
    }
  }

  /**
   * Lists all roles.
   */
  public List<TSentryRole> listAllRoles() throws InternalException {
    SentryServiceClient client = getClient();
    try {
      return Lists.newArrayList(client.get().listRoles(user_.getName()));
    } catch (SentryUserException e) {
      throw new InternalException("Error listing roles: ", e);
    } finally {
      client.release();
    }
  }

  /**
   * Lists all privileges granted to a role.
   */
  public List<TSentryPrivilege> listRolePrivileges(String roleName)
      throws InternalException {
    SentryServiceClient client = getClient();
    try {
      return Lists.newArrayList(client.get().listAllPrivilegesByRoleName(user_.getName(),
          roleName));
    } catch (SentryUserException e) {
      throw new InternalException("Error listing privileges by role name: ", e);
    } finally {
      client.release();
    }
  }

  /**
   * Gets a client from the pool. If the pool is empty a new client is created.
   * Throws an "InternalException" if there is an error creating the client.
   */
  private SentryServiceClient getClient() throws InternalException {
    SentryServiceClient client = clientPool_.poll();
    // The pool was empty, create a new client.
    if (client == null) client = new SentryServiceClient();
    return client;
  }
}
