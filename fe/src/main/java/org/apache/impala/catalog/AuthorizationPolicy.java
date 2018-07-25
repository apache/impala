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

package org.apache.impala.catalog;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.log4j.Logger;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.provider.cache.PrivilegeCache;

import org.apache.impala.util.TResultRowBuilder;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A thread safe authorization policy cache, consisting of roles, groups that are
 * members of that role, the privileges associated with the role, and users and the
 * privileges associated with the user. The source data this cache is backing is read from
 * the Sentry Policy Service. Writing to the cache will replace any matching items, but
 * will not write back to the Sentry Policy Service.
 * The roleCache_ contains all roles defined in Sentry whereas userCache_ only contains
 * users that have privileges defined in Sentry and does not represent all users in the
 * system. A principal type can have 0 or more privileges and principal types are stored
 * in a map of principal name to principal object. For example:
 * RoleName -> Role -> [PrincipalPriv1, ..., PrincipalPrivN]
 * UserName -> User -> [PrincipalPriv1, ..., PrincipalPrivN]
 * There is a separate cache for users since we cannot guarantee uniqueness between
 * user names and role names.
 * To ensure we can efficiently retrieve the roles that a user is a member of, a map
 * of user group name to role name is tracked as grantGroups_.
 * To reduce duplication of metadata, privileges are linked to roles/users using a
 * "principal ID" rather than embedding the principal name. When a privilege is added to
 * a principal type, we do a lookup to get the principal ID to probe the principalIds_
 * map.
 * Acts as the backing cache for the Sentry cached based provider (which is why
 * PrivilegeCache is implemented).
 * TODO: Instead of calling into Sentry to perform final authorization checks, we
 * should parse/validate the privileges in Impala.
 */
public class AuthorizationPolicy implements PrivilegeCache {
  private static final Logger LOG = Logger.getLogger(AuthorizationPolicy.class);

  // Need to keep separate caches of role names and user names since there is
  // no uniqueness guarantee across roles and users
  // Cache of role names (case-insensitive) to role objects.
  private final CatalogObjectCache<Role> roleCache_ = new CatalogObjectCache<>();

  // Cache of user names (case-insensitive) to user objects.
  private final CatalogObjectCache<User> userCache_ = new CatalogObjectCache<>();

  // Map of principal ID -> user/role name. Used to match privileges to users/roles.
  private final Map<Integer, String> principalIds_ = Maps.newHashMap();

  // Map of group name (case sensitive) to set of role names (case insensitive) that
  // have been granted to this group. Kept in sync with roleCache_. Provides efficient
  // lookups of Role by group name.
  Map<String, Set<String>> groupsToRoles_ = Maps.newHashMap();

  /**
   * Adds a new principal to the policy. If a principal with the same name already
   * exists and the principal ID's are different, it will be overwritten by the new
   * principal. If a principal exists and the principal IDs are the same, the privileges
   * from the old principal will be copied to the new principal.
   */
  public synchronized void addPrincipal(Principal principal) {
    Principal existingPrincipal = getPrincipal(principal.getName(),
        principal.getPrincipalType());
    // There is already a newer version of this principal in the catalog, ignore
    // just return.
    if (existingPrincipal != null &&
        existingPrincipal.getCatalogVersion() >= principal.getCatalogVersion()) return;

    // If there was an existing principal that was replaced we first need to remove it.
    if (existingPrincipal != null) {
      // Remove the principal. This will also clean up the grantGroup mappings.
      removePrincipal(existingPrincipal.getName(), existingPrincipal.getPrincipalType());
      CatalogObjectVersionSet.INSTANCE.removeAll(existingPrincipal.getPrivileges());
      if (existingPrincipal.getId() == principal.getId()) {
        // Copy the privileges from the existing principal.
        for (PrincipalPrivilege p: existingPrincipal.getPrivileges()) {
          principal.addPrivilege(p);
        }
      }
    }
    if (principal.getPrincipalType() == TPrincipalType.USER) {
      Preconditions.checkArgument(principal instanceof User);
      userCache_.add((User) principal);
    } else {
      Preconditions.checkArgument(principal instanceof Role);
      roleCache_.add((Role) principal);
    }

    // Add new grants
    for (String groupName: principal.getGrantGroups()) {
      Set<String> grantedRoles = groupsToRoles_.get(groupName);
      if (grantedRoles == null) {
        grantedRoles = Sets.newHashSet();
        groupsToRoles_.put(groupName, grantedRoles);
      }
      grantedRoles.add(principal.getName().toLowerCase());
    }

    // Add this principal to the principal ID mapping
    principalIds_.put(principal.getId(), principal.getName());
  }

  /**
   * Adds a new privilege to the policy mapping to the principal specified by the
   * principal ID in the privilege. Throws a CatalogException no principal with a
   * corresponding ID existing in the catalog.
   */
  public synchronized void addPrivilege(PrincipalPrivilege privilege)
      throws CatalogException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Adding privilege: " + privilege.getName() + " " +
          Principal.toString(privilege.getPrincipalType()).toLowerCase() +
          " ID: " + privilege.getPrincipalId());
    }
    Principal principal = getPrincipal(privilege.getPrincipalId(),
        privilege.getPrincipalType());
    if (principal == null) {
      throw new CatalogException(String.format("Error adding privilege: %s. %s ID " +
          "'%d' does not exist.", privilege.getName(),
          Principal.toString(privilege.getPrincipalType()), privilege.getPrincipalId()));
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Adding privilege: " + privilege.getName() + " to " +
          Principal.toString(privilege.getPrincipalType()).toLowerCase() + ": " +
          principal.getName() + " with ID: " + principal.getId());
    }
    principal.addPrivilege(privilege);
  }

  /**
   * Removes a privilege from the policy mapping to the role specified by the principal ID
   * in the privilege. Throws a CatalogException if no role with a corresponding ID exists
   * in the catalog. Returns null if no matching privilege is found in this principal.
   */
  public synchronized PrincipalPrivilege removePrivilege(PrincipalPrivilege privilege)
      throws CatalogException {
    Principal principal = getPrincipal(privilege.getPrincipalId(),
        privilege.getPrincipalType());
    if (principal == null) {
      throw new CatalogException(String.format("Error removing privilege: %s. %s ID " +
          "'%d' does not exist.", privilege.getName(),
          Principal.toString(privilege.getPrincipalType()), privilege.getPrincipalId()));
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removing privilege: " + privilege.getName() + " from " +
          Principal.toString(privilege.getPrincipalType()).toLowerCase() + ": " +
          principal.getName() + " with ID: " + principal.getId());
    }
    return principal.removePrivilege(privilege.getName());
  }

  /**
   * Returns all roles in the policy. Returns an empty list if no roles exist.
   */
  public synchronized List<Role> getAllRoles() {
    return roleCache_.getValues();
  }

  /**
   * Returns all users in the policy. Returns an empty list if no users exist.
   */
  public synchronized List<User> getAllUsers() {
    return userCache_.getValues();
  }

  /**
   * Returns all role names in the policy. Returns an empty set if no roles exist.
   */
  public synchronized Set<String> getAllRoleNames() {
    return Sets.newHashSet(roleCache_.keySet());
  }

  /**
   * Gets a role given a role name. Returns null if no role exist with this name.
   */
  public synchronized Role getRole(String roleName) {
    return roleCache_.get(roleName);
  }

  /**
   * Gets a role given a role ID. Returns null if no role exists with this ID.
   */
  public synchronized Role getRole(int roleId) {
    String roleName = principalIds_.get(roleId);
    if (roleName == null) return null;
    return roleCache_.get(roleName);
  }

  /**
   * Returns all user names in the policy. Returns an empty set if no users exist.
   */
  public synchronized Set<String> getAllUserNames() {
    return Sets.newHashSet(userCache_.keySet());
  }

  /**
   * Gets a user given a user name. Returns null if no user exist with this name.
   */
  public synchronized User getUser(String userName) {
    return userCache_.get(userName);
  }

  /**
   * Gets a user given a user ID. Returns null if no user exists with this ID.
   */
  public synchronized User getUser(int userId) {
    String userName = principalIds_.get(userId);
    if (userName == null) return null;
    return userCache_.get(userName);
  }


  /**
   * Gets a principal given a principal name and type. Returns null if no principal exists
   * with this name and type.
   */
  public synchronized Principal getPrincipal(String principalName, TPrincipalType type) {
    return type == TPrincipalType.ROLE ?
        roleCache_.get(principalName) : userCache_.get(principalName);
  }

  /**
   * Gets a principal given a principal ID and type. Returns null if no principal exists
   * with this ID and type.
   */
  public synchronized Principal getPrincipal(int principalId, TPrincipalType type) {
    String principalName = principalIds_.get(principalId);
    if (principalName == null) return null;
    return getPrincipal(principalName, type);
  }

  /**
   * Gets all roles granted to the specified group.
   */
  public synchronized List<Role> getGrantedRoles(String groupName) {
    List<Role> grantedRoles = Lists.newArrayList();
    Set<String> roleNames = groupsToRoles_.get(groupName);
    if (roleNames != null) {
      for (String roleName: roleNames) {
        // TODO: verify they actually exist.
        Principal role = roleCache_.get(roleName);
        if (role != null) grantedRoles.add(roleCache_.get(roleName));
      }
    }
    return grantedRoles;
  }

  /**
   * Removes a principal for a given principal name and type. Returns the removed
   * principal or null if no principal with this name and type existed.
   */
  public synchronized Principal removePrincipal(String principalName,
      TPrincipalType type) {
    return type == TPrincipalType.ROLE ?
        removeRole(principalName) : removeUser(principalName);
  }

  /**
   * Removes a role. Returns the removed role or null if no role with
   * this name existed.
   */
  public synchronized Role removeRole(String roleName) {
    Role removedRole = roleCache_.remove(roleName);
    if (removedRole == null) return null;
    // Cleanup grant groups
    for (String grantGroup: removedRole.getGrantGroups()) {
      // Remove this role from all of its grant groups.
      Set<String> roles = groupsToRoles_.get(grantGroup);
      if (roles != null) roles.remove(roleName.toLowerCase());
    }
    // Cleanup role ID.
    principalIds_.remove(removedRole.getId());
    return removedRole;
  }

  /**
   * Removes a user. Returns the removed user or null if no user with
   * this name existed.
   */
  public synchronized User removeUser(String userName) {
    User removedUser = userCache_.remove(userName);
    if (removedUser == null) return null;
    // Cleanup user ID.
    principalIds_.remove(removedUser.getId());
    return removedUser;
  }

  /**
   * Adds a new grant group to the specified role. Returns the updated
   * Principal, if a matching role was found. If the role does not exist a
   * CatalogException is thrown.
   */
  public synchronized Role addRoleGrantGroup(String roleName, String groupName)
      throws CatalogException {
    Role role = roleCache_.get(roleName);
    if (role == null) throw new CatalogException("Role does not exist: " + roleName);
    role.addGrantGroup(groupName);
    Set<String> grantedRoles = groupsToRoles_.get(groupName);
    if (grantedRoles == null) {
      grantedRoles = Sets.newHashSet();
      groupsToRoles_.put(groupName, grantedRoles);
    }
    grantedRoles.add(roleName.toLowerCase());
    return role;
  }

  /**
   * Removes a grant group from the specified role. Returns the updated
   * Principal, if a matching role was found. If the role does not exist a
   * CatalogException is thrown.
   */
  public synchronized Role removeRoleGrantGroup(String roleName, String groupName)
      throws CatalogException {
    Role role = roleCache_.get(roleName);
    if (role == null) throw new CatalogException("Role does not exist: " + roleName);
    role.removeGrantGroup(groupName);
    Set<String> grantedRoles = groupsToRoles_.get(groupName);
    if (grantedRoles != null) {
      grantedRoles.remove(roleName.toLowerCase());
    }
    return role;
  }

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
          String authorizeable = privilege.getName();
          if (authorizeable == null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Ignoring invalid privilege: " + privilege.getName());
            }
            continue;
          }
          privileges.add(authorizeable);
        }
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
    Set<String> privileges = listPrivileges(groups, roleSet);
    for (String userName: users) {
      User user = getUser(userName);
      if (user != null) {
        for (PrincipalPrivilege privilege: user.getPrivileges()) {
          String authorizeable = privilege.getName();
          if (authorizeable == null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Ignoring invalid privilege: " + privilege.getName());
            }
            continue;
          }
          privileges.add(authorizeable);
        }
      }
    }
    return privileges;
  }

  @Override
  public void close() {
    // Nothing to do, but required by PrivilegeCache.
  }

  /**
   * Returns the privileges that have been granted to a principal as a tabular result set.
   * Allows for filtering based on a specific privilege spec or showing all privileges
   * granted to the principal. Used by the SHOW GRANT ROLE/USER statement.
   */
  public synchronized TResultSet getPrincipalPrivileges(String principalName,
      TPrivilege filter, TPrincipalType type) {
    TResultSet result = new TResultSet();
    result.setSchema(new TResultSetMetadata());
    result.getSchema().addToColumns(new TColumn("scope", Type.STRING.toThrift()));
    result.getSchema().addToColumns(new TColumn("database", Type.STRING.toThrift()));
    result.getSchema().addToColumns(new TColumn("table", Type.STRING.toThrift()));
    result.getSchema().addToColumns(new TColumn("column", Type.STRING.toThrift()));
    result.getSchema().addToColumns(new TColumn("uri", Type.STRING.toThrift()));
    result.getSchema().addToColumns(new TColumn("privilege", Type.STRING.toThrift()));
    result.getSchema().addToColumns(
        new TColumn("grant_option", Type.BOOLEAN.toThrift()));
    result.getSchema().addToColumns(new TColumn("create_time", Type.STRING.toThrift()));
    result.setRows(Lists.<TResultRow>newArrayList());

    Principal principal = getPrincipal(principalName, type);
    if (principal == null) return result;
    for (PrincipalPrivilege p: principal.getPrivileges()) {
      TPrivilege privilege = p.toThrift();
      if (filter != null) {
        // Check if the privileges are targeting the same object.
        filter.setPrivilege_level(privilege.getPrivilege_level());
        String privName = PrincipalPrivilege.buildPrivilegeName(filter);
        if (!privName.equalsIgnoreCase(privilege.getPrivilege_name())) continue;
      }
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      rowBuilder.add(privilege.getScope().toString().toLowerCase());
      rowBuilder.add(Strings.nullToEmpty(privilege.getDb_name()).toLowerCase());
      rowBuilder.add(Strings.nullToEmpty(privilege.getTable_name()).toLowerCase());
      rowBuilder.add(Strings.nullToEmpty(privilege.getColumn_name()).toLowerCase());
      // URIs are case sensitive
      rowBuilder.add(Strings.nullToEmpty(privilege.getUri()));
      rowBuilder.add(privilege.getPrivilege_level().toString().toLowerCase());
      rowBuilder.add(Boolean.toString(privilege.isHas_grant_opt()));
      if (privilege.getCreate_time_ms() == -1) {
        rowBuilder.add(null);
      } else {
        rowBuilder.add(
            TimeStamp.getNtpTime(privilege.getCreate_time_ms()).toDateString());
      }
      result.addToRows(rowBuilder.get());
    }
    return result;
  }
}
