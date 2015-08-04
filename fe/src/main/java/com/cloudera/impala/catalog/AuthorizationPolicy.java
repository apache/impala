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

package com.cloudera.impala.catalog;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.log4j.Logger;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.provider.cache.PrivilegeCache;

import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.util.TResultRowBuilder;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A thread safe authorization policy cache, consisting of roles, groups that are
 * members of that role, and the privileges associated with the role. The source data
 * this cache is backing is read from the Sentry Policy Service. Writing to the cache
 * will replace any matching items, but will not write back to the Sentry Policy Service.
 * A role can have 0 or more privileges and roles are stored in a map of role name
 * to role object. For example:
 * RoleName -> Role -> [RolePriv1, ..., RolePrivN]
 * To ensure we can efficiently retrieve the roles that a user is a member of, a map
 * of user group name to role name is tracked as grantGroups_.
 * To reduce duplication of metadata, privileges are linked to roles using a "role ID"
 * rather than embedding the role name. When a privilege is added to a role, we do
 * a lookup to get the role ID to using the roleIds_ map.
 * Acts as the backing cache for the Sentry cached based provider (which is why
 * PrivilegeCache is implemented).
 * TODO: Instead of calling into Sentry to perform final authorization checks, we
 * should parse/validate the privileges in Impala.
 */
public class AuthorizationPolicy implements PrivilegeCache {
  private static final Logger LOG = Logger.getLogger(AuthorizationPolicy.class);

  // Cache of role names (case-insensitive) to role objects.
  private final CatalogObjectCache<Role> roleCache_ = new CatalogObjectCache<Role>();

  // Map of role ID -> role name. Used to match privileges to roles.
  Map<Integer, String> roleIds_ = Maps.newHashMap();

  // Map of group name (case sensitive) to set of role names (case insensitive) that
  // have been granted to this group. Kept in sync with roleCache_. Provides efficient
  // lookups of Role by group name.
  Map<String, Set<String>> groupsToRoles_ = Maps.newHashMap();

  /**
   * Adds a new role to the policy. If a role with the same name already
   * exists and the role ID's are different, it will be overwritten by the new role.
   * If a role exists and the role IDs are the same, the privileges from the old
   * role will be copied to the new role.
   */
  public synchronized void addRole(Role role) {
    Role existingRole = roleCache_.get(role.getName());
    // There is already a newer version of this role in the catalog, ignore
    // just return.
    if (existingRole != null &&
        existingRole.getCatalogVersion() >= role.getCatalogVersion()) return;

    // If there was an existing role that was replaced we first need to remove it.
    if (existingRole != null) {
      // Remove the role. This will also clean up the grantGroup mappings.
      removeRole(existingRole.getName());
      if (existingRole.getId() == role.getId()) {
        // Copy the privileges from the existing role.
        for (RolePrivilege p: existingRole.getPrivileges()) {
          role.addPrivilege(p);
        }
      }
    }
    roleCache_.add(role);

    // Add new grants
    for (String groupName: role.getGrantGroups()) {
      Set<String> grantedRoles = groupsToRoles_.get(groupName);
      if (grantedRoles == null) {
        grantedRoles = Sets.newHashSet();
        groupsToRoles_.put(groupName, grantedRoles);
      }
      grantedRoles.add(role.getName().toLowerCase());
    }

    // Add this role to the role ID mapping
    roleIds_.put(role.getId(), role.getName());
  }

  /**
   * Adds a new privilege to the policy mapping to the role specified by the
   * role ID in the privilege.
   * Throws a CatalogException no role with a corresponding ID existing in the catalog.
   */
  public synchronized void addPrivilege(RolePrivilege privilege)
      throws CatalogException {
    LOG.trace("Adding privilege: " + privilege.getName() +
        " role ID: " + privilege.getRoleId());
    Role role = getRole(privilege.getRoleId());
    if (role == null) {
      throw new CatalogException(String.format("Error adding privilege: %s. Role ID " +
          "'%d' does not exist.", privilege.getName(), privilege.getRoleId()));
    }
    LOG.trace("Adding privilege: " + privilege.getName() + " to role: " +
        role.getName() + "ID: " + role.getId());
    role.addPrivilege(privilege);
  }

  /**
   * Removes a privilege from the policy mapping to the role specified by the
   * role ID in the privilege.
   * Throws a CatalogException if no role with a corresponding ID exists in the catalog.
   * Returns null if no matching privilege is found in this role.
   */
  public synchronized RolePrivilege removePrivilege(RolePrivilege privilege)
      throws CatalogException {
    Role role = getRole(privilege.getRoleId());
    if (role == null) {
      throw new CatalogException(String.format("Error removing privilege: %s. Role ID " +
          "'%d' does not exist.", privilege.getName(), privilege.getRoleId()));
    }
    LOG.trace("Removing privilege: '" + privilege.getName() + "' from Role ID: " +
        privilege.getRoleId() + " Role Name: " + role.getName());
    return role.removePrivilege(privilege.getName());
  }

  /**
   * Returns all roles in the policy. Returns an empty list if no roles exist.
   */
  public synchronized List<Role> getAllRoles() {
    return roleCache_.getValues();
  }

  /**
   * Returns all role names in the policy. Returns an empty set if no roles exist.
   */
  public synchronized Set<String> getAllRoleNames() {
    return Sets.newHashSet(roleCache_.keySet());
  }

  /**
   * Gets a role given a role name. Returns null if no roles exist with this name.
   */
  public synchronized Role getRole(String roleName) {
    return roleCache_.get(roleName);
  }

  /**
   * Gets a role given a role ID. Returns null if no roles exist with this ID.
   */
  public synchronized Role getRole(int roleId) {
    String roleName = roleIds_.get(roleId);
    if (roleName == null) return null;
    return roleCache_.get(roleName);
  }

  /**
   * Gets a privilege from the given role ID. Returns null of there are no roles with a
   * matching ID or if no privilege with this name exists for the role.
   */
  public synchronized RolePrivilege getPrivilege(int roleId, String privilegeName) {
    String roleName = roleIds_.get(roleId);
    if (roleName == null) return null;
    Role role = roleCache_.get(roleName);
    return role.getPrivilege(privilegeName);
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
        Role role = roleCache_.get(roleName);
        if (role != null) grantedRoles.add(roleCache_.get(roleName));
      }
    }
    return grantedRoles;
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
    // Cleanup role id.
    roleIds_.remove(removedRole.getId());
    return removedRole;
  }

  /**
   * Adds a new grant group to the specified role. Returns the updated
   * Role, if a matching role was found. If the role does not exist a
   * CatalogException is thrown.
   */
  public synchronized Role addGrantGroup(String roleName, String groupName)
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
   * Role, if a matching role was found. If the role does not exist a
   * CatalogException is thrown.
   */
  public synchronized Role removeGrantGroup(String roleName, String groupName)
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
  public synchronized Set<String>
      listPrivileges(Set<String> groups, ActiveRoleSet roleSet) {
    Set<String> privileges = Sets.newHashSet();
    if (roleSet != ActiveRoleSet.ALL) {
      throw new UnsupportedOperationException("Impala does not support role subsets.");
    }

    // Collect all privileges granted to all roles.
    for (String groupName: groups) {
      List<Role> grantedRoles = getGrantedRoles(groupName);
      for (Role role: grantedRoles) {
        for (RolePrivilege privilege: role.getPrivileges()) {
          String authorizeable = privilege.getName();
          if (authorizeable == null) {
            LOG.trace("Ignoring invalid privilege: " + privilege.getName());
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
   * Returns the privileges that have been granted to a role as a tabular result set.
   * Allows for filtering based on a specific privilege spec or showing all privileges
   * granted to the role. Used by the SHOW GRANT ROLE statement.
   */
  public synchronized TResultSet getRolePrivileges(String roleName, TPrivilege filter) {
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

    Role role = getRole(roleName);
    if (role == null) return result;
    for (RolePrivilege p: role.getPrivileges()) {
      TPrivilege privilege = p.toThrift();
      if (filter != null) {
        // Check if the privileges are targeting the same object.
        filter.setPrivilege_level(privilege.getPrivilege_level());
        String privName = RolePrivilege.buildRolePrivilegeName(filter);
        if (!privName.equalsIgnoreCase(privilege.getPrivilege_name())) continue;
      }
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      rowBuilder.add(privilege.getScope().toString());
      rowBuilder.add(Strings.nullToEmpty(privilege.getDb_name()));
      rowBuilder.add(Strings.nullToEmpty(privilege.getTable_name()));
      rowBuilder.add(Strings.nullToEmpty(privilege.getColumn_name()));
      rowBuilder.add(Strings.nullToEmpty(privilege.getUri()));
      rowBuilder.add(privilege.getPrivilege_level().toString());
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
