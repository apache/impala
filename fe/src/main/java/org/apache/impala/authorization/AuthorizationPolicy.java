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

package org.apache.impala.authorization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogObjectCache;
import org.apache.impala.catalog.CatalogObjectVersionSet;
import org.apache.impala.catalog.Principal;
import org.apache.impala.catalog.PrincipalPrivilege;
import org.apache.impala.catalog.Role;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TPrincipal;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

/**
 * A thread safe authorization policy cache, consisting of roles, groups that are
 * members of that role, the privileges associated with the role, and users and the
 * privileges associated with the user.
 * The roleCache_ contains all roles defined in authorization provider whereas userCache_
 * only contains users that have privileges defined in authorization provider and does
 * not represent all users in the system. A principal type can have 0 or more privileges
 * and principal types are stored in a map of principal name to principal object.
 * For example:
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
 */
public class AuthorizationPolicy {
  private static final Logger LOG = Logger.getLogger(AuthorizationPolicy.class);

  // Need to keep separate caches of role names and user names since there is
  // no uniqueness guarantee across roles and users
  // Cache of role names (case-insensitive) to role objects.
  private final CatalogObjectCache<Role> roleCache_ = new CatalogObjectCache<>();

  // Cache of user names (case-sensitive) to user objects.
  private final CatalogObjectCache<org.apache.impala.catalog.User> userCache_ =
      new CatalogObjectCache<>(false);

  // Map of principal ID -> user/role name. Used to match privileges to users/roles.
  private final Map<Integer, String> principalIds_ = new HashMap<>();

  // Map of group name (case sensitive) to set of role names (case insensitive) that
  // have been granted to this group. Kept in sync with roleCache_. Provides efficient
  // lookups of Role by group name.
  Map<String, Set<String>> groupsToRoles_ = new HashMap<>();

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
      Preconditions.checkArgument(principal instanceof org.apache.impala.catalog.User);
      userCache_.add((org.apache.impala.catalog.User) principal);
    } else {
      Preconditions.checkArgument(principal instanceof Role);
      roleCache_.add((Role) principal);
    }

    // Add new grants
    for (String groupName: principal.getGrantGroups()) {
      Set<String> grantedRoles = groupsToRoles_.get(groupName);
      if (grantedRoles == null) {
        grantedRoles = new HashSet<>();
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
   * Returns all roles in the policy. Returns an empty list if no roles exist.
   */
  public synchronized List<Role> getAllRoles() {
    return roleCache_.getValues();
  }

  /**
   * Returns all users in the policy. Returns an empty list if no users exist.
   */
  public synchronized List<org.apache.impala.catalog.User> getAllUsers() {
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
  public synchronized org.apache.impala.catalog.User getUser(String userName) {
    return userCache_.get(userName);
  }

  /**
   * Gets a user given a user ID. Returns null if no user exists with this ID.
   */
  public synchronized org.apache.impala.catalog.User getUser(int userId) {
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
    List<Role> grantedRoles = new ArrayList<>();
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
   * Removes a principal, but only if the existing principal has a lower version
   * than the specified 'dropVersion'.
   */
  public synchronized void removePrincipalIfLowerVersion(TPrincipal thriftPrincipal,
      long dropCatalogVersion) {
    Principal existingPrincipal = getPrincipal(thriftPrincipal.getPrincipal_name(),
        thriftPrincipal.getPrincipal_type());
    if (existingPrincipal == null ||
        existingPrincipal.getCatalogVersion() >= dropCatalogVersion) {
      return;
    }

    removePrincipal(thriftPrincipal.getPrincipal_name(),
        thriftPrincipal.getPrincipal_type());
    // NOTE: the privileges are added to the CatalogObjectVersionSet automatically
    // by being added to the CatalogObjectCache<Privilege> inside the principal.
    // However, since we're just removing the principal wholesale here without removing
    // anything from that map, we need to manually remove them from the version set.
    //
    // TODO(todd): it seems like it would make sense to do this in removePrincipal rather
    // than here, but this is preserving the behavior of the existing code. Perhaps
    // we have a memory leak in the catalogd due to not removing them there.
    CatalogObjectVersionSet.INSTANCE.removeAll(existingPrincipal.getPrivileges());
  }



  /**
   * Removes a privilege, but only if the existing privilege has a lower version
   * than the specified 'dropVersion'.
   */
  public synchronized void removePrivilegeIfLowerVersion(TPrivilege thriftPrivilege,
      long dropCatalogVersion) {
    Principal principal = getPrincipal(thriftPrivilege.getPrincipal_id(),
        thriftPrivilege.getPrincipal_type());
    if (principal == null) return;
    String privilegeName = PrincipalPrivilege.buildPrivilegeName(thriftPrivilege);
    PrincipalPrivilege existingPrivilege = principal.getPrivilege(privilegeName);
    if (existingPrivilege != null &&
        existingPrivilege.getCatalogVersion() < dropCatalogVersion) {
      principal.removePrivilege(privilegeName);
    }
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
  public synchronized org.apache.impala.catalog.User removeUser(String userName) {
    org.apache.impala.catalog.User removedUser = userCache_.remove(userName);
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
      grantedRoles = new HashSet<>();
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
   * Returns the privileges that have been granted to a role as a tabular result set.
   * Allows for filtering based on a specific privilege spec or showing all privileges
   * granted to the role. Used by the SHOW GRANT ROLE statement.
   */
  public synchronized TResultSet getRolePrivileges(String principalName,
      TPrivilege filter) {
    TResultSet result = new TResultSet();
    result.setSchema(new TResultSetMetadata());
    addColumnOutputColumns(result.getSchema());
    result.setRows(new ArrayList<>());

    Role role = getRole(principalName);
    if (role != null) {
      for (PrincipalPrivilege p : role.getPrivileges()) {
        TPrivilege privilege = p.toThrift();
        if (filter != null && isPrivilegeFiltered(filter, privilege)) continue;
        TResultRowBuilder rowBuilder = new TResultRowBuilder();
        result.addToRows(addShowPrincipalOutputResults(privilege, rowBuilder).get());
      }
    }
    return result;
  }

  /**
   * Check if the filter matches the privilege.
   */
  private boolean isPrivilegeFiltered(TPrivilege filter, TPrivilege privilege) {
    // Set the filter with privilege level and has grant option from the given privilege
    // since those two fields don't matter for the filter.
    filter.setPrivilege_level(privilege.getPrivilege_level());
    filter.setHas_grant_opt(privilege.isHas_grant_opt());
    String privName = PrincipalPrivilege.buildPrivilegeName(filter);
    return !privName.equalsIgnoreCase(PrincipalPrivilege.buildPrivilegeName(privilege));
  }

  /**
   * This method adds the common column headers used for both SHOW GRANT USER
   * and SHOW GRANT ROLE.
   */
  private void addColumnOutputColumns(TResultSetMetadata schema) {
    schema.addToColumns(new TColumn("scope", Type.STRING.toThrift()));
    schema.addToColumns(new TColumn("database", Type.STRING.toThrift()));
    schema.addToColumns(new TColumn("table", Type.STRING.toThrift()));
    schema.addToColumns(new TColumn("column", Type.STRING.toThrift()));
    schema.addToColumns(new TColumn("uri", Type.STRING.toThrift()));
    schema.addToColumns(new TColumn("privilege", Type.STRING.toThrift()));
    schema.addToColumns(new TColumn("grant_option", Type.BOOLEAN.toThrift()));
    schema.addToColumns(new TColumn("create_time", Type.STRING.toThrift()));
  }

  /**
   * This method adds the common output for both SHOW GRANT USER and SHOW GRANT ROLE.
   */
  private TResultRowBuilder addShowPrincipalOutputResults(TPrivilege privilege,
      TResultRowBuilder rowBuilder) {
    rowBuilder.add(privilege.getScope().toString().toLowerCase());
    rowBuilder.add(Strings.nullToEmpty(privilege.getDb_name()).toLowerCase());
    rowBuilder.add(Strings.nullToEmpty(privilege.getTable_name()).toLowerCase());
    rowBuilder.add(Strings.nullToEmpty(privilege.getColumn_name()).toLowerCase());
    // URIs are case sensitive
    rowBuilder.add(Strings.nullToEmpty(privilege.getUri()));
    rowBuilder.add(privilege.getPrivilege_level().toString().toLowerCase());
    rowBuilder.add(privilege.isHas_grant_opt());
    if (privilege.getCreate_time_ms() == -1) {
      rowBuilder.add(null);
    } else {
      rowBuilder.add(TimeStamp.getNtpTime(privilege.getCreate_time_ms()).toDateString());
    }
    return rowBuilder;
  }

  /**
   * Returns the privileges that have been granted to a user as a tabular result set.
   * Allows for filtering based on a specific privilege spec or showing all privileges
   * granted to the user. Used by the SHOW GRANT USER statement.
   */
  public synchronized TResultSet getUserPrivileges(String principalName,
      Set<String> groupNames, TPrivilege filter) throws AnalysisException {
    TResultSet result = new TResultSet();
    result.setSchema(new TResultSetMetadata());

    result.getSchema().addToColumns(new TColumn("principal_type",
        Type.STRING.toThrift()));
    result.getSchema().addToColumns(new TColumn("principal_name",
        Type.STRING.toThrift()));
    addColumnOutputColumns(result.getSchema());
    result.setRows(new ArrayList<>());

    // A user should be considered to not exist if they do not have any groups.
    if (groupNames.isEmpty()) {
      throw new AnalysisException(String.format("User '%s' does not exist.",
          principalName));
    }
    org.apache.impala.catalog.User user = getUser(principalName);
    if (user != null) {
      createShowUserPrivilegesResultRows(result, user.getPrivileges(), filter,
          principalName, TPrincipalType.USER);
    }

    // Get the groups that user belongs to, get the roles those groups belong to and
    // return those privileges as well.
    List<Role> roles = new ArrayList<>();
    for (String groupName: groupNames) {
      roles.addAll(getGrantedRoles(groupName));
    }
    for (Role role: roles) {
      Principal rolePrincipal = getRole(role.getName());
      if (rolePrincipal != null) {
        createShowUserPrivilegesResultRows(result, rolePrincipal.getPrivileges(), filter,
            rolePrincipal.getName(), TPrincipalType.ROLE);
      }
    }
    return result;
  }

  /**
   * This method adds the rows to the output for the SHOW GRANT USER statement for user
   * and associated roles.
   */
  private void createShowUserPrivilegesResultRows(TResultSet result,
      List<PrincipalPrivilege> privileges, TPrivilege filter, String name,
      TPrincipalType type) {
    for (PrincipalPrivilege p : privileges) {
      TPrivilege privilege = p.toThrift();
      if (filter != null && isPrivilegeFiltered(filter, privilege)) continue;
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      rowBuilder.add(Strings.nullToEmpty(type.name().toUpperCase()));
      rowBuilder.add(Strings.nullToEmpty(name));
      result.addToRows(addShowPrincipalOutputResults(privilege, rowBuilder).get());
    }
  }
}
