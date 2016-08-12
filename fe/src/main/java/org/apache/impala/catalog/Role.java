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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TRole;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Represents a role in an authorization policy. This class is thread safe.
 */
public class Role implements CatalogObject {
  private final TRole role_;
  // The last role ID assigned, starts at 0.
  private static AtomicInteger roleId_ = new AtomicInteger(0);
  private long catalogVersion_ = Catalog.INITIAL_CATALOG_VERSION;

  private final CatalogObjectCache<RolePrivilege> rolePrivileges_ =
      new CatalogObjectCache<RolePrivilege>();

  public Role(String roleName, Set<String> grantGroups) {
    role_ = new TRole();
    role_.setRole_name(roleName);
    role_.setRole_id(roleId_.incrementAndGet());
    role_.setGrant_groups(Lists.newArrayList(grantGroups));
  }

  private Role(TRole role) {
    role_ = role;
  }

  /**
   * Adds a privilege to the role. Returns true if the privilege was added successfully
   * or false if there was a newer version of the privilege already added to the role.
   */
  public boolean addPrivilege(RolePrivilege privilege) {
    return rolePrivileges_.add(privilege);
  }

  /**
   * Returns all privileges for this role. If no privileges have been added to the role
   * an empty list will be returned.
   */
  public List<RolePrivilege> getPrivileges() {
    return Lists.newArrayList(rolePrivileges_.getValues());
  }

  /**
   * Returns all privilege names for this role, or an empty set of no privileges are
   * granted to the role.
   */
  public Set<String> getPrivilegeNames() {
    return Sets.newHashSet(rolePrivileges_.keySet());
  }

  /**
   * Gets a privilege with the given name from this role. If no privilege exists
   * with this name null is returned.
   */
  public RolePrivilege getPrivilege(String privilegeName) {
    return rolePrivileges_.get(privilegeName);
  }

  /**
   * Removes a privilege with the given name from the role. Returns the removed
   * privilege or null if no privilege exists with this name.
   */
  public RolePrivilege removePrivilege(String privilegeName) {
    return rolePrivileges_.remove(privilegeName);
  }

  /**
   * Adds a new grant group to this role.
   */
  public synchronized void addGrantGroup(String groupName) {
    if (role_.getGrant_groups().contains(groupName)) return;
    role_.addToGrant_groups(groupName);
  }

  /**
   * Removes a grant group from this role.
   */
  public synchronized void removeGrantGroup(String groupName) {
    role_.getGrant_groups().remove(groupName);
    // Should never have duplicates in the list of groups.
    Preconditions.checkState(!role_.getGrant_groups().contains(groupName));
  }

  /**
   * Returns the Thrift representation of the role.
   */
  public TRole toThrift() {
    return role_;
  }

  /**
   * Creates a Role from a TRole thrift struct.
   */
  public static Role fromThrift(TRole thriftRole) {
    return new Role(thriftRole);
  }

  /**
   * Gets the set of group names that have been granted this role or an empty
   * Set if no groups have been granted the role.
   */
  public Set<String> getGrantGroups() {
    return Sets.newHashSet(role_.getGrant_groups());
  }
  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.ROLE; }
  @Override
  public String getName() { return role_.getRole_name(); }
  public int getId() { return role_.getRole_id(); }
  @Override
  public synchronized long getCatalogVersion() { return catalogVersion_; }
  @Override
  public synchronized void setCatalogVersion(long newVersion) {
    catalogVersion_ = newVersion;
  }
  @Override
  public boolean isLoaded() { return true; }
}