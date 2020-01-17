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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TPrincipal;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.impala.thrift.TPrincipalType;

/**
 * A base class that represents a principal in an authorization policy.
 * This class is thread safe.
 */
public abstract class Principal extends CatalogObjectImpl {
  private final TPrincipal principal_;
  // The last principal ID assigned, starts at 0.
  private static AtomicInteger principalId_ = new AtomicInteger(0);
  // URIs are case sensitive, so we need to store privilege names in a case sensitive way.
  private final CatalogObjectCache<PrincipalPrivilege> principalPrivileges_ =
      new CatalogObjectCache<>(false);

  // An index that allows efficient filtering of Privileges that are relevant to an
  // access check. Should contain exactly the same privileges as principalPrivileges_.
  // Does not support catalog version logic / removal of privileges by privilegeName,
  // so principalPrivileges_ is still needed.
  private final PrincipalPrivilegeTree privilegeTree_ = new PrincipalPrivilegeTree();

  // Protects privilegeTree_ and its coherence with principalPrivileges_.
  // Needs to be taken when accessing privilegeTree_ or when writing principalPrivileges_,
  // but not when reading principalPrivileges_.
  private final ReentrantReadWriteLock rwLock_ = new ReentrantReadWriteLock(true);

  protected Principal(String principalName, TPrincipalType type,
      Set<String> grantGroups) {
    principal_ = new TPrincipal();
    principal_.setPrincipal_name(principalName);
    principal_.setPrincipal_type(type);
    principal_.setPrincipal_id(principalId_.incrementAndGet());
    principal_.setGrant_groups(Lists.newArrayList(grantGroups));
  }

  protected Principal(TPrincipal principal) {
    principal_ = principal;
  }

  /**
   * Adds a privilege to the principal. Returns true if the privilege was added
   * successfully or false if there was a newer version of the privilege already added
   * to the principal.
   */
  public boolean addPrivilege(PrincipalPrivilege privilege) {
    try {
      rwLock_.writeLock().lock();
      if (!principalPrivileges_.add(privilege)) return false;
      privilegeTree_.add(privilege);
    } finally {
      rwLock_.writeLock().unlock();
    }
    return true;
  }

  /**
   * Returns all privileges for this principal. If no privileges have been added to the
   * principal, an empty list is returned.
   */
  public List<PrincipalPrivilege> getPrivileges() {
    return new ArrayList<>(principalPrivileges_.getValues());
  }

  /**
   * Returns all privilege names for this principal, or an empty set if no privileges are
   * granted to the principal.
   */
  public Set<String> getPrivilegeNames() {
    return new HashSet<>(principalPrivileges_.keySet());
  }

  /**
   * Returns all privilege names for this principal that match 'filter'.
   */
  public Set<String> getFilteredPrivilegeNames(PrincipalPrivilegeTree.Filter filter) {
    if (filter == null) return getPrivilegeNames();

    List<PrincipalPrivilege> privileges;
    try {
      rwLock_.readLock().lock();
      privileges = privilegeTree_.getFilteredList(filter);
    } finally {
      rwLock_.readLock().unlock();
    }

    Set<String> results = new HashSet<>();
    for (PrincipalPrivilege priv: privileges) results.add(priv.getName());
    return results;
  }

  /**
   * Gets a privilege with the given name from this principal. If no privilege exists
   * with this name null is returned.
   */
  public PrincipalPrivilege getPrivilege(String privilegeName) {
    return principalPrivileges_.get(privilegeName);
  }

  /**
   * Removes a privilege with the given name from the principal. Returns the removed
   * privilege or null if no privilege exists with this name.
   */
  public PrincipalPrivilege removePrivilege(String privilegeName) {
    try {
      rwLock_.writeLock().lock();
      PrincipalPrivilege privilege = principalPrivileges_.remove(privilegeName);
      if (privilege != null) privilegeTree_.remove(privilege);
      return privilege;
    } finally {
      rwLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a new grant group to this principal.
   */
  public synchronized void addGrantGroup(String groupName) {
    if (principal_.getGrant_groups().contains(groupName)) return;
    principal_.addToGrant_groups(groupName);
  }

  /**
   * Removes a grant group from this principal.
   */
  public synchronized void removeGrantGroup(String groupName) {
    principal_.getGrant_groups().remove(groupName);
    // Should never have duplicates in the list of groups.
    Preconditions.checkState(!principal_.getGrant_groups().contains(groupName));
  }

  /**
   * Returns the Thrift representation of the principal.
   */
  public TPrincipal toThrift() {
    return principal_;
  }

  /**
   * Creates a Principal from a TPrincipal thrift struct.
   */
  public static Principal fromThrift(TPrincipal thriftPrincipal) {
    return thriftPrincipal.getPrincipal_type() == TPrincipalType.ROLE ?
        new Role(thriftPrincipal) : new User(thriftPrincipal);
  }

  /**
   * Creates a new instance of Principal.
   */
  public static Principal newInstance(String principalName, TPrincipalType type,
      Set<String> grantGroups) {
    return type == TPrincipalType.ROLE ?
        new Role(principalName, grantGroups) : new User(principalName, grantGroups);
  }

  /**
   * Gets the set of group names that have been granted to this principal or an empty
   * set if no groups have been granted.
   */
  public Set<String> getGrantGroups() {
    return Sets.newHashSet(principal_.getGrant_groups());
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.PRINCIPAL;
  }

  @Override
  public String getName() { return principal_.getPrincipal_name(); }

  /**
   * Returns the principal ID.
   */
  public int getId() { return principal_.getPrincipal_id(); }

  @Override
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setPrincipal(toThrift());
  }

  /**
   * Returns the principal type.
   */
  public TPrincipalType getPrincipalType() { return principal_.getPrincipal_type(); }

  public static String toString(TPrincipalType type) {
    String principal;
    switch (type) {
      case ROLE:
        principal = "Role";
        break;
      case USER:
        principal = "User";
        break;
      case GROUP:
        principal = "Group";
        break;
      default:
        throw new IllegalStateException(String.format("Unsupported principal type " +
            "%s.", type));
    }
    return principal;
  }
}
