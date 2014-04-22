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

import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TPrivilegeScope;

/**
 * Represents a privilege that has been granted to a role in an authorization policy.
 * This class is thread safe.
 */
public class RolePrivilege implements CatalogObject {
  private final TPrivilege privilege_;
  private long catalogVersion_ = Catalog.INITIAL_CATALOG_VERSION;
  // The time this role was created. Used to quickly check if the same privilege
  // was dropped and re-created.
  private long createTimeMs_;

  public RolePrivilege(int idParentRole, String privilegeName, String scope,
      long createTimeMs) {
    privilege_ = new TPrivilege();
    privilege_.setPrivilege_name(privilegeName);
    privilege_.setRole_id(idParentRole);
    privilege_.setScope(Enum.valueOf(TPrivilegeScope.class, scope.toUpperCase()));
    createTimeMs_ = createTimeMs;
  }

  private RolePrivilege(TPrivilege privilege) {
    privilege_ = privilege;
  }

  public TPrivilege toThrift() { return privilege_; }
  public static RolePrivilege fromThrift(TPrivilege privilege) {
    return new RolePrivilege(privilege);
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.PRIVILEGE;
  }
  @Override
  public String getName() { return privilege_.getPrivilege_name(); }
  public int getRoleId() { return privilege_.getRole_id(); }
  @Override
  public synchronized long getCatalogVersion() { return catalogVersion_; }
  @Override
  public synchronized void setCatalogVersion(long newVersion) {
    catalogVersion_ = newVersion;
  }
  @Override
  public boolean isLoaded() { return true; }
  public long getCreateTimeMs() { return createTimeMs_; }
  public TPrivilegeScope getScope() { return privilege_.getScope(); }
}