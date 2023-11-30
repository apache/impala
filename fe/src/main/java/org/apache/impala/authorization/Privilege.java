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

import com.google.common.base.Preconditions;
import org.apache.impala.service.BackendConfig;

import java.util.EnumSet;

/**
 * List of Impala privileges. Declare them in the order from least allowing to most
 * allowing privilege so EnumSet used in {@link Privilege#getImpliedPrivileges()} can
 * iterate them in this order. This helps in more efficiently checking for VIEW_METADATA
 * and ANY privilege if the user does have access to the resource.
 */
public enum Privilege {
  SELECT,
  INSERT,
  REFRESH,
  ALTER,
  DROP,
  CREATE,
  ALL,
  RWSTORAGE,
  OWNER,
  // Privileges required to view metadata on a server object.
  VIEW_METADATA(true),
  // Special privilege that is used to determine if the user has any valid privileges
  // on a target object.
  ANY(true);

  static {
    ALL.implied_ = EnumSet.of(ALL);
    OWNER.implied_ = EnumSet.of(OWNER);
    ALTER.implied_ = EnumSet.of(ALTER);
    DROP.implied_ = EnumSet.of(DROP);
    CREATE.implied_ = EnumSet.of(CREATE);
    INSERT.implied_ = EnumSet.of(INSERT);
    SELECT.implied_ = EnumSet.of(SELECT);
    REFRESH.implied_ = EnumSet.of(REFRESH);
    RWSTORAGE.implied_ = EnumSet.of(RWSTORAGE);
    VIEW_METADATA.implied_ = EnumSet.of(INSERT, SELECT, REFRESH);
    ANY.implied_ = EnumSet.of(ALL, OWNER, ALTER, DROP, CREATE, INSERT, SELECT,
        REFRESH);

    for (Privilege privilege: values()) {
      Preconditions.checkNotNull(privilege.implied_);
    }
  }

  private EnumSet<Privilege> implied_;
  // Determines whether to check if the user has ANY the privileges defined in the
  // actions list or whether to check if the user has ALL of the privileges in the
  // actions list.
  private final boolean anyOf_;

  Privilege() {
    anyOf_ = false;
  }

  Privilege(boolean anyOf) {
    this.anyOf_ = anyOf;
  }

  /*
   * Determines whether to check if the user has ANY the privileges defined in the
   * actions list or whether to check if the user has ALL of the privileges in the
   * actions list.
   */
  public boolean hasAnyOf() { return anyOf_; }

  /**
   * Gets list of implied privileges for this privilege.
   */
  public EnumSet<Privilege> getImpliedPrivileges() { return implied_; }

  /**
   * Returns true if this implies modification on data or metadata.
   */
  public boolean impliesUpdate() {
    // When allow_catalog_cache_op_from_masked_users=false, REFRESH is considered as
    // an update operation.
    boolean considerCatalogCacheOp =
        !BackendConfig.INSTANCE.allowCatalogCacheOpFromMaskedUsers();
    return this == ALTER || this == DROP || this == CREATE || this == INSERT
        || (this == REFRESH && considerCatalogCacheOp) || this == ALL;
  }
}
