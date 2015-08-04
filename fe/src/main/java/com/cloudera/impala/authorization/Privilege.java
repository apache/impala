// Copyright 2013 Cloudera Inc.
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

package com.cloudera.impala.authorization;

import java.util.EnumSet;

import org.apache.sentry.core.model.db.DBModelAction;

/*
 * Maps an Impala Privilege to one or more Hive Access "Actions".
 */
public enum Privilege {
  ALL(DBModelAction.ALL, false),
  ALTER(DBModelAction.ALL, false),
  DROP(DBModelAction.ALL, false),
  CREATE(DBModelAction.ALL, false),
  INSERT(DBModelAction.INSERT, false),
  SELECT(DBModelAction.SELECT, false),
  // Privileges required to view metadata on a server object.
  VIEW_METADATA(EnumSet.of(DBModelAction.INSERT, DBModelAction.SELECT), true),
  // Special privilege that is used to determine if the user has any valid privileges
  // on a target object.
  ANY(EnumSet.allOf(DBModelAction.class), true),
  ;

  private final EnumSet<DBModelAction> actions;

  // Determines whether to check if the user has ANY the privileges defined in the
  // actions list or whether to check if the user has ALL of the privileges in the
  // actions list.
  private final boolean anyOf_;

  private Privilege(EnumSet<DBModelAction> actions, boolean anyOf) {
    this.actions = actions;
    this.anyOf_ = anyOf;
  }

  private Privilege(DBModelAction action, boolean anyOf) {
    this(EnumSet.of(action), anyOf);
  }

  /*
   * Returns the set of Hive Access Actions mapping to this Privilege.
   */
  public EnumSet<DBModelAction> getHiveActions() {
    return actions;
  }

  /*
   * Determines whether to check if the user has ANY the privileges defined in the
   * actions list or whether to check if the user has ALL of the privileges in the
   * actions list.
   */
  public boolean getAnyOf() { return anyOf_; }
}
