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

import java.util.EnumSet;

import org.apache.sentry.core.common.Action;

/**
 * Maps an Impala Privilege to one or more Sentry "Actions".
 */
public enum Privilege {
  ALL(SentryAction.ALL, false),
  ALTER(SentryAction.ALL, false),
  DROP(SentryAction.ALL, false),
  CREATE(SentryAction.ALL, false),
  INSERT(SentryAction.INSERT, false),
  SELECT(SentryAction.SELECT, false),
  REFRESH(SentryAction.REFRESH, false),
  // Privileges required to view metadata on a server object.
  VIEW_METADATA(EnumSet.of(SentryAction.INSERT, SentryAction.SELECT,
      SentryAction.REFRESH), true),
  // Special privilege that is used to determine if the user has any valid privileges
  // on a target object.
  ANY(EnumSet.allOf(SentryAction.class), true),
  ;

  private final EnumSet<SentryAction> actions;

  // Determines whether to check if the user has ANY the privileges defined in the
  // actions list or whether to check if the user has ALL of the privileges in the
  // actions list.
  private final boolean anyOf_;

  /**
   * This enum provides a list of Sentry actions used in Impala.
   */
  public enum SentryAction implements Action {
    SELECT("select"),
    INSERT("insert"),
    REFRESH("refresh"),
    ALL("*");

    private final String value;

    SentryAction(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private Privilege(EnumSet<SentryAction> actions, boolean anyOf) {
    this.actions = actions;
    this.anyOf_ = anyOf;
  }

  private Privilege(SentryAction action, boolean anyOf) {
    this(EnumSet.of(action), anyOf);
  }

  /*
   * Returns the set of Sentry Access Actions mapping to this Privilege.
   */
  public EnumSet<SentryAction> getSentryActions() {
    return actions;
  }

  /*
   * Determines whether to check if the user has ANY the privileges defined in the
   * actions list or whether to check if the user has ALL of the privileges in the
   * actions list.
   */
  public boolean getAnyOf() { return anyOf_; }
}
