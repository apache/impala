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
import org.apache.sentry.core.common.BitFieldAction;

/**
 * Maps an Impala Privilege to one or more Impala "Actions".
 */
public enum Privilege {
  ALL(ImpalaAction.ALL, false),
  ALTER(ImpalaAction.ALTER, false),
  DROP(ImpalaAction.DROP, false),
  CREATE(ImpalaAction.CREATE, false),
  INSERT(ImpalaAction.INSERT, false),
  SELECT(ImpalaAction.SELECT, false),
  REFRESH(ImpalaAction.REFRESH, false),
  // Privileges required to view metadata on a server object.
  VIEW_METADATA(EnumSet.of(ImpalaAction.INSERT, ImpalaAction.SELECT,
      ImpalaAction.REFRESH), true),
  // Special privilege that is used to determine if the user has any valid privileges
  // on a target object.
  ANY(EnumSet.allOf(ImpalaAction.class), true),
  ;

  private final EnumSet<ImpalaAction> actions;

  // Determines whether to check if the user has ANY the privileges defined in the
  // actions list or whether to check if the user has ALL of the privileges in the
  // actions list.
  private final boolean anyOf_;

  /**
   * This enum provides a list of Sentry actions used in Impala.
   */
  public enum ImpalaAction implements Action {
    SELECT("select", 1),
    INSERT("insert", 1 << 2),
    ALTER("alter", 1 << 3),
    CREATE("create", 1 << 4),
    DROP("drop", 1 << 5),
    REFRESH("refresh", 1 << 6),
    ALL("*",
        SELECT.getCode() |
        INSERT.getCode() |
        ALTER.getCode() |
        CREATE.getCode() |
        DROP.getCode() |
        REFRESH.getCode());

    private final BitFieldAction bitFieldAction_;

    // In Sentry 1.5.1, BitFieldAction is an abstract class. In Sentry 2.0.0,
    // BitFieldAction is no longer an absract class. To support both versions,
    // we can extend BitFieldAction.
    private static class ImpalaBitFieldAction extends BitFieldAction {
      public ImpalaBitFieldAction(String name, int code) {
        super(name, code);
      }
    }

    ImpalaAction(String value, int code) {
      bitFieldAction_ = new ImpalaBitFieldAction(value, code);
    }

    @Override
    public String getValue() { return bitFieldAction_.getValue(); }

    public int getCode() { return bitFieldAction_.getActionCode(); }

    public BitFieldAction getBitFieldAction() { return bitFieldAction_; }
  }

  private Privilege(EnumSet<ImpalaAction> actions, boolean anyOf) {
    this.actions = actions;
    this.anyOf_ = anyOf;
  }

  private Privilege(ImpalaAction action, boolean anyOf) {
    this(EnumSet.of(action), anyOf);
  }

  /*
   * Returns the set of Sentry Access Actions mapping to this Privilege.
   */
  public EnumSet<ImpalaAction> getSentryActions() {
    return actions;
  }

  /*
   * Determines whether to check if the user has ANY the privileges defined in the
   * actions list or whether to check if the user has ALL of the privileges in the
   * actions list.
   */
  public boolean getAnyOf() { return anyOf_; }
}
