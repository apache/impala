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

package org.apache.impala.authorization.sentry;

import org.apache.impala.authorization.Privilege;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.BitFieldAction;

import java.util.EnumSet;
import java.util.stream.Collectors;

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
          REFRESH.getCode()),
  OWNER("owner", ALL.getCode());

  private final BitFieldAction bitFieldAction_;

  ImpalaAction(String value, int code) {
    bitFieldAction_ = new BitFieldAction(value, code);
  }

  @Override
  public String getValue() { return bitFieldAction_.getValue(); }

  public int getCode() { return bitFieldAction_.getActionCode(); }

  public BitFieldAction getBitFieldAction() { return bitFieldAction_; }

  public static EnumSet<ImpalaAction> from(Privilege privilege) {
    switch (privilege) {
      case ALL:
        return EnumSet.of(ImpalaAction.ALL);
      case OWNER:
        return EnumSet.of(ImpalaAction.OWNER);
      case ALTER:
        return EnumSet.of(ImpalaAction.ALTER);
      case DROP:
        return EnumSet.of(ImpalaAction.DROP);
      case CREATE:
        return EnumSet.of(ImpalaAction.CREATE);
      case INSERT:
        return EnumSet.of(ImpalaAction.INSERT);
      case SELECT:
        return EnumSet.of(ImpalaAction.SELECT);
      case REFRESH:
        return EnumSet.of(ImpalaAction.REFRESH);
      case VIEW_METADATA:
      case ANY:
        return EnumSet.copyOf(privilege.getImpliedPrivileges()
            .stream()
            .flatMap(p -> from(p).stream())
            .collect(Collectors.toSet()));
      default:
        throw new IllegalArgumentException("Unsupported privilege: " + privilege);
    }
  }
}
