/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.authorization.sentry;

import com.google.common.base.Preconditions;
import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of BitFieldActionFactory for Impala.
 */
public class ImpalaActionFactory extends BitFieldActionFactory {
  @Override
  public List<? extends BitFieldAction> getActionsByCode(int actionCode) {
    Preconditions.checkArgument(
        actionCode >= 1 && actionCode <= ImpalaAction.ALL.getCode(),
        String.format("Action code must between 1 and %d.", ImpalaAction.ALL.getCode()));

    List<BitFieldAction> actions = new ArrayList<>();
    for (ImpalaAction action : ImpalaAction.values()) {
      if ((action.getCode() & actionCode) == action.getCode()) {
        actions.add(action.getBitFieldAction());
      }
    }
    return actions;
  }

  @Override
  public BitFieldAction getActionByName(String name) {
    Preconditions.checkNotNull(name);

    for (ImpalaAction action : ImpalaAction.values()) {
      if (action.getValue().equalsIgnoreCase(name)) {
        return action.getBitFieldAction();
      }
    }
    return null;
  }
}
