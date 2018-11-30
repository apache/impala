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

import java.util.Map;

import org.apache.sentry.core.common.BitFieldActionFactory;
import org.apache.sentry.core.common.ImplyMethodType;
import org.apache.sentry.core.model.db.HivePrivilegeModel;
import org.apache.sentry.core.common.Model;

/**
 * Delegates to HivePrivilegeModel for getImplyMethodMap(), but
 * uses Impala's BitFieldActionFactory implementation.
 */
public class ImpalaPrivilegeModel implements Model {
  public static final ImpalaPrivilegeModel INSTANCE = new ImpalaPrivilegeModel();
  private final ImpalaActionFactory actionFactory = new ImpalaActionFactory();

  @Override
  public Map<String, ImplyMethodType> getImplyMethodMap() {
    return HivePrivilegeModel.getInstance().getImplyMethodMap();
  }

  @Override
  public BitFieldActionFactory getBitFieldActionFactory() {
    return actionFactory;
  }
}
