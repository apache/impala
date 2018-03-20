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

import java.util.List;

import com.google.common.base.Strings;
import org.apache.sentry.core.model.db.DBModelAuthorizable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class used to authorize access to a Function.
 */
public class AuthorizeableFn extends Authorizeable {
  private final String fnName_;
  private final org.apache.sentry.core.model.db.Database database_;

  public AuthorizeableFn(String dbName, String fnName) {
    Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkState(!Strings.isNullOrEmpty(fnName));
    database_ = new org.apache.sentry.core.model.db.Database(dbName);
    fnName_ = fnName;
  }

  @Override
  public List<DBModelAuthorizable> getHiveAuthorizeableHierarchy() {
    return Lists.newArrayList((DBModelAuthorizable) database_);
  }

  @Override
  public String getName() { return database_.getName() + "." + fnName_; }

  @Override
  public String getDbName() { return database_.getName(); }

  public String getFnName() { return fnName_; };
}
