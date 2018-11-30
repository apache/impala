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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.impala.authorization.Authorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable;

import java.util.List;

/**
 * Class used to authorize access to a Function for Sentry.
 */
public class SentryAuthorizableFn extends SentryAuthorizable {
  private final String fnName_;
  private final org.apache.sentry.core.model.db.Database database_;

  public SentryAuthorizableFn(String dbName, String fnName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fnName));
    database_ = new org.apache.sentry.core.model.db.Database(dbName);
    fnName_ = fnName;
  }

  @Override
  public List<Authorizable> getAuthorizableHierarchy() {
    return Lists.newArrayList(new SentryAuthorizableDb(database_.getName()));
  }

  @Override
  public String getName() { return database_.getName() + "." + fnName_; }

  @Override
  public Type getType() { return Type.FUNCTION; }

  @Override
  public String getDbName() { return database_.getName(); }

  @Override
  public String getFnName() { return fnName_; }

  @Override
  public DBModelAuthorizable getDBModelAuthorizable() { return database_; }
}
