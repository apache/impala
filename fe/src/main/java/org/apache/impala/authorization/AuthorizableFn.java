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
import com.google.common.base.Strings;

/**
 * A class to authorize access to a function.
 */
public class AuthorizableFn extends Authorizable {
  private final String dbName_;
  private final String fnName_;

  public AuthorizableFn(String dbName, String fnName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fnName));
    dbName_ = dbName;
    fnName_ = fnName;
  }

  @Override
  public String getName() { return dbName_ + "." + fnName_; }

  @Override
  public Type getType() { return Type.FUNCTION; }

  @Override
  public String getDbName() { return dbName_; }

  @Override
  public String getFnName() { return fnName_; }
}
