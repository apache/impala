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

import com.google.common.collect.Lists;
import org.apache.impala.authorization.Authorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable;

import java.util.List;

/**
 * Class used to authorize access at the catalog level for Sentry. Generally, all
 * Impala services in the cluster will be configured with the same catalog name.
 * What Sentry refers to as a Server maps to our concept of a Catalog, thus
 * the name AuthorizableServer.
 */
public class SentryAuthorizableServer extends SentryAuthorizable {
  private final org.apache.sentry.core.model.db.Server server_;

  public SentryAuthorizableServer(String serverName) {
    server_ = new org.apache.sentry.core.model.db.Server(
        serverName == null ? "server" : serverName);
  }

  @Override
  public List<Authorizable> getAuthorizableHierarchy() {
    return Lists.newArrayList(this);
  }

  @Override
  public String getName() { return server_.getName(); }

  @Override
  public Type getType() { return Type.SERVER; }

  @Override
  public DBModelAuthorizable getDBModelAuthorizable() { return server_; }
}
