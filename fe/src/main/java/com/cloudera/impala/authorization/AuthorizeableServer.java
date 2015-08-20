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

import java.util.List;

import org.apache.sentry.core.model.db.DBModelAuthorizable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Class used to authorize access at the catalog level. Generally, all Impala
 * services in the cluster will be configured with the same catalog name.
 * What Sentry refers to as a Server maps to our concept of a Catalog, thus
 * the name AuthorizeableServer.
 */
public class AuthorizeableServer extends Authorizeable {
  private final org.apache.sentry.core.model.db.Server server_;

  public AuthorizeableServer(String serverName) {
    Preconditions.checkState(!Strings.isNullOrEmpty(serverName));
    server_ = new org.apache.sentry.core.model.db.Server(serverName);
  }

  @Override
  public List<DBModelAuthorizable> getHiveAuthorizeableHierarchy() {
    return Lists.newArrayList((DBModelAuthorizable) server_);
  }

  @Override
  public String getName() { return server_.getName(); }
}
