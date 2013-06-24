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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/*
 * Class used to authorize access to a URI.
 */
public class AuthorizeableURI implements Authorizeable {
  private final String uriName;

  public AuthorizeableURI(String uriName) {
    Preconditions.checkNotNull(uriName);
    this.uriName = uriName;
  }

  @Override
  public List<org.apache.access.core.Authorizable> getHiveAuthorizeableHierarchy() {
    org.apache.access.core.AccessURI accessURI =
        new org.apache.access.core.AccessURI(uriName);
    return Lists.newArrayList((org.apache.access.core.Authorizable) accessURI);
  }

  @Override
  public String getName() {
    return uriName;
  }
}