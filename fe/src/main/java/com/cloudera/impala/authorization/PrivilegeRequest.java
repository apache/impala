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

import com.google.common.base.Preconditions;

/*
 * Represents a privilege request in the context of an Authorizeable object.
 * For example, SELECT on table Foo in database Bar.
 */
public class PrivilegeRequest {
  private final Authorizeable authorizeable;
  private final Privilege privilege;

  public PrivilegeRequest(Authorizeable authorizeable, Privilege privilege) {
    Preconditions.checkNotNull(authorizeable);
    Preconditions.checkNotNull(privilege);
    this.authorizeable = authorizeable;
    this.privilege = privilege;
  }

  /*
   * Returns Authorizeable object.
   */
  public Authorizeable getAuthorizeable() {
    return authorizeable;
  }

  /*
   * Name of the Authorizeable.
   */
  public String getName() {
    return authorizeable.getName();
  }

  /*
   * Requested privilege on the Authorizeable.
   */
  public Privilege getPrivilege() {
    return privilege;
  }
}