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
 * Represents a privilege request in the context of an Authorizeable object. If no
 * Authorizeable object is provided, it represents a privilege request on the server.
 * For example, SELECT on table Foo in database Bar.
 */
public class PrivilegeRequest {
  private final Authorizeable authorizeable_;
  private final Privilege privilege_;

  public PrivilegeRequest(Authorizeable authorizeable, Privilege privilege) {
    Preconditions.checkNotNull(authorizeable);
    Preconditions.checkNotNull(privilege);
    authorizeable_ = authorizeable;
    privilege_ = privilege;
  }

  public PrivilegeRequest(Privilege privilege) {
    Preconditions.checkNotNull(privilege);
    authorizeable_ = null;
    privilege_ = privilege;
  }

  /*
   * Name of the Authorizeable. Authorizeable refers to the server if it's null.
   */
  public String getName() {
    return (authorizeable_ != null) ? authorizeable_.getName() : "server";
  }

  /*
   * Requested privilege on the Authorizeable.
   */
  public Privilege getPrivilege() { return privilege_; }


  /*
   * Returns Authorizeable object. Null if the request is for server-level permission.
   */
  public Authorizeable getAuthorizeable() { return authorizeable_; }

  @Override
  public int hashCode() {
    return (authorizeable_ == null ? 0 : authorizeable_.hashCode()) * 37 +
        privilege_.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PrivilegeRequest)) return false;
    if (authorizeable_ == null) {
      return ((PrivilegeRequest) o).getPrivilege().equals(privilege_);
    }
    return ((PrivilegeRequest) o).getAuthorizeable().equals(authorizeable_) &&
        ((PrivilegeRequest) o).getPrivilege().equals(privilege_);
  }
}
