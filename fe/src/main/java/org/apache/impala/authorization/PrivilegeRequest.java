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

import java.util.Objects;

/**
 * Represents a privilege request in the context of an Authorizable object. If no
 * Authorizable object is provided, it represents a privilege request on the server.
 * For example, SELECT on table Foo in database Bar.
 *
 * Use PrivilegeRequestBuilder to create this instance.
 */
public class PrivilegeRequest {
  private final Authorizable authorizable_;
  private final Privilege privilege_;
  private final boolean grantOption_;

  PrivilegeRequest(Authorizable authorizable, Privilege privilege, boolean grantOption) {
    Preconditions.checkNotNull(authorizable);
    Preconditions.checkNotNull(privilege);
    authorizable_ = authorizable;
    privilege_ = privilege;
    grantOption_ = grantOption;
  }

  /*
   * Name of the Authorizable.
   */
  public String getName() { return authorizable_.getName(); }

  /*
   * Requested privilege on the Authorizable.
   */
  public Privilege getPrivilege() { return privilege_; }

  /*
   * Returns Authorizable object. Null if the request is for server-level permission.
   */
  public Authorizable getAuthorizable() { return authorizable_; }

  /**
   * Returns whether the grant option is required or not.
   */
  public boolean hasGrantOption() { return grantOption_; }

  @Override
  public int hashCode() {
    return Objects.hash(authorizable_, privilege_, grantOption_);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrivilegeRequest that = (PrivilegeRequest) o;
    return grantOption_ == that.grantOption_ &&
        Objects.equals(authorizable_, that.authorizable_) &&
        privilege_ == that.privilege_;
  }
}
