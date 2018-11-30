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

import java.util.EnumSet;
import java.util.Set;

import org.apache.impala.authorization.Authorizable.Type;
import org.apache.impala.common.InternalException;

import com.google.common.base.Preconditions;

/**
 * A base class used to check whether a user has access to a given resource.
 */
public abstract class AuthorizationChecker {
  protected final AuthorizationConfig config_;

  /*
   * Creates a new AuthorizationChecker based on the config values.
   */
  public AuthorizationChecker(AuthorizationConfig config) {
    Preconditions.checkNotNull(config);
    config_ = config;
  }

  /*
   * Returns the configuration used to create this AuthorizationProvider.
   */
  public AuthorizationConfig getConfig() { return config_; }

  /**
   * Authorizes the PrivilegeRequest, throwing an Authorization exception if
   * the user does not have sufficient privileges.
   */
  public void checkAccess(User user, PrivilegeRequest privilegeRequest)
      throws AuthorizationException, InternalException {
    Preconditions.checkNotNull(privilegeRequest);

    if (hasAccess(user, privilegeRequest)) return;

    Privilege privilege = privilegeRequest.getPrivilege();
    if (privilegeRequest.getAuthorizable().getType() == Type.FUNCTION) {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to %s functions in: %s",
          user.getName(), grantOption(privilegeRequest.hasGrantOption()), privilege,
          privilegeRequest.getName()));
    }

    if (EnumSet.of(Privilege.ANY, Privilege.ALL, Privilege.VIEW_METADATA)
        .contains(privilege)) {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to access: %s",
          user.getName(), grantOption(privilegeRequest.hasGrantOption()),
          privilegeRequest.getName()));
    } else if (privilege == Privilege.REFRESH) {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to execute " +
          "'INVALIDATE METADATA/REFRESH' on: %s", user.getName(),
          grantOption(privilegeRequest.hasGrantOption()), privilegeRequest.getName()));
    } else if (privilege == Privilege.CREATE &&
        privilegeRequest.getAuthorizable().getType() == Type.TABLE) {
      // Creating a table requires CREATE on a database and we shouldn't
      // expose the table name.
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to execute '%s' on: %s",
          user.getName(), grantOption(privilegeRequest.hasGrantOption()), privilege,
          privilegeRequest.getAuthorizable().getDbName()));
    } else {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to execute '%s' on: %s",
          user.getName(), grantOption(privilegeRequest.hasGrantOption()), privilege,
          privilegeRequest.getName()));
    }
  }

  private static String grantOption(boolean hasGrantOption) {
    return hasGrantOption ? " with 'GRANT OPTION'" : "";
  }

  /**
   * Returns the set of groups this user belongs to.
   */
  public abstract Set<String> getUserGroups(User user) throws InternalException;

  /*
   * Returns true if the given user has permission to execute the given
   * request, false otherwise. Always returns true if authorization is disabled.
   */
  public abstract boolean hasAccess(User user, PrivilegeRequest request)
      throws InternalException;
}
