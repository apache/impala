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

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.common.InternalException;

import java.util.Set;

/**
 * An interface used to check whether a user has access to a given resource.
 */
public interface AuthorizationChecker {
  /*
   * Returns true if the given user has permission to execute the given
   * request, false otherwise. Always returns true if authorization is disabled or the
   * given user is an admin user.
   */
  boolean hasAccess(User user, PrivilegeRequest request) throws InternalException;

  /**
   * Creates a a new {@link AuthorizationContext}. {@link AuthorizationContext} gets
   * created per authorization execution.
   *
   * @param doAudits a flag whether or not to do the audits
   */
  AuthorizationContext createAuthorizationContext(boolean doAudits);

  /**
   * Authorize an analyzed statement.
   *
   * @throws AuthorizationException thrown if the user doesn't have sufficient privileges
   *                                to run this statement.
   */
  void authorize(AuthorizationContext authzCtx, AnalysisResult analysisResult,
      FeCatalog catalog) throws AuthorizationException, InternalException;

  /**
   * This method is to be executed after an authorization check has occurred.
   */
  void postAuthorize(AuthorizationContext authzCtx)
      throws AuthorizationException, InternalException;

  /**
   * Returns a set of groups for a given user.
   */
  Set<String> getUserGroups(User user) throws InternalException;

  /**
   * Invalidates an authorization cache.
   */
  void invalidateAuthorizationCache();
}
