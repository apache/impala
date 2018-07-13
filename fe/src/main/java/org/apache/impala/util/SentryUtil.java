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

package org.apache.impala.util;

import java.util.Set;

import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryGroupNotFoundException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
// See IMPALA-5540. Sentry over-shades itself (to avoid leaking Thrift),
// causing this unusual package name. In the code below, we typically
// check for either variant when it's available in the classpath.
import sentry.org.apache.sentry.core.common.exception.SentryUserException;

/**
 * Wrapper to facilitate differences in Sentry APIs across Sentry versions.
 */
public class SentryUtil {
  static boolean isSentryAlreadyExists(Exception e) {
    return e instanceof SentryAlreadyExistsException || e instanceof
      sentry.org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
  }

  static boolean isSentryAccessDenied(Exception e) {
    return e instanceof SentryAccessDeniedException || e instanceof
      sentry.org.apache.sentry.core.common.exception.SentryAccessDeniedException;
  }

  public static boolean isSentryGroupNotFound(Exception e) {
    return e instanceof SentryGroupNotFoundException;
  }

  static Set<TSentryRole> listRoles(SentryPolicyServiceClient client, String username)
      throws SentryUserException {
    return client.listAllRoles(username);
  }
}
