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

import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryGroupNotFoundException;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.thrift.transport.TTransportException;

/**
 * A helper class for Sentry.
 */
public class SentryUtil {
  private SentryUtil() {}

  public static boolean isSentryAlreadyExists(Exception e) {
    return e instanceof SentryAlreadyExistsException;
  }

  public static boolean isSentryAccessDenied(Exception e) {
    return e instanceof SentryAccessDeniedException;
  }

  public static boolean isSentryGroupNotFound(Exception e) {
    return e instanceof SentryGroupNotFoundException;
  }

  public static boolean isSentryUnavailable(Exception e) {
    return e instanceof SentryUserException &&
        e.getCause() instanceof TTransportException;
  }
}
