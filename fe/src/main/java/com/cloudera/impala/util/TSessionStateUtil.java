// Copyright 2012 Cloudera Inc.
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

package com.cloudera.impala.util;

import com.cloudera.impala.thrift.TSessionState;

/**
 * Utility functions for working with TSessionState objects.
 */
public class TSessionStateUtil {
  /**
   * Returns the actual user to perform authorisation against for the provided
   * session. That is, returns the delegated user for a session if set, otherwise
   * returns the connected user.
   */
  public static String getEffectiveUser(TSessionState session) {
    if (session.getDelegated_user() != null &&
        !session.getDelegated_user().isEmpty()) {
      return session.getDelegated_user();
    }
    return session.getConnected_user();
  }
}
