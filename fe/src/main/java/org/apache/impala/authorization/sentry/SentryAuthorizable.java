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

import com.google.common.base.Preconditions;
import org.apache.impala.authorization.Authorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A base class for Authorizable with Sentry specific operations.
 */
public abstract class SentryAuthorizable extends Authorizable {
  /*
   * Returns the list of the Authorizable objects in their hierarchical order.
   * For example:
   * [Column] would return Db -> Table -> Column
   * [Table] would return Db -> Table
   * [Db] would return [Db]
   * [URI] would return [URI]
   */
  public abstract List<Authorizable> getAuthorizableHierarchy();

  /**
   * Returns the list of the Sentry DBModelAuthorizable objects in their hierarchical
   * order.
   */
  public List<DBModelAuthorizable> getDBModelAuthorizableHierarchy() {
    return getAuthorizableHierarchy().stream()
        .map(a -> {
          Preconditions.checkState(a instanceof SentryAuthorizable);
          return ((SentryAuthorizable) a).getDBModelAuthorizable();
        })
        .collect(Collectors.toList());
  }

  /**
   * Returns the Sentry DBModelAuthorizable that represents the type
   * of Authorizable.
   */
  public abstract DBModelAuthorizable getDBModelAuthorizable();
}
