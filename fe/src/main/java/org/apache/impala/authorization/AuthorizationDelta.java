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
import org.apache.impala.thrift.TCatalogObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class keeps track of authorization catalog objects added/removed, such as:
 * - {@link org.apache.impala.catalog.Role}
 * - {@link org.apache.impala.catalog.User}
 * - {@link org.apache.impala.catalog.PrincipalPrivilege}
 */
public class AuthorizationDelta {
  private final List<TCatalogObject> added_;
  private final List<TCatalogObject> removed_;

  public AuthorizationDelta() {
    this(new ArrayList<>(), new ArrayList<>());
  }

  public AuthorizationDelta(List<TCatalogObject> added, List<TCatalogObject> removed) {
    added_ = Preconditions.checkNotNull(added);
    removed_ = Preconditions.checkNotNull(removed);
  }

  public AuthorizationDelta addCatalogObjectAdded(TCatalogObject catalogObject) {
    added_.add(catalogObject);
    return this;
  }

  public AuthorizationDelta addCatalogObjectRemoved(TCatalogObject catalogObject) {
    removed_.add(catalogObject);
    return this;
  }

  public List<TCatalogObject> getCatalogObjectsAdded() {
    return Collections.unmodifiableList(added_);
  }

  public List<TCatalogObject> getCatalogObjectsRemoved() {
    return Collections.unmodifiableList(removed_);
  }
}