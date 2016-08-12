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

package org.apache.impala.catalog;

import org.apache.impala.thrift.TCatalogObjectType;

/**
 * Interface that all catalog objects implement.
 */
public interface CatalogObject {
  // Returns the TCatalogObject type of this Catalog object.
  public TCatalogObjectType getCatalogObjectType();

  // Returns the unqualified object name.
  public String getName();

  // Returns the version of this catalog object.
  public long getCatalogVersion();

  // Sets the version of this catalog object.
  public void setCatalogVersion(long newVersion);

  // Returns true if this CatalogObject has had its metadata loaded, false otherwise.
  public boolean isLoaded();
}