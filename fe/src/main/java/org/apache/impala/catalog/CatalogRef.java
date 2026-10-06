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

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * A catalog that databases and tables belong to: CatalogD (or the HMS behind it), or an
 * Iceberg REST catalog that the coordinator reaches directly. A database belongs to
 * every catalog that lists it, and its tables are merged from all of them; each table
 * belongs to the one catalog that lists it.
 *
 * <p>The coordinator executes DML finalization and DDL itself for the tables of a REST
 * catalog with a unique name, and addresses that catalog by this name. Everything else
 * goes to CatalogD.
 */
public final class CatalogRef {
  public static final CatalogRef CATALOGD = new CatalogRef(false, null, "CatalogD");

  // Whether this is an Iceberg REST catalog rather than CatalogD.
  private final boolean isRest_;
  // Name of the REST catalog, when it is unique among the configured REST catalogs.
  private final @Nullable String restCatalogName_;
  // Human-readable identity of the catalog, for error messages.
  private final String description_;

  private CatalogRef(boolean isRest, @Nullable String restCatalogName,
      String description) {
    Preconditions.checkArgument(isRest || restCatalogName == null);
    isRest_ = isRest;
    restCatalogName_ = restCatalogName;
    description_ = Preconditions.checkNotNull(description);
  }

  /**
   * Returns a reference to an Iceberg REST catalog. 'uniqueName' is null when the
   * catalog's name is not unique, so the coordinator cannot address it.
   */
  public static CatalogRef rest(@Nullable String uniqueName, String description) {
    return new CatalogRef(true, uniqueName, description);
  }

  /** Whether this is an Iceberg REST catalog, whether or not its name is unique. */
  public boolean isRest() { return isRest_; }

  /**
   * Returns the name the coordinator addresses this REST catalog by, or null for
   * CatalogD and for REST catalogs without a unique name.
   */
  public @Nullable String getRestCatalogName() { return restCatalogName_; }

  @Override
  public String toString() { return description_; }
}
