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

package org.apache.impala.authorization.ranger;

import com.google.common.base.Preconditions;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

/**
 * A builder for creating {@link RangerAccessResourceImpl} instance.
 */
public class RangerImpalaResourceBuilder {
  public static final String DATABASE = "database";
  public static final String TABLE = "table";
  public static final String COLUMN = "column";
  public static final String UDF = "udf";
  public static final String URL = "url";
  public static final String STORAGE_TYPE = "storage-type";
  public static final String STORAGE_URL = "storage-url";

  private final RangerAccessResourceImpl rangerAccessResource =
      new RangerAccessResourceImpl();

  public RangerImpalaResourceBuilder database(String dbName) {
    rangerAccessResource.setValue(DATABASE, Preconditions.checkNotNull(dbName));
    return this;
  }

  public RangerImpalaResourceBuilder table(String tableName) {
    rangerAccessResource.setValue(TABLE, Preconditions.checkNotNull(tableName));
    return this;
  }

  public RangerImpalaResourceBuilder column(String columnName) {
    rangerAccessResource.setValue(COLUMN, Preconditions.checkNotNull(columnName));
    return this;
  }

  public RangerImpalaResourceBuilder function(String fnName) {
    rangerAccessResource.setValue(UDF, Preconditions.checkNotNull(fnName));
    return this;
  }

  public RangerImpalaResourceBuilder uri(String uri) {
    rangerAccessResource.setValue(URL, Preconditions.checkNotNull(uri));
    return this;
  }

  public RangerImpalaResourceBuilder storageType(String storageType) {
    rangerAccessResource.setValue(STORAGE_TYPE, Preconditions.checkNotNull(storageType));
    return this;
  }

  public RangerImpalaResourceBuilder storageUri(String storageUri) {
    rangerAccessResource.setValue(STORAGE_URL, Preconditions.checkNotNull(storageUri));
    return this;
  }

  public RangerImpalaResourceBuilder owner(String ownerUser) {
    rangerAccessResource.setOwnerUser(ownerUser);
    return this;
  }

  public RangerAccessResourceImpl build() { return rangerAccessResource; }
}
