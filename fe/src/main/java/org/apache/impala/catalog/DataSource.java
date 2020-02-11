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

import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDataSource;
import com.google.common.base.MoreObjects;

/**
 * Represents a data source in the catalog. Contains the data source name and all
 * information needed to locate and load the data source.
 */
public class DataSource extends CatalogObjectImpl implements FeDataSource {
  private final String dataSrcName_;
  private final String className_;
  private final String apiVersionString_;
  // Qualified path to the data source.
  private final String location_;

  public DataSource(String dataSrcName, String location, String className,
      String apiVersionString) {
    dataSrcName_ = dataSrcName;
    location_ = location;
    className_ = className;
    apiVersionString_ = apiVersionString;
  }

  public static DataSource fromThrift(TDataSource thrift) {
    return new DataSource(thrift.getName(), thrift.getHdfs_location(),
        thrift.getClass_name(), thrift.getApi_version());
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.DATA_SOURCE;
  }

  @Override
  public String getName() { return dataSrcName_; }
  @Override // FeDataSource
  public String getLocation() { return location_; }
  @Override // FeDataSource
  public String getClassName() { return className_; }
  @Override // FeDataSource
  public String getApiVersion() { return apiVersionString_; }

  public TDataSource toThrift() {
    return new TDataSource(getName(), location_, className_, apiVersionString_);
  }

  public String debugString() {
    return MoreObjects.toStringHelper(this)
        .add("name", dataSrcName_)
        .add("location", location_)
        .add("className", className_)
        .add("apiVersion", apiVersionString_)
        .toString();
  }

  public static String debugString(TDataSource thrift) {
    return fromThrift(thrift).debugString();
  }

  @Override
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setData_source(toThrift());
  }
}
