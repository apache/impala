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

import org.apache.impala.thrift.TPrivilege;

import java.util.HashMap;
import java.util.Map;

/**
 * Collection of static functions to support Apache Ranger implementation
 */
public class RangerUtil {
  private RangerUtil() { }

  /**
   * Creates a column resource for Ranger. Column resources also include
   * database and table information.
   */
  public static Map<String, String> createColumnResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();

    resource.put(RangerImpalaResourceBuilder.DATABASE, getOrAll(privilege.getDb_name()));
    resource.put(RangerImpalaResourceBuilder.TABLE, getOrAll(privilege.getTable_name()));
    resource.put(RangerImpalaResourceBuilder.COLUMN,
        getOrAll(privilege.getColumn_name()));

    return resource;
  }

  /**
   * Creates a URI resource for Ranger. In Ranger a URI is known as a URL.
   */
  public static Map<String, String> createUriResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();
    String uri = privilege.getUri();
    resource.put(RangerImpalaResourceBuilder.URL, uri == null ? "*" : uri);

    return resource;
  }

  /**
   * Creates a function resource for Ranger. Function resources also include
   * database information.
   */
  public static Map<String, String> createFunctionResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();

    resource.put(RangerImpalaResourceBuilder.DATABASE, getOrAll(privilege.getDb_name()));
    resource.put(RangerImpalaResourceBuilder.UDF, "*");

    return resource;
  }

  private static String getOrAll(String resource) {
    return (resource == null) ? "*" : resource;
  }
}
