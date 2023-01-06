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

package org.apache.impala.hive.executor;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.thrift.TFunctionBinaryType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Interface to represent a UDF defined by Hive. Hive has different
 * UDFClassTypes which can be found in their UDFClassType definition.
 */
public interface HiveJavaFunction {

  /**
   * Extract all the supported ScalarFunction objects from the Hive Java
   * function.
   */
  public default List<ScalarFunction> extract() throws CatalogException {
      return extract(new HiveLegacyFunctionExtractor());
  }

  public List<ScalarFunction> extract(HiveLegacyFunctionExtractor extractor)
      throws CatalogException;

  /**
   * Get the Hive "Function" object declared by the Hive metastore API.
   */
  public Function getHiveFunction();

  /**
   * Helper function to convert an Impala function object into a Hive metastore
   * API function object.
   */
  public static Function toHiveFunction(ScalarFunction scalarFn) {
    Preconditions.checkState(scalarFn.getBinaryType() == TFunctionBinaryType.JAVA);
    List<ResourceUri> resources = Lists.newArrayList(new ResourceUri(ResourceType.JAR,
        scalarFn.getLocation().toString()));
    return new Function(scalarFn.functionName(), scalarFn.dbName(),
        scalarFn.getSymbolName(), "", PrincipalType.USER,
        (int) (System.currentTimeMillis() / 1000),
        FunctionType.JAVA, resources);
  }
  public static Function createHiveFunction(
      String fnName, String dbName, String symbolName, List<ResourceUri> resources) {
    return new Function(fnName, dbName, symbolName, "", PrincipalType.USER,
        (int) (System.currentTimeMillis() / 1000), FunctionType.JAVA, resources);
  }
}
