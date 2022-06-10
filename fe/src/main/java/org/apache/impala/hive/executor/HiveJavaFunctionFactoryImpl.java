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
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory class to create the HiveJavaFunction instance.
 */
public class HiveJavaFunctionFactoryImpl implements HiveJavaFunctionFactory {
  /**
   * The local path contains a directory on the local file system to which the
   * jar file on hdfs can be copied.
   */
  public HiveJavaFunction create(String localLibPath, Function hiveFn,
      Type retType, Type[] paramTypes) throws CatalogException {
    checkValidFunction(hiveFn);
    String jarUri = hiveFn.getResourceUris().get(0).getUri();
    String fnName = hiveFn.getDbName() + "." + hiveFn.getFunctionName();
    try (HiveUdfLoader javaClass
        = HiveUdfLoader.createWithLocalPath(localLibPath, hiveFn)) {
      switch (javaClass.getUDFClassType()) {
        case UDF:
          return new HiveLegacyJavaFunction(javaClass.getUDFClass(), hiveFn, retType,
              paramTypes);
        case GENERIC_UDF:
          return new HiveGenericJavaFunction(javaClass.getUDFClass(), hiveFn, retType,
              paramTypes);
        default:
          throw new CatalogException("Function " + fnName + ": The class "
              + jarUri + " does not derive "
              + "from a known supported Hive UDF class (UDF).");
      }
    }
  }

  public HiveJavaFunction create(String localLibPath,
      ScalarFunction fn) throws CatalogException {
    if (fn.hasVarArgs()) {
      throw new CatalogException("Variable arguments not supported in Hive UDFs.");
    }
    return create(localLibPath, HiveJavaFunction.toHiveFunction((ScalarFunction) fn),
        fn.getReturnType(), fn.getArgs());
  }

  public HiveJavaFunction create(String localLibPath, Function hiveFn)
      throws CatalogException {
    return create(localLibPath, hiveFn, null, null);
  }

  /**
   * Checks if the Hive function 'fn' is Impala compatible. A function is Impala
   * compatible iff
   *
   * 1. The function is JAVA based,
   * 2. Has exactly one binary resource associated (We don't support loading
   *    dependencies yet) and
   * 3. The binary is of type JAR.
   *
   * Returns true if compatible and false otherwise. In case of incompatible
   * functions 'incompatMsg' has the reason for the incompatibility.
   * */
   private void checkValidFunction(Function fn) throws CatalogException {
    String errorPrefix = "Skipping load of incompatible function: " +
        fn.getFunctionName() + ". ";
    if (fn.getFunctionType() != FunctionType.JAVA) {
      throw new CatalogException(errorPrefix + "Function type: " +
          fn.getFunctionType().name() + " is not supported. Only " +
          FunctionType.JAVA.name() + " functions are supported.");
    }
    if (fn.getResourceUrisSize() == 0) {
      throw new CatalogException(errorPrefix + "No executable binary resource "
          + "(like a JAR file) is associated with this function. To fix this, recreate "
          + "the function by specifying a 'location' in the function create statement.");
    }
    if (fn.getResourceUrisSize() != 1) {
      List<String> resourceUris = new ArrayList<>();
      for (ResourceUri resource: fn.getResourceUris()) {
        resourceUris.add(resource.getUri());
      }
      throw new CatalogException(errorPrefix + "Impala does not support multiple "
          + "Jars for dependencies. (" + Joiner.on(",").join(resourceUris) + ") ");
    }
    if (fn.getResourceUris().get(0).getResourceType() != ResourceType.JAR) {
      throw new CatalogException(errorPrefix + "Function binary type: " +
        fn.getResourceUris().get(0).getResourceType().name()
        + " is not supported. Only " + ResourceType.JAR.name()
        + " type is supported.");
    }
  }
}
