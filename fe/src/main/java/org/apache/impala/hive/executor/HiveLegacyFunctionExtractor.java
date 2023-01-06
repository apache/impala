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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.log4j.Logger;

public class HiveLegacyFunctionExtractor {
  private static final Logger LOG = Logger.getLogger(HiveLegacyFunctionExtractor.class);

  public ScalarFunction extract(Function function, Method method) {
    Class<?> returnType = method.getReturnType();
    Class<?>[] fnArgs = method.getParameterTypes();
    String className = function.getClassName();

    List<Type> fnArgsList;
    ScalarType retType;
    try {
      fnArgsList = resolveParameterTypes(Arrays.asList(fnArgs));
      retType = resolveReturnType(returnType);
    } catch (ImpalaException exception) {
      LOG.debug("Processing " + className + ": " + exception.getMessage());
      return null;
    }

    HdfsUri location = null;
    List<ResourceUri> functionResources = function.getResourceUris();
    if (functionResources != null) {
      ResourceUri jarLocation = function.getResourceUris().get(0);
      location = new HdfsUri(jarLocation.getUri());
    }

    ScalarFunction fn = new ScalarFunction(
        new FunctionName(function.getDbName(), function.getFunctionName()), fnArgsList,
        retType, location, className, null, null);
    fn.setHasVarArgs(false);
    fn.setBinaryType(TFunctionBinaryType.JAVA);
    fn.setIsPersistent(true);
    return fn;
  }

  private List<Type> resolveParameterTypes(List<Class<?>> arguments)
      throws ImpalaException {
    List<Type> resolvedTypes = new ArrayList<>();
    for (Class<?> argument : arguments) {
      resolvedTypes.add(resolveParameterType(argument));
    }
    return resolvedTypes;
  }

  private ScalarType resolveParameterType(Class<?> param) throws ImpalaException {
    return resolveType(param, type -> "Param type " + type + " not supported");
  }

  private ScalarType resolveReturnType(Class<?> returnType) throws ImpalaException {
    return resolveType(returnType, type -> "Return type " + type + " not supported");
  }

  protected ScalarType resolveType(
      Class<?> type, java.util.function.Function<JavaUdfDataType, String> errorHandler)
      throws ImpalaException {
    JavaUdfDataType javaRetType = JavaUdfDataType.getType(type);
    if (javaRetType == JavaUdfDataType.INVALID_TYPE) {
      throw new CatalogException(errorHandler.apply(javaRetType));
    }
    return ScalarType.createType(
        PrimitiveType.fromThrift(javaRetType.getPrimitiveType()));
  }
}
