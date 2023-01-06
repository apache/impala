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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HiveLegacyJavaFunction generates the instance of the UDF object given
 * a className. If a return type is passed in, it will check that the types are valid
 * for the UDF. The "extract" method can be used off of the UDF class to find all
 * methods named "evaluate()" that use supported param types and return types using
 * Java reflection.
 */
public class HiveLegacyJavaFunction implements HiveJavaFunction {
  private static final Logger LOG = LoggerFactory.getLogger(HiveLegacyJavaFunction.class);

  // By convention, the function in the class must be called evaluate()
  private static final String UDF_FUNCTION_NAME = "evaluate";

  private final Function hiveFn_;

  private final UDF UDF_;

  private final Type retType_;

  private final Type[] parameterTypes_;

  // Method of the function in the UDF class. If no retType is supplied, this will be
  // null (and presumably be used for extraction of methods).
  private final Method method_;

  public HiveLegacyJavaFunction(Class<?> udfClass, Function hiveFn,
      Type retType, Type[] parameterTypes) throws CatalogException {
    try {
      hiveFn_ = hiveFn;
      retType_ = retType;
      parameterTypes_ = parameterTypes;
      UDF_ = instantiateUDFInstance(udfClass);
      method_ = (retType != null && retType != ScalarType.INVALID)
          ? getMatchingMethod(udfClass) : null;
    } catch (CatalogException e) {
      String errorMsg = "Error retrieving class " + udfClass + ": " + e.getMessage();
      throw new CatalogException(errorMsg, e);
    }
  }

  public HiveLegacyJavaFunction(Class<?> udfClass,
      Type retType, Type[] parameterTypes) throws CatalogException {
    this(udfClass, null, retType, parameterTypes);
  }

  @Override
  public Function getHiveFunction() {
    return hiveFn_;
  }

  public Method getMethod() {
    return method_;
  }

  public Class<?> getRetType() {
    return method_.getReturnType();
  }

  public Class<?>[] getParameterTypes() {
    return method_.getParameterTypes();
  }

  public UDF getUDFInstance() {
    return UDF_;
  }

  /**
   * Instantiate and return the class given by udfClass.
   */
  private UDF instantiateUDFInstance(Class<?> udfClass)
      throws CatalogException {
    try {
      Constructor<?> ctor = udfClass.getConstructor();
      return (UDF) ctor.newInstance();
    } catch (NoSuchMethodException e) {
      throw new CatalogException(
          "Unable to find constructor with no arguments.", e);
    } catch (IllegalArgumentException e) {
      throw new CatalogException(
          "Unable to call UDF constructor with no arguments.", e);
    } catch (InstantiationException e) {
      throw new CatalogException("Unable to call create UDF instance.", e);
    } catch (IllegalAccessException e) {
      throw new CatalogException("Unable to call create UDF instance.", e);
    } catch (InvocationTargetException e) {
      throw new CatalogException("Unable to call create UDF instance.", e);
    } catch (ClassCastException e) {
      throw new CatalogException(
          "Unable to cast to UDF instance.", e);
    }
  }
  /**
   * Returns a list of Impala Functions, one per compatible "evaluate" method in the UDF
   * class referred to by the given Java function. This method copies the UDF Jar
   * referenced in the function definition to a temporary file in localLibraryPath_ and
   * loads it into the jvm. Then we scan all the methods in the class using reflection and
   * extract those methods and create corresponding Impala functions. Currently, Impala
   * supports only "JAR" files for symbols and also a single Jar containing all the
   * dependent classes rather than a set of Jar files.
   */
  @Override
  public List<ScalarFunction> extract(HiveLegacyFunctionExtractor extractor)
      throws CatalogException {
    Set<String> addedSignatures = new HashSet<>();
    List<ScalarFunction> result = new ArrayList<>();
    // Load each method in the UDF class and create the corresponding Impala Function
    // object.
    try {
      for (Method m: UDF_.getClass().getMethods()) {
        if (m.getName().equals(UDF_FUNCTION_NAME)) {
          ScalarFunction fn = extractor.extract(hiveFn_, m);
          if (fn != null) {
            if (!addedSignatures.contains(fn.signatureString())) {
              result.add(fn);
              addedSignatures.add(fn.signatureString());
            }
          } else {
            LOG.warn(
                "Ignoring incompatible method: {} during load of Hive UDF: {} from {}", m,
                hiveFn_.getFunctionName(), UDF_.getClass());
          }
        }
      }
    } catch (Throwable t) {
      // Catch all runtime exceptions here. One possible runtime exception that can occur
      // is ClassNotFoundException thrown by UDF_.getClass(). We want to catch all
      // possible exceptions, mark it as a CatalogException, and let the caller decide on
      // how to handle it.
      throw new CatalogException("Error loading function " + hiveFn_.getFunctionName() +
          ":  " + t);
    }
    if (result.isEmpty()) {
      throw new CatalogException(
          "No compatible signatures found for function: " + hiveFn_.getFunctionName());
    }
    return result;
  }

  private Method getMatchingMethod(Class<?> udfClass) throws CatalogException {
    for (Method m : udfClass.getMethods()) {
      if (methodMatches(m, retType_, parameterTypes_)) {
        return m;
      }
    }
    throw new CatalogException(
        getExceptionString(udfClass.getMethods(), udfClass.toString(),
        parameterTypes_));
  }

  private static boolean methodMatches(Method m, Type retType,
      Type[] parameterTypes) {
    if (!m.getName().equals(UDF_FUNCTION_NAME)) {
      return false;
    }

    // Check if the evaluate method return type is compatible with the return type from
    // the function definition. This happens when both of them map to the same primitive
    // type.
    JavaUdfDataType javaRetType = JavaUdfDataType.getType(m.getReturnType());
    if (!javaRetType.isCompatibleWith(retType.getPrimitiveType().toThrift())) {
      return false;
    }

    // Try to match the arguments
    if (m.getParameterTypes().length != parameterTypes.length) {
      return false;
    }

    for (int i = 0; i < m.getParameterTypes().length; ++i) {
      JavaUdfDataType javaArgType =
          JavaUdfDataType.getType(m.getParameterTypes()[i]);
      if (!javaArgType.isCompatibleWith(
          parameterTypes[i].getPrimitiveType().toThrift())) {
        return false;
      }
    }
    return true;
  }

  public static String getExceptionString(Method[] methods,
      String className, Type[] parameterTypes) {
    List<String> signatures = new ArrayList<>();
    for (Method m : methods) {
      // only include "evaluate" methods
      if (m.getName().equals(UDF_FUNCTION_NAME)) {
        signatures.add(m.toGenericString());
      }
    }
    StringBuilder sb = new StringBuilder();
    sb.append("Unable to find evaluate function with the correct signature: ")
      .append(className + ".evaluate(")
      .append(Joiner.on(", ").join(parameterTypes))
      .append(")\n")
      .append("UDF contains: \n    ")
      .append(Joiner.on("\n    ").join(signatures));
    return sb.toString();
  }
}
