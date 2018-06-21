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

package org.apache.impala.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.hive.executor.UdfExecutor;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public abstract class FunctionUtils {
  public static final Logger LOG = Logger.getLogger(FunctionUtils.class);

  public static final FunctionResolutionOrder FUNCTION_RESOLUTION_ORDER =
      new FunctionResolutionOrder();

  /**
   * Returns a list of Impala Functions, one per compatible "evaluate" method in the UDF
   * class referred to by the given Java function. This method copies the UDF Jar
   * referenced by "function" to a temporary file in localLibraryPath_ and loads it
   * into the jvm. Then we scan all the methods in the class using reflection and extract
   * those methods and create corresponding Impala functions. Currently Impala supports
   * only "JAR" files for symbols and also a single Jar containing all the dependent
   * classes rather than a set of Jar files.
   */
  public static List<Function> extractFunctions(String db,
      org.apache.hadoop.hive.metastore.api.Function function,
      String localLibPath)
      throws ImpalaRuntimeException{
    List<Function> result = Lists.newArrayList();
    List<String> addedSignatures = Lists.newArrayList();
    StringBuilder warnMessage = new StringBuilder();
    if (!FunctionUtils.isFunctionCompatible(function, warnMessage)) {
      LOG.warn("Skipping load of incompatible function: " +
          function.getFunctionName() + ". " + warnMessage.toString());
      return result;
    }
    String jarUri = function.getResourceUris().get(0).getUri();
    Class<?> udfClass = null;
    Path localJarPath = null;
    try {
      // TODO(todd): cache these jars based on the mtime and file ID of the
      // remote JAR? Can we share a cache with the backend?
      localJarPath = new Path("file://" + localLibPath,
          UUID.randomUUID().toString() + ".jar");
      try {
        FileSystemUtil.copyToLocal(new Path(jarUri), localJarPath);
      } catch (IOException e) {
        String errorMsg = "Error loading Java function: " + db + "." +
            function.getFunctionName() + ". Couldn't copy " + jarUri +
            " to local path: " + localJarPath.toString();
        LOG.error(errorMsg, e);
        throw new ImpalaRuntimeException(errorMsg);
      }
      URL[] classLoaderUrls = new URL[] {new URL(localJarPath.toString())};
      try (URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls)) {
        udfClass = urlClassLoader.loadClass(function.getClassName());
        // Check if the class is of UDF type. Currently we don't support other functions
        // TODO: Remove this once we support Java UDAF/UDTF
        if (org.apache.hadoop.hive.ql.exec.FunctionUtils.getUDFClassType(udfClass) !=
            org.apache.hadoop.hive.ql.exec.FunctionUtils.UDFClassType.UDF) {
          LOG.warn("Ignoring load of incompatible Java function: " +
              function.getFunctionName() + " as " +
              org.apache.hadoop.hive.ql.exec.FunctionUtils.getUDFClassType(udfClass)
              + " is not a supported type. Only UDFs are supported");
          return result;
            }
        // Load each method in the UDF class and create the corresponding Impala Function
        // object.
        for (Method m: udfClass.getMethods()) {
          if (!m.getName().equals(UdfExecutor.UDF_FUNCTION_NAME)) continue;
          Function fn = ScalarFunction.fromHiveFunction(db,
              function.getFunctionName(), function.getClassName(),
              m.getParameterTypes(), m.getReturnType(), jarUri);
          if (fn == null) {
            LOG.warn("Ignoring incompatible method: " + m.toString() + " during load of "
                + "Hive UDF:" + function.getFunctionName() + " from " + udfClass);
            continue;
          }
          if (!addedSignatures.contains(fn.signatureString())) {
            result.add(fn);
            addedSignatures.add(fn.signatureString());
          }
        }
      }
    } catch (ClassNotFoundException c) {
      String errorMsg = "Error loading Java function: " + db + "." +
          function.getFunctionName() + ". Symbol class " + function.getClassName() +
          " not found in Jar: " + jarUri;
      LOG.error(errorMsg);
      throw new ImpalaRuntimeException(errorMsg, c);
    } catch (Exception e) {
      LOG.error("Skipping function load: " + function.getFunctionName(), e);
      throw new ImpalaRuntimeException("Error extracting functions", e);
    } catch (LinkageError e) {
      String errorMsg = "Error resolving dependencies for Java function: " + db + "." +
          function.getFunctionName();
      LOG.error(errorMsg);
      throw new ImpalaRuntimeException(errorMsg, e);
    } finally {
      if (localJarPath != null) FileSystemUtil.deleteIfExists(localJarPath);
    }
    return result;
  }

  public static List<Function> deserializeNativeFunctionsFromDbParams(
      Map<String, String> dbParams) {
    List<Function> results = Lists.newArrayList();
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
    for (Map.Entry<String, String> entry: dbParams.entrySet()) {
      if (!entry.getKey().startsWith(Db.FUNCTION_INDEX_PREFIX)) continue;
      try {
        TFunction fn = new TFunction();
        JniUtil.deserializeThrift(protocolFactory, fn,
            Base64.getDecoder().decode(entry.getValue()));
        results.add(Function.fromThrift(fn));
      } catch (ImpalaException e) {
        LOG.error("Encountered an error during function load: key=" +
            entry.getKey() + ",continuing", e);
      }
    }
    return results;
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
   private static boolean isFunctionCompatible(
       org.apache.hadoop.hive.metastore.api.Function fn, StringBuilder incompatMsg) {
    boolean isCompatible = true;
    if (fn.getFunctionType() != FunctionType.JAVA) {
      isCompatible = false;
      incompatMsg.append("Function type: " + fn.getFunctionType().name()
          + " is not supported. Only " + FunctionType.JAVA.name() + " functions "
          + "are supported.");
    } else if (fn.getResourceUrisSize() == 0) {
      isCompatible = false;
      incompatMsg.append("No executable binary resource (like a JAR file) is " +
          "associated with this function. To fix this, recreate the function by " +
          "specifying a 'location' in the function create statement.");
    } else if (fn.getResourceUrisSize() != 1) {
      isCompatible = false;
      List<String> resourceUris = Lists.newArrayList();
      for (ResourceUri resource: fn.getResourceUris()) {
        resourceUris.add(resource.getUri());
      }
      incompatMsg.append("Impala does not support multiple Jars for dependencies."
          + "(" + Joiner.on(",").join(resourceUris) + ") ");
    } else if (fn.getResourceUris().get(0).getResourceType() != ResourceType.JAR) {
      isCompatible = false;
      incompatMsg.append("Function binary type: " +
        fn.getResourceUris().get(0).getResourceType().name()
        + " is not supported. Only " + ResourceType.JAR.name()
        + " type is supported.");
    }
    return isCompatible;
  }

  public static Function resolveFunction(Iterable<Function> fns, Function desc,
      CompareMode mode) {
    Preconditions.checkNotNull(fns);
    Preconditions.checkNotNull(desc);
    Preconditions.checkNotNull(mode);

    // First check for identical
    Function f = getBestFitFunction(fns, desc, Function.CompareMode.IS_IDENTICAL);
    if (f != null || mode == Function.CompareMode.IS_IDENTICAL) return f;

    // Next check for indistinguishable
    f = getBestFitFunction(fns, desc, Function.CompareMode.IS_INDISTINGUISHABLE);
    if (f != null || mode == Function.CompareMode.IS_INDISTINGUISHABLE) return f;

    // Next check for strict supertypes
    f = getBestFitFunction(fns, desc, Function.CompareMode.IS_SUPERTYPE_OF);
    if (f != null || mode == Function.CompareMode.IS_SUPERTYPE_OF) return f;

    // Finally check for non-strict supertypes
    return getBestFitFunction(fns, desc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
  }

  private static Function getBestFitFunction(Iterable<Function> fns, Function desc,
      CompareMode mode) {
    int maxScore = -1;
    Function maxFunc = null;
    for (Function f: fns) {
      int score = f.calcMatchScore(desc, mode);
      if (score >= 0 && score > maxScore) {
        maxScore = score;
        maxFunc = f;
      }
    }
    return maxFunc;
  }

  public static List<Function> getVisibleFunctionsInCategory(
      Iterable<Function> candidates, TFunctionCategory category) {
    List<Function> result = Lists.newArrayList();
    for (Function fn: candidates) {
      if (fn.userVisible() && Function.categoryMatch(fn, category)) {
        result.add(fn);
      }
    }
    return result;
  }

  public static List<Function> getVisibleFunctions(Iterable<Function> candidates) {
    List<Function> result = Lists.newArrayList();
    for (Function fn: candidates) {
      if (fn.userVisible()) result.add(fn);
    }
    return result;
  }

  /**
   * Comparator that sorts function overloads. We want overloads to be always considered
   * in a canonical order so that overload resolution in the case of multiple valid
   * overloads does not depend on the order in which functions are added to the Db. The
   * order is based on the PrimitiveType enum because this was the order used implicitly
   * for builtin operators and functions in earlier versions of Impala.
   */
  public static class FunctionResolutionOrder implements Comparator<Function> {
    @Override
    public int compare(Function f1, Function f2) {
      int numSharedArgs = Math.min(f1.getNumArgs(), f2.getNumArgs());
      for (int i = 0; i < numSharedArgs; ++i) {
        int cmp = typeCompare(f1.getArgs()[i], f2.getArgs()[i]);
        if (cmp < 0) {
          return -1;
        } else if (cmp > 0) {
          return 1;
        }
      }
      // Put alternative with fewer args first.
      if (f1.getNumArgs() < f2.getNumArgs()) {
        return -1;
      } else if (f1.getNumArgs() > f2.getNumArgs()) {
        return 1;
      }
      return 0;
    }

    private int typeCompare(Type t1, Type t2) {
      Preconditions.checkState(!t1.isComplexType());
      Preconditions.checkState(!t2.isComplexType());
      return Integer.compare(t1.getPrimitiveType().ordinal(),
          t2.getPrimitiveType().ordinal());
    }
  }
}
