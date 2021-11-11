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
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.hive.executor.HiveUdfExecutorLegacy;
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
