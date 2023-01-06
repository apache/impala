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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFAscii;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFPI;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.thrift.TFunctionBinaryType;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;


@SuppressWarnings("restriction")
public class HiveLegacyJavaFunctionTest {
  private static final String HIVE_BUILTIN_JAR = System.getenv("HIVE_HOME") + "/" +
      "lib/hive-exec-" + System.getenv("IMPALA_HIVE_VERSION") + ".jar";

  private static final String DB = "Db";
  private static final String FN = "Fn";
  private static final String JARFILE = "JarFile";
  private static final String CLASSPATH = "ClassPath";

  public static class DummyUDF extends UDF {
    public DummyUDF() {
    }
  }

  private class ExpectedFunction {
    public final String className;
    public final ScalarType retType;
    public final List<ScalarType> paramTypes;
    public final String db;
    public final String fnName;
    public final String jarFile;
    public final String udfClassPath;

    public ExpectedFunction(Class<?> udfClass, ScalarType retType,
        List<ScalarType> paramTypes) {
      this.className = udfClass.getSimpleName();
      this.retType = retType;
      this.paramTypes = new ArrayList<>(paramTypes);
      this.db = udfClass.getSimpleName() + DB;
      this.fnName  = udfClass.getSimpleName() + FN;
      this.jarFile = udfClass.getSimpleName() + JARFILE;
      this.udfClassPath = udfClass.getSimpleName() + CLASSPATH;
    }

    public boolean matches(ScalarFunction fn) {
      if (!this.db.toLowerCase().equals(fn.dbName())) {
        return false;
      }
      if (!this.fnName.toLowerCase().equals(fn.functionName())) {
        return false;
      }
      if (!this.jarFile.equals(fn.getLocation().toString())) {
        return false;
      }
      if (!this.udfClassPath.equals(fn.getSymbolName())) {
        return false;
      }
      if (!this.retType.equals(fn.getReturnType())) {
        return false;
      }
      if (this.paramTypes.size() != fn.getArgs().length) {
        return false;
      }

      for (int i = 0; i < paramTypes.size(); ++i) {
        if (this.paramTypes.get(i) != fn.getArgs()[i]) {
          return false;
        }
      }
      return true;
    }
  }

  private class ExpectedFunctions {
    public final List<ExpectedFunction> expectedFunctions = new ArrayList<>();

    public void add(ExpectedFunction f) {
      expectedFunctions.add(f);
    }

    public int size() {
      return expectedFunctions.size();
    }

    /**
     * Check that the extracted signatures match the expected signatures.
     */
    public void checkExpectedFuncs(List<ScalarFunction> scalarFns) {
      List<ScalarFunction> scalarFnsCopy = new ArrayList<>(scalarFns);
      for (ExpectedFunction expectedFunction : expectedFunctions) {
        boolean found = false;
        for (ScalarFunction scalarFn : scalarFnsCopy) {
          if (expectedFunction.matches(scalarFn)) {
            found = true;
            scalarFnsCopy.remove(scalarFn);
            break;
          }
        }
        if (!found) {
          Assert.fail("Expected function not extracted: " + expectedFunction.retType +
              " " + expectedFunction.className + "(" +
              Joiner.on(",").join(expectedFunction.paramTypes) + ")");
        }
      }
      if (!scalarFnsCopy.isEmpty()) {
        Assert.fail("Extracted unexpected function " +
            scalarFnsCopy.get(0).getFunctionName() + " with signature: " +
            scalarFnsCopy.get(0).signatureString());
        }
      }
    }

  @Test
  public void testExtractFailNoEvaluateMethods() {
    ExpectedFunctions expectedFuncs = new ExpectedFunctions();
    Class<?> udfClass = DummyUDF.class;
    String expectedFunctionName = (udfClass.getSimpleName() + FN).toLowerCase();
    try {
      testScalar(udfClass, expectedFuncs);
    } catch (CatalogException e) {
      Assert.assertTrue(e.getMessage().contains(
          "No compatible signatures found for function: " + expectedFunctionName));
      return;
    }
    Assert.fail("Extraction should not have succeeded.");
  }

  @Test
  public void testExtractFailNotAUDF() {
    ExpectedFunctions expectedFuncs = new ExpectedFunctions();
    Class<?> udfClass = HiveLegacyJavaFunctionTest.class;
    try {
      testScalar(udfClass, expectedFuncs);
    } catch (CatalogException e) {
      Assert.assertTrue(e.getMessage().contains("Unable to cast to UDF instance."));
      return;
    }
    Assert.fail("Extraction should not have succeeded.");
  }

  @Test
  public void testExtract() throws CatalogException {
    ExpectedFunctions expectedFuncs;
    Class<?> udfClass;

    udfClass = UDFPI.class;
    expectedFuncs = new ExpectedFunctions();
    expectedFuncs.add(new ExpectedFunction(udfClass, ScalarType.DOUBLE,
        Lists.newArrayList()));
    testScalar(udfClass, expectedFuncs);

    udfClass = UDFLog.class;
    expectedFuncs = new ExpectedFunctions();
    expectedFuncs.add(new ExpectedFunction(udfClass, ScalarType.DOUBLE,
        Lists.newArrayList(ScalarType.DOUBLE)));
    expectedFuncs.add(new ExpectedFunction(udfClass, ScalarType.DOUBLE,
        Lists.newArrayList(ScalarType.DOUBLE, ScalarType.DOUBLE)));
    testScalar(udfClass, expectedFuncs);

    udfClass = UDFAscii.class;
    expectedFuncs = new ExpectedFunctions();
    expectedFuncs.add(new ExpectedFunction(udfClass, ScalarType.INT,
        Lists.newArrayList(ScalarType.STRING)));
    testScalar(udfClass, expectedFuncs);
  }

  private void testScalar(Class<?> classToTest,
      ExpectedFunctions expectedFuncs) throws CatalogException {
    ScalarFunction scalarFn = createScalarFn(classToTest);
    Function hiveFn = HiveJavaFunction.toHiveFunction(scalarFn);
    HiveJavaFunction javaFn =
        new HiveLegacyJavaFunction(classToTest, hiveFn, null, null);
    expectedFuncs.checkExpectedFuncs(javaFn.extract());
  }

  private ScalarFunction createScalarFn(Class<?> udfClass) {
    String simpleName = udfClass.getSimpleName();
    // The actual names don't matter so much. We just need to make sure that
    // the attributions of the original scalar function match the attributes
    // for all extracted functions.
    return ScalarFunction.createForTesting(
        simpleName + DB,
        simpleName + FN,
        null,
        null,
        simpleName + JARFILE,
        simpleName + CLASSPATH,
        null,
        null,
        TFunctionBinaryType.JAVA);
  }
}
