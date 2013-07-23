// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.service;

import java.io.File;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TQueryGlobals;
import com.google.common.base.Preconditions;

/**
 * This class provides the Impala executor functionality to the FE.
 * fe-support.cc implements all the native calls.
 * If the planner is executed inside Impalad, Impalad would have registered all the JNI
 * native functions already. There's no need to load the shared library.
 * For unit test (mvn test), load the shared library because the native function has not
 * been loaded yet.
 */
public class FeSupport {
  private final static Logger LOG = LoggerFactory.getLogger(FeSupport.class);

  private static boolean loaded = false;

  // Returns a serialized TColumnValue.
  public native static byte[] NativeEvalConstExpr(byte[] thriftExpr,
      byte[] thriftQueryGlobals);

  // Writes a log message to glog
  public native static void NativeLogger(
      int severity, String msg, String filename, int line);

  public static void LogToGlog(int severity, String msg, String filename, int line) {
    try {
      NativeLogger(severity, msg, filename, line);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
      NativeLogger(severity, msg, filename, line);
    }
  }

  public static TColumnValue EvalConstExpr(Expr expr, TQueryGlobals queryGlobals)
      throws InternalException {
    Preconditions.checkState(expr.isConstant());
    TExpr thriftExpr = expr.treeToThrift();
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    byte[] result;
    try {
      result = EvalConstExpr(serializer.serialize(thriftExpr),
          serializer.serialize(queryGlobals));
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TColumnValue val = new TColumnValue();
      deserializer.deserialize(val, result);
      return val;
    } catch (TException e) {
      // this should never happen
      throw new InternalException("couldn't execute expr " + expr.toSql(), e);
    }
  }

  private static byte[] EvalConstExpr(byte[] thriftExpr, byte[] thriftQueryGlobals) {
    try {
      return NativeEvalConstExpr(thriftExpr, thriftQueryGlobals);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeEvalConstExpr(thriftExpr, thriftQueryGlobals);
  }

  public static boolean EvalPredicate(Expr pred, TQueryGlobals queryGlobals)
      throws InternalException {
    Preconditions.checkState(pred.getType() == PrimitiveType.BOOLEAN);
    TColumnValue val = EvalConstExpr(pred, queryGlobals);
    // Return false if pred evaluated to false or NULL. True otherwise.
    return val.isSetBoolVal() && val.boolVal;
  }

  /**
   * This function should only be called explicitly by the FeSupport to ensure that
   * native functions are loaded.
   */
  private static synchronized void loadLibrary() {
    if (loaded) {
      return;
    }
    loaded = true;

    // Search for libfesupport.so in all library paths.
    String libPath = System.getProperty("java.library.path");
    String[] paths = libPath.split(":");
    boolean found = false;
    for (String path : paths) {
      String filePath = path + File.separator + "libfesupport.so";
      File libFile = new File(filePath);
      if (libFile.exists()) {
        System.load(filePath);
        found = true;
        break;
      }
    }
    if (!found) {
      System.out.println("Failed to load libfesupport.so from given java.library.paths ("
          + libPath + ").");
    }
  }
}
