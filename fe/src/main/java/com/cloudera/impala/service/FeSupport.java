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
import com.cloudera.impala.thrift.TSymbolLookupParams;
import com.cloudera.impala.thrift.TSymbolLookupResult;
import com.cloudera.impala.util.NativeLibUtil;
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

  // Only called if this library is explicitly loaded. This only happens
  // when running FE tests.
  public native static void NativeFeTestInit();

  // Returns a serialized TColumnValue.
  public native static byte[] NativeEvalConstExpr(byte[] thriftExpr,
      byte[] thriftQueryGlobals);

  // Returns a serialize TSymbolLookupResult
  public native static byte[] NativeLookupSymbol(byte[] thriftSymbolLookup);

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

  private static byte[] LookupSymbol(byte[] thriftParams) {
    try {
      return NativeLookupSymbol(thriftParams);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeLookupSymbol(thriftParams);
  }

  public static TSymbolLookupResult LookupSymbol(TSymbolLookupParams params)
      throws InternalException {
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      byte[] resultBytes = LookupSymbol(serializer.serialize(params));
      Preconditions.checkNotNull(resultBytes);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TSymbolLookupResult result = new TSymbolLookupResult();
      deserializer.deserialize(result, resultBytes);
      return result;
    } catch (TException e) {
      // this should never happen
      throw new InternalException("couldn't perform symbol lookup.", e);
    }
  }

  private static byte[] EvalConstExpr(byte[] thriftExpr, byte[] thriftQueryGlobals) {
    try {
      return NativeEvalConstExpr(thriftExpr, thriftQueryGlobals);
    } catch (UnsatisfiedLinkError e) {
      // We should only get here in FE tests that dont run the BE.
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
    if (loaded) return;
    LOG.info("Loading libfesupport.so");
    NativeLibUtil.loadLibrary("libfesupport.so");
    LOG.info("Loaded libfesupport.so");
    loaded = true;
    NativeFeTestInit();
  }
}
