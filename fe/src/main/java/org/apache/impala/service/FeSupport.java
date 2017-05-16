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

package org.apache.impala.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableName;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TCacheJarParams;
import org.apache.impala.thrift.TCacheJarResult;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TExprBatch;
import org.apache.impala.thrift.TPrioritizeLoadRequest;
import org.apache.impala.thrift.TPrioritizeLoadResponse;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TSymbolLookupParams;
import org.apache.impala.thrift.TSymbolLookupResult;
import org.apache.impala.thrift.TTable;
import org.apache.impala.util.NativeLibUtil;
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
  private static boolean loaded_ = false;

  // Only called if this library is explicitly loaded. This only happens
  // when running FE tests.
  public native static void NativeFeTestInit();

  // Returns a serialized TResultRow
  public native static byte[] NativeEvalExprsWithoutRow(
      byte[] thriftExprBatch, byte[] thriftQueryGlobals);

  // Returns a serialized TSymbolLookupResult
  public native static byte[] NativeLookupSymbol(byte[] thriftSymbolLookup);

  // Returns a serialized TCacheJarResult
  public native static byte[] NativeCacheJar(byte[] thriftCacheJar);

  // Does an RPCs to the Catalog Server to prioritize the metadata loading of a
  // one or more catalog objects. To keep our kerberos configuration consolidated,
  // we make make all RPCs in the BE layer instead of calling the Catalog Server
  // using Java Thrift bindings.
  public native static byte[] NativePrioritizeLoad(byte[] thriftReq);

  /**
   * Locally caches the jar at the specified HDFS location.
   *
   * @param hdfsLocation The path to the jar in HDFS
   * @return The result of the call to cache the jar, includes a status and the local
   *         path of the cached jar if the operation was successful.
   */
  public static TCacheJarResult CacheJar(String hdfsLocation) throws InternalException {
    Preconditions.checkNotNull(hdfsLocation);
    TCacheJarParams params = new TCacheJarParams(hdfsLocation);
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    byte[] result;
    try {
      result = CacheJar(serializer.serialize(params));
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TCacheJarResult thriftResult = new TCacheJarResult();
      deserializer.deserialize(thriftResult, result);
      return thriftResult;
    } catch (TException e) {
      // this should never happen
      throw new InternalException(
          "Couldn't cache jar at HDFS location " + hdfsLocation, e);
    }
  }

  private static byte[] CacheJar(byte[] thriftParams) {
    try {
      return NativeCacheJar(thriftParams);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeCacheJar(thriftParams);
  }

  public static TColumnValue EvalExprWithoutRow(Expr expr, TQueryCtx queryCtx)
      throws InternalException {
    Preconditions.checkState(!expr.contains(SlotRef.class));
    TExprBatch exprBatch = new TExprBatch();
    exprBatch.addToExprs(expr.treeToThrift());
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    byte[] result;
    try {
      result = EvalExprsWithoutRow(
          serializer.serialize(exprBatch), serializer.serialize(queryCtx));
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TResultRow val = new TResultRow();
      deserializer.deserialize(val, result);
      Preconditions.checkState(val.getColValsSize() == 1);
      return val.getColVals().get(0);
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

  private static byte[] EvalExprsWithoutRow(
      byte[] thriftExprBatch, byte[] thriftQueryContext) {
    try {
      return NativeEvalExprsWithoutRow(thriftExprBatch, thriftQueryContext);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeEvalExprsWithoutRow(thriftExprBatch, thriftQueryContext);
  }

  public static boolean EvalPredicate(Expr pred, TQueryCtx queryCtx)
      throws InternalException {
    // Shortcuts to avoid expensive BE evaluation.
    if (pred instanceof BoolLiteral) return ((BoolLiteral) pred).getValue();
    if (pred instanceof NullLiteral) return false;
    Preconditions.checkState(pred.getType().isBoolean());
    TColumnValue val = EvalExprWithoutRow(pred, queryCtx);
    // Return false if pred evaluated to false or NULL. True otherwise.
    return val.isBool_val() && val.bool_val;
  }

  /**
   * Evaluate a batch of predicates in the BE. The results are stored in a
   * TResultRow object, where each TColumnValue in it stores the result of
   * a predicate evaluation.
   *
   * TODO: This function is currently used for improving the performance of
   * partition pruning (see IMPALA-887), hence it only supports boolean
   * exprs. In the future, we can extend it to support arbitrary exprs without
   * SlotRefs.
   */
  public static TResultRow EvalPredicateBatch(ArrayList<Expr> exprs,
      TQueryCtx queryCtx) throws InternalException {
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    TExprBatch exprBatch = new TExprBatch();
    for (Expr expr: exprs) {
      // Make sure we only process boolean exprs.
      Preconditions.checkState(expr.getType().isBoolean());
      Preconditions.checkState(!expr.contains(SlotRef.class));
      exprBatch.addToExprs(expr.treeToThrift());
    }
    byte[] result;
    try {
      result = EvalExprsWithoutRow(
          serializer.serialize(exprBatch), serializer.serialize(queryCtx));
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TResultRow val = new TResultRow();
      deserializer.deserialize(val, result);
      return val;
    } catch (TException e) {
      // this should never happen
      throw new InternalException("couldn't execute a batch of exprs.", e);
    }
  }

  private static byte[] PrioritizeLoad(byte[] thriftReq) {
    try {
      return NativePrioritizeLoad(thriftReq);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativePrioritizeLoad(thriftReq);
  }

  public static TStatus PrioritizeLoad(Set<TableName> tableNames)
      throws InternalException {
    Preconditions.checkNotNull(tableNames);

    List<TCatalogObject> objectDescs = new ArrayList<TCatalogObject>(tableNames.size());
    for (TableName tableName: tableNames) {
      TCatalogObject catalogObject = new TCatalogObject();
      catalogObject.setType(TCatalogObjectType.TABLE);
      catalogObject.setTable(new TTable(tableName.getDb(), tableName.getTbl()));
      objectDescs.add(catalogObject);
    }

    TPrioritizeLoadRequest request = new TPrioritizeLoadRequest ();
    request.setHeader(new TCatalogServiceRequestHeader());
    request.setObject_descs(objectDescs);

    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      byte[] result = PrioritizeLoad(serializer.serialize(request));
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TPrioritizeLoadResponse response = new TPrioritizeLoadResponse();
      deserializer.deserialize(response, result);
      return response.getStatus();
    } catch (TException e) {
      // this should never happen
      throw new InternalException("Error processing request: " + e.getMessage(), e);
    }
  }

  /**
   * This function should only be called explicitly by the FeSupport to ensure that
   * native functions are loaded.
   */
  private static synchronized void loadLibrary() {
    if (loaded_) return;
    LOG.info("Loading libfesupport.so");
    NativeLibUtil.loadLibrary("libfesupport.so");
    LOG.info("Loaded libfesupport.so");
    loaded_ = true;
    NativeFeTestInit();
  }
}
