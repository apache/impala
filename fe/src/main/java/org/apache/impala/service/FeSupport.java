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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableName;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TAddressesList;
import org.apache.impala.thrift.TCacheJarParams;
import org.apache.impala.thrift.TCacheJarResult;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TExprBatch;
import org.apache.impala.thrift.TGetNullPartitionNameRequest;
import org.apache.impala.thrift.TGetNullPartitionNameResponse;
import org.apache.impala.thrift.TGetPartitionStatsRequest;
import org.apache.impala.thrift.TGetPartitionStatsResponse;
import org.apache.impala.thrift.TParseDateStringResult;
import org.apache.impala.thrift.TPrioritizeLoadRequest;
import org.apache.impala.thrift.TPrioritizeLoadResponse;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TSymbolLookupParams;
import org.apache.impala.thrift.TSymbolLookupResult;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.NativeLibUtil;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

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
  private static boolean externalFE_ = false;

  // Only called if this library is explicitly loaded. This happens
  // when running FE tests or external FE
  public native static void NativeFeInit(boolean externalFE);

  // Returns a serialized TResultRow
  public native static byte[] NativeEvalExprsWithoutRow(
      byte[] thriftExprBatch, byte[] thriftQueryGlobals, long maxResultSize);

  // Returns a serialized TSymbolLookupResult
  public native static byte[] NativeLookupSymbol(byte[] thriftSymbolLookup);

  // Returns a serialized TCacheJarResult
  public native static byte[] NativeCacheJar(byte[] thriftCacheJar);

  // Adds a topic item to the backend's pending metadata-topic update.
  // 'serializationBuffer' is a serialized TCatalogObject.
  // Returns the actual value size and -1 if the operation fails.
  public native static int NativeAddPendingTopicItem(long nativeCatalogServerPtr,
      String key, long version, byte[] serializationBuffer, boolean deleted);

  // Get a catalog object update from the backend. A pair of isDeletion flag and
  // serialized TCatalogObject is returned.
  public native static Pair<Boolean, ByteBuffer> NativeGetNextCatalogObjectUpdate(
      long nativeIteratorPtr);

  // The return value is true if the operation succeeds and false otherwise.
  public native static boolean NativeLibCacheSetNeedsRefresh(String hdfsLocation);
  public native static boolean NativeLibCacheRemoveEntry(String hdfsLibFile);

  // The two methods below make RPCs to the Catalog Server. To keep our kerberos
  // configuration consolidated, we make make all RPCs in the BE layer instead of calling
  // the Catalog Server using Java Thrift bindings.

  // Does an RPC to the Catalog Server to prioritize the metadata loading of a
  // one or more catalog objects.
  public native static byte[] NativePrioritizeLoad(byte[] thriftReq);

  public native static byte[] NativeGetPartialCatalogObject(byte[] thriftReq)
      throws InternalException;

  // Does an RPC to the Catalog Server to fetch specified table partition statistics.
  public native static byte[] NativeGetPartitionStats(byte[] thriftReq);

  public native static byte[] NativeUpdateTableUsage(byte[] thriftReq);

  // Parses a string of comma-separated key=value query options ('csvQueryOptions'),
  // updates the existing query options ('queryOptions') with them and returns the
  // resulting serialized TQueryOptions object.
  // A note about the function's interface: ideally we wouldn't have to pass in the
  // existing query options. We could just return the newly set query options to the
  // caller and let the caller update the existing query options with the new ones.
  // Unfortunately due to a bug in the thrift-generated TQueryOptions class, in some cases
  // it is impossible to figure out whether a query option has been set explicitly or left
  // at its default setting, therefore this approach would not work.
  public native static byte[] NativeParseQueryOptions(String csvQueryOptions,
      byte[] queryOptions);

  public native static int MinLogSpaceForBloomFilter(long ndv, double fpp);
  public native static double FalsePositiveProbForBloomFilter(
      long ndv, int logBufferpoolSpace);

  // Parses date string, verifies if it is valid and returns the resulting
  // TParseDateStringResult object. Different date string variations are accepted.
  // E.g.: '2011-01-01', '2011-01-1', '2011-1-01', '2011-01-01'.
  public native static byte[] nativeParseDateString(String date);

  // Does an RPC to the Catalog Server to get the null partition name.
  public native static byte[] NativeGetNullPartitionName(byte[] thriftReq);

  // Does an RPC to the Catalog Server to get the latest compactions.
  public native static byte[] NativeGetLatestCompactions(byte[] thriftReq);

  // Get a list of addresses for coordinators.
  public native static byte[] NativeGetCoordinators();

  // Get the number of live queries.
  public native static long NativeNumLiveQueries();

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
    byte[] result;
    try {
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
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

  /**
   * If it is known that the size of the evaluated expression is fixed, e.g.,
   * the size of an integer, then this method will be called to perform the evaluation.
   * Otherwise, the method EvalExprWithoutRowBounded that takes an additional argument
   * specifying the upper bound on the size of evaluated expression should be invoked.
   */
  public static TColumnValue EvalExprWithoutRow(Expr expr, TQueryCtx queryCtx)
    throws InternalException {
    return EvalExprWithoutRowBounded(expr, queryCtx, 0);
  }

  public static TColumnValue EvalExprWithoutRowBounded(Expr expr, TQueryCtx queryCtx,
    int maxResultSize) throws InternalException {
    Preconditions.checkState(!expr.contains(SlotRef.class));
    TExprBatch exprBatch = new TExprBatch();
    exprBatch.addToExprs(expr.treeToThrift());
    byte[] result;
    try {
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
      result = EvalExprsWithoutRowBounded(
          serializer.serialize(exprBatch), serializer.serialize(queryCtx), maxResultSize);
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TResultRow val = new TResultRow();
      deserializer.deserialize(val, result);
      if (val.getColValsSize() != 1) {
        throw new IllegalStateException(String.format("Illegal expr eval result. " +
            "Expr=%s\nTExpBatch=%s\nResult=%s", expr.toSql(), exprBatch, val));
      }
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
    try {
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
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

  /**
   * If it is known that the size of the evaluated expression is fixed, e.g.,
   * the size of an integer, then this method will be called to perform the evaluation.
   * Otherwise, the method EvalExprsWithoutRowBounded that takes an additional argument
   * specifying the upper bound on the size of rewritten expression should be invoked.
   */
  private static byte[] EvalExprsWithoutRow(
      byte[] thriftExprBatch, byte[] thriftQueryContext) {
    return EvalExprsWithoutRowBounded(thriftExprBatch, thriftQueryContext, 0);
  }

  private static byte[] EvalExprsWithoutRowBounded(
      byte[] thriftExprBatch, byte[] thriftQueryContext, int maxResultSize) {
    try {
      return NativeEvalExprsWithoutRow(thriftExprBatch, thriftQueryContext,
        maxResultSize);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeEvalExprsWithoutRow(thriftExprBatch, thriftQueryContext, maxResultSize);
  }

  public static boolean EvalPredicate(Expr pred, TQueryCtx queryCtx)
      throws InternalException {
    // Shortcuts to avoid expensive BE evaluation.
    if (pred instanceof BoolLiteral) return ((BoolLiteral) pred).getValue();
    if (Expr.IS_NULL_LITERAL.apply(pred)) return false;
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
  public static TResultRow EvalPredicateBatch(List<Expr> exprs,
      TQueryCtx queryCtx) throws InternalException {
    TExprBatch exprBatch = new TExprBatch();
    for (Expr expr: exprs) {
      // Make sure we only process boolean exprs.
      Preconditions.checkState(expr.getType().isBoolean());
      Preconditions.checkState(!expr.contains(SlotRef.class));
      exprBatch.addToExprs(expr.treeToThrift());
    }
    byte[] result;
    try {
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
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

  public static void PrioritizeLoad(Set<TableName> tableNames,
      @Nullable TUniqueId queryId) throws InternalException {
    Preconditions.checkNotNull(tableNames);

    LOG.info(String.format("Requesting prioritized load of table(s): %s",
        Joiner.on(", ").join(tableNames)));

    List<TCatalogObject> objectDescs = new ArrayList<TCatalogObject>(tableNames.size());
    for (TableName tableName: tableNames) {
      TCatalogObject catalogObject = new TCatalogObject();
      catalogObject.setType(TCatalogObjectType.TABLE);
      catalogObject.setTable(new TTable(tableName.getDb(), tableName.getTbl()));
      objectDescs.add(catalogObject);
    }

    TPrioritizeLoadRequest request = new TPrioritizeLoadRequest();
    request.setHeader(new TCatalogServiceRequestHeader());
    request.header.setQuery_id(queryId);
    request.setObject_descs(objectDescs);

    try {
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
      byte[] result = PrioritizeLoad(serializer.serialize(request));
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TPrioritizeLoadResponse response = new TPrioritizeLoadResponse();
      deserializer.deserialize(response, result);
      if (response.getStatus().getStatus_code() != TErrorCode.OK) {
        throw new InternalException("Error requesting prioritized load: " +
            Joiner.on("\n").join(response.getStatus().getError_msgs()));
      }
    } catch (TException e) {
      // this should never happen
      throw new InternalException("Error processing request: " + e.getMessage(), e);
    }
  }

  private static byte[] GetPartitionStats(byte[] thriftReq) {
    try {
      return NativeGetPartitionStats(thriftReq);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeGetPartitionStats(thriftReq);
  }

  public static TGetPartitionStatsResponse GetPartitionStats(
      TableName table) throws InternalException {
    Preconditions.checkNotNull(table);

    LOG.info("Fetching partition statistics for table {} from catalog.", table);

    TGetPartitionStatsRequest request = new TGetPartitionStatsRequest();
    request.setTable_name(table.toThrift());
    TGetPartitionStatsResponse response = new TGetPartitionStatsResponse();
    try {
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
      byte[] result = GetPartitionStats(serializer.serialize(request));
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

      deserializer.deserialize(response, result);
      if (response.getStatus().getStatus_code() != TErrorCode.OK) {
        throw new InternalException("Error requesting GetPartitionStats: "
            + Joiner.on("\n").join(response.getStatus().getError_msgs()));
      }
    } catch (TException e) {
      // this should never happen
      throw new InternalException("Error processing request: " + e.getMessage(), e);
    }
    int numPartitionsFetched = 0;
    if (response.isSetPartition_stats()) {
      numPartitionsFetched = response.partition_stats.size();
    }
    LOG.info("Fetched statistics for {} partitions of table {}.", numPartitionsFetched,
        table);
    return response;
  }

  private static byte[] ParseQueryOptions(String csvQueryOptions, byte[] queryOptions) {
    try {
      return NativeParseQueryOptions(csvQueryOptions, queryOptions);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeParseQueryOptions(csvQueryOptions, queryOptions);
  }

  /**
   * Parses a string of comma-separated key=value query options. Returns a TQueryOptions
   * object that contains the updated query options.
   */
  public static TQueryOptions ParseQueryOptions(String csvQueryOptions,
      TQueryOptions queryOptions) throws InternalException {
    Preconditions.checkNotNull(csvQueryOptions);
    Preconditions.checkNotNull(queryOptions);

    try {
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
      byte[] result = ParseQueryOptions(csvQueryOptions,
          serializer.serialize(queryOptions));
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TQueryOptions updatedQueryOptions = new TQueryOptions();
      deserializer.deserialize(updatedQueryOptions, result);
      return updatedQueryOptions;
    } catch (TException e) {
      throw new InternalException("Could not parse query options: " + e.getMessage(), e);
    }
  }

  /**
   * Returns the log (base 2) of the minimum number of bytes we need for a Bloom
   * filter with 'ndv' unique elements and a false positive probability of less
   * than 'fpp'.
   */
  public static int GetMinLogSpaceForBloomFilter(long ndv, double fpp) {
    try {
      return MinLogSpaceForBloomFilter(ndv, fpp);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return MinLogSpaceForBloomFilter(ndv, fpp);
  }

  /**
   * Returns the expected false positive rate for the given ndv and logBufferpoolSpace.
   */
  public static double GetFalsePositiveProbForBloomFilter(
      long ndv, int logBufferpoolSpace) {
    try {
      return FalsePositiveProbForBloomFilter(ndv, logBufferpoolSpace);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return FalsePositiveProbForBloomFilter(ndv, logBufferpoolSpace);
  }

  public static byte[] GetPartialCatalogObject(byte[] thriftReq)
      throws InternalException {
    try {
      return NativeGetPartialCatalogObject(thriftReq);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeGetPartialCatalogObject(thriftReq);
  }

  private static byte[] parseDateStringUtil(String date) {
    try {
      return nativeParseDateString(date);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return nativeParseDateString(date);
  }

  /**
   * Parses date string, verifies if it is valid and returns the resulting
   * TParseDateStringResult object. Different date string variations are accepted.
   * E.g.: '2011-01-01', '2011-01-1', '2011-1-01', '2011-01-01'.
   */
  public static TParseDateStringResult parseDateString(String date)
      throws InternalException {
    Preconditions.checkNotNull(date);
    try {
      byte[] result = parseDateStringUtil(date);
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TParseDateStringResult res = new TParseDateStringResult();
      deserializer.deserialize(res, result);
      return res;
    } catch (TException e) {
      throw new InternalException("Could not parse date string: " + e.getMessage(), e);
    }
  }

  private static byte[] GetNullPartitionName(byte[] thriftReq) {
    try {
      return NativeGetNullPartitionName(thriftReq);
    } catch (UnsatisfiedLinkError e) { loadLibrary(); }
    return NativeGetNullPartitionName(thriftReq);
  }

  public static String GetNullPartitionName() throws InternalException {
    TGetNullPartitionNameRequest request = new TGetNullPartitionNameRequest();
    TGetNullPartitionNameResponse response = new TGetNullPartitionNameResponse();
    try {
      byte[] result = GetNullPartitionName(
          new TSerializer(new TBinaryProtocol.Factory()).serialize(request));
      Preconditions.checkNotNull(result);
      new TDeserializer(new TBinaryProtocol.Factory()).deserialize(response, result);
      if (response.getStatus().getStatus_code() != TErrorCode.OK) {
        throw new InternalException("Error requesting GetNullPartitionName: "
            + Joiner.on("\n").join(response.getStatus().getError_msgs()));
      }
      Preconditions.checkNotNull(response.partition_value);
      return response.partition_value;
    } catch (TException e) {
      // this should never happen
      throw new InternalException("Error processing request: " + e.getMessage(), e);
    }
  }

  public static byte[] GetLatestCompactions(byte[] thriftReq) {
    try {
      return NativeGetLatestCompactions(thriftReq);
    } catch (UnsatisfiedLinkError e) { loadLibrary(); }
    return NativeGetLatestCompactions(thriftReq);
  }

  public static TAddressesList GetCoordinators() throws InternalException {
    try {
      byte[] result = NativeGetCoordinators();
      Preconditions.checkNotNull(result);
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      TAddressesList coordinators_list = new TAddressesList();
      deserializer.deserialize(coordinators_list, result);
      return coordinators_list;
    } catch (TException e) {
      throw new InternalException("Error getting coordinators", e);
     }
  }

  public static long NumLiveQueries() {
    return NativeNumLiveQueries();
  }

  /**
   * Calling this function before loadLibrary() causes external frontend
   * initialization to be used during NativeFeInit()
   */
  public static synchronized void setExternalFE() {
    Preconditions.checkState(!loaded_);
    externalFE_ = true;
  }

  /**
   * This function should be called explicitly by the FeSupport to ensure that
   * native functions are loaded. Tests that depend on JniCatalog or JniFrontend
   * being instantiated should also call this function.
   */
  public static synchronized void loadLibrary() {
    if (loaded_) return;
    LOG.info("Loading libfesupport.so");
    NativeLibUtil.loadLibrary("libfesupport.so");
    LOG.info("Loaded libfesupport.so");
    loaded_ = true;
    NativeFeInit(externalFE_);
  }
}
