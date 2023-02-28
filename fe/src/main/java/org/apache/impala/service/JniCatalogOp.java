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

import com.google.common.base.Preconditions;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.JniUtil.OperationLog;
import org.apache.impala.common.Pair;
import org.apache.impala.util.ThreadNameAnnotator;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class JniCatalogOp {
  public interface JniCatalogOpCallable<V> {
    V call() throws ImpalaException, TException;
  }

  /**
   * Executes @operand in an annotated thread environment for better supportability
   *
   * @param methodName       The name of the method which is wrapped in {@code operand}
   * @param shortDescription The description which will get applied as the thread's name
   * @param operand          The callable that does the actual operation. Type parameter
   *                         is {@code Pair<RESULT,Long>} thus the length of the resulting
   *                         object can be present regardless of the type of the result.
   * @param requestParameter Thrift request parameter which gets processed in
   *                         {@code operand}
   * @param finalClause      Finalize clause which will be called in a final block after
   *                         {@code operand}
   * @param <RESULT>         Type parameter for the result
   * @param <PARAMETER>      Type parameter for {@code requestParameter}
   */
  public static <RESULT, PARAMETER extends TBase<?, ?>> RESULT execOp(String methodName,
      String shortDescription, JniCatalogOpCallable<Pair<RESULT, Long>> operand,
      PARAMETER requestParameter, Runnable finalClause, OperationLog operationLog)
      throws ImpalaException, TException {
    Preconditions.checkNotNull(operand);
    Preconditions.checkNotNull(finalClause);

    try (ThreadNameAnnotator ignored = new ThreadNameAnnotator(shortDescription)) {
      Pair<RESULT, Long> result = operand.call();

      long size = 0;
      if (result.second != null) {
        size = result.second;
      }

      operationLog.logResponse(size, requestParameter);
      operationLog.logFinish();

      return result.first;
    } catch (Throwable e) {
      operationLog.logError();
      throw e;
    } finally {
      finalClause.run();
    }
  }

  public static <RESULT, PARAMETER extends TBase<?, ?>> RESULT execOp(String methodName,
      String shortDescription, JniCatalogOpCallable<Pair<RESULT, Long>> operand,
      PARAMETER parameter) throws ImpalaException, TException {
    OperationLog operationLog = JniUtil.logOperation(methodName, shortDescription);
    return execOp(
        methodName, shortDescription, operand, parameter, () -> {}, operationLog);
  }

  public static byte[] execAndSerialize(String methodName, String shortDescription,
      JniCatalogOpCallable<TBase<?, ?>> operand, TSerializer serializer,
      Runnable finalClause, OperationLog operationLog)
      throws ImpalaException, TException {
    return execOp(methodName, shortDescription, () -> {
      TBase<?, ?> response = operand.call();
      byte[] result = serializer.serialize(response);
      return Pair.create(result, (long) result.length);
    }, null, finalClause, operationLog);
  }

  public static byte[] execAndSerialize(String methodName, String shortDescription,
      JniCatalogOpCallable<TBase<?, ?>> operand, TSerializer serializer,
      Runnable finalClause) throws ImpalaException, TException {
    OperationLog operationLog = JniUtil.logOperation(methodName, shortDescription);
    return execAndSerialize(
        methodName, shortDescription, operand, serializer, finalClause, operationLog);
  }

  public static byte[] execAndSerializeSilentStartAndFinish(String methodName,
      String shortDescription, JniCatalogOpCallable<TBase<?, ?>> operand,
      TSerializer serializer, Runnable finalClause) throws ImpalaException, TException {
    OperationLog operationLog =
        JniUtil.logOperationSilentStartAndFinish(methodName, shortDescription);
    return execAndSerialize(
        methodName, shortDescription, operand, serializer, finalClause, operationLog);
  }
}
