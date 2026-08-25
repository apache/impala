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

import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TIcebergOperationParam;
import org.apache.impala.util.DebugUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared transaction and cleanup handling for Iceberg DML finalization. */
final class IcebergDmlFinalizer {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergDmlFinalizer.class);

  @FunctionalInterface
  interface PostOperationHook {
    void run() throws Exception;
  }

  private IcebergDmlFinalizer() {}

  static void finalizeDml(FeIcebergTable table, Transaction transaction,
      TIcebergOperationParam operation, String debugAction)
      throws ImpalaRuntimeException {
    finalizeDml(table, transaction, operation, debugAction, () -> {});
  }

  /**
   * Executes an Iceberg DML operation and commits its transaction. The hook runs after
   * the operation is added to the transaction and before the transaction is committed.
   * CatalogD uses it for its HMS event properties; other finalizers can omit it.
   */
  static void finalizeDml(FeIcebergTable table, Transaction transaction,
      TIcebergOperationParam operation, String debugAction,
      PostOperationHook postOperationHook) throws ImpalaRuntimeException {
    try {
      DebugUtils.executeDebugAction(debugAction, DebugUtils.ICEBERG_CONFLICT);
      IcebergCatalogOpExecutor.execute(table, transaction, operation);
      postOperationHook.run();
      DebugUtils.executeDebugAction(debugAction, DebugUtils.ICEBERG_COMMIT);
      transaction.commitTransaction();
    // If we have no information about the success of the commit, we should not delete
    // anything.
    } catch (CommitStateUnknownException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    // If the commit failed, the newly written files should be deleted to avoid creating
    // orphan files in the table. Only data/delete files need cleanup from Impala, Iceberg
    // deletes the metadata files created for this update.
    } catch (Exception e) {
      IcebergCatalogOpExecutor.cleanupUncommittedFiles(operation);
      LOG.info("Cleaned up uncommitted data files after failing to commit them to "
          + "table {}", table.getFullName());
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }
}
