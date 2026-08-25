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
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.catalogmanager.FeCatalogManager;
import org.apache.impala.thrift.TIcebergDmlFinalizeRequest;
import org.apache.impala.thrift.TIcebergOperation;
import org.apache.impala.thrift.TIcebergOperationParam;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.IcebergUtil;
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

  /** Finalizes Iceberg DML through its owning local metadata provider. */
  static void finalizeDml(
      FeCatalogManager catalogManager, TIcebergDmlFinalizeRequest request)
      throws ImpalaException {
    if (!request.isSetIceberg_operation()) {
      throw new ImpalaRuntimeException("Missing Iceberg operation for DML finalization");
    }
    if (!request.isSetRest_catalog_name()) {
      throw new ImpalaRuntimeException("Missing REST catalog name for DML finalization");
    }

    Transaction iceTxn;
    FeIcebergTable iceTbl;
    try {
      validateRestDmlOperation(request.getIceberg_operation());
      String catalogName = request.getRest_catalog_name();
      FeTable table = catalogManager.getCatalogForIcebergDml(catalogName)
          .getTable(request.getDb_name(), request.getTarget_table());
      iceTbl = validateRestTargetTable(request, table);
      iceTxn = IcebergUtil.getIcebergTransaction(iceTbl);
    } catch (Exception e) {
      IcebergCatalogOpExecutor.cleanupUncommittedFiles(request.getIceberg_operation());
      LOG.info("Cleaned up uncommitted data files after DML finalization failed for "
          + "table {}.{}", request.getDb_name(), request.getTarget_table());
      if (e instanceof ImpalaRuntimeException) throw (ImpalaRuntimeException)e;
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }

    String debugAction = request.isSetDebug_action()
        ? request.getDebug_action() : null;
    finalizeDml(iceTbl, iceTxn, request.getIceberg_operation(), debugAction);
  }

  private static FeIcebergTable validateRestTargetTable(
      TIcebergDmlFinalizeRequest request, FeTable table)
      throws ImpalaRuntimeException {
    if (!(table instanceof FeIcebergTable)) {
      throw new ImpalaRuntimeException(String.format(
          "DML finalization target is not an Iceberg table: %s.%s",
          request.getDb_name(), request.getTarget_table()));
    }

    FeIcebergTable iceTbl = (FeIcebergTable)table;
    if (!request.getRest_catalog_name().equals(iceTbl.getIcebergDmlCatalogName())) {
      throw new ImpalaRuntimeException(String.format(
          "REST catalog changed for table %s.%s",
          request.getDb_name(), request.getTarget_table()));
    }
    return iceTbl;
  }

  static void validateRestDmlOperation(TIcebergOperationParam operation)
      throws ImpalaRuntimeException {
    if (operation.getOperation() != TIcebergOperation.INSERT
        || operation.isIs_overwrite()) {
      throw new ImpalaRuntimeException(
          "The Iceberg REST finalizer only supports INSERT INTO");
    }
    if (!operation.isSetTable_uuid()) {
      throw new ImpalaRuntimeException(
          "Missing Iceberg table UUID for REST DML finalization");
    }
  }

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
      IcebergCatalogOpExecutor.validateTableUuid(table, operation);
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
