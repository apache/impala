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

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RESTException;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

/** Shared transaction and cleanup handling for Iceberg DML finalization. */
final class IcebergDmlFinalizer {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergDmlFinalizer.class);

  private static final EnumSet<TIcebergOperation> SUPPORTED_REST_OPS =
      EnumSet.of(TIcebergOperation.INSERT, TIcebergOperation.UPDATE,
          TIcebergOperation.DELETE, TIcebergOperation.MERGE, TIcebergOperation.OPTIMIZE);

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
      FeTable table = catalogManager.getCatalog(catalogName)
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
    if (!(table instanceof FeIcebergTable iceTbl)) {
      throw new ImpalaRuntimeException(String.format(
          "DML finalization target is not an Iceberg table: %s.%s",
          request.getDb_name(), request.getTarget_table()));
    }

    if (!request.getRest_catalog_name().equals(iceTbl.getCatalogName())) {
      throw new ImpalaRuntimeException(String.format(
          "REST catalog changed for table %s.%s",
          request.getDb_name(), request.getTarget_table()));
    }
    return iceTbl;
  }

  static void validateRestDmlOperation(TIcebergOperationParam operation)
      throws ImpalaRuntimeException {
    if (!SUPPORTED_REST_OPS.contains(operation.getOperation())) {
      throw new ImpalaRuntimeException(
          "Unsupported operation for the Iceberg REST finalizer");
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
    // Only an I/O error after the commit request was sent can mean that the REST
    // catalog applied the commit.
    boolean commitAttempted = false;
    try {
      IcebergCatalogOpExecutor.validateTableUuid(table, operation);
      DebugUtils.executeDebugAction(debugAction, DebugUtils.ICEBERG_CONFLICT);
      IcebergCatalogOpExecutor.execute(table, transaction, operation);
      postOperationHook.run();
      DebugUtils.executeDebugAction(debugAction, DebugUtils.ICEBERG_COMMIT);
      commitAttempted = true;
      transaction.commitTransaction();
    // If we have no information about the success of the commit, we should not delete
    // anything.
    } catch (CommitStateUnknownException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    // If the commit failed, the newly written files should be deleted to avoid creating
    // orphan files in the table. Only data/delete files need cleanup from Impala, Iceberg
    // deletes the metadata files created for this update.
    } catch (Exception e) {
      if (commitAttempted && isLostRestResponse(e)) {
        LOG.warn("Kept the data files written for table {}: the commit may have been "
            + "applied by the REST catalog", table.getFullName(), e);
        throw new ImpalaRuntimeException(e.getMessage(), e);
      }
      IcebergCatalogOpExecutor.cleanupUncommittedFiles(operation);
      LOG.info("Cleaned up uncommitted data files after failing to commit them to "
          + "table {}", table.getFullName());
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Whether 'e' is a REST catalog request that failed on I/O, e.g. the connection broke
   * after the request was sent. The REST catalog may have applied such a commit, but
   * Iceberg reports it as a RESTException instead of a CommitStateUnknownException.
   */
  @VisibleForTesting
  static boolean isLostRestResponse(Throwable e) {
    List<Throwable> chain = Throwables.getCausalChain(e);
    return chain.stream().anyMatch(t -> t instanceof RESTException)
        && chain.stream().anyMatch(t -> t instanceof IOException);
  }
}
