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

package org.apache.impala.catalog;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.impala.util.EventSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a table with incomplete metadata. The metadata may be incomplete because
 * it has not yet been loaded or because of errors encountered during the loading
 * process.
 *
 * NOTE: this is used on the catalogd (CatalogServiceCatalog) and on the "v1"
 * ImpaladCatalog. LocalCatalog does not use this, and instead uses
 * FailedLoadLocalTable to represent a failed table.
 */
public class IncompleteTable extends Table implements FeIncompleteTable {
  public static final Logger LOG = LoggerFactory.getLogger(IncompleteTable.class);

  // The cause for the incomplete metadata. If there is no cause given (cause_ = null),
  // then this is assumed to be an uninitialized table (table that does not have
  // its metadata loaded).
  private ImpalaException cause_;
  private TImpalaTableType tableType_;
  private String comment_;

  private IncompleteTable(Db db, String name, TImpalaTableType type, String comment,
      ImpalaException cause) {
    super(null, db, name, null);
    cause_ = cause;
    tableType_ = type;
    comment_ = comment;
    LOG.trace("Created IncompleteTable for {}.{}: type={}, comment={}",
        db.getName(), name, type, comment);
  }

  @Override
  public TImpalaTableType getTableType() { return tableType_; }

  @Override
  public String getTableComment() { return comment_; }

  /**
   * Returns the cause (ImpalaException) which led to this table's metadata being
   * incomplete.
   */
  @Override
  public ImpalaException getCause() { return cause_; }

  /**
   * Returns true if the load has failed due to recoverable errors such as
   * metastore connection error
   * @return
   */
  @Override
  public boolean isLoadFailedByRecoverableError() {
    if (cause_ instanceof TableLoadingException) {
      String metastoreConnectionError = "Could not connect to meta store";
      if (cause_.getMessage().contains(metastoreConnectionError)
          || (cause_.getCause() instanceof MetaException
                 && cause_.getCause().getMessage().contains(metastoreConnectionError))) {
        return true;
      }
    }
    return false;
  }

  /**
   * See comment on cause_.
   */
  @Override
  public boolean isLoaded() { return cause_ != null; }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    // Respect the table type if it's actually a view. It's essential to be consistent
    // with CatalogServiceCatalog#addTableToCatalogDeltaHelper().
    if (tableType_ == TImpalaTableType.VIEW) return TCatalogObjectType.VIEW;
    return TCatalogObjectType.TABLE;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
    throw new IllegalStateException(cause_);
  }

  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    if (cause_ instanceof TableLoadingException) {
      throw (TableLoadingException) cause_;
    } else {
      throw new TableLoadingException("Table metadata incomplete: ", cause_);
    }
  }

  @Override
  public TTable toThrift() {
    TTable table = new TTable(db_.getName(), name_);
    if (cause_ != null) {
      table.setLoad_status(new TStatus(TErrorCode.INTERNAL_ERROR,
          Lists.newArrayList(JniUtil.throwableToString(cause_),
                             JniUtil.throwableToStackTrace(cause_))));
    }
    if (tableType_ == TImpalaTableType.VIEW) {
      table.setTable_type(TTableType.VIEW);
    } else if (tableType_ == TImpalaTableType.MATERIALIZED_VIEW) {
      table.setTable_type(TTableType.MATERIALIZED_VIEW);
    } else {
      table.setTable_type(TTableType.UNLOADED_TABLE);
    }
    if (comment_ != null) {
      LOG.trace("Setting comment of {}: {}", getFullName(), comment_);
      table.setTbl_comment(comment_);
    }
    return table;
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) {
    if (thriftTable.isSetTbl_comment()) {
      comment_ = thriftTable.getTbl_comment();
      LOG.trace("Loaded comment from thriftTable of {}: {}", getFullName(), comment_);
    }
    if (thriftTable.isSetLoad_status()) {
      // Since the load status is set, it indicates the table is incomplete due to
      // an error loading the table metadata. The error message in the load status
      // should provide details on why. By convention, the final error message should
      // be the remote (Catalog Server) call stack. This shouldn't be displayed to the
      // user under normal circumstances, but needs to be recorded somewhere so append
      // it to the call stack of the local TableLoadingException created here.
      // TODO: Provide a mechanism (query option?) to optionally allow returning more
      // detailed errors (including the full call stack(s)) to the user.
      List<String> errorMsgs = thriftTable.getLoad_status().getError_msgs();
      String callStackStr = "<None available>";
      if (errorMsgs.size() > 1) callStackStr = errorMsgs.remove(errorMsgs.size() - 1);

      String errorMsg = Joiner.on("\n").join(errorMsgs);
      // The errorMsg will always be prefixed with "ExceptionClassName: ". Since we treat
      // all errors as TableLoadingExceptions, the prefix "TableLoadingException" is
      // redundant and can be stripped out.
      errorMsg = errorMsg.replaceFirst("^TableLoadingException: ", "");
      TableLoadingException loadingException = new TableLoadingException(errorMsg);
      List<StackTraceElement> stackTrace =
          Lists.newArrayList(loadingException.getStackTrace());
      stackTrace.add(new StackTraceElement("========",
          "<Remote stack trace on catalogd>: " + callStackStr, "", -1));
      loadingException.setStackTrace(
          stackTrace.toArray(new StackTraceElement[stackTrace.size()]));
      this.cause_ = loadingException;
    }
  }

  public static IncompleteTable createUninitializedTable(Db db, String name,
      TImpalaTableType tableType, String tableComment) {
    return new IncompleteTable(db, name, tableType, tableComment, null);
  }

  public static IncompleteTable createFailedMetadataLoadTable(Db db, String name,
      ImpalaException e) {
    return new IncompleteTable(db, name, TImpalaTableType.UNKNOWN, null, e);
  }

  /**
   * Create an instance for DropTable ops where the table type and comment is not used.
   */
  public static IncompleteTable createUninitializedTableForRemove(Db db, String name) {
    return new IncompleteTable(db, name, /*type*/null, /*comment*/null, /*cause*/null);
  }

  @Override
  public TGetPartialCatalogObjectResponse getPartialInfo(
      TGetPartialCatalogObjectRequest req) throws TableLoadingException {
    Preconditions.checkNotNull(cause_);
    Throwables.propagateIfPossible(cause_, TableLoadingException.class);
    throw new TableLoadingException(cause_.getMessage());
  }
}
