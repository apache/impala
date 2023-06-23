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

package org.apache.impala.analysis;

import com.google.common.collect.Sets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TTestCaseData;
import org.apache.impala.util.CompressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

/**
 * Statement that analyzes COPY TESTCASE [TO|FROM] <URI> [QUERY STMT]. This is used for
 * both creating a testcase file for a given query statement and loading an already
 * created testcase file.
 */

public class CopyTestCaseStmt extends StatementBase {

  // File name prefix of the testcase file for a given query statement.
  private static final String TEST_OUTPUT_FILE_PREFIX = "impala-testcase-data-";

  private static final Logger LOG = LoggerFactory.getLogger(CopyTestCaseStmt.class);

  // QueryStmt for which the testcase should be created. Set to null if we are loading
  // an existing testcase.
  private final QueryStmt queryStmt_;
  // Corresponds to:
  //  - HDFS output dir that should contain the testcase output file (or)
  //  - Full HDFS path for a given input testcase file while loading it.
  private final HdfsUri hdfsPath_;

  private CopyTestCaseStmt(QueryStmt stmt, HdfsUri path) {
    queryStmt_ = stmt;
    hdfsPath_ = path;
  }

  // Convenience c'tors for creating/loading a testcase.
  public static CopyTestCaseStmt to(QueryStmt stmt, HdfsUri path) {
    return new CopyTestCaseStmt(stmt, path);
  }

  public static CopyTestCaseStmt from(HdfsUri path) {
    return new CopyTestCaseStmt(null, path);
  }

  /**
   * @return True if this stmt corresponds to a testcase export for a given query.
   * False otherwise.
   */
  public boolean isTestCaseExport() { return queryStmt_ != null; }

  public QueryStmt getQueryStmt() { return queryStmt_; }

  public String getHdfsPath() { return hdfsPath_.getLocation(); }

  @Override
  public void collectTableRefs(List<TableRef> referencedTables) {
    if (!isTestCaseExport()) return;
    queryStmt_.collectTableRefs(referencedTables);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (isTestCaseExport()) {
      hdfsPath_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE,
          /*registerPrivReq*/ true, /*pathMustExist*/ true);
      try {
        if (!FileSystemUtil.isDir(hdfsPath_.getPath())) {
          throw new AnalysisException(String.format("Path is not a valid directory to " +
                "write the testcase output file: %s", hdfsPath_));
        }
      } catch (IOException e) {
        throw new AnalysisException(
            String.format("Error checking the status of path: %s", hdfsPath_), e);
      }
      queryStmt_.analyze(analyzer);
      // Requires VIEW_METADATA privilege to dump the metadata state of all the
      // referenced objects.
      Pair<Set<FeDb>, Set<FeTable>> referencedObjects = getReferencedCatalogObjects();
      for (FeDb db: referencedObjects.first) {
        analyzer.registerPrivReq(builder ->
            builder.onDb(db).allOf(Privilege.VIEW_METADATA).build());
      }
      for (FeTable table: referencedObjects.second) {
        analyzer.registerPrivReq(builder ->
            builder.onTable(table)
                .allOf(Privilege.VIEW_METADATA)
                .build());
      }
    } else {
      hdfsPath_.analyze(analyzer, Privilege.ALL, FsAction.READ, /*registerPrivReq*/ true,
          /*pathMustExist*/ true);
    }
  }

  /**
   * Helper method that returns all the base tables, view and databases referenced in the
   * queryStmt_. Omits query local views.
   */
  private Pair<Set<FeDb>, Set<FeTable>> getReferencedCatalogObjects() {
    Preconditions.checkState(queryStmt_.isAnalyzed());
    Set<FeTable> referencedTblsAndViews = Sets.newIdentityHashSet();
    Set<FeDb> referencedDbs = Sets.newIdentityHashSet();
    for (TableRef ref: queryStmt_.collectTableRefs()) {
      referencedDbs.add(ref.getTable().getDb());
      referencedTblsAndViews.add(ref.getTable());
    }
    for (FeView view: queryStmt_.collectInlineViews()) {
      if (view == null || view.isLocalView()) continue;
      referencedDbs.add(view.getDb());
      referencedTblsAndViews.add(view);
    }
    return new Pair(referencedDbs, referencedTblsAndViews);
  }

  /**
   * Walks through the analyzed queryStmt_ tree to identify all the referenced tables,
   * views and databases which are then serialized into the TTestCaseData output val.
   */
  @VisibleForTesting
  public TTestCaseData getTestCaseData() throws ImpalaException {
    Preconditions.checkState(queryStmt_.isAnalyzed());
    TTestCaseData result = new TTestCaseData(queryStmt_.getOrigSqlString(),
        hdfsPath_.getLocation(), BackendConfig.INSTANCE.getImpalaBuildVersion());
    Pair<Set<FeDb>, Set<FeTable>> referencedObjects = getReferencedCatalogObjects();
    // Sort the referenced objects for deterministic testcase outputs.
    List<FeDb> referencedDbs = new ArrayList<>(referencedObjects.first);
    List<FeTable> referencedTbls = new ArrayList<>(referencedObjects.second);
    Collections.sort(referencedDbs, FeDb.NAME_COMPARATOR);
    Collections.sort(referencedTbls, FeTable.NAME_COMPARATOR);
    for (FeDb db: referencedDbs) {
      result.addToDbs(db.toThrift());
    }
    for (FeTable table: referencedTbls) {
      result.addToTables_and_views(FeCatalogUtils.feTableToThrift(table));
    }
    return result;
  }

  /**
   * Builds the testcase data for the input queryStmt_ and writes it to a file in the
   * hdfsPath_ directory. Randomly generates the output filename and returns the fully
   * qualified path.
   */
  public String writeTestCaseData() throws ImpalaException {
    TTestCaseData data = getTestCaseData();
    Path filePath = new Path(
        hdfsPath_.getPath(), TEST_OUTPUT_FILE_PREFIX + UUID.randomUUID().toString());
    try {
      FileSystem fs = FileSystemUtil.getDefaultFileSystem();
      FSDataOutputStream os = fs.create(filePath);
      try {
        os.write(CompressionUtil.deflateCompress(JniUtil.serializeToThrift(data)));
      } finally {
        os.close();
      }
    } catch (IOException e) {
      throw new ImpalaRuntimeException(String.format("Error writing test case output to" +
          " file: %s", filePath), e);
    }
    LOG.info("Created testcase file {} which contains {} db(s), {} table(s)/view(s)" +
            " for query: {}",
        filePath, data.getDbsSize(), data.getTables_and_viewsSize(),
        data.getQuery_stmt());
    return filePath.toString();
  }
}
