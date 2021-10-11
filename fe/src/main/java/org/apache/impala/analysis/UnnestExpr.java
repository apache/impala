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

import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.analysis.TableRef.ZippingUnnestType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

// This class represents a zipping unnest SlotRef in a SELECT as the following:
// SELECT unnest(arr1), unnest(arr2) FROM tbl;
public class UnnestExpr extends SlotRef {
  // Stores the raw path to the underlying array without the "item" postfix.
  private List<String> rawPathWithoutItem_ = new ArrayList<>();

  public UnnestExpr(List<String> path) {
    super(path);
  }

  protected UnnestExpr(UnnestExpr other) {
    super(other);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(rawPath_);
    Preconditions.checkState(rawPath_.size() >= 1);

    verifyTableRefs(analyzer);

    // Resolve 'rawPath_' early before running super.analyzeImpl() because the required
    // CollectionTableRef might have to be created beforehand so that the array item could
    // find the corresponding CollectionTableRef during resolution.
    Path resolvedPath = resolveAndVerifyRawPath(analyzer);
    Preconditions.checkNotNull(resolvedPath);
    rawPathWithoutItem_.addAll(rawPath_);

    List<String> tableRefRawPath = constructRawPathForTableRef(resolvedPath);
    Preconditions.checkNotNull(tableRefRawPath);
    createAndRegisterCollectionTableRef(tableRefRawPath, analyzer);

    // 'rawPath_' points to an array and we need a SlotRef to refer to the item of the
    // array. Hence, adding "item" to the end of the path.
    rawPath_.add("item");
    // If 'rawPath_' contains the table or database alias then trim it to the following
    // format: 'collection_name.item'.
    if (rawPath_.size() > 2) {
      rawPath_ = rawPath_.subList(rawPath_.size() - 2, rawPath_.size());
    }
    super.analyzeImpl(analyzer);
  }

  private void verifyTableRefs(Analyzer analyzer) throws AnalysisException {
    for (TableRef ref : analyzer.getTableRefs().values()) {
      if (ref instanceof CollectionTableRef) {
        if (!ref.isZippingUnnest()) {
          throw new AnalysisException(
              "Providing zipping and joining unnests together is not supported.");
        } else if (ref.getZippingUnnestType() ==
            ZippingUnnestType.FROM_CLAUSE_ZIPPING_UNNEST) {
          throw new AnalysisException("Providing zipping unnest both in the SELECT " +
              "list and in the FROM clause is not supported.");
        }
      }
    }
  }

  private Path resolveAndVerifyRawPath(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(rawPath_);
    // If this is a re-analysis round and we already added "item" to the end of the path,
    // we remove it now before resolving the path again.
    if (resolvedPath_ != null) removeItemFromPath();
    Path resolvedPath = null;
    try {
      resolvedPath = analyzer.resolvePath(rawPath_, PathType.SLOT_REF);
      if (resolvedPath == null) {
        throw new AnalysisException("Unable to resolve path: " +
            ToSqlUtils.getPathSql(rawPath_));
      }
    } catch (TableLoadingException e) {
      throw new AnalysisException(e.toString());
    }
    Preconditions.checkNotNull(resolvedPath);
    Preconditions.checkState(!resolvedPath.getMatchedTypes().isEmpty());
    Type resolvedType =
        resolvedPath.getMatchedTypes().get(resolvedPath.getMatchedTypes().size() - 1);
    if (!resolvedType.isArrayType()) {
      throw new AnalysisException("Unnest operator is only supported for arrays. " +
          ToSqlUtils.getPathSql(rawPath_));
    }
    return resolvedPath;
  }

  private List<String> constructRawPathForTableRef(Path resolvedPath) {
    List<String> tableRefRawPath = new ArrayList<String>();
    tableRefRawPath.add(resolvedPath.getRootDesc().getAlias());
    tableRefRawPath.add(rawPath_.get(rawPath_.size() - 1));
    return tableRefRawPath;
  }

  private void createAndRegisterCollectionTableRef(List<String> tableRefRawPath,
      Analyzer analyzer) throws AnalysisException {
    TableRef tblRef = new TableRef(tableRefRawPath, null);
    tblRef = analyzer.resolveTableRef(tblRef);
    Preconditions.checkState(tblRef instanceof CollectionTableRef);
    tblRef.setZippingUnnestType(ZippingUnnestType.SELECT_LIST_ZIPPING_UNNEST);
    if (!analyzer.isRegisteredTableRef(tblRef)) {
      tblRef.analyze(analyzer);
      // This just registers the tbl ref to be added to the FROM clause because it's not
      // available here. Note, SelectStmt will add it to the FROM clause during analysis.
      analyzer.addTableRefFromUnnestExpr((CollectionTableRef)tblRef);
    }
  }

  private void removeItemFromPath() {
    Preconditions.checkNotNull(rawPath_);
    if (rawPath_.get(rawPath_.size() - 1).equals("item")) {
      rawPath_.remove(rawPath_.size() - 1);
    }
  }

  @Override
  public Expr clone() { return new UnnestExpr(this); }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    return "UNNEST(" + ToSqlUtils.getPathSql(rawPathWithoutItem_)  + ")";
  }
}