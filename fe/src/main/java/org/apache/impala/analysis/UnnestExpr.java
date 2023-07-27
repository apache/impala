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
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

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
    // Removing "item" from the end of the path is required so that re-analyze will work
    // as well after adding "item" to the path in the first round of analysis here in the
    // unnest.
    removeItemFromPath();
    rawPathWithoutItem_ = other.rawPathWithoutItem_;
  }

  protected UnnestExpr(SlotDescriptor desc) {
    super(desc);
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
    verifyNotInsideStruct(resolvedPath);

    Type type = resolvedPath.getMatchedTypes().get(
        resolvedPath.getMatchedTypes().size() - 1);
    verifyContainsNoStruct(type);

    if (!rawPathWithoutItem_.isEmpty()) rawPathWithoutItem_.clear();
    rawPathWithoutItem_.addAll(rawPath_);

    List<String> tableRefRawPath = constructRawPathForTableRef(resolvedPath);
    Preconditions.checkNotNull(tableRefRawPath);
    CollectionTableRef tblRef = createAndRegisterCollectionTableRef(tableRefRawPath,
        analyzer);

    // 'rawPath_' points to an array and we need a SlotRef to refer to the item of the
    // array. Hence, adding "item" to the end of the path.
    rawPath_.add("item");
    // If 'rawPath_' contains the table or database alias then trim it to the following
    // format: 'collection_name.item'.
    if (rawPath_.size() > 2) {
      rawPath_ = rawPath_.subList(rawPath_.size() - 2, rawPath_.size());
    }
    super.analyzeImpl(analyzer);
    Preconditions.checkState(tblRef.desc_.getSlots().size() == 1);
    analyzer.addZippingUnnestTupleId(desc_.getParent().getId());
  }

  // Verifies that the given path does not refer to a value that is within a struct.
  // Note: If using the FROM clause zipping unnest syntax, this check has to be performed
  // in CollectionTableRef.analyze().
  public static void verifyNotInsideStruct(Path resolvedPath) throws AnalysisException {
    for (Type type : resolvedPath.getMatchedTypes()) {
      if (type.isStructType()) {
          throw new AnalysisException(
              "Zipping unnest on an array that is within a struct is not supported.");
      }
    }
  }

  // Verifies that the given type does not contain a struct. Descends along array items
  // and map keys/values.
  // Note: If using the FROM clause zipping unnest syntax, this check has to be performed
  // in CollectionTableRef.analyze().
  public static void verifyContainsNoStruct(Type type) throws AnalysisException {
    if (type.isStructType()) {
          throw new AnalysisException(
              "Zipping unnest on an array that (recursively) " +
              "contains a struct is not supported.");
    } else if (type.isArrayType()) {
      ArrayType arrType = (ArrayType) type;
      verifyContainsNoStruct(arrType.getItemType());
    } else if (type.isMapType()) {
      MapType mapType = (MapType) type;
      verifyContainsNoStruct(mapType.getKeyType());
      verifyContainsNoStruct(mapType.getValueType());
    }
  }

  private void verifyTableRefs(Analyzer analyzer) throws AnalysisException {
    for (TableRef ref : analyzer.getTableRefs().values()) {
      if (ref instanceof CollectionTableRef) {
        if (!ref.isZippingUnnest() && !ref.isCollectionInSelectList()) {
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

  private CollectionTableRef createAndRegisterCollectionTableRef(
      List<String> tableRefRawPath, Analyzer analyzer) throws AnalysisException {
    String alias = "";
    if (rawPath_ != null && rawPath_.size() > 0) {
      alias = rawPath_.get(rawPath_.size() - 1);
    }
    TableRef tblRef = new TableRef(tableRefRawPath, alias);
    tblRef = analyzer.resolveTableRef(tblRef);
    Preconditions.checkState(tblRef instanceof CollectionTableRef);
    tblRef.setZippingUnnestType(ZippingUnnestType.SELECT_LIST_ZIPPING_UNNEST);
    TableRef existingTblRef = analyzer.getRegisteredTableRef(tblRef.getUniqueAlias());
    if (existingTblRef == null) {
      tblRef.analyze(analyzer);
      // This just registers the table ref in the analyzer to be added to the FROM clause
      // because the FROM clause is not available here. Note, a query rewrite will add it
      // eventually before a re-analysis.
      analyzer.addTableRefFromUnnestExpr((CollectionTableRef)tblRef);
      return (CollectionTableRef)tblRef;
    } else if (existingTblRef.isCollectionInSelectList() ||
        existingTblRef.getZippingUnnestType() == ZippingUnnestType.NONE) {
      Preconditions.checkState(existingTblRef instanceof CollectionTableRef);
      // This case happens when unnesting an array that comes from a view where the array
      // is present in the select list of the view. Here the view has already registered
      // the table ref to the analyzer.
      existingTblRef.setZippingUnnestType(ZippingUnnestType.SELECT_LIST_ZIPPING_UNNEST);
      analyzer.addTableRefFromUnnestExpr((CollectionTableRef)existingTblRef);
    }
    existingTblRef.setHidden(false);
    existingTblRef.getDesc().setHidden(false);
    return (CollectionTableRef)existingTblRef;
  }

  private void removeItemFromPath() {
    if (rawPath_ == null || rawPath_.isEmpty()) return;
    if (rawPath_.get(rawPath_.size() - 1).equals("item")) {
      rawPath_.remove(rawPath_.size() - 1);
    }
  }

  @Override
  public Expr clone() { return new UnnestExpr(this); }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    Preconditions.checkState(isAnalyzed());
    String label = "";
    if (rawPathWithoutItem_ != null) label = ToSqlUtils.getPathSql(rawPathWithoutItem_);
    if (label.equals("") && label_ != null) {
      // If 'rawPathWithoutItem_' is null then this slot is from a view and we have to
      // use 'label_' for displaying purposes.
      if (label_.endsWith(".item")) label = label_.substring(0, label_.length() - 5);
    }
    return "UNNEST(" + label  + ")";
  }

  @Override
  protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer) {
    if (smap == null) return this;
    SlotRef slotRef = new SlotRef(this.desc_);
    Expr substExpr = smap.get(slotRef);
    if (substExpr == null) {
      UnnestExpr unnestExpr = new UnnestExpr(this.desc_);
      substExpr = smap.get(unnestExpr);
      if (substExpr == null) return this;
      return substExpr;
    }

    return new UnnestExpr(((SlotRef)substExpr).getDesc());
  }

  @Override
  public boolean isBoundByTupleIds(List<TupleId> tids) {
    Preconditions.checkState(desc_ != null, "Null desc_ in UnnestExpr");
    Preconditions.checkState(desc_.getParent() != null, "Null parent in UnnestExpr");
    if (desc_.getParent().getRootDesc() != null) {
      TupleId parentId = desc_.getParent().getRootDesc().getId();
      for (TupleId tid : tids) {
        if (tid.equals(parentId)) return true;
      }
    }
    return super.isBoundByTupleIds(tids);
  }
}
