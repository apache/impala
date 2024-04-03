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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.apache.iceberg.Table;
import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeIcebergTable.Utils;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ColumnAliasGenerator;
import org.apache.impala.common.Pair;
import org.apache.impala.common.TableAliasGenerator;
import org.apache.impala.common.TreeNode;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Representation of a single select block, including GROUP BY, ORDER BY and HAVING
 * clauses.
 */
public class SelectStmt extends QueryStmt {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  protected SelectList selectList_;
  protected final List<String> colLabels_; // lower case column labels
  protected final FromClause fromClause_;
  protected Expr whereClause_;

  // Grouping expressions for this select block, including expressions from the original
  // query statement and additional expressions that may be added during rewriting.
  protected List<Expr> groupingExprs_;

  // The original GROUP BY clause with information about grouping sets, etc.
  // If there was no original GROUP BY clause, but grouping exprs are added during
  // rewriting, this is initialized as a placeholder empty group by list.
  // Non-null iff groupingExprs_ is non-null.
  private GroupByClause groupByClause_;

  protected Expr havingClause_;  // original having clause

  // havingClause with aliases and agg output resolved
  private Expr havingPred_;

  // set if we have any kind of aggregation operation, include SELECT DISTINCT
  private MultiAggregateInfo multiAggInfo_;

  // set if we have AnalyticExprs in the select list/order by clause
  private AnalyticInfo analyticInfo_;

  // substitutes all exprs in this select block to reference base tables
  // directly
  private ExprSubstitutionMap baseTblSmap_ = new ExprSubstitutionMap();

  // END: Members that need to be reset()
  /////////////////////////////////////////

  // SQL string of this SelectStmt before inline-view expression substitution.
  // Set in analyze().
  protected String sqlString_;

  SelectStmt(SelectList selectList,
             FromClause fromClause,
             Expr wherePredicate, GroupByClause groupByClause,
             Expr havingPredicate, List<OrderByElement> orderByElements,
             LimitElement limitElement) {
    super(orderByElements, limitElement);
    selectList_ = selectList;
    if (fromClause == null) {
      fromClause_ = new FromClause();
    } else {
      fromClause_ = fromClause;
    }
    whereClause_ = wherePredicate;
    groupByClause_ = groupByClause;
    if (groupByClause != null) {
      groupingExprs_ = Expr.cloneList(groupByClause.getOrigGroupingExprs());
    } else {
      groupingExprs_ = null;
    }
    havingClause_ = havingPredicate;
    colLabels_ = new ArrayList<>();
    havingPred_ = null;
    multiAggInfo_ = null;
    sortInfo_ = null;
  }

  /**
   * @return the original select list items from the query
   */
  public SelectList getSelectList() { return selectList_; }

  /**
   * @return the HAVING clause post-analysis and with aliases resolved
   */
  public Expr getHavingPred() { return havingPred_; }

  @Override
  public List<String> getColLabels() { return colLabels_; }

  public List<TableRef> getTableRefs() { return fromClause_.getTableRefs(); }
  public boolean hasWhereClause() { return whereClause_ != null; }
  public boolean hasGroupByClause() { return groupingExprs_ != null; }
  public Expr getWhereClause() { return whereClause_; }
  public void setWhereClause(Expr whereClause) { whereClause_ = whereClause; }
  public MultiAggregateInfo getMultiAggInfo() { return multiAggInfo_; }
  public boolean hasMultiAggInfo() { return multiAggInfo_ != null; }
  public AnalyticInfo getAnalyticInfo() { return analyticInfo_; }
  public boolean hasAnalyticInfo() { return analyticInfo_ != null; }
  public boolean hasHavingClause() { return havingClause_ != null; }
  public ExprSubstitutionMap getBaseTblSmap() { return baseTblSmap_; }

  public void addToFromClause(TableRef ref) { fromClause_.add(ref); }

  /**
   * A simple limit statement has a limit but no order-by,
   * group-by, aggregates or analytic functions. Joins are
   * allowed if one of the table refs has a limit_to_sample
   * hint. The statement can have a WHERE clause if it has an
   * always_true hint.
   * This method does not explicitly check for subqueries. If the
   * subquery occurs in the WHERE, the initial check for simple
   * limit may succeed. Subsequently, in the planning process
   * the statement rewriter may transform it to a join or an
   * equivalent plan and analyze will be called again and this time
   * it may or may not satisfy the simple limit criteria.
   * Note that FROM clause subquery is generally ok since it is
   * a table ref.
   */
  public Pair<Boolean, Long> checkSimpleLimitStmt() {
    if (hasSimpleLimitEligibleTableRefs()
        && !hasGroupByClause() && !hasOrderByClause()
        && !hasMultiAggInfo() && !hasAnalyticInfo()) {
      if (hasWhereClause() && !Expr.IS_ALWAYS_TRUE_PREDICATE.apply(getWhereClause())) {
        return null;
      }
      if (hasLimit()) {
        return new Pair<>(Boolean.valueOf(true), getLimit());
      } else {
        // even if this SELECT statement does not have a LIMIT, it is a
        // simple select which may be an inline view and eligible for a
        // limit pushdown from an outer block, so we return a non-null value
        return new Pair<>(Boolean.valueOf(false), null);
      }
    }
    return null;
  }

  /**
   * Append additional grouping expressions to the select list. Used by StmtRewriter.
   */
  protected void addGroupingExprs(List<Expr> addtlGroupingExprs) {
    if (groupingExprs_ == null) {
      groupByClause_ = new GroupByClause(
          Collections.emptyList(), GroupByClause.GroupingSetsType.NONE);
      groupingExprs_ = new ArrayList<>();
    }
    groupingExprs_.addAll(addtlGroupingExprs);
  }

  private boolean hasSimpleLimitEligibleTableRefs() {
    // for single table query blocks, limit pushdown can be considered
    // even if no table hint is present
    if (getTableRefs().size() == 1) return true;
    // if there are multiple table refs, at least 1 should have the hint
    // for limit to sample
    for (TableRef ref : getTableRefs()) {
      if (ref.hasConvertLimitToSampleHint()) return true;
    }
    return false;
  }

  /**
   * Remove the group by clause. Used by StmtRewriter. This changes the semantics
   * of this statement and should only be called when the query is being rewritten
   * in a way such that the GROUP BY is *not* required for correctness.
   */
  protected void removeGroupBy() {
    groupByClause_ = null;
    groupingExprs_ = null;
  }

  // Column alias generator used during query rewriting.
  private ColumnAliasGenerator columnAliasGenerator_ = null;
  public ColumnAliasGenerator getColumnAliasGenerator() {
    if (columnAliasGenerator_ == null) {
      columnAliasGenerator_ = new ColumnAliasGenerator(colLabels_, null);
    }
    return columnAliasGenerator_;
  }

  // Table alias generator used during query rewriting.
  private TableAliasGenerator tableAliasGenerator_ = null;
  public TableAliasGenerator getTableAliasGenerator() {
    if (tableAliasGenerator_ == null) {
      tableAliasGenerator_ = new TableAliasGenerator(analyzer_, null);
    }
    return tableAliasGenerator_;
  }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    boolean hasChanges = super.resolveTableMask(analyzer);
    // Recurse in all places that could have subqueries. After resolveTableMask() is done
    // on the whole AST, SlotRefs referencing the original table refs will be reset and
    // re-analyzed to reference the table masking views. So we don't need to deal with
    // SlotRefs here.
    hasChanges |= fromClause_.resolveTableMask(analyzer);
    for (SelectListItem item : selectList_.getItems()) {
      if (item.isStar()) continue;
      hasChanges |= item.getExpr().resolveTableMask(analyzer);
    }
    if (whereClause_ != null) hasChanges |= whereClause_.resolveTableMask(analyzer);
    if (havingClause_ != null) hasChanges |= havingClause_.resolveTableMask(analyzer);
    return hasChanges;
  }

  /**
   * Creates resultExprs and baseTblResultExprs.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    new SelectAnalyzer(analyzer).analyze();
    this.optimizePlainCountStarQueryForIcebergTable();
  }

  /**
   * Algorithm class for the SELECT statement analyzer. Holds
   * the analyzer and intermediate state.
   */
  private class SelectAnalyzer {

    private final Analyzer analyzer_;
    private List<Expr> groupingExprsCopy_;
    private List<FunctionCallExpr> aggExprs_;
    private ExprSubstitutionMap countAllMap_;

    private class StarExpandedPathInfo {
      public StarExpandedPathInfo(Path expandedPath, Path originalRootPath) {
        expandedPath_ = expandedPath;
        originalRootPath_ = originalRootPath;
      }

      public Path getExpandedPath() { return expandedPath_; }
      public boolean shouldRegisterForColumnMasking() {
        // Empty matched types means this is expanded from star of a catalog table. For
        // star of complex types, e.g. my_struct.*, my_array.*, my_map.*, the matched
        // types will have the complex type so it's not empty.
        // TODO: IMPALA-11712: We should sort out column masking and complex types. The
        // above comment may not always be true: in the query
        //   select a.* from mix_struct_array t, t.struct_in_arr a;
        // getMatchedTypes() returns an empty list for the star path even though it is not
        // from a catalog table.
        // We should also find out whether we can determine from the expanded path alone
        // (and not from the path of the star item) whether we need to register it for
        // column masking, for example by checking if it is within a complex type.
        return originalRootPath_.getMatchedTypes().isEmpty();
      }

      // The path expanded from a star select list item.
      private final Path expandedPath_;

      // The original path of the star select list item from which 'expandedPath_' was
      // expanded.
      // Can be the path of a table, a struct or a collection.
      private final Path originalRootPath_;
    }

    // A map from star 'SelectListItem's to the paths to which they are expanded.
    private final Map<SelectListItem, List<StarExpandedPathInfo>> starExpandedPaths_
        = new HashMap<>();

    private SelectAnalyzer(Analyzer analyzer) {
      this.analyzer_ = analyzer;
    }

    private void analyze() throws AnalysisException {
      // Start out with table refs to establish aliases.
      fromClause_.analyze(analyzer_);

      // Register struct paths (including those expanded from star expressions) before
      // analyzeSelectClause() to guarantee tuple memory sharing between structs and
      // struct members. See registerStructSlotRefPathsWithAnalyzer().
      collectStarExpandedPaths();
      registerStructSlotRefPathsWithAnalyzer();

      analyzeSelectClause();
      verifyResultExprs();
      registerViewColumnPrivileges();
      analyzeWhereClause();
      createSortInfo(analyzer_);

      setZippingUnnestSlotRefsFromViews();

      // Analyze aggregation-relevant components of the select block (Group By
      // clause, select list, Order By clause), substitute AVG with SUM/COUNT,
      // create the AggregationInfo, including the agg output tuple, and transform
      // all post-agg exprs given AggregationInfo's smap.
      analyzeHavingClause();
      if (checkForAggregates()) {
        verifyAggSemantics();
        analyzeGroupingExprs();
        collectAggExprs();
        buildAggregateExprs();
        buildResultExprs();
        verifyAggregation();
      }

      createAnalyticInfo();
      if (evaluateOrderBy_) createSortTupleInfo(analyzer_);

      // Remember the SQL string before inline-view expression substitution.
      sqlString_ = toSql();
      if (origSqlString_ == null) origSqlString_ = sqlString_;
      resolveInlineViewRefs();

      // If this block's select-project-join portion returns an empty result set and the
      // block has no aggregation, then mark this block as returning an empty result set.
      if (analyzer_.hasEmptySpjResultSet() && multiAggInfo_ == null) {
        analyzer_.setHasEmptyResultSet();
      }

      buildColumnLineageGraph();
      analyzer_.setSimpleLimitStatus(checkSimpleLimitStmt());
    }

    /** Ensure that embedded (struct member) expressions share the tuple memory of their
     * enclosing struct expressions by registering struct paths before analysis of select
     * list items. Struct paths are registered in order of increasing number of path
     * elements - this guarantees that when a struct member path is registered, its
     * enclosing struct has already been registered, so Analyzer.registerSlotRef() can
     * return the SlotDescriptor already created for the struct member within the struct,
     * instead of creating a new SlotDescriptor for the struct member outside of the
     * struct.
     *
     * Note that struct members can themselves be structs: this is the reason that
     * ordering by increasing path element number (increasing embedding depth) is
     * necessary and simply registering struct paths before other paths is not enough.
     */
    private void registerStructSlotRefPathsWithAnalyzer() throws AnalysisException {
      Stream<Path> nonStarPaths = collectNonStarPaths();
      Stream<Path> starExpandedPaths = starExpandedPaths_.values().stream()
          // Get a flat list of paths (and booleans) belonging to all star items.
          .flatMap(pathList -> pathList.stream())
          // Discard the booleans and keep only the actual paths.
          .map((StarExpandedPathInfo pathInfo)  -> pathInfo.getExpandedPath());

      Stream<Path> allPaths = Stream.concat(nonStarPaths, starExpandedPaths);
      List<Path> structPaths = allPaths
          .filter(path -> path.destType().isStructType())
          .collect(Collectors.toList());

      // Sort paths by length in ascending order so that structs that contain other
      // structs come before their children.
      Collections.sort(structPaths,
          Comparator.<Path>comparingInt(path -> path.getMatchedTypes().size()));
      for (Path p : structPaths) {
        analyzer_.registerSlotRef(p);
      }
    }

    private Stream<Path> collectNonStarPaths() {
      Preconditions.checkNotNull(selectList_);
      Stream<Expr> selectListExprs = selectList_.getItems().stream()
        .filter(elem -> !elem.isStar())
        .map(elem -> elem.getExpr());
      Stream<Expr> nonSelectListExprs = collectExprsOutsideSelectList();

      Stream<Expr> exprs = Stream.concat(selectListExprs, nonSelectListExprs);

      // Use a LinkedHashSet for deterministic iteration order.
      LinkedHashSet<SlotRef> slotRefs = new LinkedHashSet<>();
      exprs.forEach(expr -> expr.collect(SlotRef.class, slotRefs));

      return slotRefs.stream()
          .map(this::slotRefToResolvedPath)
          .filter(path -> path != null);
    }

    private Stream<Expr> collectExprsOutsideSelectList() {
      Stream<Expr> res = Stream.empty();
      if (whereClause_ != null) {
        res = Stream.concat(res, Stream.of(whereClause_));
      }
      if (havingClause_ != null) {
        res = Stream.concat(res, Stream.of(havingClause_));
      }
      if (groupingExprs_ != null) {
        res = Stream.concat(res, groupingExprs_.stream());
      }
      if (sortInfo_ != null) {
        res = Stream.concat(res, sortInfo_.getSortExprs().stream());
      }
      if (analyticInfo_ != null) {
        res = Stream.concat(res,
            analyticInfo_.getAnalyticExprs().stream()
            .map(analyticExpr -> (Expr) analyticExpr));
      }
      return res;
    }

    private Path slotRefToResolvedPath(SlotRef slotRef) {
      try {
        Path resolvedPath = analyzer_.resolvePathWithMasking(slotRef.getRawPath(),
            PathType.SLOT_REF);
        return resolvedPath;
      } catch (TableLoadingException e) {
        // Should never happen because we only check registered table aliases.
        Preconditions.checkState(false);
        return null;
      } catch (AnalysisException e) {
        // Return null if analysis did not succeed. This means this path will not be
        // registered here.
        return null;
      }
    }

    private void analyzeSelectClause() throws AnalysisException {
      // Generate !empty() predicates to filter out empty collections.
      // Skip this step when analyzing a WITH-clause because CollectionTableRefs
      // do not register collection slots in their parent in that context
      // (see CollectionTableRef.analyze()).
      if (!analyzer_.hasWithClause()) registerIsNotEmptyPredicates();

      // analyze plan hints from select list
      selectList_.analyzePlanHints(analyzer_);

      // populate resultExprs_, aliasSmap_, and colLabels_
      // This additional map is used for performance reasons and not for finding
      // ambiguous alias.
      Map<String, Expr> existingAliasExprs = new LinkedHashMap<>();
      for (int i = 0; i < selectList_.getItems().size(); ++i) {
        SelectListItem item = selectList_.getItems().get(i);
        if (item.isStar()) {
          analyzeStarItem(item);
        } else {
          analyzeNonStarItem(item, existingAliasExprs, i);
        }
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Analyzed select clause aliasSmap={}", aliasSmap_.debugString());
      }
    }

    private void analyzeStarItem(SelectListItem item) throws AnalysisException {
      Preconditions.checkState(item.isStar());
      List<StarExpandedPathInfo> starExpandedPathInfos = starExpandedPaths_.get(item);
      // If complex types are not expanded, a star item may expand to zero items, in which
      // case starExpandedPaths_ does not have a value for it.
      if (starExpandedPathInfos == null) {
        Preconditions.checkState(
            !analyzer_.getQueryCtx().client_request.query_options.expand_complex_types);
        return;
      }

      for (StarExpandedPathInfo pathInfo : starExpandedPathInfos) {
        addStarExpandedPathResultExpr(pathInfo);
      }
    }

    private void analyzeNonStarItem(SelectListItem item,
        Map<String, Expr> existingAliasExprs, int selectListPos)
        throws AnalysisException {
      // Analyze the resultExpr before generating a label to ensure enforcement
      // of expr child and depth limits (toColumn() label may call toSql()).
      item.getExpr().analyze(analyzer_);
      // Check for scalar subquery types which are not supported
      List<Subquery> subqueryExprs = new ArrayList<>();
      item.getExpr().collect(Subquery.class, subqueryExprs);
      for (Subquery s : subqueryExprs) {
        Preconditions.checkState(s.getStatement() instanceof SelectStmt);
        if (!s.returnsScalarColumn()) {
          throw new AnalysisException("A non-scalar subquery is not supported in "
              + "the expression: " + item.getExpr().toSql());
        }
        if (s.getStatement().isRuntimeScalar()) {
          throw new AnalysisException(
              "A subquery which may return more than one row is not supported in "
              + "the expression: " + item.getExpr().toSql());
        }
        if (!((SelectStmt)s.getStatement()).returnsAtMostOneRow()) {
          throw new AnalysisException("Only subqueries that are guaranteed to return "
              + "a single row are supported: " + item.getExpr().toSql());
        }
      }
      resultExprs_.add(item.getExpr());
      String label = item.toColumnLabel(selectListPos, analyzer_.useHiveColLabels());
      SlotRef aliasRef = new SlotRef(label);
      Expr existingAliasExpr = existingAliasExprs.get(label);
      if (existingAliasExpr != null && !existingAliasExpr.equals(item.getExpr())) {
        // If we have already seen this alias, it refers to more than one column and
        // therefore is ambiguous.
        ambiguousAliasList_.add(aliasRef);
      } else {
        existingAliasExprs.put(label, item.getExpr());
      }
      aliasSmap_.put(aliasRef, item.getExpr().clone());
      colLabels_.add(label);
    }

    private void verifyResultExprs() throws AnalysisException {
      // Star exprs only expand to the scalar-typed columns/fields, so
      // the resultExprs_ could be empty.
      if (resultExprs_.isEmpty()) {
        throw new AnalysisException("The star exprs expanded to an empty select list " +
            "because the referenced tables only have complex-typed columns.\n" +
            "Star exprs only expand to scalar-typed columns because " +
            "currently not all complex-typed exprs " +
            "are supported in the select list.\n" +
            "Affected select statement:\n" + toSql());
      }

      for (Expr expr: resultExprs_) {
        if (selectList_.isDistinct() && expr.getType().isComplexType()) {
          throw new AnalysisException("Complex types are not supported " +
              "in SELECT DISTINCT clauses. Expr: '" + expr.toSql() + "', type: '"
              + expr.getType().toSql() + "'.");
        }

        if (expr.getType().isArrayType()) {
          ArrayType arrayType = (ArrayType) expr.getType();
          if (!arrayType.getItemType().isSupported()) {
            throw new AnalysisException("Unsupported type '" +
                expr.getType().toSql() + "' in '" + expr.toSql() + "'.");
          }
        } else if (expr.getType().isMapType()) {
          MapType mapType = (MapType) expr.getType();
          if (!mapType.getKeyType().isSupported()
              || !mapType.getValueType().isSupported()) {
            throw new AnalysisException("Unsupported type '" +
                expr.getType().toSql() + "' in '" + expr.toSql() + "'.");
          }
        }
        if (!expr.getType().isSupported()) {
          throw new AnalysisException("Unsupported type '"
              + expr.getType().toSql() + "' in '" + expr.toSql() + "'.");
        }
      }

      if (TreeNode.contains(resultExprs_, AnalyticExpr.class)) {
        if (fromClause_.isEmpty()) {
          throw new AnalysisException("Analytic expressions require FROM clause.");
        }

        // do this here, not after analyzeAggregation(), otherwise the AnalyticExprs
        // will get substituted away
        if (selectList_.isDistinct()) {
          throw new AnalysisException(
              "cannot combine SELECT DISTINCT with analytic functions");
        }
      }
    }

    private void analyzeWhereClause() throws AnalysisException {
      if (whereClause_ == null) return;
      whereClause_.analyze(analyzer_);
      if (whereClause_.contains(Expr.IS_AGGREGATE)) {
        throw new AnalysisException(
            "aggregate function not allowed in WHERE clause");
      }
      whereClause_.checkReturnsBool("WHERE clause", false);
      Expr e = whereClause_.findFirstOf(AnalyticExpr.class);
      if (e != null) {
        throw new AnalysisException(
            "WHERE clause must not contain analytic expressions: " + e.toSql());
      }

      verifyZippingUnnestSlots();

      analyzer_.registerConjuncts(whereClause_, false);
    }

    /**
     * Don't allow a WHERE conjunct on an array item that is part of a zipping unnest.
     * In case there is only one zipping unnested array this restriction is not needed
     * as the UNNEST node has to handle a single array and it's safe to do the filtering
     * in the scanner.
     */
    private void verifyZippingUnnestSlots() throws AnalysisException {
      if (analyzer_.getNumZippingUnnests() <= 1) return;
      List<TupleId> zippingUnnestTupleIds = Lists.newArrayList(
          analyzer_.getZippingUnnestTupleIds());

      List<SlotRef> slotRefsInWhereClause = new ArrayList<>();
      whereClause_.collect(SlotRef.class, slotRefsInWhereClause);
      for (SlotRef slotRef : slotRefsInWhereClause) {
        if (slotRef.isBoundByTupleIds(zippingUnnestTupleIds)) {
          throw new AnalysisException("Not allowed to add a filter on an unnested " +
              "array under the same select statement: " + slotRef.toSql());
        }
      }
    }

    /**
     * When zipping unnest is performed using the SQL standard compliant syntax (where
     * the unnest is in the FROM clause) the SlotRefs used for the zipping unnest are not
     * UnnestExprs as with the other approach (where zipping unnest is in the select list)
     * but regular SlotRefs. As a result they have to be marked so that later on anything
     * specific for zipping unnest could be executed for them.
     * This function identifies the SlotRefs that are for zipping unnesting and sets a
     * flag for them. Note, only marks the SlotRefs that are originated form a view.
     */
    private void setZippingUnnestSlotRefsFromViews() {
      for (TableRef tblRef : fromClause_.getTableRefs()) {
        if (!tblRef.isFromClauseZippingUnnest()) continue;
        Preconditions.checkState(tblRef instanceof CollectionTableRef);
        ExprSubstitutionMap exprSubMap = getBaseTableSMapFromTableRef(tblRef);
        if (exprSubMap == null) continue;

        for (SelectListItem item : selectList_.getItems()) {
          if (item.isStar()) continue;
          Expr itemExpr = item.getExpr();
          List<SlotRef> slotRefs = new ArrayList<>();
          itemExpr.collect(SlotRef.class, slotRefs);
          for (SlotRef slotRef : slotRefs) {
            Expr subbedExpr = exprSubMap.get(slotRef);
            if (subbedExpr == null || !(subbedExpr instanceof SlotRef)) continue;
            SlotRef subbedSlotRef = (SlotRef)subbedExpr;
            CollectionTableRef collTblRef = (CollectionTableRef)tblRef;
            SlotRef collectionSlotRef = (SlotRef)collTblRef.getCollectionExpr();
            // Check if 'slotRef' is originated from 'collectionSlotRef'.
            if (subbedSlotRef.getDesc().getParent().getId() ==
                collectionSlotRef.getDesc().getItemTupleDesc().getId()) {
              slotRef.setIsZippingUnnest(true);
            }
          }
        }
      }
    }

    /**
     * If 'tblRef' is originated from a view then returns the baseTblSmap from the view.
     * Returns false otherwise.
     */
    private ExprSubstitutionMap getBaseTableSMapFromTableRef(TableRef tblRef) {
      if (tblRef.getResolvedPath().getRootDesc() == null) return null;
      if (tblRef.getResolvedPath().getRootDesc().getSourceView() == null) return null;
      return tblRef.getResolvedPath().getRootDesc().getSourceView().getBaseTblSmap();
    }

    /**
     * Generates and registers !empty() predicates to filter out empty
     * collections directly in the parent scan of collection table refs. This is
     * a performance optimization to avoid the expensive processing of empty
     * collections inside a subplan that would yield an empty result set.
     *
     * For correctness purposes, the predicates are generated in cases where we
     * can ensure that they will be assigned only to the parent scan, and no other
     * plan node.
     *
     * The conditions are as follows:
     * - collection table ref is relative and non-correlated
     * - collection table ref represents the rhs of an inner/cross/semi join
     * - collection table ref's parent tuple is not outer joined
     *
     * Example: table T has field A which is of type array<array<int>>.
     * 1) ... T join T.A a join a.item a_nest ... :
     *                       all nodes on the path T -> a -> a_nest
     *                       are required so are checked for !empty.
     * 2) ... T left outer join T.A a join a.item a_nest ... : no !empty.
     * 3) ... T join T.A a left outer join a.item a_nest ... :
     *                       a checked for !empty.
     * 4) ... T left outer join T.A a left outer join a.item a_nest ... :
     *                       no !empty.
     *
     *
     * TODO: In some cases, it is possible to generate !empty() predicates for
     * a correlated table ref, but in general, that is not correct for non-trivial
     * query blocks. For example, if the block with the correlated ref has an
     * aggregation then adding a !empty() predicate would incorrectly discard rows
     * from the final result set.
     *
     * TODO: Evaluating !empty() predicates at non-scan nodes interacts poorly with
     * our BE projection of collection slots. For example, rows could incorrectly
     * be filtered if a !empty() predicate is assigned to a plan node that comes
     * after the unnest of the collection that also performs the projection.
     */
    private void registerIsNotEmptyPredicates() throws AnalysisException {
      for (TableRef tblRef: fromClause_.getTableRefs()) {
        Preconditions.checkState(tblRef.isResolved());
        if (!(tblRef instanceof CollectionTableRef)) continue;
        CollectionTableRef ref = (CollectionTableRef) tblRef;
        // Skip non-relative and correlated refs.
        if (!ref.isRelative() || ref.isCorrelated()) continue;
        // Skip outer and anti joins.
        if (ref.getJoinOp().isOuterJoin() || ref.getJoinOp().isAntiJoin()) continue;
        // Do not generate a predicate if the parent tuple is outer joined.
        if (analyzer_.isOuterJoined(ref.getResolvedPath().getRootDesc().getId()))
          continue;
        // Don't push down the "is not empty" predicate for zipping unnests if there are
        // multiple zipping unnests in the FROM clause.
        if (tblRef.isZippingUnnest() && analyzer_.getNumZippingUnnests() > 1) {
          continue;
        }
        IsNotEmptyPredicate isNotEmptyPred =
            new IsNotEmptyPredicate(ref.getCollectionExpr().clone());
        isNotEmptyPred.analyze(analyzer_);
        // Register the predicate as an On-clause conjunct because it should only
        // affect the result of this join and not the whole FROM clause.
        analyzer_.registerOnClauseConjuncts(
            Lists.<Expr>newArrayList(isNotEmptyPred), ref);
      }
    }

    /**
      * Populates baseTblSmap_ with our combined inline view smap and creates
      * baseTblResultExprs.
      */
    private void resolveInlineViewRefs()
        throws AnalysisException {
      // Gather the inline view substitution maps from the enclosed inline views
      for (TableRef tblRef: fromClause_) {
        if (tblRef instanceof InlineViewRef) {
          InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
          baseTblSmap_ =
              ExprSubstitutionMap.combine(baseTblSmap_, inlineViewRef.getBaseTblSmap());
        }
      }
      baseTblResultExprs_ =
          Expr.trySubstituteList(resultExprs_, baseTblSmap_, analyzer_, false);
      if (LOG.isTraceEnabled()) {
        LOG.trace("baseTblSmap_: " + baseTblSmap_.debugString());
        LOG.trace("resultExprs: " + Expr.debugString(resultExprs_));
        LOG.trace("baseTblResultExprs: " + Expr.debugString(baseTblResultExprs_));
      }
    }

    /**
     * Resolves the given raw path as a STAR path and checks its legality.
     * Returns the resolved legal path, or throws if the raw path could not
     * be resolved or is an illegal star path.
     */
    private Path analyzeStarPath(List<String> rawPath, Analyzer analyzer)
        throws AnalysisException {
      Path resolvedPath = null;
      try {
        resolvedPath = analyzer.resolvePathWithMasking(rawPath, PathType.STAR);
      } catch (TableLoadingException e) {
        // Should never happen because we only check registered table aliases.
        Preconditions.checkState(false);
      }
      Preconditions.checkNotNull(resolvedPath);
      return resolvedPath;
    }

    /**
     * Expand "*" select list item, ignoring semi-joined tables because those are
     * currently illegal in any select list (even for inline views, etc.). Also ignores
     * complex-typed fields for backwards compatibility unless EXPAND_COMPLEX_TYPES is set
     * to true.
     */
    private void expandStar(SelectListItem selectListItem) throws AnalysisException {
      if (fromClause_.isEmpty()) {
        throw new AnalysisException(
            "'*' expression in select list requires FROM clause.");
      }
      // expand in From clause order
      for (TableRef tableRef: fromClause_) {
        if (tableRef.isHidden()) continue;
        if (analyzer_.isSemiJoined(tableRef.getId())) continue;
        Path resolvedPath = new Path(tableRef.getDesc(),
            Collections.<String>emptyList());
        Preconditions.checkState(resolvedPath.resolve());
        expandStar(selectListItem, resolvedPath);
      }
    }

    /**
     * Expand "path.*" from a resolved path, ignoring complex-typed fields for backwards
     * compatibility unless EXPAND_COMPLEX_TYPES is set to true.
     */
    private void expandStar(SelectListItem selectListItem, Path resolvedPath)
        throws AnalysisException {
      Preconditions.checkState(resolvedPath.isResolved());
      if (resolvedPath.destTupleDesc() != null &&
          resolvedPath.destTupleDesc().getTable() != null &&
          resolvedPath.destTupleDesc().getPath().getMatchedTypes().isEmpty()) {
        // The resolved path targets a registered tuple descriptor of a catalog
        // table. Expand the '*' based on the Hive-column order.
        TupleDescriptor tupleDesc = resolvedPath.destTupleDesc();
        FeTable table = tupleDesc.getTable();
        for (Column c: table.getColumnsInHiveOrder()) {
          // Omit auto-incrementing column for Kudu table since it's a hidden column.
          if (c instanceof KuduColumn && ((KuduColumn)c).isAutoIncrementing()) continue;
          addStarExpandedPath(selectListItem, resolvedPath, c.getName());
        }
      } else {
        // The resolved path does not target the descriptor of a catalog table.
        // Expand '*' based on the destination type of the resolved path.
        Preconditions.checkState(resolvedPath.destType().isStructType());
        StructType structType = (StructType) resolvedPath.destType();
        Preconditions.checkNotNull(structType);

        // Star expansion for references to nested collections.
        // Collection Type                    Star Expansion
        // array<int>                     --> item
        // array<struct<f1,f2,...,fn>>    --> f1, f2, ..., fn
        // map<int,int>                   --> key, value
        // map<int,struct<f1,f2,...,fn>>  --> key, f1, f2, ..., fn
        if (structType instanceof CollectionStructType) {
          CollectionStructType cst = (CollectionStructType) structType;
          if (cst.isMapStruct()) {
            addStarExpandedPath(selectListItem, resolvedPath, Path.MAP_KEY_FIELD_NAME);
          }
          if (cst.getOptionalField().getType().isStructType()) {
            structType = (StructType) cst.getOptionalField().getType();
            for (StructField f: structType.getFields()) {
              addStarExpandedPath(selectListItem, resolvedPath,
                  cst.getOptionalField().getName(), f.getName());
            }
          } else if (cst.isMapStruct()) {
            addStarExpandedPath(selectListItem, resolvedPath, Path.MAP_VALUE_FIELD_NAME);
          } else {
            addStarExpandedPath(selectListItem, resolvedPath, Path.ARRAY_ITEM_FIELD_NAME);
          }
        } else {
          // Default star expansion.
          for (StructField f: structType.getFields()) {
            if (f.isHidden()) continue;
            addStarExpandedPath(selectListItem, resolvedPath, f.getName());
          }
        }
      }
    }

    /**
     * Expand star items to paths and store them in 'starExpandedPaths_'.
     */
    private void collectStarExpandedPaths() throws AnalysisException {
      for (SelectListItem item : selectList_.getItems()) {
        if (item.isStar()) {
          if (item.getRawPath() != null) {
            Path resolvedPath = analyzeStarPath(item.getRawPath(), analyzer_);
            expandStar(item, resolvedPath);
          } else {
            expandStar(item);
          }
        }
      }
    }

    /**
     * Helper function used during star expansion to add a single expanded path based on a
     * given raw path to be resolved relative to an existing path.
     */
    private void addStarExpandedPath(SelectListItem selectListItem, Path resolvedRootPath,
        String... relRawPath) throws AnalysisException {
      Path starExpandedPath = Path.createRelPath(resolvedRootPath, relRawPath);
      Preconditions.checkState(starExpandedPath.resolve());
      if (starExpandedPath.destType().isComplexType() &&
          !starExpandedPath.comesFromIcebergMetadataTable() &&
          !analyzer_.getQueryCtx().client_request.query_options.expand_complex_types) {
        return;
      }

      if (!starExpandedPaths_.containsKey(selectListItem)) {
        starExpandedPaths_.put(selectListItem, new ArrayList<>());
      }
      List<StarExpandedPathInfo> pathsOfStarItem = starExpandedPaths_.get(selectListItem);
      pathsOfStarItem.add(new StarExpandedPathInfo(starExpandedPath, resolvedRootPath));
    }

    private void addStarExpandedPathResultExpr(StarExpandedPathInfo starExpandedPathInfo)
        throws AnalysisException {
      Preconditions.checkState(starExpandedPathInfo.getExpandedPath().isResolved());

      SlotDescriptor slotDesc = analyzer_.registerSlotRef(
          starExpandedPathInfo.getExpandedPath(), false);
      SlotRef slotRef = new SlotRef(slotDesc);
      if (slotRef.getType().isStructType()) {
        slotRef.checkForUnsupportedStructFeatures();
      }
      Preconditions.checkState(slotRef.isAnalyzed(),
          "Analysis should be done in constructor");

      if (starExpandedPathInfo.shouldRegisterForColumnMasking()) {
        analyzer_.registerColumnForMasking(slotDesc);
      }
      resultExprs_.add(slotRef);
      final List<String> starExpandedRawPath = starExpandedPathInfo
        .getExpandedPath().getRawPath();
      colLabels_.add(starExpandedRawPath.get(starExpandedRawPath.size() - 1));
    }

    /**
     * Registers view column privileges only when the view a catalog view.
     */
    private void registerViewColumnPrivileges() {
      for (TableRef tableRef: fromClause_.getTableRefs()) {
        if (!(tableRef instanceof InlineViewRef)) continue;
        InlineViewRef inlineViewRef = (InlineViewRef) tableRef;
        FeView view = inlineViewRef.getView();
        // For, local views (CTE), the view definition is explicitly defined in the query,
        // this requires registering base column privileges instead of view column
        // privileges. For example:
        //
        // with v as (select id as foo from functional.alltypes) select foo from v
        //
        // The query will register "functional.alltypes.id" column instead of
        // "v.foo" column. This behavior is similar to inline views, e.g.
        //
        // select foo from (select id as foo from functional.alltypes) v
        //
        // The query will register "functional.alltypes.id" column instead of "v.foo"
        // column.
        //
        // Contrasting this with catalog views.
        //
        // create view v(foo) as select id from functional.alltypes
        // select v.foo from v
        //
        // The "select v.foo from v" query requires "v.foo" view column privilege because
        // the "v" view definition is not exposed in the query. This behavior is also
        // consistent with view access, such that the query requires a "select" privilege
        // on "v" view and not "functional.alltypes" table.
        boolean isCatalogView = view != null && !view.isLocalView();
        if (!isCatalogView) continue;
        for (Expr expr: getResultExprs()) {
          List<Expr> slotRefs = new ArrayList<>();
          expr.collectAll(Predicates.instanceOf(SlotRef.class), slotRefs);
          for (Expr e: slotRefs) {
            SlotRef slotRef = (SlotRef) e;
            analyzer_.registerPrivReq(builder -> builder
                .allOf(Privilege.SELECT)
                .onColumn(view.getDb().getName(), view.getName(),
                    slotRef.getDesc().getLabel(), view.getOwnerUser())
                .build());
          }
        }
      }
    }

    /**
     * Analyze the HAVING clause. The HAVING clause is a predicate, not a list
     * (like GROUP BY or ORDER BY) and so cannot contain ordinals.
     *
     * At present, Impala's alias substitution logic only works for top-level
     * expressions in a list. Since HAVING is a predicate, alias substitution
     * does not work (the top-level predicate in HAVING is a Boolean expression.)
     *
     * TODO: Modify alias substitution to work like column resolution so that aliases
     * can appear anywhere in the expression. Then, enable alias substitution in HAVING.
     */
    private void analyzeHavingClause() throws AnalysisException {
      // Analyze the HAVING clause first so we can check if it contains aggregates.
      // We need to analyze/register it even if we are not computing aggregates.
      if (havingClause_ == null) return;
      List<Expr> subqueries = new ArrayList<>();
      havingClause_.collectAll(Predicates.instanceOf(Subquery.class), subqueries);
      if (subqueries.size() > 1) {
        throw new AnalysisException(
            "Multiple subqueries are not supported in expression: "
            + havingClause_.toSql());
      }
      // Resolve (top-level) aliases and analyzes
      havingPred_ = resolveReferenceExpr(havingClause_, "HAVING", analyzer_,
          BackendConfig.INSTANCE.getAllowOrdinalsInHaving());
      // can't contain analytic exprs
      Expr analyticExpr = havingPred_.findFirstOf(AnalyticExpr.class);
      if (analyticExpr != null) {
        throw new AnalysisException(
            "HAVING clause must not contain analytic expressions: "
               + analyticExpr.toSql());
      }
      havingPred_.checkReturnsBool("HAVING clause", true);
    }

    private boolean checkForAggregates() throws AnalysisException {
      if (!hasAggregate(/*includeDistinct=*/ true)) {
        // We're not computing aggregates but we still need to register the HAVING
        // clause which could, e.g., contain a constant expression evaluating to false.
        if (havingPred_ != null) analyzer_.registerConjuncts(havingPred_, true);
        return false;
      }
      return true;
    }

    private void verifyAggSemantics() throws AnalysisException {
      // If we're computing an aggregate, we must have a FROM clause.
      if (fromClause_.isEmpty()) {
        throw new AnalysisException(
            "aggregation without a FROM clause is not allowed");
      }

      if (selectList_.isDistinct()
          && (groupingExprs_ != null
              || TreeNode.contains(resultExprs_, Expr.IS_AGGREGATE)
              || (havingPred_ != null
                  && havingPred_.contains(Expr.IS_AGGREGATE)))) {
        throw new AnalysisException(
          "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
      }

      // Disallow '*' with explicit GROUP BY or aggregation function (we can't group by
      // '*', and if you need to name all star-expanded cols in the group by clause you
      // might as well do it in the select list).
      if (groupingExprs_ != null ||
          TreeNode.contains(resultExprs_, Expr.IS_AGGREGATE)) {
        for (SelectListItem item : selectList_.getItems()) {
          if (item.isStar()) {
            throw new AnalysisException(
                "cannot combine '*' in select list with grouping or aggregation");
          }
        }
      }

      // disallow subqueries in the GROUP BY clause
      if (groupingExprs_ != null) {
        for (Expr expr: groupingExprs_) {
          if (expr.contains(Predicates.instanceOf(Subquery.class))) {
            throw new AnalysisException(
                "Subqueries are not supported in the GROUP BY clause.");
          }
        }
      }
    }

    private void analyzeGroupingExprs() throws AnalysisException {
      if (groupingExprs_ == null) {
        groupingExprsCopy_ = new ArrayList<>();
        return;
      }
      // make a deep copy here, we don't want to modify the original
      // exprs during analysis (in case we need to print them later)
      groupingExprsCopy_ = Expr.cloneList(groupingExprs_);
      substituteOrdinalsAndAliases(groupingExprsCopy_, "GROUP BY", analyzer_);

      for (int i = 0; i < groupingExprsCopy_.size(); ++i) {
        groupingExprsCopy_.get(i).analyze(analyzer_);
        if (groupingExprsCopy_.get(i).contains(Expr.IS_AGGREGATE)) {
          // reference the original expr in the error msg
          throw new AnalysisException(
              "GROUP BY expression must not contain aggregate functions: "
                  + groupingExprs_.get(i).toSql());
        }
        if (groupingExprsCopy_.get(i).contains(AnalyticExpr.class)) {
          // reference the original expr in the error msg
          throw new AnalysisException(
              "GROUP BY expression must not contain analytic expressions: "
                  + groupingExprsCopy_.get(i).toSql());
        }
      }

      if (groupByClause_ != null && groupByClause_.hasGroupingSets()) {
        groupByClause_.analyzeGroupingSets(groupingExprsCopy_);
      }
    }

    private void collectAggExprs() {
      // Collect the aggregate expressions from the SELECT, HAVING and ORDER BY clauses
      // of this statement.
      aggExprs_ = new ArrayList<>();
      TreeNode.collect(resultExprs_, Expr.IS_AGGREGATE, aggExprs_);
      if (havingPred_ != null) {
        havingPred_.collect(Expr.IS_AGGREGATE, aggExprs_);
      }
      if (sortInfo_ != null) {
        // TODO: Avoid evaluating aggs in ignored order-bys
        TreeNode.collect(sortInfo_.getSortExprs(), Expr.IS_AGGREGATE,
            aggExprs_);
      }
    }

    private void buildAggregateExprs() throws AnalysisException {
      // When DISTINCT aggregates are present, non-distinct (i.e. ALL) aggregates are
      // evaluated in two phases (see AggregateInfo for more details). In particular,
      // COUNT(c) in "SELECT COUNT(c), AGG(DISTINCT d) from R" is transformed to
      // "SELECT SUM(cnt) FROM (SELECT COUNT(c) as cnt from R group by d ) S".
      // Since a group-by expression is added to the inner query it returns no rows if
      // R is empty, in which case the SUM of COUNTs will return NULL.
      // However the original COUNT(c) should have returned 0 instead of NULL in this
      // case.
      // Therefore, COUNT([ALL]) is transformed into zeroifnull(COUNT([ALL]) if
      // i) There is no GROUP-BY clause, and
      // ii) Other DISTINCT aggregates are present.
      countAllMap_ = createCountAllMap();
      List<Expr> substitutedAggs =
          Expr.substituteList(aggExprs_, countAllMap_, analyzer_, false);
      aggExprs_.clear();
      TreeNode.collect(substitutedAggs, Expr.IS_AGGREGATE, aggExprs_);

      List<Expr> groupingExprs = groupingExprsCopy_;
      if (selectList_.isDistinct()) {
        // Create multiAggInfo for SELECT DISTINCT:
        // - all select list items turn into grouping exprs
        // - there are no aggregate exprs
        Preconditions.checkState(groupingExprsCopy_.isEmpty());
        Preconditions.checkState(aggExprs_.isEmpty());
        groupingExprs = Expr.cloneList(resultExprs_);
      }
      // All expressions passed into the MultiAggregateInfo must be deduplicated.
      // The analyzed grouping sets were already deduplicated.
      Expr.removeDuplicates(aggExprs_);
      Expr.removeDuplicates(groupingExprs);
      List<List<Expr>> groupingSets =
          (groupByClause_ != null && groupByClause_.hasGroupingSets()) ?
          groupByClause_.getAnalyzedGroupingSets() : null;
      multiAggInfo_ = new MultiAggregateInfo(groupingExprs, aggExprs_, groupingSets);
      multiAggInfo_.analyze(analyzer_);
    }

    private void buildResultExprs() throws AnalysisException {
      ExprSubstitutionMap finalOutputSmap = multiAggInfo_.getOutputSmap();
      ExprSubstitutionMap combinedSmap =
          ExprSubstitutionMap.compose(countAllMap_, finalOutputSmap, analyzer_);

      // change select list, having and ordering exprs to point to agg output. We need
      // to reanalyze the exprs at this point.
      if (LOG.isTraceEnabled()) {
        LOG.trace("combined smap: " + combinedSmap.debugString());
        LOG.trace("desctbl: " + analyzer_.getDescTbl().debugString());
        LOG.trace("resultexprs: " + Expr.debugString(resultExprs_));
      }
      resultExprs_ = Expr.substituteList(resultExprs_, combinedSmap, analyzer_, false);
      if (LOG.isTraceEnabled()) {
        LOG.trace("post-agg selectListExprs: " + Expr.debugString(resultExprs_));
      }
      if (havingPred_ != null) {
        havingPred_ = havingPred_.substitute(combinedSmap, analyzer_, false);
        analyzer_.registerConjuncts(havingPred_, true);
        if (LOG.isTraceEnabled()) {
          LOG.trace("post-agg havingPred: " + havingPred_.debugString());
        }
      }
      if (sortInfo_ != null) {
        sortInfo_.substituteSortExprs(combinedSmap, analyzer_);
        if (LOG.isTraceEnabled()) {
          LOG.trace("post-agg orderingExprs: " +
              Expr.debugString(sortInfo_.getSortExprs()));
        }
      }
    }

    private void verifyAggregation() throws AnalysisException {
      // check that all post-agg exprs point to agg output
      for (int i = 0; i < selectList_.getItems().size(); ++i) {
        if (!resultExprs_.get(i).isBound(multiAggInfo_.getResultTupleId())) {
          SelectListItem selectListItem = selectList_.getItems().get(i);
          throw new AnalysisException(
              "select list expression not produced by aggregation output "
              + "(missing from GROUP BY clause?): "
              + selectListItem.toSql());
        }
      }
      if (orderByElements_ != null) {
        for (int i = 0; i < orderByElements_.size(); ++i) {
          if (!sortInfo_.getSortExprs().get(i).isBound(
              multiAggInfo_.getResultTupleId())) {
            throw new AnalysisException(
                "ORDER BY expression not produced by aggregation output "
                + "(missing from GROUP BY clause?): "
                + orderByElements_.get(i).getExpr().toSql());
          }
        }
      }
      if (havingPred_ != null) {
        if (!havingPred_.isBound(multiAggInfo_.getResultTupleId())) {
          throw new AnalysisException(
              "HAVING clause not produced by aggregation output "
              + "(missing from GROUP BY clause?): "
              + havingClause_.toSql());
        }
      }
    }

    /**
     * Create a map from COUNT([ALL]) -> zeroifnull(COUNT([ALL])) if
     * i) There is no GROUP-BY, and
     * ii) There are other distinct aggregates to be evaluated.
     * This transformation is necessary for COUNT to correctly return 0
     * for empty input relations.
     */
    private ExprSubstitutionMap createCountAllMap()
        throws AnalysisException {
      ExprSubstitutionMap scalarCountAllMap = new ExprSubstitutionMap();

      if (groupingExprs_ != null && !groupingExprs_.isEmpty()) {
        // There are grouping expressions, so no substitution needs to be done.
        return scalarCountAllMap;
      }

      com.google.common.base.Predicate<FunctionCallExpr> isNotDistinctPred =
          new com.google.common.base.Predicate<FunctionCallExpr>() {
            @Override
            public boolean apply(FunctionCallExpr expr) {
              return !expr.isDistinct();
            }
          };
      if (Iterables.all(aggExprs_, isNotDistinctPred)) {
        // Only [ALL] aggs, so no substitution needs to be done.
        return scalarCountAllMap;
      }

      com.google.common.base.Predicate<FunctionCallExpr> isCountPred =
          new com.google.common.base.Predicate<FunctionCallExpr>() {
            @Override
            public boolean apply(FunctionCallExpr expr) {
              return expr.getFnName().getFunction().equals("count");
            }
          };

      Iterable<FunctionCallExpr> countAllAggs =
          Iterables.filter(aggExprs_, Predicates.and(isCountPred, isNotDistinctPred));
      for (FunctionCallExpr countAllAgg: countAllAggs) {
        // Replace COUNT(ALL) with zeroifnull(COUNT(ALL))
        List<Expr> zeroIfNullParam = Lists.newArrayList(countAllAgg.clone());
        FunctionCallExpr zeroIfNull =
            new FunctionCallExpr("zeroifnull", zeroIfNullParam);
        zeroIfNull.analyze(analyzer_);
        scalarCountAllMap.put(countAllAgg, zeroIfNull);
      }

      return scalarCountAllMap;
    }

    /**
     * If the select list contains AnalyticExprs, create AnalyticInfo and substitute
     * AnalyticExprs using the AnalyticInfo's smap.
     */
    private void createAnalyticInfo()
        throws AnalysisException {
      // collect AnalyticExprs from the SELECT and ORDER BY clauses
      List<AnalyticExpr> analyticExprs = new ArrayList<>();
      TreeNode.collect(resultExprs_, AnalyticExpr.class, analyticExprs);
      if (sortInfo_ != null) {
        TreeNode.collect(sortInfo_.getSortExprs(), AnalyticExpr.class,
            analyticExprs);
      }
      if (analyticExprs.isEmpty()) return;
      ExprSubstitutionMap rewriteSmap = new ExprSubstitutionMap();
      for (Expr expr: analyticExprs) {
        AnalyticExpr toRewrite = (AnalyticExpr)expr;
        Expr newExpr = AnalyticExpr.rewrite(toRewrite);
        if (newExpr != null) {
          newExpr.analyze(analyzer_);
          if (!rewriteSmap.containsMappingFor(toRewrite)) {
            rewriteSmap.put(toRewrite, newExpr);
          }
        }
      }
      if (rewriteSmap.size() > 0) {
        // Substitute the exprs with their rewritten versions.
        List<Expr> updatedAnalyticExprs =
            Expr.substituteList(analyticExprs, rewriteSmap, analyzer_, false);
        // This is to get rid the original exprs which have been rewritten.
        analyticExprs.clear();
        // Collect the new exprs introduced through the rewrite and the
        // non-rewrite exprs.
        TreeNode.collect(updatedAnalyticExprs, AnalyticExpr.class, analyticExprs);
      }

      analyticInfo_ = AnalyticInfo.create(analyticExprs, analyzer_);

      ExprSubstitutionMap smap = analyticInfo_.getSmap();
      // If 'exprRewritten' is true, we have to compose the new smap with
      // the existing one.
      if (rewriteSmap.size() > 0) {
        smap = ExprSubstitutionMap.compose(
            rewriteSmap, analyticInfo_.getSmap(), analyzer_);
      }
      // change select list and ordering exprs to point to analytic output. We need
      // to reanalyze the exprs at this point.
      resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer_, false);
      if (LOG.isTraceEnabled()) {
        LOG.trace("post-analytic selectListExprs: " + Expr.debugString(resultExprs_));
      }
      if (sortInfo_ != null) {
        sortInfo_.substituteSortExprs(smap, analyzer_);
        if (LOG.isTraceEnabled()) {
          LOG.trace("post-analytic orderingExprs: " +
              Expr.debugString(sortInfo_.getSortExprs()));
        }
      }
    }

    private void buildColumnLineageGraph() {
      ColumnLineageGraph graph = analyzer_.getColumnLineageGraph();
      if (multiAggInfo_ != null && multiAggInfo_.hasAggregateExprs()) {
        graph.addDependencyPredicates(multiAggInfo_.getGroupingExprs());
      }
      if (sortInfo_ != null && hasLimit()) {
        // When there is a LIMIT clause in conjunction with an ORDER BY, the
        // ordering exprs must be added in the column lineage graph.
        graph.addDependencyPredicates(sortInfo_.getSortExprs());
      }
    }
  }

  /**
   * Marks all unassigned join predicates as well as exprs in aggInfo and sortInfo.
   */
  @Override
  public void materializeRequiredSlots(Analyzer analyzer) {
    // Mark unassigned join predicates. Some predicates that must be evaluated by a join
    // can also be safely evaluated below the join (picked up by getBoundPredicates() and
    // migrateConjunctsToInlineView()).
    // Such predicates will be marked twice and that is ok.
    List<Expr> unassigned =
        analyzer.getUnassignedConjuncts(getTableRefIds(), true);
    List<Expr> unassignedJoinConjuncts = new ArrayList<>();
    for (Expr e: unassigned) {
      if (analyzer.evalAfterJoin(e)) unassignedJoinConjuncts.add(e);
    }
    List<Expr> baseTblJoinConjuncts =
        Expr.substituteList(unassignedJoinConjuncts, baseTblSmap_, analyzer, false);
    materializeSlots(analyzer, baseTblJoinConjuncts);

    if (evaluateOrderBy_) {
      // mark ordering exprs before marking agg/analytic exprs because they could contain
      // agg/analytic exprs that are not referenced anywhere but the ORDER BY clause
      sortInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }

    if (hasAnalyticInfo()) {
      // Mark analytic exprs before marking agg exprs because they could contain agg
      // exprs that are not referenced anywhere but the analytic expr.
      // Gather unassigned predicates and mark their slots. It is not desirable
      // to account for propagated predicates because if an analytic expr is only
      // referenced by a propagated predicate, then it's better to not materialize the
      // analytic expr at all.
      List<TupleId> tids = new ArrayList<>();
      getMaterializedTupleIds(tids); // includes the analytic tuple
      List<Expr> conjuncts = analyzer.getUnassignedConjuncts(tids, false);
      // The predicates that can be bounded to KuduScanNode don't need to materialize
      // here. Because we don't need to materialize the predicates that can be evaluated
      // by Kudu.
      for (TupleId tid : tids) {
        if (analyzer.getTupleDesc(tid).getTable() instanceof FeKuduTable) {
          Iterator<Expr> iterator = conjuncts.iterator();
          while (iterator.hasNext()) {
            Expr e = iterator.next();
            List<TupleId> etids = new ArrayList<>();
            e.getIds(etids, null);
            if (1 == etids.size() && etids.get(0) == tid) {
              iterator.remove();
            }
          }
        }
      }
      materializeSlots(analyzer, conjuncts);
      analyticInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }

    if (multiAggInfo_ != null) {
      // Mark all agg slots required for conjunct evaluation as materialized before
      // calling MultiAggregateInfo.materializeRequiredSlots().
      List<Expr> conjuncts = multiAggInfo_.collectConjuncts(analyzer, false);
      materializeSlots(analyzer, conjuncts);
      multiAggInfo_.materializeRequiredSlots(analyzer, baseTblSmap_);
    }
  }

  public List<TupleId> getTableRefIds() {
    List<TupleId> result = new ArrayList<>();
    for (TableRef ref: fromClause_) {
      result.add(ref.getId());
    }
    return result;
  }

  /**
   * If given expr is rewritten into an integer literal, then return the original expr,
   * otherwise return the rewritten expr.
   * Used for GROUP BY, ORDER BY, and HAVING where we don't want to create an ordinal
   * from a constant arithmetic expr, e.g. 1 * 2 =/=> 2
   */
  private Expr rewriteCheckOrdinalResult(ExprRewriter rewriter, Expr expr)
      throws AnalysisException {
    Expr rewrittenExpr = rewriter.rewrite(expr, analyzer_);
    if (Expr.IS_LITERAL.apply(rewrittenExpr) && rewrittenExpr.getType().isIntegerType()) {
      return expr;
    } else {
      return rewrittenExpr;
    }
  }


  /**
   * Set totalRecordsNumVx_ in analyzer_ for the plain count(*) queries of Iceberg tables.
   * Queries that can be rewritten need to meet the following requirements:
   *  - stmt does not have WHERE clause
   *  - stmt does not have GROUP BY clause
   *  - stmt does not have HAVING clause
   *  - tableRefs contains only one BaseTableRef
   *  - tableRef doesn't have sampling param
   *  - table is the Iceberg table
   *  - SelectList must contains 'count(*)' or 'count(constant)'
   *  - SelectList can contain constant
   *  - stmt does not have WITH clause
   *  - only for V1: SelectList can contain other agg functions, e.g. min, sum, etc
   * e.g. 'SELECT count(*) FROM iceberg_tbl' would be rewritten as 'SELECT constant'.
   */
  public void optimizePlainCountStarQueryForIcebergTable() throws AnalysisException {
    // When optimizing the simple count star query for the Iceberg table, the WITH CLAUSE
    // should be skipped, but that doesn't mean the SQL can't be optimized, because when
    // the WITH CLAUSE is inlined, the final Stmt is optimized by CountStarToConstRule.
    if (this.analyzer_.hasWithClause()) return;

    if (this.hasWhereClause()) return;
    if (this.hasGroupByClause()) return;
    if (this.hasHavingClause()) return;

    List<TableRef> tables = this.getTableRefs();
    if (tables.size() != 1) return;
    TableRef tableRef = tables.get(0);
    if (!(tableRef instanceof BaseTableRef)) return;
    if (tableRef.getSampleParams() != null) return;

    TableName tableName = tableRef.getDesc().getTableName();
    FeTable table;
    try {
      table = analyzer_.getCatalog().getTable(tableName.getDb(), tableName.getTbl());
    } catch (DatabaseNotFoundException e) {
      throw new AnalysisException(
          Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + tableName.getDb(), e);
    }
    if (!(table instanceof FeIcebergTable)) return;
    if (analyzer_.getQueryOptions().iceberg_disable_count_star_optimization) {
      return;
    }
    analyzer_.checkStmtExprLimit();
    FeIcebergTable iceTable = ((FeIcebergTable) table);
    if (Utils.hasDeleteFiles(iceTable, tableRef.getTimeTravelSpec())) {
      optimizePlainCountStarQueryV2(tableRef, iceTable);
    } else {
      optimizePlainCountStarQueryV1(tableRef, iceTable.getIcebergApiTable());
    }
  }

  private void optimizePlainCountStarQueryV2(TableRef tableRef, FeIcebergTable table)
      throws AnalysisException {
    for (SelectListItem selectItem : getSelectList().getItems()) {
      Expr expr = selectItem.getExpr();
      if (expr == null) return;
      if (expr.isConstant()) continue;
      if (!FunctionCallExpr.isCountStarFunctionCallExpr(expr)) return;
    }
    long num = Utils.getRecordCountV2(table, tableRef.getTimeTravelSpec());
    if (num > 0) {
      analyzer_.getQueryCtx().setOptimize_count_star_for_iceberg_v2(true);
      analyzer_.setTotalRecordsNumV2(num);
    }
  }

  private void optimizePlainCountStarQueryV1(TableRef tableRef, Table iceTable) {
    boolean hasCountStarFunc = false;
    boolean hasAggFunc = false;
    for (SelectListItem selectItem : getSelectList().getItems()) {
      Expr expr = selectItem.getExpr();
      if (expr == null) return;
      if (expr.isConstant()) continue;
      if (FunctionCallExpr.isCountStarFunctionCallExpr(expr)) { hasCountStarFunc = true; }
      else if (expr.isAggregate()) { hasAggFunc = true; }
      else return;
    }
    if (!hasCountStarFunc) return;
    long num = Utils.getRecordCountV1(iceTable, tableRef.getTimeTravelSpec());
    if (num <= 0) return;
    analyzer_.setTotalRecordsNumV1(num);
    if (hasAggFunc) return;
    // When all select items are 'count(*)' or constant, 'select count(*) from ice_tbl;'
    // would need to be rewritten as 'select const;'
    fromClause_.getTableRefs().clear();
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    selectList_.rewriteExprs(rewriter, analyzer_);
    for (TableRef ref: fromClause_.getTableRefs()) ref.rewriteExprs(rewriter, analyzer_);
    List<Subquery> subqueryExprs = new ArrayList<>();
    if (whereClause_ != null) {
      whereClause_ = rewriter.rewrite(whereClause_, analyzer_);
      whereClause_.collect(Subquery.class, subqueryExprs);
    }
    if (havingClause_ != null) {
      havingClause_ = rewriteCheckOrdinalResult(rewriter, havingClause_);
      havingClause_.collect(Subquery.class, subqueryExprs);
    }
    for (Subquery s : subqueryExprs) s.getStatement().rewriteExprs(rewriter);
    if (groupingExprs_ != null) {
      for (int i = 0; i < groupingExprs_.size(); ++i) {
        groupingExprs_.set(i, rewriteCheckOrdinalResult(
            rewriter, groupingExprs_.get(i)));
      }
    }
    if (orderByElements_ != null) {
      for (OrderByElement orderByElem: orderByElements_) {
        orderByElem.setExpr(rewriteCheckOrdinalResult(rewriter, orderByElem.getExpr()));
      }
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    // Return the SQL string before inline-view expression substitution.
    if (!options.showRewritten() && sqlString_ != null) return sqlString_;

    StringBuilder strBuilder = new StringBuilder();
    if (withClause_ != null) {
      strBuilder.append(withClause_.toSql(options));
      strBuilder.append(" ");
    }

    // Select list
    strBuilder.append("SELECT ");
    if (selectList_.isDistinct()) {
      strBuilder.append("DISTINCT ");
    }
    if (selectList_.hasPlanHints()) {
      strBuilder.append(ToSqlUtils.getPlanHintsSql(options, selectList_.getPlanHints()))
          .append(" ");
    }
    for (int i = 0; i < selectList_.getItems().size(); ++i) {
      strBuilder.append(selectList_.getItems().get(i).toSql(options));
      strBuilder.append((i+1 != selectList_.getItems().size()) ? ", " : "");
    }
    // From clause
    if (!fromClause_.isEmpty()) {
      strBuilder.append(fromClause_.toSql(options));
    }
    // Where clause
    if (whereClause_ != null) {
      strBuilder.append(" WHERE ");
      List<PlanHint> predHints = whereClause_.getPredicateHints();
      if (predHints != null && predHints.size() > 0) {
        strBuilder.append(ToSqlUtils.getPlanHintsSql(options,
            predHints)).append(" ");
      }
      strBuilder.append(whereClause_.toSql(options));
    }
    // Group By clause
    if (groupByClause_ != null) {
      // Handle both analyzed (multiAggInfo_ != null) and unanalyzed cases.
      // Unanalyzed case us used to generate SQL such as for views.
      // See ToSqlUtils.getCreateViewSql().
      List<Expr> groupingExprs = multiAggInfo_ == null
          ? groupingExprs_ : multiAggInfo_.getGroupingExprs();
      strBuilder.append(groupByClause_.toSql(groupingExprs, options));
    }
    // Having clause
    if (havingClause_ != null) {
      strBuilder.append(" HAVING ");
      strBuilder.append(havingClause_.toSql(options));
    }
    // Order By clause
    if (orderByElements_ != null) {
      strBuilder.append(" ORDER BY ");
      for (int i = 0; i < orderByElements_.size(); ++i) {
        strBuilder.append(orderByElements_.get(i).toSql(options));
        strBuilder.append((i+1 != orderByElements_.size()) ? ", " : "");
      }
    }
    // Limit clause.
    strBuilder.append(limitElement_.toSql(options));
    return strBuilder.toString();
  }

  /**
   * If the select statement has a sort/top that is evaluated, then the sort tuple
   * is materialized. Else, if there is aggregation then the aggregate tuple id is
   * materialized. Otherwise, all referenced tables are materialized as long as they are
   * not semi-joined. If there are analytics and no sort, then the returned tuple
   * ids also include the logical analytic output tuple.
   */
  @Override
  public void getMaterializedTupleIds(List<TupleId> tupleIdList) {
    if (evaluateOrderBy_) {
      tupleIdList.add(sortInfo_.getSortTupleDescriptor().getId());
    } else if (multiAggInfo_ != null) {
      // Return the tuple id produced in the final aggregation step.
      tupleIdList.add(multiAggInfo_.getResultTupleId());
    } else {
      for (TableRef tblRef: fromClause_) {
        // Don't include materialized tuple ids from semi-joined table
        // refs (see IMPALA-1526)
        if (tblRef.getJoinOp().isLeftSemiJoin()) continue;
        // Remove the materialized tuple ids of all the table refs that
        // are semi-joined by the right semi/anti join.
        if (tblRef.getJoinOp().isRightSemiJoin()) tupleIdList.clear();
        tupleIdList.addAll(tblRef.getMaterializedTupleIds());
      }
    }
    // We materialize the agg tuple or the table refs together with the analytic tuple.
    if (hasAnalyticInfo() && !evaluateOrderBy_) {
      tupleIdList.add(analyticInfo_.getOutputTupleId());
    }
  }

  /**
   * C'tor for cloning.
   */
  private SelectStmt(SelectStmt other) {
    super(other);
    selectList_ = other.selectList_.clone();
    fromClause_ = other.fromClause_.clone();
    whereClause_ = (other.whereClause_ != null) ? other.whereClause_.clone() : null;
    groupingExprs_ =
        (other.groupingExprs_ != null) ? Expr.cloneList(other.groupingExprs_) : null;
    groupByClause_ =
        (other.groupByClause_ != null) ? other.groupByClause_.clone() : null;
    havingClause_ = (other.havingClause_ != null) ? other.havingClause_.clone() : null;
    colLabels_ = Lists.newArrayList(other.colLabels_);
    multiAggInfo_ = (other.multiAggInfo_ != null) ? other.multiAggInfo_.clone() : null;
    analyticInfo_ = (other.analyticInfo_ != null) ? other.analyticInfo_.clone() : null;
    sqlString_ = (other.sqlString_ != null) ? new String(other.sqlString_) : null;
    baseTblSmap_ = other.baseTblSmap_.clone();
  }

  @Override
  protected void collectTableRefs(List<TableRef> tblRefs, boolean fromClauseOnly) {
    super.collectTableRefs(tblRefs, fromClauseOnly);
    if (fromClauseOnly) {
      fromClause_.collectFromClauseTableRefs(tblRefs);
    } else {
      // Collect TableRefs in all subqueries.
      fromClause_.collectTableRefs(tblRefs);
      List<Subquery> subqueries = new ArrayList<>();
      if (whereClause_ != null) {
        whereClause_.collect(Subquery.class, subqueries);
      }
      if (havingClause_ != null) {
        havingClause_.collect(Subquery.class, subqueries);
      }
      for (SelectListItem item : selectList_.getItems()) {
        if (item.isStar()) continue;
        item.getExpr().collect(Subquery.class, subqueries);
      }

      for (Subquery sq: subqueries) {
        sq.getStatement().collectTableRefs(tblRefs, fromClauseOnly);
      }
    }
  }

  @Override
  public void collectInlineViews(Set<FeView> inlineViews) {
    super.collectInlineViews(inlineViews);
    List<TableRef> fromTblRefs = getTableRefs();
    Preconditions.checkNotNull(inlineViews);
    for (TableRef fromTblRef : fromTblRefs) {
      if (fromTblRef instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef) fromTblRef;
        inlineViews.add(inlineViewRef.getView());
        inlineViewRef.getViewStmt().collectInlineViews(inlineViews);
      }
    }
    if (whereClause_ != null) {
      for (Expr conjunct : whereClause_.getConjuncts()) {
        List<Subquery> whereSubQueries = Lists.newArrayList();
        conjunct.collect(Predicates.instanceOf(Subquery.class), whereSubQueries);
        if (whereSubQueries.size() == 0) continue;
        // Check that multiple subqueries do not exist in the same expression. This
        // should have been already caught by the analysis passes.
        Preconditions.checkState(whereSubQueries.size() == 1, "Invariant " +
            "violated: Multiple subqueries found in a single expression: " +
            conjunct.toSql());
        whereSubQueries.get(0).getStatement().collectInlineViews(inlineViews);
      }
    }
    List<Subquery> subqueries = Lists.newArrayList();
    for (SelectListItem item : selectList_.getItems()) {
      if (item.isStar()) continue;
      item.getExpr().collect(Subquery.class, subqueries);
    }
    if (havingClause_ != null) {
      havingClause_.collect(Subquery.class, subqueries);
    }
    for (Subquery sq : subqueries) {
      sq.getStatement().collectInlineViews(inlineViews);
    }
  }

  @Override
  public void reset() {
    super.reset();
    selectList_.reset();
    colLabels_.clear();
    fromClause_.reset();
    if (whereClause_ != null) whereClause_.reset();
    if (groupByClause_ != null) groupByClause_.reset();
    if (groupingExprs_ != null) Expr.resetList(groupingExprs_);
    if (havingClause_ != null) havingClause_.reset();
    havingPred_ = null;
    multiAggInfo_ = null;
    analyticInfo_ = null;
    baseTblSmap_.clear();
  }

  @Override
  public SelectStmt clone() { return new SelectStmt(this); }

  /**
   * Check if the stmt returns at most one row. This can happen
   * in the following cases:
   * 1. select stmt with a 'limit 1' clause
   * 2. select stmt with an aggregate function and no group by.
   * 3. select stmt with no from clause.
   * 4. select from an inline view that returns at most one row.
   *
   * This function may produce false negatives because the cardinality of the
   * result set also depends on the data a stmt is processing.
   *
   * TODO: IMPALA-1285 to cover more cases that can be determinded at plan time such has a
   * group by clause where all grouping expressions are bound to constant expressions.
   */
  public boolean returnsAtMostOneRow() {
    return returnsSingleRow(false);
  }

  /**
   * Check if the stmt returns exactly one row. This can happen
   * in the following cases:
   * 1. select stmt with an aggregate function and no group by and no having clause.
   * 2. select stmt with no from clause and no where or having clause.
   * 3. select from an inline view that returns exactly one row and no where or
   *    having clause.
   *
   * This function may produce false negatives because the cardinality of the
   * result set also depends on the data a stmt is processing.
   *
   * TODO: IMPALA-1285 to cover more cases that can be determinded at plan time such has a
   * group by clause where all grouping expressions are bound to constant expressions.
   */
  public boolean returnsExactlyOneRow() {
    return returnsSingleRow(true);
  }

  /**
   * Helper for returnsAtMostOneRow() and returnsExactlyOneRow(). If exactlyOne
   * is true, returns true iff this SELECT always returns exactly one row. If
   * exactlyOne is false, returns true iff this SELECT returns zero or one rows.
   */
  private boolean returnsSingleRow(boolean exactlyOne) {
    // This function checks the clauses in reverse order in which they are applied.
    // We must return false as soon as we determine there is no upper bound on the rows
    // returned.
    // If exactlyOne is true, we also must return false when we determine there is no
    // lower bound on the rows returned.
    // If exactlyOne is false, we can also return true as soon as we determine an upper
    // bound on the rows returned.

    if (limitElement_ != null && hasLimit()) {
      // Limit 0 or 1 clause. This is an upper bound on rows returned.
      if (!exactlyOne && limitElement_.getLimit() <= 1) return true;
      // Limit 0 clause - the query will return 0 rows.
      if (exactlyOne && limitElement_.getLimit() == 0) return false;
    }

    // HAVING cause can eliminate any rows produced by the statement, therefore we have
    // no lower bound on rows returned.
    if (exactlyOne && havingClause_ != null && !havingClause_.isTriviallyTrue()) {
      return false;
    }

    // Aggregation with no GROUP BY and no DISTINCT. The aggregation produces exactly one
    // row and the WHERE clause cannot eliminate that row.
    if (hasMultiAggInfo() && !hasGroupByClause() && !selectList_.isDistinct()) {
      return true;
    }

    // The WHERE clause can eliminate the singular row produced by the FROM clause
    // (or lack of FROM clause).
    if (exactlyOne && whereClause_ != null && !whereClause_.isTriviallyTrue()) {
      return false;
    }

    // No FROM clause (base tables or inline views). Exactly one row is produced.
    if (fromClause_.isEmpty()) return true;

    List<TableRef> tableRefs = fromClause_.getTableRefs();
    if (tableRefs.size() == 1 && tableRefs.get(0) instanceof InlineViewRef) {
      InlineViewRef inlineView = (InlineViewRef)tableRefs.get(0);
      if (inlineView.queryStmt_ instanceof SelectStmt) {
        SelectStmt selectStmt = (SelectStmt)inlineView.queryStmt_;
        return selectStmt.returnsSingleRow(exactlyOne);
      }
    }
    // In all other cases, return false.
    return false;
  }

  /**
   * @param includeDistinct if true, a distinct in the select list is counted
   *    as requiring an aggregation
   * @returns true if query block has an aggregate function or grouping.
   */
  public boolean hasAggregate(boolean includeDistinct) throws AnalysisException {
    return groupingExprs_ != null || (includeDistinct && selectList_.isDistinct()) ||
          TreeNode.contains(resultExprs_, Expr.IS_AGGREGATE) ||
          (havingPred_ != null && havingPred_.contains(Expr.IS_AGGREGATE)) ||
          (sortInfo_ != null &&
           TreeNode.contains(sortInfo_.getSortExprs(), Expr.IS_AGGREGATE));
  }
}
