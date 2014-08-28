package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AggregateFunction;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base class for AggregateInfo and AnalyticInfo containing the intermediate and output
 * tuple descriptors as well as their smaps for evaluating aggregate functions.
 */
public class AggregateExprsTupleInfo {
  private final static Logger LOG =
      LoggerFactory.getLogger(AggregateExprsTupleInfo.class);

  // For aggregations: All unique grouping expressions from a select block.
  // For analytics: Empty.
  protected ArrayList<Expr> groupingExprs_;

  // For aggregations: All unique aggregate expressions from a select block.
  // For analytics: All unique analytic function calls from a set of compatible
  // AnalyticExprs of the same select block.
  protected ArrayList<FunctionCallExpr> aggregateExprs_;

  // The tuple into which the intermediate output of an aggregation is materialized.
  // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
  // contain the values of the grouping exprs, followed by slots into which the
  // aggregateExprs' update()/merge() symbols materialize their output, i.e., slots
  // of the aggregate functions' intermediate types.
  // Identical to outputTupleDesc_ if no aggregateExpr has an output type that is
  // different from its intermediate type.
  protected TupleDescriptor intermediateTupleDesc_;

  // The tuple into which the final output of the aggregation is materialized.
  // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
  // contain the values of the grouping exprs, followed by slots into which the
  // aggregateExprs' finalize() symbol write its result, i.e., slots of the aggregate
  // functions' output types.
  protected TupleDescriptor outputTupleDesc_;

  // Map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
  // in the intermediate tuple. Identical to outputTupleSmap_ if no aggregateExpr has an
  // output type that is different from its intermediate type.
  protected ExprSubstitutionMap intermediateTupleSmap_ = new ExprSubstitutionMap();

  // Map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
  // in the output tuple.
  protected ExprSubstitutionMap outputTupleSmap_ = new ExprSubstitutionMap();

  // Map from slots of outputTupleSmap_ to the corresponding slot in
  // intermediateTupleSmap_.
  protected final ExprSubstitutionMap outputToIntermediateTupleSmap_ =
      new ExprSubstitutionMap();

  protected AggregateExprsTupleInfo(ArrayList<Expr> groupingExprs,
      ArrayList<FunctionCallExpr> aggExprs)  {
    Preconditions.checkState(groupingExprs != null || aggExprs != null);
    groupingExprs_ =
        (groupingExprs != null
        ? Expr.cloneList(groupingExprs)
            : new ArrayList<Expr>());
    Preconditions.checkState(aggExprs != null || !(this instanceof AnalyticInfo));
    aggregateExprs_ =
        (aggExprs != null
        ? Expr.cloneList(aggExprs)
            : new ArrayList<FunctionCallExpr>());
  }

  /**
   * Creates the intermediate and output tuple descriptors as well as their smaps.
   * If no agg expr has an intermediate type different from its output type, then
   * only the output tuple descriptor is created and the intermediate tuple/smap
   * are set to the output tuple/smap. If two different tuples are required, also
   * populates the output-to-intermediate smap and registers auxiliary equivalence
   * predicates between the grouping slots of the two tuples.
   */
  protected void createTupleDescs(Analyzer analyzer) {
    // Determine whether we need different output and intermediate tuples.
    boolean requiresIntermediateTuple = false;
    for (FunctionCallExpr aggExpr: aggregateExprs_) {
      Type intermediateType = ((AggregateFunction)aggExpr.fn_).getIntermediateType();
      if (intermediateType != null) {
        requiresIntermediateTuple = true;
        break;
      }
    }

    // Create the intermediate tuple desc first, so that the tuple ids are increasing
    // from bottom to top in the plan tree.
    intermediateTupleDesc_ = createAggTupleDesc(analyzer, false);
    if (requiresIntermediateTuple) {
      outputTupleDesc_ = createAggTupleDesc(analyzer, true);
      // Populate smap from output slots to intermediate slots, and register aux
      // equivalence predicates between the corresponding grouping slots.
      for (int i = 0; i < outputTupleDesc_.getSlots().size(); ++i) {
        outputToIntermediateTupleSmap_.put(
            new SlotRef(outputTupleDesc_.getSlots().get(i)),
            new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
        if (i < groupingExprs_.size()) {
          analyzer.createAuxEquivPredicate(
              new SlotRef(outputTupleDesc_.getSlots().get(i)),
              new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
        }
      }
    } else {
      outputTupleDesc_ = intermediateTupleDesc_;
      outputTupleSmap_ = intermediateTupleSmap_;
    }
  }

  /**
   * Returns a tuple descriptor for the aggregation/analytic's intermediate or final
   * result, depending on whether isOutputTuple is true or false.
   * Also updates the appropriate substitution map, and creates and registers auxiliary
   * equality predicates between the grouping slots and the grouping exprs.
   */
  private TupleDescriptor createAggTupleDesc(Analyzer analyzer, boolean isOutputTuple) {
    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor();
    List<Expr> exprs = Lists.newArrayListWithCapacity(
        groupingExprs_.size() + aggregateExprs_.size());
    exprs.addAll(groupingExprs_);
    exprs.addAll(aggregateExprs_);

    ExprSubstitutionMap smap =
        (isOutputTuple) ? outputTupleSmap_ : intermediateTupleSmap_;
    int aggregateExprStartIndex = groupingExprs_.size();
    for (int i = 0; i < exprs.size(); ++i) {
      Expr expr = exprs.get(i);
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(result);
      slotDesc.setLabel(expr.toSql());
      slotDesc.setStats(ColumnStats.fromExpr(expr));
      Preconditions.checkState(expr.getType().isValid());
      slotDesc.setType(expr.getType());
      if (i < aggregateExprStartIndex) {
        // register equivalence between grouping slot and grouping expr;
        // do this only when the grouping expr isn't a constant, otherwise
        // it'll simply show up as a gratuitous HAVING predicate
        // (which would actually be incorrect if the constant happens to be NULL)
        if (!expr.isConstant()) {
          analyzer.createAuxEquivPredicate(new SlotRef(slotDesc), expr.clone());
        }
      } else {
        Preconditions.checkArgument(expr instanceof FunctionCallExpr);
        FunctionCallExpr aggExpr = (FunctionCallExpr)expr;
        if (aggExpr.isMergeAggFn()) {
          slotDesc.setLabel(aggExpr.getChild(0).toSql());
        } else {
          slotDesc.setLabel(aggExpr.toSql());
        }

        // count(*) is non-nullable.
        if (aggExpr.getFnName().getFunction().equals("count")) {
          // TODO: Consider making nullability a property of types or of builtin agg fns.
          // row_number, rank, and dense_rank are non-nullable as well.
          slotDesc.setIsNullable(false);
        }
        if (!isOutputTuple) {
          Type intermediateType = ((AggregateFunction)aggExpr.fn_).getIntermediateType();
          if (intermediateType != null) {
            // Use the output type as intermediate if the function has a wildcard decimal.
            if (!intermediateType.isWildcardDecimal()) {
              slotDesc.setType(intermediateType);
            } else {
              Preconditions.checkState(expr.getType().isDecimal());
            }
          }
        }
      }
      smap.put(expr.clone(), new SlotRef(slotDesc));
    }
    String prefix = (isOutputTuple ? "result " : "intermediate ");
    LOG.trace(prefix + " tuple=" + result.debugString());
    LOG.trace(prefix + " smap=" + smap.debugString());
    return result;
  }

  public ArrayList<Expr> getGroupingExprs() { return groupingExprs_; }
  public ArrayList<FunctionCallExpr> getAggregateExprs() { return aggregateExprs_; }
  public TupleDescriptor getOutputTupleDesc() { return outputTupleDesc_; }
  public TupleDescriptor getIntermediateTupleDesc() { return intermediateTupleDesc_; }
  public TupleId getIntermediateTupleId() { return intermediateTupleDesc_.getId(); }
  public TupleId getOutputTupleId() { return outputTupleDesc_.getId(); }
  public ExprSubstitutionMap getIntermediateSmap() { return intermediateTupleSmap_; }
  public ExprSubstitutionMap getOutputSmap() { return outputTupleSmap_; }
  public ExprSubstitutionMap getOutputToIntermediateSmap() {
    return outputToIntermediateTupleSmap_;
  }
  public boolean hasDiffIntermediateTuple() {
    Preconditions.checkNotNull(intermediateTupleDesc_);
    Preconditions.checkNotNull(outputTupleDesc_);
    return intermediateTupleDesc_ != outputTupleDesc_;
  }
}
