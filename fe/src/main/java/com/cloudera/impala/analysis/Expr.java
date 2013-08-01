// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprOpcode;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Root of the expr node hierarchy.
 *
 */
abstract public class Expr extends TreeNode<Expr> implements ParseNode, Cloneable {
  // id that's unique across the entire query statement and is assigned by
  // Analyzer.registerConjuncts(); only assigned for the top-level terms of a
  // conjunction, and therefore null for most Exprs
  protected ExprId id;

  protected PrimitiveType type;  // result of analysis
  protected boolean isAnalyzed;  // true after analyze() has been called
  protected TExprOpcode opcode;  // opcode for this expr

  // Flag to indicate whether to wrap this expr's toSql() in parenthesis. Set by parser.
  // Needed for properly capturing expr precedences in the SQL string.
  protected boolean printSqlInParens = false;

  // estimated probability of a predicate evaluating to true;
  // set during analysis;
  // between 0 and 1 if valid: invalid: -1
  protected double selectivity;

  // to be used where we can't come up with a better estimate
  protected static double defaultSelectivity = 0.1;

  // estimated number of distinct values produced by Expr; invalid: -1
  // set during analysis
  protected long numDistinctValues;

  protected Expr() {
    super();
    type = PrimitiveType.INVALID_TYPE;
    opcode = TExprOpcode.INVALID_OPCODE;
    selectivity = -1.0;
    numDistinctValues = -1;
  }

  public ExprId getId() { return id; }
  protected void setId(ExprId id) { this.id = id; }
  public PrimitiveType getType() { return type; }
  public TExprOpcode getOpcode() { return opcode; }
  public double getSelectivity() { return selectivity; }
  public long getNumDistinctValues() { return numDistinctValues; }
  public void setPrintSqlInParens(boolean b) { printSqlInParens = b; }

  /* Perform semantic analysis of node and all of its children.
   * Throws exception if any errors found.
   * @see com.cloudera.impala.parser.ParseNode#analyze(com.cloudera.impala.parser.Analyzer)
   */
  public void analyze(Analyzer analyzer) throws AnalysisException {
    for (Expr child: children) {
      child.analyze(analyzer);
    }
    isAnalyzed = true;

    if (isConstant()) {
      numDistinctValues = 1;
    } else {
      // if this Expr contains slotrefs, we estimate the # of distinct values
      // to be the maximum such number for any of the slotrefs;
      // the subclass analyze() function may well want to override this, if it
      // knows better
      List<SlotRef> slotRefs = Lists.newArrayList();
      this.collect(SlotRef.class, slotRefs);
      numDistinctValues = -1;
      for (SlotRef slotRef: slotRefs) {
        numDistinctValues = Math.max(numDistinctValues, slotRef.numDistinctValues);
      }
    }
  }

  /**
   * Helper function: analyze list of exprs
   * @param exprs
   * @param analyzer
   * @throws AnalysisException
   */
  public static void analyze(List<? extends Expr> exprs, Analyzer analyzer)
      throws AnalysisException {
    for (Expr expr: exprs) {
      expr.analyze(analyzer);
    }
  }

  @Override
  public String toSql() {
    return (printSqlInParens) ? "(" + toSqlImpl() + ")" : toSqlImpl();
  }

  /**
   * Returns a SQL string representing this expr. Subclasses should override this method
   * instead of toSql() to ensure that parenthesis are properly added around the toSql().
   */
  protected abstract String toSqlImpl();

  // Convert this expr, including all children, to its Thrift representation.
  public TExpr treeToThrift() {
    TExpr result = new TExpr();
    treeToThriftHelper(result);
    return result;
  }

  // Append a flattened version of this expr, including all children, to 'container'.
  protected void treeToThriftHelper(TExpr container) {
    TExprNode msg = new TExprNode();
    msg.type = type.toThrift();
    msg.num_children = children.size();
    toThrift(msg);
    container.addToNodes(msg);
    for (Expr child: children) {
      child.treeToThriftHelper(container);
    }
  }

  // Convert this expr into msg (excluding children), which requires setting
  // msg.op as well as the expr-specific field.
  protected abstract void toThrift(TExprNode msg);

  public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
    List<TExpr> result = Lists.newArrayList();
    for (Expr expr: exprs) {
      result.add(expr.treeToThrift());
    }
    return result;
  }

  public List<String> childrenToSql() {
    List<String> result = Lists.newArrayList();
    for (Expr child: children) {
      result.add(child.toSql());
    }
    return result;
  }

  public String debugString() {
    return debugString(children);
  }

  public static String debugString(List<? extends Expr> exprs) {
    if (exprs == null || exprs.isEmpty()) return "";
    List<String> strings = Lists.newArrayList();
    for (Expr expr: exprs) {
      strings.add(expr.debugString());
    }
    return "(" + Joiner.on(" ").join(strings) + ")";
  }

  public static String toSql(List<? extends Expr> exprs) {
    if (exprs == null || exprs.isEmpty()) return "";
    List<String> strings = Lists.newArrayList();
    for (Expr expr: exprs) {
      strings.add(expr.toSql());
    }
    return "(" + Joiner.on(" ").join(strings) + ")";
  }

  /**
   * Creates a shallow copy of this Expr. If a deep copy is desired, use clone(null)
   * instead of this method.
   * This clone method takes advantage of having Java generate the field-by-field copy
   * c'tors for the Expr subclasses.
   * @see java.lang.Object#clone()
   */
  @Override
  public Expr clone() {
    try {
      return (Expr) super.clone();
    } catch (CloneNotSupportedException e) {
      // all Expr subclasses should implement Cloneable
      Writer w = new StringWriter();
      PrintWriter pw = new PrintWriter(w);
      e.printStackTrace(pw);
      throw new UnsupportedOperationException(w.toString());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    // don't compare type, this could be called pre-analysis
    Expr expr = (Expr) obj;
    if (children.size() != expr.children.size()) return false;
    for (int i = 0; i < children.size(); ++i) {
      if (!children.get(i).equals(expr.children.get(i))) return false;
    }
    return true;
  }

  /**
   * Return true if l1[i].equals(l2[i]) for all i.
   */
  public static <C extends Expr> boolean equalLists(List<C> l1, List<C> l2) {
    if (l1.size() != l2.size()) return false;
    Iterator<C> l1Iter = l1.iterator();
    Iterator<C> l2Iter = l2.iterator();
    while (l1Iter.hasNext()) {
      if (!l1Iter.next().equals(l2Iter.next())) return false;
    }
    return true;
  }

  /**
   * Return true if l1 equals l2 when both lists are interpreted as sets.
   * TODO: come up with something better than O(n^2)?
   */
  public static <C extends Expr> boolean equalSets(List<C> l1, List<C> l2) {
    if (l1.size() != l2.size()) return false;
    return l1.containsAll(l2) && l2.containsAll(l1);
  }

  /**
   * Return true if l1 is a subset of l2.
   */
  public static <C extends Expr> boolean isSubset(List<C> l1, List<C> l2) {
    if (l1.size() > l2.size()) return false;
    return l2.containsAll(l1);
  }

  @Override
  public int hashCode() {
    if (id == null) {
      throw new UnsupportedOperationException("Expr.hashCode() is not implemented");
    } else {
      return id.asInt();
    }
  }

  /**
   * Gather conjuncts from this expr and return them in a list.
   * A conjunct is an expr that returns a boolean, e.g., Predicates, function calls,
   * SlotRefs, etc. Hence, this method is placed here and not in Predicate.
   */
  public List<Expr> getConjuncts() {
    List<Expr> list = Lists.newArrayList();
    if (this instanceof CompoundPredicate
        && ((CompoundPredicate) this).getOp() == CompoundPredicate.Operator.AND) {
      // TODO: we have to convert CompoundPredicate.AND to two expr trees for
      // conjuncts because NULLs are handled differently for CompoundPredicate.AND
      // and conjunct evaluation.  This is not optimal for jitted exprs because it
      // will result in two functions instead of one. Create a new CompoundPredicate
      // Operator (i.e. CONJUNCT_AND) with the right NULL semantics and use that
      // instead
      list.addAll((getChild(0)).getConjuncts());
      list.addAll((getChild(1)).getConjuncts());
    } else {
      list.add(this);
    }
    return list;
  }

  /**
   * Map of expression substitutions (lhs[i] gets substituted with rhs[i]).
   *
   */
  static public class SubstitutionMap {
    public ArrayList<Expr> lhs;  // left-hand side
    public ArrayList<Expr> rhs;  // right-hand side

    public SubstitutionMap() {
      this.lhs = Lists.newArrayList();
      this.rhs = Lists.newArrayList();
    }

    public String debugString() {
      Preconditions.checkState(lhs.size() == rhs.size());
      List<String> output = Lists.newArrayList();
      for (int i = 0; i < lhs.size(); ++i) {
        output.add(lhs.get(i).debugString() + ":" + rhs.get(i).debugString());
      }
      return "smap(" + Joiner.on(" ").join(output) + ")";
    }

    /**
     * Create combined map which is equivalent to applying f followed by g,
     * i.e., g(f()).
     */
    public static SubstitutionMap combine(SubstitutionMap f, SubstitutionMap g) {
      SubstitutionMap result = new SubstitutionMap();
      // f's substitution targets need to be substituted via g
      result.lhs = Expr.cloneList(f.lhs, null);
      result.rhs = Expr.cloneList(f.rhs, g);
      // substitution maps are cumulative: the combined map contains all
      // substitutions from f and g
      result.lhs.addAll(Expr.cloneList(g.lhs, null));
      result.rhs.addAll(Expr.cloneList(g.rhs, null));

      // check that we don't have duplicate entries, ie, that all lhs entries are
      // unique
      for (int i = 0; i < result.lhs.size(); ++i) {
        for (int j = i + 1; j < result.lhs.size(); ++j) {
          Preconditions.checkState(!result.lhs.get(i).equals(result.lhs.get(j)));
        }
      }
      return result;
    }

    public void clear() {
      lhs.clear();
      rhs.clear();
    }
  }

  /**
   * Create a deep copy of 'this'. If sMap is non-null,
   * use it to substitute 'this' or its subnodes.
   *
   * Expr subclasses that add non-value-type members must override this.
   * @param smap
   * @return
   */
  public Expr clone(SubstitutionMap sMap) {
    if (sMap != null) {
      for (int i = 0; i < sMap.lhs.size(); ++i) {
        if (this.equals(sMap.lhs.get(i))) {
          return sMap.rhs.get(i).clone(null);
        }
      }
    }
    Expr result = (Expr) this.clone();
    result.children = Lists.newArrayList();
    for (Expr child: children) {
      result.children.add(((Expr) child).clone(sMap));
    }
    return result;
  }

  /**
   * Create a deep copy of 'l'. If sMap is non-null, use it to substitute the
   * elements of l.
   */
  public static <C extends Expr> ArrayList<C> cloneList(
      List<C> l, SubstitutionMap sMap) {
    Preconditions.checkNotNull(l);
    ArrayList<C> result = new ArrayList<C>();
    for (C element: l) {
      result.add((C) element.clone(sMap));
    }
    return result;
  }

  /**
   * Collect all unique Expr nodes of type 'cl' present in 'input' and add them to
   * 'output' if they do not exist in 'output'.
   * This can't go into TreeNode<>, because we'd be using the template param
   * NodeType.
   */
  public static <C extends Expr> void collectList(
      List<? extends Expr> input, Class<C> cl, List<C> output) {
    Preconditions.checkNotNull(input);
    for (Expr e: input) {
      e.collect(cl, output);
    }
  }

  /**
   * Return true if the list contains a node of type C in any of
   * its elements or their children, otherwise return false.
   */
  public static <C extends Expr> boolean contains(
      List<? extends Expr> input, Class<C> cl) {
    Preconditions.checkNotNull(input);
    for (Expr e: input) {
      if (e.contains(cl)) return true;
    }
    return false;
  }

  /**
   * Return 'this' with all sub-exprs substituted according to
   * sMap. Ids of 'this' and its children are retained.
   */
  public Expr substitute(SubstitutionMap sMap) {
    Preconditions.checkNotNull(sMap);
    for (int i = 0; i < sMap.lhs.size(); ++i) {
      if (this.equals(sMap.lhs.get(i))) {
        Expr result = sMap.rhs.get(i).clone(null);
        if (id != null) result.id = id;
        return result;
      }
    }
    for (int i = 0; i < children.size(); ++i) {
      children.set(i, children.get(i).substitute(sMap));
    }
    return this;
  }

  /**
   * Substitute sub-exprs in the input list according to sMap.
   */
  public static <C extends Expr> void substituteList(
      List<C> l, SubstitutionMap sMap) {
    if (l == null) return;
    ListIterator<C> it = l.listIterator();
    while (it.hasNext()) {
      it.set((C) it.next().substitute(sMap));
    }
  }

  /**
   * Removes duplicate exprs (according to equals()).
   */
  public static <C extends Expr> void removeDuplicates(List<C> l) {
    if (l == null) return;
    ListIterator<C> it1 = l.listIterator();
    while (it1.hasNext()) {
      C e1 = it1.next();
      ListIterator<C> it2 = l.listIterator();
      boolean duplicate = false;
      while (it2.hasNext()) {
        C e2 = it2.next();
          // only check up to but excluding e1
        if (e1 == e2) break;
        if (e1.equals(e2)) {
          duplicate = true;
          break;
        }
      }
      if (duplicate) it1.remove();
    }
  }

  /**
   * Removes constant exprs
   */
  public static <C extends Expr> void removeConstants(List<C> l) {
    if (l == null) return;
    ListIterator<C> it = l.listIterator();
    while (it.hasNext()) {
      C e = it.next();
      if (e.isConstant()) it.remove();
    }
  }

  /**
   * Returns true if expr is fully bound by tid, otherwise false.
   */
  public boolean isBound(TupleId tid) {
    return isBound(Lists.newArrayList(tid));
  }

  /**
   * Returns true if expr is fully bound by tids, otherwise false.
   */
  public boolean isBound(List<TupleId> tids) {
    for (Expr child: children) {
      if (!child.isBound(tids)) return false;
    }
    return true;
  }

  /**
   * Returns true if expr is fully bound by slotId, otherwise false.
   */
  public boolean isBound(SlotId slotId) {
    for (Expr child: children) {
      if (!child.isBound(slotId)) return false;
    }
    return true;
  }

  public static boolean isBound(List<? extends Expr> exprs, List<TupleId> tids) {
    for (Expr expr: exprs) {
      if (!expr.isBound(tids)) return false;
    }
    return true;
  }

  public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
    for (Expr child: children) {
      child.getIds(tupleIds, slotIds);
    }
  }

  public static <C extends Expr> void getIds(List<? extends Expr> exprs,
      List<TupleId> tupleIds, List<SlotId> slotIds) {
    if (exprs == null) return;
    for (Expr e: exprs) {
      e.getIds(tupleIds, slotIds);
    }
  }

  /**
   * @return true if this is an instance of LiteralExpr
   */
  public boolean isLiteral() {
    return this instanceof LiteralExpr;
  }

  /**
   * @return true if this expr can be evaluated with Expr::GetValue(NULL),
   * ie, if it doesn't contain any references to runtime variables (which
   * at the moment are only slotrefs).
   */
  public boolean isConstant() {
    return !contains(SlotRef.class);
  }

  /**
   * Checks whether this expr returns a boolean type or NULL type.
   * If not, throws an AnalysisException with an appropriate error message using
   * 'name' as a prefix. For example, 'name' could be "WHERE clause".
   * The error message only contains this.toSql() if printExpr is true.
   */
  public void checkReturnsBool(String name, boolean printExpr) throws AnalysisException {
    if (type != PrimitiveType.BOOLEAN && !type.isNull()) {
      throw new AnalysisException(
          String.format("%s%s requires return type 'BOOLEAN'. " +
              "Actual type is '%s'.", name, (printExpr) ? " '" + toSql() + "'" : "",
              type.toString()));
    }
  }

  /**
   * Checks validity of cast, and
   * calls uncheckedCastTo() to
   * create a cast expression that casts
   * this to a specific type.
   *
   * @param targetType
   *          type to be cast to
   * @return cast expression, or converted literal,
   *         should never return null
   * @throws AnalysisException
   *           when an invalid cast is asked for, for example,
   *           failure to convert a string literal to a date literal
   *
   */
  public final Expr castTo(PrimitiveType targetType) throws AnalysisException {
    PrimitiveType type = PrimitiveType.getAssignmentCompatibleType(this.type, targetType);
    Preconditions.checkState(type.isValid(), "cast %s to %s", this.type, targetType);
    // If the targetType is NULL_TYPE then ignore the cast because NULL_TYPE
    // is compatible with all types and no cast is necessary.
    if (targetType.isNull()) return this;
    // requested cast must be to assignment-compatible type
    // (which implies no loss of precision)
    Preconditions.checkArgument(type == targetType);
    return uncheckedCastTo(targetType);
  }

  /**
   * Create an expression equivalent to 'this' but returning targetType;
   * possibly by inserting an implicit cast,
   * or by returning an altogether new expression
   * or by returning 'this' with a modified return type'.
   *
   * @param targetType
   *          type to be cast to
   * @return cast expression, or converted literal,
   *         should never return null
   * @throws AnalysisException
   *           when an invalid cast is asked for, for example,
   *           failure to convert a string literal to a date literal
   *
   */
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    return new CastExpr(targetType, this, true);
  }

  /**
   * Add a cast expression above child.
   * If child is a literal expression, we attempt to
   * convert the value of the child directly, and not insert a cast node.
   *
   * @param targetType
   *          type to be cast to
   * @param childIndex
   *          index of child to be cast
   */
  public void castChild(PrimitiveType targetType,
      int childIndex) throws AnalysisException {
    Expr child = getChild(childIndex);
    Expr newChild = child.castTo(targetType);
    setChild(childIndex, newChild);
  }

  /**
   * Returns child expr if this expr is an implicit cast, otherwise returns 'this'.
   */
  public Expr ignoreImplicitCast() {
    if (this instanceof CastExpr) {
      CastExpr cast = (CastExpr) this;
      if (cast.isImplicit()) return cast.getChild(0).ignoreImplicitCast();
    }
    return this;
  }

  /**
   * Cast the operands of a binary operation as necessary,
   * give their compatible type.
   * String literals are converted first, to enable casting of the
   * the other non-string operand.
   *
   * @param compatibleType
   * @return
   *         The possibly changed compatibleType
   *         (if a string literal forced casting the other operand)
   */
  public PrimitiveType castBinaryOp(PrimitiveType compatibleType)
      throws AnalysisException {
    Preconditions.checkState(
        this instanceof BinaryPredicate || this instanceof ArithmeticExpr);
    PrimitiveType t1 = getChild(0).getType();
    PrimitiveType t2 = getChild(1).getType();
    // Convert string literals if the other operand is numeric,
    // then get compatible type again to see if non-string type needs a cast as well.
    if (t1.isStringType() && getChild(0).isLiteral() && t2.isNumericType() ) {
      StringLiteral firstChild = (StringLiteral) getChild(0);
      children.set(0, firstChild.convertToNumber());
      t1 = getChild(0).getType();
      compatibleType = PrimitiveType.getAssignmentCompatibleType(t1, t2);
    } else if (t2.isStringType() && getChild(1).isLiteral() && t1.isNumericType()) {
      StringLiteral secondChild = (StringLiteral) getChild(1);
      children.set(1, secondChild.convertToNumber());
      t2 = getChild(1).getType();
      compatibleType = PrimitiveType.getAssignmentCompatibleType(t1, t2);
    }
    // add operand casts
    Preconditions.checkState(compatibleType.isValid());
    if (t1 != compatibleType) castChild(compatibleType, 0);
    if (t2 != compatibleType) castChild(compatibleType, 1);
    return compatibleType;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
        .add("id", id)
        .add("type", type)
        .add("sel", selectivity)
        .add("#distinct", numDistinctValues)
        .toString();
  }

  /**
   * If 'this' is a SlotRef or a Cast that wraps a SlotRef, returns that SlotRef.
   * Otherwise returns null.
   */
  public SlotRef unwrapSlotRef() {
    if (this instanceof SlotRef) {
      return (SlotRef) this;
    } else if (this instanceof CastExpr && getChild(0) instanceof SlotRef) {
      return (SlotRef) getChild(0);
    } else {
      return null;
    }
  }
}
