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

package org.apache.impala.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LikePredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TimestampLiteral;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.Type;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergPredicateConverter {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergPredicateConverter.class);
  private final Schema schema_;
  private final Analyzer analyzer_;
  // Ids of the fields that are the sources of partition fields.
  private final Set<Integer> partitionSourceIds_;

  /**
   * Status of the conversion result.
   * FULLY_CONVERTED: The predicate was fully converted to an Iceberg expression that is
   *   semantically equivalent to the original predicate. No additional evaluation needed
   *   by Impala.
   * PARTIALLY_CONVERTED: The predicate was partially converted to a relaxed Iceberg
   *   expression that is less restrictive than the original. The original predicate must
   *   still be evaluated by Impala on surviving rows.
   * FAILED: The predicate could not be converted to an Iceberg expression.
   */
  public enum ConversionStatus {
    FULLY_CONVERTED,
    PARTIALLY_CONVERTED,
    FAILED
  }

  /**
   * Result of converting an Impala predicate to an Iceberg expression.
   */
  public static class ConverterResult {
    private final Expression icebergExpression;
    private final ConversionStatus status;
    private final String errorMessage;

    public ConverterResult(Expression icebergExpression, ConversionStatus status) {
      this(icebergExpression, status, null);
    }

    public ConverterResult(ConversionStatus status, String errorMessage) {
      this(null, status, errorMessage);
    }

    private ConverterResult(Expression icebergExpression, ConversionStatus status,
        String errorMessage) {
      this.icebergExpression = icebergExpression;
      this.status = status;
      this.errorMessage = errorMessage;
    }

    public Expression getIcebergExpression() {
      return icebergExpression;
    }

    public ConversionStatus getStatus() {
      return status;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public boolean isFullyConverted() {
      return status == ConversionStatus.FULLY_CONVERTED;
    }

    public boolean isPartiallyConverted() {
      return status == ConversionStatus.PARTIALLY_CONVERTED;
    }

    public boolean isFailed() {
      return status == ConversionStatus.FAILED;
    }
  }

  public IcebergPredicateConverter(Schema schema, Analyzer analyzer) {
    this(schema, analyzer, Collections.emptySet());
  }

  /**
   * 'partitionSourceIds' are the ids of the fields that are the sources of partition
   * fields, see makeNullSafe().
   */
  public IcebergPredicateConverter(Schema schema, Analyzer analyzer,
      Set<Integer> partitionSourceIds) {
    this.schema_ = schema;
    this.analyzer_ = analyzer;
    this.partitionSourceIds_ = partitionSourceIds;
  }

  public ConverterResult convert(Expr expr) {
    ConverterResult result = convertExpr(expr);
    if (result.isFailed()) return result;
    try {
      return new ConverterResult(makeNullSafe(result.getIcebergExpression()),
          result.getStatus());
    } catch (ImpalaRuntimeException e) {
      return new ConverterResult(ConversionStatus.FAILED, e.getMessage());
    }
  }

  /**
   * Returns true if 'columnName' refers to a nullable column whose values are also
   * evaluated as partition values.
   */
  private boolean isNullablePartitionSource(String columnName) {
    Types.NestedField field = schema_.caseInsensitiveFindField(columnName);
    return field != null && field.isOptional() &&
        partitionSourceIds_.contains(field.fieldId());
  }

  /**
   * Iceberg orders NULL values before any other value, so it evaluates e.g. 'p < 5',
   * 'p != 5' and NOT (p = 5) as true for NULL values, while they are NULL in Impala, so
   * the row doesn't match. This matters for partition values: the scanner doesn't
   * evaluate the predicates that Iceberg fully evaluated on them, and partition DDL
   * statements use them to select partitions. Returns 'expr' with its negations pushed
   * down to the predicates, and 'IS NOT NULL' added to the predicates on nullable
   * partition source columns that would match NULL values. This is equivalent in
   * Impala's three valued logic, because NOT(a OR b) = NOT a AND NOT b,
   * NOT(a < 5) = a >= 5, etc.
   */
  private Expression makeNullSafe(Expression expr) throws ImpalaRuntimeException {
    if (partitionSourceIds_.isEmpty()) return expr;
    NullSafeRewriter rewriter = new NullSafeRewriter();
    Expression ret = rewriter.rewrite(Expressions.rewriteNot(expr));
    // Iceberg fails to evaluate LIKE and NOT LIKE for NULL partition values, which is
    // only avoided if the NULL partitions are pruned, i.e. 'ret' is false for them.
    for (String columnName : rewriter.likeColumns_) {
      if (mayBeTrueForNull(ret, columnName)) {
        throw new ImpalaRuntimeException(String.format("LIKE on the nullable " +
            "partition column %s can't be converted to an Iceberg expression that " +
            "can be true for NULL values of the column", columnName));
      }
    }
    // Keep the original expression if there was nothing to change.
    return rewriter.changed_ ? ret : expr;
  }

  /**
   * Returns false if 'expr', which was rewritten by NullSafeRewriter, is false for every
   * NULL value of 'columnName'.
   */
  private static boolean mayBeTrueForNull(Expression expr, String columnName) {
    switch (expr.op()) {
      case TRUE:
        return true;
      case FALSE:
        return false;
      case AND:
        And and = (And) expr;
        return mayBeTrueForNull(and.left(), columnName) &&
            mayBeTrueForNull(and.right(), columnName);
      case OR:
        Or or = (Or) expr;
        return mayBeTrueForNull(or.left(), columnName) ||
            mayBeTrueForNull(or.right(), columnName);
      default:
        break;
    }
    UnboundPredicate<?> pred = (UnboundPredicate<?>) expr;
    // Predicates on other columns may be true.
    if (!pred.ref().name().equalsIgnoreCase(columnName)) return true;
    return pred.op() == Operation.IS_NULL;
  }

  private class NullSafeRewriter {
    boolean changed_ = false;
    // The columns of the LIKE and NOT LIKE predicates.
    final Set<String> likeColumns_ = new HashSet<>();

    Expression rewrite(Expression expr) {
      switch (expr.op()) {
        case TRUE:
        case FALSE:
          return expr;
        case AND:
          And and = (And) expr;
          return Expressions.and(rewrite(and.left()), rewrite(and.right()));
        case OR:
          Or or = (Or) expr;
          return Expressions.or(rewrite(or.left()), rewrite(or.right()));
        default:
          break;
      }
      // Expressions.rewriteNot() removes every NOT.
      Preconditions.checkState(expr instanceof UnboundPredicate, expr);
      return rewrite((UnboundPredicate<?>) expr);
    }

    <T> Expression rewrite(UnboundPredicate<T> pred) {
      String columnName = pred.ref().name();
      if (!isNullablePartitionSource(columnName)) return pred;
      Expression ret = pred;
      switch (pred.op()) {
        case STARTS_WITH:
          likeColumns_.add(columnName);
          return pred;
        case NOT_STARTS_WITH:
          likeColumns_.add(columnName);
          break;
        case IN:
        case NOT_IN:
          // Iceberg fails to evaluate IN and NOT IN with several string values for NULL
          // values, but it can evaluate = and !=.
          if (pred.literals().size() > 1 &&
              pred.literals().get(0).value() instanceof CharSequence) {
            changed_ = true;
            List<Expression> preds = new ArrayList<>();
            for (Literal<T> literal : pred.literals()) {
              preds.add(pred.op() == Operation.IN ?
                  Expressions.equal(pred.term(), literal.value()) :
                  Expressions.notEqual(pred.term(), literal.value()));
            }
            ret = combine(preds, pred.op() == Operation.IN, 0, preds.size());
          }
          if (pred.op() == Operation.IN) return ret;
          break;
        case LT:
        case LT_EQ:
        case NOT_EQ:
        case NOT_NAN:
          break;
        default:
          // The other predicates don't match NULL values in Iceberg either.
          return pred;
      }
      changed_ = true;
      return Expressions.and(Expressions.notNull(columnName), ret);
    }
  }

  /**
   * Combines exprs[begin, end) with OR if 'isOr' is true, with AND otherwise, into a
   * balanced tree.
   */
  private static Expression combine(List<Expression> exprs, boolean isOr, int begin,
      int end) {
    if (end - begin == 1) return exprs.get(begin);
    int mid = (begin + end) / 2;
    Expression left = combine(exprs, isOr, begin, mid);
    Expression right = combine(exprs, isOr, mid, end);
    return isOr ? Expressions.or(left, right) : Expressions.and(left, right);
  }

  private ConverterResult convertExpr(Expr expr) {
    if (expr instanceof BoolLiteral) {
      BoolLiteral boolLiteral = (BoolLiteral) expr;
      Expression iceExpr = boolLiteral.getValue() ? Expressions.alwaysTrue() :
          Expressions.alwaysFalse();
      return new ConverterResult(iceExpr, ConversionStatus.FULLY_CONVERTED);
    } else if (expr instanceof BinaryPredicate) {
      return convert((BinaryPredicate) expr);
    } else if (expr instanceof InPredicate) {
      return convert((InPredicate) expr);
    } else if (expr instanceof IsNullPredicate) {
      return convert((IsNullPredicate) expr);
    } else if (expr instanceof CompoundPredicate) {
      return convert((CompoundPredicate) expr);
    } else if (expr instanceof LikePredicate) {
      return convert((LikePredicate) expr);
    } else {
      return new ConverterResult(ConversionStatus.FAILED,
          String.format("Unsupported expression: %s", expr.toSql()));
    }
  }

  protected ConverterResult convert(BinaryPredicate predicate) {
    try {
      Term term = getTerm(predicate.getChild(0));

      LiteralExpr literal = getSecondChildAsLiteralExpr(predicate);
      checkNullLiteral(literal);
      Operation op = getOperation(predicate);
      Object value = getIcebergValue(term, op, literal);

      List<Object> literals = Collections.singletonList(value);
      Expression iceExpr = Expressions.predicate(op, term.term_, literals);
      // Iceberg orders -0.0 before 0.0, but they are equal in Impala, e.g. a file whose
      // upper bound is -0.0 must not be skipped for 'd = 0', and a -0.0 partition value
      // must not match 'd < 0'.
      Object negativeZero = getNegativeZero(value);
      if (negativeZero != null) {
        if (op == Operation.EQ) {
          iceExpr = Expressions.in(term.term_, value, negativeZero);
        } else if (op == Operation.GT_EQ) {
          iceExpr = Expressions.greaterThanOrEqual(term.term_, negativeZero);
        } else if (op == Operation.LT) {
          iceExpr = Expressions.lessThan(term.term_, negativeZero);
        } else if (op == Operation.NOT_EQ) {
          iceExpr = Expressions.notIn(term.term_, value, negativeZero);
        }
      }
      // NaN is greater than any other value in Iceberg, so e.g. a NaN partition value
      // would match 'd > 5'.
      if ((value instanceof Double || value instanceof Float) &&
          (op == Operation.GT || op == Operation.GT_EQ)) {
        iceExpr = Expressions.and(iceExpr, Expressions.notNaN(term.term_));
      }
      return new ConverterResult(iceExpr, ConversionStatus.FULLY_CONVERTED);
    } catch (ImpalaRuntimeException e) {
      return new ConverterResult(ConversionStatus.FAILED, e.getMessage());
    }
  }

  protected ConverterResult convert(InPredicate predicate) {
    try {
      Term term = getTerm(predicate.getChild(0));
      // Expressions takes a list of values as Objects
      List<Object> values = new ArrayList<>();
      Operation op = predicate.isNotIn() ? Operation.NOT_IN : Operation.IN;
      for (int i = 1; i < predicate.getChildren().size(); ++i) {
        if (!Expr.IS_LITERAL.apply(predicate.getChild(i))) {
          return new ConverterResult(ConversionStatus.FAILED,
              String.format("Expression is not a literal: %s",
                  predicate.getChild(i)));
        }
        LiteralExpr literal = (LiteralExpr) predicate.getChild(i);
        checkNullLiteral(literal);
        Object value = getIcebergValue(term, op, literal);
        values.add(value);
        Object negativeZero = getNegativeZero(value);
        if (negativeZero != null) values.add(negativeZero);
      }

      // According to the method:
      // 'org.apache.iceberg.expressions.InclusiveMetricsEvaluator
      // .MetricsEvalVisitor#notIn'
      // Expressions.notIn only works when the push-down column is the partition column
      UnboundPredicate<Object> iceExpr;
      if (predicate.isNotIn()) {
        iceExpr = Expressions.notIn(term.term_, values);
      } else {
        iceExpr = Expressions.in(term.term_, values);
      }
      return new ConverterResult(iceExpr, ConversionStatus.FULLY_CONVERTED);
    } catch (ImpalaRuntimeException e) {
      return new ConverterResult(ConversionStatus.FAILED, e.getMessage());
    }
  }

  protected ConverterResult convert(IsNullPredicate predicate) {
    try {
      Term term = getTerm(predicate.getChild(0));
      UnboundPredicate<Object> iceExpr;
      if (predicate.isNotNull()) {
        iceExpr = Expressions.notNull(term.term_);
      } else {
        iceExpr = Expressions.isNull(term.term_);
      }
      return new ConverterResult(iceExpr, ConversionStatus.FULLY_CONVERTED);
    } catch (ImpalaRuntimeException e) {
      return new ConverterResult(ConversionStatus.FAILED, e.getMessage());
    }
  }

  protected ConverterResult convert(CompoundPredicate predicate) {
    try {
      Operation op = getOperation(predicate);

      // Iceberg's ResidualEvaluator throws on NULL partition values in startsWith() and
      // notStartsWith(). Iceberg's partition filter drops NULL partitions before that
      // only if no NOT or OR above the LIKE can make the filter true for them.
      if (!op.equals(Operation.AND) && predicate.contains(LikePredicate.class)) {
        return new ConverterResult(ConversionStatus.FAILED, String.format(
            "LIKE under NOT or OR cannot be pushed down to Iceberg: %s",
            predicate.toSql()));
      }

      Expr leftExpr = predicate.getChild(0);
      ConverterResult leftResult = convertExpr(leftExpr);

      // If left child failed, propagate failure
      if (leftResult.isFailed()) {
        return leftResult;
      }

      if (op.equals(Operation.NOT)) {
        // NOT of a relaxed expression is stricter than NOT of the original predicate,
        // e.g. NOT startsWith(s, 'a') would skip 'axc' for NOT (s LIKE 'a%b').
        if (leftResult.isPartiallyConverted()) {
          return new ConverterResult(ConversionStatus.FAILED, String.format(
              "NOT of a partially converted predicate cannot be pushed down to " +
              "Iceberg: %s", predicate.toSql()));
        }
        // Iceberg rewrites e.g. NOT(d < 5) to d >= 5, which is not equivalent for NaN.
        if (hasFloatingPointComparison(leftExpr)) {
          return new ConverterResult(ConversionStatus.FAILED,
              "Cannot negate a predicate on a floating-point column: " +
              leftExpr.toSql());
        }
        Expression iceExpr = Expressions.not(leftResult.getIcebergExpression());
        return new ConverterResult(iceExpr, ConversionStatus.FULLY_CONVERTED);
      }

      Expr rightExpr = predicate.getChild(1);
      ConverterResult rightResult = convertExpr(rightExpr);

      // If right child failed, propagate failure
      if (rightResult.isFailed()) {
        return rightResult;
      }

      Expression iceExpr = op.equals(Operation.AND) ?
          Expressions.and(leftResult.getIcebergExpression(),
              rightResult.getIcebergExpression()) :
          Expressions.or(leftResult.getIcebergExpression(),
              rightResult.getIcebergExpression());

      // If either child is partially converted, the compound predicate
      // is partially converted
      ConversionStatus status = ConversionStatus.FULLY_CONVERTED;
      if (leftResult.isPartiallyConverted() || rightResult.isPartiallyConverted()) {
        status = ConversionStatus.PARTIALLY_CONVERTED;
      }
      return new ConverterResult(iceExpr, status);
    } catch (ImpalaRuntimeException e) {
      return new ConverterResult(ConversionStatus.FAILED, e.getMessage());
    }
  }

  private static boolean hasFloatingPointComparison(Expr expr) {
    return expr.contains((Predicate<Expr>) e ->
        (e instanceof BinaryPredicate || e instanceof InPredicate) &&
        e.getChild(0).getType().isFloatingPointType());
  }

  /**
   * Checks if a wildcard character at the given position is escaped by counting
   * preceding backslashes. An odd number of backslashes means the wildcard is escaped.
   */
  static boolean isWildcardEscaped(String pattern, int wildcardPos) {
    int backslashCount = 0;
    int j = wildcardPos - 1;
    while (j >= 0 && pattern.charAt(j) == '\\') {
      backslashCount++;
      j--;
    }
    return backslashCount % 2 == 1;
  }

  /**
   * Finds the position of the first unescaped wildcard (% or _) in the pattern.
   * Wildcards can be escaped with '\', e.g., 'asd\%' has no unescaped wildcard.
   *
   * @return Position of first unescaped wildcard, or -1 if none found
   */
  static int findFirstUnescapedWildcard(String pattern) {
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);

      // Check if this character is a wildcard
      if (c == '%' || c == '_') {
        // Return position if not escaped
        if (!isWildcardEscaped(pattern, i)) {
          return i;
        }
      }
    }
    return -1;
  }

  /**
   * Returns the literal string that pattern[0, endPos) matches in the backend
   * (LikePredicate::ConvertLikePattern()): a backslash only escapes '%', '_' and '\',
   * before any other character or at the end it is a literal. The range must not
   * contain unescaped wildcards.
   *
   * @param pattern The pattern string from getBackendString()
   * @param endPos Position to stop processing, or -1 to process entire string
   */
  static String unescapeLikePattern(String pattern, int endPos) {
    int len = (endPos == -1) ? pattern.length() : endPos;
    StringBuilder sb = new StringBuilder(len);

    for (int i = 0; i < len; i++) {
      char c = pattern.charAt(i);
      if (c == '\\' && i + 1 < len && isEscapable(pattern.charAt(i + 1))) {
        sb.append(pattern.charAt(i + 1));
        i++;
        continue;
      }
      sb.append(c);
    }

    return sb.toString();
  }

  private static boolean isEscapable(char c) {
    return c == '%' || c == '_' || c == '\\';
  }

  /**
   * Returns true if the unescaped wildcard at wildcardPos and everything after it only
   * match "any suffix", i.e. all characters are '%'.
   */
  static boolean matchesAnySuffixFrom(String pattern, int wildcardPos) {
    for (int i = wildcardPos; i < pattern.length(); i++) {
      if (pattern.charAt(i) != '%') return false;
    }
    return true;
  }

  protected ConverterResult convert(LikePredicate predicate) {
    try {
      // Only LIKE operator is supported, not RLIKE, REGEXP, etc.
      if (predicate.getOp() != LikePredicate.Operator.LIKE) {
        return new ConverterResult(ConversionStatus.FAILED,
            String.format("Only LIKE operator is supported for Iceberg pushdown, got: %s",
                predicate.getOp()));
      }

      Term term = getTerm(predicate.getChild(0));
      // In partition statements the term can be a transform, e.g. truncate(3, s).
      Type termType = predicate.getChild(0).getType();
      if (!termType.isStringType()) {
        return new ConverterResult(ConversionStatus.FAILED,
            String.format("LIKE predicate pushdown only supports strings, got: %s",
                termType));
      }
      @SuppressWarnings("unchecked")
      UnboundTerm<String> ref = (UnboundTerm<String>) (UnboundTerm<?>) term.term_;

      LiteralExpr literal = getSecondChildAsLiteralExpr(predicate);
      checkNullLiteral(literal);

      if (!(literal instanceof StringLiteral)) {
        return new ConverterResult(ConversionStatus.FAILED,
            String.format("LIKE pattern must be a string literal, got: %s",
                literal.toSql()));
      }

      String pattern = getBackendString((StringLiteral) literal);
      if (pattern == null || pattern.isEmpty()) {
        return new ConverterResult(ConversionStatus.FAILED,
            "LIKE pattern cannot be null or empty");
      }

      int firstWildcard = findFirstUnescapedWildcard(pattern);
      if (firstWildcard == 0) {
        return new ConverterResult(ConversionStatus.FAILED,
            String.format("LIKE pattern '%s' cannot be pushed down to Iceberg. " +
                "Patterns must start with at least one literal character.", pattern));
      }
      // No unescaped wildcard, push down the unescaped pattern.
      if (firstWildcard == -1) {
        return new ConverterResult(
            Expressions.equal(ref, unescapeLikePattern(pattern, -1)),
            ConversionStatus.FULLY_CONVERTED);
      }
      // E.g. LIKE 'd%d' pushes down startsWith('d') and the scan still evaluates
      // LIKE 'd%d'.
      ConversionStatus status = matchesAnySuffixFrom(pattern, firstWildcard) ?
          ConversionStatus.FULLY_CONVERTED : ConversionStatus.PARTIALLY_CONVERTED;
      Expression iceExpr = Expressions.startsWith(ref,
          unescapeLikePattern(pattern, firstWildcard));
      return new ConverterResult(iceExpr, status);
    } catch (ImpalaRuntimeException e) {
      return new ConverterResult(ConversionStatus.FAILED, e.getMessage());
    }
  }

  /**
   * Returns the string that the backend receives for 'literal'. Constant folded literals
   * are not unescaped again, unlike with getUnescapedValue().
   */
  protected static String getBackendString(StringLiteral literal)
      throws ImpalaRuntimeException {
    if (!literal.isValidUtf8()) {
      throw new ImpalaRuntimeException(String.format(
          "Non UTF-8 string can't be converted to Iceberg expression: %s",
          literal.toSql()));
    }
    return literal.unescapeIfNeeded();
  }

  protected void checkNullLiteral(LiteralExpr literal) throws ImpalaRuntimeException {
    if (Expr.IS_NULL_LITERAL.apply(literal)) {
      throw new ImpalaRuntimeException("Expression can't be NULL literal: " + literal);
    }
  }

  protected Object getIcebergValue(Term term, Operation op, LiteralExpr literal)
      throws ImpalaRuntimeException {
    IcebergColumn column = term.referencedColumn_;
    PrimitiveType primitiveType = literal.getType().getPrimitiveType();
    switch (primitiveType) {
      case BOOLEAN: return ((BoolLiteral) literal).getValue();
      case TINYINT:
      case SMALLINT:
      case INT: return ((NumericLiteral) literal).getIntValue();
      case BIGINT: return ((NumericLiteral) literal).getLongValue();
      case FLOAT: {
        // Narrowing to FLOAT can give -0.0, e.g. for CAST(-1e-46 AS FLOAT).
        float value = (float) getIcebergDoubleValue(op, (NumericLiteral) literal);
        return value == 0 ? 0.0f : value;
      }
      case DOUBLE: return getIcebergDoubleValue(op, (NumericLiteral) literal);
      case UUID: return getIcebergUuidValue(term, op, (StringLiteral) literal);
      case STRING:
      case DATETIME:
      case CHAR: return getBackendString((StringLiteral) literal);
      case TIMESTAMP: return getIcebergTsValue(literal, column, schema_);
      case DATE: return ((DateLiteral) literal).getValue();
      case DECIMAL: return getIcebergDecimalValue(column, (NumericLiteral) literal);
      // Wrapping the byte array into a ByteBuffer,
      // so Iceberg handles it as a BinaryLiteral instead of FixedLiteral.
      case BINARY: return ByteBuffer.wrap(((StringLiteral) literal).getBinValue());
      default: {
        throw new ImpalaRuntimeException(
            String.format("Unable to parse Iceberg value '%s' for type %s",
                literal.getStringValue(), primitiveType));
      }
    }
  }

  /**
   * Returns the value of a FLOAT or DOUBLE literal if Iceberg can't skip files with
   * matching rows for 'op'. Iceberg compares floating-point values by their total order,
   * while Impala follows IEEE 754, where NaN is not equal to anything.
   */
  protected double getIcebergDoubleValue(Operation op, NumericLiteral literal)
      throws ImpalaRuntimeException {
    // Iceberg assumes that a file without NaN value counts has no NaNs, so it skips files
    // where the lower and upper bounds equal the literal. Impala doesn't write NaN value
    // counts and leaves NaNs out of the bounds.
    if (op == Operation.NOT_EQ || op == Operation.NOT_IN) {
      throw new ImpalaRuntimeException(
          "!= and NOT IN can't be converted to Iceberg expressions for floating-point " +
          "values");
    }
    return literal.getDoubleValue();
  }

  /**
   * Returns -0.0 of the same type if 'value' is a floating-point zero, null otherwise.
   * Literals can't be -0.0 in Impala.
   */
  private static Object getNegativeZero(Object value) {
    if (value instanceof Double && (Double) value == 0) return -0.0d;
    if (value instanceof Float && (Float) value == 0) return -0.0f;
    return null;
  }

  /**
   * Returns the Iceberg value for a UUID literal. Subclasses may override this to allow
   * UUID value conversion in contexts where it is safe, such as metadata-only operations.
   */
  protected Object getIcebergUuidValue(Term term, Operation op, StringLiteral literal)
      throws ImpalaRuntimeException {
    // The Iceberg Java library compares UUIDs as signed longs, but Parquet min/max bounds
    // use unsigned byte order, so Iceberg could skip data files that contain matching
    // rows. This is a bug in the Java library, not in the Iceberg spec.
    // TODO: IMPALA-15476: push down = and IN on columns that are identity partitioned in
    // every spec, and derived bucket(N, col) = <hash> predicates, which are safe.
    throw new ImpalaRuntimeException(
        "UUID values cannot be converted to Iceberg expressions");
  }

  /**
   * Returns Iceberg operator by BinaryPredicate operator.
   * Throws ImpalaRuntimeException if the operation is not supported by Iceberg.
   */
  protected Operation getIcebergOperator(BinaryPredicate.Operator op)
      throws ImpalaRuntimeException {
    switch (op) {
      case EQ: return Operation.EQ;
      case NE: return Operation.NOT_EQ;
      case LE: return Operation.LT_EQ;
      case GE: return Operation.GT_EQ;
      case LT: return Operation.LT;
      case GT: return Operation.GT;
      default:
        throw new ImpalaRuntimeException(
            String.format("Unsupported Impala operator: %s", op.getName()));
    }
  }

  /**
   * Returns Iceberg operator by CompoundPredicate operator. Throws
   * ImpalaRuntimeException if the operation is not supported by Iceberg.
   */
  protected Operation getIcebergOperator(CompoundPredicate.Operator op)
      throws ImpalaRuntimeException {
    switch (op) {
      case AND: return Operation.AND;
      case OR: return Operation.OR;
      case NOT: return Operation.NOT;
      default:
        throw new ImpalaRuntimeException(
            String.format("Unsupported Impala operator: %s", op));
    }
  }

  protected BigDecimal getIcebergDecimalValue(IcebergColumn column,
      NumericLiteral literal) throws ImpalaRuntimeException {
    Type colType = column.getType();
    int scale = colType.getDecimalDigits();
    BigDecimal literalValue = literal.getValue();

    if (literalValue.scale() > scale) {
      throw new ImpalaRuntimeException(
          String.format("Invalid scale %d for type: %s", literalValue.scale(),
              colType.toSql()));
    }
    // Iceberg DecimalLiteral needs to have the exact same scale.
    if (literalValue.scale() < scale) {
      return literalValue.setScale(scale);
    }
    return literalValue;
  }

  protected Long getIcebergTsValue(LiteralExpr literal, IcebergColumn column,
      Schema iceSchema) throws ImpalaRuntimeException {
    // Iceberg timestamps have microsecond precision. Rounding a literal with nanoseconds
    // would change the result of comparisons, e.g. ts < '00:00:00.0000004' would become
    // ts < '00:00:00', which skips files where ts = '00:00:00'.
    // TODO: IMPALA-15709: round depending on the operator instead, e.g. push down
    // ts < '00:00:00.0000004' as ts < '00:00:00.000001'.
    if (literal instanceof TimestampLiteral &&
        ((TimestampLiteral) literal).getNanosOfDay() % 1000 != 0) {
      throw new ImpalaRuntimeException(
          "Timestamps with sub-microsecond precision can't be converted to Iceberg " +
          "expressions: " + literal.toSql());
    }
    try {
      org.apache.iceberg.types.Type iceType = iceSchema.findType(column.getFieldId());
      Preconditions.checkState(iceType instanceof Types.TimestampType);
      Types.TimestampType tsType = (Types.TimestampType) iceType;
      if (tsType.shouldAdjustToUTC()) {
        return ExprUtil.localTimestampToUnixTimeMicros(analyzer_, literal);
      } else {
        return ExprUtil.utcTimestampToUnixTimeMicros(analyzer_, literal);
      }
    } catch (InternalException ex) {
      // We cannot interpret the timestamp literal. Maybe the timestamp is invalid,
      // or the local timestamp ambiguously converts to UTC due to daylight saving
      // time backward turn. E.g. '2021-10-31 02:15:00 Europe/Budapest' converts to
      // either '2021-10-31 00:15:00 UTC' or '2021-10-31 01:15:00 UTC'.
      LOG.warn("Exception occurred during timestamp conversion: %s"
          + "\nThis means timestamp predicate is not pushed to Iceberg, let Impala "
          + "backend handle it.", ex);
    } catch (AnalysisException ignored) {}
    throw new ImpalaRuntimeException(
        String.format("Unable to parse timestamp value from: %s",
            literal.getStringValue()));
  }

  protected Column getColumnFromSlotRef(SlotRef slotRef) throws ImpalaRuntimeException {
    SlotDescriptor desc = slotRef.getDesc();
    // If predicate contains map/struct, this column would be null
    Column column = desc.getColumn();
    if (column == null) {
      throw new ImpalaRuntimeException(
          "Expressions with complex types can't be converted to Iceberg expressions: "
          + slotRef);
    }
    return column;
  }


  protected LiteralExpr getSecondChildAsLiteralExpr(Expr expr)
      throws ImpalaRuntimeException {
    if (!(expr.getChild(1) instanceof LiteralExpr)) {
      throw new ImpalaRuntimeException(
          String.format("Invalid child expression: %s", expr));
    }
    return (LiteralExpr) expr.getChild(1);
  }

  protected Operation getOperation(Expr expr) throws ImpalaRuntimeException {
    Operation op;
    if (expr instanceof BinaryPredicate) {
      op = getIcebergOperator(((BinaryPredicate) expr).getOp());
    } else if (expr instanceof CompoundPredicate) {
      op = getIcebergOperator(((CompoundPredicate) expr).getOp());
    } else {
      throw new ImpalaRuntimeException(
          String.format("Invalid expression type: %s", expr.getType()));
    }
    return op;
  }

  protected Term getTerm(Expr expr) throws ImpalaRuntimeException {
    if(!(expr instanceof SlotRef)){
      throw new ImpalaRuntimeException(
          String.format("Unable to create term from expression: %s", expr.toSql()));
    }
    Column column = getColumnFromSlotRef((SlotRef) expr);
    if(!(column instanceof IcebergColumn)){
      throw new ImpalaRuntimeException(
          String.format("Invalid column type %s for column: %s", column.getType(),
              column));
    }

    IcebergColumn iceColumn = (IcebergColumn) column;
    // Iceberg binds the expression to the schema by column name, so the name must refer
    // to the same field. It doesn't for hidden columns like _file_row_id, and might not
    // for time travel to a snapshot with an older schema.
    Types.NestedField field = schema_.caseInsensitiveFindField(column.getName());
    if (field == null || field.fieldId() != iceColumn.getFieldId()) {
      throw new ImpalaRuntimeException(String.format(
          "Column %s is not field %d of the Iceberg schema", column.getName(),
          iceColumn.getFieldId()));
    }
    return new Term(Expressions.ref(column.getName()), iceColumn);
  }

  public static class Term {
    public final UnboundTerm<Object> term_;
    public final IcebergColumn referencedColumn_;

    public Term(UnboundTerm<Object> term, IcebergColumn referencedColumn){
      term_ = term;
      referencedColumn_ = referencedColumn;
    }
  }
}
