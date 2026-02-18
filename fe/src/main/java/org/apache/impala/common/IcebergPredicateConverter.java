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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
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

  public IcebergPredicateConverter(Schema schema, Analyzer analyzer) {
    this.schema_ = schema;
    this.analyzer_ = analyzer;
  }

  public Expression convert(Expr expr) throws ImpalaRuntimeException {
    if (expr instanceof BoolLiteral) {
      BoolLiteral boolLiteral = (BoolLiteral) expr;
      return boolLiteral.getValue() ? Expressions.alwaysTrue() :
          Expressions.alwaysFalse();
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
      throw new ImpalaRuntimeException(String.format(
          "Unsupported expression: %s", expr.toSql()));
    }
  }

  protected Expression convert(BinaryPredicate predicate) throws ImpalaRuntimeException {
    Term term = getTerm(predicate.getChild(0));
    IcebergColumn column = term.referencedColumn_;

    LiteralExpr literal = getSecondChildAsLiteralExpr(predicate);
    checkNullLiteral(literal);
    Operation op = getOperation(predicate);
    Object value = getIcebergValue(column, literal);

    List<Object> literals = Collections.singletonList(value);
    return Expressions.predicate(op, term.term_, literals);
  }

  protected UnboundPredicate<Object> convert(InPredicate predicate)
      throws ImpalaRuntimeException {
    Term term = getTerm(predicate.getChild(0));
    IcebergColumn column = term.referencedColumn_;
    // Expressions takes a list of values as Objects
    List<Object> values = new ArrayList<>();
    for (int i = 1; i < predicate.getChildren().size(); ++i) {
      if (!Expr.IS_LITERAL.apply(predicate.getChild(i))) {
        throw new ImpalaRuntimeException(
            String.format("Expression is not a literal: %s",
                predicate.getChild(i)));
      }
      LiteralExpr literal = (LiteralExpr) predicate.getChild(i);
      checkNullLiteral(literal);
      Object value = getIcebergValue(column, literal);
      values.add(value);
    }

    // According to the method:
    // 'org.apache.iceberg.expressions.InclusiveMetricsEvaluator.MetricsEvalVisitor#notIn'
    // Expressions.notIn only works when the push-down column is the partition column
    if (predicate.isNotIn()) {
      return Expressions.notIn(term.term_, values);
    } else {
      return Expressions.in(term.term_, values);
    }
  }

  protected UnboundPredicate<Object> convert(IsNullPredicate predicate)
      throws ImpalaRuntimeException {
    Term term = getTerm(predicate.getChild(0));
    if (predicate.isNotNull()) {
      return Expressions.notNull(term.term_);
    } else {
      return Expressions.isNull(term.term_);
    }
  }

  protected Expression convert(CompoundPredicate predicate)
      throws ImpalaRuntimeException {
    Operation op = getOperation(predicate);

    Expr leftExpr = predicate.getChild(0);
    Expression left = convert(leftExpr);

    if (op.equals(Operation.NOT)) {
      return Expressions.not(left);
    }

    Expr rightExpr = predicate.getChild(1);
    Expression right = convert(rightExpr);

    return op.equals(Operation.AND) ? Expressions.and(left, right) :
        Expressions.or(left, right);
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
   * Checks if there's any literal (non-wildcard) content after the given position.
   * This determines if a pattern like 'd%d' has content after the first wildcard.
   * Only unescaped % and _ are considered non-literal.
   *
   * @param pattern The pattern string from StringLiteral.getUnescapedValue()
   * @param afterPos Position to check after (exclusive)
   * @return True if there's any literal content after afterPos
   */
  static boolean hasLiteralContentAfterWildcard(String pattern, int afterPos) {
    for (int i = afterPos + 1; i < pattern.length(); i++) {
      char c = pattern.charAt(i);

      // Check if this is a wildcard
      if (c == '%' || c == '_') {
        // If escaped, it's literal content
        if (isWildcardEscaped(pattern, i)) {
          return true;
        }
        // Unescaped wildcard, continue checking
        continue;
      }

      // Any non-wildcard character is literal content
      return true;
    }
    return false;
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
   * Removes backslash escapes from LIKE wildcards in the pattern.
   *
   * Background: When Impala parses SQL LIKE 'test\%value',
   * StringLiteral.getUnescapedValue() preserves LIKE-specific escape sequences
   * and returns the Java string "test\\%value" (backslash followed by %). This
   * method converts it to "test%value" (literal %) for Iceberg, which doesn't
   * use backslash escaping.
   *
   * Example:
   *   Input: "test\\%value" (from SQL: LIKE 'test\%value')
   *   Output: "test%value" (literal % for Iceberg equal())
   *
   * @param pattern The pattern string from StringLiteral.getUnescapedValue()
   * @param endPos Position to stop processing, or -1 to process entire string
   * @return Pattern with LIKE escape sequences removed
   */
  static String unescapeLikePattern(String pattern, int endPos) {
    int len = (endPos == -1) ? pattern.length() : endPos;
    StringBuilder sb = new StringBuilder(len);

    for (int i = 0; i < len; i++) {
      char c = pattern.charAt(i);

      if (c == '\\' && i + 1 < pattern.length()) {
        char next = pattern.charAt(i + 1);
        // Unescape LIKE wildcards: \% -> %, \_ -> _
        // Also handle escaped backslash: \\ -> \
        if (next == '%' || next == '_' || next == '\\') {
          sb.append(next);
          i++; // Skip the next character as we've already processed it
          continue;
        }
      }

      sb.append(c);
    }

    return sb.toString();
  }

  protected Expression convert(LikePredicate predicate)
      throws ImpalaRuntimeException {
    // Only LIKE operator is supported, not RLIKE, REGEXP, etc.
    if (predicate.getOp() != LikePredicate.Operator.LIKE) {
      throw new ImpalaRuntimeException(String.format(
          "Only LIKE operator is supported for Iceberg pushdown, got: %s",
          predicate.getOp()));
    }

    Term term = getTerm(predicate.getChild(0));
    IcebergColumn column = term.referencedColumn_;

    // Check if the column is a string type
    if (!column.getType().isStringType()) {
      throw new ImpalaRuntimeException(String.format(
          "LIKE predicate pushdown only supports string columns, got: %s",
          column.getType()));
    }

    LiteralExpr literal = getSecondChildAsLiteralExpr(predicate);
    checkNullLiteral(literal);

    if (!(literal instanceof StringLiteral)) {
      throw new ImpalaRuntimeException(String.format(
          "LIKE pattern must be a string literal, got: %s", literal.toSql()));
    }

    String pattern = ((StringLiteral) literal).getUnescapedValue();
    if (pattern == null || pattern.isEmpty()) {
      throw new ImpalaRuntimeException("LIKE pattern cannot be null or empty");
    }

    // Find first unescaped wildcard position
    int firstWildcard = findFirstUnescapedWildcard(pattern);

    // Case 1: Pattern starts with wildcard - cannot push down
    if (firstWildcard == 0) {
      throw new ImpalaRuntimeException(String.format(
          "LIKE pattern '%s' cannot be pushed down to Iceberg. Patterns must start "
          + "with at least one literal character.", pattern));
    }

    // Case 2: No wildcards - exact match
    if (firstWildcard == -1) {
      String unescapedPattern = unescapeLikePattern(pattern, -1);
      return Expressions.equal(column.getName(), unescapedPattern);
    }

    // Case 3: Wildcard in middle with literal content after - cannot push down
    if (hasLiteralContentAfterWildcard(pattern, firstWildcard)) {
      throw new ImpalaRuntimeException(String.format(
          "LIKE pattern '%s' cannot be pushed down to Iceberg. Only prefix patterns "
          + "(e.g., 'prefix%%') are supported.", pattern));
    }

    // Case 4: Pure prefix pattern (wildcard only at end) - use startsWith
    String unescapedPattern = unescapeLikePattern(pattern, firstWildcard);
    return Expressions.startsWith(column.getName(), unescapedPattern);
  }

  protected void checkNullLiteral(LiteralExpr literal) throws ImpalaRuntimeException {
    if (Expr.IS_NULL_LITERAL.apply(literal)) {
      throw new ImpalaRuntimeException("Expression can't be NULL literal: " + literal);
    }
  }

  protected Object getIcebergValue(IcebergColumn column, LiteralExpr literal)
      throws ImpalaRuntimeException {
    PrimitiveType primitiveType = literal.getType().getPrimitiveType();
    switch (primitiveType) {
      case BOOLEAN: return ((BoolLiteral) literal).getValue();
      case TINYINT:
      case SMALLINT:
      case INT: return ((NumericLiteral) literal).getIntValue();
      case BIGINT: return ((NumericLiteral) literal).getLongValue();
      case FLOAT: return (float) ((NumericLiteral) literal).getDoubleValue();
      case DOUBLE: return ((NumericLiteral) literal).getDoubleValue();
      case STRING:
      case DATETIME:
      case CHAR: return ((StringLiteral) literal).getUnescapedValue();
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
   * Returns Iceberg operator by BinaryPredicate operator, or null if the operation is not
   * supported by Iceberg.
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
   * Returns Iceberg operator by CompoundPredicate operator, or null if the operation is
   * not supported by Iceberg.
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

    return new Term(Expressions.ref(column.getName()), (IcebergColumn) column);
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
