/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.planner.paimon;

import static org.apache.paimon.predicate.PredicateBuilder.convertJavaObject;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LikePredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TimestampLiteral;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.paimon.PaimonImpalaTypeUtils;
import org.apache.impala.catalog.paimon.PaimonUtil;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.util.ExprUtil;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Conversion from {@link Expr} to paimon {@link Predicate} if it is able to push down.
 * Conversion follows the same safety rules as {@link org.apache.impala.common
 * .IcebergPredicateConverter}: bare column references, literal types must match the
 * column type, and NULL literals are never pushed in comparisons.
 */
public class ImpalaExprConverter {
  // Escape character of LIKE patterns, same as the backend LikePredicate.
  private static final char LIKE_ESCAPE_CHAR = '\\';

  public static final Pattern REGEXP_ENDS_WITH_PATTERN =
      Pattern.compile("\\.\\*([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\$");

  public static final Pattern REGEXP_STARTS_WITH_PATTERN =
      Pattern.compile("\\^([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\.\\*");

  public static final Pattern REGEXP_EQUALS_PATTERN =
      Pattern.compile("\\^([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\$");

  public static final Pattern REGEXP_STARTS_ENDS_WITH_PATTERN =
      Pattern.compile("\\^([^\\.\\^\\{\\\\\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\."
          + "\\*([^\\.\\^\\{\\\\\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\$");

  public enum ConversionStatus {
    FULLY_CONVERTED,
    PARTIALLY_CONVERTED,
    FAILED
  }

  public static class ConverterResult {
    private final Predicate predicate_;
    private final ConversionStatus status_;
    // True when the predicate may be a partition filter but not a data filter.
    // Paimon skips a file whose min and max both equal a != / NOT IN literal, and
    // Impala does not record NaN in those bounds, so a file that also contains NaN
    // would be dropped. Partition values are evaluated exactly.
    private final boolean partitionOnly_;

    public ConverterResult(Predicate predicate, ConversionStatus status) {
      this(predicate, status, false);
    }

    public ConverterResult(
        Predicate predicate, ConversionStatus status, boolean partitionOnly) {
      predicate_ = predicate;
      status_ = status;
      partitionOnly_ = partitionOnly;
    }

    public static ConverterResult failed() {
      return new ConverterResult(null, ConversionStatus.FAILED);
    }

    public Predicate getPredicate() { return predicate_; }

    public ConversionStatus getStatus() { return status_; }

    public boolean isFailed() { return status_ == ConversionStatus.FAILED; }

    public boolean isPartiallyConverted() {
      return status_ == ConversionStatus.PARTIALLY_CONVERTED;
    }

    public boolean isFullyConverted() {
      return status_ == ConversionStatus.FULLY_CONVERTED;
    }

    /**
     * True when this predicate must not be installed as a data filter. It is still
     * safe as a partition filter when every referenced field is a partition key.
     */
    public boolean isPartitionOnly() { return partitionOnly_; }
  }

  private final RowType rowType_;
  private final PredicateBuilder builder_;
  @Nullable
  private final Analyzer analyzer_;

  public ImpalaExprConverter(RowType rowType) {
    this(rowType, null);
  }

  public ImpalaExprConverter(RowType rowType, @Nullable Analyzer analyzer) {
    rowType_ = rowType;
    builder_ = new PredicateBuilder(rowType);
    analyzer_ = analyzer;
  }

  @Nullable
  public Predicate convertIgnoreFailure(Expr filter) {
    ConverterResult result = convertExpr(filter);
    return result.isFailed() ? null : result.getPredicate();
  }

  public ConverterResult convertExpr(Expr filter) {
    if (!(filter instanceof org.apache.impala.analysis.Predicate)) {
      return ConverterResult.failed();
    }
    try {
      if (filter instanceof CompoundPredicate) {
        return convertCompound((CompoundPredicate) filter);
      } else if (filter instanceof BinaryPredicate) {
        return convertBinary((BinaryPredicate) filter);
      } else if (filter instanceof IsNullPredicate) {
        return convertIsNull((IsNullPredicate) filter);
      } else if (filter instanceof InPredicate) {
        return convertIn((InPredicate) filter);
      } else if (filter instanceof LikePredicate) {
        return convertLike((LikePredicate) filter);
      } else if (filter instanceof BoolLiteral) {
        return convertBool((BoolLiteral) filter);
      }
      return ConverterResult.failed();
    } catch (Exception e) {
      return ConverterResult.failed();
    }
  }

  private ConverterResult convertCompound(CompoundPredicate compoundPredicate)
      throws ImpalaRuntimeException {
    switch (compoundPredicate.getOp()) {
      case AND: {
        ConverterResult left = convertExpr(compoundPredicate.getChild(0));
        if (left.isFailed()) return ConverterResult.failed();
        ConverterResult right = convertExpr(compoundPredicate.getChild(1));
        if (right.isFailed()) return ConverterResult.failed();
        ConversionStatus status = mergeStatus(left.getStatus(), right.getStatus());
        return new ConverterResult(
            PredicateBuilder.and(left.getPredicate(), right.getPredicate()), status,
            left.isPartitionOnly() || right.isPartitionOnly());
      }
      case OR: {
        ConverterResult left = convertExpr(compoundPredicate.getChild(0));
        if (left.isFailed()) return ConverterResult.failed();
        ConverterResult right = convertExpr(compoundPredicate.getChild(1));
        if (right.isFailed()) return ConverterResult.failed();
        ConversionStatus status = mergeStatus(left.getStatus(), right.getStatus());
        return new ConverterResult(
            PredicateBuilder.or(left.getPredicate(), right.getPredicate()), status,
            left.isPartitionOnly() || right.isPartitionOnly());
      }
      case NOT: {
        // Paimon rewrites NOT (d < x) to d >= x. That drops -0.0, which Impala
        // treats as equal to 0.0, and it can match NaN.
        if (hasFloatingPointComparison(compoundPredicate.getChild(0))) {
          return ConverterResult.failed();
        }
        ConverterResult child = convertExpr(compoundPredicate.getChild(0));
        // A partial conversion is a superset of the original predicate. Negating that
        // superset drops rows the original NOT would keep, and Paimon applies the
        // filter before Impala can re-check it.
        if (child.isFailed() || !child.isFullyConverted()) {
          return ConverterResult.failed();
        }
        Optional<Predicate> negate = child.getPredicate().negate();
        if (!negate.isPresent()) {
          throw new ImpalaRuntimeException(String.format("Negation of predicate %s is "
              + "not possible", compoundPredicate.toSql()));
        }
        return new ConverterResult(
            negate.get(), ConversionStatus.FULLY_CONVERTED, child.isPartitionOnly());
      }
      default:
        return ConverterResult.failed();
    }
  }

  private static ConversionStatus mergeStatus(
      ConversionStatus a, ConversionStatus b) {
    if (a == ConversionStatus.PARTIALLY_CONVERTED
        || b == ConversionStatus.PARTIALLY_CONVERTED) {
      return ConversionStatus.PARTIALLY_CONVERTED;
    }
    return ConversionStatus.FULLY_CONVERTED;
  }

  private ConverterResult convertBinary(BinaryPredicate binaryPredicate)
      throws ImpalaRuntimeException {
    Expr columnExpr;
    LiteralExpr literal;
    BinaryPredicate.Operator op = binaryPredicate.getOp();
    LiteralExpr rightLit = asComparableLiteral(binaryPredicate.getChild(1));
    if (rightLit != null) {
      columnExpr = binaryPredicate.getChild(0);
      literal = rightLit;
    } else {
      LiteralExpr leftLit = asComparableLiteral(binaryPredicate.getChild(0));
      if (leftLit == null) {
        throw new ImpalaRuntimeException(
            String.format("Invalid child expression: %s", binaryPredicate.toSql()));
      }
      columnExpr = binaryPredicate.getChild(1);
      literal = leftLit;
      op = flipComparisonOp(op);
    }
    SlotRef attribute = getColumnSlotRef(columnExpr);
    int index = getAttributeFieldIndex(attribute);
    rejectCharComparison(index, binaryPredicate.toSql());
    rejectBinaryComparison(index, binaryPredicate.toSql());
    if (literal instanceof NullLiteral || Expr.IS_NULL_LITERAL.apply(literal)) {
      // <=> and IS NOT DISTINCT FROM NULL are IS NULL. IS DISTINCT FROM NULL is
      // IS NOT NULL. Ordinary comparisons with NULL are not pushed.
      if (op == BinaryPredicate.Operator.NULL_MATCHING_EQ
          || op == BinaryPredicate.Operator.NOT_DISTINCT) {
        return fully(builder_.isNull(index));
      }
      if (op == BinaryPredicate.Operator.DISTINCT_FROM) {
        return fully(builder_.isNotNull(index));
      }
      checkNullLiteral(literal);
    }
    checkComparisonTypes(columnExpr, literal, attribute);
    Object literalValue = convertLiteral(index, literal);
    if (literalValue == null) {
      throw new ImpalaRuntimeException("Unable to convert literal for pushdown");
    }
    if (isFloatingPointColumn(index)) {
      return convertFloatingComparison(index, op, literalValue);
    }
    switch (op) {
      case EQ:
        return fully(builder_.equal(index, literalValue));
      case NE:
        return fully(builder_.notEqual(index, literalValue));
      case GT:
        return fully(builder_.greaterThan(index, literalValue));
      case GE:
        return fully(builder_.greaterOrEqual(index, literalValue));
      case LT:
        return fully(builder_.lessThan(index, literalValue));
      case LE:
        return fully(builder_.lessOrEqual(index, literalValue));
      case NULL_MATCHING_EQ:
      case NOT_DISTINCT:
        // A non-null literal does not match NULL, so this is equality plus IS NOT NULL.
        return fully(PredicateBuilder.and(
            builder_.isNotNull(index), builder_.equal(index, literalValue)));
      case DISTINCT_FROM:
        return fully(PredicateBuilder.or(
            builder_.isNull(index), builder_.notEqual(index, literalValue)));
      default:
        throw new ImpalaRuntimeException(
            String.format("Unsupported operator: %s", binaryPredicate.getOp()));
    }
  }

  private static ConverterResult convertBool(BoolLiteral boolLiteral) {
    return ConverterResult.failed();
  }

  private ConverterResult convertIsNull(IsNullPredicate isNullPredicate)
      throws ImpalaRuntimeException {
    SlotRef attribute = getColumnSlotRef(isNullPredicate.getChild(0));
    int index = getAttributeFieldIndex(attribute);
    if (!isNullPredicate.isNotNull()) {
      return fully(builder_.isNull(index));
    }
    return fully(builder_.isNotNull(index));
  }

  private ConverterResult convertIn(InPredicate in) throws ImpalaRuntimeException {
    SlotRef attribute = getColumnSlotRef(in.getChild(0));
    int index = getAttributeFieldIndex(attribute);
    rejectCharComparison(index, in.toSql());
    rejectBinaryComparison(index, in.toSql());
    boolean floating = isFloatingPointColumn(index);
    List<Object> values = Lists.newArrayList();
    for (int i = 1; i < in.getChildren().size(); ++i) {
      LiteralExpr literal = asComparableLiteral(in.getChild(i));
      if (literal == null) {
        throw new ImpalaRuntimeException(
            String.format("Expression is not a literal: %s", in.getChild(i).toSql()));
      }
      checkComparisonTypes(in.getChild(0), literal, attribute);
      checkNullLiteral(literal);
      Object value = convertLiteral(index, literal);
      if (value == null) {
        throw new ImpalaRuntimeException(
            "Unable to convert IN-list literal for pushdown");
      }
      values.add(normalizeSignedZero(value));
      if (floating && isFloatingZero(value)) {
        Object negZero = negativeZero(value);
        if (!containsRawBits(values, negZero)) values.add(negZero);
      }
    }
    if (in.isNotIn()) {
      // Float NOT IN is not a data filter: file min/max omit NaN, so Paimon would
      // skip a file of the literal plus NaN. NaN != x is true in Impala.
      return new ConverterResult(builder_.notIn(index, values),
          ConversionStatus.FULLY_CONVERTED, floating);
    }
    return fully(builder_.in(index, values));
  }

  private ConverterResult convertLike(LikePredicate likePredicate)
      throws ImpalaRuntimeException {
    SlotRef attribute = getColumnSlotRef(likePredicate.getChild(0));
    if (!(likePredicate.getChild(1) instanceof StringLiteral)) {
      throw new ImpalaRuntimeException("LIKE pattern must be a string literal");
    }
    StringLiteral literal = (StringLiteral) likePredicate.getChild(1);
    Preconditions.checkArgument(literal.isValidUtf8());
    int index = getAttributeFieldIndex(attribute);
    if (rowType_.getTypeAt(index).getTypeRoot() == DataTypeRoot.CHAR) {
      throw new ImpalaRuntimeException(String.format(
          "%s on CHAR column is unsupported", likePredicate.toSql()));
    }
    String pattern = literal.getUnescapedValue();
    Predicate predicate = extractLikePredicate(likePredicate.getOp(), index, pattern);
    if (predicate == null) return ConverterResult.failed();
    ConversionStatus status = isEquivalentPattern(likePredicate.getOp(), pattern) ?
        ConversionStatus.FULLY_CONVERTED :
        ConversionStatus.PARTIALLY_CONVERTED;
    return new ConverterResult(predicate, status);
  }

  private static ConverterResult fully(Predicate predicate) {
    return new ConverterResult(predicate, ConversionStatus.FULLY_CONVERTED);
  }

  private int getAttributeFieldIndex(SlotRef slotRef) {
    String field = slotRef.getResolvedPath().destColumn().getName();
    return fieldIndex(field);
  }

  private static void checkNullLiteral(LiteralExpr literal)
      throws ImpalaRuntimeException {
    if (Expr.IS_NULL_LITERAL.apply(literal)) {
      throw new ImpalaRuntimeException(
          "Expression can't be NULL literal: " + literal.toSql());
    }
  }

  private void checkComparisonTypes(Expr columnExpr, LiteralExpr literal,
      SlotRef attribute) throws ImpalaRuntimeException {
    if (!columnExpr.getType().equals(literal.getType())) {
      Type storageType = PaimonImpalaTypeUtils.toImpalaType(
          rowType_.getTypeAt(getAttributeFieldIndex(attribute)));
      if (storageType.isDecimal() && literal.getType().isNumericType()) {
        // Analyzed decimal comparisons may use integer literals.
      } else if (columnExpr.getType().isStringType()
          && literal.getType().isStringType()) {
        // STRING/VARCHAR/CHAR literals and columns may differ in analyzed types.
      } else {
        throw new ImpalaRuntimeException(String.format(
            "Type mismatch between column (%s) and literal (%s)",
            columnExpr.getType().toSql(), literal.getType().toSql()));
      }
    }
    Type storageType = PaimonImpalaTypeUtils.toImpalaType(
        rowType_.getTypeAt(getAttributeFieldIndex(attribute)));
    PrimitiveType columnPrimitive = storageType.getPrimitiveType();
    PrimitiveType literalPrimitive = literal.getType().getPrimitiveType();
    if (columnPrimitive == literalPrimitive) return;
    if (columnPrimitive == PrimitiveType.DECIMAL && literal.getType().isNumericType()) {
      return;
    }
    if (storageType.isStringType() && literal.getType().isStringType()) return;
    throw new ImpalaRuntimeException(String.format(
        "Literal type %s does not match column type %s",
        literal.getType().toSql(), storageType.toSql()));
  }

  private void rejectCharComparison(int index, String exprSql)
      throws ImpalaRuntimeException {
    if (rowType_.getTypeAt(index).getTypeRoot() == DataTypeRoot.CHAR) {
      throw new ImpalaRuntimeException(
          String.format("Predicates on CHAR columns are unsupported: %s", exprSql));
    }
  }

  private void rejectBinaryComparison(int index, String exprSql)
      throws ImpalaRuntimeException {
    if (rowType_.getTypeAt(index).getTypeRoot() == DataTypeRoot.BINARY) {
      throw new ImpalaRuntimeException(
          String.format("Predicates on BINARY columns are unsupported: %s", exprSql));
    }
  }

  /**
   * A literal, or a cast of a numeric literal to FLOAT or DOUBLE. The frontend
   * constant folder leaves CAST(-1e-46 AS FLOAT) unfolded because the value
   * underflows a decimal literal; narrowing it here yields -0.0.
   */
  private static LiteralExpr asComparableLiteral(Expr expr) {
    if (expr instanceof LiteralExpr) return (LiteralExpr) expr;
    if (!(expr instanceof CastExpr) || !(expr.getChild(0) instanceof NumericLiteral)
        || expr.getType() == null || !expr.getType().isFloatingPointType()) {
      return null;
    }
    try {
      return new NumericLiteral(
          ((NumericLiteral) expr.getChild(0)).getDoubleValue(), expr.getType());
    } catch (AnalysisException e) {
      return null;
    }
  }

  private boolean isFloatingPointColumn(int index) {
    DataTypeRoot root = rowType_.getTypeAt(index).getTypeRoot();
    return root == DataTypeRoot.FLOAT || root == DataTypeRoot.DOUBLE;
  }

  /**
   * Paimon orders floats with compareTo: -0.0 is less than 0.0, and NaN is greater
   * than every value, including +Infinity. Impala treats -0.0 as equal to 0.0, and
   * ordered comparisons with NaN are false. Rewrite the predicate so the two agree.
   * != and IS DISTINCT FROM stay partition-only; see {@link ConverterResult}.
   */
  private ConverterResult convertFloatingComparison(
      int index, BinaryPredicate.Operator op, Object literalValue)
      throws ImpalaRuntimeException {
    literalValue = normalizeSignedZero(literalValue);
    boolean zero = isFloatingZero(literalValue);
    Object negZero = negativeZero(literalValue);
    Object posInf = positiveInfinity(literalValue);
    switch (op) {
      case EQ:
        if (!zero) return fully(builder_.equal(index, literalValue));
        return fully(builder_.in(index, Lists.newArrayList(literalValue, negZero)));
      case NE:
        if (!zero) return partitionOnly(builder_.notEqual(index, literalValue));
        return partitionOnly(
            builder_.notIn(index, Lists.newArrayList(literalValue, negZero)));
      case GT:
        return fully(excludeNan(
            index, builder_.greaterThan(index, literalValue), posInf));
      case GE: {
        Object bound = zero ? negZero : literalValue;
        return fully(excludeNan(
            index, builder_.greaterOrEqual(index, bound), posInf));
      }
      case LT:
        if (!zero) return fully(builder_.lessThan(index, literalValue));
        return fully(builder_.lessThan(index, negZero));
      case LE:
        return fully(builder_.lessOrEqual(index, literalValue));
      case NULL_MATCHING_EQ:
      case NOT_DISTINCT: {
        // A non-null literal does not match NULL, so this is equality plus IS NOT NULL.
        Predicate eq = zero ?
            builder_.in(index, Lists.newArrayList(literalValue, negZero)) :
            builder_.equal(index, literalValue);
        return fully(PredicateBuilder.and(builder_.isNotNull(index), eq));
      }
      case DISTINCT_FROM: {
        Predicate ne = zero ?
            builder_.notIn(index, Lists.newArrayList(literalValue, negZero)) :
            builder_.notEqual(index, literalValue);
        return partitionOnly(PredicateBuilder.or(builder_.isNull(index), ne));
      }
      default:
        throw new ImpalaRuntimeException(
            String.format("Unsupported operator: %s", op));
    }
  }

  /** d > x and d >= x also require d <= +Infinity, which excludes NaN. */
  private Predicate excludeNan(int index, Predicate comparison, Object posInf) {
    return PredicateBuilder.and(comparison, builder_.lessOrEqual(index, posInf));
  }

  private static ConverterResult partitionOnly(Predicate predicate) {
    return new ConverterResult(predicate, ConversionStatus.FULLY_CONVERTED, true);
  }

  private static Object normalizeSignedZero(Object value) {
    if (value instanceof Float && ((Float) value).floatValue() == 0.0f) return 0.0f;
    if (value instanceof Double && ((Double) value).doubleValue() == 0.0d) return 0.0d;
    return value;
  }

  private static boolean isFloatingZero(Object value) {
    if (value instanceof Float) return ((Float) value).floatValue() == 0.0f;
    if (value instanceof Double) return ((Double) value).doubleValue() == 0.0d;
    return false;
  }

  private static Object negativeZero(Object value) {
    if (value instanceof Float) return -0.0f;
    return -0.0d;
  }

  private static Object positiveInfinity(Object value) {
    if (value instanceof Float) return Float.POSITIVE_INFINITY;
    return Double.POSITIVE_INFINITY;
  }

  private static boolean containsRawBits(List<Object> values, Object candidate) {
    for (Object value : values) {
      if (value instanceof Float && candidate instanceof Float
          && Float.floatToRawIntBits((Float) value)
              == Float.floatToRawIntBits((Float) candidate)) {
        return true;
      }
      if (value instanceof Double && candidate instanceof Double
          && Double.doubleToRawLongBits((Double) value)
              == Double.doubleToRawLongBits((Double) candidate)) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when a comparison or IN predicate references a floating-point column.
   * Negating that comparison is not equivalent under Paimon's total order.
   */
  private static boolean hasFloatingPointComparison(Expr expr) {
    return expr.contains((com.google.common.base.Predicate<Expr>) e -> {
      if (!(e instanceof BinaryPredicate) && !(e instanceof InPredicate)) return false;
      for (Expr child : e.getChildren()) {
        if (child instanceof LiteralExpr) continue;
        if (child.getType() != null && child.getType().isFloatingPointType()) {
          return true;
        }
      }
      return false;
    });
  }

  private static BinaryPredicate.Operator flipComparisonOp(BinaryPredicate.Operator op)
      throws ImpalaRuntimeException {
    switch (op) {
      case GT: return BinaryPredicate.Operator.LT;
      case GE: return BinaryPredicate.Operator.LE;
      case LT: return BinaryPredicate.Operator.GT;
      case LE: return BinaryPredicate.Operator.GE;
      case EQ:
      case NE:
      case NULL_MATCHING_EQ:
      case NOT_DISTINCT:
      case DISTINCT_FROM:
        return op;
      default:
        throw new ImpalaRuntimeException(
            String.format("Unsupported flipped operator: %s", op));
    }
  }

  private SlotRef getColumnSlotRef(Expr expr) throws ImpalaRuntimeException {
    if (expr instanceof CastExpr && !((CastExpr) expr).isImplicit()) {
      throw new ImpalaRuntimeException(
          String.format("Explicit cast not supported for pushdown: %s", expr.toSql()));
    }
    if (!(expr instanceof SlotRef) && !(expr instanceof CastExpr)) {
      throw new ImpalaRuntimeException(
          String.format("Unable to create term from expression: %s", expr.toSql()));
    }
    SlotRef slotRef = expr.unwrapSlotRef(true);
    if (slotRef == null) {
      throw new ImpalaRuntimeException(
          String.format("Unable to create term from expression: %s", expr.toSql()));
    }
    return slotRef;
  }

  private static String unescapeLikeLiteral(String fragment) {
    StringBuilder sb = new StringBuilder(fragment.length());
    boolean isEscaped = false;
    for (int i = 0; i < fragment.length(); i++) {
      char c = fragment.charAt(i);
      if (!isEscaped && c == LIKE_ESCAPE_CHAR) {
        isEscaped = true;
        continue;
      }
      sb.append(c);
      isEscaped = false;
    }
    return sb.toString();
  }

  /**
   * True when the pushed startsWith/endsWith/equal predicate matches the original
   * pattern. A prefix plus a suffix is only a superset: {@code startsWith('ab') AND
   * endsWith('bc')} accepts {@code abc}, which {@code LIKE 'ab%bc'} rejects.
   */
  private static boolean isEquivalentPattern(
      LikePredicate.Operator op, String patternStr) {
    if (op == LikePredicate.Operator.LIKE) return isEquivalentLikePattern(patternStr);
    if (REGEXP_EQUALS_PATTERN.matcher(patternStr).matches()
        || REGEXP_STARTS_WITH_PATTERN.matcher(patternStr).matches()
        || REGEXP_ENDS_WITH_PATTERN.matcher(patternStr).matches()) {
      return true;
    }
    return false;
  }

  /**
   * Equivalent LIKE patterns are an exact string, a literal followed only by {@code %},
   * or {@code %} followed only by a literal. '_' and a wildcard span in the middle are
   * supersets of the pushed predicate.
   */
  private static boolean isEquivalentLikePattern(String patternStr) {
    int first = -1;
    int last = -1;
    boolean isEscaped = false;
    boolean sawUnderscore = false;
    for (int i = 0; i < patternStr.length(); i++) {
      char c = patternStr.charAt(i);
      if (isEscaped) {
        isEscaped = false;
        continue;
      }
      if (c == LIKE_ESCAPE_CHAR) {
        isEscaped = true;
        continue;
      }
      if (c == '_') {
        sawUnderscore = true;
        if (first == -1) first = i;
        last = i;
      } else if (c == '%') {
        if (first == -1) first = i;
        last = i;
      }
    }
    if (isEscaped || sawUnderscore) return false;
    if (first == -1) return true;
    boolean atStart = first == 0;
    boolean atEnd = last == patternStr.length() - 1;
    // A wildcard run at both ends, or a gap that is not all '%', is not equivalent.
    if (atStart == atEnd) return false;
    for (int i = first; i <= last; i++) {
      if (patternStr.charAt(i) != '%') return false;
    }
    return true;
  }

  @Nullable
  private Predicate extractLikePredicate(
      LikePredicate.Operator op, int colIndex, String patternStr) {
    switch (op) {
      case LIKE: {
        int first = -1;
        int last = -1;
        boolean isEscaped = false;
        for (int i = 0; i < patternStr.length(); i++) {
          char c = patternStr.charAt(i);
          if (isEscaped) {
            isEscaped = false;
            continue;
          }
          if (c == LIKE_ESCAPE_CHAR) {
            isEscaped = true;
            continue;
          }
          if (c == '%' || c == '_') {
            if (first == -1) first = i;
            last = i;
          }
        }
        if (isEscaped) return null;

        if (first == -1) {
          return builder_.equal(
              colIndex, BinaryString.fromString(unescapeLikeLiteral(patternStr)));
        }
        String prefix =
            first > 0 ? unescapeLikeLiteral(patternStr.substring(0, first)) : "";
        String suffix = last < patternStr.length() - 1 ?
            unescapeLikeLiteral(patternStr.substring(last + 1)) :
            "";
        Predicate startsWith = prefix.isEmpty() ?
            null :
            builder_.startsWith(colIndex, BinaryString.fromString(prefix));
        Predicate endsWith = suffix.isEmpty() ?
            null :
            builder_.endsWith(colIndex, BinaryString.fromString(suffix));
        if (startsWith != null && endsWith != null) {
          return PredicateBuilder.and(startsWith, endsWith);
        }
        return startsWith != null ? startsWith : endsWith;
      }
      case RLIKE:
      case REGEXP: {
        Matcher startMatch = REGEXP_STARTS_WITH_PATTERN.matcher(patternStr);
        Matcher endMatch = REGEXP_ENDS_WITH_PATTERN.matcher(patternStr);
        Matcher eqMatch = REGEXP_EQUALS_PATTERN.matcher(patternStr);
        Matcher startsOrEndsWithMatch =
            REGEXP_STARTS_ENDS_WITH_PATTERN.matcher(patternStr);

        if (startMatch.matches()) {
          String matched = startMatch.group(1);
          return builder_.startsWith(colIndex, BinaryString.fromString(matched));
        } else if (endMatch.matches()) {
          String matched = endMatch.group(1);
          return builder_.endsWith(colIndex, BinaryString.fromString(matched));
        } else if (eqMatch.matches()) {
          String matched = eqMatch.group(1);
          return builder_.equal(colIndex, BinaryString.fromString(matched));
        } else if (startsOrEndsWithMatch.matches()) {
          String prefix = startsOrEndsWithMatch.group(1);
          String suffix = startsOrEndsWithMatch.group(2);
          return PredicateBuilder.and(
              builder_.startsWith(colIndex, BinaryString.fromString(prefix)),
              builder_.endsWith(colIndex, BinaryString.fromString(suffix)));
        }
        return null;
      }
      default:
        return null;
    }
  }

  public Object convertLiteral(String field, LiteralExpr value)
      throws ImpalaRuntimeException {
    return convertLiteral(fieldIndex(field), value);
  }

  private Object convertLiteral(int index, LiteralExpr value)
      throws ImpalaRuntimeException {
    DataType columnType = rowType_.getTypeAt(index);
    Object v;
    if (value instanceof BoolLiteral) {
      v = ((BoolLiteral) value).getValue();
    } else if (value instanceof NumericLiteral) {
      v = getNumericValue(columnType, (NumericLiteral) value);
    } else if (value instanceof DateLiteral) {
      v = LocalDate.ofEpochDay(((DateLiteral) value).getValue());
    } else if (value instanceof StringLiteral) {
      v = ((StringLiteral) value).getUnescapedValue();
    } else if (value instanceof TimestampLiteral) {
      v = getTimestampValue(columnType, value);
    } else if (value instanceof NullLiteral) {
      return null;
    } else {
      throw new ImpalaRuntimeException(
          "Unsupported literal type " + value.getType().prettyPrint());
    }
    Object converted = convertJavaObject(columnType, v);
    if (converted == null) {
      throw new ImpalaRuntimeException(
          String.format("Unable to convert literal %s for column type %s",
              value.toSql(), columnType.asSQLString()));
    }
    return converted;
  }

  private Object getNumericValue(DataType columnType, NumericLiteral literal)
      throws ImpalaRuntimeException {
    PrimitiveType literalType = literal.getType().getPrimitiveType();
    switch (literalType) {
      case TINYINT:
      case SMALLINT:
      case INT:
        return literal.getIntValue();
      case BIGINT:
        return literal.getLongValue();
      case FLOAT: {
        float f = (float) literal.getDoubleValue();
        if (Float.isNaN(f) || Float.isInfinite(f)) {
          throw new ImpalaRuntimeException("Non-finite float literal cannot be pushed");
        }
        return f;
      }
      case DOUBLE: {
        double d = literal.getDoubleValue();
        if (Double.isNaN(d) || Double.isInfinite(d)) {
          throw new ImpalaRuntimeException("Non-finite double literal cannot be pushed");
        }
        return d;
      }
      case DECIMAL:
        if (!(columnType instanceof DecimalType)) {
          throw new ImpalaRuntimeException("DECIMAL column type expected");
        }
        return getDecimalValue((DecimalType) columnType, literal);
      default:
        throw new ImpalaRuntimeException(
            String.format("Unsupported numeric literal type %s", literalType));
    }
  }

  private static BigDecimal getDecimalValue(
      DecimalType columnType, NumericLiteral literal) throws ImpalaRuntimeException {
    BigDecimal literalValue = literal.getValue();
    int scale = columnType.getScale();
    if (literalValue.scale() > scale) {
      throw new ImpalaRuntimeException(String.format(
          "Invalid scale %d for decimal column scale %d",
          literalValue.scale(), scale));
    }
    if (literalValue.precision() - literalValue.scale() > columnType.getPrecision()
        - scale) {
      throw new ImpalaRuntimeException("Decimal literal out of range for column");
    }
    if (literalValue.scale() < scale) {
      return literalValue.setScale(scale);
    }
    return literalValue;
  }

  private Object getTimestampValue(DataType columnType, LiteralExpr literal)
      throws ImpalaRuntimeException {
    if (literal instanceof TimestampLiteral
        && hasSubMicroFraction((TimestampLiteral) literal)) {
      throw new ImpalaRuntimeException(String.format(
          "Sub-microsecond timestamp literals are not pushed: %s", literal.toSql()));
    }
    if (analyzer_ == null) {
      return LocalDateTime.parse(literal.getStringValue().replace(' ', 'T'));
    }
    try {
      if (columnType instanceof LocalZonedTimestampType) {
        // Paimon expects one Instant; skip push when session-local time is ambiguous
        // or invalid (e.g. DST fall-back / spring-forward gaps).
        Long preMicros = ExprUtil.localTimestampToUnixTimeMicros(
            analyzer_, literal, true);
        Long postMicros = ExprUtil.localTimestampToUnixTimeMicros(
            analyzer_, literal, false);
        if (preMicros == null || postMicros == null
            || !preMicros.equals(postMicros)) {
          throw new ImpalaRuntimeException(String.format(
              "Non-unique or invalid local timestamp literal is not pushed: %s",
              literal.toSql()));
        }
        return microsToUtcInstant(preMicros);
      }
      if (columnType instanceof TimestampType) {
        long micros = ExprUtil.utcTimestampToUnixTimeMicros(analyzer_, literal);
        return microsToUtcLocalDateTime(micros);
      }
    } catch (AnalysisException | InternalException e) {
      throw new ImpalaRuntimeException(
          String.format("Unable to parse timestamp value from: %s", literal.toSql()));
    }
    return LocalDateTime.parse(literal.getStringValue().replace(' ', 'T'));
  }

  /**
   * True when the literal has a non-zero sub-microsecond fraction. Impala formats
   * any fractional timestamp with nine digits, so the string length is not the
   * precision of the value.
   */
  static boolean hasSubMicroFraction(TimestampLiteral literal) {
    return literal.getNanosOfDay() % 1000 != 0;
  }

  private static Instant microsToUtcInstant(long micros) {
    long secs = Math.floorDiv(micros, 1_000_000L);
    int nanos = (int) (Math.floorMod(micros, 1_000_000L) * 1000L);
    return Instant.ofEpochSecond(secs, nanos);
  }

  private static LocalDateTime microsToUtcLocalDateTime(long micros) {
    return LocalDateTime.ofInstant(microsToUtcInstant(micros), ZoneOffset.UTC);
  }

  private int fieldIndex(String field) {
    return PaimonUtil.getFieldIndexByNameIgnoreCase(rowType_, field);
  }
}
