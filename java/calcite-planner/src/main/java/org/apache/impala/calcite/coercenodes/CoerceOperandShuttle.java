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

package org.apache.impala.calcite.coercenodes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlDatetimePlusOperator;
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Util;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.functions.ImplicitTypeChecker;
import org.apache.impala.calcite.operators.ImpalaDecodeFunction;
import org.apache.impala.calcite.type.ImpalaTypeConverter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CoerceOperandShuttle is a RexShuttle that walks through a RexNode and changes
 * it to match a function signature within Impala. It also is responsible for
 * changing RexLiteral types. It changes all CHAR literal types to STRING literal
 * types. It changes Integer numeric literals to the smallest type which can hold the
 * integer (e.g. 2 gets changed from INTEGER to TINYINT). It also changes RexInputRefs
 * to match its input type.
 *
 * One small added responsibility is to take the "Sarg" call and call the Calcite
 * RexUtil.expandSearch method and expands it to something Impala understands. There
 * are various Impala rules that create this "Sarg" method. Impala-13369 has been filed to
 * investigate if there is a more optimal Impala function that can be used.
 */

public class CoerceOperandShuttle extends RexShuttle {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CoerceOperandShuttle.class.getName());
  private final RelDataTypeFactory factory;
  private final RexBuilder rexBuilder;
  private final List<RelNode> inputs;

  public static Set<SqlKind> NO_CAST_OPERATORS =
      ImmutableSet.<SqlKind> builder()
      // Cast doesn't need any operand casting because it is already a cast.
      .add(SqlKind.CAST)
      // OR and AND operands are always boolean. Just skip processing rather
      // than remove the varargs (since these can have many operands and Impala's
      // signature only has 2 operands)
      .add(SqlKind.OR)
      .add(SqlKind.AND)
      .build();

  public CoerceOperandShuttle(RelDataTypeFactory factory, RexBuilder rexBuilder,
      List<RelNode> inputs) {
    this.factory = factory;
    this.rexBuilder = rexBuilder;
    this.inputs = inputs;
  }

  @Override
  public RexNode visitCall(RexCall call) {

    // Eliminate the "Sarg" function which is unknown to Impala.
    // TODO: this is kinda hacky. It would be better if Impala can handle this
    // directly, so this needs investigation.
    if (call.getOperator().getKind().equals(SqlKind.SEARCH)) {
      return visitCall((RexCall) RexUtil.expandSearch(rexBuilder, null, call));
    }

    // recursively call all embedded RexCalls first
    RexCall castedOperandsCall = (RexCall) super.visitCall(call);

    // Certain operators will never need casting for their operands.
    if (!isCastingNeeded(castedOperandsCall)) {
      return castedOperandsCall;
    }

    // had to special case decode since the casting is different from common
    // functions.
    if (castedOperandsCall.getOperator().getName().equals("DECODE")) {
      return castDecodedFunction(castedOperandsCall, factory, rexBuilder);
    }

    Function fn = FunctionResolver.getSupertypeFunction(castedOperandsCall);

    if (fn == null) {
      throw new RuntimeException("Could not find a matching signature for call " +
          call);
    }

    RelDataType retType = getReturnType(castedOperandsCall, fn.getReturnType());

    // This code does not handle changes in the return type when the Calcite
    // function is not a decimal but the function resolves to a function that
    // returns a decimal type. The Decimal type from the function resolver would
    // have to calculate the precision and scale based on operand types. If
    // necessary, this code should be added later.
    Preconditions.checkState(!SqlTypeUtil.isDecimal(retType) ||
        SqlTypeUtil.isDecimal(castedOperandsCall.getType()));

    // So if the original return type is Decimal and the function resolves to
    // decimal, the precision and scale are saved from the original function.
    if (SqlTypeUtil.isDecimal(retType)) {
      retType = castedOperandsCall.getType();
    }

    List<RexNode> newOperands = getCastedArgTypes(fn, castedOperandsCall.getOperands(),
        retType, factory, rexBuilder);

    // keep the original call if nothing changed, else build a new RexCall.
    return retType.equals(castedOperandsCall.getType())
           && newOperands.equals(castedOperandsCall.getOperands())
        ? castedOperandsCall
        : (RexCall) rexBuilder.makeCall(retType, castedOperandsCall.getOperator(),
            newOperands);
  }

  @Override
  public RexNode visitOver(RexOver over) {
    // recursively call all embedded RexCalls first
    RexOver castedOver = (RexOver) super.visitOver(over);

    Function fn = FunctionResolver.getSupertypeFunction(castedOver);

    if (fn == null) {
      throw new RuntimeException("Could not find a matching signature for call " +
          over);
    }

    RelDataType retType = getReturnType(castedOver, fn.getReturnType());

    List<RexNode> newOperands =
        getCastedArgTypes(fn, castedOver.getOperands(), retType, factory, rexBuilder);

    return retType.equals(castedOver.getType()) &&
           newOperands.equals(castedOver.getOperands())
        ? castedOver
        : (RexOver) rexBuilder.makeOver(retType, castedOver.getAggOperator(),
              newOperands, castedOver.getWindow().partitionKeys,
              castedOver.getWindow().orderKeys, castedOver.getWindow().getLowerBound(),
              castedOver.getWindow().getUpperBound(), castedOver.getWindow().isRows(),
              true /*allowPartial*/, false /*nullWhenCountZero*/, castedOver.isDistinct(),
              castedOver.ignoreNulls());
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal) {
    // Coerce CHAR literal types into STRING
    if (literal.getType().getSqlTypeName().equals(SqlTypeName.CHAR)) {
      return rexBuilder.makeLiteral(RexLiteral.stringValue(literal),
          ImpalaTypeConverter.getRelDataType(Type.STRING), true, true);
    }

    // Coerce INTEGER literal types into the smallest possible Numeric type
    if (literal.getType().getSqlTypeName().equals(SqlTypeName.INTEGER)) {
      BigDecimal bd0 = literal.getValueAs(BigDecimal.class);
      RelDataType type = ImpalaTypeConverter.getLiteralDataType(bd0, literal.getType());
      return rexBuilder.makeLiteral(bd0, type);
    }
    return literal;
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    // Adjust the InputRef type if it changed
    RelDataType inputRefIndexType = getInputRefIndexType(inputs, inputRef.getIndex());

    return inputRef.getType().equals(inputRefIndexType)
        ? inputRef
        : rexBuilder.makeInputRef(inputRefIndexType, inputRef.getIndex());
  }


  private RelDataType getReturnType(RexNode rexNode, Type impalaReturnType) {

    RelDataType retType = ImpalaTypeConverter.getRelDataType(impalaReturnType);

    // This code does not handle changes in the return type when the Calcite
    // function is not a decimal but the function resolves to a function that
    // returns a decimal type. The Decimal type from the function resolver would
    // have to calculate the precision and scale based on operand types. If
    // necessary, this code should be added later.
    Preconditions.checkState(!SqlTypeUtil.isDecimal(retType) ||
        SqlTypeUtil.isDecimal(rexNode.getType()));

    // So if the original return type is Decimal and the function resolves to
    // decimal, the precision and scale are saved from the original function.
    if (SqlTypeUtil.isDecimal(retType)) {
      retType = rexNode.getType();
    }

    return retType;
  }

  private boolean isCastingNeeded(RexCall rexCall) {
    if (NO_CAST_OPERATORS.contains(rexCall.getOperator().getKind())) {
      return false;
    }

    // Time stamp operators don't require casting. When they are converted
    // into Expr objects, they undergo special processing, so we leave them
    // as/is.
    if (isTimestampArithExpr(rexCall)) {
      return false;
    }

    if (rexCall.getOperator().getName().equals("EXPLICIT_CAST")) {
      return false;
    }

    return true;
  }

  private static boolean isTimestampArithExpr(RexCall rexCall) {
    return rexCall.getOperator() instanceof SqlDatetimePlusOperator
        || rexCall.getOperator() instanceof SqlDatetimeSubtractionOperator
        || SqlTypeName.INTERVAL_TYPES.contains(rexCall.getType().getSqlTypeName())
        || ((rexCall.getOperator().equals("+") || rexCall.getOperator().equals("-")) &&
            (SqlTypeUtil.isDatetime(rexCall.getOperands().get(0).getType()) ||
            SqlTypeUtil.isDatetime(rexCall.getOperands().get(1).getType())));
  }

  /**
   * Handle getting the type of index. If there is only one input, then we
   * just use the index value to get the type. If there are two inputs,
   * then the second input's index value starts at the number which is
   * the size of the first input.
   */
  private RelDataType getInputRefIndexType(List<RelNode> inputs, int index) {
    if (inputs.size() == 1) {
      return inputs.get(0).getRowType().getFieldList().get(index).getType();
    }

    // currently only works for joins which have 2 inputs
    Preconditions.checkState(inputs.size() == 2);
    List<RelDataTypeField> leftFieldList = inputs.get(0).getRowType().getFieldList();

    // If the index number is greater than or equal to the number of fields in
    // the left input, it must be in the right input.
    if (index < leftFieldList.size()) {
      return leftFieldList.get(index).getType();
    }
    int rightIndex = index - leftFieldList.size();
    return inputs.get(1).getRowType().getFieldList().get(rightIndex).getType();
  }

  /**
   * Return a list of the operands, casting whenever needed.
   */
  private static List<RexNode> getCastedArgTypes(Function fn, List<RexNode> operands,
      RelDataType retType, RelDataTypeFactory factory, RexBuilder rexBuilder) {
    List<RelDataType> argTypes = Util.transform(operands, RexNode::getType);
    List<RexNode> newOperands = new ArrayList<>();
    // The "Case" operator is special because the operands alternate between
    // "when" and "then" conditions, and the "when" conditions are always
    // boolean, so they don't need casting.
    boolean isCaseFunction = isCaseFunction(fn);
    boolean castedOperand = false;
    Preconditions.checkState(argTypes.size() == 0 || fn.getNumArgs() > 0);
    for (int i = 0; i < argTypes.size(); ++i) {
      if (isCaseFunction &&
          FunctionResolver.shouldSkipOperandForCase(argTypes.size(), i)) {
        // if skipped, we leave the operand type as/is.
        newOperands.add(operands.get(i));
        continue;
      }

      // in the case of varargs, take the last argument in the signature.
      int indexToUse = Math.min(i, fn.getNumArgs() - 1);
      Type toImpalaType = fn.getArgs()[indexToUse];
      RelDataType toType = useReturnTypeForCastingArg(fn, argTypes.get(indexToUse))
          ? retType
          : getCastedToType(argTypes.get(i), toImpalaType, factory);

      RexNode operand = castOperand(operands.get(i), toType,
          factory, rexBuilder);
      Preconditions.checkNotNull(operand);
      newOperands.add(operand);
      if (!operands.get(i).equals(operand)) {
        castedOperand = true;
      }
    }

    return castedOperand ? newOperands : operands;
  }

  private static boolean useReturnTypeForCastingArg(Function fn, RelDataType argType) {
    // case functions use the precalculated return type from the function resolver.
    if (isCaseFunction(fn)) {
      return true;
    }

    // For functions that have decimal varargs and return a decimal
    // (e.g. greatest, least), the type has been calculated at validation time.
    return SqlTypeUtil.isDecimal(argType) &&
        fn.getReturnType().isDecimal() && fn.hasVarArgs();
  }

  private static boolean isCaseFunction(Function fn) {
    return fn.functionName().equals("case");
  }

  private static RelDataType getCastedToType(RelDataType fromType,
      Type toImpalaType, RelDataTypeFactory factory) {

    if (!toImpalaType.isDecimal() || SqlTypeUtil.isNull(fromType)) {
      return ImpalaTypeConverter.getRelDataType(toImpalaType);
    }

    // Integer based type needs special conversion to Decimal types based on the
    // size of the type of Integer (e.g. TINYINT, SMALLINT, etc...), but don't change
    // the type if the from type is also DECIMAL.
    ScalarType impalaType = (ScalarType) ImpalaTypeConverter.createImpalaType(fromType);
    ScalarType decimalType = impalaType.getMinResolutionDecimal();
    return factory.createSqlType(SqlTypeName.DECIMAL,
        decimalType.decimalPrecision(), decimalType.decimalScale());
  }

  /**
   * castOperand takes a RexNode and a toType and returns the RexNode
   * with a potential cast wrapper.  If the types match, then the
   * original RexNode is returned. If the toType is an incompatible
   * type, this method returns null.
   */
  private static RexNode castOperand(RexNode node, RelDataType toType,
      RelDataTypeFactory factory, RexBuilder rexBuilder) {
    RelDataType fromType = node.getType();


    // no need to cast if type is interval, this will get handled when
    // changing to the physical Expr object.
    if (SqlTypeUtil.isInterval(fromType)) {
      return node;
    }

    // No need to cast if types are the same
    if (fromType.getSqlTypeName().equals(toType.getSqlTypeName()) &&
        fromType.getPrecision() == toType.getPrecision() &&
        fromType.getScale() == toType.getScale()) {
      return node;
    }

    if (SqlTypeUtil.isNull(fromType)) {
      if (SqlTypeUtil.isDecimal(toType)) {
        Type impalaType = ImpalaTypeConverter.createImpalaType(Type.DECIMAL, 1, 0);
        toType = ImpalaTypeConverter.createRelDataType(impalaType);
      }
      return rexBuilder.makeCast(toType, node);
    }

    if (!ImplicitTypeChecker.supportsImplicitCasting(fromType, toType)) {
      return null;
    }

    return rexBuilder.makeCast(toType, node);
  }

  /**
   * Special casting function for the decode operator. While the case operator
   * has its operands as boolean types, the operands within the decode operator
   * can be any compatible type with the search operator. Therefore, all operands
   * need to be checked to see if they need to be cast to a compatible type, not
   * just the return parameters.
   */
  private static RexNode castDecodedFunction(RexCall decodeCall,
      RelDataTypeFactory factory, RexBuilder rexBuilder) {
    List<RexNode> operands = decodeCall.getOperands();
    List<RelDataType> argTypes =
        Util.transform(operands, RexNode::getType);
    List<RexNode> newOperands = new ArrayList<>(operands.size());

    // Grab the compatible search operand from all the search operands
    RelDataType searchOperand =
        ImpalaDecodeFunction.getCompatibleSearchOperand(argTypes, factory);
    // Grab the compatible return operand from all the return operands
    RelDataType returnType =
        ImpalaDecodeFunction.getCompatibleReturnType(argTypes, factory);

    boolean hasElse = (argTypes.size() % 2 == 0);
    int numNonElseParams = hasElse ? argTypes.size() - 1 : argTypes.size();


    // start by potentially casting all the search operands. Check the first one outside
    // of the loop.
    newOperands.add(castOperand(operands.get(0), searchOperand, factory, rexBuilder));

    // Now we loop through all the operands, alternating between search type
    // and return type. We don't include the last one in case there is an
    // else parameter
    for (int i = 1; i < numNonElseParams; ++i) {
      // Alternate between return and search
      RelDataType toType = (i % 2 == 0) ? returnType : searchOperand;
      newOperands.add(castOperand(operands.get(i), toType, factory, rexBuilder));
    }

    // Check the else param for casting
    if (hasElse) {
      int elseParam = operands.size() - 1;
      newOperands.add(castOperand(operands.get(elseParam), returnType, factory,
          rexBuilder));
    }

    // If any operand was casted, create the new RexNode, otherwise, just send back
    // the existing one.
    return operands.equals(newOperands)
        ? decodeCall
        : (RexCall) rexBuilder.makeCall(returnType, decodeCall.getOperator(),
            newOperands);
  }
}
