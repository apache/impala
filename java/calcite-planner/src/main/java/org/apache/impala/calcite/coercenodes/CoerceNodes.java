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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CoerceNodes is responsible for coercing Calcite RelNode objects into
 * RelNodes that conform to Impala requirements. The entry point is via
 * the coerceNodes method.
 *
 * The long term goal would be to have Calcite validation be responsible
 * for creating the correct parameter types and return types for functions
 * at validation time. They do have type coercion capabilities at validation
 * time, but Calcite has some shortcomings.  These shortcomings are:
 *   - Integer literals are always created as type INTEGER. In Impala,
 *     the number 2 would be treated as a TINYINT
 *   - Char literals are treated as CHAR<X> in Calcite.  In Impala, they
 *     are treated as string types
 *   - We try to use Calcite operators whenever possible since they are
 *     used and created throughout the analysis and optimization phases. This
 *     can possibly differ from Impala function checks so it is nice to have
 *     the override capabilities.
 *
 * Until we can fix these issues at validation time, we implement the coercion
 * of RelNodes and RexNodes at the end of optimization to ensure that Impala
 * has the correction functions.
 */
public class CoerceNodes{
  protected static final Logger LOG =
      LoggerFactory.getLogger(CoerceNodes.class.getName());

  /**
   * coerceNodes is the entry point method (the only public method in this class).
   */
  public static RelNode coerceNodes(RelNode relNode, RexBuilder rexBuilder) {
    RelNode newRelNode = coerceNodesInternal(relNode, rexBuilder);
    return newRelNode != null ? newRelNode : relNode;
  }

  /**
   * coerceNodesInternal is a recursive walkthrough of the RelNode tree. The bottom
   * levels are processed first.
   */
  private static RelNode coerceNodesInternal(RelNode relNode, RexBuilder rexBuilder) {
    boolean isInputChanged = false;
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : relNode.getInputs()) {
      RelNode changedInput = coerceNodesInternal(input, rexBuilder);
      isInputChanged |= (changedInput != input);
      newInputs.add(changedInput);
    }

    switch (ImpalaPlanRel.getRelNodeType(relNode)) {
      case AGGREGATE:
        return processAggNode(relNode, newInputs, rexBuilder, isInputChanged);
      case FILTER:
        return processFilterNode(relNode, newInputs, rexBuilder, isInputChanged);
      case JOIN:
        return processJoinNode(relNode, newInputs, rexBuilder, isInputChanged);
      case PROJECT:
        return processProjectNode(relNode, newInputs, rexBuilder, isInputChanged);
      case SORT:
        return processSortNode(relNode, newInputs, rexBuilder, isInputChanged);
      case UNION:
        return processUnionNode(relNode, newInputs, rexBuilder, isInputChanged);
      case VALUES:
        return processValuesNode(relNode, newInputs, rexBuilder, isInputChanged);
      case HDFSSCAN:
        // HDFS Scan node will never need coercing.
        return relNode;
    }

    throw new RuntimeException("Unrecognized RelNode: " + relNode);
  }

  /**
   * processFilterNode: Checks and coerces any method in the condition.
   */
  private static RelNode processFilterNode(RelNode relNode, List<RelNode> inputs,
      RexBuilder rexBuilder, boolean isInputChanged) {
    final LogicalFilter filter = (LogicalFilter) relNode;

    RexNode condition = filter.getCondition();

    List<RexNode> changedRexNodes =
        processRexNodes(relNode, inputs, ImmutableList.of(condition));

    // If any RexNode was changed or any RelNode input was changed, we need
    // to create a new RelNode. Otherwise, just return existing RelNode.
    if (changedRexNodes == null && !isInputChanged) {
      return relNode;
    }

    RexNode newCondition = (changedRexNodes == null) ? condition : changedRexNodes.get(0);
    return filter.copy(filter.getTraitSet(), inputs.get(0), newCondition);
  }

  /**
   * processJoinNode: Checks and coerces any method in the condition.
   */
  private static RelNode processJoinNode(RelNode relNode, List<RelNode> inputs,
      RexBuilder rexBuilder, boolean isInputChanged) {
    final LogicalJoin join = (LogicalJoin) relNode;

    RexNode condition = join.getCondition();

    List<RexNode> changedRexNodes =
        processRexNodes(relNode, inputs, ImmutableList.of(condition));

    // If any RexNode was changed or any RelNode input was changed, we need
    // to create a new RelNode. Otherwise, just return existing RelNode.
    if (changedRexNodes == null && !isInputChanged) {
      return relNode;
    }

    RexNode newCondition = (changedRexNodes == null) ? condition : changedRexNodes.get(0);

    return join.copy(join.getTraitSet(), newCondition, inputs.get(0), inputs.get(1),
        join.getJoinType(), join.isSemiJoinDone());
  }

  /**
   * processProjectNode: Checks and coerces any method in the projects.
   */
  private static RelNode processProjectNode(RelNode relNode, List<RelNode> inputs,
      RexBuilder rexBuilder, boolean isInputChanged) {
    final LogicalProject project = (LogicalProject) relNode;

    List<RexNode> projects = project.getProjects();
    List<RexNode> changedRexNodes = processRexNodes(relNode, inputs, projects);

    // If any RexNode was changed or any RelNode input was changed, we need
    // to create a new RelNode. Otherwise, just return existing RelNode.
    if (changedRexNodes == null && !isInputChanged) {
      return relNode;
    }

    List<RexNode> newProjects = changedRexNodes == null ? projects : changedRexNodes;

    RelDataTypeFactory factory = rexBuilder.getTypeFactory();
    List<RelDataType> typeList = Util.transform(newProjects, RexNode::getType);
    RelDataType rowType =
        factory.createStructType(typeList, project.getRowType().getFieldNames());
    return project.copy(project.getTraitSet(), inputs.get(0), newProjects, rowType);
  }

  /**
   * processSortNode: recreates sort node if an input was changed.
   */
  private static RelNode processSortNode(RelNode relNode, List<RelNode> inputs,
      RexBuilder rexBuilder, boolean isInputChanged) {
    return isInputChanged ? relNode.copy(relNode.getTraitSet(), inputs) : relNode;
  }

  /**
   * processAggNode: Checks if there is an existing function signature for
   * the aggregation call. If a coercion is needed, we need to do some processing.
   * The Aggregate RelNode is not allowed to contain a casted expression.
   * Instead, a Project RelNode is placed as an input to the Aggregate RelNode. The
   * Project RelNode will have its first <n> fields map directly into the original input
   * node of the Aggregate. Additional 'cast' RexCalls will be added into the Project
   * and the AggregateCall will point into these input columns.
   */
  private static RelNode processAggNode(RelNode relNode, List<RelNode> inputs,
      RexBuilder rexBuilder, boolean isInputChanged) {
    final LogicalAggregate agg = (LogicalAggregate) relNode;

    int numInputFields = agg.getInput(0).getRowType().getFieldCount();

    // List of aggCalls that are either copied over from the original agg node
    // or transformed because of coercion.
    List<AggregateCall> transformedAggCallList = new ArrayList<>();

    // New fields and fieldnames that are needed because of coercion.
    List<RexNode> newProjectFields = new ArrayList<>();
    List<String> newProjectFieldNames = new ArrayList<>();

    for (AggregateCall aggCall : agg.getAggCallList()) {
      List<RelDataType> operandTypes = getOperandTypes(inputs.get(0), aggCall);
      // check to see if the current agg call matches an existing Impala
      // function without casting any operands
      if (matchesSignature(aggCall, operandTypes)) {
        // just use original aggCall
        transformedAggCallList.add(aggCall);
        continue;
      }

      // if here, need to transform
      List<RelDataType> newOperandTypes = getCastedOperandTypes(aggCall, operandTypes);

      // last parameter is the starting point for new project fields added
      // (see method comment for details).
      AggregateCall newAggCall = getNewAggCall(aggCall, operandTypes, newOperandTypes,
          numInputFields + newProjectFields.size());
      transformedAggCallList.add(newAggCall);

      // Add the newly cast fields into a soon to be generated underlying Project for
      // this AggCall
      newProjectFields.addAll(getNewProjectFields(
          agg.getCluster().getRexBuilder(), aggCall, operandTypes, newOperandTypes));
      newProjectFieldNames.addAll(getNewProjectFieldNames(
          agg.getInput(0), aggCall, operandTypes, newOperandTypes));
    }

    if (newProjectFields.isEmpty()) {
      return isInputChanged ? relNode.copy(relNode.getTraitSet(), inputs) : relNode;
    }

    RelNode project =
        createProject(inputs.get(0), newProjectFields, newProjectFieldNames);
    return LogicalAggregate.create(project, agg.getGroupSet(), agg.getGroupSets(),
        transformedAggCallList);
  }

  /**
   * processUnionNode: Checks the types of all columns in the inputs. They need
   * to match. If they do not match across all inputs, a Project node is created
   * to cast the column to a common type. The Union node is then recreated with
   * the new common type.
   */
  private static RelNode processUnionNode(RelNode relNode, List<RelNode> inputs,
      RexBuilder rexBuilder, boolean isInputChanged) {
    final LogicalUnion union = (LogicalUnion) relNode;

    // no work to be done if no input changed and there's only one input.
    if (!isInputChanged && inputs.size() == 1) {
      return relNode;
    }

    // Calculate the common row types for all the columns.
    List<RelDataType> commonRowType = getCompatibleRowType(relNode, inputs, rexBuilder);

    // Check to see if the union rowtype is different from the common row type calculated.
    // The boolean inputsChanged is used to determine if we need to recreate the Union
    // node, so if any column type is different, we set the value to true.
    boolean inputsChanged = isInputChanged ||
        haveTypesChanged(commonRowType, union.getRowType().getFieldList());

    List<RelNode> changedRelNodes = new ArrayList<>();
    for (RelNode input : inputs) {
      // getChangedInput returns the same RelNode if the RelNode has not changed.
      RelNode changedRelNode = getChangedUnionInput(input, commonRowType, rexBuilder);
      boolean inputChanged = !changedRelNode.equals(input);
      changedRelNodes.add(inputChanged ? changedRelNode : input);
      inputsChanged |= inputChanged;
    }

    return inputsChanged ? LogicalUnion.create(changedRelNodes, union.all) : relNode;
  }

  /**
   * processValuesNode: Coerces Value node literals (numerics and strings). For
   * strings, an intermediate Project node needs to be created (see comment below).
   */
  private static RelNode processValuesNode(RelNode relNode, List<RelNode> inputs,
      RexBuilder rexBuilder, boolean isInputChanged) {
    final LogicalValues values = (LogicalValues) relNode;
    if (values.getTuples().size() == 0) {
      return relNode;
    }

    int nColumns = values.getRowType().getFieldList().size();
    // initialize list to have null values for all columns
    List<RelDataType> relDataTypes = Arrays.asList(new RelDataType[nColumns]);

    boolean needProject = false;
    for (List<RexLiteral> tuple : values.getTuples()) {
      List<RexNode> rexNodes = castToRexNodeList(tuple);
      List<RexNode> changedRexNodes = processRexNodes(relNode, inputs, rexNodes);
      if (changedRexNodes == null) {
        continue;
      }
      needProject = true;
      Preconditions.checkState(changedRexNodes.size() == relDataTypes.size());
      for (int i = 0; i < changedRexNodes.size(); ++i) {
        if (changedRexNodes.get(i) != null) {
          Preconditions.checkState(changedRexNodes.get(i).getKind() == SqlKind.CAST ||
              changedRexNodes.get(i) instanceof RexLiteral);
        }
        // if changedRexNodes.get(i) is something other than null, the type needs to
        // be coerced. We want to take the tightest type we can. The current tightest
        // type is in the relDataTypes.get(i). On initialization, it is set to null,
        // so if this is the first row that has a coerced type for the ith column,
        // the tightest type will be the current changedRexNodes.get(i).getType() type.
        relDataTypes.set(i, getCompatibleDataType(
            relDataTypes.get(i), changedRexNodes.get(i).getType(), rexBuilder));
      }
    }

    if (!needProject) {
      return relNode;
    }

    // Need to create a project node on top of the values: A project node
    // does not add any overhead performance-wise since it doesn't create
    // a new node. However, it is needed here because Calcite creates string
    // literals as CHAR type and Impala requires a STRING type. The project
    // node creates this casting which will get removed when converting to the
    // Impala Expr object (where the RelNodes get converted to the physical layer).
    List<RexNode> projects = new ArrayList<>();

    for (int i = 0; i < relDataTypes.size(); ++i) {
      RexInputRef inputRef = rexBuilder.makeInputRef(values, i);

      RexNode project = (relDataTypes.get(i) != null)
          ? rexBuilder.makeCast(relDataTypes.get(i), inputRef)
          : inputRef;
      projects.add(project);
    }

    return LogicalProject.create(values, new ArrayList<>(), projects,
        values.getRowType().getFieldNames());
  }

  //////////////////////////////////////////////////////////
  // General method helpers
  //////////////////////////////////////////////////////////

  /**
   * processRexNodes is a shared method that coerces the list of RexNodes passed in.
   */
  private static List<RexNode> processRexNodes(RelNode relNode, List<RelNode> inputs,
      List<RexNode> rexNodes) {
    RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
    CoerceOperandShuttle shuttle = new CoerceOperandShuttle(
        relNode.getCluster().getTypeFactory(), rexBuilder, inputs);
    List<RexNode> changedRexNodes = new ArrayList<>();
    boolean rexNodeChanged = false;
    for (RexNode rexNode : rexNodes) {
      RexNode changedRexNode = shuttle.apply(rexNode);

      changedRexNode = RexUtil.pullFactors(rexBuilder, changedRexNode);
      // TODO: IMPALA-13436: use max_cnf_exprs query option instead of hardcoded 100.
      // The default for max_cnf_exprs is 200, but we use 100 here because tpcds
      // q41 is super slow when the value is at 200.
      changedRexNode = RexUtil.toCnf(rexBuilder, 100, changedRexNode);

      changedRexNodes.add(changedRexNode);
      rexNodeChanged |= (changedRexNode != rexNode);
    }
    return rexNodeChanged ? changedRexNodes : null;
  }

  /**
   * method to translate a RexLiteral list to a RexNode list
   */
  private static List<RexNode> castToRexNodeList(List<RexLiteral> literalList) {
    Class<RexNode> clazz = RexNode.class;
    return literalList.stream()
        .map(clazz::cast)
        .collect(Collectors.toList());
  }

  /**
   * compare 2 datatypes and return a datatype that is compatible to both of them.
   */
  private static RelDataType getCompatibleDataType(RelDataType dt1,
      RelDataType dt2, RexBuilder rexBuilder) {
     if (dt1 == null) {
       return dt2;
     }

     if (dt2 == null) {
       return dt1;
     }

    return ImpalaTypeConverter.getCompatibleType(
        ImmutableList.of(dt1, dt2), rexBuilder.getTypeFactory());
  }

  /**
   * getCompatibleDataType takes a list of inputs with multiple column types
   * and returns a list of types that are common (and castable) for all
   * the inputs.
   */
  private static List<RelDataType> getCompatibleRowType(RelNode origRelNode,
      List<RelNode> inputs, RexBuilder rexBuilder) {
    RelDataTypeFactory factory = rexBuilder.getTypeFactory();

    List<RelDataType> finalTypes = new ArrayList<>();
    for (RelDataTypeField field : inputs.get(0).getRowType().getFieldList()) {
      finalTypes.add(field.getType());
    }

    for (int j = 1; j < inputs.size(); ++j ) {
      RelNode input = inputs.get(j);
      for (int i = 0; i < input.getRowType().getFieldList().size(); ++i) {
        RelDataType type0 = finalTypes.get(i);
        RelDataType type1 = input.getRowType().getFieldList().get(i).getType();
        finalTypes.set(i,
            ImpalaTypeConverter.getCompatibleType(type0, type1, factory));
      }
    }
    return finalTypes;
  }

  /**
   * getChangedUnionInput takes a RelNode and a new "common" row type. If any column
   * type in the common row type is different from a column in the RelNode,
   * a Project node will be created that casts the offending columns into the
   * common type.
   */
  private static RelNode getChangedUnionInput(RelNode relNode,
      List<RelDataType> commonRowType, RexBuilder rexBuilder) {
    boolean changed = false;
    List<RexNode> projects = new ArrayList<>();
    for (int i = 0; i < relNode.getRowType().getFieldList().size(); ++i) {
      RexInputRef inputRef = rexBuilder.makeInputRef(relNode, i);
      RelDataType inputType = relNode.getRowType().getFieldList().get(i).getType();
      boolean projectChanged = !inputType.equals(commonRowType.get(i));
      RexNode project = projectChanged
          ? rexBuilder.makeCast(commonRowType.get(i), inputRef)
          : inputRef;
      projects.add(project);
      changed |= projectChanged;
    }

    if (!changed) {
      return relNode;
    }

    RelDataTypeFactory factory = rexBuilder.getTypeFactory();
    RelDataType rowType =
        factory.createStructType(commonRowType, relNode.getRowType().getFieldNames());
    return LogicalProject.create(relNode, new ArrayList<>(), projects, rowType);
  }

  /**
   * haveTypesChanged is a helper method to determine if any type in a
   * RelDataTypeField list is different from a type in a common row type.
   */
  private static boolean haveTypesChanged(List<RelDataType> commonRowType,
      List<RelDataTypeField> fields) {
    Preconditions.checkState(commonRowType.size() == fields.size());
    for (int i = 0; i < commonRowType.size(); ++i) {
      if (!commonRowType.get(i).equals(fields.get(i).getType())) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesSignature(AggregateCall aggCall,
      List<RelDataType> operandTypes) {
    // The single_value function is what Calcite uses for cardinality checks. Impala
    // creates the CardinalityCheckNode when this function is translated. So the
    // single_value function is not a normal agg function that exists as an Impala
    // function.
    // We just return true since a matched signature means that no more processing or
    // coercing needs to be done.
    if (aggCall.getAggregation().getName().toLowerCase().equals("single_value")) {
      return true;
    }

    // quick pass to check for nulls.  Since Impala does not contain a null as
    // an input type, return false because the signature will not match.
    for (RelDataType relDataType : operandTypes) {
      if (relDataType.getSqlTypeName() == SqlTypeName.NULL) {
        return false;
      }
    }

    // Look for a function match.  If found, no need to coerce
    Function fn = FunctionResolver.getExactFunction(aggCall.getAggregation().getName(),
        operandTypes);

    if (fn == null) {
      return false;
    }

    // One last check to make sure the return types match.
    RelDataType retType = ImpalaTypeConverter.getRelDataType(fn.getReturnType());
    return retType.getSqlTypeName().equals(aggCall.getType().getSqlTypeName());
  }

  /**
   * getOperandTypes for an AggCall.  Calcite only keeps the input ref number in the
   * AggCall, so to get the actual operand type, we need to go to the input RelNode.
   */
  private static List<RelDataType> getOperandTypes(RelNode input, AggregateCall aggCall) {
    List<RelDataType> operandTypes = new ArrayList<>();
    for (Integer i : aggCall.getArgList()) {
      // add the type of then ith column in the input
      operandTypes.add(input.getRowType().getFieldList().get(i).getType());
    }
    return operandTypes;
  }

  /**
   * getCastedOperandTypes takes an AggregateCall with a list of its
   * parameters types and returns a corresponding list of the types
   * in a matching Impala function signature.
   */
  private static List<RelDataType> getCastedOperandTypes(AggregateCall aggCall,
      List<RelDataType> operandTypes) {
    // Get the Impala function. Getting the "supertype" function will retrieve
    // the closest function where operands may be cast.
    Function fn = FunctionResolver.getSupertypeFunction(
        aggCall.getAggregation().getName(), operandTypes);
    Preconditions.checkNotNull(fn, "Could not find matching functions for " +
        aggCall.getAggregation().getName());
    RelDataType retType = ImpalaTypeConverter.getRelDataType(fn.getReturnType());

    // Not changing return type, they should be the same. The code will get more
    // complicated if this has to change.
    Preconditions.checkState(
        retType.getSqlTypeName().equals(aggCall.getType().getSqlTypeName()) ||
        aggCall.getType().getSqlTypeName().equals(SqlTypeName.NULL));

    List<RelDataType> newOperandTypes = new ArrayList<>();
    for (int i = 0; i < operandTypes.size(); ++i) {
      Type t = (i < fn.getArgs().length)
          ? fn.getArgs()[i]
          : fn.getArgs()[fn.getArgs().length - 1];
      newOperandTypes.add(ImpalaTypeConverter.getRelDataType(t));
    }
    return newOperandTypes;
  }

  /**
   * genNewAggCall generates a new Aggregate call based on new operand
   * types that are needed. The AggregateCall method does not contain
   * any type information nor does it contain a RexCall. It only contains
   * an input ref number from its input node. So the AggregateCall needs
   * to change in steps. If the AggregateCall requires a casted type, we
   * will need to create an underlying Project column which creates this
   * cast. We can't just change the column in case other AggregateCalls
   * depend on that input ref (it would make the logic more complex to
   * handle it that way).
   *
   * The numProjects passed in represents the already existing number of
   * project columns. Any newly created input will start with a reference
   * number from this value.
   */
  private static AggregateCall getNewAggCall(AggregateCall aggCall,
      List<RelDataType> operandTypes, List<RelDataType> newOperandTypes,
      int numProjects) {
    List<Integer> newArgList = new ArrayList<>();
    Preconditions.checkState(aggCall.getArgList().size() == operandTypes.size());
    Preconditions.checkState(operandTypes.size() == newOperandTypes.size());
    for (int i = 0; i < operandTypes.size(); ++i) {
      // If the new needed operand type is the same as the previous operand
      // type, we use the previous operand type which will be the same
      // index number in the underlying Project. If a new one is needed,
      // the underlying Project will be the next available index.
      boolean typesEqual = areSqlTypesEqual(operandTypes.get(i), newOperandTypes.get(i));
      int newArg = typesEqual
          ? aggCall.getArgList().get(i)
          : numProjects++;
      newArgList.add(newArg);
    }
    return aggCall.withArgList(newArgList);
  }

  /**
   * genNewProjectFields works in tandem with getNewAggCall, but this creates the
   * actual RexNodes used in the underlying Project. We iterate through both the
   * old types and the new types. If we see the corresponding types don't match,
   * for the ith value, we create a RexNode casting the ith input ref into the new
   * type.
   */
  private static List<RexNode> getNewProjectFields(RexBuilder rexBuilder,
      AggregateCall aggCall, List<RelDataType> operandTypes,
      List<RelDataType> newOperandTypes) {
    List<RexNode> newProjects = new ArrayList<>();
    for (int i = 0; i < operandTypes.size(); ++i) {
      if (!areSqlTypesEqual(operandTypes.get(i), newOperandTypes.get(i))) {
        RexInputRef inputRef = rexBuilder.makeInputRef(
            operandTypes.get(i), aggCall.getArgList().get(i));
        RexNode newProject = rexBuilder.makeCast(newOperandTypes.get(i), inputRef);
        newProjects.add(newProject);
      }
    }
    return newProjects;
  }

  /**
   * genNewProjectFieldNames works the same as getNewProjectFields, but the
   * names are generated for the new fields with a "cast_" prefix.
   */
  private static List<String> getNewProjectFieldNames(RelNode input,
      AggregateCall aggCall, List<RelDataType> operandTypes,
      List<RelDataType> newOperandTypes) {
    List<String> newNames = new ArrayList<>();
    for (int i = 0; i < operandTypes.size(); ++i) {
      if (!areSqlTypesEqual(operandTypes.get(i), newOperandTypes.get(i))) {
        String precastFieldName =
            input.getRowType().getFieldNames().get(aggCall.getArgList().get(i));
        newNames.add("cast_" + precastFieldName);
      }
    }
    return newNames;
  }

  private static boolean areSqlTypesEqual(RelDataType r1, RelDataType r2) {
    return r1.getSqlTypeName().equals(r2.getSqlTypeName());
  }

  /**
   * createProject creates the underlying Project node under the Aggregate
   * for when there are added casts. The first <n> fields match the original
   * input and we just need to create a matching RexInputRef aligning the columns.
   * After the first <n> input columns, we add the new casted columns to the
   * Project.
   */
  private static RelNode createProject(RelNode input, List<RexNode> newProjectFields,
      List<String> newFieldNames) {
    List<RexNode> projects = new ArrayList<>();
    RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    for (int i = 0; i < input.getRowType().getFieldCount(); ++i) {
      projects.add(rexBuilder.makeInputRef(
          input.getRowType().getFieldList().get(i).getType(), i));
    }
    List<String> fieldNames =
        new ArrayList<>(input.getRowType().getFieldNames());
    projects.addAll(newProjectFields);
    fieldNames.addAll(newFieldNames);

    return RelFactories.DEFAULT_PROJECT_FACTORY.createProject(input,
        new ArrayList<>(), projects, fieldNames, new HashSet<>());
  }
}
