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

package org.apache.impala.planner;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;

import org.apache.impala.service.BackendConfig;

import java.math.RoundingMode;
import java.util.List;
import java.util.function.Supplier;

/**
 * A base class that encapsulate processing cost which models a total cost or amount
 * of work shared across all instances of specific {@link PlanNode}, {@link DataSink}, or
 * {@link PlanFragment}.
 */
public abstract class ProcessingCost implements Cloneable {
  public static ProcessingCost invalid() { return new BaseProcessingCost(-1, 1, 0); }
  public static ProcessingCost zero() { return new BaseProcessingCost(0, 1, 0); }

  public static ProcessingCost maxCost(ProcessingCost a, ProcessingCost b) {
    return (a.getTotalCost() >= b.getTotalCost()) ? a : b;
  }

  public static ProcessingCost sumCost(ProcessingCost a, ProcessingCost b) {
    return new SumProcessingCost(a, b);
  }

  public static ProcessingCost scaleCost(ProcessingCost cost, long factor) {
    return new ScaledProcessingCost(cost, factor);
  }

  public static ProcessingCost broadcastCost(
      ProcessingCost cost, Supplier<Integer> numInstanceSupplier) {
    return new BroadcastProcessingCost(cost, numInstanceSupplier);
  }

  protected static void tryAdjustConsumerParallelism(int nodeStepCount,
      int minParallelism, int maxParallelism, ProcessingCost producer,
      ProcessingCost consumer) {
    Preconditions.checkState(consumer.getNumInstancesExpected() > 0);
    Preconditions.checkState(producer.getNumInstancesExpected() > 0);
    if (producer.getCostPerRowProduced() > 0
        && (consumer.canReducedBy(nodeStepCount, minParallelism, producer)
            || (consumer.canIncreaseBy(nodeStepCount, maxParallelism, producer)))) {
      // Adjust consumer's concurrency following producer's parallelism and their
      // produce-consume rate ratio.
      float consProdRatio = consumer.consumerProducerRatio(producer);
      int adjustedCount = (int) Math.ceil(consProdRatio
                              * producer.getNumInstancesExpected() / nodeStepCount)
          * nodeStepCount;
      final int finalCount =
          Math.max(minParallelism, Math.min(maxParallelism, adjustedCount));
      consumer.setNumInstanceExpected(() -> finalCount);
    } else if (maxParallelism < consumer.getNumInstancesExpected()) {
      consumer.setNumInstanceExpected(() -> maxParallelism);
    }
  }

  private static ProcessingCost computeValidBaseCost(
      long cardinality, float exprsCost, float materializationCost) {
    return new BaseProcessingCost(
        Math.max(0, cardinality), exprsCost, materializationCost);
  }

  public static ProcessingCost basicCost(
      String label, long cardinality, float exprsCost, float materializationCost) {
    ProcessingCost processingCost =
        computeValidBaseCost(cardinality, exprsCost, materializationCost);
    processingCost.setLabel(label);
    return processingCost;
  }

  public static ProcessingCost basicCost(
      String label, long cardinality, float exprsCost) {
    ProcessingCost processingCost = computeValidBaseCost(cardinality, exprsCost, 0);
    processingCost.setLabel(label);
    return processingCost;
  }

  /**
   * Create new ProcessingCost.
   * 'totalCost' must not be a negative, NaN, or infinite.
   */
  public static ProcessingCost basicCost(String label, double totalCost) {
    try {
      ProcessingCost processingCost = new BaseProcessingCost(totalCost);
      processingCost.setLabel(label);
      return processingCost;
    } catch (IllegalArgumentException ex) {
      // Rethrow with label mentioned in the exception message.
      throw new IllegalArgumentException(
          String.format("Invalid totalCost supplied for %s", label), ex);
    }
  }

  /**
   * Merge multiple ProcessingCost into a single new ProcessingCost.
   * <p>
   * The resulting ProcessingCost will have the total cost, number of rows produced,
   * and number of rows consumed as a sum of respective properties of all ProcessingCost
   * in the given list. Meanwhile, the number of instances expected is the maximum among
   * all ProcessingCost is the list.
   *
   * @param costs list of all ProcessingCost to merge.
   * @return A new combined ProcessingCost.
   */
  protected static ProcessingCost fullMergeCosts(List<ProcessingCost> costs) {
    Preconditions.checkNotNull(costs);
    Preconditions.checkArgument(!costs.isEmpty());

    ProcessingCost resultingCost = ProcessingCost.zero();
    long inputCardinality = 0;
    long outputCardinality = 0;
    int maxProducerParallelism = 1;
    for (ProcessingCost cost : costs) {
      resultingCost = ProcessingCost.sumCost(resultingCost, cost);
      inputCardinality += cost.getNumRowToConsume();
      outputCardinality += cost.getNumRowToProduce();
      maxProducerParallelism =
          Math.max(maxProducerParallelism, cost.getNumInstancesExpected());
    }
    resultingCost.setNumRowToConsume(inputCardinality);
    resultingCost.setNumRowToProduce(outputCardinality);
    final int finalProducerParallelism = maxProducerParallelism;
    resultingCost.setNumInstanceExpected(() -> finalProducerParallelism);
    return resultingCost;
  }

  protected Supplier<Integer> numInstanceSupplier_ = null;
  private long numRowToProduce_ = 0;
  private long numRowToConsume_ = 0;
  private String label_ = null;
  private boolean isSetNumRowToProduce_ = false;
  private boolean isSetNumRowToConsume_ = false;

  public abstract long getTotalCost();

  public abstract boolean isValid();

  public abstract ProcessingCost clone();

  public String getDetails() {
    StringBuilder output = new StringBuilder();
    output.append("cost-total=")
        .append(getTotalCost())
        .append(" max-instances=")
        .append(getNumInstanceMax());
    if (hasAdjustedInstanceCount()) {
      output.append(" adj-instances=").append(getNumInstancesExpected());
    }
    output.append(" cost/inst=")
        .append(getPerInstanceCost())
        .append(" #cons:#prod=")
        .append(numRowToConsume_)
        .append(":")
        .append(numRowToProduce_);
    if (isSetNumRowToConsume_ && isSetNumRowToProduce_) {
      output.append(" reduction=").append(getReduction());
    }
    if (isSetNumRowToConsume_) {
      output.append(" cost/cons=").append(getCostPerRowConsumed());
    }
    if (isSetNumRowToProduce_) {
      output.append(" cost/prod=").append(getCostPerRowProduced());
    }
    return output.toString();
  }

  public String debugString() {
    StringBuilder output = new StringBuilder();
    if (label_ != null) {
      output.append(label_);
      output.append("=");
    }
    output.append(this);
    return output.toString();
  }

  @Override
  public String toString() {
    return "{" + getDetails() + "}";
  }

  public String getExplainString(String detailPrefix, boolean fullExplain) {
    return detailPrefix + getDetails();
  }

  public void setNumInstanceExpected(Supplier<Integer> countSupplier) {
    Preconditions.checkArgument(
        countSupplier.get() > 0, "Number of instance must be greater than 0!");
    numInstanceSupplier_ = countSupplier;
  }

  public int getNumInstancesExpected() {
    return hasAdjustedInstanceCount() ? numInstanceSupplier_.get() : getNumInstanceMax();
  }

  private boolean hasAdjustedInstanceCount() {
    return numInstanceSupplier_ != null && numInstanceSupplier_.get() > 0;
  }

  protected int getNumInstanceMax() { return getNumInstanceMax(1); }

  protected int getNumInstanceMax(int numNodes) {
    long maxParallelism = LongMath.divide(getTotalCost(),
        BackendConfig.INSTANCE.getMinProcessingPerThread(), RoundingMode.CEILING);
    return roundUpNumNodeMultiple(maxParallelism, numNodes);
  }

  protected static int roundUpNumNodeMultiple(long parallelism, int numNodes) {
    // Round up to the nearest multiple of numNodes.
    // Little over-parallelize is better than under-parallelize.
    long maxParallelism =
        LongMath.divide(parallelism, numNodes, RoundingMode.CEILING) * numNodes;

    if (maxParallelism <= 0) {
      maxParallelism = 1;
    } else if (maxParallelism > Integer.MAX_VALUE) {
      // Floor Integer.MAX_VALUE to the nearest multiple of numNodes.
      maxParallelism = Integer.MAX_VALUE - (Integer.MAX_VALUE % numNodes);
    }
    return (int) maxParallelism;
  }

  /**
   * Set num rows to produce.
   *
   * @param numRowToProduce Number of rows to produce by plan node or data sink associated
   *     with this cost. Assume 0 rows if negative value is given.
   */
  public void setNumRowToProduce(long numRowToProduce) {
    numRowToProduce_ = Math.max(0, numRowToProduce);
    isSetNumRowToProduce_ = true;
  }

  /**
   * Set num rows to consume.
   *
   * @param numRowToConsume Number of rows to consume by plan node or data sink associated
   *     with this cost. Assume 0 rows if negative value is given.
   */
  protected void setNumRowToConsume(long numRowToConsume) {
    numRowToConsume_ = Math.max(0, numRowToConsume);
    isSetNumRowToConsume_ = true;
  }

  public void setLabel(String label) { label_ = label; }
  public long getNumRowToConsume() { return numRowToConsume_; }
  public long getNumRowToProduce() { return numRowToProduce_; }

  private int getPerInstanceCost() {
    Preconditions.checkState(getNumInstancesExpected() > 0);
    return (int) Math.ceil((float) getTotalCost() / getNumInstancesExpected());
  }

  private float getReduction() {
    return (float) numRowToConsume_ / Math.max(1, numRowToProduce_);
  }

  private float getCostPerRowProduced() {
    return (float) getTotalCost() / Math.max(1, numRowToProduce_);
  }

  private float getCostPerRowConsumed() {
    return (float) getTotalCost() / Math.max(1, numRowToConsume_);
  }

  private float instanceRatio(ProcessingCost other) {
    Preconditions.checkState(getNumInstancesExpected() > 0);
    return (float) getNumInstancesExpected() / other.getNumInstancesExpected();
  }

  private float consumerProducerRatio(ProcessingCost other) {
    return getCostPerRowConsumed() / Math.max(1, other.getCostPerRowProduced());
  }

  private boolean isAtLowestInstanceRatio(
      int nodeStepCount, int minParallelism, ProcessingCost other) {
    if (getNumInstancesExpected() - nodeStepCount < minParallelism) {
      return true;
    } else {
      float lowerRatio = (float) (getNumInstancesExpected() - nodeStepCount)
          / other.getNumInstancesExpected();
      return lowerRatio < consumerProducerRatio(other);
    }
  }

  private boolean isAtHighestInstanceRatio(
      int nodeStepCount, int maxInstance, ProcessingCost other) {
    if (getNumInstancesExpected() + nodeStepCount > maxInstance) {
      return true;
    } else {
      float higherRatio = (float) (getNumInstancesExpected() + nodeStepCount)
          / other.getNumInstancesExpected();
      return higherRatio > consumerProducerRatio(other);
    }
  }

  private boolean canReducedBy(
      int nodeStepCount, int minParallelism, ProcessingCost other) {
    return !isAtLowestInstanceRatio(nodeStepCount, minParallelism, other)
        && consumerProducerRatio(other) < instanceRatio(other);
  }

  private boolean canIncreaseBy(
      int nodeStepCount, int maxInstance, ProcessingCost other) {
    return !isAtHighestInstanceRatio(nodeStepCount, maxInstance, other)
        && consumerProducerRatio(other) > instanceRatio(other);
  }
}
