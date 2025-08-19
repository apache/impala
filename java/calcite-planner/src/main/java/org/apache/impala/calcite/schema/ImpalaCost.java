/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.schema;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * <code>ImpalaCost</code> represents the cost of a plan node.
 *
 * <p>This class is immutable: none of the methods modify any member
 * variables.
 */
class ImpalaCost implements RelOptCost {
  //~ Static fields/initializers ---------------------------------------------

  static final ImpalaCost INFINITY =
      new ImpalaCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        @Override public String toString() {
          return "{inf}";
        }
      };

  static final ImpalaCost HUGE =
      new ImpalaCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        @Override public String toString() {
          return "{huge}";
        }
      };

  static final ImpalaCost ZERO =
      new ImpalaCost(0.0, 0.0, 0.0) {
        @Override public String toString() {
          return "{0}";
        }
      };

  static final ImpalaCost TINY =
      new ImpalaCost(1.0, 1.0, 0.0) {
        @Override public String toString() {
          return "{tiny}";
        }
      };

  public static final RelOptCostFactory FACTORY = new Factory();

  //~ Instance fields --------------------------------------------------------

  final double cpu;
  final double io;
  final double rowCount;

  //~ Constructors -----------------------------------------------------------

  ImpalaCost(double rowCount, double cpu, double io) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public double getCpu() {
    return cpu;
  }

  @Override public boolean isInfinite() {
    return (this == INFINITY)
        || (this.rowCount == Double.POSITIVE_INFINITY)
        || (this.cpu == Double.POSITIVE_INFINITY)
        || (this.io == Double.POSITIVE_INFINITY);
  }

  @Override public double getIo() {
    return io;
  }

  @Override public boolean isLe(RelOptCost other) {
    ImpalaCost that = (ImpalaCost) other;
    if (true) {
      return this == that
          || this.rowCount <= that.rowCount;
    }
    return (this == that)
        || ((this.rowCount <= that.rowCount)
        && (this.cpu <= that.cpu)
        && (this.io <= that.io));
  }

  @Override public boolean isLt(RelOptCost other) {
    if (true) {
      ImpalaCost that = (ImpalaCost) other;
      return this.rowCount < that.rowCount;
    }
    return isLe(other) && !equals(other);
  }

  @Override public double getRows() {
    return rowCount;
  }

  @Override public int hashCode() {
    return Objects.hash(rowCount, cpu, io);
  }

  @SuppressWarnings("NonOverridingEquals")
  @Override public boolean equals(RelOptCost other) {
    return this == other
        || other instanceof ImpalaCost
        && (this.rowCount == ((ImpalaCost) other).rowCount)
        && (this.cpu == ((ImpalaCost) other).cpu)
        && (this.io == ((ImpalaCost) other).io);
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (obj instanceof ImpalaCost) {
      return equals((ImpalaCost) obj);
    }
    return false;
  }

  @Override public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof ImpalaCost)) {
      return false;
    }
    ImpalaCost that = (ImpalaCost) other;
    return (this == that)
        || ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
        && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
        && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON));
  }

  @Override public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    ImpalaCost that = (ImpalaCost) other;
    return new ImpalaCost(
        this.rowCount - that.rowCount,
        this.cpu - that.cpu,
        this.io - that.io);
  }

  @Override public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new ImpalaCost(rowCount * factor, cpu * factor, io * factor);
  }

  @Override public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    ImpalaCost that = (ImpalaCost) cost;
    double d = 1;
    double n = 0;
    if ((this.rowCount != 0)
        && !Double.isInfinite(this.rowCount)
        && (that.rowCount != 0)
        && !Double.isInfinite(that.rowCount)) {
      d *= this.rowCount / that.rowCount;
      ++n;
    }
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.io != 0)
        && !Double.isInfinite(this.io)
        && (that.io != 0)
        && !Double.isInfinite(that.io)) {
      d *= this.io / that.io;
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  @Override public RelOptCost plus(RelOptCost other) {
    ImpalaCost that = (ImpalaCost) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new ImpalaCost(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io);
  }

  @Override public String toString() {
    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
  }

  /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
   * that creates {@link org.apache.calcite.plan.volcano.ImpalaCost}s. */
  private static class Factory implements RelOptCostFactory {
    @Override public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
      return new ImpalaCost(dRows, dCpu, dIo);
    }

    @Override public RelOptCost makeHugeCost() {
      return ImpalaCost.HUGE;
    }

    @Override public RelOptCost makeInfiniteCost() {
      return ImpalaCost.INFINITY;
    }

    @Override public RelOptCost makeTinyCost() {
      return ImpalaCost.TINY;
    }

    @Override public RelOptCost makeZeroCost() {
      return ImpalaCost.ZERO;
    }
  }
}
