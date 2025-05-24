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

import java.util.Objects;

/**
 * ImpalaCost: Cost model for Impala. A good chunk of this is copied from VolcanoCost,
 * but the cost is calculated only using cpu and io.
 */
public class ImpalaCost implements RelOptCost {
  //~ Static fields/initializers ---------------------------------------------

  static final ImpalaCost INFINITY =
      new ImpalaCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        @Override public String toString() {
          return "{inf}";
        }
      };

  static final ImpalaCost HUGE =
      new ImpalaCost(Double.MAX_VALUE, Double.MAX_VALUE) {
        @Override public String toString() {
          return "{huge}";
        }
      };

  static final ImpalaCost ZERO =
      new ImpalaCost(0.0, 0.0) {
        @Override public String toString() {
          return "{0}";
        }
      };

  static final ImpalaCost TINY =
      new ImpalaCost(1.0, 0.0) {
        @Override public String toString() {
          return "{tiny}";
        }
      };

  public static final RelOptCostFactory FACTORY = new Factory();

  //~ Instance fields --------------------------------------------------------

  final double cpu;
  final double io;

  //~ Constructors -----------------------------------------------------------

  ImpalaCost(double cpu, double io) {
    this.cpu = cpu;
    this.io = io;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public double getCpu() {
    return cpu;
  }

  @Override public boolean isInfinite() {
    return (this == INFINITY)
        || (this.cpu == Double.POSITIVE_INFINITY)
        || (this.io == Double.POSITIVE_INFINITY);
  }

  @Override public double getIo() {
    return io;
  }

  @Override public boolean isLe(RelOptCost other) {
    return (this.cpu + this.io < other.getCpu() + other.getIo()) ||
          ((this.cpu + this.io == other.getCpu() + other.getIo()));
  }

  @Override public boolean isLt(RelOptCost other) {
    return isLe(other) && !equals(other);
  }

  @Override public double getRows() {
    return 1;
  }

  @Override public int hashCode() {
    return Objects.hash(cpu, io);
  }

  @SuppressWarnings("NonOverridingEquals")
  @Override public boolean equals(RelOptCost other) {
    return (this == other) ||
            ((this.cpu + this.io == other.getCpu() + other.getIo()));
  }

  @Override public boolean equals(Object obj) {
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
    // changed the epsilon value to be higher.  Without this, q17
    // was producing a bad plan.  It should now pick
    // ((store_sales, store_returns), catalog_sales)
    // over
    // ((catalog_sales, store_returns), store_sales)
    // The problem was that store_returns does not have a PK/FK
    // relationship with either table. It is a subset.  But the
    // cardinality calculation doesn't understand this, so the
    // number of join rows calculated is equal for both. The
    // store_sales join produces waaaaay more rows.
    // The join optimizer has a secondary way of determining join
    // ordering if the cost is a tie, so if the cost is
    // essentially the same, we'd prefer to use the tiebreaking method
    // rather than this method.
    return (this == other)
      || Math.abs(1.0 - (this.cpu + this.io) / (other.getCpu() + other.getIo())) < .01;
  }

  @Override public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    ImpalaCost that = (ImpalaCost) other;
    return new ImpalaCost(
        this.cpu - that.cpu,
        this.io - that.io);
  }

  @Override public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new ImpalaCost(cpu * factor, io * factor);
  }

  @Override public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    ImpalaCost that = (ImpalaCost) cost;
    double d = 1;
    double n = 0;
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
        this.cpu + that.cpu,
        this.io + that.io);
  }

  @Override public String toString() {
    return "{" + cpu + " cpu, " + io + " io}";
  }

  /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
   * that creates {@link org.apache.calcite.plan.volcano.ImpalaCost}s. */
  private static class Factory implements RelOptCostFactory {
    @Override public RelOptCost makeCost(double rowCount, double dCpu, double dIo) {
      return new ImpalaCost(dCpu, dIo);
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
