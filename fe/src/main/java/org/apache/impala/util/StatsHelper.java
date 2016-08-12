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

package org.apache.impala.util;

/**
 *  This is a utility class to incrementally calculate average, variance
 *  and standard deviation. It's based on an algorithm devised by Knuth.
 *
 *  Please keep in mind, that there might be edge cases where the below algorithm
 *  might produce a loss of precision.
 *
 *  See below link for more detail:
 *
 *  http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Incremental_algorithm
 */
public class StatsHelper<T extends Number> {

  private long count_ = 0;

  // Current mean
  private double mean_ = 0.0d;

  // Sum of the square differences from the mean
  private double m2_ = 0.0d;

  public void addSample(T val) {
    ++count_;
    mean_ += (val.doubleValue() - mean_) / count_;
    m2_ += Math.pow(val.doubleValue() - mean_, 2);
  }

  public long count() { return count_; }

  public double mean() {
    return count_ > 0 ? mean_ : 0.0;
  }

  public double variance() {
    return count_ > 1 ? m2_ / (count_ - 1) : 0.0d;
  }

  public double stddev() {
    return Math.sqrt(variance());
  }
}
