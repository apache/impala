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
package org.apache.impala.calcite.rules;

import org.apache.calcite.plan.Context;

import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * ImpalaMQContext is a context class that can be used inside a Calcite Planner
 * object. The object is placed at a query level and allows information to be
 * passed while traversing the RelMetadataQuery framework.
 */
public class ImpalaMQContext implements Context {
  // Input columns from a parent RelNode passed through the RelMetadataQuery framework.
  private ImmutableBitSet inputRefs_ = ImmutableBitSet.of();

  @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
    return clazz.isInstance(this) ? clazz.cast(this) : null;
  }

  public void setInputRefs(ImmutableBitSet bitSet) {
    inputRefs_ = bitSet;
  }

  public ImmutableBitSet getInputRefs() {
    return inputRefs_;
  }

  public void clear() {
    inputRefs_ = ImmutableBitSet.of();
  }
}
