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

package org.apache.impala.catalog;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.lang.*;

import org.apache.impala.analysis.*;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.apache.impala.catalog.HdfsPartition.comparePartitionKeyValues;

public class HdfsPartitionTest {

  private List<LiteralExpr> valuesNull_= Lists.newArrayList();
  private List<LiteralExpr> valuesDecimal_ = Lists.newArrayList();
  private List<LiteralExpr> valuesDecimal1_ = Lists.newArrayList();
  private List<LiteralExpr> valuesDecimal2_ = Lists.newArrayList();
  private List<LiteralExpr> valuesMixed_= Lists.newArrayList();
  private List<LiteralExpr> valuesMixed1_ = Lists.newArrayList();
  private List<LiteralExpr> valuesMixed2_ = Lists.newArrayList();

  public HdfsPartitionTest() {
    valuesNull_.add(NullLiteral.create(Type.BIGINT));

    valuesDecimal_.add(new NumericLiteral(BigDecimal.valueOf(1)));
    valuesDecimal1_.add(new NumericLiteral(BigDecimal.valueOf(3)));
    valuesDecimal2_.add(new NumericLiteral(BigDecimal.valueOf(5)));

    valuesMixed_.add(new NumericLiteral(BigDecimal.valueOf(3)));
    valuesMixed_.add(NullLiteral.create(Type.BIGINT));

    valuesMixed1_.add(new NumericLiteral(BigDecimal.valueOf(1)));
    valuesMixed1_.add(NullLiteral.create(Type.STRING));
    valuesMixed1_.add(new BoolLiteral(true));

    valuesMixed2_.add(new NumericLiteral(BigDecimal.valueOf(1)));
    valuesMixed2_.add(new StringLiteral("Large"));
    valuesMixed2_.add(new BoolLiteral(false));
  }

  @Test
  public void testCompare() {
    List<List<LiteralExpr>> allLists = Lists.newArrayList();
    allLists.add(valuesNull_);
    allLists.add(valuesDecimal_);
    allLists.add(valuesDecimal1_);
    allLists.add(valuesDecimal2_);
    allLists.add(valuesMixed_);
    allLists.add(valuesMixed1_);
    allLists.add(valuesMixed2_);

    for (List<LiteralExpr> l1: allLists) {
      verifyReflexive(l1);
      for (List<LiteralExpr> l2: allLists) {
        verifySymmetric(l1, l2);
        for (List<LiteralExpr> l3: allLists) {
          verifyTransitive(l1, l2, l3);
        }
      }
    }

    List<LiteralExpr> valuesTest = Lists.newArrayList();
    valuesTest.add(new NumericLiteral(BigDecimal.valueOf(3)));
    verifyAntiSymmetric(valuesDecimal1_, valuesTest, valuesNull_);
    valuesTest.add(NullLiteral.create(Type.BIGINT));
    verifyAntiSymmetric(valuesMixed_, valuesTest, valuesDecimal_);
  }

  private void verifySymmetric(List<LiteralExpr> o1, List<LiteralExpr> o2) {
    // sgn(compare(x, y)) == -sgn(compare(y, x)),
    // compare(x, y) == 0, then compare(y, x) == 0
    assertTrue(Integer.signum(comparePartitionKeyValues(o1, o2)) ==
        -Integer.signum(comparePartitionKeyValues(o2, o1)));
  }
  private void verifyTransitive(List<LiteralExpr> o1, List<LiteralExpr> o2,
                                List<LiteralExpr> o3) {
    // ((compare(x, y)>0) && (compare(y, z)>0)) implies compare(x, z)>0
    if ((comparePartitionKeyValues(o1, o2) > 0) &&
        (comparePartitionKeyValues(o2, o3) > 0)) {
      assertTrue(comparePartitionKeyValues(o1, o3) > 0);
    }
  }
  private void verifyReflexive(List<LiteralExpr> o1) {
    // (compare(x, x)==0) is always true
    assertTrue(comparePartitionKeyValues(o1, o1) == 0);
  }
  private void verifyAntiSymmetric(List<LiteralExpr> o1, List<LiteralExpr> o2,
                                   List<LiteralExpr> o3) {
    // compare(x, y)==0 implies that sgn(compare(x, z))==sgn(compare(y, z)) for all z.
    if (comparePartitionKeyValues(o1, o2) == 0) {
      assertTrue(Integer.signum(comparePartitionKeyValues(o1, o3)) ==
          Integer.signum(comparePartitionKeyValues(o2, o3)));
    }
  }
}
