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
package org.apache.impala.authorization.sentry;

import com.google.common.collect.Lists;
import org.apache.impala.authorization.sentry.ImpalaAction;
import org.apache.impala.authorization.sentry.ImpalaActionFactory;
import org.apache.sentry.core.common.BitFieldAction;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class ImpalaActionFactoryTest {
  @Test
  public void testGetActionsByCode() {
    ImpalaActionFactory factory = new ImpalaActionFactory();

    List<? extends BitFieldAction> actual = factory.getActionsByCode(
        ImpalaAction.SELECT.getCode() |
        ImpalaAction.INSERT.getCode() |
        ImpalaAction.CREATE.getCode());
    List<ImpalaAction> expected = Lists.newArrayList(
        ImpalaAction.SELECT,
        ImpalaAction.INSERT,
        ImpalaAction.CREATE);
    assertBitFieldActions(expected, actual);

    actual = factory.getActionsByCode(
        ImpalaAction.SELECT.getCode() |
        ImpalaAction.INSERT.getCode() |
        ImpalaAction.ALTER.getCode() |
        ImpalaAction.CREATE.getCode() |
        ImpalaAction.DROP.getCode() |
        ImpalaAction.REFRESH.getCode());
    expected = Lists.newArrayList(
         ImpalaAction.SELECT,
         ImpalaAction.INSERT,
         ImpalaAction.ALTER,
         ImpalaAction.CREATE,
         ImpalaAction.DROP,
         ImpalaAction.REFRESH,
         ImpalaAction.ALL,
         ImpalaAction.OWNER);
     assertBitFieldActions(expected, actual);

    actual = factory.getActionsByCode(ImpalaAction.ALL.getCode());
    expected = Lists.newArrayList(
        ImpalaAction.SELECT,
        ImpalaAction.INSERT,
        ImpalaAction.ALTER,
        ImpalaAction.CREATE,
        ImpalaAction.DROP,
        ImpalaAction.REFRESH,
        ImpalaAction.ALL,
        ImpalaAction.OWNER);
    assertBitFieldActions(expected, actual);

    try {
      factory.getActionsByCode(Integer.MAX_VALUE);
      fail("IllegalArgumentException should be thrown.");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format("Action code must between 1 and %d.",
          ImpalaAction.ALL.getCode()), e.getMessage());
    }

    try {
      factory.getActionsByCode(Integer.MIN_VALUE);
      fail("IllegalArgumentException should be thrown.");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format("Action code must between 1 and %d.",
          ImpalaAction.ALL.getCode()), e.getMessage());
    }
  }

  private static void assertBitFieldActions(List<ImpalaAction> expected,
      List<? extends BitFieldAction> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < actual.size(); i++) {
      assertEquals(expected.get(i).getValue(), actual.get(i).getValue());
      assertEquals(expected.get(i).getCode(), actual.get(i).getActionCode());
    }
  }

  @Test
  public void testGetActionByName() {
    ImpalaActionFactory impala = new ImpalaActionFactory();

    for (ImpalaAction action : ImpalaAction.values()) {
      testGetActionByName(impala, action, action.getValue());
    }
    assertNull(impala.getActionByName("foo"));
  }

  private static void testGetActionByName(ImpalaActionFactory impala,
      ImpalaAction expected, String name) {
    assertEquals(toBitFieldAction(expected),
        impala.getActionByName(name.toUpperCase()));
    assertEquals(toBitFieldAction(expected),
        impala.getActionByName(name.toLowerCase()));
    assertEquals(toBitFieldAction(expected),
        impala.getActionByName(randomizeCaseSensitivity(name)));
  }

  private static String randomizeCaseSensitivity(String str) {
    char[] chars = str.toCharArray();
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < chars.length; i++) {
      chars[i] = (random.nextBoolean()) ? Character.toUpperCase(chars[i]) : chars[i];
    }
    return new String(chars);
  }

  private static BitFieldAction toBitFieldAction(ImpalaAction action) {
    return new BitFieldAction(action.getValue(), action.getCode());
  }
}