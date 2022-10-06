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

package org.apache.impala.service;

import static org.apache.impala.service.JniCatalogOp.execOp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.thrift.TException;
import org.junit.Test;

public class JniCatalogOpTest extends FrontendTestBase {
  public static final String RESULT_STRING = "result";

  @Test
  public void testExecOpNullParameter() throws TException, ImpalaException {
    String result;
    result = execOp(
        "methodName", "shortDescription", () -> new Pair<>(RESULT_STRING, 1L), null);

    assertEquals(RESULT_STRING, result);
  }

  @Test
  public void testExecOp() throws TException, ImpalaException {
    String result;
    result = execOp("methodName", "shortDescription",
        ()
            -> new Pair<>(RESULT_STRING, 1L),
        new TCatalogObject(TCatalogObjectType.TABLE, 0L));
    assertEquals(RESULT_STRING, result);
  }

  @Test
  public void testExecOpNullPairAndParameter() throws TException, ImpalaException {
    String result;
    result = execOp("methodName", "shortDescription", () -> new Pair<>(null, null), null);
    assertNull(result);
  }

  @Test
  public void testExecOpNullStrings() throws TException, ImpalaException {
    String result;
    result = execOp(null, null, () -> new Pair<>(RESULT_STRING, 1L), null);
    assertEquals(RESULT_STRING, result);
  }
}