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

package org.apache.impala.common;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base Unit test class that manages the Frontend fixture
 * to initialize and shut down the front-end, and to remove any
 * temporary tables created by the test. Derive tests from this class
 * if the test does anything "special." Derive from
 * {@link FrontendTestBase} for routine tests that can leverage the
 * many default functions available.
 *
 * A special test is one that:
 *
 * * Needs specialized query options.
 * * Needs specialized query handling, such as inspecting bits of the
 *   AST, decorated AST or query plan.
 *
 * In these cases, use the fixtures directly as they provide more control
 * than do the generic methods in FrontendTestBase.
 */
public abstract class AbstractFrontendTest {
  protected static FrontendFixture feFixture_ = FrontendFixture.instance();

  @BeforeClass
  public static void setUp() throws Exception {
    feFixture_.setUp();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    feFixture_.cleanUp();
  }

  @After
  public void tearDown() {
    feFixture_.tearDown();
  }
}
