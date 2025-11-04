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

package org.apache.impala.calcite.operators;

/**
 * Special "In" operator for Impala.
 *
 * Calcite treats all IN operators as subqueries. Impala needs its own
 * distinct operator to deal with the IN function;
 */
public class ImpalaInOperator extends ImpalaOperator {

  public static final ImpalaInOperator OP = new ImpalaInOperator();

  private ImpalaInOperator() {
    // Using "in_set_lookup" for the function name. Impala needs a valid
    // function here for the coerce nodes module to make sure all datatypes
    // are correct. At physical conversion time, the actual function used
    // may be changed to in_iterate based on certain conditions.
    super("in_set_lookup");
  }
}
