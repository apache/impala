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

package org.apache.impala.analysis;

import org.apache.impala.common.Id;
import org.apache.impala.common.IdGenerator;

public class EquivalenceClassId extends Id<EquivalenceClassId> {
  // Construction only allowed via an IdGenerator.
  protected EquivalenceClassId(int id) {
    super(id);
  }

  public static IdGenerator<EquivalenceClassId> createGenerator() {
    return new IdGenerator<EquivalenceClassId>() {
      @Override
      public EquivalenceClassId getNextId() { return new EquivalenceClassId(nextId_++); }
      @Override
      public EquivalenceClassId getMaxId() { return new EquivalenceClassId(nextId_ - 1); }
    };
  }
}
