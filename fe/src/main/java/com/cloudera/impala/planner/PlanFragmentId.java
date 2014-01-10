// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;

public class PlanFragmentId extends Id<PlanFragmentId> {
  // Construction only allowed via an IdGenerator.
  protected PlanFragmentId(int id) {
    super(id);
  }

  public static IdGenerator<PlanFragmentId> createGenerator() {
    return new IdGenerator<PlanFragmentId>() {
      @Override
      public PlanFragmentId getNextId() { return new PlanFragmentId(nextId_++); }
      @Override
      public PlanFragmentId getMaxId() { return new PlanFragmentId(nextId_ - 1); }
    };
  }

  @Override
  public String toString() {
    return String.format("F%02d", id_);
  }
}
