// Copyright 2015 Cloudera Inc.
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

public class RuntimeFilterId extends Id<RuntimeFilterId> {
  // Construction only allowed via an IdGenerator.
  protected RuntimeFilterId(int id) {
    super(id);
  }

  public static IdGenerator<RuntimeFilterId> createGenerator() {
    return new IdGenerator<RuntimeFilterId>() {
      @Override
      public RuntimeFilterId getNextId() { return new RuntimeFilterId(nextId_++); }
      @Override
      public RuntimeFilterId getMaxId() { return new RuntimeFilterId(nextId_ - 1); }
    };
  }

  @Override
  public String toString() {
    return String.format("RF%03d", id_);
  }

  @Override
  public int hashCode() { return id_; }
}
