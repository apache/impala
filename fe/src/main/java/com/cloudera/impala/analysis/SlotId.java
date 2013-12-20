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

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;

public class SlotId extends Id<SlotId> {
  // Construction only allowed via an IdGenerator.
  protected SlotId(int id) {
    super(id);
  }

  public static IdGenerator<SlotId> createGenerator() {
    return new IdGenerator<SlotId>() {
      @Override
      public SlotId getNextId() { return new SlotId(nextId_++); }
      @Override
      public SlotId getMaxId() { return new SlotId(nextId_ - 1); }
    };
  }
}
