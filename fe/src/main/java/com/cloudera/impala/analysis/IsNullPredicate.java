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

import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TIsNullPredicate;
import com.google.common.base.Preconditions;

public class IsNullPredicate extends Predicate {
  private final boolean isNotNull;

  public IsNullPredicate(Expr e, boolean isNotNull) {
    super();
    this.isNotNull = isNotNull;
    Preconditions.checkNotNull(e);
    children.add(e);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((IsNullPredicate) obj).isNotNull == isNotNull;
  }

  @Override
  public String toSql() {
    return getChild(0).toSql() + (isNotNull ? " IS NOT NULL" : " IS NULL");
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.IS_NULL_PRED;
    msg.is_null_pred = new TIsNullPredicate(isNotNull);
  }

}
