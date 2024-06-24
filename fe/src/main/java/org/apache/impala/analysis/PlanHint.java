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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Joiner;

/**
 * Class to parse and store query plan hints, which can occur in various places inside SQL
 * query statements. A hint consist of a name and an optional list of arguments.
 */
public class PlanHint {
  /// The plan hint name.
  private final String name_;

  /// Optional list of arguments.
  private final List<String> args_;

  public PlanHint(String name) {
    name_ = name;
    args_ = new ArrayList<>();
  }

  public PlanHint(String name, List<String> args) {
    name_ = name;
    args_ = args;
  }

  /// Check whether this hint equals to a given string, ignoring case.
  public boolean is(String s) { return name_.equalsIgnoreCase(s); }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (getClass() != o.getClass()) return false;
    PlanHint oh = (PlanHint) o;
    return name_.equals(oh.name_) && args_.equals(oh.args_);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name_, args_);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name_);
    if (!args_.isEmpty()) {
      sb.append("(");
      sb.append(Joiner.on(",").join(args_));
      sb.append(")");
    }
    return sb.toString();
  }

  public List<String> getArgs() { return args_; }
  public String toSql() { return toString(); }
}
