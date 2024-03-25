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

package org.apache.impala.calcite.functions;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FunctionResolver is a wrapper around the Impala Function Resolver (via the
 * (Db.getFunction() method). In this current iteration, only exact matches are
 * resolved. TODO: IMPALA-13022: change this comment when implicit conversion is handled.
 */
public class FunctionResolver {
  protected static final Logger LOG =
      LoggerFactory.getLogger(FunctionResolver.class.getName());

  // Map of the Calcite Kind to an Impala function name
  public static Map<SqlKind, String> CALCITE_KIND_TO_IMPALA_FUNC =
      ImmutableMap.<SqlKind, String> builder()
      .put(SqlKind.EQUALS, "eq")
      .build();

  public static Function getFunction(String name, SqlKind kind,
      List<RelDataType> argTypes) {
    String mappedName = CALCITE_KIND_TO_IMPALA_FUNC.get(kind);
    return mappedName == null
        ? getFunction(name, argTypes)
        : getFunction(mappedName, argTypes);
  }

  public static Function getFunction(String name, List<RelDataType> argTypes) {
    String lowercaseName = name.toLowerCase();

    List<Type> impalaArgTypes = ImpalaTypeConverter.getNormalizedImpalaTypes(argTypes);
    Function searchDesc = new Function(new FunctionName(BuiltinsDb.NAME, lowercaseName),
        impalaArgTypes, Type.INVALID, false);

    Function fn = BuiltinsDb.getInstance().getFunction(searchDesc,
        Function.CompareMode.IS_INDISTINGUISHABLE);

    if (fn == null) {
      LOG.debug("Failed to find function " + lowercaseName);
    }

    return fn;
  }
}
