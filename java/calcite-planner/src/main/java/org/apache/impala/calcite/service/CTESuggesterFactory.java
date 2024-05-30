/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.service;

import org.apache.calcite.rel.RelCommonExpressionSuggester;
import org.apache.hadoop.conf.Configuration;
import org.apache.impala.common.AnalysisException;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

public final class CTESuggesterFactory {

  public static final String CTE_SUGGESTER_CLASS = "impala.cte_suggester_class";
  public static final String CTE_THRESHOLD = "impala.cte_threshold";

  private CTESuggesterFactory() {
    throw new IllegalStateException("Must not instantiate");
  }

  public static RelCommonExpressionSuggester create(Configuration configuration)
      throws AnalysisException {
    String name = configuration.get(CTE_SUGGESTER_CLASS);
    if (name == null || name.isEmpty()) {
      return (query, conf) -> Collections.emptyList();
    }
    try {
      Class<?> suggesterClass = Class.forName(name);
      if (RelCommonExpressionSuggester.class.isAssignableFrom(suggesterClass)) {
        return (RelCommonExpressionSuggester)
            suggesterClass.getDeclaredConstructor().newInstance();
      }
      throw new AnalysisException(suggesterClass.getSimpleName() +
          " must implement " + RelCommonExpressionSuggester.class.getSimpleName());
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
        NoSuchMethodException | InvocationTargetException e) {
      throw new AnalysisException(
          "Failed to instantiate suggester from class: " + name, e);
    }
  }
}
