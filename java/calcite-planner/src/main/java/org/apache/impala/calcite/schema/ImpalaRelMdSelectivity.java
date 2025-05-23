/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.schema;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaRelMdSelectivity is an extension of the Calcite selectivity class.
 * The selectivity is baked into the row count because of partition pruning
 * processing. So we override selectivity to always be 1.0.
 */
public class ImpalaRelMdSelectivity
    implements MetadataHandler<BuiltInMetadata.Selectivity> {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaRelMdSelectivity.class.getName());

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new ImpalaRelMdSelectivity(), BuiltInMetadata.Selectivity.Handler.class);

  //~ Constructors -----------------------------------------------------------

  public ImpalaRelMdSelectivity() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.Selectivity> getDef() {
    return BuiltInMetadata.Selectivity.DEF;
  }

  public Double getSelectivity(Aggregate rel, RelMetadataQuery mq, RexNode predicate) {
    return 1.0;
  }

  public Double getSelectivity(Filter rel, RelMetadataQuery mq, RexNode predicate) {
    return 1.0;
  }

  public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
    return 1.0;
  }

  public Double getSelectivity(Project rel, RelMetadataQuery mq, RexNode predicate) {
    return 1.0;
  }

  public Double getSelectivity(Sort rel, RelMetadataQuery mq, RexNode predicate) {
    return 1.0;
  }

  public Double getSelectivity(Union rel, RelMetadataQuery mq, RexNode predicate) {
    return 1.0;
  }

  public Double getSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
    return 1.0;
  }
}
