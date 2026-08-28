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

import com.google.common.collect.Multimap;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.impala.calcite.rel.node.ImpalaCTEConsumer;

public class ImpalaRelMdNodeTypes implements MetadataHandler<BuiltInMetadata.NodeTypes> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new ImpalaRelMdNodeTypes(), BuiltInMetadata.NodeTypes.Handler.class);

  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(
      ImpalaCTEConsumer r, RelMetadataQuery mq) {
    // The fact that we delegate node types to the underlying CTE is necessary
    // for MV rules to trigger properly. An undesired side effect is that
    // actual Consumer node and its type never appear in the result.
    // The node types for ImpalaCTEConsumer is primarily needed for MV rewrite
    // so the side effect is acceptable at the moment.
    return mq.getNodeTypes(r.getCTE());
  }

  @Override
  public MetadataDef<BuiltInMetadata.NodeTypes> getDef() {
    return BuiltInMetadata.NodeTypes.DEF;
  }
}
