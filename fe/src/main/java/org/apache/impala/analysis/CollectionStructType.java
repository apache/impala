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

import java.util.List;

import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Generated struct type describing the fields of a collection type
 * that can be referenced in paths.
 *
 * Parent Type      CollectionStructType
 * array<i>    -->  struct<item:i,pos:bigint>
 * map<k,v>    -->  struct<key:k,value:v>
 */
public class CollectionStructType extends StructType {
  // True if this struct describes the fields of a map,
  // false if it describes the fields of an array.
  private final boolean isMapStruct_;

  // Field that can be skipped by implicit paths if its type is a struct.
  private final StructField optionalField_;

  private CollectionStructType(List<StructField> fields, boolean isMapStruct) {
    super(fields);
    isMapStruct_ = isMapStruct;
    if (isMapStruct_) {
      optionalField_ = getField(Path.MAP_VALUE_FIELD_NAME);
    } else {
      optionalField_ = getField(Path.ARRAY_ITEM_FIELD_NAME);
    }
    Preconditions.checkNotNull(optionalField_);
  }

  public static CollectionStructType createArrayStructType(ArrayType arrayType) {
    Type itemType = arrayType.getItemType();
    List<StructField> fields = Lists.newArrayListWithCapacity(2);
    // The item field name comes before the pos field name so that a path to the
    // stored item corresponds to its physical path.
    fields.add(new StructField(Path.ARRAY_ITEM_FIELD_NAME, itemType));
    fields.add(new StructField(Path.ARRAY_POS_FIELD_NAME, ScalarType.BIGINT));
    return new CollectionStructType(fields, false);
  }

  public static CollectionStructType createMapStructType(MapType mapType) {
    List<StructField> mapFields = Lists.newArrayListWithCapacity(2);
    mapFields.add(new StructField(Path.MAP_KEY_FIELD_NAME, mapType.getKeyType()));
    mapFields.add(new StructField(Path.MAP_VALUE_FIELD_NAME, mapType.getValueType()));
    return new CollectionStructType(mapFields, true);
  }

  public StructField getOptionalField() { return optionalField_; }
  public boolean isMapStruct() { return isMapStruct_; }
  public boolean isArrayStruct() { return !isMapStruct_; }
}
