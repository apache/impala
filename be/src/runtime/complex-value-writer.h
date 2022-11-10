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

#pragma once

#include <type_traits>

#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "runtime/row-batch.h"
#include "runtime/tuple.h"
#include "udf/udf-internal.h"

namespace impala {

// Class with static methods that write complex types (collections and structs) in JSON
// format.
template <class JsonStream>
class ComplexValueWriter {
 public:
  // Gets a non-null CollectionValue and writes it in JSON format using 'writer'.
  // 'collection_type' should be either TYPE_ARRAY or TYPE_MAP.
  static void CollectionValueToJSON(const CollectionValue& collection_value,
      PrimitiveType collection_type, const TupleDescriptor* item_tuple_desc,
      rapidjson::Writer<JsonStream>* writer);

  // Gets a non-null StructVal and writes it in JSON format using 'writer'. Uses
  // 'column_type' to figure out field names and types. This function can call itself
  // recursively in case of nested structs.
  static void StructValToJSON(const impala_udf::StructVal& struct_val,
      const ColumnType& column_type, rapidjson::Writer<JsonStream>* writer);

 private:
  static void PrimitiveValueToJSON(void* value, const ColumnType& type, bool map_key,
      rapidjson::Writer<JsonStream>* writer);
  static void WriteNull(rapidjson::Writer<JsonStream>* writer);
  static void CollectionElementToJSON(Tuple* item_tuple, const SlotDescriptor& slot_desc,
      bool map_key, rapidjson::Writer<JsonStream>* writer);
  static void ArrayValueToJSON(const CollectionValue& array_value,
    const TupleDescriptor* item_tuple_desc, rapidjson::Writer<JsonStream>* writer);
  static void MapValueToJSON(const CollectionValue& map_value,
      const TupleDescriptor* item_tuple_desc, rapidjson::Writer<JsonStream>* writer);

};

} // namespace impala
