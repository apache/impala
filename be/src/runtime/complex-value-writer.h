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

// Class that writes complex types (collections and structs) in the JSON format.
template <class JsonStream>
class ComplexValueWriter {
 public:
   // Will use 'writer' to write JSON text. Does not take ownership of 'writer', the
   // caller is responsible for managing its lifetime. If 'stringify_map_keys' is true,
   // converts map keys to strings; see IMPALA-11778.
   ComplexValueWriter(rapidjson::Writer<JsonStream>* writer, bool stringify_map_keys);

  // Gets a non-null CollectionValue and writes it in JSON format. 'collection_type'
  // should be either TYPE_ARRAY or TYPE_MAP.
  void CollectionValueToJSON(const CollectionValue& collection_value,
      PrimitiveType collection_type, const TupleDescriptor* item_tuple_desc);

  // Gets a non-null StructVal and writes it in JSON format. Uses 'slot_desc' to figure
  // out field names and types. This function can call itself recursively in case of
  // nested structs.
  void StructValToJSON(const impala_udf::StructVal& struct_val,
      const SlotDescriptor& slot_desc);

 private:
  void PrimitiveValueToJSON(void* value, const ColumnType& type, bool map_key);
  void WriteNull(bool map_key);
  void StructInCollectionToJSON(Tuple* item_tuple,
      const SlotDescriptor& struct_slot_desc);
  void CollectionElementToJSON(Tuple* item_tuple, const SlotDescriptor& slot_desc,
      bool map_key);
  void ArrayValueToJSON(const CollectionValue& array_value,
    const TupleDescriptor* item_tuple_desc);
  void MapValueToJSON(const CollectionValue& map_value,
      const TupleDescriptor* item_tuple_desc);

  rapidjson::Writer<JsonStream>* const writer_;

  // If true, converts map keys to strings; see IMPALA-11778.
  const bool stringify_map_keys_;
};

} // namespace impala
