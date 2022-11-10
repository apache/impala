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

#include "runtime/complex-value-writer.h"

#include <string>

#include "runtime/raw-value.inline.h"

namespace impala {

using impala_udf::StructVal;

namespace {

bool IsPrimitiveTypePrintedAsString(const ColumnType& type) {
  return type.IsStringType() || type.IsDateType() || type.IsTimestampType();
}

} // anonymous namespace

// Writes a primitive value using 'writer'. If 'map_key' is true, treats the value as a
// map key. Maps are printed as JSON objects, but in Impala maps can have non-string keys
// while JSON objects can't. We circumvent this rule by writing raw values with the
// 'rapidjson::kStringType' type when printing map keys. These non-string key values will
// not have quotes around them in the printed text.
template <class JsonStream>
void ComplexValueWriter<JsonStream>::PrimitiveValueToJSON(void* value,
    const ColumnType& type, bool map_key, rapidjson::Writer<JsonStream>* writer) {
  if (type.IsBooleanType() && !map_key) {
    writer->Bool( *(reinterpret_cast<bool*>(value)) );
    return;
  }

  string tmp;
  // Passing -1 as 'scale' means we do not modify the precision setting of the stream
  // (used when printing floating point values).
  constexpr int scale = -1;
  RawValue::PrintValue(value, type, scale, &tmp);
  if (IsPrimitiveTypePrintedAsString(type)) {
    writer->String(tmp.c_str());
  } else {
    writer->RawValue(tmp.c_str(), tmp.size(),
        map_key ? rapidjson::kStringType : rapidjson::kNumberType);
  }
}

template <class JsonStream>
void ComplexValueWriter<JsonStream>::WriteNull(rapidjson::Writer<JsonStream>* writer) {
  // We don't use writer->Null() because that does not work for map keys. Maps are
  // modelled as JSON objects but JSON objects can only have string keys, not nulls. We
  // circumvent this limitation by writing a raw value.
  constexpr char const* null_str  = RawValue::NullLiteral(/*top_level*/ false);
  const int null_str_len = std::char_traits<char>::length(null_str);
  writer->RawValue(null_str, null_str_len, rapidjson::kStringType);
}

template <class JsonStream>
void ComplexValueWriter<JsonStream>::CollectionElementToJSON(Tuple* item_tuple,
    const SlotDescriptor& slot_desc, bool map_key,
    rapidjson::Writer<JsonStream>* writer) {
  void* element = item_tuple->GetSlot(slot_desc.tuple_offset());
  DCHECK(element != nullptr);
  const ColumnType& element_type = slot_desc.type();
  bool element_is_null = item_tuple->IsNull(slot_desc.null_indicator_offset());
  if (element_is_null) {
    WriteNull(writer);
  } else if (element_type.IsStructType()) {
    DCHECK(false) << "Structs in collections are not supported yet.";
  } else if (element_type.IsCollectionType()) {
    const CollectionValue* nested_collection_val =
      reinterpret_cast<CollectionValue*>(element);
    const TupleDescriptor* child_item_tuple_desc =
      slot_desc.children_tuple_descriptor();
    DCHECK(child_item_tuple_desc != nullptr);
    ComplexValueWriter::CollectionValueToJSON(*nested_collection_val, element_type.type,
        child_item_tuple_desc, writer);
  } else {
    PrimitiveValueToJSON(element, element_type, map_key, writer);
  }
}

// Gets an ArrayValue and writes it in JSON format using 'writer'.
template <class JsonStream>
void ComplexValueWriter<JsonStream>::ArrayValueToJSON(const CollectionValue& array_value,
    const TupleDescriptor* item_tuple_desc,
    rapidjson::Writer<JsonStream>* writer) {
  DCHECK(item_tuple_desc != nullptr);

  const int item_byte_size = item_tuple_desc->byte_size();
  const vector<SlotDescriptor*>& slot_descs = item_tuple_desc->slots();

  DCHECK(slot_descs.size() == 1);
  DCHECK(slot_descs[0] != nullptr);

  writer->StartArray();

  for (int i = 0; i < array_value.num_tuples; ++i) {
    Tuple* item_tuple = reinterpret_cast<Tuple*>(
        array_value.ptr + i * item_byte_size);
    // Print the value.
    const SlotDescriptor& value_slot_desc = *slot_descs[0];
    CollectionElementToJSON(item_tuple, value_slot_desc, false, writer);
  }

  writer->EndArray();
}

// Gets a MapValue and writes it in JSON format using 'writer'.
template <class JsonStream>
void ComplexValueWriter<JsonStream>::MapValueToJSON(const CollectionValue& map_value,
    const TupleDescriptor* item_tuple_desc, rapidjson::Writer<JsonStream>* writer) {
  DCHECK(item_tuple_desc != nullptr);

  const int item_byte_size = item_tuple_desc->byte_size();
  const vector<SlotDescriptor*>& slot_descs = item_tuple_desc->slots();

  DCHECK(slot_descs.size() == 2);
  DCHECK(slot_descs[0] != nullptr);
  DCHECK(slot_descs[1] != nullptr);

  writer->StartObject();

  for (int i = 0; i < map_value.num_tuples; ++i) {
    Tuple* item_tuple = reinterpret_cast<Tuple*>(
        map_value.ptr + i * item_byte_size);
    // Print key.
    const SlotDescriptor& key_slot_desc = *slot_descs[0];
    CollectionElementToJSON(item_tuple, key_slot_desc, true, writer);

    // Print the value.
    const SlotDescriptor& value_slot_desc = *slot_descs[1];
    CollectionElementToJSON(item_tuple, value_slot_desc, false, writer);
  }

  writer->EndObject();
}

template <class JsonStream>
void ComplexValueWriter<JsonStream>::CollectionValueToJSON(
    const CollectionValue& collection_value, PrimitiveType collection_type,
    const TupleDescriptor* item_tuple_desc, rapidjson::Writer<JsonStream>* writer) {
  if (collection_type == PrimitiveType::TYPE_MAP) {
    MapValueToJSON(collection_value, item_tuple_desc, writer);
  } else {
    DCHECK_EQ(collection_type, PrimitiveType::TYPE_ARRAY);
    ArrayValueToJSON(collection_value, item_tuple_desc, writer);
  }
}

template <class JsonStream>
void ComplexValueWriter<JsonStream>::StructValToJSON(const StructVal& struct_val,
    const ColumnType& column_type, rapidjson::Writer<JsonStream>* writer) {
  DCHECK(column_type.type == TYPE_STRUCT);
  DCHECK_EQ(struct_val.num_children, column_type.children.size());
  writer->StartObject();
  for (int i = 0; i < struct_val.num_children; ++i) {
    writer->String(column_type.field_names[i].c_str());
    void* child = (void*)(struct_val.ptr[i]);
    const ColumnType& child_type = column_type.children[i];
    if (child == nullptr) {
      WriteNull(writer);
    } else if (child_type.IsStructType()) {
      StructValToJSON(*((StructVal*)child), child_type, writer);
    } else {
      PrimitiveValueToJSON(child, child_type, false, writer);
    }
  }
  writer->EndObject();
}

} // namespace impala
