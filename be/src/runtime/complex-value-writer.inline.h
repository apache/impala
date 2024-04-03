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
#include "util/coding-util.h"

namespace impala {

using impala_udf::StructVal;

namespace {

bool IsPrimitiveTypePrintedAsString(const ColumnType& type) {
  return type.IsStringType() || type.IsDateType() || type.IsTimestampType();
}

} // anonymous namespace

// Writes a primitive value. If 'map_key' is true, treats the value as a
// map key. Maps are printed as JSON objects, but in Impala maps can have non-string keys
// while JSON objects can't. We circumvent this rule by writing values in the following
// way:
//  - if 'stringify_map_keys_' is false, we write raw values with the
//    'rapidjson::kStringType' type when printing map keys; these non-string key values
//    will not have quotes around them in the printed text.
//  - if 'stringify_map_keys_' is true, we convert the map keys to strings and write them
//    with quotes.
// If 'map_key' is false, 'stringify_map_keys_' has no effect.
template <class JsonStream>
void ComplexValueWriter<JsonStream>::PrimitiveValueToJSON(void* value,
    const ColumnType& type, bool map_key) {
  if (type.IsBooleanType() && !map_key) {
    writer_->Bool( *(reinterpret_cast<bool*>(value)) );
    return;
  }

  string tmp;
  // Passing -1 as 'scale' means we do not modify the precision setting of the stream
  // (used when printing floating point values).
  constexpr int scale = -1;
  RawValue::PrintValue(value, type, scale, &tmp);
  const bool should_convert_to_string = map_key && stringify_map_keys_;
  if (IsPrimitiveTypePrintedAsString(type) || should_convert_to_string) {
    if (type.IsBinaryType()) {
      int64_t base64_max_len;
      bool succ = Base64EncodeBufLen(tmp.size(), &base64_max_len);
      DCHECK(succ);

      // 'base64_max_len' includes the null terminator.
      string buf(base64_max_len - 1, '\0');
      unsigned base64_len = 0;
      succ = Base64Encode(tmp.c_str(), tmp.size(), base64_max_len, buf.data(),
          &base64_len);
      DCHECK(succ);

      tmp = std::move(buf);
    }
    writer_->String(tmp.c_str());
  } else {
    writer_->RawValue(tmp.c_str(), tmp.size(),
        map_key ? rapidjson::kStringType : rapidjson::kNumberType);
  }
}

template <class JsonStream>
void ComplexValueWriter<JsonStream>::WriteNull(bool map_key) {
  // We don't use writer_->Null() because that does not work for map keys. Maps are
  // modelled as JSON objects but JSON objects can only have string keys, not nulls. We
  // circumvent this limitation by writing the string "null" with quotes if
  // 'stringify_map_keys_' is true and a raw null value without quotes if it is false.
  constexpr char const* null_str  = RawValue::NullLiteral(/*top_level*/ false);

  if (map_key && stringify_map_keys_) {
    writer_->String(null_str);
  } else {
    const int null_str_len = std::char_traits<char>::length(null_str);
    writer_->RawValue(null_str, null_str_len, rapidjson::kStringType);
  }
}

// Structs in collections are not converted to StructVal but are left as they are in the
// tuple.
template <class JsonStream>
void ComplexValueWriter<JsonStream>::StructInCollectionToJSON(Tuple* item_tuple,
    const SlotDescriptor& struct_slot_desc) {
  const TupleDescriptor* children_tuple_desc =
      struct_slot_desc.children_tuple_descriptor();
  DCHECK(children_tuple_desc != nullptr);

  const ColumnType& struct_type = struct_slot_desc.type();
  const std::vector<SlotDescriptor*>& child_slots = children_tuple_desc->slots();

  DCHECK(struct_type.type == TYPE_STRUCT);
  DCHECK_EQ(child_slots.size(), struct_type.children.size());

  writer_->StartObject();
  for (int i = 0; i < child_slots.size(); ++i) {
    writer_->String(struct_type.field_names[i].c_str());

    const SlotDescriptor& child_slot_desc = *child_slots[i];
    bool element_is_null = item_tuple->IsNull(child_slot_desc.null_indicator_offset());
    if (element_is_null) {
      WriteNull(writer_);
      continue;
    }

    const ColumnType& child_type = child_slot_desc.type();
    void* child = item_tuple->GetSlot(child_slot_desc.tuple_offset());
    if (child_type.IsStructType()) {
      StructInCollectionToJSON(item_tuple, child_slot_desc);
    } else if (child_type.IsCollectionType()) {
      const CollectionValue* nested_collection_val =
          reinterpret_cast<CollectionValue*>(child);
      const TupleDescriptor* child_item_tuple_desc =
          child_slot_desc.children_tuple_descriptor();
      DCHECK(child_item_tuple_desc != nullptr);
      CollectionValueToJSON(*nested_collection_val, child_type.type,
          child_item_tuple_desc);
    } else {
      PrimitiveValueToJSON(child, child_type, false);
    }
  }
  writer_->EndObject();
}

template <class JsonStream>
void ComplexValueWriter<JsonStream>::CollectionElementToJSON(Tuple* item_tuple,
    const SlotDescriptor& slot_desc, bool map_key) {
  void* element = item_tuple->GetSlot(slot_desc.tuple_offset());
  DCHECK(element != nullptr);
  const ColumnType& element_type = slot_desc.type();
  bool element_is_null = item_tuple->IsNull(slot_desc.null_indicator_offset());
  if (element_is_null) {
    WriteNull(map_key);
  } else if (element_type.IsStructType()) {
    DCHECK(!map_key) << "Structs cannot be map keys.";
    StructInCollectionToJSON(item_tuple, slot_desc);
  } else if (element_type.IsCollectionType()) {
    const CollectionValue* nested_collection_val =
      reinterpret_cast<CollectionValue*>(element);
    const TupleDescriptor* child_item_tuple_desc =
      slot_desc.children_tuple_descriptor();
    DCHECK(child_item_tuple_desc != nullptr);
    ComplexValueWriter::CollectionValueToJSON(*nested_collection_val, element_type.type,
        child_item_tuple_desc);
  } else {
    PrimitiveValueToJSON(element, element_type, map_key);
  }
}

// Gets an array value and writes it in JSON format.
template <class JsonStream>
void ComplexValueWriter<JsonStream>::ArrayValueToJSON(const CollectionValue& array_value,
    const TupleDescriptor* item_tuple_desc) {
  DCHECK(item_tuple_desc != nullptr);

  const int item_byte_size = item_tuple_desc->byte_size();
  const vector<SlotDescriptor*>& slot_descs = item_tuple_desc->slots();

  DCHECK(slot_descs.size() == 1);
  DCHECK(slot_descs[0] != nullptr);

  writer_->StartArray();

  for (int i = 0; i < array_value.num_tuples; ++i) {
    Tuple* item_tuple = reinterpret_cast<Tuple*>(
        array_value.ptr + i * item_byte_size);
    const SlotDescriptor& value_slot_desc = *slot_descs[0];
    CollectionElementToJSON(item_tuple, value_slot_desc, false);
  }

  writer_->EndArray();
}

// Gets a map value and writes it in JSON format.
template <class JsonStream>
void ComplexValueWriter<JsonStream>::MapValueToJSON(const CollectionValue& map_value,
    const TupleDescriptor* item_tuple_desc) {
  DCHECK(item_tuple_desc != nullptr);

  const int item_byte_size = item_tuple_desc->byte_size();
  const vector<SlotDescriptor*>& slot_descs = item_tuple_desc->slots();

  DCHECK(slot_descs.size() == 2);
  DCHECK(slot_descs[0] != nullptr);
  DCHECK(slot_descs[1] != nullptr);

  writer_->StartObject();

  for (int i = 0; i < map_value.num_tuples; ++i) {
    Tuple* item_tuple = reinterpret_cast<Tuple*>(
        map_value.ptr + i * item_byte_size);
    // Print key.
    const SlotDescriptor& key_slot_desc = *slot_descs[0];
    CollectionElementToJSON(item_tuple, key_slot_desc, true);

    // Print the value.
    const SlotDescriptor& value_slot_desc = *slot_descs[1];
    CollectionElementToJSON(item_tuple, value_slot_desc, false);
  }

  writer_->EndObject();
}

template <class JsonStream>
ComplexValueWriter<JsonStream>::ComplexValueWriter(rapidjson::Writer<JsonStream>* writer,
    bool stringify_map_keys)
  : writer_(writer),
    stringify_map_keys_(stringify_map_keys) {}

template <class JsonStream>
void ComplexValueWriter<JsonStream>::CollectionValueToJSON(
    const CollectionValue& collection_value, PrimitiveType collection_type,
    const TupleDescriptor* item_tuple_desc) {
  if (collection_type == PrimitiveType::TYPE_MAP) {
    MapValueToJSON(collection_value, item_tuple_desc);
  } else {
    DCHECK_EQ(collection_type, PrimitiveType::TYPE_ARRAY);
    ArrayValueToJSON(collection_value, item_tuple_desc);
  }
}

template <class JsonStream>
void ComplexValueWriter<JsonStream>::StructValToJSON(const StructVal& struct_val,
    const SlotDescriptor& slot_desc) {
  const ColumnType& struct_type = slot_desc.type();
  DCHECK(struct_type.type == TYPE_STRUCT);
  DCHECK_EQ(struct_val.num_children, struct_type.children.size());

  const TupleDescriptor* children_item_tuple_desc = slot_desc.children_tuple_descriptor();
  DCHECK(children_item_tuple_desc != nullptr);
  const std::vector<SlotDescriptor*>& child_slot_descs =
      children_item_tuple_desc->slots();
  DCHECK_EQ(struct_val.num_children, child_slot_descs.size());

  writer_->StartObject();
  for (int i = 0; i < struct_val.num_children; ++i) {
    writer_->String(struct_type.field_names[i].c_str());
    void* child = (void*)(struct_val.ptr[i]);
    const SlotDescriptor& child_slot_desc = *child_slot_descs[i];
    const ColumnType& child_type = struct_type.children[i];
    DCHECK_EQ(child_type, child_slot_desc.type());
    if (child == nullptr) {
      WriteNull(false);
    } else if (child_type.IsStructType()) {
      StructValToJSON(*((StructVal*)child), child_slot_desc);
    } else if (child_type.IsCollectionType()) {
      CollectionValue* collection_child = reinterpret_cast<CollectionValue*>(child);
      ComplexValueWriter::CollectionValueToJSON(*collection_child, child_type.type,
          child_slot_desc.children_tuple_descriptor());
    } else {
      PrimitiveValueToJSON(child, child_type, false);
    }
  }
  writer_->EndObject();
}

} // namespace impala
