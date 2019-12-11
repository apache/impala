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

#include "util/json-util.h"

#include <vector>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

#include "util/redactor.h"

#include "common/names.h"

using rapidjson::Document;
using rapidjson::Value;

namespace impala {

// Convert a repeated field to a JSON array and add it to 'obj'.
static void RepeatedFieldToJson(const google::protobuf::Message& pb,
    const google::protobuf::Reflection* reflection,
    const google::protobuf::FieldDescriptor* field, Document* document, Value* obj) {
  Value arr(rapidjson::kArrayType);
  int size = reflection->FieldSize(pb, field);
  for (int i = 0; i < size; i++) {
    switch (field->cpp_type()) {
      case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
        arr.PushBack(
            reflection->GetRepeatedInt32(pb, field, i), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
        arr.PushBack(
            reflection->GetRepeatedInt64(pb, field, i), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
        arr.PushBack(
            reflection->GetRepeatedUInt32(pb, field, i), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
        arr.PushBack(
            reflection->GetRepeatedUInt64(pb, field, i), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
        arr.PushBack(
            reflection->GetRepeatedFloat(pb, field, i), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
        arr.PushBack(
            reflection->GetRepeatedDouble(pb, field, i), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
        arr.PushBack(reflection->GetRepeatedBool(pb, field, i), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
        Value enum_str(reflection->GetRepeatedEnum(pb, field, i)->name().c_str(),
            document->GetAllocator());
        arr.PushBack(enum_str, document->GetAllocator());
        break;
      }
      case google::protobuf::FieldDescriptor::CPPTYPE_STRING: {
        string str = reflection->GetRepeatedString(pb, field, i);
        Redact(&str, nullptr);
        Value val_str(str.c_str(), document->GetAllocator());
        arr.PushBack(val_str, document->GetAllocator());
        break;
      }
      case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
        Value child_obj(rapidjson::kObjectType);
        ProtobufToJson(
            reflection->GetRepeatedMessage(pb, field, i), document, &child_obj);
        arr.PushBack(child_obj, document->GetAllocator());
        break;
      }
      default:
        DCHECK(false) << "Type NYI: " << field->cpp_type() << " " << field->name();
    }
  }
  Value field_name(field->name().c_str(), document->GetAllocator());
  obj->AddMember(field_name, arr, document->GetAllocator());
}

void ProtobufToJson(const google::protobuf::Message& pb, Document* document, Value* obj) {
  const google::protobuf::Reflection* reflection = pb.GetReflection();
  vector<const google::protobuf::FieldDescriptor*> fields;
  reflection->ListFields(pb, &fields);
  for (const google::protobuf::FieldDescriptor* field : fields) {
    if (field->is_repeated()) {
      RepeatedFieldToJson(pb, reflection, field, document, obj);
      continue;
    }
    Value field_name(field->name().c_str(), document->GetAllocator());
    switch (field->cpp_type()) {
      case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
        obj->AddMember(
            field_name, reflection->GetInt32(pb, field), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
        obj->AddMember(
            field_name, reflection->GetInt64(pb, field), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
        obj->AddMember(
            field_name, reflection->GetUInt32(pb, field), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
        obj->AddMember(
            field_name, reflection->GetUInt64(pb, field), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
        obj->AddMember(
            field_name, reflection->GetFloat(pb, field), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
        obj->AddMember(
            field_name, reflection->GetDouble(pb, field), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
        obj->AddMember(
            field_name, reflection->GetBool(pb, field), document->GetAllocator());
        break;
      case google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
        Value enum_str(
            reflection->GetEnum(pb, field)->name().c_str(), document->GetAllocator());
        obj->AddMember(field_name, enum_str, document->GetAllocator());
        break;
      }
      case google::protobuf::FieldDescriptor::CPPTYPE_STRING: {
        string str = reflection->GetString(pb, field);
        Redact(&str, nullptr);
        Value val_str(str.c_str(), document->GetAllocator());
        obj->AddMember(field_name, val_str, document->GetAllocator());
        break;
      }
      case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
        Value child_obj(rapidjson::kObjectType);
        ProtobufToJson(reflection->GetMessage(pb, field), document, &child_obj);
        obj->AddMember(field_name, child_obj, document->GetAllocator());
        break;
      }
      default:
        DCHECK(false) << "Type NYI: " << field->cpp_type() << " " << field->name();
    }
  }
}
} // namespace impala
