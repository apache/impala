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


#include <boost/algorithm/string.hpp>
#include <sstream>

#include "catalog/catalog-util.h"
#include "common/status.h"
#include "exec/read-write-util.h"
#include "util/compress.h"
#include "util/debug-util.h"

#include "common/names.h"

using boost::algorithm::to_upper_copy;

namespace impala {

TCatalogObjectType::type TCatalogObjectTypeFromName(const string& name) {
  const string& upper = to_upper_copy(name);
  if (upper == "DATABASE") {
    return TCatalogObjectType::DATABASE;
  } else if (upper == "TABLE") {
    return TCatalogObjectType::TABLE;
  } else if (upper == "VIEW") {
    return TCatalogObjectType::VIEW;
  } else if (upper == "FUNCTION") {
    return TCatalogObjectType::FUNCTION;
  } else if (upper == "CATALOG") {
    return TCatalogObjectType::CATALOG;
  } else if (upper == "DATA_SOURCE") {
    return TCatalogObjectType::DATA_SOURCE;
  } else if (upper == "HDFS_CACHE_POOL") {
    return TCatalogObjectType::HDFS_CACHE_POOL;
  } else if (upper == "ROLE") {
    return TCatalogObjectType::ROLE;
  } else if (upper == "PRIVILEGE") {
    return TCatalogObjectType::PRIVILEGE;
  }
  return TCatalogObjectType::UNKNOWN;
}

Status TCatalogObjectFromEntryKey(const string& key,
    TCatalogObject* catalog_object) {
  // Reconstruct the object based only on the key.
  size_t pos = key.find(":");
  if (pos == string::npos || pos >= key.size() - 1) {
    stringstream error_msg;
    error_msg << "Invalid topic entry key format: " << key;
    return Status(error_msg.str());
  }

  TCatalogObjectType::type object_type = TCatalogObjectTypeFromName(key.substr(0, pos));
  string object_name = key.substr(pos + 1);
  return TCatalogObjectFromObjectName(object_type, object_name, catalog_object);
}

Status TCatalogObjectFromObjectName(const TCatalogObjectType::type& object_type,
    const string& object_name, TCatalogObject* catalog_object) {
  switch (object_type) {
    case TCatalogObjectType::DATABASE:
      catalog_object->__set_type(object_type);
      catalog_object->__set_db(TDatabase());
      catalog_object->db.__set_db_name(object_name);
      break;
    case TCatalogObjectType::TABLE:
    case TCatalogObjectType::VIEW: {
      catalog_object->__set_type(object_type);
      catalog_object->__set_table(TTable());
      // Parse what should be a fully qualified table name
      int pos = object_name.find(".");
      if (pos == string::npos || pos >= object_name.size() - 1) {
        stringstream error_msg;
        error_msg << "Invalid table name: " << object_name;
        return Status(error_msg.str());
      }
      catalog_object->table.__set_db_name(object_name.substr(0, pos));
      catalog_object->table.__set_tbl_name(object_name.substr(pos + 1));
      break;
    }
    case TCatalogObjectType::FUNCTION: {
      // The key looks like: <db>.fn(<args>). We need to parse out the
      // db, fn and signature.
      catalog_object->__set_type(object_type);
      catalog_object->__set_fn(TFunction());
      int dot = object_name.find(".");
      int paren = object_name.find("(");
      if (dot == string::npos || dot >= object_name.size() - 1 ||
          paren == string::npos || paren >= object_name.size() - 1 ||
          paren <= dot) {
        stringstream error_msg;
        error_msg << "Invalid function name: " << object_name;
        return Status(error_msg.str());
      }
      catalog_object->fn.name.__set_db_name(object_name.substr(0, dot));
      catalog_object->fn.name.__set_function_name(
          object_name.substr(dot + 1, paren - dot - 1));
      catalog_object->fn.__set_signature(object_name.substr(dot + 1));
      break;
    }
    case TCatalogObjectType::DATA_SOURCE:
      catalog_object->__set_type(object_type);
      catalog_object->__set_data_source(TDataSource());
      catalog_object->data_source.__set_name(object_name);
      break;
    case TCatalogObjectType::HDFS_CACHE_POOL:
      catalog_object->__set_type(object_type);
      catalog_object->__set_cache_pool(THdfsCachePool());
      catalog_object->cache_pool.__set_pool_name(object_name);
      break;
    case TCatalogObjectType::ROLE:
      catalog_object->__set_type(object_type);
      catalog_object->__set_role(TRole());
      catalog_object->role.__set_role_name(object_name);
      break;
    case TCatalogObjectType::PRIVILEGE: {
      int pos = object_name.find(".");
      if (pos == string::npos || pos >= object_name.size() - 1) {
        stringstream error_msg;
        error_msg << "Invalid privilege name: " << object_name;
        return Status(error_msg.str());
      }
      catalog_object->__set_type(object_type);
      catalog_object->__set_privilege(TPrivilege());
      catalog_object->privilege.__set_role_id(atoi(object_name.substr(0, pos).c_str()));
      catalog_object->privilege.__set_privilege_name(object_name.substr(pos + 1));
      break;
    }
    case TCatalogObjectType::CATALOG:
    case TCatalogObjectType::UNKNOWN:
    default:
      stringstream error_msg;
      error_msg << "Unexpected object type: " << object_type;
      return Status(error_msg.str());
  }
  return Status::OK();
}

string TCatalogObjectToEntryKey(const TCatalogObject& catalog_object) {
  // The key format is: "TCatalogObjectType:<fully qualified object name>"
  stringstream entry_key;
  entry_key << PrintTCatalogObjectType(catalog_object.type) << ":";
  switch (catalog_object.type) {
    case TCatalogObjectType::DATABASE:
      entry_key << catalog_object.db.db_name;
      break;
    case TCatalogObjectType::TABLE:
    case TCatalogObjectType::VIEW:
      entry_key << catalog_object.table.db_name << "." << catalog_object.table.tbl_name;
      break;
    case TCatalogObjectType::FUNCTION:
      entry_key << catalog_object.fn.name.db_name << "."
                << catalog_object.fn.signature;
      break;
    case TCatalogObjectType::CATALOG:
      entry_key << catalog_object.catalog.catalog_service_id;
      break;
    case TCatalogObjectType::DATA_SOURCE:
      entry_key << catalog_object.data_source.name;
      break;
    case TCatalogObjectType::HDFS_CACHE_POOL:
      entry_key << catalog_object.cache_pool.pool_name;
      break;
    case TCatalogObjectType::ROLE:
      entry_key << catalog_object.role.role_name;
      break;
    case TCatalogObjectType::PRIVILEGE:
      entry_key << catalog_object.privilege.role_id << "."
                << catalog_object.privilege.privilege_name;
      break;
    default:
      break;
  }
  return entry_key.str();
}

Status CompressCatalogObject(string* catalog_object) {
  scoped_ptr<Codec> compressor;
  RETURN_IF_ERROR(Codec::CreateCompressor(nullptr, false, THdfsCompression::LZ4,
      &compressor));
  string output_buffer;
  int64_t compressed_data_len = compressor->MaxOutputLen(catalog_object->size());
  int64_t output_buffer_len = compressed_data_len + sizeof(uint32_t);
  output_buffer.resize(output_buffer_len);
  uint8_t* output_buffer_ptr =
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(output_buffer.data()));
  ReadWriteUtil::PutInt(output_buffer_ptr, static_cast<uint32_t>(catalog_object->size()));
  output_buffer_ptr += sizeof(uint32_t);
  compressor->ProcessBlock(true, catalog_object->size(),
      reinterpret_cast<const uint8_t*>(catalog_object->data()), &compressed_data_len,
      &output_buffer_ptr);
  output_buffer.resize(compressed_data_len + sizeof(uint32_t));
  *catalog_object = move(output_buffer);
  return Status::OK();
}

Status DecompressCatalogObject(const string& compressed_catalog_object,
    vector<uint8_t>* output_buffer) {
  scoped_ptr<Codec> decompressor;
  RETURN_IF_ERROR(Codec::CreateDecompressor(nullptr, false, THdfsCompression::LZ4,
      &decompressor));
  const uint8_t* input_data_ptr =
      reinterpret_cast<const uint8_t*>(compressed_catalog_object.data());
  int64_t decompressed_len = ReadWriteUtil::GetInt<uint32_t>(input_data_ptr);
  output_buffer->resize(decompressed_len);
  input_data_ptr += sizeof(uint32_t);
  uint8_t* decompressed_data_ptr = output_buffer->data();
  int64_t compressed_data_len = compressed_catalog_object.size() - sizeof(uint32_t);
  RETURN_IF_ERROR(decompressor->ProcessBlock(true, compressed_data_len,
      input_data_ptr, &decompressed_len, &decompressed_data_ptr));
  return Status::OK();
}

}
