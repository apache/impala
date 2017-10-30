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


#ifndef IMPALA_CATALOG_CATALOG_UTIL_H
#define IMPALA_CATALOG_CATALOG_UTIL_H

#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h"

namespace impala {

/// Contains utility functions for working with TCatalogObjects and their related types.

class Status;

/// Converts a string to the matching TCatalogObjectType enum type. Returns
/// TCatalogObjectType::UNKNOWN if no match was found.
TCatalogObjectType::type TCatalogObjectTypeFromName(const std::string& name);

/// Populates a TCatalogObject based on the given object type (TABLE, DATABASE, etc) and
/// object name string.
Status TCatalogObjectFromObjectName(const TCatalogObjectType::type& object_type,
    const std::string& object_name, TCatalogObject* catalog_object);

/// Builds and returns the topic entry key string for the given TCatalogObject. The key
/// format is: "TCatalogObjectType:<fully qualified object name>". So a table named
/// "foo" in a database named "bar" would have a key of: "TABLE:bar.foo"
/// Returns an empty string if there were any problem building the key.
std::string TCatalogObjectToEntryKey(const TCatalogObject& catalog_object);

/// Compresses a serialized catalog object using LZ4 and stores it back in
/// 'catalog_object'. Stores the size of the uncopressed catalog object in the
/// first sizeof(uint32_t) bytes of 'catalog_object'.
Status CompressCatalogObject(std::string* catalog_object) WARN_UNUSED_RESULT;

/// Decompress an LZ4-compressed catalog object. The decompressed object
/// is stored in 'output_buffer'. The first sizeof(uint32_t) bytes of
/// 'compressed_catalog_object' store the size of the uncompressed catalog
/// object.
Status DecompressCatalogObject(const std::string& compressed_catalog_object,
    std::vector<uint8_t>* output_buffer) WARN_UNUSED_RESULT;

}

#endif
