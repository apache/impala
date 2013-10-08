// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_CATALOG_CATALOG_UTIL_H
#define IMPALA_CATALOG_CATALOG_UTIL_H

#include "gen-cpp/CatalogObjects_types.h"

namespace impala {

// Contains utility functions for working with TCatalogObjects and their related types.

class Status;

// Converts a string to the matching TCatalogObjectType enum type. Returns
// TCatalogObjectType::UNKNOWN if no match was found.
TCatalogObjectType::type TCatalogObjectTypeFromName(const std::string& name);

// Parses the given IMPALA_CATALOG_TOPIC topic entry key to determine the
// TCatalogObjectType and unique object name. Populates catalog_object with the result.
// This is used to reconstruct type information when an item is deleted from the
// topic. At that time the only context available about the object being deleted is its
// its topic entry key which contains only the type and object name. The resulting
// TCatalogObject can then be used to removing a matching item from the catalog.
Status TCatalogObjectFromEntryKey(const std::string& key,
    TCatalogObject* catalog_object);

// Populates a TCatalogObject based on the given object type (TABLE, DATABASE, etc) and
// object name string.
Status TCatalogObjectFromObjectName(const TCatalogObjectType::type& object_type,
    const std::string& object_name, TCatalogObject* catalog_object);

// Builds and returns the topic entry key string for the given TCatalogObject. The key
// format is: "TCatalogObjectType:<fully qualified object name>". So a table named
// "foo" in a database named "bar" would have a key of: "TABLE:bar.foo"
// Returns an empty string if there were any problem building the key.
std::string TCatalogObjectToEntryKey(const TCatalogObject& catalog_object);

}

#endif
