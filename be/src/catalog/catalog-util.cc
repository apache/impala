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


#include <boost/algorithm/string.hpp>
#include <sstream>

#include "catalog/catalog-util.h"
#include "common/status.h"
#include "util/debug-util.h"

using namespace boost::algorithm;
using namespace std;

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
      catalog_object->__set_type(object_type);
      catalog_object->__set_fn(TFunction());
      // The key only contains the database name and signature string, which is all that
      // is needed to uniquely identify the function.
      int pos = object_name.find(".");
      if (pos == string::npos || pos >= object_name.size() - 1) {
        stringstream error_msg;
        error_msg << "Invalid function name: " << object_name;
        return Status(error_msg.str());
      }
      catalog_object->fn.name.__set_db_name(object_name.substr(0, pos));
      catalog_object->fn.__set_signature(object_name.substr(pos + 1));
      break;
    }
    case TCatalogObjectType::CATALOG:
    case TCatalogObjectType::UNKNOWN:
    default:
      stringstream error_msg;
      error_msg << "Unexpected object type: " << object_type;
      return Status(error_msg.str());
  }
  return Status::OK;
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
    default:
      break;
  }
  return entry_key.str();
}


}
