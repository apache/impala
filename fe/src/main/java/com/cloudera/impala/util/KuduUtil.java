
// Copyright 2015 Cloudera Inc.
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

package com.cloudera.impala.util;

import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.KeyBuilder;
import org.kududb.client.KuduTable;

import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;

public class KuduUtil {

  private static final String SPLIT_KEYS_ERROR_MESSAGE = "Error parsing splits keys.";

  /**
   * Compare the schema of a HMS table and a Kudu table. Returns true if both tables have
   * a matching schema.
   */
  public static boolean compareSchema(Table hiveTable, KuduTable kuduTable)
      throws ImpalaRuntimeException {
    List<FieldSchema> hiveFields = hiveTable.getSd().getCols();
    List<ColumnSchema> kuduFields = kuduTable.getSchema().getColumns();
    if (hiveFields.size() != kuduFields.size()) return false;

    HashMap<String, ColumnSchema> kuduFieldMap = Maps.newHashMap();
    for (ColumnSchema kuduField: kuduFields) {
      kuduFieldMap.put(kuduField.getName().toUpperCase(), kuduField);
    }

    for (FieldSchema hiveField: hiveFields) {
      ColumnSchema kuduField = kuduFieldMap.get(hiveField.getName().toUpperCase());
      if (kuduField == null || fromImpalaType(
          com.cloudera.impala.catalog.Type.parseColumnType(hiveField)) !=
          kuduField.getType()) {
        return false;
      }
    }

    return true;
  }

  /**
   * Parses split keys from statements.
   *
   * Split keys are expected to be in json, as an array of arrays, in the form:
   * '[[value1_col1, value1_col2, ...], [value2_col1, value2_col2, ...], ...]'
   *
   * Each inner array corresponds to a split key and is expected to have the same
   * number and type of values as specified in 'keyProjection'.
   */
  public static List<KeyBuilder> parseSplits(Schema keyProjection, String kuduSplits)
      throws ImpalaRuntimeException {

    // If there are no splits return early.
    if (kuduSplits == null || kuduSplits.isEmpty()) return ImmutableList.of();

    ImmutableList.Builder<KeyBuilder> keyBuilders = ImmutableList.builder();

    // ...Otherwise parse the splits. We're expecting splits in the format of a list of
    // lists of keys. We only support specifying splits for int and string keys
    // (currently those are the only type of keys allowed in Kudu too).
    try {
      JsonReader jr = Json.createReader(new StringReader(kuduSplits));
      JsonArray keysList = jr.readArray();
      for (int i = 0; i < keysList.size(); i++) {
        KeyBuilder keyBuilder = new KeyBuilder(keyProjection);
        JsonArray compoundKey = keysList.getJsonArray(i);
        if (compoundKey.size() != keyProjection.getKeysCount()) {
          throw new ImpalaRuntimeException(SPLIT_KEYS_ERROR_MESSAGE +
              " Wrong number of keys.");
        }
        for (int j = 0; j < compoundKey.size(); j++) {
          setKey(keyBuilder, keyProjection.getColumn(j).getType(), compoundKey, j);
        }
        keyBuilders.add(keyBuilder);
      }
    } catch (ImpalaRuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new ImpalaRuntimeException(SPLIT_KEYS_ERROR_MESSAGE + " Problem parsing json"
          + ": " + e.getMessage(), e);
    }

    return keyBuilders.build();
  }

  private static void setKey(KeyBuilder keyBuilder, Type type, JsonArray array, int pos)
      throws ImpalaRuntimeException {
    switch (type) {
      case INT8: keyBuilder.addByte((byte) array.getInt(pos)); break;
      case INT16: keyBuilder.addShort((short) array.getInt(pos)); break;
      case INT32: keyBuilder.addInt(array.getInt(pos)); break;
      case INT64: keyBuilder.addLong(array.getJsonNumber(pos).longValue()); break;
      case STRING: keyBuilder.addString(array.getString(pos)); break;
      default:
        throw new ImpalaRuntimeException("Key columns not supported for type: "
            + type.toString());
    }
  }

  /**
   * Parses a string of the form "a, b, c" and returns a set of values split by ',' and
   * stripped of the whitespace.
   */
  public static HashSet<String> parseKeyColumns(String cols) {

    Function<String, String> strip = new Function<String, String>() {
      @Override
      public String apply(String input) {
        return input.trim();
      }
    };

    return Sets.newHashSet(Lists.transform(Lists.newArrayList(cols.split(",")), strip));
  }

  /**
   * Helper function that takes a string with a list of comma separated addresses and
   * transforms it into a list of HostAndPorts
   */
  public static List<HostAndPort> stringToHostAndPort(String masters) {
    return stringToHostAndPort(Lists.newArrayList(masters.split(",")));
  }

  /**
   * Helper function that transforms a list of Strings to HostAndPorts
   */
  public static List<HostAndPort> stringToHostAndPort(List<String> masters) {
    Function<String, HostAndPort> fun = new Function<String, HostAndPort>() {
      @Override
      public HostAndPort apply(String input) {
        return HostAndPort.fromString(input.trim());
      }
    };
    return Lists.transform(masters, fun);
  }

  /**
   * Helper function that transforms a list of HostAndPort instances to a list of strings
   */
  public static List<String> hostAndPortToString(List<HostAndPort> masters) {
    return Lists.transform(masters, new Function<HostAndPort, String>() {
      @Override
      public String apply(HostAndPort input) {
        return input.toString();
      }
    });
  }

  /**
   * Converts a given Impala catalog type to the Kudu type. Throws an exception if the
   * type cannot be converted.
   */
  public static Type fromImpalaType(com.cloudera.impala.catalog.Type t)
      throws ImpalaRuntimeException {
    if (!t.isScalarType()) {
      throw new ImpalaRuntimeException(String.format(
          "Non-scalar type %s is not supported in Kudu", t.toSql()));
    }
    ScalarType s = (ScalarType) t;
    switch (s.getPrimitiveType()) {
      case TINYINT: return Type.INT8;
      case SMALLINT: return Type.INT16;
      case INT: return Type.INT32;
      case BIGINT: return Type.INT64;
      case BOOLEAN: return Type.BOOL;
      case CHAR: return Type.STRING;
      case STRING: return Type.STRING;
      case VARCHAR: return Type.STRING;
      case DOUBLE: return Type.DOUBLE;
      case FLOAT: return Type.FLOAT;
        /* Fall through below */
      case INVALID_TYPE:
      case NULL_TYPE:
      case TIMESTAMP:
      case BINARY:
      case DATE:
      case DATETIME:
      case DECIMAL:
      default:
        throw new ImpalaRuntimeException(String.format(
            "Type %s is not supported in Kudu", s.toSql()));
    }
  }
}
