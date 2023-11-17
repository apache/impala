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

package org.apache.impala.datagenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Optional;
import java.util.Random;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class RandomNestedDataGenerator {

  public static Random rand;
  public static int maxNormalStrLen;
  public static int maxNumListItems;
  public static int numElementsGenerated = 0;
  public static ArrayList<Double> doubleCache;
  public static ArrayList<Float> floatCache;
  public static ArrayList<Integer> intCache;
  public static ArrayList<Long> longCache;
  public static ArrayList<String> stringCache;
  public static String alphabet = "abcdfghijklmnopqrstuvwxyz0123456789";
  public static final Integer NUM_ELEMENTS = 100;
  public static final Integer RAND_SEED = 12345;
  public static final Double CHANCE_UNIQUE = 0.02;

  private static void generateDataToFile(
      String schemaFile, int targetNumElements, String outputFile, Optional<Long> seed)
      throws IOException {
    buildCache();
    rand = seed.isPresent() ? new Random(seed.get()) : new Random();
    Schema schema = new Schema.Parser().parse(new File(schemaFile));
    Configuration conf = new Configuration();
    conf.set("parquet.avro.write-old-list-structure", "false");
    AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(
        new Path("file:///" + outputFile),
        schema,
        CompressionCodecName.UNCOMPRESSED,
        AvroParquetWriter.DEFAULT_BLOCK_SIZE,
        AvroParquetWriter.DEFAULT_PAGE_SIZE,
        false,
        conf);

    try {
      while (numElementsGenerated < targetNumElements) {
        Record record = (Record) generateDatum(schema, 0);
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }

  private static void buildCache() {
    buildDoubleCache();
    buildFloatCache();
    buildIntCache();
    buildLongCache();
    buildStringCache();
  }

  private static void buildDoubleCache() {
    rand = new Random(RAND_SEED);
    doubleCache = new ArrayList<Double>();
    for (double i = 0.0d; i < NUM_ELEMENTS; i++) {
      doubleCache.add(i);
    }
  }

  private static void buildFloatCache() {
    rand = new Random(RAND_SEED);
    floatCache = new ArrayList<Float>();
    for (float i = 0.0f; i < NUM_ELEMENTS; i++) {
      floatCache.add(i);
    }
  }

  private static void buildIntCache() {
    rand = new Random(RAND_SEED);
    intCache = new ArrayList<Integer>();
    for (int i = 0; i < NUM_ELEMENTS; i++) {
      intCache.add(i);
    }
  }

  private static void buildLongCache() {
    rand = new Random(RAND_SEED);
    longCache = new ArrayList<Long>();
    for (long i = 0; i < NUM_ELEMENTS; i++) {
      longCache.add(i);
    }
  }

  private static void buildStringCache() {
    rand = new Random(RAND_SEED);
    stringCache = new ArrayList<String>();
    for (int i = 0; i < NUM_ELEMENTS; i++) {
      StringBuilder sb = new StringBuilder();
      int len = rand.nextInt(maxNormalStrLen);
      for (int j = 0; j < len; j++) {
        sb.append(alphabet.charAt(rand.nextInt(alphabet.length())));
      }
      stringCache.add(sb.toString());
    }
  }

  private static boolean isOptional(Schema schema) {
    if (schema.getType() != Type.UNION) return false;
    for (Schema s: schema.getTypes()) {
      if (s.getType() == Type.NULL) return true;
    }
    return false;
  }

  private static int generateListLength(int depth) {
    if (rand.nextDouble() < 0.1) return 0; // empty list
    return rand.nextInt(maxNumListItems);
  }

  private static Schema getNonNullSchema(Schema schema) {
    if (schema.getType() != Type.UNION) return schema;
    assert schema.getTypes().size() == 2;
    for (Schema s: schema.getTypes()) {
      if (s.getType() != Type.NULL) return s;
    }
    assert false;
    return null;
  }

  private static boolean chooseNull() {
    return rand.nextDouble() < 0.2;
  }

  private static Double getRandomDouble() {
    numElementsGenerated += 1;
    if (rand.nextDouble() < CHANCE_UNIQUE) return rand.nextDouble();
    return doubleCache.get(rand.nextInt(doubleCache.size()));
  }

  private static Float getRandomFloat() {
    numElementsGenerated += 1;
    if (rand.nextDouble() < CHANCE_UNIQUE) return rand.nextFloat();
    return floatCache.get(rand.nextInt(floatCache.size()));
  }

  private static Integer getRandomInt() {
    numElementsGenerated += 1;
    if (rand.nextDouble() < CHANCE_UNIQUE) return rand.nextInt();
    return intCache.get(rand.nextInt(intCache.size()));
  }

  private static Long getRandomLong() {
    numElementsGenerated += 1;
    if (rand.nextDouble() < CHANCE_UNIQUE) return rand.nextLong();
    return longCache.get(rand.nextInt(longCache.size()));
  }

  private static Boolean getRandomBoolean() {
    numElementsGenerated += 1;
    return rand.nextBoolean();
  }

  private static String getRandomString() {
    numElementsGenerated += 1;
    if (rand.nextDouble() < CHANCE_UNIQUE) {
      // return a long unique
      StringBuilder sb = new StringBuilder();
      int len = rand.nextInt(900);
      for (int i = 0; i < len; i++){
        sb.append(alphabet.charAt(rand.nextInt(alphabet.length())));
      }
      return sb.toString();
    }
    return stringCache.get(rand.nextInt(stringCache.size()));
  }

  private static Object generateDatum(Schema schema, int depth) {
    if (isOptional(schema) && chooseNull()) {
      return null;
    }
    schema = getNonNullSchema(schema);

    switch (schema.getType()) {
      case RECORD: {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Field f: schema.getFields()) {
          builder.set(f, generateDatum(f.schema(), depth));
        }
        return builder.build();
      }
      case ARRAY: {
        Schema elementSchema = schema.getElementType();
        ArrayList<Object> arr = new ArrayList<Object>();
        int numElements = generateListLength(depth);
        for (int i = 0; i < numElements; i++) {
          arr.add(generateDatum(elementSchema, depth + 1));
        }
        return arr;
      }
      case MAP: {
        // All Avro keys are strings...
        Schema valueSchema = schema.getValueType();
        HashMap<String, Object> m = new HashMap<String, Object>();
        int numElements = generateListLength(depth);
        for (int i = 0; i < numElements; i++) {
          // Note: key collisions are possible
          String key = getRandomString();
          m.put(key, generateDatum(valueSchema, depth + 1));
        }
        return m;
      }
      case BOOLEAN: return Boolean.valueOf(getRandomBoolean());
      case DOUBLE: return Double.valueOf(getRandomDouble());
      case FLOAT: return Float.valueOf(getRandomFloat());
      case INT: return Integer.valueOf(getRandomInt());
      case LONG: return Long.valueOf(getRandomLong());
      case STRING: return getRandomString();
      // TODO: Decimal
      // TODO: Timestamp
    }
    assert false;
    return null;
  }

  public static void main(String[] args) throws Exception {
    final int num_args = args.length;
    if (num_args < 5 || num_args > 6) {
      System.err.println(
          "Arguments: schema_file num_elements max_normal_str_len max_list_len " +
          "output_file [random_seed]");
      System.exit(1);
    }
    String schemaFile = args[0];
    int numElements = Integer.valueOf(args[1]);
    maxNormalStrLen = Integer.valueOf(args[2]);
    maxNumListItems = Integer.valueOf(args[3]);
    String outputFile = args[4];

    Optional<Long> seed;
    if (num_args > 5) {
      seed = Optional.of(Long.valueOf(args[5]));
    } else {
      seed = Optional.empty();
    }

    generateDataToFile(schemaFile, numElements, outputFile, seed);
  }
}
