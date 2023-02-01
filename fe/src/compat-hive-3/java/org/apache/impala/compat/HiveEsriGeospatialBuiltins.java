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

package org.apache.impala.compat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.esri.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.impala.builtins.ST_ConvexHull_Wrapper;
import org.apache.impala.builtins.ST_LineString_Wrapper;
import org.apache.impala.builtins.ST_MultiPoint_Wrapper;
import org.apache.impala.builtins.ST_Polygon_Wrapper;
import org.apache.impala.builtins.ST_Union_Wrapper;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.hive.executor.BinaryToBinaryHiveLegacyFunctionExtractor;
import org.apache.impala.hive.executor.HiveJavaFunction;
import org.apache.impala.hive.executor.HiveLegacyJavaFunction;

import com.google.common.base.Preconditions;

import org.apache.impala.analysis.FunctionName;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveEsriGeospatialBuiltins {
  /**
   * Initializes Hive's ESRI geospatial UDFs as builtins.
   */
  public static void initBuiltins(Db db) {
    addLegacyUDFs(db);
    addGenericUDFs(db);
    addVarargsUDFs(db);
  }

  private static void addLegacyUDFs(Db db) {
    List<UDF> legacyUDFs = Arrays.asList(new ST_Area(), new ST_AsBinary(),
        new ST_AsGeoJson(), new ST_AsJson(), new ST_AsShape(), new ST_AsText(),
        new ST_Boundary(), new ST_Buffer(), new ST_Centroid(), new ST_CoordDim(),
        new ST_Difference(), new ST_Dimension(), new ST_Distance(), new ST_EndPoint(),
        new ST_Envelope(), new ST_EnvIntersects(), new ST_ExteriorRing(),
        new ST_GeodesicLengthWGS84(), new ST_GeomCollection(), new ST_GeometryN(),
        new ST_GeometryType(), new ST_GeomFromShape(), new ST_GeomFromText(),
        new ST_GeomFromWKB(), new ST_InteriorRingN(), new ST_Intersection(),
        new ST_Is3D(), new ST_IsClosed(), new ST_IsEmpty(), new ST_IsMeasured(),
        new ST_IsRing(), new ST_IsSimple(), new ST_Length(), new ST_LineFromWKB(),
        new ST_M(), new ST_MaxM(), new ST_MaxX(), new ST_MaxY(), new ST_MaxZ(),
        new ST_MinM(), new ST_MinX(), new ST_MinY(), new ST_MinZ(), new ST_MLineFromWKB(),
        new ST_MPointFromWKB(), new ST_MPolyFromWKB(), new ST_NumGeometries(),
        new ST_NumInteriorRing(), new ST_NumPoints(), new ST_Point(),
        new ST_PointFromWKB(), new ST_PointN(), new ST_PointZ(), new ST_PolyFromWKB(),
        new ST_Relate(), new ST_SRID(), new ST_StartPoint(), new ST_SymmetricDiff(),
        new ST_X(), new ST_Y(), new ST_Z(), new ST_SetSRID());

    for (UDF udf : legacyUDFs) {
      for (Function fn : extractFromLegacyHiveBuiltin(udf, db.getName())) {
        db.addBuiltin(fn);
      }
    }
  }

  private static void addGenericUDFs(Db db) {
    List<ScalarFunction> genericUDFs = new ArrayList<>();

    List<Set<Type>> stBinArguments =
        ImmutableList.of(ImmutableSet.of(Type.DOUBLE, Type.BIGINT),
            ImmutableSet.of(Type.STRING, Type.BINARY));
    List<Set<Type>> stBinEnvelopeArguments =
        ImmutableList.of(ImmutableSet.of(Type.DOUBLE, Type.BIGINT),
            ImmutableSet.of(Type.STRING, Type.BINARY, Type.BIGINT));

    genericUDFs.addAll(
        createMappedGenericUDFs(stBinArguments, Type.BIGINT, ST_Bin.class));
    genericUDFs.addAll(createMappedGenericUDFs(
        stBinEnvelopeArguments, Type.BINARY, ST_BinEnvelope.class));
    genericUDFs.add(createScalarFunction(
        ST_GeomFromGeoJson.class, Type.BINARY, new Type[] {Type.STRING}));
    genericUDFs.add(createScalarFunction(
        ST_GeomFromJson.class, Type.BINARY, new Type[] {Type.STRING}));
    genericUDFs.add(createScalarFunction(
        ST_MultiPolygon.class, Type.BINARY, new Type[] {Type.STRING}));
    genericUDFs.add(createScalarFunction(
        ST_MultiLineString.class, Type.BINARY, new Type[] {Type.STRING}));

    createRelationalGenericUDFs(genericUDFs);

    for (ScalarFunction function : genericUDFs) {
      db.addBuiltin(function);
    }
  }

  private static void createRelationalGenericUDFs(List<ScalarFunction> genericUDFs) {
    List<GenericUDF> relationalUDFs = Arrays.asList(new ST_Contains(), new ST_Crosses(),
        new ST_Disjoint(), new ST_Equals(), new ST_Intersects(), new ST_Overlaps(),
        new ST_Touches(), new ST_Within());

    List<Set<Type>> relationalUDFArguments =
        ImmutableList.of(ImmutableSet.of(Type.STRING, Type.BINARY),
            ImmutableSet.of(Type.STRING, Type.BINARY));

    for (GenericUDF relationalUDF : relationalUDFs) {
      genericUDFs.addAll(createMappedGenericUDFs(
          relationalUDFArguments, Type.BOOLEAN, relationalUDF.getClass()));
    }
  }

  private static void addVarargsUDFs(Db db) {
    List<ScalarFunction> varargsUDFs = new ArrayList<>();
    varargsUDFs.addAll(
        extractFunctions(ST_Union_Wrapper.class, ST_Union.class, db.getName()));
    varargsUDFs.addAll(
        extractFunctions(ST_Polygon_Wrapper.class, ST_Polygon.class, db.getName()));
    varargsUDFs.addAll(
        extractFunctions(ST_LineString_Wrapper.class, ST_LineString.class, db.getName()));
    varargsUDFs.addAll(
        extractFunctions(ST_MultiPoint_Wrapper.class, ST_MultiPoint.class, db.getName()));
    varargsUDFs.addAll(
        extractFunctions(ST_ConvexHull_Wrapper.class, ST_ConvexHull.class, db.getName()));

    for (ScalarFunction function : varargsUDFs) {
      db.addBuiltin(function);
    }
  }

  private static List<ScalarFunction> extractFromLegacyHiveBuiltin(
      UDF udf, String dbName) {
    return extractFunctions(udf.getClass(), udf.getClass(), dbName);
  }

  private static List<ScalarFunction> extractFunctions(
      Class<?> udfClass, Class<?> signatureClass, String dbName) {
    // The function has the same name as the signature class name
    String fnName = signatureClass.getSimpleName().toLowerCase();
    // The symbol name is coming from the UDF class which contains the functions
    String symbolName = udfClass.getName();
    org.apache.hadoop.hive.metastore.api.Function hiveFunction =
        HiveJavaFunction.createHiveFunction(fnName, dbName, symbolName, null);
    try {
      return new HiveLegacyJavaFunction(udfClass, hiveFunction, null, null)
          .extract(new BinaryToBinaryHiveLegacyFunctionExtractor());
    } catch (CatalogException ex) {
      // It is a fatal error if we fail to load a builtin function.
      Preconditions.checkState(false, ex.getMessage());
      return Collections.emptyList();
    }
  }

  private static ScalarFunction createScalarFunction(
      Class<?> udf, String name, Type returnType, Type[] arguments) {
    ScalarFunction function = new ScalarFunction(
        new FunctionName(BuiltinsDb.NAME, name), arguments, returnType, false);
    function.setSymbolName(udf.getName());
    function.setUserVisible(true);
    function.setHasVarArgs(false);
    function.setBinaryType(TFunctionBinaryType.JAVA);
    function.setIsPersistent(true);
    return function;
  }

  private static ScalarFunction createScalarFunction(
      Class<?> udf, Type returnType, Type[] arguments) {
    return createScalarFunction(
        udf, udf.getSimpleName().toLowerCase(), returnType, arguments);
  }

  private static List<ScalarFunction> createMappedGenericUDFs(
      List<Set<Type>> listOfArgumentOptions, Type returnType, Class<?> genericUDF) {
    return Sets.cartesianProduct(listOfArgumentOptions)
        .stream()
        .map(types -> {
          Type[] arguments = types.toArray(new Type[0]);
          return createScalarFunction(genericUDF, returnType, arguments);
        })
        .collect(Collectors.toList());
  }
}
