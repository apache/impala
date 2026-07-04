# ESRI Geospatial UDF Functions

These files are copied from Apache Hive's geospatial UDF library, which itself was
originally ported from the [Esri Spatial Framework for Hadoop](https://github.com/Esri/spatial-framework-for-hadoop).

## Provenance

- Original source: `com.esri.hadoop.hive` (Esri Spatial Framework for Hadoop)
- Intermediate: `org.apache.hadoop.hive.ql.udf.esri` (Apache Hive)
- This copy: `org.apache.impala.hive.geospatial.esri` (Apache Impala)

## Modifications from Hive

- Package renamed from `org.apache.hadoop.hive.ql.udf.esri`
- `serde/` and `shims/` subdirectories not copied (Hive-specific table SerDes, not
  needed for ST_ UDF execution)
- Deserialized geometry caching removed from `GeometryUtils` and `HiveGeometryOIHelper`.
  The original Hive caching (`geometryCache` / `bytesRecycled`) never worked in Impala
  due to the way Hive UDFs parameters are passed.
- `GeometryUtils.SerializationFormat` enum added; the active format is set once at
  startup via `HiveEsriGeospatialBuiltins.initBuiltins` and must be consistent across
  coordinators and executors.

## Serialization Modes

The active mode is controlled by the `--geospatial_library` impalad startup flag.

### HIVE_ESRI (default)

Geometries are serialized as ESRI Shape format: a 4-byte WKID (spatial reference ID)
followed by a 1-byte OGC type tag, followed by the ESRI binary shape payload.  Native
C++ implementations of selected ST_ functions are registered alongside the Java UDFs.
Relational functions (e.g. `ST_Intersects`) accept both `STRING` and `BINARY` arguments.

### WKB_EXPERIMENTAL

Geometries are serialized as OGC Well-Known Binary (WKB).  This mode is intended as
the foundation for future native C++ implementations using the WKB wire format.

Key behavioral differences from HIVE_ESRI:

1. **No native C++ functions** — all ST_ functions execute in Java.
2. **SRID is not stored** — WKB has no SRID field, so a serialize/deserialize roundtrip
   always yields SRID 0.  `ST_SRID` and functions that compare spatial references ignore
   the SRID silently.
3. **Higher-dimension geometry dropped** — geometries with Z/M coordinates are not
   supported.  This simplifies future C++ implementation and testing.
4. **Relational functions are BINARY-only** — `ST_Intersects`, `ST_Contains`, etc.
   are registered only for `(BINARY, BINARY)` argument types; the `STRING` overloads
   present in HIVE_ESRI mode are non-standard and are intentionally dropped.
   An alternative can be to allow implicit casting from STRING to GEOMETRY in the
   future that parses the STRING as WKT.
