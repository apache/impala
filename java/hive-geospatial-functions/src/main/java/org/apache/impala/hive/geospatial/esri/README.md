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
