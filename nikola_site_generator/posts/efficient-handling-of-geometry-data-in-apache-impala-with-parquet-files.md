<!--
.. title: Efficient handling of geometry data in Apache Impala with Parquet files
.. slug: efficient-handling-of-geometry-data-in-apache-impala-with-parquet-files
.. date: 2023-10-10 08:00:00 UTC-03:00
.. tags: ccna23
.. category: talks
.. link:
.. description:
.. type: text
.. author: Csaba Ringhofer, Daniel Becker
-->

Apache Impala, a distributed massively parallel analytic query engine written in C++ and Java, has
recently been extended with experimental geospatial support. Although this project is still in its
early stages, we have already gained useful insights into how we can achieve significant performance
improvements in geometry-related queries.

Even without purpose-built geospatial indices, the existing features of Apache Parquet (one of the
most widely used open source column-oriented file formats in Big Data) and partitioning allow the
vast majority of the data to be discarded in queries that filter for a bounding rectangle.

An advantage of relying on existing file and table format features (such as page indices, dictionary
encoding etc.) is that it is independent of database engines and does not require them to be aware
of geospatial concepts. Therefore also Apache Hive, Spark etc. could benefit from this approach.

Our method is based on a two-level division of space into cells. The coarser division is used for
table partitioning and the more fine-grained one for sorting the data within a partition. This can
work with Hive Metastore partitions or the Apache Iceberg table format – the latter further enhances
the efficiency and convenience of these optimisations. This scheme seems to be suitably flexible for
unevenly distributed real-world data and it provides enough filtering capability without causing
issues like inefficient compression or the small file problem.

[[slides](https://impala.apache.org/gh-docs/tue_geospatial_ringhofer-becker-daniel-becker.pdf)]

_Appeared in <https://communityovercode.org/past-sessions/community-over-code-na-2023/>_
