<!--
.. title: Anatomy of reading Apache Parquet files (from the Apache Impala perspective)
.. slug: anatomy-of-reading-apache-parquet-files-from-the-apache-impala-perspective
.. date: 2024-07-27 07:02:43 UTC-07:00
.. tags:
.. category: talks
.. link:
.. description:
.. type: text
-->

Reading file formats efficiently is a crucial part of big data systems -
in selective scans data is often only big before hitting the first
filter and becomes manageable during the rest of the processing. The
talk describes this early stage of query execution in Apache Impala,
from reading the bytes of Parquet files on the filesystem to applying
predicates and runtime filters on individual rows.

Apache Impala is a distributed massively parallel analytic query engine
written in C++ and Java. It is optimized both for object stores (S3,
ABFS) and on-prem distributed file systems (HDFS, Ozone). Apache Parquet
is one of the most widely used open source column-oriented file formats
in Big Data.

Impala has its own Parquet scanner written in C++ instead of using
existing Parquet libraries like Parquet-mr or Parquet-cpp. This allows
tighter integration with IO and and memory management, enabling features
like:

* Data caching to memory and local drive
* Execution within memory bounds
* Efficient parallelism

These features all play an important role in giving Impala an edge in
the world of Big Data query engines.

_Appeared in <https://eu.communityovercode.org/sessions/2024/anatomy-parquet-files/>_
