# Welcome to Impala

Lightning-fast, distributed [SQL](https://en.wikipedia.org/wiki/SQL) queries for petabytes
of data stored in open data and table formats.

Impala is a modern, massively-distributed, massively-parallel, C++ query engine that lets
you analyze, transform and combine data from a variety of data sources:

* Best of breed performance and scalability.
* Support for data stored in [Apache Iceberg](https://iceberg.apache.org/), [HDFS](https://hadoop.apache.org/),
  [Apache HBase](https://hbase.apache.org/), [Apache Kudu](https://kudu.apache.org/),
  [Amazon S3](https://aws.amazon.com/s3/),
  [Azure Data Lake Storage](https://azure.microsoft.com/en-us/services/storage/data-lake-storage/),
  [Apache Hadoop Ozone](https://hadoop.apache.org/ozone/) and more!
* Wide analytic SQL support, including window functions and subqueries.
* On-the-fly code generation using [LLVM](http://llvm.org/) to generate lightning-fast
  code tailored specifically to each individual query.
* Support for the most commonly-used Hadoop file formats, including
  [Apache Parquet](https://parquet.apache.org/) and [Apache ORC](https://orc.apache.org).
* Support for industry-standard security protocols, including Kerberos, LDAP and TLS.
* Apache-licensed, 100% open source.

## More about Impala

The fastest way to try out Impala is a [quickstart Docker container](
https://github.com/apache/impala/blob/master/docker/README.md#docker-quickstart-with-docker-compose).
You can try out running queries and processing data sets in Impala on a single machine
without installing dependencies. It can automatically load test data sets into Apache Kudu
and Apache Parquet formats and you can start playing around with Apache Impala SQL
within minutes.

To learn more about Impala as a user or administrator, or to try Impala, please
visit the [Impala homepage](https://impala.apache.org). Detailed documentation for
administrators and users is available at
[Apache Impala documentation](https://impala.apache.org/impala-docs.html).


If you are interested in contributing to Impala as a developer, or learning more about
Impala's internals and architecture, visit the
[Impala wiki](https://cwiki.apache.org/confluence/display/IMPALA/Impala+Home).

## Supported Platforms

Impala only supports Linux at the moment.
Impala supports x86_64 and has experimental support for arm64 (as of Impala 4.0).
[Impala Requirements](https://impala.apache.org/docs/build/html/topics/impala_prereqs.html)
contains more detailed information on the minimum CPU requirements.

## Supported OS Distributions

Impala runs on Linux systems only. The supported distros are

* Ubuntu 16.04/18.04
* CentOS/RHEL 7/8

Other systems, e.g. SLES12, may also be supported but are not tested by the community.

## Export Control Notice

This distribution uses cryptographic software and may be subject to export controls.
Please refer to EXPORT\_CONTROL.md for more information.

## Build Instructions

See [Impala's developer documentation](https://cwiki.apache.org/confluence/display/IMPALA/Impala+Home)
to get started.

[Detailed build notes](README-build.md) has some detailed information on the project
layout and build.
