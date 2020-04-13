# Welcome to Impala

Lightning-fast, distributed [SQL](https://en.wikipedia.org/wiki/SQL) queries for petabytes
of data stored in Apache Hadoop clusters.

Impala is a modern, massively-distributed, massively-parallel, C++ query engine that lets
you analyze, transform and combine data from a variety of data sources:

* Best of breed performance and scalability.
* Support for data stored in [HDFS](https://hadoop.apache.org/),
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

To learn more about Impala as a business user, or to try Impala live or in a VM, please
visit the [Impala homepage](https://impala.apache.org). Detailed documentation for
administrators and users is available at
[Apache Impala documentation](https://impala.apache.org/impala-docs.html).

If you are interested in contributing to Impala as a developer, or learning more about
Impala's internals and architecture, visit the
[Impala wiki](https://cwiki.apache.org/confluence/display/IMPALA/Impala+Home).

## Supported Platforms

Impala only supports Linux at the moment.

## Export Control Notice

This distribution uses cryptographic software and may be subject to export controls.
Please refer to EXPORT\_CONTROL.md for more information.

## Build Instructions

See [Impala's developer documentation](https://cwiki.apache.org/confluence/display/IMPALA/Impala+Home)
to get started.

[Detailed build notes](README-build.md) has some detailed information on the project
layout and build.
