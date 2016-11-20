# Welcome to Impala

Lightning-fast, distributed [SQL](http://en.wikipedia.org/wiki/SQL) queries for petabytes
of data stored in Apache Hadoop clusters.

Impala is a modern, massively-distributed, massively-parallel, C++ query engine that lets
you analyze, transform and combine data from a variety of data sources:

* Best of breed performance and scalability.
* Support for data stored in [HDFS](https://hadoop.apache.org/),
  [Apache HBase](http://hbase.apache.org/) and [Amazon S3](http://aws.amazon.com/s3/).
* Wide analytic SQL support, including window functions and subqueries.
* On-the-fly code generation using [LLVM](http://llvm.org/) to generate CPU-efficient
  code tailored specifically to each individual query.
* Support for the most commonly-used Hadoop file formats, including the
  [Apache Parquet](https://parquet.incubator.apache.org/) (incubating) project.
* Apache-licensed, 100% open source.

## More about Impala

To learn more about Impala as a business user, or to try Impala live or in a VM, please
visit the [Impala homepage](https://impala.apache.org).

If you are interested in contributing to Impala as a developer, or learning more about
Impala's internals and architecture, visit the
[Impala wiki](https://cwiki.apache.org/confluence/display/IMPALA/Impala+Home).

## Supported Platforms

Impala only supports Linux at the moment.

## Build Instructions

See bin/bootstrap_build.sh.

## Export Control Notice

This distribution uses cryptographic software and may be subject to export controls.
Please refer to EXPORT\_CONTROL.md for more information.
