<!--
.. title: Impala learned some new tricks while living on Iceberg
.. slug: impala-learned-some-new-tricks-while-living-on-iceberg
.. date: 2023-10-09 08:00:00 UTC-03:00
.. tags: ccna23
.. category: talks
.. link:
.. description:
.. type: text
.. author: Zoltán Borók-Nagy
-->

Apache Impala is a high-performance distributed query engine
specifically designed for lightning fast read operations. Apache Iceberg
is a groundbreaking table format that introduces advanced features such
as partition and schema evolution, data partitioning through transform
functions, time-travel capabilities, and row-level deletes. Recognizing
the significance of Iceberg, the Impala team has invested tremendous
development efforts to provide comprehensive support for its features.
This talk will cover the following key points:

* A brief overview of how Impala integrates with Iceberg
* The limitations of traditional table formats
* How Iceberg addresses these existing challenges and unlocks new possibilities
* An exploration of Impala’s new features and some insights into their implementation

Iceberg has transformed Impala from being solely a query engine
optimized for read operations into a robust data warehouse engine. With
Iceberg, Impala now extends its support to include ACID write operations
and table maintenance functions, enabling it to fulfill the role of a
comprehensive and versatile data warehouse engine. Join us for this
session to gain valuable insights into the integration between Impala
and Iceberg, and discover the new functionalities of Impala.

[[slides](https://impala.apache.org/gh-docs/mon_bigdata_impala_learned_some_new_tricks_while_living_on_iceberg-zoltan-borok-nagy.pdf)]

_Appeared in <https://communityovercode.org/past-sessions/community-over-code-na-2023/>_
