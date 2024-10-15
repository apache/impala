<!--
.. title: Healing Iceberg Tables with Impala
.. slug: healing-iceberg-tables-with-impala
.. date: 2024-10-10 10:55:00 UTC-06:00
.. tags: ccna24
.. category: talks
.. link:
.. description:
.. type: text
.. author: Noémi Pap-Takács
-->

Apache Iceberg handles dynamically changing data at large scale. However, frequent modifications
come at a cost: eventually, tables will become fragmented. This degrades the performance of read
operations over time. To address this challenge, we introduced table maintenance features in Apache
Impala, the high performance, distributed DB engine for big data.

The new OPTIMIZE statement merges small data files and eliminates delete files to uphold table
health. It allows rewriting the table according to the latest schema and partition layout, and also
offers the flexibility of file filtering to optimize recurring maintenance jobs. Additionally, the
DROP PARTITION statement allows selective partition removal based on predicates.

Discover in this session how Impala ensures high performance on top of dynamically changing data.

[[slides](https://impala.apache.org/gh-docs/ccna24-table_maintenance.pdf)]

_Appeared in [Community Over Code NA 2024](https://communityovercode.org/schedule/#sz-tab-45575)_
