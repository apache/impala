<!--
.. title: Impalas living on Iceberg
.. slug: impalas-living-on-iceberg
.. date: 2024-10-07 16:00:00 UTC-06:00
.. tags: ccna24
.. category: talks
.. link:
.. description:
.. type: text
.. author: Gabor Kaszab
-->

Apache Impala is a horizontally scalable database engine renowned for its emphasis on query
efficiency. Over recent years, the Impala team has dedicated substantial effort to support Iceberg
tables. Impala can read, write, modify, and optimize Iceberg tables. Additionally, it facilitates
schema and partition evolution through DDL statements.

Attendees can anticipate a comprehensive overview of all Iceberg-related features within Impala,
along with insights into forthcoming developments expected this year. Given Impala's Java/C++ hybrid
architecture — where the frontend, responsible for analysis and planning, is Java-based while
backend executors are coded in C++ — the integration process encountered its own set of challenges.
This presentation will delve into some details of this integration work, shedding light on the
technical nuances.

[[slides](https://impala.apache.org/gh-docs/ccna24-impalas_living_on_iceberg.pdf)]

_Appeared in [Community Over Code NA 2024](https://communityovercode.org/schedule/#sz-tab-45572)_
