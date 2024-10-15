<!--
.. title: Intelligent Utilization Aware Autoscaling for Impala Virtual Compute Clusters
.. slug: intelligent-utilization-aware-autoscaling-for-impala-virtual-compute-clusters
.. date: 2024-10-08 14:50:00 UTC-06:00
.. tags: ccna24
.. category: talks
.. link:
.. description:
.. type: text
.. author: Riza Suminto
-->

Sizing Virtual compute clusters for diverse and complex workloads is hard. Queries often require
large clusters to meet SLA requirements which can lead to low utilization and excessive cloud spend.
To solve this problem, we present intelligent autoscaling for Impala virtual warehouses. This
powerful feature dynamically analyzes query execution plans and resource requirements and adjusts
cluster size to follow the workload. Once cluster size limits have been set, the autoscaler works in
the background to maintain the ideal cluster size and is transparent to users. Observability UIs
have also been enhanced to help understand autoscaler behavior and further tune workloads.

We achieved 2 key goals with the design - delivering better ROI for compute cost and ease of use for
admins/users to manage Virtual Clusters.

[[slides](https://impala.apache.org/gh-docs/ccna24-intelligent_utilization_aware_scheduling.pdf)]

_Appeared in [Community Over Code NA 2024](https://communityovercode.org/schedule/#sz-tab-45573)_
