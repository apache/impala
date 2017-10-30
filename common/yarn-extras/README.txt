Extra Hadoop classes from Yarn needed by Impala.

This is necessary because Impala has an admission controller that is configured using the
same configuration as Yarn (i.e. a fair-scheduler.xml). Some Yarn classes are used to
provide user to pool resolution, authorization, and accessing pool configurations.
