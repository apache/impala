## What is impala-gdb.py?

This is a collection of Python GDB functions (macros) that can be invoked from a GDB
session to aid analysis of Impala core dumps.

To use the functions, source impala-gdb.py as follows:

```

(gdb) source $IMPALA_HOME/lib/python/impala_py_lib/gdb/impala-gdb.py

```

Currently, the following functions are available:

```
1. find-query-ids
2. find-fragment-instances
```

Here's a sample interaction:

```
(gdb) help find-query-ids
Find IDs of all queries this impalad is currently executing.
(gdb) find-query-ids
f74c863dff66a34d:1d983cc300000000
364525e12495932b:73f5dd0200000000
bc4a3eec25481981:edda04b800000000
(gdb)
(gdb)
(gdb) help find-fragment-instances
Find all query fragment instance to thread Id mappings in this impalad.
(gdb) find-fragment-instances
Fragment Instance Id  Thread IDs

364525e12495932b:73f5dd02000000a2 [69]
364525e12495932b:73f5dd0200000171 [196, 136]
bc4a3eec25481981:edda04b8000001a8 [252, 237, 206]
f74c863dff66a34d:1d983cc30000009b [200, 14, 13, 12, 6, 5, 3, 2]
f74c863dff66a34d:1d983cc30000013a [4]
(gdb)
```
