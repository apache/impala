The content of this folder imports the functionality needed for HLL approximate
algorithm from Apache DataSketches by copying the necessary files from that
project into this folder. Note, that the original structure of files was
changed during this process as originally hll/ and common/ libraries were
both affected but I copied these into the same directory so that Impala can
compile them without rewriting the include paths in the files themselves. Also
note, that not the whole common/ directory was copied just the files needed for
HLL.

The git hash of the snapshot I used as a source for the files:
a6265b307a03085abe26c20413fdbf7d7a5eaf29

Browse the source files here:
https://github.com/apache/incubator-datasketches-cpp

