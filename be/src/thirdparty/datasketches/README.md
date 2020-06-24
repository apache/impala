The content of this folder imports the functionality needed for HLL and KLL
approximate algorithms from Apache DataSketches by copying the necessary files
from that project into this folder. Note, that the original structure of files was
changed during this process as originally the following folders were affected:
  hll/include/
  kll/include/
  common/include/
I copied the content of these folders into the same directory so that Impala
can compile them without rewriting the include paths in the files themselves.

The git hash of the snapshot I used as a source for the files:
c67d92faad3827932ca3b5d864222e64977f2c20

Browse the source files here:
https://github.com/apache/incubator-datasketches-cpp

