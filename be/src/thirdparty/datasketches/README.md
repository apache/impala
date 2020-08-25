The content of this folder imports the functionality needed for HLL and KLL
approximate algorithms from Apache DataSketches by copying the necessary files
from that project into this folder. Note, that the original structure of files was
changed during this process as originally the following folders were affected:
  hll/include/
  kll/include/
  common/include/
I copied the content of these folders into the same directory so that Impala
can compile them without rewriting the include paths in the files themselves.

The git branch of the snapshot I used as a source for the files: 2.1.0-incubating
The hash: c1a6f8edb49699520f248d3d02019b87429b4241

Browse the source files here:
https://github.com/apache/incubator-datasketches-cpp/tree/2.1.0-incubating-rc1
