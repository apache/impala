The content of this folder imports the functionality needed for CPC, HLL, KLL and
Theta approximate algorithms from Apache DataSketches by copying the necessary files
from that project into this folder. Note, that the original structure of files was
changed during this process as originally the following folders were affected:
  hll/include/
  cpc/include/
  theta/include/
  kll/include/
  common/include/
I copied the content of these folders into the same directory so that Impala
can compile them without rewriting the include paths in the files themselves.

The git branch of the snapshot I used as a source for the files: 3.0.0
The hash: 45885c0c8c0807bb9480886d60ca7042000a4c43

Browse the source files here:
https://github.com/apache/datasketches-cpp/tree/3.0.0