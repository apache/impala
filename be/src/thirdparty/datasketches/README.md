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

The git branch of the snapshot I used as a source for the files:
The hash: b2f749ed5ce6ba650f4259602b133c310c3a5ee4

Browse the source files here:
https://github.com/apache/datasketches-cpp/tree/b2f749ed5ce6ba650f4259602b133c310c3a5ee4
