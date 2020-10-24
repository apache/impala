The content of this folder imports the functionality needed for HLL CPC and KLL
approximate algorithms from Apache DataSketches by copying the necessary files
from that project into this folder. Note, that the original structure of files was
changed during this process as originally the following folders were affected:
  hll/include/
  cpc/include/
  kll/include/
  common/include/
I copied the content of these folders into the same directory so that Impala
can compile them without rewriting the include paths in the files themselves.

The git branch of the snapshot I used as a source for the files:
The hash: 2b84e213067b681b696ec883d245ddf911790ff2

Browse the source files here:
https://github.com/apache/incubator-datasketches-cpp/tree/2b84e213067b681b696ec883d245ddf911790ff2
