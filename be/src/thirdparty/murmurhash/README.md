This library contains implementations for different variations of murmurhash. This is
basically a copy-paste from an external source: https://github.com/aappleby/smhasher.

I used the following commit for our snapshot in Impala:
https://github.com/aappleby/smhasher/tree/92cf3702fcfaadc84eb7bef59825a23e0cd84f56/

Note, the name of the include guard had to be changed to avoid collision with
Datasketches version of murmurhash algorithms in
be/src/thirdparty/datasketches/MurmurHash3.h
