git clean -Xdf

cmake -DCMAKE_BUILD_TYPE=Debug .
make clean && make
