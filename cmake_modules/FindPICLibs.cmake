# In order to statically link in the Boost, bz2, event and z libraries, they
# needs to be recompiled with -fPIC. Set $PIC_LIB_PATH to the location of
# these libraries in the environment, or dynamic linking will be used instead.

IF (DEFINED ENV{PIC_LIB_PATH})
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_STATIC_RUNTIME ON)
    set(LIBBZ2 $ENV{PIC_LIB_PATH}/libbz2.a)
    set(LIBZ $ENV{PIC_LIB_PATH}/libz.a)
    set(LIBEVENT $ENV{PIC_LIB_PATH}/libevent.a)
ELSE (DEFINED ENV{PIC_LIB_PATH})
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_STATIC_RUNTIME OFF)
    set(LIBBZ2 -lbz2)
    set(LIBZ -lz)
    set(LIBEVENT event)
ENDIF (DEFINED ENV{PIC_LIB_PATH})

