/*
   Copyright (c) 2008 - 2021, Ilan Schnell; All Rights Reserved
   bitarray is published under the PSF license.

   Author: Ilan Schnell
*/
#define BITARRAY_VERSION  "2.3.0"

/* .ob_size is buffer size (in bytes), not the number of elements.
   The number of elements (bits) is .nbits. */
typedef struct {
    PyObject_VAR_HEAD
    char *ob_item;              /* buffer */
    Py_ssize_t allocated;       /* allocated buffer size (in bytes) */
    Py_ssize_t nbits;           /* length of bitarray, i.e. elements */
    int endian;                 /* bit endianness of bitarray */
    int ob_exports;             /* how many buffer exports */
    PyObject *weakreflist;      /* list of weak references */
    Py_buffer *buffer;          /* used when importing a buffer */
    int readonly;               /* buffer is readonly */
} bitarrayobject;

/* --- bit endianness --- */
#define ENDIAN_LITTLE  0
#define ENDIAN_BIG     1

/* the endianness string */
#define ENDIAN_STR(endian)  ((endian) == ENDIAN_LITTLE ? "little" : "big")

/* number of bits that can be stored in given bytes */
#define BITS(bytes)  ((bytes) << 3)

/* number of bytes necessary to store given bits */
#define BYTES(bits)  (((bits) + 7) >> 3)

#define BITMASK(endian, i)  \
    (((char) 1) << ((endian) == ENDIAN_LITTLE ? ((i) & 7) : (7 - ((i) & 7))))

/* assert that .nbits is in agreement with .ob_size */
#define assert_nbits(self)  assert(BYTES((self)->nbits) == Py_SIZE(self))

/* assert byte index is in range */
#define assert_byte_in_range(self, j)  \
    assert(self->ob_item && 0 <= (j) && (j) < Py_SIZE(self))

/* --------------- definitions not specific to bitarray ---------------- */

#ifdef STDC_HEADERS
#include <stddef.h>
#else  /* !STDC_HEADERS */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>      /* For size_t */
#endif /* HAVE_SYS_TYPES_H */
#endif /* !STDC_HEADERS */

/* Compatibility with Visual Studio 2013 and older which don't support
   the inline keyword in C (only in C++): use __inline instead.
   (copied from pythoncapi_compat.h) */
#if (defined(_MSC_VER) && _MSC_VER < 1900 \
     && !defined(__cplusplus) && !defined(inline))
#define inline __inline
#endif

/* --- definitions specific to Python --- */

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#define BYTES_SIZE_FMT  "y#"
#else
/* the Py_MIN and Py_MAX macros were introduced in Python 3.3 */
#define Py_MIN(x, y)  (((x) > (y)) ? (y) : (x))
#define Py_MAX(x, y)  (((x) > (y)) ? (x) : (y))
#define PySlice_GetIndicesEx(slice, len, start, stop, step, slicelength) \
    PySlice_GetIndicesEx(((PySliceObject *) slice),                      \
                         (len), (start), (stop), (step), (slicelength))
#define PyLong_FromLong  PyInt_FromLong
#define BYTES_SIZE_FMT  "s#"
#endif

/* ------------ low level access to bits in bitarrayobject ------------- */

static inline int
getbit(bitarrayobject *self, Py_ssize_t i)
{
    assert_nbits(self);
    assert_byte_in_range(self, i >> 3);
    assert(0 <= i && i < self->nbits);
    return (self->ob_item[i >> 3] & BITMASK(self->endian, i) ? 1 : 0);
}

static inline void
setbit(bitarrayobject *self, Py_ssize_t i, int vi)
{
    char *cp, mask;

    assert_nbits(self);
    assert_byte_in_range(self, i >> 3);
    assert(0 <= i && i < self->nbits);
    assert(self->readonly == 0);

    mask = BITMASK(self->endian, i);
    cp = self->ob_item + (i >> 3);
    if (vi)
        *cp |= mask;
    else
        *cp &= ~mask;
}

/* Return the (padded with zeros) last byte of the buffer.  When called with
   a bitarray whose number of bits is a multiple of 8, return a NUL byte. */
static inline char
zeroed_last_byte(bitarrayobject *self)
{
    const char mask_table[16] = {
        0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f,  /* little endian */
        0x00, 0x80, 0xc0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe,  /* big endian */
    };
    Py_ssize_t t = Py_SIZE(self) - 1;     /* index of last byte in buffer */
    int r = self->nbits % 8;      /* index into mask table (minus offset) */
    int be = self->endian == ENDIAN_BIG;  /* is big endian */

    if (r == 0)
        return 0x00;
    assert_nbits(self);
    assert_byte_in_range(self, t);
    return mask_table[r + 8 * be] & self->ob_item[t];
}

/* Unless the buffer is readonly, zero out pad bits.
   Always return the number of pad bits - leave self->nbits unchanged */
static inline int
setunused(bitarrayobject *self)
{
    int r = self->nbits % 8;

    if (r == 0)
        return 0;
    if (self->readonly == 0)
        self->ob_item[Py_SIZE(self) - 1] = zeroed_last_byte(self);
    return 8 - r;
}

static const unsigned char bitcount_lookup[256] = {
#define B2(n)  n, n + 1, n + 1, n + 2
#define B4(n)  B2(n), B2(n + 1), B2(n + 1), B2(n + 2)
#define B6(n)  B4(n), B4(n + 1), B4(n + 1), B4(n + 2)
    B6(0), B6(1), B6(1), B6(2)
#undef B2
#undef B4
#undef B6
};

/* normalize index (which may be negative), such that 0 <= i <= n */
static inline void
normalize_index(Py_ssize_t n, Py_ssize_t *i)
{
    if (*i < 0) {
        *i += n;
        if (*i < 0)
            *i = 0;
    }
    if (*i > n)
        *i = n;
}

/* Interpret a PyObject (usually PyLong or PyBool) as a bit, return 0 or 1.
   On error, return -1 and set error message. */
static inline int
pybit_as_int(PyObject *value)
{
    Py_ssize_t x;

    x = PyNumber_AsSsize_t(value, NULL);
    if (x == -1 && PyErr_Occurred())
        return -1;

    if (x < 0 || x > 1) {
        PyErr_Format(PyExc_ValueError, "bit must be 0 or 1, got %zd", x);
        return -1;
    }
    return (int) x;
}
