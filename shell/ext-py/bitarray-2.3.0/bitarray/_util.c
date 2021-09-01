/*
   Copyright (c) 2019 - 2021, Ilan Schnell; All Rights Reserved
   bitarray is published under the PSF license.

   This file contains the C implementation of some useful utility functions.

   Author: Ilan Schnell
*/

#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "pythoncapi_compat.h"
#include "bitarray.h"

#define IS_LE(a)  ((a)->endian == ENDIAN_LITTLE)
#define IS_BE(a)  ((a)->endian == ENDIAN_BIG)

/* set using the Python module function _set_bato() */
static PyObject *bitarray_type_obj = NULL;

/* Return 0 if obj is bitarray.  If not, return -1 and set an exception. */
static int
ensure_bitarray(PyObject *obj)
{
    int t;

    if (bitarray_type_obj == NULL)
        Py_FatalError("bitarray_type_obj not set");
    t = PyObject_IsInstance(obj, bitarray_type_obj);
    if (t < 0)
        return -1;
    if (t == 0) {
        PyErr_Format(PyExc_TypeError, "bitarray expected, not %s",
                     Py_TYPE(obj)->tp_name);
        return -1;
    }
    return 0;
}

/* ensure object is a bitarray of given length */
static int
ensure_ba_of_length(PyObject *a, const Py_ssize_t n)
{
    if (ensure_bitarray(a) < 0)
        return -1;
    if (((bitarrayobject *) a)->nbits != n) {
        PyErr_SetString(PyExc_ValueError, "size mismatch");
        return -1;
    }
    return 0;
}

/* ------------------------------- count_n ----------------------------- */

/* return the smallest index i for which a.count(1, 0, i) == n, or when
   n exceeds the total count return -1  */
static Py_ssize_t
count_to_n(bitarrayobject *a, Py_ssize_t n)
{
    const Py_ssize_t nbits = a->nbits;
    Py_ssize_t i = 0;        /* index */
    Py_ssize_t j = 0;        /* total count up to index */
    Py_ssize_t block_start, block_stop, k, m;

    assert(0 <= n && n <= nbits);
    if (n == 0)
        return 0;

#define BLOCK_BITS  8192
    /* by counting big blocks we save comparisons */
    while (i + BLOCK_BITS < nbits) {
        m = 0;
        assert(i % 8 == 0);
        block_start = i >> 3;
        block_stop = block_start + (BLOCK_BITS >> 3);
        assert(block_stop <= Py_SIZE(a));
        for (k = block_start; k < block_stop; k++)
            m += bitcount_lookup[(unsigned char) a->ob_item[k]];
        if (j + m >= n)
            break;
        j += m;
        i += BLOCK_BITS;
    }
#undef BLOCK_BITS

    while (i + 8 < nbits) {
        k = i >> 3;
        assert(k < Py_SIZE(a));
        m = bitcount_lookup[(unsigned char) a->ob_item[k]];
        if (j + m >= n)
            break;
        j += m;
        i += 8;
    }

    while (j < n && i < nbits ) {
        j += getbit(a, i);
        i++;
    }
    if (j < n)
        return -1;

    return i;
}

static PyObject *
count_n(PyObject *module, PyObject *args)
{
    PyObject *a;
    Py_ssize_t n, i;

    if (!PyArg_ParseTuple(args, "On:count_n", &a, &n))
        return NULL;
    if (ensure_bitarray(a) < 0)
        return NULL;

    if (n < 0) {
        PyErr_SetString(PyExc_ValueError, "non-negative integer expected");
        return NULL;
    }
#define aa  ((bitarrayobject *) a)
    if (n > aa->nbits)  {
        PyErr_SetString(PyExc_ValueError, "n larger than bitarray size");
        return NULL;
    }
    i = count_to_n(aa, n);        /* do actual work here */
#undef aa
    if (i < 0) {
        PyErr_SetString(PyExc_ValueError, "n exceeds total count");
        return NULL;
    }
    return PyLong_FromSsize_t(i);
}

PyDoc_STRVAR(count_n_doc,
"count_n(a, n, /) -> int\n\
\n\
Return lowest index `i` for which `a[:i].count() == n`.\n\
Raises `ValueError`, when n exceeds total count (`a.count()`).");

/* ----------------------------- right index --------------------------- */

/* return index of highest occurrence of vi in self[a:b], -1 when not found */
static Py_ssize_t
find_last(bitarrayobject *self, int vi, Py_ssize_t a, Py_ssize_t b)
{
    const Py_ssize_t n = b - a;
    Py_ssize_t res, i;

    assert(0 <= a && a <= self->nbits);
    assert(0 <= b && b <= self->nbits);
    assert(0 <= vi && vi <= 1);
    if (n <= 0)
        return -1;

    /* the logic here is the same as in find_bit() in _bitarray.c */
#ifdef PY_UINT64_T
    if (n > 64) {
        const Py_ssize_t word_a = (a + 63) / 64;
        const Py_ssize_t word_b = b / 64;
        const PY_UINT64_T w = vi ? 0 : ~0;

        if ((res = find_last(self, vi, 64 * word_b, b)) >= 0)
            return res;

        for (i = word_b - 1; i >= word_a; i--) {  /* skip uint64 words */
            if (w ^ ((PY_UINT64_T *) self->ob_item)[i])
                return find_last(self, vi, 64 * i, 64 * i + 64);
        }
        return find_last(self, vi, a, 64 * word_a);
    }
#endif
    if (n > 8) {
        const Py_ssize_t byte_a = BYTES(a);
        const Py_ssize_t byte_b = b / 8;
        const char c = vi ? 0 : ~0;

        if ((res = find_last(self, vi, BITS(byte_b), b)) >= 0)
            return res;

        for (i = byte_b - 1; i >= byte_a; i--) {  /* skip bytes */
            assert_byte_in_range(self, i);
            if (c ^ self->ob_item[i])
                return find_last(self, vi, BITS(i), BITS(i) + 8);
        }
        return find_last(self, vi, a, BITS(byte_a));
    }
    assert(n <= 8);
    for (i = b - 1; i >= a; i--) {
        if (getbit(self, i) == vi)
            return i;
    }
    return -1;
}

static PyObject *
r_index(PyObject *module, PyObject *args)
{
    PyObject *value = Py_True, *a;
    Py_ssize_t start = 0, stop = PY_SSIZE_T_MAX, res;
    int vi;

    if (!PyArg_ParseTuple(args, "O|Onn:rindex", &a, &value, &start, &stop))
        return NULL;
    if (ensure_bitarray(a) < 0)
        return NULL;
    if ((vi = pybit_as_int(value)) < 0)
        return NULL;

#define aa  ((bitarrayobject *) a)
    normalize_index(aa->nbits, &start);
    normalize_index(aa->nbits, &stop);
    res = find_last(aa, vi, start, stop);
#undef aa
    if (res < 0)
        return PyErr_Format(PyExc_ValueError, "%d not in bitarray", vi);

    return PyLong_FromSsize_t(res);
}

PyDoc_STRVAR(rindex_doc,
"rindex(bitarray, value=1, start=0, stop=<end of array>, /) -> int\n\
\n\
Return the rightmost (highest) index of `value` in bitarray.\n\
Raises `ValueError` if the value is not present.");

/* --------------------------- unary functions ------------------------- */

static PyObject *
parity(PyObject *module, PyObject *a)
{
    Py_ssize_t i;
    unsigned char par = 0;

    if (ensure_bitarray(a) < 0)
        return NULL;

#define aa  ((bitarrayobject *) a)
    for (i = 0; i < aa->nbits / 8; i++)
        par ^= aa->ob_item[i];
    if (aa->nbits % 8)
        par ^= zeroed_last_byte(aa);
#undef aa

    return PyLong_FromLong((long) bitcount_lookup[par] % 2);
}

PyDoc_STRVAR(parity_doc,
"parity(a, /) -> int\n\
\n\
Return the parity of bitarray `a`.\n\
This is equivalent to `a.count() % 2` (but more efficient).");

/* --------------------------- binary functions ------------------------ */

enum kernel_type {
    KERN_cand,     /* count bitwise and -> int */
    KERN_cor,      /* count bitwise or -> int */
    KERN_cxor,     /* count bitwise xor -> int */
    KERN_subset,   /* is subset -> bool */
};

static PyObject *
binary_function(PyObject *args, enum kernel_type kern, const char *format)
{
    Py_ssize_t res = 0, s, i;
    PyObject *a, *b;
    unsigned char c;
    int r;

    if (!PyArg_ParseTuple(args, format, &a, &b))
        return NULL;
    if (ensure_bitarray(a) < 0 || ensure_bitarray(b) < 0)
        return NULL;

#define aa  ((bitarrayobject *) a)
#define bb  ((bitarrayobject *) b)
    if (aa->nbits != bb->nbits) {
        PyErr_SetString(PyExc_ValueError,
                        "bitarrays of equal length expected");
        return NULL;
    }
    if (aa->endian != bb->endian) {
        PyErr_SetString(PyExc_ValueError,
                        "bitarrays of equal endianness expected");
        return NULL;
    }
    s = aa->nbits / 8;       /* number of whole bytes in buffer */
    r = aa->nbits % 8;       /* remaining bits  */

    switch (kern) {
    case KERN_cand:
        for (i = 0; i < s; i++) {
            c = aa->ob_item[i] & bb->ob_item[i];
            res += bitcount_lookup[c];
        }
        if (r) {
            c = zeroed_last_byte(aa) & zeroed_last_byte(bb);
            res += bitcount_lookup[c];
        }
        break;

    case KERN_cor:
        for (i = 0; i < s; i++) {
            c = aa->ob_item[i] | bb->ob_item[i];
            res += bitcount_lookup[c];
        }
        if (r) {
            c = zeroed_last_byte(aa) | zeroed_last_byte(bb);
            res += bitcount_lookup[c];
        }
        break;

    case KERN_cxor:
        for (i = 0; i < s; i++) {
            c = aa->ob_item[i] ^ bb->ob_item[i];
            res += bitcount_lookup[c];
        }
        if (r) {
            c = zeroed_last_byte(aa) ^ zeroed_last_byte(bb);
            res += bitcount_lookup[c];
        }
        break;

    case KERN_subset:
        for (i = 0; i < s; i++) {
            if ((aa->ob_item[i] & bb->ob_item[i]) != aa->ob_item[i])
                Py_RETURN_FALSE;
        }
        if (r) {
            if ((zeroed_last_byte(aa) & zeroed_last_byte(bb)) !=
                 zeroed_last_byte(aa))
                Py_RETURN_FALSE;
        }
        Py_RETURN_TRUE;

    default:  /* cannot happen */
        return NULL;
    }
#undef aa
#undef bb
    return PyLong_FromSsize_t(res);
}

#define COUNT_FUNC(oper, ochar)                                         \
static PyObject *                                                       \
count_ ## oper (bitarrayobject *module, PyObject *args)                 \
{                                                                       \
    return binary_function(args, KERN_c ## oper, "OO:count_" #oper);    \
}                                                                       \
PyDoc_STRVAR(count_ ## oper ## _doc,                                    \
"count_" #oper "(a, b, /) -> int\n\
\n\
Return `(a " ochar " b).count()` in a memory efficient manner,\n\
as no intermediate bitarray object gets created.")

COUNT_FUNC(and, "&");           /* count_and */
COUNT_FUNC(or,  "|");           /* count_or  */
COUNT_FUNC(xor, "^");           /* count_xor */


static PyObject *
subset(PyObject *module, PyObject *args)
{
    return binary_function(args, KERN_subset, "OO:subset");
}

PyDoc_STRVAR(subset_doc,
"subset(a, b, /) -> bool\n\
\n\
Return `True` if bitarray `a` is a subset of bitarray `b`.\n\
`subset(a, b)` is equivalent to `(a & b).count() == a.count()` but is more\n\
efficient since we can stop as soon as one mismatch is found, and no\n\
intermediate bitarray object gets created.");

/* ---------------------------- serialization -------------------------- */

static PyObject *
serialize(PyObject *module, PyObject *a)
{
    PyObject *result;
    Py_ssize_t nbytes;
    char *str;

    if (ensure_bitarray(a) < 0)
        return NULL;

    nbytes = Py_SIZE(a);
    if ((result = PyBytes_FromStringAndSize(NULL, nbytes + 1)) == NULL)
        return PyErr_NoMemory();

    str = PyBytes_AsString(result);
#define aa  ((bitarrayobject *) a)
    *str = (char) (16 * IS_BE(aa) + setunused(aa));
    memcpy(str + 1, aa->ob_item, (size_t) nbytes);
#undef aa
    return result;
}

PyDoc_STRVAR(serialize_doc,
"serialize(bitarray, /) -> bytes\n\
\n\
Return a serialized representation of the bitarray, which may be passed to\n\
`deserialize()`.  It efficiently represents the bitarray object (including\n\
its endianness) and is guaranteed not to change in future releases.");

/* ----------------------------- hexadecimal --------------------------- */

static const char hexdigits[] = "0123456789abcdef";

static int
hex_to_int(char c)
{
    if ('0' <= c && c <= '9')
        return c - '0';
    if ('a' <= c && c <= 'f')
        return c - 'a' + 10;
    if ('A' <= c && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

static PyObject *
ba2hex(PyObject *module, PyObject *a)
{
    PyObject *result;
    size_t i, strsize;
    char *str;
    int le, be;

    if (ensure_bitarray(a) < 0)
        return NULL;

#define aa  ((bitarrayobject *) a)
    if (aa->nbits % 4) {
        PyErr_SetString(PyExc_ValueError, "bitarray length not multiple of 4");
        return NULL;
    }

    /* strsize = aa->nbits / 4;  could make strsize odd */
    strsize = 2 * Py_SIZE(a);
    str = (char *) PyMem_Malloc(strsize);
    if (str == NULL)
        return PyErr_NoMemory();

    le = IS_LE(aa);
    be = IS_BE(aa);
    for (i = 0; i < strsize; i += 2) {
        unsigned char c = aa->ob_item[i / 2];
        str[i + le] = hexdigits[c >> 4];
        str[i + be] = hexdigits[0x0f & c];
    }
    result = Py_BuildValue("s#", str, aa->nbits / 4);
#undef aa
    PyMem_Free((void *) str);
    return result;
}

PyDoc_STRVAR(ba2hex_doc,
"ba2hex(bitarray, /) -> hexstr\n\
\n\
Return a string containing the hexadecimal representation of\n\
the bitarray (which has to be multiple of 4 in length).");


/* Translate hexadecimal digits into the bitarray's buffer.
   Each digit corresponds to 4 bits in the bitarray.
   The number of digits may be odd. */
static PyObject *
hex2ba(PyObject *module, PyObject *args)
{
    PyObject *a;
    char *str;
    Py_ssize_t i, strsize;
    int le, be, x, y;

    if (!PyArg_ParseTuple(args, "Os#", &a, &str, &strsize))
        return NULL;
    if (ensure_ba_of_length(a, 4 * strsize) < 0)
        return NULL;

#define aa  ((bitarrayobject *) a)
    le = IS_LE(aa);
    be = IS_BE(aa);
    assert(le + be == 1 && str[strsize] == 0);
    for (i = 0; i < strsize; i += 2) {
        x = hex_to_int(str[i + le]);
        y = hex_to_int(str[i + be]);
        if (x < 0 || y < 0) {
            /* ignore the terminating NUL - happends when strsize is odd */
            if (i + le == strsize) /* str[i+le] is NUL */
                x = 0;
            if (i + be == strsize) /* str[i+be] is NUL */
                y = 0;
            /* there is an invalid byte - or (non-terminating) NUL */
            if (x < 0 || y < 0) {
                PyErr_SetString(PyExc_ValueError,
                                "Non-hexadecimal digit found");
                return NULL;
            }
        }
        assert(x < 16 && y < 16);
        aa->ob_item[i / 2] = x << 4 | y;
    }
#undef aa
    Py_RETURN_NONE;
}

/* ----------------------- base 2, 4, 8, 16, 32, 64 -------------------- */

/* RFC 4648 Base32 alphabet */
static const char base32_alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/* standard base 64 alphabet */
static const char base64_alphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int
digit_to_int(char c, int n)
{
    if (n <= 16) {              /* base 2, 4, 8, 16 */
        int i = hex_to_int(c);
        if (0 <= i && i < n)
            return i;
    }
    if (n == 32) {              /* base 32 */
        if ('A' <= c && c <= 'Z')
            return c - 'A';
        if ('2' <= c && c <= '7')
            return c - '2' + 26;
    }
    if (n == 64) {              /* base 64 */
        if ('A' <= c && c <= 'Z')
            return c - 'A';
        if ('a' <= c && c <= 'z')
            return c - 'a' + 26;
        if ('0' <= c && c <= '9')
            return c - '0' + 52;
        if (c == '+')
            return 62;
        if (c == '/')
            return 63;
    }
    return -1;
}

/* return m = log2(n) for m = 1..6 */
static int
base_to_length(int n)
{
    int m;

    for (m = 1; m < 7; m++) {
        if (n == (1 << m))
            return m;
    }
    PyErr_SetString(PyExc_ValueError, "base must be 2, 4, 8, 16, 32 or 64");
    return -1;
}

static PyObject *
ba2base(PyObject *module, PyObject *args)
{
    const char *alphabet;
    PyObject *result, *a;
    size_t i, strsize;
    char *str;
    int n, m, x, k, le;

    if (!PyArg_ParseTuple(args, "iO:ba2base", &n, &a))
        return NULL;
    if ((m = base_to_length(n)) < 0)
        return NULL;
    if (ensure_bitarray(a) < 0)
        return NULL;

    switch (n) {
    case 32: alphabet = base32_alphabet; break;
    case 64: alphabet = base64_alphabet; break;
    default: alphabet = hexdigits;
    }

#define aa  ((bitarrayobject *) a)
    if (aa->nbits % m)
        return PyErr_Format(PyExc_ValueError,
                            "bitarray length must be multiple of %d", m);

    strsize = aa->nbits / m;
    if ((str = (char *) PyMem_Malloc(strsize)) == NULL)
        return PyErr_NoMemory();

    le = IS_LE(aa);
    for (i = 0; i < strsize; i++) {
        x = 0;
        for (k = 0; k < m; k++)
            x |= getbit(aa, m * i + (le ? k : (m - k - 1))) << k;
        str[i] = alphabet[x];
    }
    result = Py_BuildValue("s#", str, strsize);
#undef aa
    PyMem_Free((void *) str);
    return result;
}

PyDoc_STRVAR(ba2base_doc,
"ba2base(n, bitarray, /) -> str\n\
\n\
Return a string containing the base `n` ASCII representation of\n\
the bitarray.  Allowed values for `n` are 2, 4, 8, 16, 32 and 64.\n\
The bitarray has to be multiple of length 1, 2, 3, 4, 5 or 6 respectively.\n\
For `n=16` (hexadecimal), `ba2hex()` will be much faster, as `ba2base()`\n\
does not take advantage of byte level operations.\n\
For `n=32` the RFC 4648 Base32 alphabet is used, and for `n=64` the\n\
standard base 64 alphabet is used.");


/* Translate ASCII digits into the bitarray's buffer.
   The (Python) arguments to this functions are:
   - base n, one of 2, 4, 8, 16, 32, 64  (n=2^m   where m bits per digit)
   - bitarray (of length m * len(s)) whose buffer is written into
   - byte object s containing the ASCII digits
*/
static PyObject *
base2ba(PyObject *module, PyObject *args)
{
    PyObject *a;
    Py_ssize_t i, strsize;
    char *str;
    int n, m, d, k, le;

    if (!PyArg_ParseTuple(args, "iOs#", &n, &a, &str, &strsize))
        return NULL;
    if ((m = base_to_length(n)) < 0)
        return NULL;
    if (ensure_ba_of_length(a, m * strsize) < 0)
        return NULL;

#define aa  ((bitarrayobject *) a)
    memset(aa->ob_item, 0x00, (size_t) Py_SIZE(a));

    le = IS_LE(aa);
    for (i = 0; i < strsize; i++) {
        d = digit_to_int(str[i], n);
        if (d < 0) {
            PyErr_SetString(PyExc_ValueError, "invalid digit found");
            return NULL;
        }
        for (k = 0; k < m; k++)
            setbit(aa, m * i + (le ? k : (m - k - 1)), d & (1 << k));
    }
#undef aa
    Py_RETURN_NONE;
}

/* ------------------- variable length bitarray format ----------------- */

/* PADBITS is always 3 - the number of bits that represent the number of
   padding bits.  The actual number of padding bits is called 'padding'
   below, and is in range(0, 7).
   Also note that 'padding' refers to the pad bits within the variable
   length format, which is not the same as the pad bits of the actual
   bitarray.  For example, b'\x10' has padding = 1, and decodes to
   bitarray('000'), which has 5 pad bits. */
#define PADBITS  3

/* Consume iterator while decoding bytes into bitarray.
   As we don't have access to bitarrays resize() C function, we give this
   function a bitarray (large enough in many cases).  We manipulate .nbits
   and .ob_size (using Py_SET_SIZE) directly without having to call resize().
   Whenever we need a larger bitarray, we call .frombytes() with a multiple
   of 7 dummy bytes, such that the added bytes are aligned for the next time
   we call .frombytes() (to avoid expensive bit shifts).
   We drop the over-allocated bitarray on the Python side after this function
   is called.
*/
static PyObject *
vl_decode(PyObject *module, PyObject *args)
{
    const Py_ssize_t ibits = 256;    /* initial number of bits in a */
    PyObject *iter, *item, *res, *a;
    Py_ssize_t padding = 0;  /* number of pad bits read from header byte */
    Py_ssize_t i = 0;        /* bit counter */
    unsigned char b = 0x80;  /* empty stream will raise StopIteration */
    Py_ssize_t k;

    /* Ensure that bits will be aligned when gowing memory below.
       Possible values for ibits are: 32, 88, 144, 200, 256, 312, ... */
    assert((ibits + PADBITS) % 7 == 0 && ibits % 8 == 0);

    if (!PyArg_ParseTuple(args, "OO", &iter, &a))
        return NULL;
    if (!PyIter_Check(iter))
        return PyErr_Format(PyExc_TypeError, "iterator or bytes expected, "
                            "got '%s'", Py_TYPE(iter)->tp_name);
    if (ensure_ba_of_length(a, ibits) < 0)
        return NULL;

#define aa  ((bitarrayobject *) a)
    while ((item = PyIter_Next(iter))) {
#ifdef IS_PY3K
        if (PyLong_Check(item))
            b = (unsigned char) PyLong_AsLong(item);
#else
        if (PyBytes_Check(item))
            b = (unsigned char) *PyBytes_AS_STRING(item);
#endif
        else {
            PyErr_Format(PyExc_TypeError, "int (byte) iterator expected, "
                         "got '%s' element", Py_TYPE(item)->tp_name);
            Py_DECREF(item);
            return NULL;
        }
        Py_DECREF(item);

        assert(i == 0 || (i + PADBITS) % 7 == 0);
        if (i == aa->nbits) {
            /* grow memory - see above */
            assert(i % 8 == 0);  /* added dummy bytes are aligned */
            /* 63 is a multiple of 7 - bytes will be aligned for next call */
            res = PyObject_CallMethod(a, "frombytes", BYTES_SIZE_FMT,
                                      base64_alphabet, (Py_ssize_t) 63);
            if (res == NULL)
                return NULL;
            Py_DECREF(res);  /* drop frombytes result */
        }
        assert(i + 6 < aa->nbits);

        if (i == 0) {
            padding = (b & 0x70) >> 4;
            if (padding >= 7 || ((b & 0x80) == 0 && padding > 4))
                return PyErr_Format(PyExc_ValueError,
                                    "invalid header byte: 0x%02x", b);
            for (k = 0; k < 4; k++)
                setbit(aa, i++, (0x08 >> k) & b);
        }
        else {
            for (k = 0; k < 7; k++)
                setbit(aa, i++, (0x40 >> k) & b);
        }
        if ((b & 0x80) == 0)
            break;
    }
    /* set final length of bitarray */
    aa->nbits = i - padding;
    Py_SET_SIZE(a, BYTES(aa->nbits));
    assert_nbits(aa);
#undef aa

    if (PyErr_Occurred())       /* from PyIter_Next() */
        return NULL;

    if (b & 0x80) {
        k = (i + PADBITS) / 7;
        return PyErr_Format(PyExc_StopIteration,
                            "no terminating byte found, bytes read: %zd", k);
    }
    Py_RETURN_NONE;
}

static PyObject *
vl_encode(PyObject *module, PyObject *a)
{
    PyObject *result;
    Py_ssize_t padding, n, m, i, k;
    Py_ssize_t j = 0;           /* byte conter */
    char *str;

    if (ensure_bitarray(a) < 0)
        return NULL;

#define aa  ((bitarrayobject *) a)
    n = (aa->nbits + PADBITS + 6) / 7;  /* number of resulting bytes */
    m = 7 * n - PADBITS;      /* number of bits resulting bytes can hold */
    padding = m - aa->nbits;  /* number of pad bits */
    assert(0 <= padding && padding < 7);

    if ((result = PyBytes_FromStringAndSize(NULL, n)) == NULL)
        return PyErr_NoMemory();

    str = PyBytes_AsString(result);
    str[0] = aa->nbits > 4 ? 0x80 : 0x00;  /* leading bit */
    str[0] |= padding << 4;                /* encode padding */
    for (i = 0; i < 4 && i < aa->nbits; i++)
        str[0] |= (0x08 >> i) * getbit(aa, i);

    for (i = 4; i < aa->nbits; i++) {
        k = (i - 4) % 7;
        if (k == 0) {
            j++;
            str[j] = j < n - 1 ? 0x80 : 0x00;  /* leading bit */
        }
        str[j] |= (0x40 >> k) * getbit(aa, i);
    }
#undef aa
    assert(j == n - 1);

    return result;
}

PyDoc_STRVAR(vl_encode_doc,
"vl_encode(bitarray, /) -> bytes\n\
\n\
Return variable length binary representation of bitarray.\n\
This representation is useful for efficiently storing small bitarray\n\
in a binary stream.  Use `vl_decode()` for decoding.");

/* --------------------------------------------------------------------- */

/* Set bitarray_type_obj (bato).  This function must be called before any
   other Python function in this module. */
static PyObject *
set_bato(PyObject *module, PyObject *obj)
{
    bitarray_type_obj = obj;
    Py_RETURN_NONE;
}

static PyMethodDef module_functions[] = {
    {"count_n",   (PyCFunction) count_n,   METH_VARARGS, count_n_doc},
    {"rindex",    (PyCFunction) r_index,   METH_VARARGS, rindex_doc},
    {"parity",    (PyCFunction) parity,    METH_O,       parity_doc},
    {"count_and", (PyCFunction) count_and, METH_VARARGS, count_and_doc},
    {"count_or",  (PyCFunction) count_or,  METH_VARARGS, count_or_doc},
    {"count_xor", (PyCFunction) count_xor, METH_VARARGS, count_xor_doc},
    {"subset",    (PyCFunction) subset,    METH_VARARGS, subset_doc},
    {"serialize", (PyCFunction) serialize, METH_O,       serialize_doc},
    {"ba2hex",    (PyCFunction) ba2hex,    METH_O,       ba2hex_doc},
    {"_hex2ba",   (PyCFunction) hex2ba,    METH_VARARGS, 0},
    {"ba2base",   (PyCFunction) ba2base,   METH_VARARGS, ba2base_doc},
    {"_base2ba",  (PyCFunction) base2ba,   METH_VARARGS, 0},
    {"vl_encode", (PyCFunction) vl_encode, METH_O,       vl_encode_doc},
    {"_vl_decode",(PyCFunction) vl_decode, METH_VARARGS, 0},
    {"_set_bato", (PyCFunction) set_bato,  METH_O,       0},
    {NULL,        NULL}  /* sentinel */
};

/******************************* Install Module ***************************/

#ifdef IS_PY3K
static PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_util", 0, -1, module_functions,
};
#endif

PyMODINIT_FUNC
#ifdef IS_PY3K
PyInit__util(void)
#else
init_util(void)
#endif
{
    PyObject *m;

#ifdef IS_PY3K
    m = PyModule_Create(&moduledef);
    if (m == NULL)
        return NULL;
    return m;
#else
    m = Py_InitModule3("_util", module_functions, 0);
    if (m == NULL)
        return;
#endif
}
