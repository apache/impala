/*
   Copyright (c) 2019, Ilan Schnell
   bitarray is published under the PSF license.

   This file contains the C implementation of some useful utility functions.

   Author: Ilan Schnell
*/

#define PY_SSIZE_T_CLEAN
#include "Python.h"

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#ifdef STDC_HEADERS
#include <stddef.h>
#else  /* !STDC_HEADERS */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>      /* For size_t */
#endif /* HAVE_SYS_TYPES_H */
#endif /* !STDC_HEADERS */


typedef long long int idx_t;

typedef struct {
    PyObject_VAR_HEAD
    char *ob_item;
    Py_ssize_t allocated;       /* how many bytes allocated */
    idx_t nbits;                /* length of bitarray, i.e. elements */
    int endian;                 /* bit endianness of bitarray */
    int ob_exports;             /* how many buffer exports */
    PyObject *weakreflist;      /* list of weak references */
} bitarrayobject;

#define BITS(bytes)  ((idx_t) (bytes) << 3)

#define BYTES(bits)  (((bits) == 0) ? 0 : (((bits) - 1) / 8 + 1))

#define BITMASK(endian, i)  (((char) 1) << ((endian) ? (7 - (i)%8) : (i)%8))

/* ------------ low level access to bits in bitarrayobject ------------- */

#ifndef NDEBUG
static int GETBIT(bitarrayobject *self, idx_t i) {
    assert(0 <= i && i < self->nbits);
    return ((self)->ob_item[(i) / 8] & BITMASK((self)->endian, i) ? 1 : 0);
}
#else
#define GETBIT(self, i)  \
    ((self)->ob_item[(i) / 8] & BITMASK((self)->endian, i) ? 1 : 0)
#endif

static void
setbit(bitarrayobject *self, idx_t i, int bit)
{
    char *cp, mask;

    assert(0 <= i && i < BITS(Py_SIZE(self)));
    mask = BITMASK(self->endian, i);
    cp = self->ob_item + i / 8;
    if (bit)
        *cp |= mask;
    else
        *cp &= ~mask;
}

/* set using the Python module function _set_babt() */
static PyObject *bitarray_basetype = NULL;

/* return 1 if obj is a bitarray, 0 otherwise */
static int
bitarray_Check(PyObject *obj)
{
    if (bitarray_basetype == NULL) /* fallback */
        return PyObject_HasAttrString(obj, "endian");
    return PyObject_IsInstance(obj, bitarray_basetype);
}

static void
setunused(bitarrayobject *self)
{
    const idx_t n = BITS(Py_SIZE(self));
    idx_t i;

    for (i = self->nbits; i < n; i++)
        setbit(self, i, 0);
}

static int bitcount_lookup[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
};

/* return the smallest index i for which a.count(1, 0, i) == n, or when
   n exceeds the total count return -1  */
static idx_t
count_to_n(bitarrayobject *a, idx_t n)
{
    idx_t i = 0, j = 0, m;  /* i is the index, j the total count up to i */
    Py_ssize_t block_start, block_stop, k;
    unsigned char c;

    if (n == 0)
        return 0;

#define BLOCK_BITS  8192
    /* by counting big blocks we save comparisons */
    while (i + BLOCK_BITS < a->nbits) {
        m = 0;
        assert(i % 8 == 0);
        block_start = (Py_ssize_t) (i / 8);
        block_stop = block_start + (BLOCK_BITS / 8);
        for (k = block_start; k < block_stop; k++) {
            assert(k < Py_SIZE(a));
            c = a->ob_item[k];
            m += bitcount_lookup[c];
        }
        if (j + m >= n)
            break;
        j += m;
        i += BLOCK_BITS;
    }
#undef BLOCK_SIZE

    while (i + 8 < a->nbits) {
        k = (Py_ssize_t) (i / 8);
        assert(k < Py_SIZE(a));
        c = a->ob_item[k];
        m = bitcount_lookup[c];
        if (j + m >= n)
            break;
        j += m;
        i += 8;
    }

    while (j < n && i < a->nbits ) {
        j += GETBIT(a, i);
        i++;
    }
    if (j < n)
        return -1;

    return i;
}

/* return index of last occurrence of vi, -1 when x is not in found. */
static idx_t
find_last(bitarrayobject *self, int vi)
{
    Py_ssize_t j;
    idx_t i;
    char c;

    if (self->nbits == 0)
        return -1;

    /* search within top byte */
    for (i = self->nbits - 1; i >= BITS(self->nbits / 8); i--)
        if (GETBIT(self, i) == vi)
            return i;

    if (i < 0)  /* not found within top byte */
        return -1;
    assert((i + 1) % 8 == 0);

    /* seraching for 1 means: break when byte is not 0x00
       searching for 0 means: break when byte is not 0xff */
    c = vi ? 0x00 : 0xff;

    /* skip ahead by checking whole bytes */
    for (j = BYTES(i) - 1; j >= 0; j--)
        if (c ^ self->ob_item[j])
            break;

    if (j < 0)  /* not found within bytes */
        return -1;

    /* search within byte found */
    for (i = BITS(j + 1) - 1; i >= BITS(j); i--)
        if (GETBIT(self, i) == vi)
            return i;

    return -1;
}

/*************************** Module functions **********************/

static PyObject *
count_n(PyObject *self, PyObject *args)
{
    PyObject *a;
    idx_t n, i;

    if (!PyArg_ParseTuple(args, "OL:count_n", &a, &n))
        return NULL;

    if (!bitarray_Check(a)) {
        PyErr_SetString(PyExc_TypeError, "bitarray object expected");
        return NULL;
    }
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
    return PyLong_FromLongLong(i);
}

PyDoc_STRVAR(count_n_doc,
"count_n(a, n, /) -> int\n\
\n\
Find the smallest index `i` for which `a[:i].count() == n`.\n\
Raises `ValueError`, when n exceeds the `a.count()`.");


static PyObject *
r_index(PyObject *self, PyObject *args)
{
    PyObject *a, *x = Py_True;
    idx_t i;
    long vi;

    if (!PyArg_ParseTuple(args, "O|O:rindex", &a, &x))
        return NULL;

    if (!bitarray_Check(a)) {
        PyErr_SetString(PyExc_TypeError, "bitarray object expected");
        return NULL;
    }
    vi = PyObject_IsTrue(x);
    if (vi < 0)
        return NULL;

    i = find_last((bitarrayobject *) a, vi);
    if (i < 0) {
        PyErr_SetString(PyExc_ValueError, "index(x): x not in bitarray");
        return NULL;
    }
    return PyLong_FromLongLong(i);
}

PyDoc_STRVAR(rindex_doc,
"rindex(bitarray, value=True, /) -> int\n\
\n\
Return the rightmost index of `bool(value)` in bitarray.\n\
Raises `ValueError` if the value is not present.");


enum kernel_type {
    KERN_cand,     /* count bitwise and -> int */
    KERN_cor,      /* count bitwise or -> int */
    KERN_cxor,     /* count bitwise xor -> int */
    KERN_subset,   /* is subset -> bool */
};

static PyObject *
two_bitarray_func(PyObject *args, enum kernel_type kern, char *format)
{
    PyObject *a, *b;
    Py_ssize_t i;
    idx_t res = 0;
    unsigned char c;

    if (!PyArg_ParseTuple(args, format, &a, &b))
        return NULL;
    if (!(bitarray_Check(a) && bitarray_Check(b))) {
        PyErr_SetString(PyExc_TypeError, "bitarray object expected");
        return NULL;
    }
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
    setunused(aa);
    setunused(bb);
    switch (kern) {
    case KERN_cand:
        for (i = 0; i < Py_SIZE(aa); i++) {
            c = aa->ob_item[i] & bb->ob_item[i];
            res += bitcount_lookup[c];
        }
        break;
    case KERN_cor:
        for (i = 0; i < Py_SIZE(aa); i++) {
            c = aa->ob_item[i] | bb->ob_item[i];
            res += bitcount_lookup[c];
        }
        break;
    case KERN_cxor:
        for (i = 0; i < Py_SIZE(aa); i++) {
            c = aa->ob_item[i] ^ bb->ob_item[i];
            res += bitcount_lookup[c];
        }
        break;
    case KERN_subset:
        for (i = 0; i < Py_SIZE(aa); i++)
            if ((aa->ob_item[i] & bb->ob_item[i]) != aa->ob_item[i])
                Py_RETURN_FALSE;
        Py_RETURN_TRUE;
    default:  /* should never happen */
        return NULL;
    }
#undef aa
#undef bb
    return PyLong_FromLongLong(res);
}

#define COUNT_FUNC(oper, ochar)                                         \
static PyObject *                                                       \
count_ ## oper (bitarrayobject *self, PyObject *args)                   \
{                                                                       \
    return two_bitarray_func(args, KERN_c ## oper, "OO:count_" #oper);  \
}                                                                       \
PyDoc_STRVAR(count_ ## oper ## _doc,                                    \
"count_" #oper "(a, b, /) -> int\n\
\n\
Returns `(a " ochar " b).count()`, but is more memory efficient,\n\
as no intermediate bitarray object gets created.")

COUNT_FUNC(and, "&");
COUNT_FUNC(or,  "|");
COUNT_FUNC(xor, "^");


static PyObject *
subset(PyObject *self, PyObject *args)
{
    return two_bitarray_func(args, KERN_subset, "OO:subset");
}

PyDoc_STRVAR(subset_doc,
"subset(a, b, /) -> bool\n\
\n\
Return True if bitarray `a` is a subset of bitarray `b` (False otherwise).\n\
`subset(a, b)` is equivalent to `(a & b).count() == a.count()` but is more\n\
efficient since we can stop as soon as one mismatch is found, and no\n\
intermediate bitarray object gets created.");


/* set bitarray_basetype (babt) */
static PyObject *
set_babt(PyObject *self, PyObject *obj)
{
    bitarray_basetype = obj;
    Py_RETURN_NONE;
}

static PyMethodDef module_functions[] = {
    {"count_n",   (PyCFunction) count_n,   METH_VARARGS, count_n_doc},
    {"rindex",    (PyCFunction) r_index,   METH_VARARGS, rindex_doc},
    {"count_and", (PyCFunction) count_and, METH_VARARGS, count_and_doc},
    {"count_or",  (PyCFunction) count_or,  METH_VARARGS, count_or_doc},
    {"count_xor", (PyCFunction) count_xor, METH_VARARGS, count_xor_doc},
    {"subset",    (PyCFunction) subset,    METH_VARARGS, subset_doc},
    {"_set_babt", (PyCFunction) set_babt,  METH_O,       ""},
    {NULL,        NULL}  /* sentinel */
};

/*********************** Install Module **************************/

#ifdef IS_PY3K
static PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_util", 0, -1, module_functions,
};
PyMODINIT_FUNC
PyInit__util(void)
#else
PyMODINIT_FUNC
init_util(void)
#endif
{
    PyObject *m;

#ifdef IS_PY3K
    m = PyModule_Create(&moduledef);
    if (m == NULL)
        return NULL;
#else
    m = Py_InitModule3("_util", module_functions, 0);
    if (m == NULL)
        return;
#endif

#ifdef IS_PY3K
    return m;
#endif
}
