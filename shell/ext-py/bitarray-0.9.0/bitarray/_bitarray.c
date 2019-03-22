/*
   This file is the C part of the bitarray package.  Almost all
   functionality is implemented here.

   Author: Ilan Schnell
*/

#define PY_SSIZE_T_CLEAN
#include "Python.h"

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#ifdef IS_PY3K
#include "bytesobject.h"
#define PyString_FromStringAndSize  PyBytes_FromStringAndSize
#define PyString_FromString  PyBytes_FromString
#define PyString_Check  PyBytes_Check
#define PyString_Size  PyBytes_Size
#define PyString_AsString  PyBytes_AsString
#define PyString_ConcatAndDel  PyBytes_ConcatAndDel
#define Py_TPFLAGS_HAVE_WEAKREFS  0
#endif

#if PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION < 6
/* backward compatibility with Python 2.5 */
#define Py_TYPE(ob)   (((PyObject *) (ob))->ob_type)
#define Py_SIZE(ob)   (((PyVarObject *) (ob))->ob_size)
#endif

#if PY_MAJOR_VERSION == 3 || (PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION == 7)
/* (new) buffer protocol */
#define WITH_BUFFER
#endif

#ifdef STDC_HEADERS
#include <stddef.h>
#else  /* !STDC_HEADERS */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>      /* For size_t */
#endif /* HAVE_SYS_TYPES_H */
#endif /* !STDC_HEADERS */


typedef long long int idx_t;

/* throughout:  0 = little endian   1 = big endian */
#define DEFAULT_ENDIAN  1

typedef struct {
    PyObject_VAR_HEAD
#ifdef WITH_BUFFER
    int ob_exports;             /* how many buffer exports */
#endif
    char *ob_item;
    Py_ssize_t allocated;       /* how many bytes allocated */
    idx_t nbits;                /* length og bitarray */
    int endian;                 /* bit endianness of bitarray */
    PyObject *weakreflist;      /* list of weak references */
} bitarrayobject;

static PyTypeObject Bitarraytype;

#define bitarray_Check(obj)  PyObject_TypeCheck(obj, &Bitarraytype)

#define BITS(bytes)  (((idx_t) 8) * ((idx_t) (bytes)))

#define BYTES(bits)  (((bits) == 0) ? 0 : (((bits) - 1) / 8 + 1))

#define BITMASK(endian, i)  (((char) 1) << ((endian) ? (7 - (i)%8) : (i)%8))

/* ------------ low level access to bits in bitarrayobject ------------- */

#define GETBIT(self, i)  \
    ((self)->ob_item[(i) / 8] & BITMASK((self)->endian, i) ? 1 : 0)

static void
setbit(bitarrayobject *self, idx_t i, int bit)
{
    char *cp, mask;

    mask = BITMASK(self->endian, i);
    cp = self->ob_item + i / 8;
    if (bit)
        *cp |= mask;
    else
        *cp &= ~mask;
}

static int
check_overflow(idx_t nbits)
{
    idx_t max_bits;

    assert(nbits >= 0);
    if (sizeof(void *) == 4) {  /* 32bit machine */
        max_bits = ((idx_t) 1) << 34;  /* 2^34 = 16 Gbits*/
        if (nbits > max_bits) {
            char buff[256];
            sprintf(buff, "cannot create bitarray of size %lld, "
                          "max size is %lld", nbits, max_bits);
            PyErr_SetString(PyExc_OverflowError, buff);
            return -1;
        }
    }
    return 0;
}

static int
resize(bitarrayobject *self, idx_t nbits)
{
    Py_ssize_t newsize;
    size_t _new_size;       /* for allocation */

    if (check_overflow(nbits) < 0)
        return -1;

    newsize = (Py_ssize_t) BYTES(nbits);

    /* Bypass realloc() when a previous overallocation is large enough
       to accommodate the newsize.  If the newsize is 16 smaller than the
       current size, then proceed with the realloc() to shrink the list.
    */
    if (self->allocated >= newsize &&
        Py_SIZE(self) < newsize + 16 &&
        self->ob_item != NULL)
    {
        Py_SIZE(self) = newsize;
        self->nbits = nbits;
        return 0;
    }

    if (newsize >= Py_SIZE(self) + 65536)
        /* Don't overallocate when the size increase is very large. */
        _new_size = newsize;
    else
        /* This over-allocates proportional to the bitarray size, making
           room for additional growth.  The over-allocation is mild, but is
           enough to give linear-time amortized behavior over a long
           sequence of appends() in the presence of a poorly-performing
           system realloc().
           The growth pattern is:  0, 4, 8, 16, 25, 34, 44, 54, 65, 77, ...
           Note, the pattern starts out the same as for lists but then
           grows at a smaller rate so that larger bitarrays only overallocate
           by about 1/16th -- this is done because bitarrays are assumed
           to be memory critical.
        */
        _new_size = (newsize >> 4) + (Py_SIZE(self) < 8 ? 3 : 7) + newsize;

    self->ob_item = PyMem_Realloc(self->ob_item, _new_size);
    if (self->ob_item == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    Py_SIZE(self) = newsize;
    self->allocated = _new_size;
    self->nbits = nbits;
    return 0;
}

/* create new bitarray object without initialization of buffer */
static PyObject *
newbitarrayobject(PyTypeObject *type, idx_t nbits, int endian)
{
    bitarrayobject *obj;
    Py_ssize_t nbytes;

    if (check_overflow(nbits) < 0)
        return NULL;

    obj = (bitarrayobject *) type->tp_alloc(type, 0);
    if (obj == NULL)
        return NULL;

    nbytes = (Py_ssize_t) BYTES(nbits);
    Py_SIZE(obj) = nbytes;
    obj->nbits = nbits;
    obj->endian = endian;
    if (nbytes == 0) {
        obj->ob_item = NULL;
    }
    else {
        obj->ob_item = PyMem_Malloc((size_t) nbytes);
        if (obj->ob_item == NULL) {
            PyObject_Del(obj);
            PyErr_NoMemory();
            return NULL;
        }
    }
    obj->allocated = nbytes;
    obj->weakreflist = NULL;
    return (PyObject *) obj;
}

static void
bitarray_dealloc(bitarrayobject *self)
{
    if (self->weakreflist != NULL)
        PyObject_ClearWeakRefs((PyObject *) self);

    if (self->ob_item != NULL)
        PyMem_Free((void *) self->ob_item);

    Py_TYPE(self)->tp_free((PyObject *) self);
}

/* copy n bits from other (starting at b) onto self (starting at a) */
static void
copy_n(bitarrayobject *self, idx_t a,
       bitarrayobject *other, idx_t b, idx_t n)
{
    idx_t i;

    assert(0 <= n && n <= self->nbits && n <= other->nbits);
    assert(0 <= a && a <= self->nbits - n);
    assert(0 <= b && b <= other->nbits - n);
    if (n == 0) {
        return;
    }

    if (self->endian == other->endian && a % 8 == 0 && b % 8 == 0 && n >= 8)
    {
        const Py_ssize_t bytes = (Py_ssize_t) n / 8;
        const idx_t bits = bytes * 8;

        if (a <= b) {
            memmove(self->ob_item + a / 8, other->ob_item + b / 8, bytes);
        }
        if (n != bits) {
            copy_n(self, bits + a, other, bits + b, n - bits);
        }
        if (a > b) {
            memmove(self->ob_item + a / 8, other->ob_item + b / 8, bytes);
        }
        return;
    }

    /* the different type of looping is only relevant when other and self
       are the same object, i.e. when copying a piece of an bitarrayobject
       onto itself */
    if (a <= b) {
        for (i = 0; i < n; i++)             /* loop forward (delete) */
            setbit(self, i + a, GETBIT(other, i + b));
    }
    else {
        for (i = n - 1; i >= 0; i--)      /* loop backwards (insert) */
            setbit(self, i + a, GETBIT(other, i + b));
    }
}

/* starting at start, delete n bits from self */
static int
delete_n(bitarrayobject *self, idx_t start, idx_t n)
{
    assert(0 <= start && start <= self->nbits);
    assert(0 <= n && n <= self->nbits - start);
    if (n == 0)
        return 0;

    copy_n(self, start, self, start + n, self->nbits - start - n);
    return resize(self, self->nbits - n);
}

/* starting at start, insert n (uninitialized) bits into self */
static int
insert_n(bitarrayobject *self, idx_t start, idx_t n)
{
    assert(0 <= start && start <= self->nbits);
    assert(n >= 0);
    if (n == 0)
        return 0;

    if (resize(self, self->nbits + n) < 0)
        return -1;
    copy_n(self, start + n, self, start, self->nbits - start - n);
    return 0;
}

/* sets ususet bits to 0, i.e. the ones in the last byte (if any),
   and return the number of bits set -- self->nbits is unchanged */
static int
setunused(bitarrayobject *self)
{
    idx_t i, n;
    int res = 0;

    n = BITS(Py_SIZE(self));
    for (i = self->nbits; i < n; i++) {
        setbit(self, i, 0);
        res++;
    }
    assert(res < 8);
    return res;
}

/* repeat self n times */
static int
repeat(bitarrayobject *self, idx_t n)
{
    idx_t nbits, i;

    if (n <= 0) {
        if (resize(self, 0) < 0)
            return -1;
    }
    if (n > 1) {
        nbits = self->nbits;
        if (resize(self, nbits * n) < 0)
            return -1;
        for (i = 1; i < n; i++)
            copy_n(self, i * nbits, self, 0, nbits);
    }
    return 0;
}


enum op_type {
    OP_and,
    OP_or,
    OP_xor,
};

/* perform bitwise operation */
static int
bitwise(bitarrayobject *self, PyObject *arg, enum op_type oper)
{
    bitarrayobject *other;
    Py_ssize_t i;

    if (!bitarray_Check(arg)) {
        PyErr_SetString(PyExc_TypeError,
                        "bitarray object expected for bitwise operation");
        return -1;
    }
    other = (bitarrayobject *) arg;
    if (self->nbits != other->nbits) {
        PyErr_SetString(PyExc_ValueError,
               "bitarrays of equal length expected for bitwise operation");
        return -1;
    }
    setunused(self);
    setunused(other);
    switch (oper) {
    case OP_and:
        for (i = 0; i < Py_SIZE(self); i++)
            self->ob_item[i] &= other->ob_item[i];
        break;
    case OP_or:
        for (i = 0; i < Py_SIZE(self); i++)
            self->ob_item[i] |= other->ob_item[i];
        break;
    case OP_xor:
        for (i = 0; i < Py_SIZE(self); i++)
            self->ob_item[i] ^= other->ob_item[i];
        break;
    }
    return 0;
}

/* set the bits from start to stop (excluding) in self to val */
static void
setrange(bitarrayobject *self, idx_t start, idx_t stop, int val)
{
    idx_t i;

    assert(0 <= start && start <= self->nbits);
    assert(0 <= stop && stop <= self->nbits);
    for (i = start; i < stop; i++)
        setbit(self, i, val);
}

static void
invert(bitarrayobject *self)
{
    Py_ssize_t i;

    for (i = 0; i < Py_SIZE(self); i++)
        self->ob_item[i] = ~self->ob_item[i];
}

/* reverse the order of bits in each byte of the buffer */
static void
bytereverse(bitarrayobject *self)
{
    static char trans[256];
    static int setup = 0;
    Py_ssize_t i;
    unsigned char c;

    if (!setup) {
        /* setup translation table, which maps each byte to it's reversed:
           trans = {0, 128, 64, 192, 32, 160, ..., 255} */
        int j, k;
        for (k = 0; k < 256; k++) {
            trans[k] = 0x00;
            for (j = 0; j < 8; j++)
                if (1 << (7 - j) & k)
                    trans[k] |= 1 << j;
        }
        setup = 1;
    }

    setunused(self);
    for (i = 0; i < Py_SIZE(self); i++) {
        c = self->ob_item[i];
        self->ob_item[i] = trans[c];
    }
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

/* returns number of 1 bits */
static idx_t
count(bitarrayobject *self)
{
    Py_ssize_t i;
    idx_t res = 0;
    unsigned char c;

    setunused(self);
    for (i = 0; i < Py_SIZE(self); i++) {
        c = self->ob_item[i];
        res += bitcount_lookup[c];
    }
    return res;
}

/* return index of first occurrence of vi, -1 when x is not in found. */
static idx_t
findfirst(bitarrayobject *self, int vi, idx_t start, idx_t stop)
{
    Py_ssize_t j;
    idx_t i;
    char c;

    if (Py_SIZE(self) == 0)
        return -1;
    if (start < 0 || start > self->nbits)
        start = 0;
    if (stop < 0 || stop > self->nbits)
        stop = self->nbits;
    if (start >= stop)
        return -1;

    if (stop > start + 8) {
        /* seraching for 1 means: break when byte is not 0x00
           searching for 0 means: break when byte is not 0xff */
        c = vi ? 0x00 : 0xff;

        /* skip ahead by checking whole bytes */
        for (j = (Py_ssize_t) (start / 8); j < BYTES(stop); j++)
            if (c ^ self->ob_item[j])
                break;

        if (j == Py_SIZE(self))
            j--;
        assert(0 <= j && j < Py_SIZE(self));

        if (start < BITS(j))
            start = BITS(j);
    }

    /* fine grained search */
    for (i = start; i < stop; i++)
        if (GETBIT(self, i) == vi)
            return i;

    return -1;
}

/* search for the first occurrence bitarray xa (in self), starting at p,
   and return its position (-1 when not found)
*/
static idx_t
search(bitarrayobject *self, bitarrayobject *xa, idx_t p)
{
    idx_t i;

    assert(p >= 0);
    while (p < self->nbits - xa->nbits + 1) {
        for (i = 0; i < xa->nbits; i++)
            if (GETBIT(self, p + i) != GETBIT(xa, i))
                goto next;

        return p;
    next:
        p++;
    }
    return -1;
}

static int
set_item(bitarrayobject *self, idx_t i, PyObject *v)
{
    long vi;

    assert(0 <= i && i < self->nbits);
    vi = PyObject_IsTrue(v);
    if (vi < 0)
        return -1;
    setbit(self, i, vi);
    return 0;
}

static int
append_item(bitarrayobject *self, PyObject *item)
{
    if (resize(self, self->nbits + 1) < 0)
        return -1;
    return set_item(self, self->nbits - 1, item);
}

static PyObject *
unpack(bitarrayobject *self, char zero, char one)
{
    PyObject *res;
    Py_ssize_t i;
    char *str;

    if (self->nbits > PY_SSIZE_T_MAX) {
        PyErr_SetString(PyExc_OverflowError, "bitarray too large to unpack");
        return NULL;
    }
    str = PyMem_Malloc((size_t) self->nbits);
    if (str == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    for (i = 0; i < self->nbits; i++) {
        *(str + i) = GETBIT(self, i) ? one : zero;
    }
    res = PyString_FromStringAndSize(str, (Py_ssize_t) self->nbits);
    PyMem_Free((void *) str);
    return res;
}

static int
extend_bitarray(bitarrayobject *self, bitarrayobject *other)
{
    idx_t n_sum;
    idx_t n_other_bits;

    if (other->nbits == 0)
        return 0;

    /*
        Note: other may be self. Thus we take the size before we resize,
        ensuring we only copy the right parts of the array.
    */
    n_other_bits = other->nbits;
    n_sum = self->nbits + other->nbits;

    if (resize(self, n_sum) < 0)
        return -1;

    copy_n(self, n_sum - n_other_bits, other, 0, n_other_bits);
    return 0;
}

static int
extend_iter(bitarrayobject *self, PyObject *iter)
{
    PyObject *item;

    assert(PyIter_Check(iter));
    while ((item = PyIter_Next(iter)) != NULL) {
        if (append_item(self, item) < 0) {
            Py_DECREF(item);
            return -1;
        }
        Py_DECREF(item);
    }
    if (PyErr_Occurred())
        return -1;

    return 0;
}

static int
extend_list(bitarrayobject *self, PyObject *list)
{
    PyObject *item;
    Py_ssize_t n, i;

    assert(PyList_Check(list));
    n = PyList_Size(list);
    if (n == 0)
        return 0;

    if (resize(self, self->nbits + n) < 0)
        return -1;

    for (i = 0; i < n; i++) {
        item = PyList_GetItem(list, i);
        if (item == NULL)
            return -1;
        if (set_item(self, self->nbits - n + i, item) < 0)
            return -1;
    }
    return 0;
}

static int
extend_tuple(bitarrayobject *self, PyObject *tuple)
{
    PyObject *item;
    Py_ssize_t n, i;

    assert(PyTuple_Check(tuple));
    n = PyTuple_Size(tuple);
    if (n == 0)
        return 0;

    if (resize(self, self->nbits + n) < 0)
        return -1;

    for (i = 0; i < n; i++) {
        item = PyTuple_GetItem(tuple, i);
        if (item == NULL)
            return -1;
        if (set_item(self, self->nbits - n + i, item) < 0)
            return -1;
    }
    return 0;
}

/* extend_string(): extend the bitarray from a string, where each whole
   characters is converted to a single bit
*/
enum conv_tp {
    STR_01,    /*  '0' -> 0    '1'  -> 1   no other characters allowed */
    STR_RAW,   /*  0x00 -> 0   other -> 1                              */
};

static int
extend_string(bitarrayobject *self, PyObject *string, enum conv_tp conv)
{
    Py_ssize_t strlen, i;
    char c, *str;
    int vi = 0;

    assert(PyString_Check(string));
    strlen = PyString_Size(string);
    if (strlen == 0)
        return 0;

    if (resize(self, self->nbits + strlen) < 0)
        return -1;

    str = PyString_AsString(string);

    for (i = 0; i < strlen; i++) {
        c = *(str + i);
        /* depending on conv, map c to bit */
        switch (conv) {
        case STR_01:
            switch (c) {
            case '0': vi = 0; break;
            case '1': vi = 1; break;
            default:
                PyErr_Format(PyExc_ValueError,
                             "character must be '0' or '1', found '%c'", c);
                return -1;
            }
            break;
        case STR_RAW:
            vi = c ? 1 : 0;
            break;
        }
        setbit(self, self->nbits - strlen + i, vi);
    }
    return 0;
}

static int
extend_rawstring(bitarrayobject *self, PyObject *string)
{
    Py_ssize_t strlen;
    char *str;

    assert(PyString_Check(string) && self->nbits % 8 == 0);
    strlen = PyString_Size(string);
    if (strlen == 0)
        return 0;

    if (resize(self, self->nbits + BITS(strlen)) < 0)
        return -1;

    str = PyString_AsString(string);
    memcpy(self->ob_item + (Py_SIZE(self) - strlen), str, strlen);
    return 0;
}

static int
extend_dispatch(bitarrayobject *self, PyObject *obj)
{
    PyObject *iter;
    int ret;

    /* dispatch on type */
    if (bitarray_Check(obj))                              /* bitarray */
        return extend_bitarray(self, (bitarrayobject *) obj);

    if (PyList_Check(obj))                                    /* list */
        return extend_list(self, obj);

    if (PyTuple_Check(obj))                                  /* tuple */
        return extend_tuple(self, obj);

    if (PyString_Check(obj))                                 /* str01 */
        return extend_string(self, obj, STR_01);

#ifdef IS_PY3K
    if (PyUnicode_Check(obj)) {                               /* str01 */
        PyObject *string;
        string = PyUnicode_AsEncodedString(obj, NULL, NULL);
        ret = extend_string(self, string, STR_01);
        Py_DECREF(string);
        return ret;
    }
#endif

    if (PyIter_Check(obj))                                    /* iter */
        return extend_iter(self, obj);

    /* finally, try to get the iterator of the object */
    iter = PyObject_GetIter(obj);
    if (iter == NULL) {
        PyErr_SetString(PyExc_TypeError, "could not extend bitarray");
        return -1;
    }
    ret = extend_iter(self, iter);
    Py_DECREF(iter);
    return ret;
}

/* --------- helper functions NOT involving bitarrayobjects ------------ */

#define ENDIAN_STR(ba)  (((ba)->endian) ? "big" : "little")

#ifdef IS_PY3K
#define IS_INDEX(x)  (PyLong_Check(x) || PyIndex_Check(x))
#define IS_INT_OR_BOOL(x)  (PyBool_Check(x) || PyLong_Check(x))
#else
#define IS_INDEX(x)  (PyInt_Check(x) || PyLong_Check(x) || PyIndex_Check(x))
#define IS_INT_OR_BOOL(x)  (PyBool_Check(x) || PyInt_Check(x) || \
                                               PyLong_Check(x))
#endif

/* given an PyLong (which must be 0 or 1), or a PyBool, return 0 or 1,
   or -1 on error */
static int
IntBool_AsInt(PyObject *v)
{
    long x;

    if (PyBool_Check(v))
        return PyObject_IsTrue(v);

#ifndef IS_PY3K
    if (PyInt_Check(v)) {
        x = PyInt_AsLong(v);
    }
    else
#endif
    if (PyLong_Check(v)) {
        x = PyLong_AsLong(v);
    }
    else {
        PyErr_SetString(PyExc_TypeError, "integer or bool expected");
        return -1;
    }

    if (x < 0 || x > 1) {
        PyErr_SetString(PyExc_ValueError,
                        "integer value between 0 and 1 expected");
        return -1;
    }
    return (int) x;
}

/* Extract a slice index from a PyInt or PyLong or an object with the
   nb_index slot defined, and store in *i.
   However, this function returns -1 on error and 0 on success.

   This is almost _PyEval_SliceIndex() with Py_ssize_t replaced by idx_t
*/
static int
getIndex(PyObject *v, idx_t *i)
{
    idx_t x;

#ifndef IS_PY3K
    if (PyInt_Check(v)) {
        x = PyInt_AS_LONG(v);
    }
    else
#endif
    if (PyLong_Check(v)) {
        x = PyLong_AsLongLong(v);
    }
    else if (PyIndex_Check(v)) {
        x = PyNumber_AsSsize_t(v, NULL);
        if (x == -1 && PyErr_Occurred())
            return -1;
    }
    else {
        PyErr_SetString(PyExc_TypeError, "slice indices must be integers or "
                                         "None or have an __index__ method");
        return -1;
    }
    *i = x;
    return 0;
}

/* this is PySlice_GetIndicesEx() with Py_ssize_t replaced by idx_t */
static int
slice_GetIndicesEx(PySliceObject *r, idx_t length,
                   idx_t *start, idx_t *stop, idx_t *step, idx_t *slicelength)
{
    idx_t defstart, defstop;

    if (r->step == Py_None) {
        *step = 1;
    }
    else {
        if (getIndex(r->step, step) < 0)
            return -1;
        if (*step == 0) {
            PyErr_SetString(PyExc_ValueError, "slice step cannot be zero");
            return -1;
        }
    }
    defstart = *step < 0 ? length - 1 : 0;
    defstop = *step < 0 ? -1 : length;

    if (r->start == Py_None) {
        *start = defstart;
    }
    else {
        if (getIndex(r->start, start) < 0)
            return -1;
        if (*start < 0) *start += length;
        if (*start < 0) *start = (*step < 0) ? -1 : 0;
        if (*start >= length) *start = (*step < 0) ? length - 1 : length;
    }

    if (r->stop == Py_None) {
        *stop = defstop;
    }
    else {
        if (getIndex(r->stop, stop) < 0)
            return -1;
        if (*stop < 0) *stop += length;
        if (*stop < 0) *stop = -1;
        if (*stop > length) *stop = length;
    }

    if ((*step < 0 && *stop >= *start) || (*step > 0 && *start >= *stop)) {
        *slicelength = 0;
    }
    else if (*step < 0) {
        *slicelength = (*stop - *start + 1) / (*step) + 1;
    }
    else {
        *slicelength = (*stop - *start - 1) / (*step) + 1;
    }

    return 0;
}

/**************************************************************************
                         Implementation of API methods
 **************************************************************************/

static PyObject *
bitarray_length(bitarrayobject *self)
{
    return PyLong_FromLongLong(self->nbits);
}

PyDoc_STRVAR(length_doc,
"length() -> int\n\
\n\
Return the length, i.e. number of bits stored in the bitarray.\n\
This method is preferred over __len__ (used when typing ``len(a)``),\n\
since __len__ will fail for a bitarray object with 2^31 or more elements\n\
on a 32bit machine, whereas this method will return the correct value,\n\
on 32bit and 64bit machines.");

PyDoc_STRVAR(len_doc,
"__len__() -> int\n\
\n\
Return the length, i.e. number of bits stored in the bitarray.\n\
This method will fail for a bitarray object with 2^31 or more elements\n\
on a 32bit machine.  Use bitarray.length() instead.");


static PyObject *
bitarray_copy(bitarrayobject *self)
{
    PyObject *res;

    res = newbitarrayobject(Py_TYPE(self), self->nbits, self->endian);
    if (res == NULL)
        return NULL;

    memcpy(((bitarrayobject *) res)->ob_item, self->ob_item, Py_SIZE(self));
    return res;
}

PyDoc_STRVAR(copy_doc,
"copy() -> bitarray\n\
\n\
Return a copy of the bitarray.");


static PyObject *
bitarray_count(bitarrayobject *self, PyObject *args)
{
    idx_t n1;
    long x = 1;

    if (!PyArg_ParseTuple(args, "|i:count", &x))
        return NULL;

    n1 = count(self);
    return PyLong_FromLongLong(x ? n1 : (self->nbits - n1));
}

PyDoc_STRVAR(count_doc,
"count([value]) -> int\n\
\n\
Return number of occurrences of value (defaults to True) in the bitarray.");


static PyObject *
bitarray_index(bitarrayobject *self, PyObject *args)
{
    PyObject *x;
    idx_t i, start = 0, stop = -1;
    long vi;

    if (!PyArg_ParseTuple(args, "O|LL:index", &x, &start, &stop))
        return NULL;

    vi = PyObject_IsTrue(x);
    if (vi < 0)
        return NULL;

    i = findfirst(self, vi, start, stop);
    if (i < 0) {
        PyErr_SetString(PyExc_ValueError, "index(x): x not in bitarray");
        return NULL;
    }
    return PyLong_FromLongLong(i);
}

PyDoc_STRVAR(index_doc,
"index(value, [start, [stop]]) -> int\n\
\n\
Return index of the first occurrence of bool(value) in the bitarray.\n\
Raises ValueError if the value is not present.");


static PyObject *
bitarray_extend(bitarrayobject *self, PyObject *obj)
{
    if (extend_dispatch(self, obj) < 0)
        return NULL;
    Py_RETURN_NONE;
}

PyDoc_STRVAR(extend_doc,
"extend(object)\n\
\n\
Append bits to the end of the bitarray.  The objects which can be passed\n\
to this method are the same iterable objects which can given to a bitarray\n\
object upon initialization.");


static PyObject *
bitarray_contains(bitarrayobject *self, PyObject *x)
{
    long res;

    if (IS_INT_OR_BOOL(x)) {
        int vi;

        vi = IntBool_AsInt(x);
        if (vi < 0)
            return NULL;
        res = findfirst(self, vi, 0, -1) >= 0;
    }
    else if (bitarray_Check(x)) {
        res = search(self, (bitarrayobject *) x, 0) >= 0;
    }
    else {
        PyErr_SetString(PyExc_TypeError, "bitarray or bool expected");
        return NULL;
    }
    return PyBool_FromLong(res);
}

PyDoc_STRVAR(contains_doc,
"__contains__(x) -> bool\n\
\n\
Return True if bitarray contains x, False otherwise.\n\
The value x may be a boolean (or integer between 0 and 1), or a bitarray.");


static PyObject *
bitarray_search(bitarrayobject *self, PyObject *args)
{
    PyObject *list = NULL;   /* list of matching positions to be returned */
    PyObject *x, *item = NULL;
    Py_ssize_t limit = -1;
    bitarrayobject *xa;
    idx_t p;

    if (!PyArg_ParseTuple(args, "O|n:_search", &x, &limit))
        return NULL;

    if (!bitarray_Check(x)) {
        PyErr_SetString(PyExc_TypeError, "bitarray expected for search");
        return NULL;
    }
    xa = (bitarrayobject *) x;
    if (xa->nbits == 0) {
        PyErr_SetString(PyExc_ValueError, "can't search for empty bitarray");
        return NULL;
    }
    list = PyList_New(0);
    if (list == NULL)
        return NULL;
    if (xa->nbits > self->nbits || limit == 0)
        return list;

    p = 0;
    while (1) {
        p = search(self, xa, p);
        if (p < 0)
            break;
        item = PyLong_FromLongLong(p);
        p++;
        if (item == NULL || PyList_Append(list, item) < 0) {
            Py_XDECREF(item);
            Py_XDECREF(list);
            return NULL;
        }
        Py_DECREF(item);
        if (limit > 0 && PyList_Size(list) >= limit)
            break;
    }
    return list;
}

PyDoc_STRVAR(search_doc,
"search(bitarray, [limit]) -> list\n\
\n\
Searches for the given a bitarray in self, and returns the start positions\n\
where bitarray matches self as a list.\n\
The optional argument limits the number of search results to the integer\n\
specified.  By default, all search results are returned.");


static PyObject *
bitarray_buffer_info(bitarrayobject *self)
{
    PyObject *res, *ptr;

    ptr = PyLong_FromVoidPtr(self->ob_item),
    res = Py_BuildValue("OLsiL",
                        ptr,
                        (idx_t) Py_SIZE(self),
                        ENDIAN_STR(self),
                        (int) (BITS(Py_SIZE(self)) - self->nbits),
                        (idx_t) self->allocated);
    Py_DECREF(ptr);
    return res;
}

PyDoc_STRVAR(buffer_info_doc,
"buffer_info() -> tuple\n\
\n\
Return a tuple (address, size, endianness, unused, allocated) giving the\n\
current memory address, the size (in bytes) used to hold the bitarray's\n\
contents, the bit endianness as a string, the number of unused bits\n\
(e.g. a bitarray of length 11 will have a buffer size of 2 bytes and\n\
5 unused bits), and the size (in bytes) of the allocated memory.");


static PyObject *
bitarray_endian(bitarrayobject *self)
{
#ifdef IS_PY3K
    return PyUnicode_FromString(ENDIAN_STR(self));
#else
    return PyString_FromString(ENDIAN_STR(self));
#endif
}

PyDoc_STRVAR(endian_doc,
"endian() -> string\n\
\n\
Return the bit endianness as a string (either 'little' or 'big').");


static PyObject *
bitarray_append(bitarrayobject *self, PyObject *v)
{
    if (append_item(self, v) < 0)
        return NULL;

    Py_RETURN_NONE;
}

PyDoc_STRVAR(append_doc,
"append(item)\n\
\n\
Append the value bool(item) to the end of the bitarray.");


static PyObject *
bitarray_all(bitarrayobject *self)
{
    if (findfirst(self, 0, 0, -1) >= 0)
        Py_RETURN_FALSE;
    else
        Py_RETURN_TRUE;
}

PyDoc_STRVAR(all_doc,
"all() -> bool\n\
\n\
Returns True when all bits in the array are True.");


static PyObject *
bitarray_any(bitarrayobject *self)
{
    if (findfirst(self, 1, 0, -1) >= 0)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}

PyDoc_STRVAR(any_doc,
"any() -> bool\n\
\n\
Returns True when any bit in the array is True.");


static PyObject *
bitarray_reduce(bitarrayobject *self)
{
    PyObject *dict, *repr = NULL, *result = NULL;
    char *str;

    dict = PyObject_GetAttrString((PyObject *) self, "__dict__");
    if (dict == NULL) {
        PyErr_Clear();
        dict = Py_None;
        Py_INCREF(dict);
    }
    /* the first byte indicates the number of unused bits at the end, and
       the rest of the bytes consist of the raw binary data */
    str = PyMem_Malloc(Py_SIZE(self) + 1);
    if (str == NULL) {
        PyErr_NoMemory();
        goto error;
    }
    str[0] = (char) setunused(self);
    memcpy(str + 1, self->ob_item, Py_SIZE(self));
    repr = PyString_FromStringAndSize(str, Py_SIZE(self) + 1);
    if (repr == NULL)
        goto error;
    PyMem_Free((void *) str);
    result = Py_BuildValue("O(Os)O", Py_TYPE(self),
                           repr, ENDIAN_STR(self), dict);
error:
    Py_DECREF(dict);
    Py_XDECREF(repr);
    return result;
}

PyDoc_STRVAR(reduce_doc, "state information for pickling");


static PyObject *
bitarray_reverse(bitarrayobject *self)
{
    PyObject *t;    /* temp bitarray to store lower half of self */
    idx_t i, m;

    if (self->nbits < 2)
        Py_RETURN_NONE;

    t = newbitarrayobject(Py_TYPE(self), self->nbits / 2, self->endian);
    if (t == NULL)
        return NULL;

#define tt  ((bitarrayobject *) t)
    /* copy lower half of array into temporary array */
    memcpy(tt->ob_item, self->ob_item, Py_SIZE(tt));

    m = self->nbits - 1;

    /* reverse the upper half onto the lower half. */
    for (i = 0; i < tt->nbits; i++)
        setbit(self, i, GETBIT(self, m - i));

    /* revert the stored away lower half onto the upper half. */
    for (i = 0; i < tt->nbits; i++)
        setbit(self, m - i, GETBIT(tt, i));
#undef tt
    Py_DECREF(t);
    Py_RETURN_NONE;
}

PyDoc_STRVAR(reverse_doc,
"reverse()\n\
\n\
Reverse the order of bits in the array (in-place).");


static PyObject *
bitarray_fill(bitarrayobject *self)
{
    long p;

    p = setunused(self);
    self->nbits += p;
#ifdef IS_PY3K
    return PyLong_FromLong(p);
#else
    return PyInt_FromLong(p);
#endif
}

PyDoc_STRVAR(fill_doc,
"fill() -> int\n\
\n\
Adds zeros to the end of the bitarray, such that the length of the bitarray\n\
will be a multiple of 8.  Returns the number of bits added (0..7).");


static PyObject *
bitarray_invert(bitarrayobject *self)
{
    invert(self);
    Py_RETURN_NONE;
}

PyDoc_STRVAR(invert_doc,
"invert()\n\
\n\
Invert all bits in the array (in-place),\n\
i.e. convert each 1-bit into a 0-bit and vice versa.");


static PyObject *
bitarray_bytereverse(bitarrayobject *self)
{
    bytereverse(self);
    Py_RETURN_NONE;
}

PyDoc_STRVAR(bytereverse_doc,
"bytereverse()\n\
\n\
For all bytes representing the bitarray, reverse the bit order (in-place).\n\
Note: This method changes the actual machine values representing the\n\
bitarray; it does not change the endianness of the bitarray object.");


static PyObject *
bitarray_setall(bitarrayobject *self, PyObject *v)
{
    long vi;

    vi = PyObject_IsTrue(v);
    if (vi < 0)
        return NULL;

    memset(self->ob_item, vi ? 0xff : 0x00, Py_SIZE(self));
    Py_RETURN_NONE;
}

PyDoc_STRVAR(setall_doc,
"setall(value)\n\
\n\
Set all bits in the bitarray to bool(value).");


static PyObject *
bitarray_sort(bitarrayobject *self, PyObject *args, PyObject *kwds)
{
    idx_t n, n0, n1;
    int reverse = 0;
    static char* kwlist[] = {"reverse", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:sort", kwlist, &reverse))
        return NULL;

    n = self->nbits;
    n1 = count(self);

    if (reverse) {
        setrange(self, 0, n1, 1);
        setrange(self, n1, n, 0);
    }
    else {
        n0 = n - n1;
        setrange(self, 0, n0, 0);
        setrange(self, n0, n, 1);
    }
    Py_RETURN_NONE;
}

PyDoc_STRVAR(sort_doc,
"sort(reverse=False)\n\
\n\
Sort the bits in the array (in-place).");


#ifdef IS_PY3K
static PyObject *
bitarray_fromfile(bitarrayobject *self, PyObject *args)
{
    PyObject *f;
    Py_ssize_t newsize, nbytes = -1;
    PyObject *reader, *rargs, *result;
    size_t nread;
    idx_t t, p;

    if (!PyArg_ParseTuple(args, "O|n:fromfile", &f, &nbytes))
        return NULL;

    if (nbytes == 0)
        Py_RETURN_NONE;

    reader = PyObject_GetAttrString(f, "read");
    if (reader == NULL)
    {
        PyErr_SetString(PyExc_TypeError,
                        "first argument must be an open file");
        return NULL;
    }
    rargs = Py_BuildValue("(n)", nbytes);
    if (rargs == NULL) {
        Py_DECREF(reader);
        return NULL;
    }
    result = PyEval_CallObject(reader, rargs);
    if (result != NULL) {
        if (!PyBytes_Check(result)) {
            PyErr_SetString(PyExc_TypeError,
                            "first argument must be an open file");
            Py_DECREF(result);
            Py_DECREF(rargs);
            Py_DECREF(reader);
            return NULL;
        }

        nread = PyBytes_Size(result);

        t = self->nbits;
        p = setunused(self);
        self->nbits += p;

        newsize = Py_SIZE(self) + nread;

        if (resize(self, BITS(newsize)) < 0) {
            Py_DECREF(result);
            Py_DECREF(rargs);
            Py_DECREF(reader);
            return NULL;
        }

        memcpy(self->ob_item + (Py_SIZE(self) - nread),
               PyBytes_AS_STRING(result), nread);

        if (nbytes > 0 && nread < (size_t) nbytes) {
            PyErr_SetString(PyExc_EOFError, "not enough items read");
            return NULL;
        }
        if (delete_n(self, t, p) < 0)
            return NULL;
        Py_DECREF(result);
    }

    Py_DECREF(rargs);
    Py_DECREF(reader);

    Py_RETURN_NONE;
}
#else
static PyObject *
bitarray_fromfile(bitarrayobject *self, PyObject *args)
{
    PyObject *f;
    FILE *fp;
    Py_ssize_t newsize, nbytes = -1;
    size_t nread;
    idx_t t, p;
    long cur;

    if (!PyArg_ParseTuple(args, "O|n:fromfile", &f, &nbytes))
        return NULL;

    fp = PyFile_AsFile(f);
    if (fp == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "first argument must be an open file");
        return NULL;
    }

    /* find number of bytes till EOF */
    if (nbytes < 0) {
        if ((cur = ftell(fp)) < 0)
            goto EOFerror;

        if (fseek(fp, 0L, SEEK_END) || (nbytes = ftell(fp)) < 0)
            goto EOFerror;

        nbytes -= cur;
        if (fseek(fp, cur, SEEK_SET)) {
        EOFerror:
            PyErr_SetString(PyExc_EOFError, "could not find EOF");
            return NULL;
        }
    }
    if (nbytes == 0)
        Py_RETURN_NONE;

    /* file exists and there are more than zero bytes to read */
    t = self->nbits;
    p = setunused(self);
    self->nbits += p;

    newsize = Py_SIZE(self) + nbytes;
    if (resize(self, BITS(newsize)) < 0)
        return NULL;

    nread = fread(self->ob_item + (Py_SIZE(self) - nbytes), 1, nbytes, fp);
    if (nread < (size_t) nbytes) {
        newsize -= nbytes - nread;
        if (resize(self, BITS(newsize)) < 0)
            return NULL;
        PyErr_SetString(PyExc_EOFError, "not enough items in file");
        return NULL;
    }

    if (delete_n(self, t, p) < 0)
        return NULL;
    Py_RETURN_NONE;
}
#endif

PyDoc_STRVAR(fromfile_doc,
"fromfile(f, [n])\n\
\n\
Read n bytes from the file object f and append them to the bitarray\n\
interpreted as machine values.  When n is omitted, as many bytes are\n\
read until EOF is reached.");


#ifdef IS_PY3K
static PyObject *
bitarray_tofile(bitarrayobject *self, PyObject *f)
{
    PyObject *writer, *value, *args, *result;

    if (f == NULL) {
        PyErr_SetString(PyExc_TypeError, "writeobject with NULL file");
        return NULL;
    }
    writer = PyObject_GetAttrString(f, "write");
    if (writer == NULL)
        return NULL;
    setunused(self);
    value = PyBytes_FromStringAndSize(self->ob_item, Py_SIZE(self));
    if (value == NULL) {
        Py_DECREF(writer);
        return NULL;
    }
    args = PyTuple_Pack(1, value);
    if (args == NULL) {
        Py_DECREF(value);
        Py_DECREF(writer);
        return NULL;
    }
    result = PyEval_CallObject(writer, args);
    Py_DECREF(args);
    Py_DECREF(value);
    Py_DECREF(writer);
    if (result == NULL)
    {
        PyErr_SetString(PyExc_TypeError, "open file expected");
        return NULL;
    }
    Py_DECREF(result);
    Py_RETURN_NONE;
}
#else
static PyObject *
bitarray_tofile(bitarrayobject *self, PyObject *f)
{
    FILE *fp;

    fp = PyFile_AsFile(f);
    if (fp == NULL) {
        PyErr_SetString(PyExc_TypeError, "open file expected");
        return NULL;
    }
    if (Py_SIZE(self) == 0)
        Py_RETURN_NONE;

    setunused(self);
    if (fwrite(self->ob_item, 1, Py_SIZE(self), fp) !=
        (size_t) Py_SIZE(self))
    {
        PyErr_SetFromErrno(PyExc_IOError);
        clearerr(fp);
        return NULL;
    }
    Py_RETURN_NONE;
}
#endif

PyDoc_STRVAR(tofile_doc,
"tofile(f)\n\
\n\
Write all bits (as machine values) to the file object f.\n\
When the length of the bitarray is not a multiple of 8,\n\
the remaining bits (1..7) are set to 0.");


static PyObject *
bitarray_tolist(bitarrayobject *self)
{
    PyObject *list;
    idx_t i;

    list = PyList_New((Py_ssize_t) self->nbits);
    if (list == NULL)
        return NULL;

    for (i = 0; i < self->nbits; i++)
        if (PyList_SetItem(list, (Py_ssize_t) i,
                           PyBool_FromLong(GETBIT(self, i))) < 0)
            return NULL;
    return list;
}

PyDoc_STRVAR(tolist_doc,
"tolist() -> list\n\
\n\
Return an ordinary list with the items in the bitarray.\n\
Note that the list object being created will require 32 or 64 times more\n\
memory than the bitarray object, which may cause a memory error if the\n\
bitarray is very large.\n\
Also note that to extend a bitarray with elements from a list,\n\
use the extend method.");


static PyObject *
bitarray_frombytes(bitarrayobject *self, PyObject *string)
{
    idx_t t, p;

    if (!PyString_Check(string)) {
        PyErr_SetString(PyExc_TypeError, "byte string expected");
        return NULL;
    }
    t = self->nbits;
    p = setunused(self);
    self->nbits += p;

    if (extend_rawstring(self, string) < 0)
        return NULL;
    if (delete_n(self, t, p) < 0)
        return NULL;
    Py_RETURN_NONE;
}

PyDoc_STRVAR(frombytes_doc,
"frombytes(bytes)\n\
\n\
Append from a byte string, interpreted as machine values.");


static PyObject *
bitarray_tobytes(bitarrayobject *self)
{
    setunused(self);
    return PyString_FromStringAndSize(self->ob_item, Py_SIZE(self));
}

PyDoc_STRVAR(tobytes_doc,
"tobytes() -> bytes\n\
\n\
Return the byte representation of the bitarray.\n\
When the length of the bitarray is not a multiple of 8, the few remaining\n\
bits (1..7) are set to 0.");


static PyObject *
bitarray_to01(bitarrayobject *self)
{
#ifdef IS_PY3K
    PyObject *string, *unpacked;

    unpacked = unpack(self, '0', '1');
    string = PyUnicode_FromEncodedObject(unpacked, NULL, NULL);
    Py_DECREF(unpacked);
    return string;
#else
    return unpack(self, '0', '1');
#endif
}

PyDoc_STRVAR(to01_doc,
"to01() -> string\n\
\n\
Return a string containing '0's and '1's, representing the bits in the\n\
bitarray object.\n\
Note: To extend a bitarray from a string containing '0's and '1's,\n\
use the extend method.");


static PyObject *
bitarray_unpack(bitarrayobject *self, PyObject *args, PyObject *kwds)
{
    char zero = 0x00, one = 0xff;
    static char* kwlist[] = {"zero", "one", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|cc:unpack", kwlist,
                                     &zero, &one))
        return NULL;

    return unpack(self, zero, one);
}

PyDoc_STRVAR(unpack_doc,
"unpack(zero=b'\\x00', one=b'\\xff') -> bytes\n\
\n\
Return a byte string containing one character for each bit in the bitarray,\n\
using the specified mapping.\n\
See also the pack method.");


static PyObject *
bitarray_pack(bitarrayobject *self, PyObject *string)
{
    if (!PyString_Check(string)) {
        PyErr_SetString(PyExc_TypeError, "byte string expected");
        return NULL;
    }
    if (extend_string(self, string, STR_RAW) < 0)
        return NULL;

    Py_RETURN_NONE;
}

PyDoc_STRVAR(pack_doc,
"pack(bytes)\n\
\n\
Extend the bitarray from a byte string, where each characters corresponds to\n\
a single bit.  The character b'\\x00' maps to bit 0 and all other characters\n\
map to bit 1.\n\
This method, as well as the unpack method, are meant for efficient\n\
transfer of data between bitarray objects to other python objects\n\
(for example NumPy's ndarray object) which have a different view of memory.");


static PyObject *
bitarray_repr(bitarrayobject *self)
{
    PyObject *string;
#ifdef IS_PY3K
    PyObject *decoded;
#endif

    if (self->nbits == 0) {
        string = PyString_FromString("bitarray()");
        if (string == NULL)
            return NULL;
    }
    else {
        string = PyString_FromString("bitarray(\'");
        if (string == NULL)
            return NULL;
        PyString_ConcatAndDel(&string, unpack(self, '0', '1'));
        PyString_ConcatAndDel(&string, PyString_FromString("\')"));
    }
#ifdef IS_PY3K
    decoded = PyUnicode_FromEncodedObject(string, NULL, NULL);
    Py_DECREF(string);
    string = decoded;
#endif
    return string;
}


static PyObject *
bitarray_insert(bitarrayobject *self, PyObject *args)
{
    idx_t i;
    PyObject *v;

    if (!PyArg_ParseTuple(args, "LO:insert", &i, &v))
        return NULL;

    if (i < 0) {
        i += self->nbits;
        if (i < 0)
            i = 0;
    }
    if (i > self->nbits)
        i = self->nbits;

    if (insert_n(self, i, 1) < 0)
        return NULL;
    if (set_item(self, i, v) < 0)
        return NULL;
    Py_RETURN_NONE;
}

PyDoc_STRVAR(insert_doc,
"insert(i, item)\n\
\n\
Insert bool(item) into the bitarray before position i.");


static PyObject *
bitarray_pop(bitarrayobject *self, PyObject *args)
{
    idx_t i = -1;
    long vi;

    if (!PyArg_ParseTuple(args, "|L:pop", &i))
        return NULL;

    if (self->nbits == 0) {
        /* special case -- most common failure cause */
        PyErr_SetString(PyExc_IndexError, "pop from empty bitarray");
        return NULL;
    }
    if (i < 0)
        i += self->nbits;

    if (i < 0 || i >= self->nbits) {
        PyErr_SetString(PyExc_IndexError, "pop index out of range");
        return NULL;
    }
    vi = GETBIT(self, i);
    if (delete_n(self, i, 1) < 0)
        return NULL;
    return PyBool_FromLong(vi);
}

PyDoc_STRVAR(pop_doc,
"pop([i]) -> item\n\
\n\
Return the i-th (default last) element and delete it from the bitarray.\n\
Raises IndexError if bitarray is empty or index is out of range.");


static PyObject *
bitarray_remove(bitarrayobject *self, PyObject *v)
{
    idx_t i;
    long vi;

    vi = PyObject_IsTrue(v);
    if (vi < 0)
        return NULL;

    i = findfirst(self, vi, 0, -1);
    if (i < 0) {
        PyErr_SetString(PyExc_ValueError, "remove(x): x not in bitarray");
        return NULL;
    }
    if (delete_n(self, i, 1) < 0)
        return NULL;
    Py_RETURN_NONE;
}

PyDoc_STRVAR(remove_doc,
"remove(item)\n\
\n\
Remove the first occurrence of bool(item) in the bitarray.\n\
Raises ValueError if item is not present.");


/* --------- special methods ----------- */

static PyObject *
bitarray_getitem(bitarrayobject *self, PyObject *a)
{
    PyObject *res;
    idx_t start, stop, step, slicelength, j, i = 0;

    if (IS_INDEX(a)) {
        if (getIndex(a, &i) < 0)
            return NULL;
        if (i < 0)
            i += self->nbits;
        if (i < 0 || i >= self->nbits) {
            PyErr_SetString(PyExc_IndexError, "bitarray index out of range");
            return NULL;
        }
        return PyBool_FromLong(GETBIT(self, i));
    }
    if (PySlice_Check(a)) {
        if (slice_GetIndicesEx((PySliceObject *) a, self->nbits,
                               &start, &stop, &step, &slicelength) < 0) {
            return NULL;
        }
        res = newbitarrayobject(Py_TYPE(self), slicelength, self->endian);
        if (res == NULL)
            return NULL;

        for (i = 0, j = start; i < slicelength; i++, j += step)
            setbit((bitarrayobject *) res, i, GETBIT(self, j));

        return res;
    }
    PyErr_SetString(PyExc_TypeError, "index or slice expected");
    return NULL;
}

/* Sets the elements, specified by slice, in self to the value(s) given by v
   which is either a bitarray or a boolean.
*/
static int
setslice(bitarrayobject *self, PySliceObject *slice, PyObject *v)
{
    idx_t start, stop, step, slicelength, j, i = 0;

    if (slice_GetIndicesEx(slice, self->nbits,
                           &start, &stop, &step, &slicelength) < 0)
        return -1;

    if (bitarray_Check(v)) {
#define vv  ((bitarrayobject *) v)
        if (vv->nbits == slicelength) {
            for (i = 0, j = start; i < slicelength; i++, j += step)
                setbit(self, j, GETBIT(vv, i));
            return 0;
        }
        if (step != 1) {
            char buff[256];
            sprintf(buff, "attempt to assign sequence of size %lld "
                          "to extended slice of size %lld",
                    vv->nbits, (idx_t) slicelength);
            PyErr_SetString(PyExc_ValueError, buff);
            return -1;
        }
        /* make self bigger or smaller */
        if (vv->nbits > slicelength) {
            if (insert_n(self, start, vv->nbits - slicelength) < 0)
                return -1;
        }
        else {
            if (delete_n(self, start, slicelength - vv->nbits) < 0)
                return -1;
        }
        /* copy the new values into self */
        copy_n(self, start, vv, 0, vv->nbits);
#undef vv
        return 0;
    }
    if (IS_INT_OR_BOOL(v)) {
        int vi;

        vi = IntBool_AsInt(v);
        if (vi < 0)
            return -1;
        for (i = 0, j = start; i < slicelength; i++, j += step)
            setbit(self, j, vi);
        return 0;
    }
    PyErr_SetString(PyExc_IndexError,
                    "bitarray or bool expected for slice assignment");
    return -1;
}

static PyObject *
bitarray_setitem(bitarrayobject *self, PyObject *args)
{
    PyObject *a, *v;
    idx_t i = 0;

    if (!PyArg_ParseTuple(args, "OO:__setitem__", &a, &v))
        return NULL;

    if (IS_INDEX(a)) {
        if (getIndex(a, &i) < 0)
            return NULL;
        if (i < 0)
            i += self->nbits;
        if (i < 0 || i >= self->nbits) {
            PyErr_SetString(PyExc_IndexError, "bitarray index out of range");
            return NULL;
        }
        if (set_item(self, i, v) < 0)
            return NULL;
        Py_RETURN_NONE;
    }
    if (PySlice_Check(a)) {
        if (setslice(self, (PySliceObject *) a, v) < 0)
            return NULL;
        Py_RETURN_NONE;
    }
    PyErr_SetString(PyExc_TypeError, "index or slice expected");
    return NULL;
}

static PyObject *
bitarray_delitem(bitarrayobject *self, PyObject *a)
{
    idx_t start, stop, step, slicelength, j, i = 0;

    if (IS_INDEX(a)) {
        if (getIndex(a, &i) < 0)
            return NULL;
        if (i < 0)
            i += self->nbits;
        if (i < 0 || i >= self->nbits) {
            PyErr_SetString(PyExc_IndexError, "bitarray index out of range");
            return NULL;
        }
        if (delete_n(self, i, 1) < 0)
            return NULL;
        Py_RETURN_NONE;
    }
    if (PySlice_Check(a)) {
        if (slice_GetIndicesEx((PySliceObject *) a, self->nbits,
                               &start, &stop, &step, &slicelength) < 0) {
            return NULL;
        }
        if (slicelength == 0)
            Py_RETURN_NONE;

        if (step < 0) {
            stop = start + 1;
            start = stop + step * (slicelength - 1) - 1;
            step = -step;
        }
        if (step == 1) {
            assert(stop - start == slicelength);
            if (delete_n(self, start, slicelength) < 0)
                return NULL;
            Py_RETURN_NONE;
        }
        /* this is the only complicated part when step > 1 */
        for (i = j = start; i < self->nbits; i++)
            if ((i - start) % step != 0 || i >= stop) {
                setbit(self, j, GETBIT(self, i));
                j++;
            }
        if (resize(self, self->nbits - slicelength) < 0)
            return NULL;
        Py_RETURN_NONE;
    }
    PyErr_SetString(PyExc_TypeError, "index or slice expected");
    return NULL;
}

/* ---------- number methods ---------- */

static PyObject *
bitarray_add(bitarrayobject *self, PyObject *other)
{
    PyObject *res;

    res = bitarray_copy(self);
    if (extend_dispatch((bitarrayobject *) res, other) < 0) {
        Py_DECREF(res);
        return NULL;
    }
    return res;
}

static PyObject *
bitarray_iadd(bitarrayobject *self, PyObject *other)
{
    if (extend_dispatch(self, other) < 0)
        return NULL;
    Py_INCREF(self);
    return (PyObject *) self;
}

static PyObject *
bitarray_mul(bitarrayobject *self, PyObject *v)
{
    PyObject *res;
    idx_t vi = 0;

    if (!IS_INDEX(v)) {
        PyErr_SetString(PyExc_TypeError,
                        "integer value expected for bitarray repetition");
        return NULL;
    }
    if (getIndex(v, &vi) < 0)
        return NULL;
    res = bitarray_copy(self);
    if (repeat((bitarrayobject *) res, vi) < 0) {
        Py_DECREF(res);
        return NULL;
    }
    return res;
}

static PyObject *
bitarray_imul(bitarrayobject *self, PyObject *v)
{
    idx_t vi = 0;

    if (!IS_INDEX(v)) {
        PyErr_SetString(PyExc_TypeError,
            "integer value expected for in-place bitarray repetition");
        return NULL;
    }
    if (getIndex(v, &vi) < 0)
        return NULL;
    if (repeat(self, vi) < 0)
        return NULL;
    Py_INCREF(self);
    return (PyObject *) self;
}

static PyObject *
bitarray_cpinvert(bitarrayobject *self)
{
    PyObject *res;

    res = bitarray_copy(self);
    invert((bitarrayobject *) res);
    return res;
}

#define BITWISE_FUNC(oper)  \
static PyObject *                                                   \
bitarray_ ## oper (bitarrayobject *self, PyObject *other)           \
{                                                                   \
    PyObject *res;                                                  \
                                                                    \
    res = bitarray_copy(self);                                      \
    if (bitwise((bitarrayobject *) res, other, OP_ ## oper) < 0) {  \
        Py_DECREF(res);                                             \
        return NULL;                                                \
    }                                                               \
    return res;                                                     \
}

BITWISE_FUNC(and)
BITWISE_FUNC(or)
BITWISE_FUNC(xor)


#define BITWISE_IFUNC(oper)  \
static PyObject *                                            \
bitarray_i ## oper (bitarrayobject *self, PyObject *other)   \
{                                                            \
    if (bitwise(self, other, OP_ ## oper) < 0)               \
        return NULL;                                         \
    Py_INCREF(self);                                         \
    return (PyObject *) self;                                \
}

BITWISE_IFUNC(and)
BITWISE_IFUNC(or)
BITWISE_IFUNC(xor)

/******************* variable length encoding and decoding ***************/

static PyObject *
bitarray_encode(bitarrayobject *self, PyObject *args)
{
    PyObject *codedict, *iterable, *iter, *symbol, *bits;

    if (!PyArg_ParseTuple(args, "OO:_encode", &codedict, &iterable))
        return NULL;

    iter = PyObject_GetIter(iterable);
    if (iter == NULL) {
        PyErr_SetString(PyExc_TypeError, "iterable object expected");
        return NULL;
    }
    /* extend self with the bitarrays from codedict */
    while ((symbol = PyIter_Next(iter)) != NULL) {
        bits = PyDict_GetItem(codedict, symbol);
        Py_DECREF(symbol);
        if (bits == NULL) {
            PyErr_SetString(PyExc_ValueError, "symbol not in prefix code");
            goto error;
        }
        if (extend_bitarray(self, (bitarrayobject *) bits) < 0)
            goto error;
    }
    Py_DECREF(iter);
    if (PyErr_Occurred())
        return NULL;
    Py_RETURN_NONE;
error:
    Py_DECREF(iter);
    return NULL;
}

PyDoc_STRVAR(encode_doc,
"_encode(code, iterable)\n\
\n\
like the encode method without code checking");


/* Binary Tree definition */
typedef struct _bin_node
{
    PyObject *symbol;
    struct _bin_node *child[2];
} binode;


static binode *
new_binode(void)
{
    binode *nd;

    nd = PyMem_Malloc(sizeof *nd);
    if (nd == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    nd->symbol = NULL;
    nd->child[0] = NULL;
    nd->child[1] = NULL;
    return nd;
}

static void
delete_binode_tree(binode *root)
{
    if (root == NULL)
        return;

    delete_binode_tree(root->child[0]);
    delete_binode_tree(root->child[1]);
    PyMem_Free(root);
}

static int
insert_symbol(binode *root, bitarrayobject *self, PyObject *symbol)
{
    binode *nd = root, *prev;
    Py_ssize_t i;
    int k;

    for (i = 0; i < self->nbits; i++) {
        k = GETBIT(self, i);
        prev = nd;
        nd = nd->child[k];
        if (!nd) {
            nd = new_binode();
            if (nd == NULL)
                return -1;
            prev->child[k] = nd;
        }
    }

    if (nd->symbol) {
        PyErr_SetString(PyExc_ValueError, "prefix code ambiguous");
        return -1;
    }
    nd->symbol = symbol;
    return 0;
}

static binode *
make_tree(PyObject *codedict)
{
    binode *root;
    PyObject *symbol, *array;
    Py_ssize_t pos = 0;

    root = new_binode();
    if (root == NULL)
        return NULL;

    while (PyDict_Next(codedict, &pos, &symbol, &array)) {
        if (insert_symbol(root, (bitarrayobject *) array, symbol) < 0) {
            delete_binode_tree(root);
            return NULL;
        }
    }
    return root;
}

static PyObject *
tree_traverse(bitarrayobject *self, idx_t *indexp, binode *tree)
{
    binode *nd = tree;
    int k;

    while (*indexp < self->nbits) {
        k = GETBIT(self, *indexp);
        (*indexp)++;
        nd = nd->child[k];
        if (nd == NULL) {
            PyErr_SetString(PyExc_ValueError,
                            "prefix code does not match data in bitarray");
            return NULL;
        }
        if (nd->symbol)  /* leaf */
            return nd->symbol;
    }
    if (nd != tree)
        PyErr_SetString(PyExc_ValueError, "decoding not terminated");

    return NULL;
}

static PyObject *
bitarray_decode(bitarrayobject *self, PyObject *codedict)
{
    binode *tree, *nd;
    PyObject *list;
    Py_ssize_t i;
    int k;

    tree = make_tree(codedict);
    if (tree == NULL || PyErr_Occurred())
        return NULL;

    nd = tree;
    list = PyList_New(0);
    if (list == NULL) {
        delete_binode_tree(tree);
        return NULL;
    }
    for (i = 0; i < self->nbits; i++) {
        k = GETBIT(self, i);
        nd = nd->child[k];
        if (nd == NULL) {
            PyErr_SetString(PyExc_ValueError,
                            "prefix code does not match data in bitarray");
            goto error;
        }
        if (nd->symbol) {  /* leaf */
            if (PyList_Append(list, nd->symbol) < 0)
                goto error;
            nd = tree;
        }
    }
    if (nd != tree) {
        PyErr_SetString(PyExc_ValueError, "decoding not terminated");
        goto error;
    }
    delete_binode_tree(tree);
    return list;

error:
    delete_binode_tree(tree);
    Py_DECREF(list);
    return NULL;
}

PyDoc_STRVAR(decode_doc,
"_decode(codedict) -> list\n\
\n\
Given a code dictionary, decode the content of the bitarray and return\n\
the list of symbols.");

/*********************** (Bitarray) Decode Iterator *********************/


typedef struct {
    PyObject_HEAD
    bitarrayobject *bao;        /* bitarray we're searching in */
    binode *tree;               /* prefix tree containing symbols */
    idx_t index;                /* current index in bitarray */
} decodeiterobject;

static PyTypeObject DecodeIter_Type;

#define DecodeIter_Check(op)  PyObject_TypeCheck(op, &DecodeIter_Type)



/* create a new initialized bitarray search iterator object */
static PyObject *
bitarray_iterdecode(bitarrayobject *self, PyObject *codedict)
{
    decodeiterobject *it;  /* iterator to be returned */
    binode *tree;

    tree = make_tree(codedict);
    if (tree == NULL || PyErr_Occurred())
        return NULL;

    it = PyObject_GC_New(decodeiterobject, &DecodeIter_Type);
    if (it == NULL)
        return NULL;

    it->tree = tree;

    Py_INCREF(self);
    it->bao = self;
    it->index = 0;
    PyObject_GC_Track(it);
    return (PyObject *) it;
}

PyDoc_STRVAR(iterdecode_doc,
"_iterdecode(codedict) -> iterator\n\
\n\
Given a code dictionary, decode the content of the bitarray and iterate\n\
over the represented symbols.");

static PyObject *
decodeiter_next(decodeiterobject *it)
{
    PyObject *symbol;

    assert(DecodeIter_Check(it));
    symbol = tree_traverse(it->bao, &(it->index), it->tree);
    if (symbol == NULL)  /* stop iteration OR error occured */
        return NULL;
    Py_INCREF(symbol);
    return symbol;
}

static void
decodeiter_dealloc(decodeiterobject *it)
{
    delete_binode_tree(it->tree);
    PyObject_GC_UnTrack(it);
    Py_XDECREF(it->bao);
    PyObject_GC_Del(it);
}

static int
decodeiter_traverse(decodeiterobject *it, visitproc visit, void *arg)
{
    Py_VISIT(it->bao);
    return 0;
}

static PyTypeObject DecodeIter_Type = {
#ifdef IS_PY3K
    PyVarObject_HEAD_INIT(&DecodeIter_Type, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                                        /* ob_size */
#endif
    "bitarraydecodeiterator",                 /* tp_name */
    sizeof(decodeiterobject),                 /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    (destructor) decodeiter_dealloc,          /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    PyObject_GenericGetAttr,                  /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,  /* tp_flags */
    0,                                        /* tp_doc */
    (traverseproc) decodeiter_traverse,       /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    PyObject_SelfIter,                        /* tp_iter */
    (iternextfunc) decodeiter_next,           /* tp_iternext */
    0,                                        /* tp_methods */
};

/*********************** (Bitarray) Search Iterator *********************/

typedef struct {
    PyObject_HEAD
    bitarrayobject *bao;        /* bitarray we're searching in */
    bitarrayobject *xa;         /* bitarray being searched for */
    idx_t p;                    /* current search position */
} searchiterobject;

static PyTypeObject SearchIter_Type;

#define SearchIter_Check(op)  PyObject_TypeCheck(op, &SearchIter_Type)

/* create a new initialized bitarray search iterator object */
static PyObject *
bitarray_itersearch(bitarrayobject *self, PyObject *x)
{
    searchiterobject *it;  /* iterator to be returned */
    bitarrayobject *xa;

    if (!bitarray_Check(x)) {
        PyErr_SetString(PyExc_TypeError, "bitarray expected for itersearch");
        return NULL;
    }
    xa = (bitarrayobject *) x;
    if (xa->nbits == 0) {
        PyErr_SetString(PyExc_ValueError, "can't search for empty bitarray");
        return NULL;
    }

    it = PyObject_GC_New(searchiterobject, &SearchIter_Type);
    if (it == NULL)
        return NULL;

    Py_INCREF(self);
    it->bao = self;
    Py_INCREF(xa);
    it->xa = xa;
    it->p = 0;  /* start search at position 0 */
    PyObject_GC_Track(it);
    return (PyObject *) it;
}

PyDoc_STRVAR(itersearch_doc,
"itersearch(bitarray) -> iterator\n\
\n\
Searches for the given a bitarray in self, and return an iterator over\n\
the start positions where bitarray matches self.");

static PyObject *
searchiter_next(searchiterobject *it)
{
    idx_t p;

    assert(SearchIter_Check(it));
    p = search(it->bao, it->xa, it->p);
    if (p < 0)  /* no more positions -- stop iteration */
        return NULL;
    it->p = p + 1;  /* next search position */
    return PyLong_FromLongLong(p);
}

static void
searchiter_dealloc(searchiterobject *it)
{
    PyObject_GC_UnTrack(it);
    Py_XDECREF(it->bao);
    Py_XDECREF(it->xa);
    PyObject_GC_Del(it);
}

static int
searchiter_traverse(searchiterobject *it, visitproc visit, void *arg)
{
    Py_VISIT(it->bao);
    return 0;
}

static PyTypeObject SearchIter_Type = {
#ifdef IS_PY3K
    PyVarObject_HEAD_INIT(&SearchIter_Type, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                                        /* ob_size */
#endif
    "bitarraysearchiterator",                 /* tp_name */
    sizeof(searchiterobject),                 /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    (destructor) searchiter_dealloc,          /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    PyObject_GenericGetAttr,                  /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,  /* tp_flags */
    0,                                        /* tp_doc */
    (traverseproc) searchiter_traverse,       /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    PyObject_SelfIter,                        /* tp_iter */
    (iternextfunc) searchiter_next,           /* tp_iternext */
    0,                                        /* tp_methods */
};

/*************************** Method definitions *************************/

static PyMethodDef
bitarray_methods[] = {
    {"all",          (PyCFunction) bitarray_all,         METH_NOARGS,
     all_doc},
    {"any",          (PyCFunction) bitarray_any,         METH_NOARGS,
     any_doc},
    {"append",       (PyCFunction) bitarray_append,      METH_O,
     append_doc},
    {"buffer_info",  (PyCFunction) bitarray_buffer_info, METH_NOARGS,
     buffer_info_doc},
    {"bytereverse",  (PyCFunction) bitarray_bytereverse, METH_NOARGS,
     bytereverse_doc},
    {"copy",         (PyCFunction) bitarray_copy,        METH_NOARGS,
     copy_doc},
    {"count",        (PyCFunction) bitarray_count,       METH_VARARGS,
     count_doc},
    {"_decode",      (PyCFunction) bitarray_decode,      METH_O,
     decode_doc},
    {"_iterdecode",  (PyCFunction) bitarray_iterdecode,  METH_O,
     iterdecode_doc},
    {"_encode",      (PyCFunction) bitarray_encode,      METH_VARARGS,
     encode_doc},
    {"endian",       (PyCFunction) bitarray_endian,      METH_NOARGS,
     endian_doc},
    {"extend",       (PyCFunction) bitarray_extend,      METH_O,
     extend_doc},
    {"fill",         (PyCFunction) bitarray_fill,        METH_NOARGS,
     fill_doc},
    {"fromfile",     (PyCFunction) bitarray_fromfile,    METH_VARARGS,
     fromfile_doc},
    {"frombytes",    (PyCFunction) bitarray_frombytes,   METH_O,
     frombytes_doc},
    {"index",        (PyCFunction) bitarray_index,       METH_VARARGS,
     index_doc},
    {"insert",       (PyCFunction) bitarray_insert,      METH_VARARGS,
     insert_doc},
    {"invert",       (PyCFunction) bitarray_invert,      METH_NOARGS,
     invert_doc},
    {"length",       (PyCFunction) bitarray_length,      METH_NOARGS,
     length_doc},
    {"pack",         (PyCFunction) bitarray_pack,        METH_O,
     pack_doc},
    {"pop",          (PyCFunction) bitarray_pop,         METH_VARARGS,
     pop_doc},
    {"remove",       (PyCFunction) bitarray_remove,      METH_O,
     remove_doc},
    {"reverse",      (PyCFunction) bitarray_reverse,     METH_NOARGS,
     reverse_doc},
    {"setall",       (PyCFunction) bitarray_setall,      METH_O,
     setall_doc},
    {"search",       (PyCFunction) bitarray_search,      METH_VARARGS,
     search_doc},
    {"itersearch",   (PyCFunction) bitarray_itersearch,  METH_O,
     itersearch_doc},
    {"sort",         (PyCFunction) bitarray_sort,        METH_VARARGS |
                                                         METH_KEYWORDS,
     sort_doc},
    {"tofile",       (PyCFunction) bitarray_tofile,      METH_O,
     tofile_doc},
    {"tolist",       (PyCFunction) bitarray_tolist,      METH_NOARGS,
     tolist_doc},
    {"tobytes",      (PyCFunction) bitarray_tobytes,     METH_NOARGS,
     tobytes_doc},
    {"to01",         (PyCFunction) bitarray_to01,        METH_NOARGS,
     to01_doc},
    {"unpack",       (PyCFunction) bitarray_unpack,      METH_VARARGS |
                                                         METH_KEYWORDS,
     unpack_doc},

    /* special methods */
    {"__copy__",     (PyCFunction) bitarray_copy,        METH_NOARGS,
     copy_doc},
    {"__deepcopy__", (PyCFunction) bitarray_copy,        METH_O,
     copy_doc},
    {"__len__",      (PyCFunction) bitarray_length,      METH_NOARGS,
     len_doc},
    {"__contains__", (PyCFunction) bitarray_contains,    METH_O,
     contains_doc},
    {"__reduce__",   (PyCFunction) bitarray_reduce,      METH_NOARGS,
     reduce_doc},

    /* slice methods */
    {"__delitem__",  (PyCFunction) bitarray_delitem,     METH_O,       0},
    {"__getitem__",  (PyCFunction) bitarray_getitem,     METH_O,       0},
    {"__setitem__",  (PyCFunction) bitarray_setitem,     METH_VARARGS, 0},

    /* number methods */
    {"__add__",      (PyCFunction) bitarray_add,         METH_O,       0},
    {"__iadd__",     (PyCFunction) bitarray_iadd,        METH_O,       0},
    {"__mul__",      (PyCFunction) bitarray_mul,         METH_O,       0},
    {"__rmul__",     (PyCFunction) bitarray_mul,         METH_O,       0},
    {"__imul__",     (PyCFunction) bitarray_imul,        METH_O,       0},
    {"__and__",      (PyCFunction) bitarray_and,         METH_O,       0},
    {"__or__",       (PyCFunction) bitarray_or,          METH_O,       0},
    {"__xor__",      (PyCFunction) bitarray_xor,         METH_O,       0},
    {"__iand__",     (PyCFunction) bitarray_iand,        METH_O,       0},
    {"__ior__",      (PyCFunction) bitarray_ior,         METH_O,       0},
    {"__ixor__",     (PyCFunction) bitarray_ixor,        METH_O,       0},
    {"__invert__",   (PyCFunction) bitarray_cpinvert,    METH_NOARGS,  0},

    {NULL,           NULL}  /* sentinel */
};


static PyObject *
bitarray_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyObject *a;  /* to be returned in some cases */
    PyObject *initial = NULL;
    char *endian_str = NULL;
    int endian;
    static char* kwlist[] = {"initial", "endian", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds,
                        "|Os:bitarray", kwlist, &initial, &endian_str))
        return NULL;

    if (endian_str == NULL) {
        endian = DEFAULT_ENDIAN;  /* use default value */
    }
    else if (strcmp(endian_str, "little") == 0) {
        endian = 0;
    }
    else if (strcmp(endian_str, "big") == 0) {
        endian = 1;
    }
    else {
        PyErr_SetString(PyExc_ValueError,
                        "endian must be 'little' or 'big'");
        return NULL;
    }

    /* no arg or None */
    if (initial == NULL || initial == Py_None)
        return newbitarrayobject(type, 0, endian);

    /* int, long */
    if (IS_INDEX(initial)) {
        idx_t nbits = 0;

        if (getIndex(initial, &nbits) < 0)
            return NULL;
        if (nbits < 0) {
            PyErr_SetString(PyExc_ValueError,
                            "cannot create bitarray with negative length");
            return NULL;
        }
        return newbitarrayobject(type, nbits, endian);
    }

    /* from bitarray itself */
    if (bitarray_Check(initial)) {
#define np  ((bitarrayobject *) initial)
        a = newbitarrayobject(type, np->nbits,
                              endian_str == NULL ? np->endian : endian);
        if (a == NULL)
            return NULL;
        memcpy(((bitarrayobject *) a)->ob_item, np->ob_item, Py_SIZE(np));
#undef np
        return a;
    }

    /* string */
    if (PyString_Check(initial)) {
        Py_ssize_t strlen;
        char *str;

        strlen = PyString_Size(initial);
        if (strlen == 0)        /* empty string */
            return newbitarrayobject(type, 0, endian);

        str = PyString_AsString(initial);
        if (0 <= str[0] && str[0] < 8) {
            /* when the first character is smaller than 8, it indicates the
               number of unused bits at the end, and rest of the bytes
               consist of the raw binary data, this is used for pickling */
            if (strlen == 1 && str[0] > 0) {
                PyErr_Format(PyExc_ValueError,
                             "did not expect 0x0%d", (int) str[0]);
                return NULL;
            }
            a = newbitarrayobject(type, BITS(strlen - 1) - ((idx_t) str[0]),
                                  endian);
            if (a == NULL)
                return NULL;
            memcpy(((bitarrayobject *) a)->ob_item, str + 1, strlen - 1);
            return a;
        }
    }

    /* leave remaining type dispatch to the extend method */
    a = newbitarrayobject(type, 0, endian);
    if (a == NULL)
        return NULL;
    if (extend_dispatch((bitarrayobject *) a, initial) < 0) {
        Py_DECREF(a);
        return NULL;
    }
    return a;
}


static PyObject *
richcompare(PyObject *v, PyObject *w, int op)
{
    int cmp, vi, wi;
    idx_t i, vs, ws;

    if (!bitarray_Check(v) || !bitarray_Check(w)) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }
#define va  ((bitarrayobject *) v)
#define wa  ((bitarrayobject *) w)
    vs = va->nbits;
    ws = wa->nbits;
    if (vs != ws) {
        /* shortcut for EQ/NE: if sizes differ, the bitarrays differ */
        if (op == Py_EQ)
            Py_RETURN_FALSE;
        if (op == Py_NE)
            Py_RETURN_TRUE;
    }

    /* to avoid uninitialized warning for some compilers */
    vi = wi = 0;
    /* search for the first index where items are different */
    for (i = 0; i < vs && i < ws; i++) {
        vi = GETBIT(va, i);
        wi = GETBIT(wa, i);
        if (vi != wi) {
            /* we have an item that differs -- first, shortcut for EQ/NE */
            if (op == Py_EQ)
                Py_RETURN_FALSE;
            if (op == Py_NE)
                Py_RETURN_TRUE;
            /* compare the final item using the proper operator */
            switch (op) {
            case Py_LT: cmp = vi <  wi; break;
            case Py_LE: cmp = vi <= wi; break;
            case Py_EQ: cmp = vi == wi; break;
            case Py_NE: cmp = vi != wi; break;
            case Py_GT: cmp = vi >  wi; break;
            case Py_GE: cmp = vi >= wi; break;
            default: return NULL;  /* cannot happen */
            }
            return PyBool_FromLong((long) cmp);
        }
    }
#undef va
#undef wa

    /* no more items to compare -- compare sizes */
    switch (op) {
    case Py_LT: cmp = vs <  ws; break;
    case Py_LE: cmp = vs <= ws; break;
    case Py_EQ: cmp = vs == ws; break;
    case Py_NE: cmp = vs != ws; break;
    case Py_GT: cmp = vs >  ws; break;
    case Py_GE: cmp = vs >= ws; break;
    default: return NULL;  /* cannot happen */
    }
    return PyBool_FromLong((long) cmp);
}

/************************** Bitarray Iterator **************************/

typedef struct {
    PyObject_HEAD
    bitarrayobject *bao;        /* bitarray we're iterating over */
    idx_t index;                /* current index in bitarray */
} bitarrayiterobject;

static PyTypeObject BitarrayIter_Type;

#define BitarrayIter_Check(op)  PyObject_TypeCheck(op, &BitarrayIter_Type)

/* create a new initialized bitarray iterator object, this object is
   returned when calling item(a) */
static PyObject *
bitarray_iter(bitarrayobject *self)
{
    bitarrayiterobject *it;

    assert(bitarray_Check(self));
    it = PyObject_GC_New(bitarrayiterobject, &BitarrayIter_Type);
    if (it == NULL)
        return NULL;

    Py_INCREF(self);
    it->bao = self;
    it->index = 0;
    PyObject_GC_Track(it);
    return (PyObject *) it;
}

static PyObject *
bitarrayiter_next(bitarrayiterobject *it)
{
    long vi;

    assert(BitarrayIter_Check(it));
    if (it->index < it->bao->nbits) {
        vi = GETBIT(it->bao, it->index);
        it->index++;
        return PyBool_FromLong(vi);
    }
    return NULL;  /* stop iteration */
}

static void
bitarrayiter_dealloc(bitarrayiterobject *it)
{
    PyObject_GC_UnTrack(it);
    Py_XDECREF(it->bao);
    PyObject_GC_Del(it);
}

static int
bitarrayiter_traverse(bitarrayiterobject *it, visitproc visit, void *arg)
{
    Py_VISIT(it->bao);
    return 0;
}

static PyTypeObject BitarrayIter_Type = {
#ifdef IS_PY3K
    PyVarObject_HEAD_INIT(&BitarrayIter_Type, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                                        /* ob_size */
#endif
    "bitarrayiterator",                       /* tp_name */
    sizeof(bitarrayiterobject),               /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    (destructor) bitarrayiter_dealloc,        /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    PyObject_GenericGetAttr,                  /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,  /* tp_flags */
    0,                                        /* tp_doc */
    (traverseproc) bitarrayiter_traverse,     /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    PyObject_SelfIter,                        /* tp_iter */
    (iternextfunc) bitarrayiter_next,         /* tp_iternext */
    0,                                        /* tp_methods */
};

/********************* Bitarray Buffer Interface ************************/
#ifdef WITH_BUFFER

#if PY_MAJOR_VERSION == 2
static Py_ssize_t
bitarray_buffer_getreadbuf(bitarrayobject *self,
                           Py_ssize_t index, const void **ptr)
{
    if (index != 0) {
        PyErr_SetString(PyExc_SystemError, "accessing non-existent segment");
        return -1;
    }
    *ptr = (void *) self->ob_item;
    return Py_SIZE(self);
}

static Py_ssize_t
bitarray_buffer_getwritebuf(bitarrayobject *self,
                            Py_ssize_t index, const void **ptr)
{
    if (index != 0) {
        PyErr_SetString(PyExc_SystemError, "accessing non-existent segment");
        return -1;
    }
    *ptr = (void *) self->ob_item;
    return Py_SIZE(self);
}

static Py_ssize_t
bitarray_buffer_getsegcount(bitarrayobject *self, Py_ssize_t *lenp)
{
    if (lenp)
        *lenp = Py_SIZE(self);
    return 1;
}

static Py_ssize_t
bitarray_buffer_getcharbuf(bitarrayobject *self,
                           Py_ssize_t index, const char **ptr)
{
    if (index != 0) {
        PyErr_SetString(PyExc_SystemError, "accessing non-existent segment");
        return -1;
    }
    *ptr = self->ob_item;
    return Py_SIZE(self);
}

#endif

static int
bitarray_getbuffer(bitarrayobject *self, Py_buffer *view, int flags)
{
    int ret;
    void *ptr;

    if (view == NULL) {
        self->ob_exports++;
        return 0;
    }
    ptr = (void *) self->ob_item;
    ret = PyBuffer_FillInfo(view, (PyObject *) self, ptr,
                            Py_SIZE(self), 0, flags);
    if (ret >= 0) {
        self->ob_exports++;
    }
    return ret;
}

static void
bitarray_releasebuffer(bitarrayobject *self, Py_buffer *view)
{
    self->ob_exports--;
}

static PyBufferProcs bitarray_as_buffer = {
#if PY_MAJOR_VERSION == 2   /* old buffer protocol */
    (readbufferproc) bitarray_buffer_getreadbuf,
    (writebufferproc) bitarray_buffer_getwritebuf,
    (segcountproc) bitarray_buffer_getsegcount,
    (charbufferproc) bitarray_buffer_getcharbuf,
#endif
    (getbufferproc) bitarray_getbuffer,
    (releasebufferproc) bitarray_releasebuffer,
};

#endif  /* WITH_BUFFER */

/************************** Bitarray Type *******************************/

static PyTypeObject Bitarraytype = {
#ifdef IS_PY3K
    PyVarObject_HEAD_INIT(&Bitarraytype, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                                        /* ob_size */
#endif
    "bitarray._bitarray",                     /* tp_name */
    sizeof(bitarrayobject),                   /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    (destructor) bitarray_dealloc,            /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    (reprfunc) bitarray_repr,                 /* tp_repr */
    0,                                        /* tp_as_number*/
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    PyObject_GenericGetAttr,                  /* tp_getattro */
    0,                                        /* tp_setattro */
#ifdef WITH_BUFFER
    &bitarray_as_buffer,                      /* tp_as_buffer */
#else
    0,                                        /* tp_as_buffer */
#endif
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_WEAKREFS
#if defined(WITH_BUFFER) && PY_MAJOR_VERSION == 2
    | Py_TPFLAGS_HAVE_NEWBUFFER
#endif
    ,                                         /* tp_flags */
    0,                                        /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    richcompare,                              /* tp_richcompare */
    offsetof(bitarrayobject, weakreflist),    /* tp_weaklistoffset */
    (getiterfunc) bitarray_iter,              /* tp_iter */
    0,                                        /* tp_iternext */
    bitarray_methods,                         /* tp_methods */
    0,                                        /* tp_members */
    0,                                        /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    0,                                        /* tp_init */
    PyType_GenericAlloc,                      /* tp_alloc */
    bitarray_new,                             /* tp_new */
    PyObject_Del,                             /* tp_free */
};

/*************************** Module functions **********************/

static PyObject *
bitdiff(PyObject *self, PyObject *args)
{
    PyObject *a, *b;
    Py_ssize_t i;
    idx_t res = 0;
    unsigned char c;

    if (!PyArg_ParseTuple(args, "OO:bitdiff", &a, &b))
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
    setunused(aa);
    setunused(bb);
    for (i = 0; i < Py_SIZE(aa); i++) {
        c = aa->ob_item[i] ^ bb->ob_item[i];
        res += bitcount_lookup[c];
    }
#undef aa
#undef bb
    return PyLong_FromLongLong(res);
}

PyDoc_STRVAR(bitdiff_doc,
"bitdiff(a, b) -> int\n\
\n\
Return the difference between two bitarrays a and b.\n\
This is function does the same as (a ^ b).count(), but is more memory\n\
efficient, as no intermediate bitarray object gets created");


static PyObject *
bits2bytes(PyObject *self, PyObject *v)
{
    idx_t n = 0;

    if (!IS_INDEX(v)) {
        PyErr_SetString(PyExc_TypeError, "integer expected");
        return NULL;
    }
    if (getIndex(v, &n) < 0)
        return NULL;
    if (n < 0) {
        PyErr_SetString(PyExc_ValueError, "positive value expected");
        return NULL;
    }
    return PyLong_FromLongLong(BYTES(n));
}

PyDoc_STRVAR(bits2bytes_doc,
"bits2bytes(n) -> int\n\
\n\
Return the number of bytes necessary to store n bits.");


static PyObject *
sysinfo(void)
{
    return Py_BuildValue("iiiiL",
                         (int) sizeof(void *),
                         (int) sizeof(size_t),
                         (int) sizeof(Py_ssize_t),
                         (int) sizeof(idx_t),
                         (idx_t) PY_SSIZE_T_MAX);
}

PyDoc_STRVAR(sysinfo_doc,
"_sysinfo() -> tuple\n\
\n\
tuple(sizeof(void *),\n\
      sizeof(size_t),\n\
      sizeof(Py_ssize_t),\n\
      sizeof(idx_t),\n\
      PY_SSIZE_T_MAX)");


static PyMethodDef module_functions[] = {
    {"bitdiff",    (PyCFunction) bitdiff,    METH_VARARGS, bitdiff_doc   },
    {"bits2bytes", (PyCFunction) bits2bytes, METH_O,       bits2bytes_doc},
    {"_sysinfo",   (PyCFunction) sysinfo,    METH_NOARGS,  sysinfo_doc   },
    {NULL,         NULL}  /* sentinel */
};

/*********************** Install Module **************************/

#ifdef IS_PY3K
static PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_bitarray", 0, -1, module_functions,
};
PyMODINIT_FUNC
PyInit__bitarray(void)
#else
PyMODINIT_FUNC
init_bitarray(void)
#endif
{
    PyObject *m;

    Py_TYPE(&Bitarraytype) = &PyType_Type;
    Py_TYPE(&SearchIter_Type) = &PyType_Type;
    Py_TYPE(&DecodeIter_Type) = &PyType_Type;
    Py_TYPE(&BitarrayIter_Type) = &PyType_Type;
#ifdef IS_PY3K
    m = PyModule_Create(&moduledef);
    if (m == NULL)
        return NULL;
#else
    m = Py_InitModule3("_bitarray", module_functions, 0);
    if (m == NULL)
        return;
#endif

    Py_INCREF((PyObject *) &Bitarraytype);
    PyModule_AddObject(m, "_bitarray", (PyObject *) &Bitarraytype);
#ifdef IS_PY3K
    return m;
#endif
}
