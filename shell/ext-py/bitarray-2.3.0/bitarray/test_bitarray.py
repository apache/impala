"""
Tests for bitarray

Author: Ilan Schnell
"""
from __future__ import absolute_import

import re
import os
import sys
import unittest
import tempfile
import shutil
from random import randint

# imports needed inside tests
import copy
import pickle
import itertools
import shelve


is_py3k = bool(sys.version_info[0] == 3)

if is_py3k:
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO  # type: ignore
    range = xrange  # type: ignore


from bitarray import (bitarray, frozenbitarray, bits2bytes, decodetree,
                      get_default_endian, _set_default_endian,
                      _sysinfo, __version__)

SYSINFO = _sysinfo()
DEBUG = SYSINFO[6]

def buffer_info(a, key=None):
    fields = (
        "address",    # 0. address of byte buffer
        "size",       # 1. buffer size in bytes
        "endian",     # 2. bit endianness
        "padding",    # 3. number of pad bits
        "allocated",  # 4. allocated memory
        "readonly",   # 5. memory is read-only
        "imported",   # 6. buffer is imported
        "exports",    # 7. number of buffer exports
    )
    info = a.buffer_info()
    res = dict(zip(fields, info))
    return res if key is None else res[key]


# avoid importing from bitarray.util
def zeros(n, endian=None):
    a = bitarray(n, endian or get_default_endian())
    a.setall(0)
    return a

def urandom(n, endian=None):
    a = bitarray(0, endian or get_default_endian())
    a.frombytes(os.urandom(bits2bytes(n)))
    del a[n:]
    return a

tests = []  # type: list

class Util(object):

    @staticmethod
    def random_endian():
        return ['little', 'big'][randint(0, 1)]

    @staticmethod
    def randombitarrays(start=0):
        for n in list(range(start, 25)) + [randint(1000, 2000)]:
            a = bitarray(endian=['little', 'big'][randint(0, 1)])
            a.frombytes(os.urandom(bits2bytes(n)))
            del a[n:]
            yield a

    def randomlists(self):
        for a in self.randombitarrays():
            yield a.tolist()

    @staticmethod
    def rndsliceidx(length):
        if randint(0, 1):
            return None
        else:
            return randint(-length - 5, length + 5)

    @staticmethod
    def other_endian(endian):
        t = {'little': 'big',
             'big': 'little'}
        return t[endian]

    @staticmethod
    def slicelen(s, length):
        assert isinstance(s, slice)
        start, stop, step = s.indices(length)
        slicelength = (stop - start + (1 if step < 0 else -1)) // step + 1
        if slicelength < 0:
            slicelength = 0
        return slicelength

    def check_obj(self, a):
        self.assertIsInstance(a, bitarray)

        ptr, size, endian, unused, alloc, readonly, buf, exports = \
                                                          a.buffer_info()

        self.assertEqual(size, bits2bytes(len(a)))
        self.assertEqual(unused, 8 * size - len(a))
        self.assertTrue(0 <= unused < 8)
        self.assertEqual(endian, a.endian())
        self.assertTrue(endian in ('little', 'big'))

        if buf:
            # imported buffer implies that no extra memory is allocated
            self.assertEqual(alloc, 0)
            # an imported buffer will always have a multiple of 8 bits
            self.assertEqual(len(a) % 8, 0)
            self.assertEqual(len(a), 8 * size)
            self.assertEqual(unused, 0)
        else:
            # the allocated memory is always larger than the buffer size
            self.assertTrue(alloc >= size)

        if ptr == 0:
            # the buffer being a NULL pointer implies that the buffer size
            # and the allocated memory size are 0
            self.assertEqual(size, 0)
            self.assertEqual(alloc, 0)

        if type(a).__name__ == 'frozenbitarray':
            # frozenbitarray have read-only memory
            self.assertEqual(readonly, 1)
        elif not buf:
            # otherwise, unless the buffer is imported, it is writable
            self.assertEqual(readonly, 0)

    def assertEQUAL(self, a, b):
        self.assertEqual(a, b)
        self.assertEqual(a.endian(), b.endian())

    def assertIsType(self, a, b):
        self.assertEqual(type(a).__name__, b)
        self.assertEqual(
            repr(type(a)), "<%s 'bitarray.%s'>" %
            ('class' if is_py3k or b == 'frozenbitarray' else 'type', b))

    def assertBitEqual(self, x, y):
        for z in x, y:
            self.assertEqual('01'[z], repr(z))
        self.assertEqual(x, y)

    def assertStopIteration(self, it):
        self.assertRaises(StopIteration, next, it)

    def assertRaisesMessage(self, excClass, msg, callable, *args, **kwargs):
        try:
            callable(*args, **kwargs)
            raise AssertionError("%s not raised" % excClass.__name__)
        except excClass as e:
            if msg != str(e):
                raise AssertionError("message: %s\n got: %s" % (msg, e))

# ---------------------------------------------------------------------------

class TestsModuleFunctions(unittest.TestCase, Util):

    def test_version_string(self):
        # the version string is not a function, but test it here anyway
        self.assertIsInstance(__version__, str)

    def test_sysinfo(self):
        info = _sysinfo()
        self.assertIsInstance(info, tuple)
        for x in info:
            self.assertIsInstance(x, int)

    def test_set_default_endian(self):
        self.assertRaises(TypeError, _set_default_endian, 0)
        self.assertRaises(TypeError, _set_default_endian, 'little', 0)
        self.assertRaises(ValueError, _set_default_endian, 'foo')
        for default_endian in 'big', 'little', u'big', u'little':
            _set_default_endian(default_endian)
            a = bitarray()
            self.assertEqual(a.endian(), default_endian)
            for x in None, 0, 64, '10111', [1, 0]:
                a = bitarray(x)
                self.assertEqual(a.endian(), default_endian)

            for endian in 'big', 'little':
                a = bitarray(endian=endian)
                self.assertEqual(a.endian(), endian)

            # make sure that calling _set_default_endian wrong does not
            # change the default endianness
            self.assertRaises(ValueError, _set_default_endian, 'foobar')
            self.assertEqual(bitarray().endian(), default_endian)

    def test_get_default_endian(self):
        # takes no arguments
        self.assertRaises(TypeError, get_default_endian, 'big')
        for default_endian in 'big', 'little':
            _set_default_endian(default_endian)
            endian = get_default_endian()
            self.assertEqual(endian, default_endian)
            self.assertIsInstance(endian, str)

    def test_bits2bytes(self):
        for arg in 'foo', [], None, {}, 187.0, -4.0:
            self.assertRaises(TypeError, bits2bytes, arg)

        self.assertRaises(TypeError, bits2bytes)
        self.assertRaises(TypeError, bits2bytes, 1, 2)

        self.assertRaises(ValueError, bits2bytes, -1)
        self.assertRaises(ValueError, bits2bytes, -924)

        self.assertEqual(bits2bytes(0), 0)
        for n in range(1, 100):
            m = bits2bytes(n)
            self.assertEqual(m, (n - 1) // 8 + 1)
            self.assertIsInstance(m, int)

        for n, m in [(0, 0), (1, 1), (2, 1), (7, 1), (8, 1), (9, 2),
                     (10, 2), (15, 2), (16, 2), (64, 8), (65, 9),
                     (2**31, 2**28), (2**32, 2**29), (2**34, 2**31),
                     (2**34+793, 2**31+100), (2**35-8, 2**32-1),
                     (2**62, 2**59), (2**63-8, 2**60-1)]:
            self.assertEqual(bits2bytes(n), m)

tests.append(TestsModuleFunctions)

# ---------------------------------------------------------------------------

class CreateObjectTests(unittest.TestCase, Util):

    def test_noInitializer(self):
        a = bitarray()
        self.assertEqual(len(a), 0)
        self.assertEqual(a.tolist(), [])
        self.assertIsType(a, 'bitarray')
        self.check_obj(a)

    def test_endian(self):
        a = bitarray(endian='little')
        a.frombytes(b'ABC')
        self.assertEqual(a.endian(), 'little')
        self.assertIsInstance(a.endian(), str)
        self.check_obj(a)

        b = bitarray(endian='big')
        b.frombytes(b'ABC')
        self.assertEqual(b.endian(), 'big')
        self.assertIsInstance(a.endian(), str)
        self.check_obj(b)

        self.assertNotEqual(a, b)
        self.assertEqual(a.tobytes(), b.tobytes())

    def test_endian_default(self):
        _set_default_endian('big')
        a_big = bitarray()
        _set_default_endian('little')
        a_little = bitarray()
        _set_default_endian('big')

        self.assertEqual(a_big.endian(), 'big')
        self.assertEqual(a_little.endian(), 'little')

    def test_endian_wrong(self):
        self.assertRaises(TypeError, bitarray, endian=0)
        self.assertRaises(ValueError, bitarray, endian='')
        self.assertRaisesMessage(
            ValueError,
            "bit endianness must be either 'little' or 'big', got: 'foo'",
            bitarray, endian='foo')
        self.assertRaisesMessage(TypeError,
                                 "'ellipsis' object is not iterable",
                                 bitarray, Ellipsis)

    def test_buffer(self):
        # buffer requires no initial argument
        self.assertRaises(TypeError, bitarray, 5, buffer=b'DATA\0')

        for endian in 'big', 'little':
            a = bitarray(buffer=b'', endian=endian)
            self.assertEQUAL(a, bitarray(0, endian))

            _set_default_endian(endian)
            a = bitarray(buffer=b'A')
            self.assertEqual(a.endian(), endian)
            self.assertEqual(len(a), 8)

        a = bitarray(buffer=b'\xf0', endian='little')
        self.assertRaises(TypeError, a.clear)
        self.assertRaises(TypeError, a.__setitem__, 3, 1)
        self.assertEQUAL(a, bitarray('00001111', 'little'))
        self.check_obj(a)

        # positinal arguments
        a = bitarray(None, 'big', bytearray([15]))
        self.assertEQUAL(a, bitarray('00001111', 'big'))
        a = bitarray(None, 'little', None)
        self.assertEQUAL(a, bitarray(0, 'little'))

    def test_integers(self):
        for n in range(50):
            a = bitarray(n)
            self.assertEqual(len(a), n)
            self.check_obj(a)

            a = bitarray(int(n))
            self.assertEqual(len(a), n)
            self.check_obj(a)

        if not is_py3k:
            a = bitarray(long(29))
            self.assertEqual(len(a), 29)

        self.assertRaises(ValueError, bitarray, -1)
        self.assertRaises(ValueError, bitarray, -924)

    def test_list(self):
        lst = [0, 1, False, True]
        a = bitarray(lst)
        self.assertEqual(a, bitarray('0101'))
        self.check_obj(a)

        if not is_py3k:
            a = bitarray([long(1), long(0)])
            self.assertEqual(a, bitarray('10'))

        self.assertRaises(ValueError, bitarray, [0, 1, 2])
        self.assertRaises(TypeError, bitarray, [0, 1, None])

        for n in range(50):
            lst = [bool(randint(0, 1)) for d in range(n)]
            a = bitarray(lst)
            self.assertEqual(a.tolist(), lst)
            self.check_obj(a)

    def test_tuple(self):
        tup = (0, True, False, 1)
        a = bitarray(tup)
        self.assertEqual(a, bitarray('0101'))
        self.check_obj(a)

        self.assertRaises(ValueError, bitarray, (0, 1, 2))
        self.assertRaises(TypeError, bitarray, (0, 1, None))

        for n in range(50):
            lst = [bool(randint(0, 1)) for d in range(n)]
            a = bitarray(tuple(lst))
            self.assertEqual(a.tolist(), lst)
            self.check_obj(a)

    def test_iter1(self):
        for n in range(50):
            lst = [bool(randint(0, 1)) for d in range(n)]
            a = bitarray(iter(lst))
            self.assertEqual(a.tolist(), lst)
            self.check_obj(a)

    def test_iter2(self):
        for lst in self.randomlists():
            def foo():
                for x in lst:
                    yield x
            a = bitarray(foo())
            self.assertEqual(a, bitarray(lst))
            self.check_obj(a)

    def test_iter3(self):
        a = bitarray(itertools.repeat(False, 10))
        self.assertEqual(a, zeros(10))
        a = bitarray(itertools.repeat(1, 10))
        self.assertEqual(a, bitarray(10 * '1'))

    def test_range(self):
        self.assertEqual(bitarray(range(2)), bitarray('01'))
        self.assertRaises(ValueError, bitarray, range(0, 3))

    def test_string01(self):
        for s in ('0010111', u'0010111', '0010 111', u'0010 111',
                  '0010_111', u'0010_111'):
            a = bitarray(s)
            self.assertEqual(a.tolist(), [0, 0, 1, 0, 1, 1, 1])
            self.check_obj(a)

        for n in range(50):
            lst = [bool(randint(0, 1)) for d in range(n)]
            s = ''.join([['0', '1'][x] for x in lst])
            a = bitarray(s)
            self.assertEqual(a.tolist(), lst)
            self.check_obj(a)

        self.assertRaises(ValueError, bitarray, '01021')
        self.assertRaises(UnicodeEncodeError, bitarray, u'1\u26050')

    def test_string01_whitespace(self):
        whitespace = ' \n\r\t\v'
        a = bitarray(whitespace)
        self.assertEqual(a, bitarray())

        # For Python 2 (where strings are bytes), we are in the lucky
        # position that none of the valid characters ('\t'=9, '\n'=10,
        # '\v'=11, '\r'=13, ' '=32, '0'=48 and '1'=49) are valid header
        # bytes for deserialization 0..7, 16..23.  Therefore a string of
        # '0's and '1'a can start with any whitespace character, as well
        # as '0' or '1' obviously.
        for c in whitespace:
            a = bitarray(c + '1101110001')
            self.assertEqual(a, bitarray('1101110001'))

        a = bitarray(' 0\n1\r0\t1\v0 ')
        self.assertEqual(a, bitarray('01010'))

    def test_rawbytes(self):
        self.assertEqual(bitarray(b'\x00').endian(), 'little')
        self.assertEqual(bitarray(b'\x10').endian(), 'big')

        # this representation is used for pickling
        for s, r in [(b'\x00', ''), (b'\x07\xff', '1'), (b'\x03\xff', '11111'),
                     (b'\x01\x87\xda', '10000111 1101101')]:
            self.assertEqual(bitarray(s, endian='big'), bitarray(r))

        self.assertEQUAL(bitarray(b'\x12\x0f', 'little'),
                         bitarray('111100', 'little'))
        self.assertEQUAL(bitarray(b'\x02\x0f', 'big'),
                         bitarray('000011', 'big'))

        for a, s in [
                (bitarray(0, 'little'),   b'\x00'),
                (bitarray(0, 'big'),      b'\x10'),
                (bitarray('1', 'little'), b'\x07\x01'),
                (bitarray('1', 'big'),    b'\x17\x80'),
                (bitarray('11110000', 'little'), b'\x00\x0f'),
                (bitarray('11110000', 'big'),    b'\x10\xf0'),
        ]:
            self.assertEQUAL(bitarray(s), a)

    def test_rawbytes_invalid(self):
        for s in b'\x01', b'\x04', b'\x07', b'\x11', b'\x15', b'\x17':
            # this error is raised in newbitarray_from_pickle() (C function)
            if is_py3k:
                self.assertRaisesMessage(ValueError,
                                         "invalid header byte: 0x%02x" % s[0],
                                         bitarray, s)
            else:
                # Python 2: PyErr_Format() seems to handle "0x%02x"
                # incorrectly.  Oh well...
                self.assertRaises(ValueError, bitarray, s)

            a = bitarray(s + b'\x00')
            head = s[0] if is_py3k else ord(s[0])
            endian, unused = divmod(head, 16)
            self.assertEqual(a.endian(), ['little', 'big'][endian])
            self.assertEqual(len(a), 8 - unused)
            self.assertFalse(a.any())

        s = b'\x21'
        if is_py3k:
            # on Python 3, we don't allow bitarrays being created from bytes
            error = TypeError
            msg = ("cannot extend bitarray with 'bytes', use .pack() or "
                   ".frombytes() instead")
        else:
            # on Python 2, we have an invalid character in the string
            error = ValueError
            msg = ("expected '0' or '1' (or whitespace, or underscore), "
                   "got '!' (0x21)")
        self.assertRaisesMessage(error, msg, bitarray, s)

    def test_bitarray_simple(self):
        for n in range(10):
            a = bitarray(n)
            b = bitarray(a)
            self.assertFalse(a is b)
            self.assertEQUAL(a, b)

    def test_bitarray_endian(self):
        # Test creating a new bitarray with different endianness from an
        # existing bitarray.
        for endian in 'little', 'big':
            a = bitarray(endian=endian)
            b = bitarray(a)
            self.assertFalse(a is b)
            self.assertEQUAL(a, b)

            endian2 = self.other_endian(endian)
            b = bitarray(a, endian2)
            self.assertEqual(b.endian(), endian2)
            self.assertEqual(a, b)

        for a in self.randombitarrays():
            endian2 = self.other_endian(a.endian())
            b = bitarray(a, endian2)
            self.assertEqual(a, b)
            self.assertEqual(b.endian(), endian2)
            self.assertNotEqual(a.endian(), b.endian())

    def test_bitarray_endianness(self):
        a = bitarray('11100001', endian='little')
        b = bitarray(a, endian='big')
        self.assertEqual(a, b)
        self.assertNotEqual(a.tobytes(), b.tobytes())

        b.bytereverse()
        self.assertNotEqual(a, b)
        self.assertEqual(a.tobytes(), b.tobytes())

        c = bitarray('11100001', endian='big')
        self.assertEqual(a, c)

    def test_frozenbitarray(self):
        a = bitarray(frozenbitarray())
        self.assertEQUAL(a, bitarray())
        self.assertIsType(a, 'bitarray')

        for endian in 'little', 'big':
            a = bitarray(frozenbitarray('011', endian=endian))
            self.assertEQUAL(a, bitarray('011', endian))
            self.assertIsType(a, 'bitarray')

    def test_create_empty(self):
        for x in (None, 0, '', list(), tuple(), set(), dict(), u'',
                  bitarray(), frozenbitarray()):
            a = bitarray(x)
            self.assertEqual(len(a), 0)
            self.assertEQUAL(a, bitarray())

        if is_py3k:
            self.assertRaises(TypeError, bitarray, b'')
        else:
            self.assertEqual(bitarray(b''), bitarray())

    def test_wrong_args(self):
        # wrong types
        for x in False, True, Ellipsis, slice(0), 0.0, 0 + 0j:
            self.assertRaises(TypeError, bitarray, x)
        if is_py3k:
            self.assertRaises(TypeError, bitarray, b'10')
        else:
            self.assertEQUAL(bitarray(b'10'), bitarray('10'))
        # wrong values
        for x in -1, 'A':
            self.assertRaises(ValueError, bitarray, x)
        # test second (endian) argument
        self.assertRaises(TypeError, bitarray, 0, None)
        self.assertRaises(TypeError, bitarray, 0, 0)
        self.assertRaises(ValueError, bitarray, 0, 'foo')
        # too many args
        self.assertRaises(TypeError, bitarray, 0, 'big', 0)

tests.append(CreateObjectTests)

# ---------------------------------------------------------------------------

class ToObjectsTests(unittest.TestCase, Util):

    def test_numeric(self):
        a = bitarray()
        self.assertRaises(Exception, int, a)
        self.assertRaises(Exception, float, a)
        self.assertRaises(Exception, complex, a)

    def test_list(self):
        for a in self.randombitarrays():
            self.assertEqual(list(a), a.tolist())

    def test_tuple(self):
        for a in self.randombitarrays():
            self.assertEqual(tuple(a), tuple(a.tolist()))

tests.append(ToObjectsTests)

# ---------------------------------------------------------------------------

class MetaDataTests(unittest.TestCase):

    def test_buffer_info(self):
        a = bitarray(13, endian='little')
        self.assertEqual(a.buffer_info()[1:4], (2, 'little', 3))

        info = a.buffer_info()
        self.assertIsInstance(info, tuple)
        self.assertEqual(len(info), 8)
        for i, item in enumerate(info):
            if i == 2:
                self.assertIsInstance(item, str)
                continue
            self.assertIsInstance(item, int)

    def test_endian(self):
        for endian in 'big', 'little':
            a = bitarray(endian=endian)
            self.assertEqual(a.endian(), endian)

    def test_len(self):
        for n in range(100):
            a = bitarray(n)
            self.assertEqual(len(a), n)

tests.append(MetaDataTests)

# ---------------------------------------------------------------------------

class InternalTests(unittest.TestCase, Util):

    # Internal functionality exposed for the purpose of testing.
    # This class will only be part of the test suite in debug mode.

    def test_shift_r8_empty(self):
        a = bitarray()
        a._shift_r8(0, 0, 3)
        self.assertEqual(a, bitarray())

        a = urandom(80)
        for i in range(11):
            b = a.copy()
            a._shift_r8(i, i, 5)
            self.assertEqual(a, b)

    def test_shift_r8_explicit(self):
        x = bitarray('11000100 11111111 11100111 11111111 00001000')
        y = bitarray('11000100 00000111 11111111 00111111 00001000')
        x._shift_r8(1, 4, 5)
        self.assertEqual(x, y)

        x = bitarray('11000100 11110')
        y = bitarray('00011000 10011')
        x._shift_r8(0, 2, 3)
        self.assertEqual(x, y)

    def test_shift_r8_random_bytes(self):
        for N in range(100):
            a = randint(0, N)
            b = randint(a, N)
            n = randint(0, 7)
            x = urandom(8 * N, self.random_endian())
            y = x.copy()
            x._shift_r8(a, b, n)
            y[8 * a : 8 * b] >>= n
            self.assertEQUAL(x, y)
            self.assertEqual(len(x), 8 * N)

    def test_copy_n_explicit(self):
        x = bitarray('11000100 11110')
        #                 ^^^^ ^
        y = bitarray('0101110001')
        #              ^^^^^
        x._copy_n(4, y, 1, 5)
        self.assertEqual(x, bitarray('11001011 11110'))
        #                                 ^^^^ ^
        x = bitarray('10110111 101', 'little')
        y = x.copy()
        x._copy_n(3, x, 3, 7)  # copy region of x onto x
        self.assertEqual(x, y)
        x._copy_n(3, bitarray(x, 'big'), 3, 7)  # as before but other endian
        self.assertEqual(x, y)
        x._copy_n(5, bitarray(), 0, 0)  # copy empty bitarray onto x
        self.assertEqual(x, y)

    def test_copy_n_example(self):
        # example givin in bitarray/copy_n.txt
        y = bitarray(
            '00101110 11111001 01011101 11001011 10110000 01011110 011')
        x =  bitarray(
            '01011101 11100101 01110101 01011001 01110100 10001010 01111011')
        x._copy_n(21, y, 6, 31)
        self.assertEqual(x, bitarray(
            '01011101 11100101 01110101 11110010 10111011 10010111 01101011'))

    def check_copy_n(self, N, M, a, b, n):
        x = urandom(N, self.random_endian())
        x_lst = x.tolist()
        y = x if M < 0 else urandom(M, self.random_endian())
        x_lst[a:a + n] = y.tolist()[b:b + n]
        x._copy_n(a, y, b, n)
        self.assertEqual(x, bitarray(x_lst))
        self.assertEqual(len(x), N)
        self.check_obj(x)

    def test_copy_n_range(self):
        for a in range(8):
            for b in range(8):
                for n in range(90):
                    self.check_copy_n(100,  -1, a, b, n)
                    self.check_copy_n(100, 100, a, b, n)

    def test_copy_n_random_self(self):
        for N in range(500):
            n = randint(0, N)
            a = randint(0, N - n)
            b = randint(0, N - n)
            self.check_copy_n(N, -1, a, b, n)

    def test_copy_n_random_other(self):
        for N in range(500):
            M = randint(0, 5 + 2 * N)
            n = randint(0, min(N, M))
            a = randint(0, N - n)
            b = randint(0, M - n)
            self.check_copy_n(N, M, a, b, n)

    @staticmethod
    def getslice(a, start, slicelength):
        # this is the Python eqivalent of __getitem__ for slices with step=1
        b = bitarray(slicelength, a.endian())
        b._copy_n(0, a, start, slicelength)
        return b

    def test_getslice(self):
        for a in self.randombitarrays():
            a_lst = a.tolist()
            n = len(a)
            i = randint(0, n)
            j = randint(i, n)
            b = self.getslice(a, i, j - i)
            self.assertEqual(b.tolist(), a_lst[i:j])
            self.assertEQUAL(b, a[i:j])

if DEBUG:
    tests.append(InternalTests)

# ---------------------------------------------------------------------------

class SliceTests(unittest.TestCase, Util):

    def test_getitem_1(self):
        a = bitarray()
        self.assertRaises(IndexError, a.__getitem__,  0)
        a.append(True)
        self.assertBitEqual(a[0], 1)
        self.assertBitEqual(a[-1], 1)
        self.assertRaises(IndexError, a.__getitem__,  1)
        self.assertRaises(IndexError, a.__getitem__, -2)
        a.append(False)
        self.assertBitEqual(a[1], 0)
        self.assertBitEqual(a[-1], 0)
        self.assertRaises(IndexError, a.__getitem__,  2)
        self.assertRaises(IndexError, a.__getitem__, -3)
        self.assertRaises(TypeError, a.__getitem__, 1.5)
        self.assertRaises(TypeError, a.__getitem__, None)
        self.assertRaises(TypeError, a.__getitem__, 'A')

    def test_getitem_2(self):
        a = bitarray('1100010')
        for i, b in enumerate(a):
            self.assertBitEqual(a[i], b)
            self.assertBitEqual(a[i - 7], b)
        self.assertRaises(IndexError, a.__getitem__,  7)
        self.assertRaises(IndexError, a.__getitem__, -8)

    def test_getslice(self):
        a = bitarray('01001111 00001')
        self.assertEQUAL(a[:], a)
        self.assertFalse(a[:] is a)
        self.assertEQUAL(a[13:2:-3], bitarray('1010'))
        self.assertEQUAL(a[2:-1:4], bitarray('010'))
        self.assertEQUAL(a[::2], bitarray('0011001'))
        self.assertEQUAL(a[8:], bitarray('00001'))
        self.assertEQUAL(a[7:], bitarray('100001'))
        self.assertEQUAL(a[:8], bitarray('01001111'))
        self.assertEQUAL(a[::-1], bitarray('10000111 10010'))
        self.assertEQUAL(a[:8:-1], bitarray('1000'))

        self.assertRaises(ValueError, a.__getitem__, slice(None, None, 0))
        self.assertRaises(TypeError, a.__getitem__, (1, 2))

    def test_getslice_random(self):
        for a in self.randombitarrays(start=1):
            aa = a.tolist()
            la = len(a)
            for _ in range(10):
                step = self.rndsliceidx(la) or None
                s = slice(self.rndsliceidx(la), self.rndsliceidx(la), step)
                self.assertEQUAL(a[s], bitarray(aa[s], endian=a.endian()))

    def test_getslice_random2(self):
        n = randint(1000, 2000)
        a = urandom(n, self.random_endian())
        sa = a.to01()
        for _ in range(50):
            i = randint(0, n)
            j = randint(i, n)
            b = a[i:j]
            self.assertEqual(b.to01(), sa[i:j])
            self.assertEqual(len(b), j - i)
            self.assertEqual(b.endian(), a.endian())

    def test_setitem_simple(self):
        a = bitarray('0')
        a[0] = 1
        self.assertEqual(a, bitarray('1'))

        a = bitarray(2)
        a[0] = 0
        a[1] = 1
        self.assertEqual(a, bitarray('01'))
        a[-1] = 0
        a[-2] = 1
        self.assertEqual(a, bitarray('10'))

        self.assertRaises(ValueError, a.__setitem__, 0, -1)
        self.assertRaises(TypeError, a.__setitem__, 1, None)

        self.assertRaises(IndexError, a.__setitem__,  2, True)
        self.assertRaises(IndexError, a.__setitem__, -3, False)
        self.assertRaises(TypeError, a.__setitem__, 1.5, 1)  # see issue 114
        self.assertRaises(TypeError, a.__setitem__, None, 0)
        self.assertRaises(TypeError, a.__setitem__, 'a', True)
        self.assertEqual(a, bitarray('10'))

    def test_setitem_random(self):
        for a in self.randombitarrays(start=1):
            i = randint(0, len(a) - 1)
            aa = a.tolist()
            val = bool(randint(0, 1))
            a[i] = val
            aa[i] = val
            self.assertEqual(a.tolist(), aa)
            self.check_obj(a)

    def test_setslice_simple(self):
        for a in self.randombitarrays(start=1):
            la = len(a)
            b = bitarray(la)
            b[0:la] = bitarray(a)
            self.assertEqual(a, b)
            self.assertFalse(a is b)

            b = bitarray(la)
            b[:] = bitarray(a)
            self.assertEqual(a, b)
            self.assertFalse(a is b)

            b = bitarray(la)
            b[::-1] = bitarray(a)
            self.assertEqual(a.tolist()[::-1], b.tolist())

    def test_setslice_random(self):
        for a in self.randombitarrays(start=1):
            la = len(a)
            for _ in range(10):
                step = self.rndsliceidx(la) or None
                s = slice(self.rndsliceidx(la), self.rndsliceidx(la), step)
                lb = randint(0, 10) if step is None else self.slicelen(s, la)
                b = bitarray(lb)
                c = bitarray(a)
                c[s] = b
                self.check_obj(c)
                cc = a.tolist()
                cc[s] = b.tolist()
                self.assertEqual(c, bitarray(cc))

    def test_setslice_self_random(self):
        for a in self.randombitarrays():
            for step in -1, 1:
                s = slice(None, None, step)
                aa = a.tolist()
                a[s] = a
                aa[s] = aa
                self.assertEqual(a, bitarray(aa))

    def test_setslice_special(self):
        for n in 0, 1, 10, 87:
            a = urandom(n)
            for m in 0, 1, 10, 99:
                x = urandom(m)
                b = a.copy()
                b[n:n] = x  # insert at end - extend
                self.assertEqual(b, a + x)
                self.assertEqual(len(b), len(a) + len(x))
                b[0:0] = x  # insert at 0 - prepend
                self.assertEqual(b, x + a + x)
                self.check_obj(b)
                self.assertEqual(len(b), len(a) + 2 * len(x))

    def test_setslice_range(self):
        # tests C function insert_n()
        for endian in 'big', 'little':
            for n in range(500):
                a = urandom(n, endian)
                p = randint(0, n)
                m = randint(0, 500)

                x = urandom(m, self.random_endian())
                b = a.copy()
                b[p:p] = x
                self.assertEQUAL(b, a[:p] + x + a[p:])
                self.assertEqual(len(b), len(a) + m)
                self.check_obj(b)

    def test_setslice_resize(self):
        N, M = 200, 300
        for endian in 'big', 'little':
            for n in 0, randint(0, N), N:
                a = urandom(n, endian)
                for p1 in 0, randint(0, n), n:
                    for p2 in 0, randint(0, p1), p1, randint(0, n), n:
                        for m in 0, randint(0, M), M:
                            x = urandom(m, self.random_endian())
                            b = a.copy()
                            b[p1:p2] = x
                            b_lst = a.tolist()
                            b_lst[p1:p2] = x.tolist()
                            self.assertEqual(b.tolist(), b_lst)
                            if p1 <= p2:
                                self.assertEQUAL(b, a[:p1] + x + a[p2:])
                                self.assertEqual(len(b), n + p1 - p2 + len(x))
                            else:
                                self.assertEqual(b, a[:p1] + x + a[p1:])
                                self.assertEqual(len(b), n + len(x))
                            self.check_obj(b)

    def test_setslice_self(self):
        a = bitarray('1100111')
        a[::-1] = a
        self.assertEqual(a, bitarray('1110011'))
        a[4:] = a
        self.assertEqual(a, bitarray('11101110011'))
        a[:-5] = a
        self.assertEqual(a, bitarray('1110111001110011'))

        a = bitarray('01001')
        a[:-1] = a
        self.assertEqual(a, bitarray('010011'))
        a[2::] = a
        self.assertEqual(a, bitarray('01010011'))
        a[2:-2:1] = a
        self.assertEqual(a, bitarray('010101001111'))

        a = bitarray('011')
        a[2:2] = a
        self.assertEqual(a, bitarray('010111'))
        a[:] = a
        self.assertEqual(a, bitarray('010111'))

    def test_setslice_bitarray(self):
        a = bitarray('11111111 1111')
        a[2:6] = bitarray('0010')
        self.assertEqual(a, bitarray('11001011 1111'))
        a.setall(0)
        a[::2] = bitarray('111001')
        self.assertEqual(a, bitarray('10101000 0010'))
        a.setall(0)
        a[3:] = bitarray('111')
        self.assertEqual(a, bitarray('000111'))

        a = bitarray(12)
        a.setall(0)
        a[1:11:2] = bitarray('11101')
        self.assertEqual(a, bitarray('01010100 0100'))

        a = bitarray(12)
        a.setall(0)
        a[:-6:-1] = bitarray('10111')
        self.assertEqual(a, bitarray('00000001 1101'))

    def test_setslice_bitarray_2(self):
        a = bitarray('1111')
        a[3:3] = bitarray('000')  # insert
        self.assertEqual(a, bitarray('1110001'))
        a[2:5] = bitarray()  # remove
        self.assertEqual(a, bitarray('1101'))

        a = bitarray('1111')
        a[1:3] = bitarray('0000')
        self.assertEqual(a, bitarray('100001'))
        a[:] = bitarray('010')  # replace all values
        self.assertEqual(a, bitarray('010'))

        # assign slice to bitarray with different length
        a = bitarray('111111')
        a[3:4] = bitarray('00')
        self.assertEqual(a, bitarray('1110011'))
        a[2:5] = bitarray('0')  # remove
        self.assertEqual(a, bitarray('11011'))

    def test_setslice_bitarray_random_same_length(self):
        for endian in 'little', 'big':
            for _ in range(100):
                n = randint(0, 200)
                a = urandom(n, endian)
                lst_a = a.tolist()
                b = urandom(randint(0, n), self.random_endian())
                lst_b = b.tolist()
                i = randint(0, n - len(b))
                j = i + len(b)
                self.assertEqual(j - i, len(b))
                a[i:j] = b
                lst_a[i:j] = lst_b
                self.assertEqual(a.tolist(), lst_a)
                # a didn't change length
                self.assertEqual(len(a), n)
                self.assertEqual(a.endian(), endian)
                self.check_obj(a)

    def test_setslice_bitarray_random_step_1(self):
        for _ in range(50):
            n = randint(0, 300)
            a = urandom(n, self.random_endian())
            lst_a = a.tolist()
            b = urandom(randint(0, 100), self.random_endian())
            lst_b = b.tolist()
            s = slice(self.rndsliceidx(n), self.rndsliceidx(n), None)
            a[s] = b
            lst_a[s] = lst_b
            self.assertEqual(a.tolist(), lst_a)
            self.check_obj(a)

    def test_setslice_bool_explicit(self):
        a = bitarray('11111111')
        a[::2] = False
        self.assertEqual(a, bitarray('01010101'))
        a[4::] = True #                   ^^^^
        self.assertEqual(a, bitarray('01011111'))
        a[-2:] = False #                    ^^
        self.assertEqual(a, bitarray('01011100'))
        a[:2:] = True #               ^^
        self.assertEqual(a, bitarray('11011100'))
        a[:] = True #                 ^^^^^^^^
        self.assertEqual(a, bitarray('11111111'))
        a[2:5] = False #                ^^^
        self.assertEqual(a, bitarray('11000111'))
        a[1::3] = False #              ^  ^  ^
        self.assertEqual(a, bitarray('10000110'))
        a[1:6:2] = True #              ^ ^ ^
        self.assertEqual(a, bitarray('11010110'))
        a[3:3] = False # zero slicelength
        self.assertEqual(a, bitarray('11010110'))
        a[:] = False #                ^^^^^^^^
        self.assertEqual(a, bitarray('00000000'))
        a[-2:2:-1] = 1 #                 ^^^^
        self.assertEqual(a, bitarray('00011110'))

    def test_setslice_bool_simple(self):
        for _ in range(100):
            N = randint(100, 2000)
            s = slice(randint(0, 20), randint(N - 20, N), randint(1, 20))
            a = zeros(N)
            a[s] = 1
            b = zeros(N)
            for i in range(s.start, s.stop, s.step):
                b[i] = 1
            self.assertEqual(a, b)

    def test_setslice_bool_range(self):
        N = 200
        a = bitarray(N)
        b = bitarray(N)
        for step in range(-N - 1, N):
            if step == 0:
                continue
            v = randint(0, 1)
            a.setall(not v)
            a[::step] = v

            b.setall(not v)
            for i in range(0, N, abs(step)):
                b[i] = v
            if step < 0:
                b.reverse()
            self.assertEqual(a, b)

    def test_setslice_bool_random(self):
        N = 100
        a = bitarray(N)
        for _ in range(100):
            a.setall(0)
            aa = a.tolist()
            step = self.rndsliceidx(N) or None
            s = slice(self.rndsliceidx(N), self.rndsliceidx(N), step)
            a[s] = 1
            aa[s] = self.slicelen(s, N) * [1]
            self.assertEqual(a.tolist(), aa)

    def test_setslice_bool_random2(self):
        for a in self.randombitarrays():
            n = len(a)
            aa = a.tolist()
            step = self.rndsliceidx(n) or None
            s = slice(self.rndsliceidx(n), self.rndsliceidx(n), step)
            v = randint(0, 1)
            a[s] = v
            aa[s] = self.slicelen(s, n) * [v]
            self.assertEqual(a.tolist(), aa)

    def test_setslice_to_int(self):
        a = bitarray('11111111')
        a[::2] = 0 #  ^ ^ ^ ^
        self.assertEqual(a, bitarray('01010101'))
        a[4::] = 1 #                      ^^^^
        self.assertEqual(a, bitarray('01011111'))
        a.__setitem__(slice(-2, None, None), 0)
        self.assertEqual(a, bitarray('01011100'))
        self.assertRaises(ValueError, a.__setitem__, slice(None, None, 2), 3)
        self.assertRaises(ValueError, a.__setitem__, slice(None, 2, None), -1)
        # a[:2:] = '0'
        self.assertRaises(TypeError, a.__setitem__, slice(None, 2, None), '0')

    def test_setslice_to_invalid(self):
        a = bitarray('11111111')
        s = slice(2, 6, None)
        self.assertRaises(TypeError, a.__setitem__, s, 1.2)
        self.assertRaises(TypeError, a.__setitem__, s, None)
        self.assertRaises(TypeError, a.__setitem__, s, "0110")
        a[s] = False
        self.assertEqual(a, bitarray('11000011'))
        # step != 1 and slicelen != length of assigned bitarray
        self.assertRaisesMessage(
            ValueError,
            "attempt to assign sequence of size 3 to extended slice of size 4",
            a.__setitem__, slice(None, None, 2), bitarray('000'))
        self.assertRaisesMessage(
            ValueError,
            "attempt to assign sequence of size 3 to extended slice of size 2",
            a.__setitem__, slice(None, None, 4), bitarray('000'))
        self.assertRaisesMessage(
            ValueError,
            "attempt to assign sequence of size 7 to extended slice of size 8",
            a.__setitem__, slice(None, None, -1), bitarray('0001000'))
        self.assertEqual(a, bitarray('11000011'))

    def test_sieve(self):  # Sieve of Eratosthenes
        a = bitarray(50)
        a.setall(1)
        a[0:2] = 0
        for i in range(2, 8):
            if a[i]:
                a[i * i::i] = 0
        primes = a.search(1)
        self.assertEqual(primes, [2, 3, 5, 7, 11, 13, 17, 19,
                                  23, 29, 31, 37, 41, 43, 47])

    def test_delitem_simple(self):
        a = bitarray('100110')
        del a[1]
        self.assertEqual(len(a), 5)
        del a[3], a[-2]
        self.assertEqual(a, bitarray('100'))
        self.assertRaises(IndexError, a.__delitem__,  3)
        self.assertRaises(IndexError, a.__delitem__, -4)

    def test_delitem_random(self):
        for a in self.randombitarrays(start=1):
            n = len(a)
            b = a.copy()
            i = randint(0, n - 1)
            del b[i]
            self.assertEQUAL(b, a[:i] + a[i + 1:])
            self.assertEqual(len(b), n - 1)
            self.check_obj(b)

    def test_delslice_explicit(self):
        a = bitarray('10101100 10110')
        del a[3:9] #     ^^^^^ ^
        self.assertEqual(a, bitarray('1010110'))
        del a[::3] #                  ^  ^  ^
        self.assertEqual(a, bitarray('0111'))
        a = bitarray('10101100 101101111')
        del a[5:-3:3] #    ^   ^  ^
        self.assertEqual(a, bitarray('1010100 0101111'))
        a = bitarray('10101100 1011011')
        del a[:-9:-2] #        ^ ^ ^ ^
        self.assertEqual(a, bitarray('10101100 011'))
        del a[3:3] # zero slicelength
        self.assertEqual(a, bitarray('10101100 011'))
        self.assertRaises(ValueError, a.__delitem__, slice(None, None, 0))
        self.assertEqual(len(a), 11)
        del a[:]
        self.assertEqual(a, bitarray())

    def test_delslice_special(self):
        for n in 0, 1, 10, 73:
            a = urandom(n)
            b = a.copy()
            del b[:0]
            del b[n:]
            self.assertEqual(b, a)
            del b[10:]  # delete at end
            self.assertEqual(b, a[:10])
            del b[:]  # clear
            self.assertEqual(len(b), 0)
            self.check_obj(b)

    def test_delslice_random(self):
        for a in self.randombitarrays():
            la = len(a)
            for _ in range(10):
                step = self.rndsliceidx(la) or None
                s = slice(self.rndsliceidx(la), self.rndsliceidx(la), step)
                c = a.copy()
                del c[s]
                self.check_obj(c)
                c_lst = a.tolist()
                del c_lst[s]
                self.assertEQUAL(c, bitarray(c_lst, endian=c.endian()))

    def test_delslice_range(self):
        # tests C function delete_n()
        for endian in 'little', 'big':
            for n in range(500):
                a = urandom(n, endian)
                p = randint(0, n)
                m = randint(0, 500)

                b = a.copy()
                del b[p:p + m]
                self.assertEQUAL(b, a[:p] + a[p + m:])
                self.check_obj(b)

tests.append(SliceTests)

# ---------------------------------------------------------------------------

class MiscTests(unittest.TestCase, Util):

    def test_instancecheck(self):
        a = bitarray('011')
        self.assertIsInstance(a, bitarray)
        self.assertFalse(isinstance(a, str))

    def test_booleanness(self):
        self.assertEqual(bool(bitarray('')), False)
        self.assertEqual(bool(bitarray('0')), True)
        self.assertEqual(bool(bitarray('1')), True)

    def test_to01(self):
        a = bitarray()
        self.assertEqual(a.to01(), '')
        self.assertIsInstance(a.to01(), str)

        a = bitarray('101')
        self.assertEqual(a.to01(), '101')
        self.assertIsInstance(a.to01(), str)

    def test_iterate(self):
        for lst in self.randomlists():
            acc = []
            for b in bitarray(lst):
                acc.append(b)
            self.assertEqual(acc, lst)

    def test_iter1(self):
        it = iter(bitarray('011'))
        self.assertIsType(it, 'bitarrayiterator')
        self.assertBitEqual(next(it), 0)
        self.assertBitEqual(next(it), 1)
        self.assertBitEqual(next(it), 1)
        self.assertStopIteration(it)

    def test_iter2(self):
        for a in self.randombitarrays():
            aa = a.tolist()
            self.assertEqual(list(a), aa)
            self.assertEqual(list(iter(a)), aa)

    def test_assignment(self):
        a = bitarray('00110111001')
        a[1:3] = a[7:9]
        a[-1:] = a[:1]
        b = bitarray('01010111000')
        self.assertEqual(a, b)

    def test_subclassing(self):
        class ExaggeratingBitarray(bitarray):

            def __new__(cls, data, offset):
                return bitarray.__new__(cls, data)

            def __init__(self, data, offset):
                self.offset = offset

            def __getitem__(self, i):
                return bitarray.__getitem__(self, i - self.offset)

        for a in self.randombitarrays():
            b = ExaggeratingBitarray(a, 1234)
            for i in range(len(a)):
                self.assertEqual(a[i], b[i + 1234])

    def test_endianness1(self):
        a = bitarray(endian='little')
        a.frombytes(b'\x01')
        self.assertEqual(a.to01(), '10000000')

        b = bitarray(endian='little')
        b.frombytes(b'\x80')
        self.assertEqual(b.to01(), '00000001')

        c = bitarray(endian='big')
        c.frombytes(b'\x80')
        self.assertEqual(c.to01(), '10000000')

        d = bitarray(endian='big')
        d.frombytes(b'\x01')
        self.assertEqual(d.to01(), '00000001')

        self.assertEqual(a, c)
        self.assertEqual(b, d)

    def test_endianness2(self):
        a = bitarray(8, endian='little')
        a.setall(False)
        a[0] = True
        self.assertEqual(a.tobytes(), b'\x01')
        a[1] = True
        self.assertEqual(a.tobytes(), b'\x03')
        a.frombytes(b' ')
        self.assertEqual(a.tobytes(), b'\x03 ')
        self.assertEqual(a.to01(), '1100000000000100')

    def test_endianness3(self):
        a = bitarray(8, endian='big')
        a.setall(False)
        a[7] = True
        self.assertEqual(a.tobytes(), b'\x01')
        a[6] = True
        self.assertEqual(a.tobytes(), b'\x03')
        a.frombytes(b' ')
        self.assertEqual(a.tobytes(), b'\x03 ')
        self.assertEqual(a.to01(), '0000001100100000')

    def test_endianness4(self):
        a = bitarray('00100000', endian='big')
        self.assertEqual(a.tobytes(), b' ')
        b = bitarray('00000100', endian='little')
        self.assertEqual(b.tobytes(), b' ')
        self.assertNotEqual(a, b)

    def test_pickle(self):
        for a in self.randombitarrays():
            b = pickle.loads(pickle.dumps(a))
            self.assertFalse(b is a)
            self.assertEQUAL(a, b)

    def test_overflow(self):
        self.assertRaises(OverflowError, bitarray, 2 ** 63)
        a = bitarray(1)
        for i in -7, -1, 0, 1:
            self.assertRaises(OverflowError, a.__imul__, 2 ** 63 + i)
        a = bitarray(2 ** 10)
        self.assertRaises(OverflowError, a.__imul__, 2 ** 53)

        if SYSINFO[0] == 8:
            return

        a = bitarray(10 ** 6)
        self.assertRaises(OverflowError, a.__imul__, 17180)
        for i in -7, -1, 0, 1:
            self.assertRaises(OverflowError, bitarray, 2 ** 31 + i)
        try:
            a = bitarray(2 ** 31 - 8);
        except MemoryError:
            return
        self.assertRaises(OverflowError, bitarray.append, a, True)

    def test_unicode_create(self):
        a = bitarray(u'')
        self.assertEqual(a, bitarray())

        a = bitarray(u'111001')
        self.assertEqual(a, bitarray('111001'))

    def test_unhashable(self):
        a = bitarray()
        self.assertRaises(TypeError, hash, a)
        self.assertRaises(TypeError, dict, [(a, 'foo')])

tests.append(MiscTests)

# ---------------------------------------------------------------------------

class RichCompareTests(unittest.TestCase, Util):

    def test_wrong_types(self):
        a = bitarray()
        for x in None, 7, 'A':
            self.assertEqual(a.__eq__(x), NotImplemented)
            self.assertEqual(a.__ne__(x), NotImplemented)
            self.assertEqual(a.__ge__(x), NotImplemented)
            self.assertEqual(a.__gt__(x), NotImplemented)
            self.assertEqual(a.__le__(x), NotImplemented)
            self.assertEqual(a.__lt__(x), NotImplemented)

    def test_explicit(self):
        for sa, sb, res in [
                ('',   '',   '101010'),
                ('1',  '',   '011100'),
                ('11', '10', '011100'),
                ('0',  '1',  '010011'),
        ]:
            a = bitarray(sa, self.random_endian())
            b = bitarray(sb, self.random_endian())
            self.assertEqual(a == b, int(res[0]))
            self.assertEqual(a != b, int(res[1]))
            self.assertEqual(a >= b, int(res[2]))
            self.assertEqual(a >  b, int(res[3]))
            self.assertEqual(a <= b, int(res[4]))
            self.assertEqual(a <  b, int(res[5]))

    def test_eq_ne(self):
        for _ in range(10):
            self.assertTrue(bitarray(0, self.random_endian()) ==
                            bitarray(0, self.random_endian()))
            self.assertFalse(bitarray(0, self.random_endian()) !=
                             bitarray(0, self.random_endian()))

        for n in range(1, 20):
            a = bitarray(n, self.random_endian())
            a.setall(1)
            b = bitarray(a, self.random_endian())
            self.assertTrue(a == b)
            self.assertFalse(a != b)
            b[n - 1] = 0
            self.assertTrue(a != b)
            self.assertFalse(a == b)

    def test_eq_ne_random(self):
        for a in self.randombitarrays(start=1):
            b = bitarray(a, self.random_endian())
            self.assertTrue(a == b)
            self.assertFalse(a != b)
            b.invert(randint(0, len(a) - 1))
            self.assertTrue(a != b)
            self.assertFalse(a == b)

    def check(self, a, b, c, d):
        self.assertEqual(a == b, c == d)
        self.assertEqual(a != b, c != d)
        self.assertEqual(a <= b, c <= d)
        self.assertEqual(a <  b, c <  d)
        self.assertEqual(a >= b, c >= d)
        self.assertEqual(a >  b, c >  d)

    def test_invert_random_element(self):
        for a in self.randombitarrays(start=1):
            n = len(a)
            b = bitarray(a, self.random_endian())
            i = randint(0, n - 1)
            b.invert(i)
            self.check(a, b, a[i], b[i])

    def test_size(self):
        for _ in range(100):
            a = zeros(randint(1, 20), self.random_endian())
            b = zeros(randint(1, 20), self.random_endian())
            self.check(a, b, len(a), len(b))

    def test_random(self):
        for a in self.randombitarrays():
            aa = a.tolist()
            if randint(0, 1):
                a = frozenbitarray(a)
            for b in self.randombitarrays():
                bb = b.tolist()
                if randint(0, 1):
                    b = frozenbitarray(b)
                self.check(a, b, aa, bb)
                self.check(a, b, aa, bb)

tests.append(RichCompareTests)

# ---------------------------------------------------------------------------

class SpecialMethodTests(unittest.TestCase, Util):

    def test_all(self):
        a = bitarray()
        self.assertTrue(a.all())
        for s, r in ('0', False), ('1', True), ('01', False):
            self.assertTrue(bitarray(s).all() is r)

        for a in self.randombitarrays():
            self.assertTrue(a.all() is all(a))

        N = randint(1000, 2000)
        a = bitarray(N)
        a.setall(1)
        self.assertTrue(a.all())
        a[N - 1] = 0
        self.assertFalse(a.all())

    def test_any(self):
        a = bitarray()
        self.assertFalse(a.any())
        for s, r in ('0', False), ('1', True), ('01', True):
            self.assertTrue(bitarray(s).any() is r)

        for a in self.randombitarrays():
            self.assertTrue(a.any() is any(a))

        N = randint(1000, 2000)
        a = bitarray(N)
        a.setall(0)
        self.assertFalse(a.any())
        a[N - 1] = 1
        self.assertTrue(a.any())

    def test_repr(self):
        r = repr(bitarray())
        self.assertEqual(r, "bitarray()")
        self.assertIsInstance(r, str)

        r = repr(bitarray('10111'))
        self.assertEqual(r, "bitarray('10111')")
        self.assertIsInstance(r, str)

        for a in self.randombitarrays():
            b = eval(repr(a))
            self.assertFalse(b is a)
            self.assertEqual(a, b)
            self.check_obj(b)

    def test_copy(self):
        for a in self.randombitarrays():
            b = a.copy()
            self.assertFalse(b is a)
            self.assertEQUAL(a, b)

            b = copy.copy(a)
            self.assertFalse(b is a)
            self.assertEQUAL(a, b)

            b = copy.deepcopy(a)
            self.assertFalse(b is a)
            self.assertEQUAL(a, b)

    def assertReallyEqual(self, a, b):
        # assertEqual first, because it will have a good message if the
        # assertion fails.
        self.assertEqual(a, b)
        self.assertEqual(b, a)
        self.assertTrue(a == b)
        self.assertTrue(b == a)
        self.assertFalse(a != b)
        self.assertFalse(b != a)
        if not is_py3k:
            self.assertEqual(0, cmp(a, b))
            self.assertEqual(0, cmp(b, a))

    def assertReallyNotEqual(self, a, b):
        # assertNotEqual first, because it will have a good message if the
        # assertion fails.
        self.assertNotEqual(a, b)
        self.assertNotEqual(b, a)
        self.assertFalse(a == b)
        self.assertFalse(b == a)
        self.assertTrue(a != b)
        self.assertTrue(b != a)
        if not is_py3k:
            self.assertNotEqual(0, cmp(a, b))
            self.assertNotEqual(0, cmp(b, a))

    def test_equality(self):
        self.assertReallyEqual(bitarray(''), bitarray(''))
        self.assertReallyEqual(bitarray('0'), bitarray('0'))
        self.assertReallyEqual(bitarray('1'), bitarray('1'))

    def test_not_equality(self):
        self.assertReallyNotEqual(bitarray(''), bitarray('1'))
        self.assertReallyNotEqual(bitarray(''), bitarray('0'))
        self.assertReallyNotEqual(bitarray('0'), bitarray('1'))

    def test_equality_random(self):
        for a in self.randombitarrays(start=1):
            b = a.copy()
            self.assertReallyEqual(a, b)
            n = len(a)
            b.invert(n - 1)  # flip last bit
            self.assertReallyNotEqual(a, b)

    def test_sizeof(self):
        a = bitarray()
        size = sys.getsizeof(a)
        self.assertEqual(size, a.__sizeof__())
        self.assertIsInstance(size, int if is_py3k else (int, long))
        self.assertTrue(size < 200)
        a = bitarray(8000)
        self.assertTrue(sys.getsizeof(a) > 1000)

tests.append(SpecialMethodTests)

# ---------------------------------------------------------------------------

class SequenceMethodsTests(unittest.TestCase, Util):

    def test_concat(self):
        a = bitarray('001')
        b = a + bitarray('110')
        self.assertEQUAL(b, bitarray('001110'))
        b = a + [0, 1, True]
        self.assertEQUAL(b, bitarray('001011'))
        b = a + '100'
        self.assertEQUAL(b, bitarray('001100'))
        b = a + (1, 0, True)
        self.assertEQUAL(b, bitarray('001101'))
        self.assertRaises(ValueError, a.__add__, (0, 1, 2))
        self.assertEQUAL(a, bitarray('001'))

        self.assertRaises(TypeError, a.__add__, 42)
        if is_py3k:
            self.assertRaises(TypeError, a.__add__, b'1101')
        else:
            self.assertEqual(a + b'10', bitarray('00110'))

        for a in self.randombitarrays():
            aa = a.copy()
            for b in self.randombitarrays():
                bb = b.copy()
                c = a + b
                self.assertEqual(c, bitarray(a.tolist() + b.tolist()))
                self.assertEqual(c.endian(), a.endian())
                self.check_obj(c)

                self.assertEQUAL(a, aa)
                self.assertEQUAL(b, bb)

    def test_inplace_concat(self):
        a = bitarray('001')
        a += bitarray('110')
        self.assertEqual(a, bitarray('001110'))
        a += [0, 1, True]
        self.assertEqual(a, bitarray('001110011'))
        a += '100'
        self.assertEqual(a, bitarray('001110011100'))
        a += (1, 0, True)
        self.assertEqual(a, bitarray('001110011100101'))

        a = bitarray('110')
        self.assertRaises(ValueError, a.__iadd__, [0, 1, 2])
        self.assertEqual(a, bitarray('110'))

        self.assertRaises(TypeError, a.__iadd__, 42)
        b = b'101'
        if is_py3k:
            self.assertRaises(TypeError, a.__iadd__, b)
        else:
            a += b
            self.assertEqual(a, bitarray('110101'))

        for a in self.randombitarrays():
            for b in self.randombitarrays():
                c = bitarray(a)
                d = c
                d += b
                self.assertEqual(d, a + b)
                self.assertTrue(c is d)
                self.assertEQUAL(c, d)
                self.assertEqual(d.endian(), a.endian())
                self.check_obj(d)

    def test_repeat_explicit(self):
        for m, s, r in [
                ( 0,        '',      ''),
                ( 0, '1001111',      ''),
                (-1,  '100110',      ''),
                (11,        '',      ''),
                ( 1,     '110',   '110'),
                ( 2,      '01',  '0101'),
                ( 5,       '1', '11111'),
        ]:
            a = bitarray(s)
            self.assertEqual(a * m, bitarray(r))
            self.assertEqual(m * a, bitarray(r))
            c = a.copy()
            c *= m
            self.assertEqual(c, bitarray(r))

    def test_repeat_wrong_args(self):
        a = bitarray()
        self.assertRaises(TypeError, a.__mul__, None)
        self.assertRaises(TypeError, a.__mul__, 2.0)
        self.assertRaises(TypeError, a.__imul__, None)
        self.assertRaises(TypeError, a.__imul__, 3.0)

    def test_repeat_random(self):
        for a in self.randombitarrays():
            b = a.copy()
            for m in list(range(-3, 5)) + [randint(100, 200)]:
                res = bitarray(m * a.to01(), endian=a.endian())
                self.assertEqual(len(res), len(a) * max(0, m))

                c = a * m
                self.assertEQUAL(c, res)
                c = m * a
                self.assertEQUAL(c, res)

                c = a.copy()
                c *= m
                self.assertEQUAL(c, res)
                self.check_obj(c)

            self.assertEQUAL(a, b)

    def test_contains_simple(self):
        a = bitarray()
        self.assertFalse(False in a)
        self.assertFalse(True in a)
        self.assertTrue(bitarray() in a)
        a.append(True)
        self.assertTrue(True in a)
        self.assertFalse(False in a)
        a = bitarray([False])
        self.assertTrue(False in a)
        self.assertFalse(True in a)
        a.append(True)
        self.assertTrue(0 in a)
        self.assertTrue(1 in a)
        if not is_py3k:
            self.assertTrue(long(0) in a)
            self.assertTrue(long(1) in a)

    def test_contains_errors(self):
        a = bitarray()
        self.assertEqual(a.__contains__(1), False)
        a.append(1)
        self.assertEqual(a.__contains__(1), True)
        a = bitarray('0011')
        self.assertEqual(a.__contains__(bitarray('01')), True)
        self.assertEqual(a.__contains__(bitarray('10')), False)
        self.assertRaises(TypeError, a.__contains__, 'asdf')
        self.assertRaises(ValueError, a.__contains__, 2)
        self.assertRaises(ValueError, a.__contains__, -1)
        if not is_py3k:
            self.assertRaises(ValueError, a.__contains__, long(2))

    def test_contains_range(self):
        for n in range(2, 50):
            a = bitarray(n)
            a.setall(0)
            self.assertTrue(False in a)
            self.assertFalse(True in a)
            a[randint(0, n - 1)] = 1
            self.assertTrue(True in a)
            self.assertTrue(False in a)
            a.setall(1)
            self.assertTrue(True in a)
            self.assertFalse(False in a)
            a[randint(0, n - 1)] = 0
            self.assertTrue(True in a)
            self.assertTrue(False in a)

    def test_contains_explicit(self):
        a = bitarray('011010000001')
        for s, r in [('', True), ('1', True), ('11', True), ('111', False),
                     ('011', True), ('0001', True), ('00011', False)]:
            self.assertEqual(bitarray(s) in a, r)

tests.append(SequenceMethodsTests)

# ---------------------------------------------------------------------------

class NumberTests(unittest.TestCase, Util):

    def test_misc(self):
        for a in self.randombitarrays():
            b = ~a
            c = a & b
            self.assertEqual(c.any(), False)
            self.assertEqual(a, a ^ c)
            d = a ^ b
            self.assertEqual(d.all(), True)
            b &= d
            self.assertEqual(~b, a)

    def test_bool(self):
        a = bitarray()
        self.assertTrue(bool(a) is False)
        a.append(0)
        self.assertTrue(bool(a) is True)
        a.append(1)
        self.assertTrue(bool(a) is True)

    def test_size_error(self):
        a = bitarray('11001')
        b = bitarray('100111')
        self.assertRaises(ValueError, lambda: a & b)
        self.assertRaises(ValueError, lambda: a | b)
        self.assertRaises(ValueError, lambda: a ^ b)
        for x in (a.__and__, a.__or__, a.__xor__,
                  a.__iand__, a.__ior__, a.__ixor__):
            self.assertRaises(ValueError, x, b)

    def test_endianness_error(self):
        a = bitarray('11001', 'big')
        b = bitarray('10011', 'little')
        self.assertRaises(ValueError, lambda: a & b)
        self.assertRaises(ValueError, lambda: a | b)
        self.assertRaises(ValueError, lambda: a ^ b)
        for x in (a.__and__, a.__or__, a.__xor__,
                  a.__iand__, a.__ior__, a.__ixor__):
            self.assertRaises(ValueError, x, b)

    def test_and(self):
        a = bitarray('11001')
        b = bitarray('10011')
        c = a & b
        self.assertEqual(c, bitarray('10001'))
        self.check_obj(c)

        self.assertRaises(TypeError, lambda: a & 1)
        self.assertRaises(TypeError, lambda: 1 & a)
        self.assertEqual(a, bitarray('11001'))
        self.assertEqual(b, bitarray('10011'))

    def test_or(self):
        a = bitarray('11001')
        b = bitarray('10011')
        c = a | b
        self.assertEqual(c, bitarray('11011'))
        self.check_obj(c)

        self.assertRaises(TypeError, lambda: a | 1)
        self.assertRaises(TypeError, lambda: 1 | a)
        self.assertEqual(a, bitarray('11001'))
        self.assertEqual(b, bitarray('10011'))

    def test_xor(self):
        a = bitarray('11001')
        b = bitarray('10011')
        c = a ^ b
        self.assertEQUAL(c, bitarray('01010'))
        self.check_obj(c)

        self.assertRaises(TypeError, lambda: a ^ 1)
        self.assertRaises(TypeError, lambda: 1 ^ a)
        self.assertEqual(a, bitarray('11001'))
        self.assertEqual(b, bitarray('10011'))

    def test_iand(self):
        a = bitarray('110010110')
        b = bitarray('100110011')
        a &= b
        self.assertEqual(a, bitarray('100010010'))
        self.assertEqual(b, bitarray('100110011'))
        self.check_obj(a)
        self.check_obj(b)
        try:
            a &= 1
        except TypeError:
            error = 1
        self.assertEqual(error, 1)

    def test_ior(self):
        a = bitarray('110010110')
        b = bitarray('100110011')
        a |= b
        self.assertEQUAL(a, bitarray('110110111'))
        self.assertEQUAL(b, bitarray('100110011'))
        try:
            a |= 1
        except TypeError:
            error = 1
        self.assertEqual(error, 1)

    def test_ixor(self):
        a = bitarray('110010110')
        b = bitarray('100110011')
        a ^= b
        self.assertEQUAL(a, bitarray('010100101'))
        self.assertEQUAL(b, bitarray('100110011'))
        try:
            a ^= 1
        except TypeError:
            error = 1
        self.assertEqual(error, 1)

    def test_bitwise_self(self):
        for a in self.randombitarrays():
            aa = a.copy()
            self.assertEQUAL(a & a, aa)
            self.assertEQUAL(a | a, aa)
            self.assertEQUAL(a ^ a, zeros(len(aa), aa.endian()))
            self.assertEQUAL(a, aa)

    def test_bitwise_inplace_self(self):
        for a in self.randombitarrays():
            aa = a.copy()
            a &= a
            self.assertEQUAL(a, aa)
            a |= a
            self.assertEQUAL(a, aa)
            a ^= a
            self.assertEqual(a, zeros(len(aa), aa.endian()))

    def test_invert(self):
        a = bitarray('11011')
        b = ~a
        self.assertEQUAL(b, bitarray('00100'))
        self.assertEQUAL(a, bitarray('11011'))
        self.assertFalse(a is b)
        self.check_obj(b)

        for a in self.randombitarrays():
            b = bitarray(a)
            b.invert()
            for i in range(len(a)):
                self.assertEqual(b[i], not a[i])
            self.check_obj(b)
            self.assertEQUAL(~a, b)

    @staticmethod
    def shift(a, n, direction):
        if n >= len(a):
            return zeros(len(a), a.endian())

        if direction == 'right':
            return zeros(n, a.endian()) + a[:len(a)-n]
        elif direction == 'left':
            return a[n:] + zeros(n, a.endian())
        else:
            raise ValueError("invalid direction: %s" % direction)

    def test_lshift(self):
        a = bitarray('11011')
        b = a << 2
        self.assertEQUAL(b, bitarray('01100'))
        self.assertRaises(TypeError, lambda: a << 1.2)
        self.assertRaises(TypeError, a.__lshift__, 1.2)
        self.assertRaises(ValueError, lambda: a << -1)
        self.assertRaises(OverflowError, a.__lshift__, 2 ** 63)

        for a in self.randombitarrays():
            c = a.copy()
            n = randint(0, len(a) + 3)
            b = a << n
            self.assertEqual(len(b), len(a))
            self.assertEQUAL(b, self.shift(a, n, 'left'))
            self.assertEQUAL(a, c)

    def test_rshift(self):
        a = bitarray('1101101')
        b = a >> 1
        self.assertEQUAL(b, bitarray('0110110'))
        self.assertRaises(TypeError, lambda: a >> 1.2)
        self.assertRaises(TypeError, a.__rshift__, 1.2)
        self.assertRaises(ValueError, lambda: a >> -1)

        for a in self.randombitarrays():
            c = a.copy()
            n = randint(0, len(a) + 3)
            b = a >> n
            self.assertEqual(len(b), len(a))
            self.assertEQUAL(b, self.shift(a, n, 'right'))
            self.assertEQUAL(a, c)

    def test_ilshift(self):
        a = bitarray('110110101')
        a <<= 7
        self.assertEQUAL(a, bitarray('010000000'))
        self.assertRaises(TypeError, a.__ilshift__, 1.2)
        self.assertRaises(ValueError, a.__ilshift__, -3)

        for a in self.randombitarrays():
            b = a.copy()
            n = randint(0, len(a) + 3)
            b <<= n
            self.assertEqual(len(b), len(a))
            self.assertEQUAL(b, self.shift(a, n, 'left'))

    def test_irshift(self):
        a = bitarray('110110111')
        a >>= 3
        self.assertEQUAL(a, bitarray('000110110'))
        self.assertRaises(TypeError, a.__irshift__, 1.2)
        self.assertRaises(ValueError, a.__irshift__, -4)

        for a in self.randombitarrays():
            b = a.copy()
            n = randint(0, len(a) + 3)
            b >>= n
            self.assertEqual(len(b), len(a))
            self.assertEQUAL(b, self.shift(a, n, 'right'))

    def check_random(self, n, endian, n_shift, direction):
        a = urandom(n, endian)
        self.assertEqual(len(a), n)

        b = a.copy()
        if direction == 'left':
            b <<= n_shift
        else:
            b >>= n_shift
        self.assertEQUAL(b, self.shift(a, n_shift, direction))

    def test_shift_range(self):
        for endian in 'little', 'big':
            for direction in 'left', 'right':
                for n in range(0, 200):
                    self.check_random(n, endian, 1, direction)
                    self.check_random(n, endian, randint(0, n), direction)
                for n_shift in range(0, 100):
                    self.check_random(100, endian, n_shift, direction)

    def test_zero_shift(self):
        for a in self.randombitarrays():
            aa = a.copy()
            self.assertEQUAL(a << 0, aa)
            self.assertEQUAL(a >> 0, aa)
            a <<= 0
            self.assertEQUAL(a, aa)
            a >>= 0
            self.assertEQUAL(a, aa)

    def test_len_or_larger_shift(self):
        # ensure shifts with len(a) (or larger) result in all zero bitarrays
        for a in self.randombitarrays():
            c = a.copy()
            z = zeros(len(a), a.endian())
            n = randint(len(a), len(a) + 10)
            self.assertEQUAL(a << n, z)
            self.assertEQUAL(a >> n, z)
            self.assertEQUAL(a, c)
            a <<= n
            self.assertEQUAL(a, z)
            a = bitarray(c)
            a >>= n
            self.assertEQUAL(a, z)

    def test_shift_example(self):
        a = bitarray('0010011')
        self.assertEqual(a << 3, bitarray('0011000'))
        a >>= 4
        self.assertEqual(a, bitarray('0000001'))

tests.append(NumberTests)

# ---------------------------------------------------------------------------

class ExtendTests(unittest.TestCase, Util):

    def test_wrongArgs(self):
        a = bitarray()
        self.assertRaises(TypeError, a.extend)
        self.assertRaises(TypeError, a.extend, None)
        self.assertRaises(TypeError, a.extend, True)
        self.assertRaises(TypeError, a.extend, 24)
        self.assertRaises(TypeError, a.extend, 1.0)

    def test_bitarray(self):
        a = bitarray()
        a.extend(bitarray())
        self.assertEqual(a, bitarray())
        a.extend(bitarray('110'))
        self.assertEqual(a, bitarray('110'))
        a.extend(bitarray('1110'))
        self.assertEqual(a, bitarray('1101110'))

        a = bitarray('00001111', endian='little')
        a.extend(bitarray('00100111', endian='big'))
        self.assertEqual(a, bitarray('00001111 00100111'))

    def test_bitarray_random(self):
        for a in self.randombitarrays():
            sa = a.to01()
            for b in self.randombitarrays():
                bb = b.copy()
                c = bitarray(a)
                c.extend(b)
                self.assertEqual(c.to01(), sa + bb.to01())
                self.assertEqual(c.endian(), a.endian())
                self.assertEqual(len(c), len(a) + len(b))
                self.check_obj(c)
                # ensure b hasn't changed
                self.assertEQUAL(b, bb)
                self.check_obj(b)

    def test_list(self):
        a = bitarray()
        a.extend([])
        self.assertEqual(a, bitarray())
        a.extend([0, 1, True, False])
        self.assertEqual(a, bitarray('0110'))
        self.assertRaises(ValueError, a.extend, [0, 1, 2])
        self.assertRaises(TypeError, a.extend, [0, 1, 'a'])
        self.assertEqual(a, bitarray('0110'))

        for a in self.randomlists():
            for b in self.randomlists():
                c = bitarray(a)
                c.extend(b)
                self.assertEqual(c.tolist(), a + b)
                self.check_obj(c)

    def test_tuple(self):
        a = bitarray()
        a.extend(tuple())
        self.assertEqual(a, bitarray())
        a.extend((0, 1, True, 0, False))
        self.assertEqual(a, bitarray('01100'))
        self.assertRaises(ValueError, a.extend, (0, 1, 2))
        self.assertRaises(TypeError, a.extend, (0, 1, 'a'))
        self.assertEqual(a, bitarray('01100'))

        for a in self.randomlists():
            for b in self.randomlists():
                c = bitarray(a)
                c.extend(tuple(b))
                self.assertEqual(c.tolist(), a + b)
                self.check_obj(c)

    def test_generator_1(self):
        def gen(lst):
            for x in lst:
                yield x
        a = bitarray('0011')
        a.extend(gen([0, 1, False, True, 0]))
        self.assertEqual(a, bitarray('0011 01010'))
        self.assertRaises(ValueError, a.extend, gen([0, 1, 2]))
        self.assertRaises(TypeError, a.extend, gen([1, 0, None]))
        self.assertEqual(a, bitarray('0011 01010'))

        a = bytearray()
        a.extend(gen([0, 1, 255]))
        self.assertEqual(a, b'\x00\x01\xff')
        self.assertRaises(ValueError, a.extend, gen([0, 1, 256]))
        self.assertRaises(TypeError, a.extend, gen([1, 0, None]))
        self.assertEqual(a, b'\x00\x01\xff')

        for a in self.randomlists():
            def foo():
                for e in a:
                    yield e
            b = bitarray()
            b.extend(foo())
            self.assertEqual(b.tolist(), a)
            self.check_obj(b)

    def test_generator_2(self):
        def gen():
            for i in range(10):
                if i == 4:
                    raise KeyError
                yield i % 2

        a = bitarray()
        self.assertRaises(KeyError, a.extend, gen())
        self.assertEqual(a, bitarray('0101'))
        a = []
        self.assertRaises(KeyError, a.extend, gen())
        self.assertEqual(a, [0, 1, 0, 1])

    def test_iterator_1(self):
        a = bitarray()
        a.extend(iter([]))
        self.assertEqual(a, bitarray())
        a.extend(iter([1, 1, 0, True, False]))
        self.assertEqual(a, bitarray('11010'))
        self.assertRaises(ValueError, a.extend, iter([1, 1, 0, 0, 2]))
        self.assertEqual(a, bitarray('11010'))

        for a in self.randomlists():
            for b in self.randomlists():
                c = bitarray(a)
                c.extend(iter(b))
                self.assertEqual(c.tolist(), a + b)
                self.check_obj(c)

    def test_iterator_2(self):
        a = bitarray()
        a.extend(itertools.repeat(True, 23))
        self.assertEqual(a, bitarray(23 * '1'))
        self.check_obj(a)

    def test_string01(self):
        a = bitarray()
        a.extend(str())
        a.extend('')
        self.assertEqual(a, bitarray())
        a.extend('0110111')
        self.assertEqual(a, bitarray('0110111'))
        self.assertRaises(ValueError, a.extend, '0011201')
        # ensure no bits got added after error was raised
        self.assertEqual(a, bitarray('0110111'))

        a = bitarray()
        self.assertRaises(ValueError, a.extend, 1000 * '01' + '.')
        self.assertEqual(a, bitarray())

        for a in self.randomlists():
            for b in self.randomlists():
                c = bitarray(a)
                c.extend(''.join(['0', '1'][x] for x in b))
                self.assertEqual(c, bitarray(a + b))
                self.check_obj(c)

    def test_string01_whitespace(self):
        a = bitarray()
        a.extend('0 1\n0\r1\t0\v1_')
        self.assertEqual(a, bitarray('010101'))
        a += '_ 1\n0\r1\t0\v'
        self.assertEqual(a, bitarray('010101 1010'))
        self.check_obj(a)

    def test_unicode(self):
        a = bitarray()
        a.extend(u'')
        self.assertEqual(a, bitarray())
        self.assertRaises(ValueError, a.extend, u'0011201')
        # ensure no bits got added after error was raised
        self.assertEqual(a, bitarray())
        self.check_obj(a)

        a = bitarray()
        a.extend(u'001 011_')
        self.assertEqual(a, bitarray('001011'))
        self.assertRaises(UnicodeEncodeError, a.extend, u'1\u2605 0')
        self.assertEqual(a, bitarray('001011'))
        self.check_obj(a)

    def test_bytes(self):
        a = bitarray()
        b = b'10110'
        if is_py3k:
            self.assertRaises(TypeError, a.extend, b)
        else:
            a.extend(b)
            self.assertEqual(a, bitarray('10110'))
        self.check_obj(a)

    def test_self(self):
        for s in '', '1', '110', '00110111':
            a = bitarray(s)
            a.extend(a)
            self.assertEqual(a, bitarray(2 * s))

        for a in self.randombitarrays():
            endian = a.endian()
            s = a.to01()
            a.extend(a)
            self.assertEqual(a.to01(), 2 * s)
            self.assertEqual(a.endian(), endian)
            self.assertEqual(len(a), 2 * len(s))
            self.check_obj(a)

tests.append(ExtendTests)

# ---------------------------------------------------------------------------

class MethodTests(unittest.TestCase, Util):

    def test_append_simple(self):
        a = bitarray()
        a.append(True)
        a.append(False)
        a.append(False)
        self.assertEQUAL(a, bitarray('100'))
        a.append(0)
        a.append(1)
        self.assertEQUAL(a, bitarray('10001'))
        self.assertRaises(ValueError, a.append, 2)
        self.assertRaises(TypeError, a.append, None)
        self.assertRaises(TypeError, a.append, '')
        self.assertEQUAL(a, bitarray('10001'))
        self.check_obj(a)

    def test_append_random(self):
        for a in self.randombitarrays():
            aa = a.tolist()
            a.append(1)
            self.assertEQUAL(a, bitarray(aa + [1], endian=a.endian()))
            a.append(0)
            self.assertEQUAL(a, bitarray(aa + [1, 0], endian=a.endian()))
            self.check_obj(a)

    def test_insert(self):
        a = bitarray('111100')
        a.insert(3, False)
        self.assertEqual(a, bitarray('1110100'))
        self.assertRaises(ValueError, a.insert, 0, 2)
        self.assertRaises(TypeError, a.insert, 0, None)
        self.assertRaises(TypeError, a.insert)
        self.assertRaises(TypeError, a.insert, None)
        self.assertEqual(a, bitarray('1110100'))
        self.check_obj(a)

    def test_insert_random(self):
        for a in self.randombitarrays():
            aa = a.tolist()
            for _ in range(20):
                item = randint(0, 1)
                pos = randint(-len(a) - 2, len(a) + 2)
                a.insert(pos, item)
                aa.insert(pos, item)
            self.assertEqual(a.tolist(), aa)
            self.check_obj(a)

    def test_fill_simple(self):
        for endian in 'little', 'big':
            a = bitarray(endian=endian)
            self.assertEqual(a.fill(), 0)
            self.assertEqual(len(a), 0)

            a = bitarray('101', endian)
            self.assertEqual(a.fill(), 5)
            self.assertEqual(a, bitarray('10100000'))
            self.assertEqual(a.fill(), 0)
            self.assertEqual(a, bitarray('10100000'))
            self.check_obj(a)

    def test_fill_random(self):
        for a in self.randombitarrays():
            b = a.copy()
            res = b.fill()
            self.assertTrue(0 <= res < 8)
            self.assertEqual(b.endian(), a.endian())
            self.check_obj(b)
            if len(a) % 8 == 0:
                self.assertEqual(b, a)
            else:
                self.assertEqual(len(b) % 8, 0)
                self.assertNotEqual(b, a)
                self.assertEqual(b[:len(a)], a)
                self.assertEqual(b[len(a):], zeros(len(b) - len(a)))

    def test_invert_simple(self):
        a = bitarray()
        a.invert()
        self.assertEQUAL(a, bitarray())
        self.check_obj(a)

        a = bitarray('11011')
        a.invert()
        self.assertEQUAL(a, bitarray('00100'))
        a.invert(2)
        self.assertEQUAL(a, bitarray('00000'))
        a.invert(-1)
        self.assertEQUAL(a, bitarray('00001'))

    def test_invert_errors(self):
        a = bitarray(5)
        self.assertRaises(IndexError, a.invert, 5)
        self.assertRaises(IndexError, a.invert, -6)
        self.assertRaises(TypeError, a.invert, "A")
        self.assertRaises(TypeError, a.invert, 0, 1)

    def test_invert_random(self):
        for a in self.randombitarrays(start=1):
            b = a.copy()
            i = randint(0, len(a) - 1)
            b.invert(i)
            a[i] = not a[i]
            self.assertEQUAL(a, b)

    def test_sort_simple(self):
        a = bitarray('1101000')
        a.sort()
        self.assertEqual(a, bitarray('0000111'))
        self.check_obj(a)

        a = bitarray('1101000')
        a.sort(reverse=True)
        self.assertEqual(a, bitarray('1110000'))
        a.sort(reverse=False)
        self.assertEqual(a, bitarray('0000111'))
        a.sort(True)
        self.assertEqual(a, bitarray('1110000'))
        a.sort(False)
        self.assertEqual(a, bitarray('0000111'))

        self.assertRaises(TypeError, a.sort, 'A')

    def test_sort_random(self):
        for rev in False, True, 0, 1, 7, -1, -7, None:
            for a in self.randombitarrays():
                lst = a.tolist()
                if rev is None:
                    lst.sort()
                    a.sort()
                else:
                    lst.sort(reverse=rev)
                    a.sort(reverse=rev)
                self.assertEqual(a, bitarray(lst))
                self.check_obj(a)

    def test_reverse_explicit(self):
        for x, y in [('', ''), ('1', '1'), ('10', '01'), ('001', '100'),
                     ('1110', '0111'), ('11100', '00111'),
                     ('011000', '000110'), ('1101100', '0011011'),
                     ('11110000', '00001111'),
                     ('11111000011', '11000011111'),
                     ('11011111 00100000 000111',
                      '111000 00000100 11111011')]:
            a = bitarray(x)
            a.reverse()
            self.assertEQUAL(a, bitarray(y))
            self.check_obj(a)

        self.assertRaises(TypeError, bitarray().reverse, 42)

    def test_reverse_random(self):
        for a in self.randombitarrays():
            b = a.copy()
            a.reverse()
            self.assertEQUAL(a, bitarray(reversed(b), endian=a.endian()))
            self.assertEQUAL(a, b[::-1])
            self.check_obj(a)

    def test_tolist(self):
        a = bitarray()
        self.assertEqual(a.tolist(), [])

        a = bitarray('110')
        lst = a.tolist()
        self.assertIsInstance(lst, list)
        self.assertEqual(repr(lst), '[1, 1, 0]')

        for lst in self.randomlists():
            a = bitarray(lst)
            self.assertEqual(a.tolist(), lst)

    def test_remove(self):
        a = bitarray('1010110')
        for val, res in [(False, '110110'), (True, '10110'),
                         (1, '0110'), (1, '010'), (0, '10'),
                         (0, '1'), (1, '')]:
            a.remove(val)
            self.assertEQUAL(a, bitarray(res))
            self.check_obj(a)

        a = bitarray('0010011')
        a.remove(1)
        self.assertEQUAL(a, bitarray('000011'))
        self.assertRaises(TypeError, a.remove, 'A')
        self.assertRaises(ValueError, a.remove, 21)

    def test_remove_errors(self):
        a = bitarray()
        for i in (True, False, 1, 0):
            self.assertRaises(ValueError, a.remove, i)

        a = bitarray(21)
        a.setall(0)
        self.assertRaises(ValueError, a.remove, 1)
        a.setall(1)
        self.assertRaises(ValueError, a.remove, 0)

    def test_pop_simple(self):
        for x, n, r, y in [('1',       0, 1, ''),
                           ('0',      -1, 0, ''),
                           ('0011100', 3, 1, '001100')]:
            a = bitarray(x)
            self.assertTrue(a.pop(n) is r)
            self.assertEqual(a, bitarray(y))
            self.check_obj(a)

        a = bitarray('01')
        self.assertEqual(a.pop(), True)
        self.assertEqual(a.pop(), False)
        # pop from empty bitarray
        self.assertRaises(IndexError, a.pop)

    def test_pop_random_1(self):
        for a in self.randombitarrays():
            self.assertRaises(IndexError, a.pop, len(a))
            self.assertRaises(IndexError, a.pop, -len(a) - 1)
            if len(a) == 0:
                continue
            aa = a.tolist()
            enda = a.endian()
            self.assertEqual(a.pop(), aa[-1])
            self.check_obj(a)
            self.assertEqual(a.endian(), enda)

    def test_pop_random_2(self):
        for a in self.randombitarrays(start=1):
            n = randint(-len(a), len(a)-1)
            aa = a.tolist()
            x = a.pop(n)
            self.assertBitEqual(x, aa[n])
            y = aa.pop(n)
            self.assertEqual(a, bitarray(aa))
            self.assertBitEqual(x, y)
            self.check_obj(a)

    def test_clear(self):
        for a in self.randombitarrays():
            endian = a.endian()
            a.clear()
            self.assertEqual(len(a), 0)
            self.assertEqual(a.endian(), endian)
            self.check_obj(a)

    def test_setall(self):
        a = bitarray(5)
        a.setall(True)
        self.assertRaises(ValueError, a.setall, -1)
        self.assertRaises(TypeError, a.setall, None)
        self.assertEQUAL(a, bitarray('11111'))
        a.setall(0)
        self.assertEQUAL(a, bitarray('00000'))
        self.check_obj(a)

    def test_setall_empty(self):
        a = bitarray()
        for v in 0, 1:
            a.setall(v)
            self.assertEqual(a, bitarray())
            self.check_obj(a)

    def test_setall_random(self):
        for a in self.randombitarrays():
            val = randint(0, 1)
            a.setall(val)
            self.assertEqual(a, bitarray(len(a) * [val]))
            self.check_obj(a)

    def test_bytereverse_explicit_all(self):
        for x, y in [('', ''),
                     ('1', '0'),
                     ('1011', '0000'),
                     ('111011', '001101'),
                     ('11101101', '10110111'),
                     ('000000011', '100000000'),
                     ('11011111 00100000 000111',
                      '11111011 00000100 001110')]:
            a = bitarray(x)
            a.bytereverse()
            self.assertEqual(a, bitarray(y))

    def test_bytereverse_explicit_range(self):
        a = bitarray('11100000 00000011 00111111 111110')
        a.bytereverse(0, 1)  # reverse byte 0
        self.assertEqual(a, bitarray('00000111 00000011 00111111 111110'))
        a.bytereverse(1, 3)  # reverse bytes 1 and 2
        self.assertEqual(a, bitarray('00000111 11000000 11111100 111110'))
        a.bytereverse(2)  # reverse bytes 2 till end of buffer
        self.assertEqual(a, bitarray('00000111 11000000 00111111 000111'))
        a.bytereverse(3)  # reverse last byte
        self.assertEqual(a, bitarray('00000111 11000000 00111111 001110'))
        a.bytereverse(3, 1)  # start > stop (nothing to reverse)
        self.assertEqual(a, bitarray('00000111 11000000 00111111 001110'))
        a.bytereverse(0, 4)  # reverse all bytes
        self.assertEqual(a, bitarray('11100000 00000011 11111100 000111'))

        self.assertRaises(IndexError, a.bytereverse, -1)
        self.assertRaises(IndexError, a.bytereverse, 0, -1)
        self.assertRaises(IndexError, a.bytereverse, 5)
        self.assertRaises(IndexError, a.bytereverse, 0, 5)

    def test_bytereverse_byte(self):
        for i in range(256):
            a = bitarray()
            a.frombytes(bytes(bytearray([i])))
            self.assertEqual(len(a), 8)
            b = a.copy()
            b.bytereverse()
            self.assertEqual(b, a[::-1])
            a.reverse()
            self.assertEqual(b, a)
            self.check_obj(b)

    def test_bytereverse_random(self):
        t = bitarray(endian=self.random_endian())
        t.frombytes(bytes(bytearray(range(256))))
        t.bytereverse()
        table = t.tobytes()  # translation table
        self.assertEqual(table[:9], b'\x00\x80\x40\xc0\x20\xa0\x60\xe0\x10')

        for n in range(100):
            a = urandom(8 * n, self.random_endian())
            i = randint(0, n)  # start
            j = randint(0, n)  # stop
            b = a.copy()
            memoryview(b)[i:j] = b.tobytes()[i:j].translate(table)
            a.bytereverse(i, j)
            self.assertEQUAL(a, b)
            self.check_obj(a)

    def test_bytereverse_endian(self):
        for n in range(20):
            a = urandom(8 * n, self.random_endian())
            b = a.copy()
            a.bytereverse()
            a = bitarray(a, self.other_endian(a.endian()))
            self.assertEqual(a.tobytes(), b.tobytes())

tests.append(MethodTests)

# ---------------------------------------------------------------------------

class CountTests(unittest.TestCase, Util):

    def test_basic(self):
        a = bitarray('10011')
        self.assertEqual(a.count(), 3)
        self.assertEqual(a.count(True), 3)
        self.assertEqual(a.count(False), 2)
        self.assertEqual(a.count(1), 3)
        self.assertEqual(a.count(0), 2)
        self.assertRaises(ValueError, a.count, 2)
        self.assertRaises(TypeError, a.count, None)
        self.assertRaises(TypeError, a.count, '')
        self.assertRaises(TypeError, a.count, 'A')
        self.assertRaises(TypeError, a.count, 1, 2.0)
        self.assertRaises(TypeError, a.count, 1, 2, 4.0)
        self.assertRaises(TypeError, a.count, 0, 'A')
        self.assertRaises(TypeError, a.count, 0, 0, 'A')

    def test_byte(self):
        for i in range(256):
            a = bitarray()
            a.frombytes(bytes(bytearray([i])))
            self.assertEqual(len(a), 8)
            self.assertEqual(a.count(), bin(i)[2:].count('1'))

    def test_whole_range(self):
        for a in self.randombitarrays():
            s = a.to01()
            self.assertEqual(a.count(1), s.count('1'))
            self.assertEqual(a.count(0), s.count('0'))

    def test_zeros(self):
        N = 37
        a = zeros(N)
        for i in range(N):
            for j in range(i, N):
                self.assertEqual(a.count(0, i, j), j - i)

    def test_explicit(self):
        a = bitarray('01001100 01110011 01')
        self.assertEqual(a.count(), 9)
        self.assertEqual(a.count(0, 12), 3)
        self.assertEqual(a.count(1, -5), 3)
        self.assertEqual(a.count(1, 2, 17), 7)
        self.assertEqual(a.count(1, 6, 11), 2)
        self.assertEqual(a.count(0, 7, -3), 4)
        self.assertEqual(a.count(1, 1, -1), 8)
        self.assertEqual(a.count(1, 17, 14), 0)

    def test_random(self):
        for a in self.randombitarrays():
            s = a.to01()
            i = randint(-3, len(a) + 2)
            j = randint(-3, len(a) + 2)
            self.assertEqual(a.count(1, i, j), s[i:j].count('1'))
            self.assertEqual(a.count(0, i, j), s[i:j].count('0'))

tests.append(CountTests)

# ---------------------------------------------------------------------------

class IndexTests(unittest.TestCase, Util):

    def test_simple(self):
        a = bitarray()
        for i in True, False, 1, 0:
            self.assertEqual(a.find(i), -1)
            self.assertRaises(ValueError, a.index, i)

        a = zeros(100)
        self.assertRaises(TypeError, a.find)
        self.assertRaises(TypeError, a.find, 1, 'a')
        self.assertRaises(TypeError, a.find, 1, 0, 'a')
        self.assertRaises(TypeError, a.find, 1, 0, 100, 1)

        self.assertRaises(ValueError, a.index, True)
        self.assertRaises(TypeError, a.index)
        self.assertRaises(TypeError, a.index, 1, 'a')
        self.assertRaises(TypeError, a.index, 1, 0, 'a')
        self.assertRaises(TypeError, a.index, 1, 0, 100, 1)

        a[20] = a[27] = 1
        for i in 1, True, bitarray('1'), bitarray('10'):
            self.assertEqual(a.index(i), 20)
            self.assertEqual(a.index(i, 21), 27)
            self.assertEqual(a.index(i, 27), 27)
            self.assertEqual(a.index(i, -73), 27)
        self.assertRaises(ValueError, a.index, -1)
        self.assertRaises(TypeError, a.index, None)
        self.assertRaises(ValueError, a.index, 1, 5, 17)
        self.assertRaises(ValueError, a.index, 1, 5, -83)
        self.assertRaises(ValueError, a.index, 1, 21, 27)
        self.assertRaises(ValueError, a.index, 1, 28)
        self.assertEqual(a.index(0), 0)
        self.assertEqual(a.find(0), 0)

        s = bitarray()
        self.assertEqual(a.index(s), 0)
        self.assertEqual(a.find(s), 0)

    def test_200(self):
        a = 200 * bitarray('1')
        self.assertRaises(ValueError, a.index, False)
        self.assertEqual(a.find(False), -1)
        a[173] = a[187] = a[189] = 0
        for i in 0, False, bitarray('0'), bitarray('01'):
            self.assertEqual(a.index(i), 173)
            self.assertEqual(a.find(i), 173)
        self.assertEqual(a.index(True), 0)
        self.assertEqual(a.find(True), 0)
        s = bitarray('010')
        self.assertEqual(a.index(s), 187)
        self.assertEqual(a.find(s), 187)

    def test_range(self):
        n = 250
        a = bitarray(n)
        for m in range(n):
            a.setall(0)
            self.assertRaises(ValueError, a.index, 1)
            self.assertEqual(a.find(1), -1)
            a[m] = 1
            self.assertEqual(a.index(1), m)
            self.assertEqual(a.find(1), m)

            a.setall(1)
            self.assertRaises(ValueError, a.index, 0)
            self.assertEqual(a.find(0), -1)
            a[m] = 0
            self.assertEqual(a.index(0), m)
            self.assertEqual(a.find(0), m)

    def test_explicit(self):
        for endian in 'big', 'little':
            a = bitarray('00001000 00000000 0010000', endian)
            self.assertEqual(a.index(1), 4)
            self.assertEqual(a.index(1, 1), 4)
            self.assertEqual(a.index(0, 4), 5)
            self.assertEqual(a.index(1, 5), 18)
            self.assertEqual(a.index(1, -11), 18)
            self.assertEqual(a.index(1, -50), 4)
            self.assertRaises(ValueError, a.index, 1, 5, 18)
            self.assertRaises(ValueError, a.index, 1, 19)

            self.assertEqual(a.find(1), 4)
            self.assertEqual(a.find(1, 1), 4)
            self.assertEqual(a.find(0, 4), 5)
            self.assertEqual(a.find(1, 5), 18)
            self.assertEqual(a.find(1, -11), 18)
            self.assertEqual(a.find(1, -50), 4)
            self.assertEqual(a.find(1, 5, 18), -1)
            self.assertEqual(a.find(1, 19), -1)

    def test_explicit_2(self):
        a = bitarray('10010101 11001111 1001011', self.random_endian())
        s = bitarray('011', self.random_endian())
        self.assertEqual(a.index(s, 15), 20)
        self.assertEqual(a.index(s, -3), 20)
        self.assertRaises(ValueError, a.index, s, 15, 22)
        self.assertRaises(ValueError, a.index, s, 15, -1)

        self.assertEqual(a.find(s, 15), 20)
        self.assertEqual(a.find(s, -3), 20)
        self.assertEqual(a.find(s, 15, 22), -1)
        self.assertEqual(a.find(s, 15, -1), -1)

    def test_random_start_stop(self):
        n = 2000
        a = zeros(n)
        for _ in range(100):
            a[randint(0, n - 1)] = 1
        aa = a.tolist()
        for _ in range(100):
            start = randint(0, n)
            stop = randint(0, n)
            try:  # reference from list
                ref = aa.index(1, start, stop)
            except ValueError:
                ref = -1

            res1 = a.find(1, start, stop)
            self.assertEqual(res1, ref)

            try:
                res2 = a.index(1, start, stop)
            except ValueError:
                res2 = -1
            self.assertEqual(res2, ref)

    def test_random_2(self):
        for n in range(1, 70):
            a = bitarray(n)
            i = randint(0, 1)
            a.setall(i)
            for _ in range(randint(1, 4)):
                a.invert(randint(0, n - 1))
            aa = a.tolist()
            for _ in range(10):
                start = randint(-10, n + 10)
                stop = randint(-10, n + 10)
                try:
                    res0 = aa.index(not i, start, stop)
                except ValueError:
                    res0 = -1

                res1 = a.find(not i, start, stop)
                self.assertEqual(res1, res0)

                try:
                    res2 = a.index(not i, start, stop)
                except ValueError:
                    res2 = -1
                self.assertEqual(res2, res0)

    def test_random_3(self):
        for a in self.randombitarrays():
            aa = a.to01()
            if a:
                self.assertEqual(a.find(a), 0)
                self.assertEqual(a.index(a), 0)

            for sub in '0', '1', '01', '01', '11', '101', '1111', '00100':
                b = bitarray(sub, self.random_endian())
                self.assertEqual(a.find(b), aa.find(sub))

                i = randint(-len(a) - 3, len(a) + 2)
                j = randint(-len(a) - 3, len(a) + 2)
                ref = aa.find(sub, i, j)
                self.assertEqual(a.find(b, i, j), ref)
                if ref == -1:
                    self.assertRaises(ValueError, a.index, b, i, j)
                else:
                    self.assertEqual(a.index(b, i, j), ref)

tests.append(IndexTests)

# ---------------------------------------------------------------------------

class SearchTests(unittest.TestCase, Util):

    def test_simple(self):
        a = bitarray()
        for s in 0, 1, False, True, bitarray('0'), bitarray('1'):
            self.assertEqual(a.search(s), [])

        a = bitarray('00100')
        for s in 1, True, bitarray('1'), bitarray('10'):
            self.assertEqual(a.search(s), [2])

        a = 100 * bitarray('1')
        self.assertEqual(a.search(0), [])
        self.assertEqual(a.search(1), list(range(100)))

        a = bitarray('10010101110011111001011')
        for limit in range(10):
            self.assertEqual(a.search(bitarray('011'), limit),
                             [6, 11, 20][:limit])

        self.assertRaises(ValueError, a.search, bitarray())
        self.assertRaises(TypeError, a.search, '010')

    def test_itersearch(self):
        a = bitarray('10011')
        self.assertRaises(ValueError, a.itersearch, bitarray())
        self.assertRaises(TypeError, a.itersearch, 1, 0)
        self.assertRaises(TypeError, a.itersearch, '')
        it = a.itersearch(1)
        self.assertIsType(it, 'searchiterator')
        self.assertEqual(next(it), 0)
        self.assertEqual(next(it), 3)
        self.assertEqual(next(it), 4)
        self.assertStopIteration(it)

    def test_explicit_1(self):
        a = bitarray('10011', self.random_endian())
        for s, res in [('0',     [1, 2]),  ('1', [0, 3, 4]),
                       ('01',    [2]),     ('11', [3]),
                       ('000',   []),      ('1001', [0]),
                       ('011',   [2]),     ('0011', [1]),
                       ('10011', [0]),     ('100111', [])]:
            b = bitarray(s, self.random_endian())
            self.assertEqual(a.search(b), res)
            self.assertEqual(list(a.itersearch(b)), res)

    def test_explicit_2(self):
        a = bitarray('10010101 11001111 1001011')
        for s, res in [('011', [6, 11, 20]),
                       ('111', [7, 12, 13, 14]),  # note the overlap
                       ('1011', [5, 19]),
                       ('100', [0, 9, 16])]:
            b = bitarray(s)
            self.assertEqual(a.search(b), res)
            self.assertEqual(list(a.itersearch(b)), res)

    def test_bool_random(self):
        for a in self.randombitarrays():
            b = a.copy()
            b.setall(0)
            for i in a.itersearch(1):
                b[i] = 1
            self.assertEQUAL(b, a)

            b.setall(1)
            for i in a.itersearch(0):
                b[i] = 0
            self.assertEQUAL(b, a)

            s = set(a.search(0) + a.search(1))
            self.assertEqual(len(s), len(a))

    def test_random(self):
        for a in self.randombitarrays():
            aa = a.to01()
            if a:
                self.assertEqual(a.search(a), [0])
                self.assertEqual(list(a.itersearch(a)), [0])

            for sub in '0', '1', '01', '01', '11', '101', '1101', '01100':
                b = bitarray(sub, self.random_endian())
                plst = [i for i in range(len(a)) if a[i:i + len(b)] == b]
                self.assertEqual(a.search(b), plst)
                for p in a.itersearch(b):
                    self.assertEqual(a[p:p + len(b)], b)

tests.append(SearchTests)

# ---------------------------------------------------------------------------

class BytesTests(unittest.TestCase, Util):

    @staticmethod
    def randombytes():
        for n in range(1, 20):
            yield os.urandom(n)

    def test_frombytes_simple(self):
        a = bitarray(endian='big')
        a.frombytes(b'A')
        self.assertEqual(a, bitarray('01000001'))

        b = a
        b.frombytes(b'BC')
        self.assertEQUAL(b, bitarray('01000001 01000010 01000011',
                                     endian='big'))
        self.assertTrue(b is a)

    def test_frombytes_empty(self):
        for a in self.randombitarrays():
            b = a.copy()
            a.frombytes(b'')
            self.assertEQUAL(a, b)
            self.assertFalse(a is b)
            self.check_obj(a)

    def test_frombytes_errors(self):
        a = bitarray()
        self.assertRaises(TypeError, a.frombytes)
        self.assertRaises(TypeError, a.frombytes, b'', b'')
        self.assertRaises(TypeError, a.frombytes, 1)
        self.check_obj(a)

    def test_frombytes_random(self):
        for b in self.randombitarrays():
            for s in self.randombytes():
                a = bitarray(endian=b.endian())
                a.frombytes(s)
                c = b.copy()
                b.frombytes(s)
                self.assertEQUAL(b[-len(a):], a)
                self.assertEQUAL(b[:-len(a)], c)
                self.assertEQUAL(b, c + a)
                self.check_obj(a)

    def test_tobytes_empty(self):
        a = bitarray()
        self.assertEqual(a.tobytes(), b'')

    def test_tobytes_endian(self):
        for end in ('big', 'little'):
            a = bitarray(endian=end)
            a.frombytes(b'foo')
            self.assertEqual(a.tobytes(), b'foo')

            for s in self.randombytes():
                a = bitarray(endian=end)
                a.frombytes(s)
                self.assertEqual(a.tobytes(), s)
                self.check_obj(a)

    def test_tobytes_explicit_ones(self):
        for n, s in [(1, b'\x01'), (2, b'\x03'), (3, b'\x07'), (4, b'\x0f'),
                     (5, b'\x1f'), (6, b'\x3f'), (7, b'\x7f'), (8, b'\xff'),
                     (12, b'\xff\x0f'), (15, b'\xff\x7f'), (16, b'\xff\xff'),
                     (17, b'\xff\xff\x01'), (24, b'\xff\xff\xff')]:
            a = bitarray(n, endian='little')
            a.setall(1)
            self.assertEqual(a.tobytes(), s)

    def test_unpack_simple(self):
        a = bitarray('01')
        self.assertIsInstance(a.unpack(), bytes)
        self.assertEqual(a.unpack(), b'\x00\x01')
        self.assertEqual(a.unpack(b'A'), b'A\x01')
        self.assertEqual(a.unpack(b'0', b'1'), b'01')
        self.assertEqual(a.unpack(one=b'\xff'), b'\x00\xff')
        self.assertEqual(a.unpack(zero=b'A'), b'A\x01')
        self.assertEqual(a.unpack(one=b't', zero=b'f'), b'ft')

    def test_unpack_random(self):
        for a in self.randombitarrays():
            self.assertEqual(a.unpack(b'0', b'1'),
                             a.to01().encode())
            # round trip
            b = bitarray()
            b.pack(a.unpack())
            self.assertEqual(b, a)
            # round trip with invert
            b = bitarray()
            b.pack(a.unpack(b'\x01', b'\x00'))
            b.invert()
            self.assertEqual(b, a)

    def test_unpack_errors(self):
        a = bitarray('01')
        self.assertRaises(TypeError, a.unpack, b'')
        self.assertRaises(TypeError, a.unpack, b'0', b'')
        self.assertRaises(TypeError, a.unpack, b'a', zero=b'b')
        self.assertRaises(TypeError, a.unpack, foo=b'b')
        self.assertRaises(TypeError, a.unpack, one=b'aa', zero=b'b')
        if is_py3k:
            self.assertRaises(TypeError, a.unpack, '0')
            self.assertRaises(TypeError, a.unpack, one='a')
            self.assertRaises(TypeError, a.unpack, b'0', '1')

    def test_pack_simple(self):
        for endian in 'little', 'big':
            _set_default_endian(endian)
            a = bitarray()
            a.pack(bytes())
            self.assertEQUAL(a, bitarray())
            a.pack(b'\x00')
            self.assertEQUAL(a, bitarray('0'))
            a.pack(b'\xff')
            self.assertEQUAL(a, bitarray('01'))
            a.pack(b'\x01\x00\x7a')
            self.assertEQUAL(a, bitarray('01101'))
            self.check_obj(a)

    def test_pack_allbytes(self):
        a = bitarray()
        a.pack(bytes(bytearray(range(256))))
        self.assertEqual(a, bitarray('0' + 255 * '1'))
        self.check_obj(a)

    def test_pack_errors(self):
        a = bitarray()
        self.assertRaises(TypeError, a.pack, 0)
        if is_py3k:
            self.assertRaises(TypeError, a.pack, '1')
        self.assertRaises(TypeError, a.pack, [1, 3])
        self.assertRaises(TypeError, a.pack, bitarray())

tests.append(BytesTests)

# ---------------------------------------------------------------------------

class FileTests(unittest.TestCase, Util):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tmpfname = os.path.join(self.tmpdir, 'testfile')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def read_file(self):
        with open(self.tmpfname, 'rb') as fi:
            return fi.read()

    def assertFileSize(self, size):
        self.assertEqual(os.path.getsize(self.tmpfname), size)


    def test_pickle(self):
        d1 = {i: a for i, a in enumerate(self.randombitarrays())}
        with open(self.tmpfname, 'wb') as fo:
            pickle.dump(d1, fo)
        with open(self.tmpfname, 'rb') as fi:
            d2 = pickle.load(fi)
        for key in d1.keys():
            self.assertEQUAL(d1[key], d2[key])

    def test_pickle_load(self):
        if not is_py3k:
            return
        # the test data file was created using bitarray 1.5.0 / Python 3.5.5
        path = os.path.join(os.path.dirname(__file__), 'test_data.pickle')
        with open(path, 'rb') as fi:
            d = pickle.load(fi)

        for i, (s, end) in enumerate([
                ('110', 'little'),
                ('011', 'big'),
                ('1110000001001000000000000000001', 'little'),
                ('0010011110000000000000000000001', 'big'),
        ]):
            b = d['b%d' % i]
            self.assertEqual(b.to01(), s)
            self.assertEqual(b.endian(), end)
            self.assertIsType(b, 'bitarray')
            self.check_obj(b)

            f = d['f%d' % i]
            self.assertEqual(f.to01(), s)
            self.assertEqual(f.endian(), end)
            self.assertIsType(f, 'frozenbitarray')
            self.check_obj(f)

    def test_shelve(self):
        if hasattr(sys, 'gettotalrefcount'):
            return

        d1 = shelve.open(self.tmpfname)
        stored = []
        for a in self.randombitarrays():
            key = str(len(a))
            d1[key] = a
            stored.append((key, a))
        d1.close()

        d2 = shelve.open(self.tmpfname)
        for k, v in stored:
            self.assertEQUAL(d2[k], v)
        d2.close()

    def test_fromfile_empty(self):
        with open(self.tmpfname, 'wb') as fo:
            pass
        self.assertFileSize(0)

        a = bitarray()
        with open(self.tmpfname, 'rb') as fi:
            a.fromfile(fi)
        self.assertEqual(a, bitarray())
        self.check_obj(a)

    def test_fromfile_Foo(self):
        with open(self.tmpfname, 'wb') as fo:
            fo.write(b'Foo')
        self.assertFileSize(3)

        a = bitarray(endian='big')
        with open(self.tmpfname, 'rb') as fi:
            a.fromfile(fi)
        self.assertEqual(a, bitarray('01000110 01101111 01101111'))

        a = bitarray(endian='little')
        with open(self.tmpfname, 'rb') as fi:
            a.fromfile(fi)
        self.assertEqual(a, bitarray('01100010 11110110 11110110'))

    def test_fromfile_wrong_args(self):
        a = bitarray()
        self.assertRaises(TypeError, a.fromfile)
        self.assertRaises(Exception, a.fromfile, 42)
        self.assertRaises(Exception, a.fromfile, 'bar')

        with open(self.tmpfname, 'wb') as fo:
            pass
        with open(self.tmpfname, 'rb') as fi:
            self.assertRaises(TypeError, a.fromfile, fi, None)

    def test_fromfile_erros(self):
        with open(self.tmpfname, 'wb') as fo:
            fo.write(b'0123456789')
        self.assertFileSize(10)

        a = bitarray()
        with open(self.tmpfname, 'wb') as fi:
            self.assertRaises(Exception, a.fromfile, fi)

        if is_py3k:
            with open(self.tmpfname, 'r') as fi:
                self.assertRaises(TypeError, a.fromfile, fi)

    def test_from_large_files(self):
        for N in range(65534, 65538):
            data = os.urandom(N)
            with open(self.tmpfname, 'wb') as fo:
                fo.write(data)

            a = bitarray()
            with open(self.tmpfname, 'rb') as fi:
                a.fromfile(fi)
            self.assertEqual(len(a), 8 * N)
            self.assertEqual(buffer_info(a, 'size'), N)
            self.assertEqual(a.tobytes(), data)

    def test_fromfile_extend_existing(self):
        with open(self.tmpfname, 'wb') as fo:
            fo.write(b'Foo')

        foo_le = '011000101111011011110110'
        a = bitarray('1', endian='little')
        with open(self.tmpfname, 'rb') as fi:
            a.fromfile(fi)

        self.assertEqual(a, bitarray('1' + foo_le))

        for n in range(20):
            a = bitarray(n, endian='little')
            a.setall(1)
            with open(self.tmpfname, 'rb') as fi:
                a.fromfile(fi)
            self.assertEqual(a, bitarray(n * '1' + foo_le))

    def test_fromfile_n(self):
        a = bitarray()
        a.frombytes(b'ABCDEFGHIJ')
        with open(self.tmpfname, 'wb') as fo:
            a.tofile(fo)
        self.assertFileSize(10)

        with open(self.tmpfname, 'rb') as f:
            a = bitarray()
            a.fromfile(f, 0);  self.assertEqual(a.tobytes(), b'')
            a.fromfile(f, 1);  self.assertEqual(a.tobytes(), b'A')
            f.read(1)  # skip B
            a.fromfile(f, 1);  self.assertEqual(a.tobytes(), b'AC')
            a = bitarray()
            a.fromfile(f, 2);  self.assertEqual(a.tobytes(), b'DE')
            a.fromfile(f, 1);  self.assertEqual(a.tobytes(), b'DEF')
            a.fromfile(f, 0);  self.assertEqual(a.tobytes(), b'DEF')
            a.fromfile(f);     self.assertEqual(a.tobytes(), b'DEFGHIJ')
            a.fromfile(f);     self.assertEqual(a.tobytes(), b'DEFGHIJ')
            self.check_obj(a)

        a = bitarray()
        with open(self.tmpfname, 'rb') as f:
            f.read(1)
            self.assertRaises(EOFError, a.fromfile, f, 10)
        # check that although we received an EOFError, the bytes were read
        self.assertEqual(a.tobytes(), b'BCDEFGHIJ')

        a = bitarray()
        with open(self.tmpfname, 'rb') as f:
            # negative values - like ommiting the argument
            a.fromfile(f, -1)
            self.assertEqual(a.tobytes(), b'ABCDEFGHIJ')
            self.assertRaises(EOFError, a.fromfile, f, 1)

    def test_fromfile_BytesIO(self):
        f = BytesIO(b'somedata')
        a = bitarray()
        a.fromfile(f, 4)
        self.assertEqual(len(a), 32)
        self.assertEqual(a.tobytes(), b'some')
        a.fromfile(f)
        self.assertEqual(len(a), 64)
        self.assertEqual(a.tobytes(), b'somedata')
        self.check_obj(a)

    def test_tofile_empty(self):
        a = bitarray()
        with open(self.tmpfname, 'wb') as f:
            a.tofile(f)

        self.assertFileSize(0)

    def test_tofile_Foo(self):
        a = bitarray('0100011 001101111 01101111', endian='big')
        b = a.copy()
        with open(self.tmpfname, 'wb') as f:
            a.tofile(f)
        self.assertEQUAL(a, b)

        self.assertFileSize(3)
        self.assertEqual(self.read_file(), b'Foo')

    def test_tofile_random(self):
        for a in self.randombitarrays():
            with open(self.tmpfname, 'wb') as fo:
                a.tofile(fo)
            n = bits2bytes(len(a))
            self.assertFileSize(n)
            raw = self.read_file()
            self.assertEqual(len(raw), n)
            self.assertEqual(raw, a.tobytes())

    def test_tofile_errors(self):
        n = 100
        a = bitarray(8 * n)
        self.assertRaises(TypeError, a.tofile)

        with open(self.tmpfname, 'wb') as f:
            a.tofile(f)
        self.assertFileSize(n)
        # write to closed file
        self.assertRaises(ValueError, a.tofile, f)

        if is_py3k:
            with open(self.tmpfname, 'w') as f:
                self.assertRaises(TypeError, a.tofile, f)

        with open(self.tmpfname, 'rb') as f:
            self.assertRaises(Exception, a.tofile, f)

    def test_tofile_large(self):
        n = 100 * 1000
        a = bitarray(8 * n)
        a.setall(0)
        a[2::37] = 1
        with open(self.tmpfname, 'wb') as f:
            a.tofile(f)
        self.assertFileSize(n)

        raw = self.read_file()
        self.assertEqual(len(raw), n)
        self.assertEqual(raw, a.tobytes())

    def test_tofile_ones(self):
        for n in range(20):
            a = n * bitarray('1', endian='little')
            with open(self.tmpfname, 'wb') as fo:
                a.tofile(fo)

            raw = self.read_file()
            self.assertEqual(len(raw), bits2bytes(len(a)))
            # when we fill the unused bits in a, we can compare
            a.fill()
            b = bitarray(endian='little')
            b.frombytes(raw)
            self.assertEqual(a, b)

    def test_tofile_BytesIO(self):
        for n in list(range(10)) + list(range(65534, 65538)):
            data = os.urandom(n)
            a = bitarray(0, 'big')
            a.frombytes(data)
            self.assertEqual(len(a), 8 * n)
            f = BytesIO()
            a.tofile(f)
            self.assertEqual(f.getvalue(), data)

    def test_mmap(self):
        if not is_py3k:
            return
        import mmap

        with open(self.tmpfname, 'wb') as fo:
            fo.write(1000 * b'\0')

        with open(self.tmpfname, 'r+b') as f:  # see issue #141
            with mmap.mmap(f.fileno(), 0) as mapping:
                a = bitarray(buffer=mapping, endian='little')
                info = buffer_info(a)
                self.assertFalse(info['readonly'])
                self.assertTrue(info['imported'])
                self.assertEqual(a, zeros(8000))
                a[::2] = True
                # not sure this is necessary, without 'del a', I get:
                # BufferError: cannot close exported pointers exist
                del a

        self.assertEqual(self.read_file(), 1000 * b'\x55')

    def test_mmap_2(self):
        if not is_py3k:
            return
        from mmap import mmap

        with open(self.tmpfname, 'wb') as fo:
            fo.write(1000 * b'\x22')

        with open(self.tmpfname, 'r+b') as f:
            a = bitarray(buffer=mmap(f.fileno(), 0), endian='little')
            info = buffer_info(a)
            self.assertFalse(info['readonly'])
            self.assertTrue(info['imported'])
            self.assertEqual(a, 1000 * bitarray('0100 0100'))
            a[::4] = 1

        self.assertEqual(self.read_file(), 1000 * b'\x33')

    def test_mmap_readonly(self):
        if not is_py3k:
            return
        import mmap

        with open(self.tmpfname, 'wb') as fo:
            fo.write(994 * b'\x89' + b'Veedon')

        with open(self.tmpfname, 'rb') as fi:  # readonly
            m = mmap.mmap(fi.fileno(), 0, access=mmap.ACCESS_READ)
            a = bitarray(buffer=m, endian='big')
            info = buffer_info(a)
            self.assertTrue(info['readonly'])
            self.assertTrue(info['imported'])
            self.assertRaisesMessage(TypeError,
                                     "cannot modify read-only memory",
                                     a.__setitem__, 0, 1)
            self.assertEqual(a[:8 * 994], 994 * bitarray('1000 1001'))
            self.assertEqual(a[8 * 994:].tobytes(), b'Veedon')

tests.append(FileTests)

# ----------------------------- Decode Tree ---------------------------------

alphabet_code = {
    ' ': bitarray('001'),         '.': bitarray('0101010'),
    'a': bitarray('0110'),        'b': bitarray('0001100'),
    'c': bitarray('000011'),      'd': bitarray('01011'),
    'e': bitarray('111'),         'f': bitarray('010100'),
    'g': bitarray('101000'),      'h': bitarray('00000'),
    'i': bitarray('1011'),        'j': bitarray('0111101111'),
    'k': bitarray('00011010'),    'l': bitarray('01110'),
    'm': bitarray('000111'),      'n': bitarray('1001'),
    'o': bitarray('1000'),        'p': bitarray('101001'),
    'q': bitarray('00001001101'), 'r': bitarray('1101'),
    's': bitarray('1100'),        't': bitarray('0100'),
    'u': bitarray('000100'),      'v': bitarray('0111100'),
    'w': bitarray('011111'),      'x': bitarray('0000100011'),
    'y': bitarray('101010'),      'z': bitarray('00011011110')
}

class DecodeTreeTests(unittest.TestCase, Util):

    def test_create(self):
        dt = decodetree(alphabet_code)
        self.assertIsType(dt, 'decodetree')
        self.assertIsInstance(dt, decodetree)
        self.assertRaises(TypeError, decodetree, None)
        self.assertRaises(TypeError, decodetree, 'foo')
        d = dict(alphabet_code)
        d['-'] = bitarray()
        self.assertRaises(ValueError, decodetree, d)

    def test_ambiguous_code(self):
        for d in [
            {'a': bitarray('0'), 'b': bitarray('0'), 'c': bitarray('1')},
            {'a': bitarray('01'), 'b': bitarray('01'), 'c': bitarray('1')},
            {'a': bitarray('0'), 'b': bitarray('01')},
            {'a': bitarray('0'), 'b': bitarray('11'), 'c': bitarray('111')},
        ]:
            self.assertRaises(ValueError, decodetree, d)

    def test_sizeof(self):
        dt = decodetree({'.': bitarray('1')})
        self.assertTrue(0 < sys.getsizeof(dt) < 100)

        dt = decodetree({'a': zeros(20)})
        self.assertTrue(sys.getsizeof(dt) > 200)

    def test_nodes(self):
        for n in range(1, 20):
            dt = decodetree({'a': zeros(n)})
            self.assertEqual(dt.nodes(), n + 1)

        dt = decodetree({'I': bitarray('1'),   'l': bitarray('01'),
                         'a': bitarray('001'), 'n': bitarray('000')})
        self.assertEqual(dt.nodes(), 7)
        dt = decodetree(alphabet_code)
        self.assertEqual(dt.nodes(), 70)

    def test_todict(self):
        t = decodetree(alphabet_code)
        d = t.todict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d, alphabet_code)

    def test_decode(self):
        t = decodetree(alphabet_code)
        a = bitarray('1011 01110 0110 1001')
        self.assertEqual(a.decode(t), ['i', 'l', 'a', 'n'])
        self.assertEqual(''.join(a.iterdecode(t)), 'ilan')
        a = bitarray()
        self.assertEqual(a.decode(t), [])
        self.assertEqual(''.join(a.iterdecode(t)), '')
        self.check_obj(a)

    def test_large(self):
        d = {i: bitarray(bool((1 << j) & i) for j in range(10))
             for i in range(1024)}
        t = decodetree(d)
        self.assertEqual(t.todict(), d)
        self.assertEqual(t.nodes(), 2047)
        self.assertTrue(sys.getsizeof(t) > 10000)

tests.append(DecodeTreeTests)

# ------------------ variable length encoding and decoding ------------------

class PrefixCodeTests(unittest.TestCase, Util):

    def test_encode_string(self):
        a = bitarray()
        a.encode(alphabet_code, '')
        self.assertEqual(a, bitarray())
        a.encode(alphabet_code, 'a')
        self.assertEqual(a, bitarray('0110'))

    def test_encode_list(self):
        a = bitarray()
        a.encode(alphabet_code, [])
        self.assertEqual(a, bitarray())
        a.encode(alphabet_code, ['e'])
        self.assertEqual(a, bitarray('111'))

    def test_encode_iter(self):
        a = bitarray()
        d = {0: bitarray('0'), 1: bitarray('1')}
        a.encode(d, iter([0, 1, 1, 0]))
        self.assertEqual(a, bitarray('0110'))

        def foo():
            for c in 1, 1, 0, 0, 1, 1:
                yield c

        a.clear()
        a.encode(d, foo())
        a.encode(d, range(2))
        self.assertEqual(a, bitarray('11001101'))
        self.assertEqual(d, {0: bitarray('0'), 1: bitarray('1')})

    def test_encode_symbol_not_in_code(self):
        d = dict(alphabet_code)
        a = bitarray()
        a.encode(d, 'is')
        self.assertEqual(a, bitarray('1011 1100'))
        self.assertRaises(ValueError, a.encode, d, 'ilAn')
        msg = "symbol not defined in prefix code"
        if is_py3k:
            msg += ": None"
        self.assertRaisesMessage(ValueError, msg, a.encode, d, [None, 2])

    def test_encode_not_iterable(self):
        d = {'a': bitarray('0'), 'b': bitarray('1')}
        a = bitarray()
        a.encode(d, 'abba')
        self.assertRaises(TypeError, a.encode, d, 42)
        self.assertRaises(TypeError, a.encode, d, 1.3)
        self.assertRaises(TypeError, a.encode, d, None)
        self.assertEqual(a, bitarray('0110'))

    def test_check_codedict_encode(self):
        a = bitarray()
        self.assertRaises(TypeError, a.encode, None, '')
        self.assertRaises(ValueError, a.encode, {}, '')
        self.assertRaises(TypeError, a.encode, {'a': 'b'}, 'a')
        self.assertRaises(ValueError, a.encode, {'a': bitarray()}, 'a')
        self.assertEqual(len(a), 0)

    def test_check_codedict_decode(self):
        a = bitarray('101')
        self.assertRaises(TypeError, a.decode, 0)
        self.assertRaises(ValueError, a.decode, {})
        self.assertRaises(TypeError, a.decode, {'a': 42})
        self.assertRaises(ValueError, a.decode, {'a': bitarray()})
        self.assertEqual(a, bitarray('101'))

    def test_check_codedict_iterdecode(self):
        a = bitarray('1100101')
        self.assertRaises(TypeError, a.iterdecode, 0)
        self.assertRaises(ValueError, a.iterdecode, {})
        self.assertRaises(TypeError, a.iterdecode, {'a': []})
        self.assertRaises(ValueError, a.iterdecode, {'a': bitarray()})
        self.assertEqual(a, bitarray('1100101'))

    def test_decode_simple(self):
        d = {'I': bitarray('1'),   'l': bitarray('01'),
             'a': bitarray('001'), 'n': bitarray('000')}
        dcopy = dict(d)
        a = bitarray('101001000')
        res = list("Ilan")
        self.assertEqual(a.decode(d), res)
        self.assertEqual(list(a.iterdecode(d)), res)
        self.assertEqual(d, dcopy)
        self.assertEqual(a, bitarray('101001000'))

    def test_iterdecode_type(self):
        a = bitarray()
        it = a.iterdecode(alphabet_code)
        self.assertIsType(it, 'decodeiterator')

    def test_iterdecode_remove_tree(self):
        d = {'I': bitarray('1'),   'l': bitarray('01'),
             'a': bitarray('001'), 'n': bitarray('000')}
        t = decodetree(d)
        a = bitarray('101001000')
        it = a.iterdecode(t)
        del t
        self.assertEqual(''.join(it), "Ilan")

    def test_decode_empty(self):
        d = {'a': bitarray('1')}
        a = bitarray()
        self.assertEqual(a.decode(d), [])
        self.assertEqual(d, {'a': bitarray('1')})
        # test decode iterator
        self.assertEqual(list(a.iterdecode(d)), [])
        self.assertEqual(d, {'a': bitarray('1')})
        self.assertEqual(len(a), 0)

    def test_decode_incomplete(self):
        d = {'a': bitarray('0'), 'b': bitarray('111')}
        a = bitarray('00011')
        msg = "incomplete prefix code at position 3"
        self.assertRaisesMessage(ValueError, msg, a.decode, d)
        it = a.iterdecode(d)
        self.assertIsType(it, 'decodeiterator')
        self.assertRaisesMessage(ValueError, msg, list, it)
        t = decodetree(d)
        self.assertRaisesMessage(ValueError, msg, a.decode, t)
        self.assertRaisesMessage(ValueError, msg, list, a.iterdecode(t))

        self.assertEqual(a, bitarray('00011'))
        self.assertEqual(d, {'a': bitarray('0'), 'b': bitarray('111')})
        self.assertEqual(t.todict(), d)

    def test_decode_incomplete_2(self):
        a = bitarray()
        a.encode(alphabet_code, "now we rise")
        x = len(a)
        a.extend('00')
        msg = "incomplete prefix code at position %d" % x
        self.assertRaisesMessage(ValueError, msg, a.decode, alphabet_code)

    def test_decode_buggybitarray(self):
        d = dict(alphabet_code)
        #             i    s    t
        a = bitarray('1011 1100 0100 011110111001101001')
        msg = "prefix code unrecognized in bitarray at position 12 .. 21"
        self.assertRaisesMessage(ValueError, msg, a.decode, d)
        self.assertRaisesMessage(ValueError, msg, list, a.iterdecode(d))
        t = decodetree(d)
        self.assertRaisesMessage(ValueError, msg, a.decode, t)
        self.assertRaisesMessage(ValueError, msg, list, a.iterdecode(d))

        self.check_obj(a)
        self.assertEqual(t.todict(), d)

    def test_iterdecode_no_term(self):
        d = {'a': bitarray('0'), 'b': bitarray('111')}
        a = bitarray('011')
        it = a.iterdecode(d)
        self.assertEqual(next(it), 'a')
        self.assertRaisesMessage(ValueError,
                                 "incomplete prefix code at position 1",
                                 next, it)
        self.assertEqual(a, bitarray('011'))

    def test_iterdecode_buggybitarray(self):
        d = {'a': bitarray('0')}
        a = bitarray('1')
        it = a.iterdecode(d)
        self.assertRaises(ValueError, next, it)
        self.assertEqual(a, bitarray('1'))
        self.assertEqual(d, {'a': bitarray('0')})

    def test_decode_buggybitarray2(self):
        d = {'a': bitarray('00'), 'b': bitarray('01')}
        a = bitarray('1')
        self.assertRaises(ValueError, a.decode, d)
        self.assertRaises(ValueError, next, a.iterdecode(d))

        t = decodetree(d)
        self.assertRaises(ValueError, a.decode, t)
        self.assertRaises(ValueError, next, a.iterdecode(t))

        self.assertEqual(a, bitarray('1'))
        self.assertEqual(d, {'a': bitarray('00'), 'b': bitarray('01')})
        self.assertEqual(t.todict(), d)

    def test_decode_random(self):
        pat1 = re.compile(r'incomplete prefix code.+\s(\d+)')
        pat2 = re.compile(r'prefix code unrecognized.+\s(\d+)\s*\.\.\s*(\d+)')
        t = decodetree(alphabet_code)
        for a in self.randombitarrays():
            try:
                a.decode(t)
            except ValueError as e:
                msg = str(e)
                m1 = pat1.match(msg)
                m2 = pat2.match(msg)
                self.assertFalse(m1 and m2)
                if m1:
                    i = int(m1.group(1))
                if m2:
                    i, j = int(m2.group(1)), int(m2.group(2))
                    self.assertFalse(a[i:j] in alphabet_code.values())
                a[:i].decode(t)

    def test_decode_ambiguous_code(self):
        for d in [
            {'a': bitarray('0'), 'b': bitarray('0'), 'c': bitarray('1')},
            {'a': bitarray('01'), 'b': bitarray('01'), 'c': bitarray('1')},
            {'a': bitarray('0'), 'b': bitarray('01')},
            {'a': bitarray('0'), 'b': bitarray('11'), 'c': bitarray('111')},
        ]:
            a = bitarray()
            self.assertRaises(ValueError, a.decode, d)
            self.assertRaises(ValueError, a.iterdecode, d)
            self.check_obj(a)

    def test_miscitems(self):
        d = {None : bitarray('00'),
             0    : bitarray('110'),
             1    : bitarray('111'),
             ''   : bitarray('010'),
             2    : bitarray('011')}
        a = bitarray()
        a.encode(d, [None, 0, 1, '', 2])
        self.assertEqual(a, bitarray('00110111010011'))
        self.assertEqual(a.decode(d), [None, 0, 1, '', 2])
        # iterator
        it = a.iterdecode(d)
        self.assertEqual(next(it), None)
        self.assertEqual(next(it), 0)
        self.assertEqual(next(it), 1)
        self.assertEqual(next(it), '')
        self.assertEqual(next(it), 2)
        self.assertStopIteration(it)

    def test_quick_example(self):
        a = bitarray()
        message = 'the quick brown fox jumps over the lazy dog.'
        a.encode(alphabet_code, message)
        self.assertEqual(a, bitarray(
            # t    h     e       q           u      i    c      k
            '0100 00000 111 001 00001001101 000100 1011 000011 00011010 001'
            # b       r    o    w      n        f      o    x
            '0001100 1101 1000 011111 1001 001 010100 1000 0000100011 001'
            # j          u      m      p      s        o    v       e   r
            '0111101111 000100 000111 101001 1100 001 1000 0111100 111 1101'
            #     t    h     e       l     a    z           y
            '001 0100 00000 111 001 01110 0110 00011011110 101010 001'
            # d     o    g      .
            '01011 1000 101000 0101010'))
        self.assertEqual(''.join(a.decode(alphabet_code)), message)
        self.assertEqual(''.join(a.iterdecode(alphabet_code)), message)
        t = decodetree(alphabet_code)
        self.assertEqual(''.join(a.decode(t)), message)
        self.assertEqual(''.join(a.iterdecode(t)), message)
        self.check_obj(a)

tests.append(PrefixCodeTests)

# --------------------------- Buffer Import ---------------------------------

class BufferImportTests(unittest.TestCase, Util):

    def test_bytes(self):
        b = 100 * b'\0'
        a = bitarray(buffer=b)

        info = buffer_info(a)
        self.assertFalse(info['allocated'])
        self.assertTrue(info['readonly'])
        self.assertTrue(info['imported'])

        self.assertRaises(TypeError, a.setall, 1)
        self.assertRaises(TypeError, a.clear)
        self.assertEqual(a, zeros(800))
        self.check_obj(a)

    def test_bytearray(self):
        b = bytearray(100 * [0])
        a = bitarray(buffer=b, endian='little')

        info = buffer_info(a)
        self.assertFalse(info['allocated'])
        self.assertFalse(info['readonly'])
        self.assertTrue(info['imported'])

        a[0] = 1
        self.assertEqual(b[0], 1)
        a[7] = 1
        self.assertEqual(b[0], 129)
        a[:] = 1
        self.assertEqual(b, bytearray(100 * [255]))
        self.assertRaises(BufferError, a.pop)
        a[8:16] = bitarray('10000010', endian='big')
        self.assertEqual(b, bytearray([255, 65] + 98 * [255]))
        self.assertEqual(a.tobytes(), bytes(b))
        for n in 7, 9:
            self.assertRaises(BufferError, a.__setitem__, slice(8, 16),
                              bitarray(n))
        b[1] = b[2] = 255
        self.assertEqual(b, bytearray(100 * [255]))
        self.assertEqual(a, 800 * bitarray('1'))
        self.check_obj(a)

    def test_array(self):
        if not is_py3k:  # Python 2's array cannot be used as buffer
            return
        from array import array

        a = array('B', [0, 255, 64])
        b = bitarray(None, 'little', a)
        self.assertEqual(b, bitarray('00000000 11111111 00000010'))
        a[1] = 32
        self.assertEqual(b, bitarray('00000000 00000100 00000010'))
        b[3] = 1
        self.assertEqual(a.tolist(), [8, 32, 64])
        self.check_obj(b)

    def test_bitarray(self):
        a = urandom(10000)
        b = bitarray(buffer=a)
        # a and b are two distict bitarrays that share the same buffer now
        self.assertFalse(a is b)

        a_info = buffer_info(a)
        self.assertFalse(a_info['imported'])
        self.assertEqual(a_info['exports'], 1)
        b_info = buffer_info(b)
        self.assertTrue(b_info['imported'])
        self.assertEqual(b_info['exports'], 0)
        # buffer address is the same!
        self.assertEqual(a_info['address'],
                         b_info['address'])

        self.assertFalse(a is b)
        self.assertEqual(a, b)
        b[437:461] = 0
        self.assertEqual(a, b)
        a[327:350] = 1
        self.assertEqual(a, b)
        b[101:1187] <<= 79
        self.assertEqual(a, b)
        a[100:9800:5] = 1
        self.assertEqual(a, b)

        self.assertRaisesMessage(
            BufferError,
            "cannot resize bitarray that is exporting buffers",
            a.pop)
        self.assertRaisesMessage(
            BufferError,
            "cannot resize imported buffer",
            b.pop)
        self.check_obj(a)
        self.check_obj(b)

    def test_bitarray_range(self):
        for n in range(100):
            a = urandom(n, self.random_endian())
            b = bitarray(buffer=a, endian=a.endian())
            # an imported buffer will always have a multiple of 8 bits
            self.assertEqual(len(b) % 8, 0)
            self.assertEQUAL(b[:n], a)
            self.check_obj(a)
            self.check_obj(b)

    def test_bitarray_chain(self):
        a = urandom(64)
        d = {0: a}
        for n in range(1, 100):
            d[n] = bitarray(buffer=d[n - 1])

        self.assertEqual(d[99], a)
        a.setall(0)
        self.assertEqual(d[99], zeros(64))
        a[:] = 1
        self.assertTrue(d[99].all())
        for c in d.values():
            self.check_obj(c)

    def test_frozenbitarray(self):
        a = frozenbitarray('10011011 011')
        self.assertTrue(buffer_info(a, 'readonly'))
        self.check_obj(a)

        b = bitarray(buffer=a)
        self.assertTrue(buffer_info(b, 'readonly'))  # also readonly
        self.assertRaises(TypeError, b.__setitem__, 1, 0)
        self.check_obj(b)

    def test_invalid_buffer(self):
        # these objects do not expose a buffer
        for arg in (123, 1.23, Ellipsis, [1, 2, 3], (1, 2, 3), {1: 2},
                    set([1, 2, 3]),):
            self.assertRaises(TypeError, bitarray, buffer=arg)

    def test_del_import_object(self):
        b = bytearray(100 * [0])
        a = bitarray(buffer=b)
        del b
        self.assertEqual(a, zeros(800))
        a.setall(1)
        self.assertTrue(a.all())
        self.check_obj(a)

    def test_readonly_errors(self):
        a = bitarray(buffer=b'A')
        info = buffer_info(a)
        self.assertTrue(info['readonly'])
        self.assertTrue(info['imported'])

        self.assertRaises(TypeError, a.append, True)
        self.assertRaises(TypeError, a.bytereverse)
        self.assertRaises(TypeError, a.clear)
        self.assertRaises(TypeError, a.encode, {'a': bitarray('0')}, 'aa')
        self.assertRaises(TypeError, a.extend, [0, 1, 0])
        self.assertRaises(TypeError, a.fill)
        self.assertRaises(TypeError, a.frombytes, b'')
        self.assertRaises(TypeError, a.insert, 0, 1)
        self.assertRaises(TypeError, a.invert)
        self.assertRaises(TypeError, a.pack, b'\0\0\xff')
        self.assertRaises(TypeError, a.pop)
        self.assertRaises(TypeError, a.remove, 1)
        self.assertRaises(TypeError, a.reverse)
        self.assertRaises(TypeError, a.setall, 0)
        self.assertRaises(TypeError, a.sort)
        self.assertRaises(TypeError, a.__delitem__, 0)
        self.assertRaises(TypeError, a.__delitem__, slice(None, None, 2))
        self.assertRaises(TypeError, a.__setitem__, 0, 0)
        self.assertRaises(TypeError, a.__iadd__, bitarray('010'))
        self.assertRaises(TypeError, a.__ior__, bitarray('100'))
        self.assertRaises(TypeError, a.__ixor__, bitarray('110'))
        self.assertRaises(TypeError, a.__irshift__, 1)
        self.assertRaises(TypeError, a.__ilshift__, 1)
        self.check_obj(a)

    def test_resize_errors(self):
        a = bitarray(buffer=bytearray([123]))
        info = buffer_info(a)
        self.assertFalse(info['readonly'])
        self.assertTrue(info['imported'])

        self.assertRaises(BufferError, a.append, True)
        self.assertRaises(BufferError, a.clear)
        self.assertRaises(BufferError, a.encode, {'a': bitarray('0')}, 'aa')
        self.assertRaises(BufferError, a.extend, [0, 1, 0])
        self.assertRaises(BufferError, a.fill)
        self.assertRaises(BufferError, a.frombytes, b'a')
        self.assertRaises(BufferError, a.insert, 0, 1)
        self.assertRaises(BufferError, a.pack, b'\0\0\xff')
        self.assertRaises(BufferError, a.pop)
        self.assertRaises(BufferError, a.remove, 1)
        self.assertRaises(BufferError, a.__delitem__, 0)
        self.check_obj(a)

tests.append(BufferImportTests)

# --------------------------- Buffer Export ---------------------------------

class BufferExportTests(unittest.TestCase, Util):

    def test_read_simple(self):
        a = bitarray('01000001 01000010 01000011', endian='big')
        v = memoryview(a)
        self.assertFalse(v.readonly)
        self.assertEqual(buffer_info(a, 'exports'), 1)
        self.assertEqual(len(v), 3)
        self.assertEqual(v[0], 65 if is_py3k else 'A')
        self.assertEqual(v.tobytes(), b'ABC')
        a[13] = 1
        self.assertEqual(v.tobytes(), b'AFC')

        w = memoryview(a)  # a second buffer export
        self.assertEqual(buffer_info(a, 'exports'), 2)
        self.check_obj(a)

    def test_many_exports(self):
        a = bitarray('01000111 01011111')
        d = {}  # put bitarrays in dict to key object around
        for n in range(1, 20):
            d[n] = bitarray(buffer=a)
            self.assertEqual(buffer_info(a, 'exports'), n)
            self.assertEqual(len(d[n]), 16)
        self.check_obj(a)

    def test_range(self):
        for n in range(100):
            a = bitarray(n)
            v = memoryview(a)
            self.assertEqual(len(v), bits2bytes(len(a)))
            info = buffer_info(a)
            self.assertFalse(info['readonly'])
            self.assertFalse(info['imported'])
            self.assertEqual(info['exports'], 1)
            self.check_obj(a)

    def test_read_random(self):
        a = bitarray()
        a.frombytes(os.urandom(100))
        v = memoryview(a)
        self.assertEqual(len(v), 100)
        b = a[34 * 8 : 67 * 8]
        self.assertEqual(v[34:67].tobytes(), b.tobytes())
        self.assertEqual(v.tobytes(), a.tobytes())
        self.check_obj(a)

    def test_resize(self):
        a = bitarray('011', endian='big')
        v = memoryview(a)
        self.assertFalse(v.readonly)
        self.assertRaises(BufferError, a.append, 1)
        self.assertRaises(BufferError, a.clear)
        self.assertRaises(BufferError, a.encode, {'a': bitarray('0')}, 'aa')
        self.assertRaises(BufferError, a.extend, '0')
        self.assertRaises(BufferError, a.fill)
        self.assertRaises(BufferError, a.frombytes, b'\0')
        self.assertRaises(BufferError, a.insert, 0, 1)
        self.assertRaises(BufferError, a.pack, b'\0')
        self.assertRaises(BufferError, a.pop)
        self.assertRaises(BufferError, a.remove, 1)
        self.assertRaises(BufferError, a.__delitem__, slice(0, 8))
        self.assertEqual(v.tobytes(), a.tobytes())
        self.check_obj(a)

    def test_frozenbitarray(self):
        a = frozenbitarray(40)
        v = memoryview(a)
        self.assertTrue(v.readonly)
        self.assertEqual(len(v), 5)
        self.assertEqual(v.tobytes(), a.tobytes())
        self.check_obj(a)

    def test_write(self):
        a = bitarray(8000)
        a.setall(0)
        v = memoryview(a)
        self.assertFalse(v.readonly)
        v[500] = 255 if is_py3k else '\xff'
        self.assertEqual(a[3999:4009], bitarray('0111111110'))
        a[4003] = 0
        self.assertEqual(a[3999:4009], bitarray('0111011110'))
        v[301:304] = b'ABC'
        self.assertEqual(a[300 * 8 : 305 * 8].tobytes(), b'\x00ABC\x00')
        self.check_obj(a)

    def test_write_py3(self):
        if not is_py3k:
            return
        a = bitarray(40)
        a.setall(0)
        m = memoryview(a)
        v = m[1:4]
        v[0] = 65
        v[1] = 66
        v[2] = 67
        self.assertEqual(a.tobytes(), b'\x00ABC\x00')
        self.check_obj(a)

tests.append(BufferExportTests)

# ---------------------------------------------------------------------------

class TestsFrozenbitarray(unittest.TestCase, Util):

    def test_init(self):
        a = frozenbitarray('110')
        self.assertEqual(a, bitarray('110'))
        self.assertEqual(a.to01(), '110')
        self.assertIsInstance(a, bitarray)
        self.assertIsType(a, 'frozenbitarray')
        self.assertTrue(buffer_info(a, 'readonly'))
        self.check_obj(a)

        a = frozenbitarray(bitarray())
        self.assertEQUAL(a, frozenbitarray())
        self.assertIsType(a, 'frozenbitarray')

        for endian in 'big', 'little':
            a = frozenbitarray(0, endian)
            self.assertEqual(a.endian(), endian)
            self.assertIsType(a, 'frozenbitarray')

            a = frozenbitarray(bitarray(0, endian))
            self.assertEqual(a.endian(), endian)
            self.assertIsType(a, 'frozenbitarray')

    def test_methods(self):
        # test a few methods which do not raise the TypeError
        a = frozenbitarray('1101100')
        self.assertEqual(a[2], 0)
        self.assertEqual(a[:4].to01(), '1101')
        self.assertEqual(a.count(), 4)
        self.assertEqual(a.index(0), 2)
        b = a.copy()
        self.assertEqual(b, a)
        self.assertIsType(b, 'frozenbitarray')
        self.assertEqual(len(b), 7)
        self.assertFalse(b.all())
        self.assertTrue(b.any())

    def test_init_from_bitarray(self):
        for a in self.randombitarrays():
            b = frozenbitarray(a)
            self.assertFalse(b is a)
            self.assertEQUAL(b, a)
            c = frozenbitarray(b)
            self.assertFalse(c is b)
            self.assertEQUAL(c, b)
            self.assertEqual(hash(c), hash(b))

    def test_init_from_misc(self):
        tup = 0, 1, 0, 1, 1, False, True
        for obj in list(tup), tup, iter(tup), bitarray(tup):
            a = frozenbitarray(obj)
            self.assertEqual(a, bitarray(tup))

    def test_repr(self):
        a = frozenbitarray()
        self.assertEqual(repr(a), "frozenbitarray()")
        self.assertEqual(str(a), "frozenbitarray()")
        a = frozenbitarray('10111')
        self.assertEqual(repr(a), "frozenbitarray('10111')")
        self.assertEqual(str(a), "frozenbitarray('10111')")

    def test_immutable(self):
        a = frozenbitarray('111')
        self.assertRaises(TypeError, a.append, True)
        self.assertRaises(TypeError, a.bytereverse)
        self.assertRaises(TypeError, a.clear)
        self.assertRaises(TypeError, a.encode, {'a': bitarray('0')}, 'aa')
        self.assertRaises(TypeError, a.extend, [0, 1, 0])
        self.assertRaises(TypeError, a.fill)
        self.assertRaises(TypeError, a.frombytes, b'')
        self.assertRaises(TypeError, a.insert, 0, 1)
        self.assertRaises(TypeError, a.invert)
        self.assertRaises(TypeError, a.pack, b'\0\0\xff')
        self.assertRaises(TypeError, a.pop)
        self.assertRaises(TypeError, a.remove, 1)
        self.assertRaises(TypeError, a.reverse)
        self.assertRaises(TypeError, a.setall, 0)
        self.assertRaises(TypeError, a.sort)
        self.assertRaises(TypeError, a.__delitem__, 0)
        self.assertRaises(TypeError, a.__delitem__, slice(None, None, 2))
        self.assertRaises(TypeError, a.__setitem__, 0, 0)
        self.assertRaises(TypeError, a.__iadd__, bitarray('010'))
        self.assertRaises(TypeError, a.__ior__, bitarray('100'))
        self.assertRaises(TypeError, a.__ixor__, bitarray('110'))
        self.assertRaises(TypeError, a.__irshift__, 1)
        self.assertRaises(TypeError, a.__ilshift__, 1)
        self.check_obj(a)

    def test_freeze(self):
        # not so much a test for frozenbitarray, but how it is initialized
        a = bitarray(78)
        self.assertFalse(buffer_info(a, 'readonly'))  # not readonly
        a._freeze()
        self.assertTrue(buffer_info(a, 'readonly'))   # readonly

    def test_memoryview(self):
        a = frozenbitarray('01000001 01000010', 'big')
        v = memoryview(a)
        self.assertEqual(v.tobytes(), b'AB')
        self.assertRaises(TypeError, v.__setitem__, 0, 255)

    def test_buffer_import_readonly(self):
        b = bytes(bytearray([15, 95, 128]))
        a = frozenbitarray(buffer=b, endian='big')
        self.assertEQUAL(a, bitarray('00001111 01011111 10000000', 'big'))
        info = buffer_info(a)
        self.assertTrue(info['readonly'])
        self.assertTrue(info['imported'])

    def test_buffer_import_writable(self):
        c = bytearray([15, 95])
        self.assertRaisesMessage(
            TypeError,
            "cannot import writable buffer into frozenbitarray",
            frozenbitarray, buffer=c)

    def test_set(self):
        a = frozenbitarray('1')
        b = frozenbitarray('11')
        c = frozenbitarray('01')
        d = frozenbitarray('011')
        s = set([a, b, c, d])
        self.assertEqual(len(s), 4)
        self.assertTrue(d in s)
        self.assertFalse(frozenbitarray('0') in s)

    def test_dictkey(self):
        a = frozenbitarray('01')
        b = frozenbitarray('1001')
        d = {a: 123, b: 345}
        self.assertEqual(d[frozenbitarray('01')], 123)
        self.assertEqual(d[frozenbitarray(b)], 345)

    def test_dictkey2(self):  # taken slightly modified from issue #74
        a1 = frozenbitarray([True, False])
        a2 = frozenbitarray([False, False])
        dct = {a1: "one", a2: "two"}
        a3 = frozenbitarray([True, False])
        self.assertEqual(a3, a1)
        self.assertEqual(dct[a3], 'one')

    def test_mix(self):
        a = bitarray('110')
        b = frozenbitarray('0011')
        self.assertEqual(a + b, bitarray('1100011'))
        a.extend(b)
        self.assertEqual(a, bitarray('1100011'))

    def test_hash_endianness_simple(self):
        a = frozenbitarray('1', 'big')
        b = frozenbitarray('1', 'little')
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        d = {a: 'value'}
        self.assertEqual(d[b], 'value')
        self.assertEqual(len(set([a, b])), 1)

    def test_hash_endianness_random(self):
        s = set()
        n = 0
        for a in self.randombitarrays():
            a = frozenbitarray(a)
            b = frozenbitarray(a, self.other_endian(a.endian()))
            self.assertEqual(a, b)
            self.assertNotEqual(a.endian(), b.endian())
            self.assertEqual(hash(a), hash(b))
            d = {a: 1, b: 2}
            self.assertEqual(len(d), 1)
            s.add(a)
            s.add(b)
            n += 1

        self.assertEqual(len(s), n)

    def test_pickle(self):
        for a in self.randombitarrays():
            f = frozenbitarray(a)
            g = pickle.loads(pickle.dumps(f))
            self.assertEqual(f, g)
            self.assertEqual(f.endian(), g.endian())
            self.assertTrue(str(g).startswith('frozenbitarray'))
            self.check_obj(a)

tests.append(TestsFrozenbitarray)

# ---------------------------------------------------------------------------

def run(verbosity=1, repeat=1):
    import bitarray.test_util as btu
    tests.extend(btu.tests)

    print('bitarray is installed in: %s' % os.path.dirname(__file__))
    print('bitarray version: %s' % __version__)
    print('sys.version: %s' % sys.version)
    print('sys.prefix: %s' % sys.prefix)
    print('pointer size: %d bit' % (8 * SYSINFO[0]))
    print('sizeof(size_t): %d' % SYSINFO[1])
    print('sizeof(bitarrayobject): %d' % SYSINFO[2])
    print('PY_UINT64_T defined: %s' % SYSINFO[5])
    print('DEBUG: %s' % DEBUG)
    suite = unittest.TestSuite()
    for cls in tests:
        for _ in range(repeat):
            suite.addTest(unittest.makeSuite(cls))

    runner = unittest.TextTestRunner(verbosity=verbosity)
    return runner.run(suite)


if __name__ == '__main__':
    run()
