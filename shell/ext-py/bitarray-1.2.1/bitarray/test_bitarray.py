"""
Tests for bitarray

Author: Ilan Schnell
"""
import os
import sys
import unittest
import tempfile
import shutil
from random import randint

is_py3k = bool(sys.version_info[0] == 3)

# imports needed inside tests
import copy
import pickle
import itertools

try:
    import shelve, hashlib
except ImportError:
    shelve = hashlib = None

if is_py3k:
    from io import StringIO
    unicode = str
else:
    from cStringIO import StringIO


from bitarray import (bitarray, frozenbitarray,
                      bitdiff, bits2bytes, __version__)


tests = []


class Util(object):

    @staticmethod
    def randombitarrays(start=1):
        for n in list(range(start, 25)) + [randint(1000, 2000)]:
            a = bitarray(endian=['little', 'big'][randint(0, 1)])
            a.frombytes(os.urandom(bits2bytes(n)))
            del a[n:]
            yield a

    @staticmethod
    def randomlists():
        for n in list(range(25)) + [randint(1000, 2000)]:
            yield [bool(randint(0, 1)) for d in range(n)]

    @staticmethod
    def rndsliceidx(length):
        if randint(0, 1):
            return None
        else:
            return randint(-length-5, length+5)

    @staticmethod
    def slicelen(s, length):
        assert isinstance(s, slice)
        start, stop, step = s.indices(length)
        slicelength = (stop - start + (1 if step < 0 else -1)) // step + 1
        if slicelength < 0:
            slicelength = 0
        return slicelength

    def check_obj(self, a):
        self.assertEqual(repr(type(a)), "<class 'bitarray.bitarray'>")
        unused = 8 * a.buffer_info()[1] - len(a)
        self.assertTrue(0 <= unused < 8)
        self.assertEqual(unused, a.buffer_info()[3])

    def assertEQUAL(self, a, b):
        self.assertEqual(a, b)
        self.assertEqual(a.endian(), b.endian())
        self.check_obj(a)
        self.check_obj(b)

    def assertStopIteration(self, it):
        if is_py3k:
            return
        self.assertRaises(StopIteration, it.next)

    if sys.version_info[:2] == (2, 6):
        def assertIsInstance(self, o, t):
            self.assertTrue(isinstance(o, t))

# ---------------------------------------------------------------------------

class TestsModuleFunctions(unittest.TestCase, Util):

    def test_bitdiff(self):
        a = bitarray('0011')
        b = bitarray('0101')
        self.assertEqual(bitdiff(a, b), 2)
        self.assertRaises(TypeError, bitdiff, a, '')
        self.assertRaises(TypeError, bitdiff, '1', b)
        self.assertRaises(TypeError, bitdiff, a, 4)
        b.append(1)
        self.assertRaises(ValueError, bitdiff, a, b)

        for n in list(range(50)) + [randint(1000, 2000)]:
            a = bitarray()
            a.frombytes(os.urandom(bits2bytes(n)))
            del a[n:]
            b = bitarray()
            b.frombytes(os.urandom(bits2bytes(n)))
            del b[n:]
            diff = sum(a[i] ^ b[i] for i in range(n))
            self.assertEqual(bitdiff(a, b), diff)

    def test_bits2bytes(self):
        for arg in ['foo', [], None, {}]:
            self.assertRaises(TypeError, bits2bytes, arg)

        self.assertRaises(TypeError, bits2bytes, 187.0)
        self.assertRaises(TypeError, bits2bytes, -4.0)
        self.assertRaises(TypeError, bits2bytes)
        self.assertRaises(TypeError, bits2bytes, 1, 2)

        self.assertRaises(ValueError, bits2bytes, -1)
        self.assertRaises(ValueError, bits2bytes, -924)

        self.assertEqual(bits2bytes(0), 0)
        for n in range(1, 100):
            self.assertEqual(bits2bytes(n), (n - 1) // 8 + 1)

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
        self.check_obj(a)

    def test_endian1(self):
        a = bitarray(endian='little')
        a.fromstring('A')
        self.assertEqual(a.endian(), 'little')
        self.assertIsInstance(a.endian(), str)
        self.check_obj(a)

        b = bitarray(endian='big')
        b.fromstring('A')
        self.assertEqual(b.endian(), 'big')
        self.assertIsInstance(a.endian(), str)
        self.check_obj(b)

        self.assertEqual(a.tostring(), b.tostring())

    def test_endian2(self):
        a = bitarray(endian='little')
        a.fromstring(' ')
        self.assertEqual(a.endian(), 'little')
        self.check_obj(a)

        b = bitarray(endian='big')
        b.fromstring(' ')
        self.assertEqual(b.endian(), 'big')
        self.check_obj(b)

        self.assertEqual(a.tostring(), b.tostring())

        self.assertRaises(TypeError, bitarray.__new__, bitarray, endian=0)
        self.assertRaises(ValueError, bitarray.__new__, bitarray, endian='')
        self.assertRaises(ValueError, bitarray.__new__,
                          bitarray, endian='foo')

    def test_integers(self):
        for n in range(50):
            a = bitarray(n)
            self.assertEqual(len(a), n)
            self.check_obj(a)

            a = bitarray(int(n))
            self.assertEqual(len(a), n)
            self.check_obj(a)

        self.assertRaises(ValueError, bitarray.__new__, bitarray, -1)
        self.assertRaises(ValueError, bitarray.__new__, bitarray, -924)

    def test_list(self):
        lst = ['foo', None, [1], {}]
        a = bitarray(lst)
        self.assertEqual(a.tolist(), [True, False, True, False])
        self.check_obj(a)

        for n in range(50):
            lst = [bool(randint(0, 1)) for d in range(n)]
            a = bitarray(lst)
            self.assertEqual(a.tolist(), lst)
            self.check_obj(a)

    def test_tuple(self):
        tup = ('', True, [], {1:2})
        a = bitarray(tup)
        self.assertEqual(a.tolist(), [False, True, False, True])
        self.check_obj(a)

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
        self.assertEqual(a, bitarray(10 * '0'))
        # Note that the through value of '0' is True: bool('0') -> True
        a = bitarray(itertools.repeat('0', 10))
        self.assertEqual(a, bitarray(10 * '1'))

    def test_01(self):
        a = bitarray('0010111')
        self.assertEqual(a.tolist(), [0, 0, 1, 0, 1, 1, 1])
        self.check_obj(a)

        for n in range(50):
            lst = [bool(randint(0, 1)) for d in range(n)]
            s = ''.join([['0', '1'][x] for x in lst])
            a = bitarray(s)
            self.assertEqual(a.tolist(), lst)
            self.check_obj(a)

        self.assertRaises(ValueError, bitarray.__new__, bitarray, '01012100')

    def test_rawbytes(self): # this representation is used for pickling
        for s, r in [(b'\x00', ''), (b'\x07\xff', '1'), (b'\x03\xff', '11111'),
                     (b'\x01\x87\xda', '10000111' '1101101')]:
            self.assertEqual(bitarray(s, endian='big'),
                             bitarray(r))

        for i in range(1, 8):
            self.assertRaises(ValueError, bitarray.__new__,
                              bitarray, bytes(bytearray([i])))

    def test_bitarray(self):
        for n in range(10):
            a = bitarray(n)
            b = bitarray(a)
            self.assertFalse(a is b)
            self.assertEQUAL(a, b)

        for end in ('little', 'big'):
            a = bitarray(endian=end)
            c = bitarray(a)
            self.assertEqual(c.endian(), end)
            c = bitarray(a, endian='little')
            self.assertEqual(c.endian(), 'little')
            c = bitarray(a, endian='big')
            self.assertEqual(c.endian(), 'big')


    def test_None(self):
        self.assertEQUAL(bitarray(), bitarray(0))
        self.assertEQUAL(bitarray(), bitarray(None))


    def test_WrongArgs(self):
        self.assertRaises(TypeError, bitarray.__new__, bitarray, 'A', 42, 69)

        self.assertRaises(TypeError, bitarray.__new__, bitarray, Ellipsis)
        self.assertRaises(TypeError, bitarray.__new__, bitarray, slice(0))

        self.assertRaises(TypeError, bitarray.__new__, bitarray, 2.345)
        self.assertRaises(TypeError, bitarray.__new__, bitarray, 4+3j)

        self.assertRaises(TypeError, bitarray.__new__, bitarray, '', 0, 42)
        self.assertRaises(ValueError, bitarray.__new__, bitarray, 0, 'foo')


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

class MetaDataTests(unittest.TestCase, Util):

    def test_buffer_info1(self):
        a = bitarray('0000111100001', endian='little')
        self.assertEqual(a.buffer_info()[1:4], (2, 'little', 3))

        a = bitarray()
        self.assertRaises(TypeError, a.buffer_info, 42)

        bi = a.buffer_info()
        self.assertIsInstance(bi, tuple)
        self.assertEqual(len(bi), 5)

        self.assertIsInstance(bi[0], int)
        if is_py3k:
            self.assertIsInstance(bi[1], int)
        self.assertIsInstance(bi[2], str)
        self.assertIsInstance(bi[3], int)
        if is_py3k:
            self.assertIsInstance(bi[4], int)

    def test_buffer_info2(self):
        for n in range(50):
            bi = bitarray(n).buffer_info()
            self.assertEqual(bi[1], bits2bytes(n))
            self.assertEqual(bi[3] + n, 8 * bi[1])
            self.assertTrue(bi[4] >= bi[1])

    def test_buffer_info3(self):
        a = bitarray(endian='little')
        self.assertEqual(a.buffer_info()[2], 'little')

        a = bitarray(endian='big')
        self.assertEqual(a.buffer_info()[2], 'big')


    def test_endian(self):
        a = bitarray(endian='little')
        self.assertEqual(a.endian(), 'little')

        a = bitarray(endian='big')
        self.assertEqual(a.endian(), 'big')


    def test_length(self):
        for n in range(100):
            a = bitarray(n)
            self.assertEqual(len(a), n)
            self.assertEqual(a.length(), n)


tests.append(MetaDataTests)

# ---------------------------------------------------------------------------

class SliceTests(unittest.TestCase, Util):

    def test_getitem1(self):
        a = bitarray()
        self.assertRaises(IndexError, a.__getitem__,  0)
        a.append(True)
        self.assertEqual(a[0], True)
        self.assertRaises(IndexError, a.__getitem__,  1)
        self.assertRaises(IndexError, a.__getitem__, -2)
        a.append(False)
        self.assertEqual(a[1], False)
        self.assertRaises(IndexError, a.__getitem__,  2)
        self.assertRaises(IndexError, a.__getitem__, -3)

    def test_getitem2(self):
        a = bitarray('1100010')
        for i, b in enumerate([True, True, False, False, False, True, False]):
            self.assertEqual(a[i], b)
            self.assertEqual(a[i-7], b)
        self.assertRaises(IndexError, a.__getitem__,  7)
        self.assertRaises(IndexError, a.__getitem__, -8)

    def test_getitem3(self):
        a = bitarray('0100000100001')
        self.assertEQUAL(a[:], a)
        self.assertFalse(a[:] is a)
        aa = a.tolist()
        self.assertEQUAL(a[11:2:-3], bitarray(aa[11:2:-3]))
        self.check_obj(a[:])

        self.assertRaises(ValueError, a.__getitem__, slice(None, None, 0))
        self.assertRaises(TypeError, a.__getitem__, (1, 2))

    def test_getitem4(self):
        for a in self.randombitarrays(start=1):
            aa = a.tolist()
            la = len(a)
            for dum in range(10):
                step = self.rndsliceidx(la) or None
                s = slice(self.rndsliceidx(la), self.rndsliceidx(la), step)
                self.assertEQUAL(a[s], bitarray(aa[s], endian=a.endian()))

    def test_setitem1(self):
        a = bitarray([False])
        a[0] = 1
        self.assertEqual(a, bitarray('1'))

        a = bitarray(2)
        a[0] = 0
        a[1] = 1
        self.assertEqual(a, bitarray('01'))
        a[-1] = 0
        a[-2] = 1
        self.assertEqual(a, bitarray('10'))

        self.assertRaises(IndexError, a.__setitem__,  2, True)
        self.assertRaises(IndexError, a.__setitem__, -3, False)

    def test_setitem2(self):
        for a in self.randombitarrays(start=1):
            la = len(a)
            i = randint(0, la - 1)
            aa = a.tolist()
            ida = id(a)
            val = bool(randint(0, 1))
            a[i] = val
            aa[i] = val
            self.assertEqual(a.tolist(), aa)
            self.assertEqual(id(a), ida)
            self.check_obj(a)

            b = bitarray(la)
            b[0:la] = bitarray(a)
            self.assertEqual(a, b)
            self.assertNotEqual(id(a), id(b))

            b = bitarray(la)
            b[:] = bitarray(a)
            self.assertEqual(a, b)
            self.assertNotEqual(id(a), id(b))

            b = bitarray(la)
            b[::-1] = bitarray(a)
            self.assertEqual(a.tolist()[::-1], b.tolist())

    def test_setitem3(self):
        a = bitarray('00000')
        a[0] = 1
        a[-2] = 1
        self.assertEqual(a, bitarray('10010'))
        self.assertRaises(IndexError, a.__setitem__, 5, 'foo')
        self.assertRaises(IndexError, a.__setitem__, -6, 'bar')

    def test_setitem4(self):
        for a in self.randombitarrays(start=1):
            la = len(a)
            for dum in range(10):
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

    def test_setslice_to_bitarray(self):
        a = bitarray('11111111' '1111')
        a[2:6] = bitarray('0010')
        self.assertEqual(a, bitarray('11001011' '1111'))
        a.setall(0)
        a[::2] = bitarray('111001')
        self.assertEqual(a, bitarray('10101000' '0010'))
        a.setall(0)
        a[3:] = bitarray('111')
        self.assertEqual(a, bitarray('000111'))
        a = bitarray(12)
        a.setall(0)
        a[1:11:2] = bitarray('11101')
        self.assertEqual(a, bitarray('01010100' '0100'))
        a = bitarray(12)
        a.setall(0)
        a[:-6:-1] = bitarray('10111')
        self.assertEqual(a, bitarray('00000001' '1101'))
        a = bitarray('1111')
        a[3:3] = bitarray('000')  # insert
        self.assertEqual(a, bitarray('1110001'))
        a[2:5] = bitarray()  # remove
        self.assertEqual(a, bitarray('1101'))
        a = bitarray('1111')
        a[1:3] = bitarray('0000')
        self.assertEqual(a, bitarray('100001'))
        a[:] = bitarray('010') # replace all values
        self.assertEqual(a, bitarray('010'))

    def test_setslice_to_bool(self):
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

    def test_sieve(self): # Sieve of Eratosthenes
        a = bitarray(50)
        a.setall(1)
        for i in range(2, 8):
            if a[i]:
                a[i*i::i] = 0
        primes = [i for i in range(2, 50) if a[i]]
        self.assertEqual(primes, [2, 3, 5, 7, 11, 13, 17, 19,
                                  23, 29, 31, 37, 41, 43, 47])

    def test_delitem1(self):
        a = bitarray('100110')
        del a[1]
        self.assertEqual(len(a), 5)
        del a[3]
        del a[-2]
        self.assertEqual(a, bitarray('100'))
        self.assertRaises(IndexError, a.__delitem__,  3)
        self.assertRaises(IndexError, a.__delitem__, -4)
        a = bitarray('10101100' '10110')
        del a[3:9] #     ^^^^^   ^
        self.assertEqual(a, bitarray('1010110'))
        del a[::3] #                  ^  ^  ^
        self.assertEqual(a, bitarray('0111'))
        a = bitarray('10101100' '1011011')
        del a[:-9:-2] #          ^ ^ ^ ^
        self.assertEqual(a, bitarray('10101100' '011'))

    def test_delitem2(self):
        for a in self.randombitarrays(start=1):
            la = len(a)
            for dum in range(10):
                step = self.rndsliceidx(la) or None
                s = slice(self.rndsliceidx(la), self.rndsliceidx(la), step)
                c = bitarray(a)
                d = c
                del c[s]
                self.assertTrue(c is d)
                self.check_obj(c)
                cc = a.tolist()
                del cc[s]
                self.assertEQUAL(c, bitarray(cc, endian=c.endian()))


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
        self.assertEqual(next(it), False)
        self.assertEqual(next(it), True)
        self.assertEqual(next(it), True)
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

    def test_compare(self):
        for a in self.randombitarrays():
            aa = a.tolist()
            for b in self.randombitarrays():
                bb = b.tolist()
                self.assertEqual(a == b, aa == bb)
                self.assertEqual(a != b, aa != bb)
                self.assertEqual(a <= b, aa <= bb)
                self.assertEqual(a <  b, aa <  bb)
                self.assertEqual(a >= b, aa >= bb)
                self.assertEqual(a >  b, aa >  bb)

    def test_subclassing(self):
        class ExaggeratingBitarray(bitarray):

            def __new__(cls, data, offset):
                return bitarray.__new__(cls, data)

            def __init__(self, data, offset):
                self.offset = offset

            def __getitem__(self, i):
                return bitarray.__getitem__(self, i - self.offset)

        for a in self.randombitarrays(start=1):
            b = ExaggeratingBitarray(a, 1234)
            for i in range(len(a)):
                self.assertEqual(a[i], b[i+1234])

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

    def test_endianness5(self):
        a = bitarray('11100000', endian='little')
        b = bitarray(a, endian='big')
        self.assertNotEqual(a, b)
        self.assertEqual(a.tobytes(), b.tobytes())

    def test_pickle(self):
        for v in range(3):
            for a in self.randombitarrays():
                b = pickle.loads(pickle.dumps(a, v))
                self.assertFalse(b is a)
                self.assertEQUAL(a, b)

    def test_overflow(self):
        if tuple.__itemsize__ == 8:
            return

        self.assertRaises(OverflowError, bitarray.__new__,
                          bitarray, 2**34 + 1)

        a = bitarray(10 ** 6)
        self.assertRaises(OverflowError, a.__imul__, 17180)

    def test_unicode1(self):
        a = bitarray(unicode())
        self.assertEqual(a, bitarray())

        a = bitarray(unicode('111001'))
        self.assertEqual(a, bitarray('111001'))

        for a in self.randombitarrays():
            b = bitarray(unicode(a.to01()))
            self.assertEqual(a, b)

    def test_unicode2(self):
        a = bitarray()
        a.extend(unicode())
        self.assertEqual(a, bitarray())

        a = bitarray()
        a.extend(unicode('001011'))
        self.assertEqual(a, bitarray('001011'))

        for a in self.randombitarrays():
            b = bitarray()
            b.extend(unicode(a.to01()))
            self.assertEqual(a, b)

    def test_unhashable(self):
        a = bitarray()
        self.assertRaises(TypeError, hash, a)
        self.assertRaises(TypeError, dict, [(a, 'foo')])

tests.append(MiscTests)

# ---------------------------------------------------------------------------

class SpecialMethodTests(unittest.TestCase, Util):

    def test_all(self):
        a = bitarray()
        self.assertTrue(a.all())
        for s, r in ('0', False), ('1', True), ('01', False):
            self.assertEqual(bitarray(s).all(), r)

        for a in self.randombitarrays():
            self.assertEqual(all(a), a.all())
            self.assertEqual(all(a.tolist()), a.all())

    def test_any(self):
        a = bitarray()
        self.assertFalse(a.any())
        for s, r in ('0', False), ('1', True), ('01', True):
            self.assertEqual(bitarray(s).any(), r)

        for a in self.randombitarrays():
            self.assertEqual(any(a), a.any())
            self.assertEqual(any(a.tolist()), a.any())


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

tests.append(SpecialMethodTests)

# ---------------------------------------------------------------------------

class NumberTests(unittest.TestCase, Util):

    def test_add(self):
        c = bitarray('001') + bitarray('110')
        self.assertEQUAL(c, bitarray('001110'))

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

        a = bitarray()
        self.assertRaises(TypeError, a.__add__, 42)


    def test_iadd(self):
        c = bitarray('001')
        c += bitarray('110')
        self.assertEQUAL(c, bitarray('001110'))

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

        a = bitarray()
        self.assertRaises(TypeError, a.__iadd__, 42)


    def test_mul(self):
        c = 0 * bitarray('1001111')
        self.assertEQUAL(c, bitarray())

        c = 3 * bitarray('001')
        self.assertEQUAL(c, bitarray('001001001'))

        c = bitarray('110') * 3
        self.assertEQUAL(c, bitarray('110110110'))

        for a in self.randombitarrays():
            b = a.copy()
            for n in range(-10, 20):
                c = a * n
                self.assertEQUAL(c, bitarray(n * a.tolist(),
                                             endian=a.endian()))
                c = n * a
                self.assertEqual(c, bitarray(n * a.tolist(),
                                             endian=a.endian()))
                self.assertEQUAL(a, b)

        a = bitarray()
        self.assertRaises(TypeError, a.__mul__, None)


    def test_imul(self):
        c = bitarray('1101110011')
        idc = id(c)
        c *= 0
        self.assertEQUAL(c, bitarray())
        self.assertEqual(idc, id(c))

        c = bitarray('110')
        c *= 3
        self.assertEQUAL(c, bitarray('110110110'))

        for a in self.randombitarrays():
            for n in range(-10, 10):
                b = a.copy()
                idb = id(b)
                b *= n
                self.assertEQUAL(b, bitarray(n * a.tolist(),
                                             endian=a.endian()))
                self.assertEqual(idb, id(b))

        a = bitarray()
        self.assertRaises(TypeError, a.__imul__, None)


tests.append(NumberTests)

# ---------------------------------------------------------------------------

class BitwiseTests(unittest.TestCase, Util):

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

    def test_and(self):
        a = bitarray('11001')
        b = bitarray('10011')
        self.assertEQUAL(a & b, bitarray('10001'))

        b = bitarray('1001')
        self.assertRaises(ValueError, a.__and__, b) # not same length

        self.assertRaises(TypeError, a.__and__, 42)

    def test_iand(self):
        a =  bitarray('110010110')
        ida = id(a)
        a &= bitarray('100110011')
        self.assertEQUAL(a, bitarray('100010010'))
        self.assertEqual(ida, id(a))

    def test_or(self):
        a = bitarray('11001')
        b = bitarray('10011')
        aa = a.copy()
        bb = b.copy()
        self.assertEQUAL(a | b, bitarray('11011'))
        self.assertEQUAL(a, aa)
        self.assertEQUAL(b, bb)

    def test_ior(self):
        a = bitarray('110010110')
        b = bitarray('100110011')
        bb = b.copy()
        a |= b
        self.assertEQUAL(a, bitarray('110110111'))
        self.assertEQUAL(b, bb)

    def test_xor(self):
        a = bitarray('11001')
        b = bitarray('10011')
        self.assertEQUAL(a ^ b, bitarray('01010'))

    def test_ixor(self):
        a =  bitarray('110010110')
        a ^= bitarray('100110011')
        self.assertEQUAL(a, bitarray('010100101'))

    def test_invert(self):
        a = bitarray()
        a.invert()
        self.assertEQUAL(a, bitarray())

        a = bitarray('11011')
        a.invert()
        self.assertEQUAL(a, bitarray('00100'))

        a = bitarray('11011')
        b = ~a
        self.assertEQUAL(b, bitarray('00100'))
        self.assertEQUAL(a, bitarray('11011'))
        self.assertFalse(a is b)

        for a in self.randombitarrays():
            b = bitarray(a)
            b.invert()
            for i in range(len(a)):
                self.assertEqual(b[i], not a[i])
            self.check_obj(b)
            c = ~a
            self.assertEQUAL(c, b)
            self.check_obj(c)


tests.append(BitwiseTests)

# ---------------------------------------------------------------------------

class SequenceTests(unittest.TestCase, Util):

    def test_contains1(self):
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

    def test_contains2(self):
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

    def test_contains3(self):
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

    def test_contains4(self):
        a = bitarray('011010000001')
        for s, r in [('', True), ('1', True), ('11', True), ('111', False),
                     ('011', True), ('0001', True), ('00011', False)]:
            self.assertEqual(bitarray(s) in a, r)


tests.append(SequenceTests)

# ---------------------------------------------------------------------------

class ExtendTests(unittest.TestCase, Util):

    def test_wrongArgs(self):
        a = bitarray()
        self.assertRaises(TypeError, a.extend)
        self.assertRaises(TypeError, a.extend, None)
        self.assertRaises(TypeError, a.extend, True)
        self.assertRaises(TypeError, a.extend, 24)
        self.assertRaises(ValueError, a.extend, '0011201')

    def test_bitarray(self):
        a = bitarray()
        a.extend(bitarray())
        self.assertEqual(a, bitarray())
        a.extend(bitarray('110'))
        self.assertEqual(a, bitarray('110'))
        a.extend(bitarray('1110'))
        self.assertEqual(a, bitarray('1101110'))

        a = bitarray('00001111', endian='little')
        a.extend(bitarray('00111100', endian='big'))
        self.assertEqual(a, bitarray('0000111100111100'))

        for a in self.randombitarrays():
            for b in self.randombitarrays():
                c = bitarray(a)
                idc = id(c)
                c.extend(b)
                self.assertEqual(id(c), idc)
                self.assertEqual(c, a + b)

    def test_list(self):
        a = bitarray()
        a.extend([0, 1, 3, None, {}])
        self.assertEqual(a, bitarray('01100'))
        a.extend([True, False])
        self.assertEqual(a, bitarray('0110010'))

        for a in self.randomlists():
            for b in self.randomlists():
                c = bitarray(a)
                idc = id(c)
                c.extend(b)
                self.assertEqual(id(c), idc)
                self.assertEqual(c.tolist(), a + b)
                self.check_obj(c)

    def test_tuple(self):
        a = bitarray()
        a.extend((0, 1, 2, 0, 3))
        self.assertEqual(a, bitarray('01101'))

        for a in self.randomlists():
            for b in self.randomlists():
                c = bitarray(a)
                idc = id(c)
                c.extend(tuple(b))
                self.assertEqual(id(c), idc)
                self.assertEqual(c.tolist(), a + b)
                self.check_obj(c)

    def test_generator(self):
        def bar():
            for x in ('', '1', None, True, []):
                yield x
        a = bitarray()
        a.extend(bar())
        self.assertEqual(a, bitarray('01010'))

        for a in self.randomlists():
            for b in self.randomlists():
                def foo():
                    for e in b:
                        yield e
                c = bitarray(a)
                idc = id(c)
                c.extend(foo())
                self.assertEqual(id(c), idc)
                self.assertEqual(c.tolist(), a + b)
                self.check_obj(c)

    def test_iterator1(self):
        a = bitarray()
        a.extend(iter([3, 9, 0, 1, -2]))
        self.assertEqual(a, bitarray('11011'))

        for a in self.randomlists():
            for b in self.randomlists():
                c = bitarray(a)
                idc = id(c)
                c.extend(iter(b))
                self.assertEqual(id(c), idc)
                self.assertEqual(c.tolist(), a + b)
                self.check_obj(c)

    def test_iterator2(self):
        a = bitarray()
        a.extend(itertools.repeat(True, 23))
        self.assertEqual(a, bitarray(23 * '1'))

    def test_string01(self):
        a = bitarray()
        a.extend('0110111')
        self.assertEqual(a, bitarray('0110111'))

        for a in self.randomlists():
            for b in self.randomlists():
                c = bitarray(a)
                idc = id(c)
                c.extend(''.join(['0', '1'][x] for x in b))
                self.assertEqual(id(c), idc)
                self.assertEqual(c.tolist(), a + b)
                self.check_obj(c)

    def test_extend_self(self):
        a = bitarray()
        a.extend(a)
        self.assertEqual(a, bitarray())

        a = bitarray('1')
        a.extend(a)
        self.assertEqual(a, bitarray('11'))

        a = bitarray('110')
        a.extend(a)
        self.assertEqual(a, bitarray('110110'))

        for a in self.randombitarrays():
            b = bitarray(a)
            a.extend(a)
            self.assertEqual(a, b + b)


tests.append(ExtendTests)

# ---------------------------------------------------------------------------

class MethodTests(unittest.TestCase, Util):

    def test_append(self):
        a = bitarray()
        a.append(True)
        a.append(False)
        a.append(False)
        self.assertEQUAL(a, bitarray('100'))
        a.append(0)
        a.append(1)
        a.append(2)
        a.append(None)
        a.append('')
        a.append('a')
        self.assertEQUAL(a, bitarray('100011001'))

        for a in self.randombitarrays():
            aa = a.tolist()
            b = a
            b.append(1)
            self.assertTrue(a is b)
            self.check_obj(b)
            self.assertEQUAL(b, bitarray(aa+[1], endian=a.endian()))
            b.append('')
            self.assertEQUAL(b, bitarray(aa+[1, 0], endian=a.endian()))


    def test_insert(self):
        a = bitarray()
        b = a
        a.insert(0, True)
        self.assertTrue(a is b)
        self.assertEqual(a, bitarray('1'))
        self.assertRaises(TypeError, a.insert)
        self.assertRaises(TypeError, a.insert, None)

        for a in self.randombitarrays():
            aa = a.tolist()
            for _ in range(50):
                item = bool(randint(0, 1))
                pos = randint(-len(a) - 2, len(a) + 2)
                a.insert(pos, item)
                aa.insert(pos, item)
                self.assertEqual(a.tolist(), aa)
                self.check_obj(a)


    def test_index1(self):
        a = bitarray()
        for i in (True, False, 1, 0):
            self.assertRaises(ValueError, a.index, i)

        a = bitarray(100 * [False])
        self.assertRaises(ValueError, a.index, True)
        self.assertRaises(TypeError, a.index)
        self.assertRaises(TypeError, a.index, 1, 'a')
        self.assertRaises(TypeError, a.index, 1, 0, 'a')
        self.assertRaises(TypeError, a.index, 1, 0, 100, 1)
        a[20] = a[27] = 1
        self.assertEqual(a.index(42), 20)
        self.assertEqual(a.index(1, 21), 27)
        self.assertEqual(a.index(1, 27), 27)
        self.assertEqual(a.index(1, -73), 27)
        self.assertRaises(ValueError, a.index, 1, 5, 17)
        self.assertRaises(ValueError, a.index, 1, 5, -83)
        self.assertRaises(ValueError, a.index, 1, 21, 27)
        self.assertRaises(ValueError, a.index, 1, 28)
        self.assertEqual(a.index(0), 0)

        a = bitarray(200 * [True])
        self.assertRaises(ValueError, a.index, False)
        a[173] = a[187] = 0
        self.assertEqual(a.index(False), 173)
        self.assertEqual(a.index(True), 0)

    def test_index2(self):
        for n in range(50):
            for m in range(n):
                a = bitarray(n)
                a.setall(0)
                self.assertRaises(ValueError, a.index, 1)
                a[m] = 1
                self.assertEqual(a.index(1), m)

                a.setall(1)
                self.assertRaises(ValueError, a.index, 0)
                a[m] = 0
                self.assertEqual(a.index(0), m)

    def test_index3(self):
        a = bitarray('00001000' '00000000' '0010000')
        self.assertEqual(a.index(1), 4)
        self.assertEqual(a.index(1, 1), 4)
        self.assertEqual(a.index(0, 4), 5)
        self.assertEqual(a.index(1, 5), 18)
        self.assertRaises(ValueError, a.index, 1, 5, 18)
        self.assertRaises(ValueError, a.index, 1, 19)

    def test_index4(self):
        a = bitarray('11110111' '11111111' '1101111')
        self.assertEqual(a.index(0), 4)
        self.assertEqual(a.index(0, 1), 4)
        self.assertEqual(a.index(1, 4), 5)
        self.assertEqual(a.index(0, 5), 18)
        self.assertRaises(ValueError, a.index, 0, 5, 18)
        self.assertRaises(ValueError, a.index, 0, 19)

    def test_index5(self):
        a = bitarray(2000)
        a.setall(0)
        for _ in range(3):
            a[randint(0, 1999)] = 1
        aa = a.tolist()
        for _ in range(100):
            start = randint(0, 2000)
            stop = randint(0, 2000)
            try:
                res1 = a.index(1, start, stop)
            except ValueError:
                res1 = None
            try:
                res2 = aa.index(1, start, stop)
            except ValueError:
                res2 = None
            self.assertEqual(res1, res2)

    def test_index6(self):
        for n in range(1, 50):
            a = bitarray(n)
            i = randint(0, 1)
            a.setall(i)
            for unused in range(randint(1, 4)):
                a[randint(0, n-1)] = 1-i
            aa = a.tolist()
            for unused in range(100):
                start = randint(-50, n+50)
                stop = randint(-50, n+50)
                try:
                    res1 = a.index(1-i, start, stop)
                except ValueError:
                    res1 = None
                try:
                    res2 = aa.index(1-i, start, stop)
                except ValueError:
                    res2 = None
                self.assertEqual(res1, res2)


    def test_count1(self):
        a = bitarray('10011')
        self.assertEqual(a.count(), 3)
        self.assertEqual(a.count(True), 3)
        self.assertEqual(a.count(False), 2)
        self.assertEqual(a.count(1), 3)
        self.assertEqual(a.count(0), 2)
        self.assertEqual(a.count(None), 2)
        self.assertEqual(a.count(''), 2)
        self.assertEqual(a.count('A'), 3)
        self.assertRaises(TypeError, a.count, 0, 'A')
        self.assertRaises(TypeError, a.count, 0, 0, 'A')

    def test_count2(self):
        for i in range(0, 256):
            a = bitarray()
            a.frombytes(bytes(bytearray([i])))
            self.assertEqual(a.count(), a.to01().count('1'))

    def test_count3(self):
        for a in self.randombitarrays():
            s = a.to01()
            self.assertEqual(a.count(1), s.count('1'))
            self.assertEqual(a.count(0), s.count('0'))

    def test_count4(self):
        N = 37
        a = bitarray(N)
        a.setall(1)
        for i in range(N):
            for j in range(i, N):
                self.assertEqual(a.count(1, i, j), j - i)

    def test_count5(self):
        for endian in 'big', 'little':
            a = bitarray('01001100' '01110011' '01', endian)
            self.assertEqual(a.count(), 9)
            self.assertEqual(a.count(0, 12), 3)
            self.assertEqual(a.count(1, -5), 3)
            self.assertEqual(a.count(1, 2, 17), 7)
            self.assertEqual(a.count(1, 6, 11), 2)
            self.assertEqual(a.count(0, 7, -3), 4)
            self.assertEqual(a.count(1, 1, -1), 8)
            self.assertEqual(a.count(1, 17, 14), 0)

    def test_count6(self):
        for a in self.randombitarrays():
            s = a.to01()
            i = randint(-3, len(a)+1)
            j = randint(-3, len(a)+1)
            self.assertEqual(a.count(1, i, j), s[i:j].count('1'))
            self.assertEqual(a.count(0, i, j), s[i:j].count('0'))


    def test_search(self):
        a = bitarray('')
        self.assertEqual(a.search(bitarray('0')), [])
        self.assertEqual(a.search(bitarray('1')), [])

        a = bitarray('1')
        self.assertEqual(a.search(bitarray('0')), [])
        self.assertEqual(a.search(bitarray('1')), [0])
        self.assertEqual(a.search(bitarray('11')), [])

        a = bitarray(100*'1')
        self.assertEqual(a.search(bitarray('0')), [])
        self.assertEqual(a.search(bitarray('1')), list(range(100)))

        a = bitarray('10010101110011111001011')
        for limit in range(10):
            self.assertEqual(a.search(bitarray('011'), limit),
                             [6, 11, 20][:limit])
        self.assertRaises(ValueError, a.search, bitarray())
        self.assertRaises(TypeError, a.search, '010')

    def test_itersearch(self):
        a = bitarray('10011')
        self.assertRaises(ValueError, a.itersearch, bitarray())
        self.assertRaises(TypeError, a.itersearch, '')
        it = a.itersearch(bitarray('1'))
        self.assertEqual(next(it), 0)
        self.assertEqual(next(it), 3)
        self.assertEqual(next(it), 4)
        self.assertStopIteration(it)

    def test_search2(self):
        a = bitarray('10011')
        for s, res in [('0',     [1, 2]),  ('1', [0, 3, 4]),
                       ('01',    [2]),     ('11', [3]),
                       ('000',   []),      ('1001', [0]),
                       ('011',   [2]),     ('0011', [1]),
                       ('10011', [0]),     ('100111', [])]:
            b = bitarray(s)
            self.assertEqual(a.search(b), res)
            self.assertEqual([p for p in a.itersearch(b)], res)

    def test_search3(self):
        a = bitarray('10010101110011111001011')
        for s, res in [('011', [6, 11, 20]),
                       ('111', [7, 12, 13, 14]), # note the overlap
                       ('1011', [5, 19]),
                       ('100', [0, 9, 16])]:
            b = bitarray(s)
            self.assertEqual(a.search(b), res)
            self.assertEqual(list(a.itersearch(b)), res)
            self.assertEqual([p for p in a.itersearch(b)], res)

    def test_search4(self):
        for a in self.randombitarrays():
            aa = a.to01()
            for sub in '0', '1', '01', '01', '11', '101', '1111111':
                sr = a.search(bitarray(sub), 1)
                try:
                    p = sr[0]
                except IndexError:
                    p = -1
                self.assertEqual(p, aa.find(sub))

    def test_search_type(self):
        a = bitarray('10011')
        it = a.itersearch(bitarray('1'))
        self.assertIsInstance(type(it), type)

    def test_fill(self):
        a = bitarray('')
        self.assertEqual(a.fill(), 0)
        self.assertEqual(len(a), 0)

        a = bitarray('101')
        self.assertEqual(a.fill(), 5)
        self.assertEQUAL(a, bitarray('10100000'))
        self.assertEqual(a.fill(), 0)
        self.assertEQUAL(a, bitarray('10100000'))

        for a in self.randombitarrays():
            aa = a.tolist()
            la = len(a)
            b = a
            self.assertTrue(0 <= b.fill() < 8)
            self.assertEqual(b.endian(), a.endian())
            bb = b.tolist()
            lb = len(b)
            self.assertTrue(a is b)
            self.check_obj(b)
            if la % 8 == 0:
                self.assertEqual(bb, aa)
                self.assertEqual(lb, la)
            else:
                self.assertTrue(lb % 8 == 0)
                self.assertNotEqual(bb, aa)
                self.assertEqual(bb[:la], aa)
                self.assertEqual(b[la:], (lb-la)*bitarray('0'))
                self.assertTrue(0 < lb-la < 8)


    def test_sort(self):
        a = bitarray('1101000')
        a.sort()
        self.assertEqual(a, bitarray('0000111'))

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

        N = 100000
        a = bitarray(randint(N, N + 100))
        for dum in range(100):
            a[randint(0, N - 1)] = randint(0, 1)
        b = a.tolist()
        a.sort()
        self.assertEqual(a, bitarray(sorted(b)))

        for a in self.randombitarrays():
            b = a.tolist()
            ida = id(a)
            rev = randint(0, 1)
            a.sort(rev)
            self.assertEqual(a, bitarray(sorted(b, reverse=rev)))
            self.assertEqual(id(a), ida)


    def test_reverse(self):
        self.assertRaises(TypeError, bitarray().reverse, 42)

        for x, y in [('', ''), ('1', '1'), ('10', '01'), ('001', '100'),
                     ('1110', '0111'), ('11100', '00111'),
                     ('011000', '000110'), ('1101100', '0011011'),
                     ('11110000', '00001111'),
                     ('11111000011', '11000011111'),
                     ('11011111' '00100000' '000111',
                      '111000' '00000100' '11111011')]:
            a = bitarray(x)
            a.reverse()
            self.assertEQUAL(a, bitarray(y))

        for a in self.randombitarrays():
            aa = a.tolist()
            b = bitarray(a)
            ida = id(a)
            a.reverse()
            self.assertEqual(ida, id(a))
            self.assertEQUAL(a, bitarray(aa[::-1], endian=a.endian()))
            self.assertEqual(a, b[::-1])


    def test_tolist(self):
        a = bitarray()
        self.assertEqual(a.tolist(), [])

        a = bitarray('110')
        self.assertEqual(a.tolist(), [True, True, False])

        for lst in self.randomlists():
            a = bitarray(lst)
            self.assertEqual(a.tolist(), lst)


    def test_remove(self):
        a = bitarray()
        for i in (True, False, 1, 0):
            self.assertRaises(ValueError, a.remove, i)

        a = bitarray(21)
        a.setall(0)
        self.assertRaises(ValueError, a.remove, 1)
        a.setall(1)
        self.assertRaises(ValueError, a.remove, 0)

        a = bitarray('1010110')
        for val, res in [(False, '110110'), (True, '10110'),
                         (1, '0110'), (1, '010'), (0, '10'),
                         (0, '1'), (1, '')]:
            a.remove(val)
            self.assertEQUAL(a, bitarray(res))

        a = bitarray('0010011')
        b = a
        b.remove('1')
        self.assertTrue(b is a)
        self.assertEQUAL(b, bitarray('000011'))


    def test_pop(self):
        for x, n, r, y in [('1', 0, True, ''),
                           ('0', -1, False, ''),
                           ('0011100', 3, True, '001100')]:
            a = bitarray(x)
            self.assertEqual(a.pop(n), r)
            self.assertEqual(a, bitarray(y))

        a = bitarray('01')
        self.assertEqual(a.pop(), True)
        self.assertEqual(a.pop(), False)
        self.assertRaises(IndexError, a.pop)

        for a in self.randombitarrays():
            self.assertRaises(IndexError, a.pop, len(a))
            self.assertRaises(IndexError, a.pop, -len(a)-1)
            if len(a) == 0:
                continue
            aa = a.tolist()
            enda = a.endian()
            self.assertEqual(a.pop(), aa[-1])
            self.check_obj(a)
            self.assertEqual(a.endian(), enda)

        for a in self.randombitarrays(start=1):
            n = randint(-len(a), len(a)-1)
            aa = a.tolist()
            self.assertEqual(a.pop(n), aa[n])
            aa.pop(n)
            self.assertEqual(a, bitarray(aa))
            self.check_obj(a)


    def test_setall(self):
        a = bitarray(5)
        a.setall(True)
        self.assertEQUAL(a, bitarray('11111'))

        for a in self.randombitarrays():
            val = randint(0, 1)
            b = a
            b.setall(val)
            self.assertEqual(b, bitarray(len(b) * [val]))
            self.assertTrue(a is b)
            self.check_obj(b)


    def test_bytereverse(self):
        for x, y in [('', ''),
                     ('1', '0'),
                     ('1011', '0000'),
                     ('111011', '001101'),
                     ('11101101', '10110111'),
                     ('000000011', '100000000'),
                     ('11011111' '00100000' '000111',
                      '11111011' '00000100' '001110')]:
            a = bitarray(x)
            a.bytereverse()
            self.assertEqual(a, bitarray(y))

        for i in range(256):
            a = bitarray()
            a.frombytes(bytes(bytearray([i])))
            aa = a.tolist()
            b = a
            b.bytereverse()
            self.assertEqual(b, bitarray(aa[::-1]))
            self.assertTrue(a is b)
            self.check_obj(b)


tests.append(MethodTests)

# ---------------------------------------------------------------------------

class StringTests(unittest.TestCase, Util):

    def randombytes(self):
        for n in range(1, 20):
            yield os.urandom(n)

    def test_frombytes(self):
        a = bitarray(endian='big')
        a.frombytes(b'A')
        self.assertEqual(a, bitarray('01000001'))

        b = a
        b.frombytes(b'BC')
        self.assertEQUAL(b, bitarray('01000001' '01000010' '01000011',
                                     endian='big'))
        self.assertTrue(b is a)

        for b in self.randombitarrays():
            c = b.copy()
            b.frombytes(b'')
            self.assertEQUAL(b, c)

        for b in self.randombitarrays():
            for s in self.randombytes():
                a = bitarray(endian=b.endian())
                a.frombytes(s)
                c = b.copy()
                b.frombytes(s)
                self.assertEQUAL(b[-len(a):], a)
                self.assertEQUAL(b[:-len(a)], c)
                self.assertEQUAL(b, c + a)


    def test_tobytes(self):
        a = bitarray()
        self.assertEqual(a.tobytes(), b'')

        for end in ('big', 'little'):
            a = bitarray(endian=end)
            a.frombytes(b'foo')
            self.assertEqual(a.tobytes(), b'foo')

            for s in self.randombytes():
                a = bitarray(endian=end)
                a.frombytes(s)
                self.assertEqual(a.tobytes(), s)

        for n, s in [(1, b'\x01'), (2, b'\x03'), (3, b'\x07'), (4, b'\x0f'),
                     (5, b'\x1f'), (6, b'\x3f'), (7, b'\x7f'), (8, b'\xff'),
                     (12, b'\xff\x0f'), (15, b'\xff\x7f'), (16, b'\xff\xff'),
                     (17, b'\xff\xff\x01'), (24, b'\xff\xff\xff')]:
            a = bitarray(n, endian='little')
            a.setall(1)
            self.assertEqual(a.tobytes(), s)


    def test_unpack(self):
        a = bitarray('01')
        if is_py3k:
            self.assertIsInstance(a.unpack(), bytes)
        else:
            self.assertIsInstance(a.unpack(), str)
        self.assertEqual(a.unpack(), b'\x00\xff')
        self.assertEqual(a.unpack(b'A'), b'A\xff')
        self.assertEqual(a.unpack(b'0', b'1'), b'01')
        self.assertEqual(a.unpack(one=b'\x01'), b'\x00\x01')
        self.assertEqual(a.unpack(zero=b'A'), b'A\xff')
        self.assertEqual(a.unpack(one=b't', zero=b'f'), b'ft')

        self.assertRaises(TypeError, a.unpack, b'a', zero=b'b')
        self.assertRaises(TypeError, a.unpack, foo=b'b')

        for a in self.randombitarrays():
            self.assertEqual(a.unpack(b'0', b'1'), a.to01().encode())

            b = bitarray()
            b.pack(a.unpack())
            self.assertEqual(b, a)

            b = bitarray()
            b.pack(a.unpack(b'\x01', b'\x00'))
            b.invert()
            self.assertEqual(b, a)


    def test_pack(self):
        a = bitarray()
        a.pack(b'\x00')
        self.assertEqual(a, bitarray('0'))
        a.pack(b'\xff')
        self.assertEqual(a, bitarray('01'))
        a.pack(b'\x01\x00\x7a')
        self.assertEqual(a, bitarray('01101'))

        a = bitarray()
        for n in range(256):
            a.pack(bytes(bytearray([n])))
        self.assertEqual(a, bitarray('0' + 255 * '1'))

        self.assertRaises(TypeError, a.pack, 0)
        if is_py3k:
            self.assertRaises(TypeError, a.pack, '1')
        self.assertRaises(TypeError, a.pack, [1, 3])
        self.assertRaises(TypeError, a.pack, bitarray())


tests.append(StringTests)

# ---------------------------------------------------------------------------

class FileTests(unittest.TestCase, Util):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tmpfname = os.path.join(self.tmpdir, 'testfile')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)


    def test_pickle(self):
        for v in range(3):
            for a in self.randombitarrays():
                with open(self.tmpfname, 'wb') as fo:
                    pickle.dump(a, fo, v)
                b = pickle.load(open(self.tmpfname, 'rb'))
                self.assertFalse(b is a)
                self.assertEQUAL(a, b)

    def test_shelve(self):
        if not shelve or hasattr(sys, 'gettotalrefcount'):
            return

        d = shelve.open(self.tmpfname)
        stored = []
        for a in self.randombitarrays():
            key = hashlib.md5(repr(a).encode() +
                              a.endian().encode()).hexdigest()
            d[key] = a
            stored.append((key, a))
        d.close()
        del d

        d = shelve.open(self.tmpfname)
        for k, v in stored:
            self.assertEQUAL(d[k], v)
        d.close()


    def test_fromfile_wrong_args(self):
        b = bitarray()
        self.assertRaises(TypeError, b.fromfile)
        self.assertRaises(TypeError, b.fromfile, StringIO()) # file not open
        self.assertRaises(TypeError, b.fromfile, 42)
        self.assertRaises(TypeError, b.fromfile, 'bar')


    def test_from_empty_file(self):
        with open(self.tmpfname, 'wb') as fo:
            pass

        a = bitarray()
        a.fromfile(open(self.tmpfname, 'rb'))
        self.assertEqual(a, bitarray())


    def test_from_large_file(self):
        N = 100000

        with open(self.tmpfname, 'wb') as fo:
            fo.write(N * b'X')

        a = bitarray()
        a.fromfile(open(self.tmpfname, 'rb'))
        self.assertEqual(len(a), 8 * N)
        self.assertEqual(a.buffer_info()[1], N)
        # make sure there is no over-allocation
        self.assertEqual(a.buffer_info()[4], N)


    def test_fromfile_Foo(self):
        with open(self.tmpfname, 'wb') as fo:
            fo.write(b'Foo\n')

        a = bitarray(endian='big')
        a.fromfile(open(self.tmpfname, 'rb'))
        self.assertEqual(a, bitarray('01000110011011110110111100001010'))

        a = bitarray(endian='little')
        a.fromfile(open(self.tmpfname, 'rb'))
        self.assertEqual(a, bitarray('01100010111101101111011001010000'))

        a = bitarray('1', endian='little')
        a.fromfile(open(self.tmpfname, 'rb'))
        self.assertEqual(a, bitarray('101100010111101101111011001010000'))

        for n in range(20):
            a = bitarray(n, endian='little')
            a.setall(1)
            a.fromfile(open(self.tmpfname, 'rb'))
            self.assertEqual(a,
                             n*bitarray('1') +
                             bitarray('01100010111101101111011001010000'))


    def test_fromfile_n(self):
        a = bitarray()
        a.fromstring('ABCDEFGHIJ')
        with open(self.tmpfname, 'wb') as fo:
            a.tofile(fo)

        b = bitarray()
        with open(self.tmpfname, 'rb') as f:
            b.fromfile(f, 0);     self.assertEqual(b.tostring(), '')
            b.fromfile(f, 1);     self.assertEqual(b.tostring(), 'A')
            f.read(1)
            b = bitarray()
            b.fromfile(f, 2);     self.assertEqual(b.tostring(), 'CD')
            b.fromfile(f, 1);     self.assertEqual(b.tostring(), 'CDE')
            b.fromfile(f, 0);     self.assertEqual(b.tostring(), 'CDE')
            b.fromfile(f);        self.assertEqual(b.tostring(), 'CDEFGHIJ')
            b.fromfile(f);        self.assertEqual(b.tostring(), 'CDEFGHIJ')

        b = bitarray()
        with open(self.tmpfname, 'rb') as f:
            f.read(1);
            self.assertRaises(EOFError, b.fromfile, f, 10)

        self.assertEqual(b.tostring(), 'BCDEFGHIJ')

        b = bitarray()
        with open(self.tmpfname, 'rb') as f:
            b.fromfile(f);
            self.assertEqual(b.tostring(), 'ABCDEFGHIJ')
            self.assertRaises(EOFError, b.fromfile, f, 1)


    def test_tofile(self):
        a = bitarray()
        with open(self.tmpfname, 'wb') as f:
            a.tofile(f)

        with open(self.tmpfname, 'rb') as fi:
            self.assertEqual(fi.read(), b'')

        a = bitarray('01000110011011110110111100001010', endian='big')
        with open(self.tmpfname, 'wb') as f:
            a.tofile(f)

        with open(self.tmpfname, 'rb') as fi:
            self.assertEqual(fi.read(), b'Foo\n')

        for a in self.randombitarrays():
            b = bitarray(a, endian='big')
            with open(self.tmpfname, 'wb') as fo:
                b.tofile(fo)

            s = open(self.tmpfname, 'rb').read()
            self.assertEqual(len(s), a.buffer_info()[1])

        for n in range(3):
            a.fromstring(n * 'A')
            self.assertRaises(TypeError, a.tofile)
            self.assertRaises(TypeError, a.tofile, StringIO())

            with open(self.tmpfname, 'wb') as f:
                a.tofile(f)
            self.assertRaises(TypeError, a.tofile, f)

        for n in range(20):
            a = n * bitarray('1', endian='little')
            with open(self.tmpfname, 'wb') as fo:
                a.tofile(fo)

            s = open(self.tmpfname, 'rb').read()
            self.assertEqual(len(s), a.buffer_info()[1])

            b = a.copy()
            b.fill()

            c = bitarray(endian='little')
            c.frombytes(s)
            self.assertEqual(c, b)


tests.append(FileTests)

# ---------------------------------------------------------------------------

class PrefixCodeTests(unittest.TestCase, Util):

    def test_encode_string(self):
        a = bitarray()
        d = {'a': bitarray('0')}
        a.encode(d, '')
        self.assertEqual(a, bitarray())
        a.encode(d, 'a')
        self.assertEqual(a, bitarray('0'))
        self.assertEqual(d, {'a': bitarray('0')})

    def test_encode_list(self):
        a = bitarray()
        d = {'a': bitarray('0')}
        a.encode(d, [])
        self.assertEqual(a, bitarray())
        a.encode(d, ['a'])
        self.assertEqual(a, bitarray('0'))
        self.assertEqual(d, {'a': bitarray('0')})

    def test_encode_iter(self):
        a = bitarray()
        d = {'a': bitarray('0'), 'b': bitarray('1')}
        a.encode(d, iter('abba'))
        self.assertEqual(a, bitarray('0110'))

        def foo():
            for c in 'bbaabb':
                yield c

        a.encode(d, foo())
        self.assertEqual(a, bitarray('0110110011'))
        self.assertEqual(d, {'a': bitarray('0'), 'b': bitarray('1')})

    def test_encode(self):
        d = {'I': bitarray('1'),
             'l': bitarray('01'),
             'a': bitarray('001'),
             'n': bitarray('000')}
        a = bitarray()
        a.encode(d, 'Ilan')
        self.assertEqual(a, bitarray('101001000'))
        a.encode(d, 'a')
        self.assertEqual(a, bitarray('101001000001'))
        self.assertEqual(d, {'I': bitarray('1'), 'l': bitarray('01'),
                             'a': bitarray('001'), 'n': bitarray('000')})
        self.assertRaises(ValueError, a.encode, d, 'arvin')

    def test_encode_symbol_not_in_code(self):
        d = {None : bitarray('0'),
             0    : bitarray('10'),
             'A'  : bitarray('11')}
        a = bitarray()
        a.encode(d, ['A', None, 0])
        self.assertEqual(a, bitarray('11010'))
        self.assertRaises(ValueError, a.encode, d, [1, 2])
        self.assertRaises(ValueError, a.encode, d, 'ABCD')

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
        self.assertRaises(TypeError, a.encode, {'a': 'b'}, '')
        self.assertRaises(ValueError, a.encode, {'a': bitarray()}, '')
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
        d = {'I': bitarray('1'),
             'l': bitarray('01'),
             'a': bitarray('001'),
             'n': bitarray('000')}
        dcopy = dict(d)
        a = bitarray('101001000')
        self.assertEqual(a.decode(d), ['I', 'l', 'a', 'n'])
        self.assertEqual(d, dcopy)
        self.assertEqual(a, bitarray('101001000'))

    def test_iterdecode_simple(self):
        d = {'I': bitarray('1'),
             'l': bitarray('01'),
             'a': bitarray('001'),
             'n': bitarray('000')}
        dcopy = dict(d)
        a = bitarray('101001000')
        self.assertEqual(list(a.iterdecode(d)), ['I', 'l', 'a', 'n'])
        self.assertEqual(d, dcopy)
        self.assertEqual(a, bitarray('101001000'))

    def test_decode_empty(self):
        d = {'a': bitarray('1')}
        a = bitarray()
        self.assertEqual(a.decode(d), [])
        self.assertEqual(d, {'a': bitarray('1')})
        # test decode iterator
        self.assertEqual(list(a.iterdecode(d)), [])
        self.assertEqual(d, {'a': bitarray('1')})
        self.assertEqual(len(a), 0)

    def test_decode_no_term(self):
        d = {'a': bitarray('0'), 'b': bitarray('111')}
        a = bitarray('011')
        self.assertRaises(ValueError, a.decode, d)
        self.assertEqual(a, bitarray('011'))
        self.assertEqual(d, {'a': bitarray('0'), 'b': bitarray('111')})

    def test_decode_buggybitarray(self):
        d = {'a': bitarray('0')}
        a = bitarray('1')
        self.assertRaises(ValueError, a.decode, d)
        self.assertEqual(a, bitarray('1'))
        self.assertEqual(d, {'a': bitarray('0')})

    def test_iterdecode_no_term(self):
        d = {'a': bitarray('0'), 'b': bitarray('111')}
        a = bitarray('011')
        it = a.iterdecode(d)
        if not is_py3k:
            self.assertEqual(it.next(), 'a')
            self.assertRaises(ValueError, it.next)
        self.assertEqual(a, bitarray('011'))
        self.assertEqual(d, {'a': bitarray('0'), 'b': bitarray('111')})

    def test_iterdecode_buggybitarray(self):
        d = {'a': bitarray('0')}
        a = bitarray('1')
        it = a.iterdecode(d)
        if not is_py3k:
            self.assertRaises(ValueError, it.next)
        self.assertEqual(a, bitarray('1'))
        self.assertEqual(d, {'a': bitarray('0')})

    def test_decode_buggybitarray2(self):
        d = {'a': bitarray('00'), 'b': bitarray('01')}
        a = bitarray('1')
        self.assertRaises(ValueError, a.decode, d)
        self.assertEqual(a, bitarray('1'))

    def test_iterdecode_buggybitarray2(self):
        d = {'a': bitarray('00'), 'b': bitarray('01')}
        a = bitarray('1')
        it = a.iterdecode(d)
        if not is_py3k:
            self.assertRaises(ValueError, it.next)
        self.assertEqual(a, bitarray('1'))

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

    def test_real_example(self):
        code = {' ': bitarray('001'),
                '.': bitarray('0101010'),
                'a': bitarray('0110'),
                'b': bitarray('0001100'),
                'c': bitarray('000011'),
                'd': bitarray('01011'),
                'e': bitarray('111'),
                'f': bitarray('010100'),
                'g': bitarray('101000'),
                'h': bitarray('00000'),
                'i': bitarray('1011'),
                'j': bitarray('0111101111'),
                'k': bitarray('00011010'),
                'l': bitarray('01110'),
                'm': bitarray('000111'),
                'n': bitarray('1001'),
                'o': bitarray('1000'),
                'p': bitarray('101001'),
                'q': bitarray('00001001101'),
                'r': bitarray('1101'),
                's': bitarray('1100'),
                't': bitarray('0100'),
                'u': bitarray('000100'),
                'v': bitarray('0111100'),
                'w': bitarray('011111'),
                'x': bitarray('0000100011'),
                'y': bitarray('101010'),
                'z': bitarray('00011011110')}
        a = bitarray()
        message = 'the quick brown fox jumps over the lazy dog.'
        a.encode(code, message)
        self.assertEqual(a, bitarray('01000000011100100001001101000100101100'
          '00110001101000100011001101100001111110010010101001000000010001100'
          '10111101111000100000111101001110000110000111100111110100101000000'
          '0111001011100110000110111101010100010101110001010000101010'))
        self.assertEqual(''.join(a.decode(code)), message)
        self.assertEqual(''.join(a.iterdecode(code)), message)


tests.append(PrefixCodeTests)

# -------------- Buffer Interface (Python 2.7 and above) --------------------

class BufferInterfaceTests(unittest.TestCase):

    def test_read1(self):
        a = bitarray('01000001' '01000010' '01000011', endian='big')
        v = memoryview(a)
        self.assertEqual(len(v), 3)
        self.assertEqual(v[0], 65 if is_py3k else 'A')
        self.assertEqual(v.tobytes(), b'ABC')
        a[13] = 1
        self.assertEqual(v.tobytes(), b'AFC')

    def test_read2(self):
        a = bitarray()
        a.frombytes(os.urandom(100))
        v = memoryview(a)
        self.assertEqual(len(v), 100)
        b = a[34 * 8 : 67 * 8]
        self.assertEqual(v[34:67].tobytes(), b.tobytes())
        self.assertEqual(v.tobytes(), a.tobytes())

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
        self.assertEqual(a[300 *8 : 305 * 8].tobytes(), b'\x00ABC\x00')

if sys.version_info[:2] >= (2, 7):
    tests.append(BufferInterfaceTests)

# ---------------------------------------------------------------------------

class TestsFrozenbitarray(unittest.TestCase, Util):

    def test_init(self):
        a = frozenbitarray('110')
        self.assertEqual(a, bitarray('110'))
        self.assertEqual(a.to01(), '110')
        for endian in 'big', 'little':
            a = frozenbitarray(0, endian)
            self.assertEqual(a.endian(), endian)

    def test_methods(self):
        # test a few methods which do not raise the TypeError
        a = frozenbitarray('1101100')
        self.assertEqual(a[2], 0)
        self.assertEqual(a[:4].to01(), '1101')
        self.assertEqual(a.count(), 4)
        self.assertEqual(a.index(0), 2)
        b = a.copy()
        self.assertEqual(b, a)
        self.assertEqual(repr(type(b)), "<class 'bitarray.frozenbitarray'>")
        self.assertEqual(len(b), 7)
        self.assertEqual(b.length(), 7)
        self.assertEqual(b.all(), False)
        self.assertEqual(b.any(), True)

    def test_init_bitarray(self):
        for a in self.randombitarrays():
            b = frozenbitarray(a)
            self.assertFalse(b is a)
            self.assertEqual(b, a)
            self.assertEqual(b.endian(), a.endian())
            c = frozenbitarray(b)
            self.assertEqual(c, b)
            self.assertFalse(c is b)
            self.assertEqual(c.endian(), a.endian())
            self.assertEqual(hash(c), hash(b))

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
        self.assertRaises(TypeError, a.__setitem__, 0, 0)
        self.assertRaises(TypeError, a.__delitem__, 0)

    def test_dictkey(self):
        a = frozenbitarray('01')
        b = frozenbitarray('1001')
        d = {a: 123, b: 345}
        self.assertEqual(d[frozenbitarray('01')], 123)
        self.assertEqual(d[frozenbitarray(b)], 345)

    def test_dictkey2(self): # taken sllightly modified from issue #74
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

tests.append(TestsFrozenbitarray)

# ---------------------------------------------------------------------------

def run(verbosity=1, repeat=1):
    import bitarray.test_util as btu
    tests.extend(btu.tests)

    print('bitarray is installed in: %s' % os.path.dirname(__file__))
    print('bitarray version: %s' % __version__)
    print('Python version: %s' % sys.version)

    suite = unittest.TestSuite()
    for cls in tests:
        for _ in range(repeat):
            suite.addTest(unittest.makeSuite(cls))

    runner = unittest.TextTestRunner(verbosity=verbosity)
    return runner.run(suite)


if __name__ == '__main__':
    run()
