# Copyright (c) 2019 - 2021, Ilan Schnell; All Rights Reserved
# bitarray is published under the PSF license.
#
# Author: Ilan Schnell
"""
Useful utilities for working with bitarrays.
"""
from __future__ import absolute_import

import os
import sys

from bitarray import bitarray, bits2bytes, get_default_endian

from bitarray._util import (
    count_n, rindex, parity, count_and, count_or, count_xor, subset,
    serialize, ba2hex, _hex2ba, ba2base, _base2ba, vl_encode, _vl_decode,
    _set_bato,
)

__all__ = [
    'zeros', 'urandom', 'pprint', 'make_endian', 'rindex', 'strip', 'count_n',
    'parity', 'count_and', 'count_or', 'count_xor', 'subset',
    'ba2hex', 'hex2ba', 'ba2base', 'base2ba', 'ba2int', 'int2ba',
    'serialize', 'deserialize', 'vl_encode', 'vl_decode', 'huffman_code',
]


# tell the _util extension what the bitarray type object is, such that it
# can check for instances thereof
_set_bato(bitarray)

_is_py2 = bool(sys.version_info[0] == 2)


def zeros(__length, endian=None):
    """zeros(length, /, endian=None) -> bitarray

Create a bitarray of length, with all values 0, and optional
endianness, which may be 'big', 'little'.
"""
    if not isinstance(__length, (int, long) if _is_py2 else int):
        raise TypeError("int expected, got '%s'" % type(__length).__name__)

    a = bitarray(__length, get_default_endian() if endian is None else endian)
    a.setall(0)
    return a


def urandom(__length, endian=None):
    """urandom(length, /, endian=None) -> bitarray

Return a bitarray of `length` random bits (uses `os.urandom`).
"""
    a = bitarray(0, get_default_endian() if endian is None else endian)
    a.frombytes(os.urandom(bits2bytes(__length)))
    del a[__length:]
    return a


def pprint(__a, stream=None, group=8, indent=4, width=80):
    """pprint(bitarray, /, stream=None, group=8, indent=4, width=80)

Prints the formatted representation of object on `stream`, followed by a
newline.  If `stream` is `None`, `sys.stdout` is used.  By default, elements
are grouped in bytes (8 elements), and 8 bytes (64 elements) per line.
Non-bitarray objects are printed by the standard library
function `pprint.pprint()`.
"""
    if stream is None:
        stream = sys.stdout

    if not isinstance(__a, bitarray):
        import pprint as _pprint
        _pprint.pprint(__a, stream=stream, indent=indent, width=width)
        return

    group = int(group)
    if group < 1:
        raise ValueError('group must be >= 1')
    indent = int(indent)
    if indent < 0:
        raise ValueError('indent must be >= 0')
    width = int(width)
    if width <= indent:
        raise ValueError('width must be > %d (indent)' % indent)

    gpl = (width - indent) // (group + 1)  # groups per line
    epl = group * gpl                      # elements per line
    if epl == 0:
        epl = width - indent - 2
    type_name = type(__a).__name__
    # here 4 is len("'()'")
    multiline = len(type_name) + 4 + len(__a) + len(__a) // group >= width
    if multiline:
        quotes = "'''"
    elif __a:
        quotes = "'"
    else:
        quotes = ""

    stream.write("%s(%s" % (type_name, quotes))
    for i, b in enumerate(__a):
        if multiline and i % epl == 0:
            stream.write('\n%s' % (indent * ' '))
        if i % group == 0 and i % epl != 0:
            stream.write(' ')
        stream.write(str(b))

    if multiline:
        stream.write('\n')

    stream.write("%s)\n" % quotes)
    stream.flush()


def make_endian(a, endian):
    """make_endian(bitarray, endian, /) -> bitarray

When the endianness of the given bitarray is different from `endian`,
return a new bitarray, with endianness `endian` and the same elements
as the original bitarray.
Otherwise (endianness is already `endian`) the original bitarray is returned
unchanged.
"""
    if not isinstance(a, bitarray):
        raise TypeError("bitarray expected, got '%s'" % type(a).__name__)

    if a.endian() == endian:
        return a

    return bitarray(a, endian)


def strip(__a, mode='right'):
    """strip(bitarray, /, mode='right') -> bitarray

Return a new bitarray with zeros stripped from left, right or both ends.
Allowed values for mode are the strings: `left`, `right`, `both`
"""
    if not isinstance(__a, bitarray):
        raise TypeError("bitarray expected, got '%s'" % type(__a).__name__)
    if not isinstance(mode, str):
        raise TypeError("str expected for mode, got '%s'" % type(__a).__name__)
    if mode not in ('left', 'right', 'both'):
        raise ValueError("mode must be 'left', 'right' or 'both', got: %r" %
                         mode)
    first = 0
    if mode in ('left', 'both'):
        try:
            first = __a.index(1)
        except ValueError:
            return __a[:0]

    last = len(__a) - 1
    if mode in ('right', 'both'):
        try:
            last = rindex(__a)
        except ValueError:
            return __a[:0]

    return __a[first:last + 1]


def hex2ba(__s, endian=None):
    """hex2ba(hexstr, /, endian=None) -> bitarray

Bitarray of hexadecimal representation.  hexstr may contain any number
(including odd numbers) of hex digits (upper or lower case).
"""
    if not isinstance(__s, (str, unicode if _is_py2 else bytes)):
        raise TypeError("str expected, got: '%s'" % type(__s).__name__)

    if isinstance(__s, unicode if _is_py2 else str):
        __s = __s.encode('ascii')
    assert isinstance(__s, bytes)

    a = bitarray(4 * len(__s),
                 get_default_endian() if endian is None else endian)
    _hex2ba(a, __s)
    return a


def base2ba(__n, __s, endian=None):
    """base2ba(n, asciistr, /, endian=None) -> bitarray

Bitarray of the base `n` ASCII representation.
Allowed values for `n` are 2, 4, 8, 16, 32 and 64.
For `n=16` (hexadecimal), `hex2ba()` will be much faster, as `base2ba()`
does not take advantage of byte level operations.
For `n=32` the RFC 4648 Base32 alphabet is used, and for `n=64` the
standard base 64 alphabet is used.
"""
    if not isinstance(__n, int):
        raise TypeError("integer expected")
    try:
        m = {2: 1, 4: 2, 8: 3, 16: 4, 32: 5, 64: 6}[__n]
    except KeyError:
        raise ValueError("base must be 2, 4, 8, 16, 32 or 64")

    if not isinstance(__s, (str, unicode if _is_py2 else bytes)):
        raise TypeError("str expected, got: '%s'" % type(s).__name__)

    if isinstance(__s, unicode if _is_py2 else str):
        __s = __s.encode('ascii')
    assert isinstance(__s, bytes)

    a = bitarray(m * len(__s),
                 get_default_endian() if endian is None else endian)
    _base2ba(__n, a, __s)
    return a


def ba2int(__a, signed=False):
    """ba2int(bitarray, /, signed=False) -> int

Convert the given bitarray into an integer.
The bit-endianness of the bitarray is respected.
`signed` indicates whether two's complement is used to represent the integer.
"""
    if not isinstance(__a, bitarray):
        raise TypeError("bitarray expected, got '%s'" % type(__a).__name__)
    length = len(__a)
    if length == 0:
        raise ValueError("non-empty bitarray expected")

    big_endian = bool(__a.endian() == 'big')
    # for big endian pad leading zeros - for little endian we don't need to
    # pad trailing zeros, as .tobytes() will treat them as zero
    if big_endian and length % 8:
        __a = zeros(8 - length % 8, 'big') + __a
    b = __a.tobytes()

    if _is_py2:
        c = bytearray(b)
        res = 0
        j = len(c) - 1 if big_endian else 0
        for x in c:
            res |= x << 8 * j
            j += -1 if big_endian else 1
    else: # py3
        res = int.from_bytes(b, byteorder=__a.endian())

    if signed and res >= 1 << (length - 1):
        res -= 1 << length
    return res


def int2ba(__i, length=None, endian=None, signed=False):
    """int2ba(int, /, length=None, endian=None, signed=False) -> bitarray

Convert the given integer to a bitarray (with given endianness,
and no leading (big-endian) / trailing (little-endian) zeros), unless
the `length` of the bitarray is provided.  An `OverflowError` is raised
if the integer is not representable with the given number of bits.
`signed` determines whether two's complement is used to represent the integer,
and requires `length` to be provided.
"""
    if not isinstance(__i, (int, long) if _is_py2 else int):
        raise TypeError("int expected, got '%s'" % type(__i).__name__)
    if length is not None:
        if not isinstance(length, int):
            raise TypeError("int expected for length")
        if length <= 0:
            raise ValueError("length must be > 0")
    if signed and length is None:
        raise TypeError("signed requires length")

    if __i == 0:
        # there are special cases for 0 which we'd rather not deal with below
        return zeros(length or 1, endian)

    if signed:
        m = 1 << (length - 1)
        if not (-m <= __i < m):
            raise OverflowError("signed integer not in range(%d, %d), "
                                "got %d" % (-m, m, __i))
        if __i < 0:
            __i += 1 << length
    else:  # unsigned
        if __i < 0:
            raise OverflowError("unsigned integer not positive, got %d" % __i)
        if length and __i >= (1 << length):
            raise OverflowError("unsigned integer not in range(0, %d), "
                                "got %d" % (1 << length, __i))

    a = bitarray(0, get_default_endian() if endian is None else endian)
    big_endian = bool(a.endian() == 'big')
    if _is_py2:
        c = bytearray()
        while __i:
            __i, r = divmod(__i, 256)
            c.append(r)
        if big_endian:
            c.reverse()
        b = bytes(c)
    else: # py3
        b = __i.to_bytes(bits2bytes(__i.bit_length()), byteorder=a.endian())

    a.frombytes(b)
    if length is None:
        return strip(a, 'left' if big_endian else 'right')

    la = len(a)
    if la > length:
        a = a[-length:] if big_endian else a[:length]
    if la < length:
        pad = zeros(length - la, endian)
        a = pad + a if big_endian else a + pad
    assert len(a) == length
    return a


def deserialize(__b):
    """deserialize(bytes, /) -> bitarray

Return a bitarray given the bytes representation returned by `serialize()`.
"""
    if not isinstance(__b, bytes):
        raise TypeError("bytes expected, got: '%s'" % type(__b).__name__)
    if len(__b) == 0:
        raise ValueError("non-empty bytes expected")

    head = ord(__b[0]) if _is_py2 else __b[0]
    assert isinstance(head, int)
    if head >= 32 or head % 16 >= 8:
        raise ValueError('invalid header byte 0x%02x' % head)
    return bitarray(__b)


def vl_decode(__stream, endian=None):
    """vl_decode(stream, /, endian=None) -> bitarray

Decode binary stream (an integer iterator, or bytes object), and return
the decoded bitarray.  This function consumes only one bitarray and leaves
the remaining stream untouched.  `StopIteration` is raised when no
terminating byte is found.
Use `vl_encode()` for encoding.
"""
    if isinstance(__stream, bytes):
        __stream = iter(__stream)

    a = bitarray(256, get_default_endian() if endian is None else endian)
    _vl_decode(__stream, a)
    return bitarray(a)  # drop previously over-allocated bitarray


def huffman_code(__freq_map, endian=None):
    """huffman_code(dict, /, endian=None) -> dict

Given a frequency map, a dictionary mapping symbols to their frequency,
calculate the Huffman code, i.e. a dict mapping those symbols to
bitarrays (with given endianness).  Note that the symbols are not limited
to being strings.  Symbols may may be any hashable object (such as `None`).
"""
    import heapq

    if not isinstance(__freq_map, dict):
        raise TypeError("dict expected, got '%s'" % type(__freq_map).__name__)
    if len(__freq_map) == 0:
        raise ValueError("non-empty dict expected")
    if endian is None:
        endian = get_default_endian()

    class Node(object):
        # a Node object will have either .symbol or .child set below,
        # .freq will always be set
        def __lt__(self, other):
            # heapq needs to be able to compare the nodes
            return self.freq < other.freq

    def huff_tree(freq_map):
        # given a dictionary mapping symbols to thier frequency,
        # construct a Huffman tree and return its root node

        minheap = []
        # create all the leaf nodes and push them onto the queue
        for sym, f in freq_map.items():
            nd = Node()
            nd.symbol = sym
            nd.freq = f
            heapq.heappush(minheap, nd)

        # repeat the process until only one node remains
        while len(minheap) > 1:
            # take the nodes with smallest frequencies from the queue
            child_0 = heapq.heappop(minheap)
            child_1 = heapq.heappop(minheap)
            # construct the new internal node and push it onto the queue
            parent = Node()
            parent.child = [child_0, child_1]
            parent.freq = child_0.freq + child_1.freq
            heapq.heappush(minheap, parent)

        # the single remaining node is the root of the Huffman tree
        return minheap[0]

    result = {}

    def traverse(nd, prefix=bitarray(0, endian)):
        try:                    # leaf
            result[nd.symbol] = prefix
        except AttributeError:  # parent, so traverse each of the children
            traverse(nd.child[0], prefix + bitarray('0'))
            traverse(nd.child[1], prefix + bitarray('1'))

    traverse(huff_tree(__freq_map))
    return result
