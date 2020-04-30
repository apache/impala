# Copyright (c) 2019, Ilan Schnell
# bitarray is published under the PSF license.
#
# Author: Ilan Schnell
"""
Useful utilities for working with bitarrays.
"""
import sys
import heapq
import binascii

from bitarray import bitarray, frozenbitarray, bits2bytes, _bitarray

from bitarray._util import (count_n, rindex,
                            count_and, count_or, count_xor, subset,
                            _set_babt)


__all__ = ['zeros', 'rindex', 'strip', 'count_n',
           'count_and', 'count_or', 'count_xor', 'subset',
           'ba2hex', 'hex2ba', 'ba2int', 'int2ba', 'huffman_code']


# tell the _util extension what the bitarray base type is, such that it can
# check for instances thereof when checking for bitarray type
_set_babt(_bitarray)

_is_py2 = bool(sys.version_info[0] == 2)


def zeros(length, endian='big'):
    """zeros(length, /, endian='big') -> bitarray

Create a bitarray of length, with all values 0.
"""
    if not isinstance(length, (int, long) if _is_py2 else int):
        raise TypeError("integer expected")

    a = bitarray(length, endian)
    a.setall(0)
    return a


def strip(a, mode='right'):
    """strip(bitarray, mode='right', /) -> bitarray

Strip zeros from left, right or both ends.
Allowed values for mode are the strings: `left`, `right`, `both`
"""
    if not isinstance(a, (bitarray, frozenbitarray)):
        raise TypeError("bitarray expected")
    if not isinstance(mode, str):
        raise TypeError("string expected for mode")
    if mode not in ('left', 'right', 'both'):
        raise ValueError("allowed values 'left', 'right', 'both', got: %r" %
                         mode)
    first = 0
    if mode in ('left', 'both'):
        try:
            first = a.index(1)
        except ValueError:
            return bitarray(endian=a.endian())

    last = a.length() - 1
    if mode in ('right', 'both'):
        try:
            last = rindex(a)
        except ValueError:
            return bitarray(endian=a.endian())

    return a[first:last + 1]


def ba2hex(a):
    """ba2hex(bitarray, /) -> hexstr

Return a bytes object containing with hexadecimal representation of
the bitarray (which has to be multiple of 4 in length).
"""
    if not isinstance(a, (bitarray, frozenbitarray)):
        raise TypeError("bitarray expected")
    if a.endian() != 'big':
        raise ValueError("big-endian bitarray expected")
    la = a.length()
    if la % 4:
        raise ValueError("bitarray length not multiple of 4")
    if la % 8:
        # make sure we don't mutate the original argument
        a = a + bitarray(4, 'big')
    assert a.length() % 8 == 0

    s = binascii.hexlify(a.tobytes())
    if la % 8:
        s = s[:-1]
    return s


def hex2ba(s):
    """hex2ba(hexstr, /) -> bitarray

Bitarray of hexadecimal representation.
hexstr may contain any number of hex digits (upper or lower case).
"""
    if not isinstance(s, (str, bytes)):
        raise TypeError("string expected")

    ls = len(s)
    if ls % 2:
        s = s + ('0' if isinstance(s, str) else b'0')
    assert len(s) % 2 == 0

    a = bitarray(endian='big')
    a.frombytes(binascii.unhexlify(s))
    if ls % 2:
        del a[-4:]
    return a


def ba2int(a):
    """ba2int(bitarray, /) -> int

Convert the given bitarray into an integer.
The bit-endianness of the bitarray is respected.
"""
    if not isinstance(a, (bitarray, frozenbitarray)):
        raise TypeError("bitarray expected")
    if not a:
        raise ValueError("non-empty bitarray expected")

    endian = a.endian()
    big_endian = bool(endian == 'big')
    if a.length() % 8:
        # pad with leading zeros, such that length is multiple of 8
        if big_endian:
            a = zeros(8 - a.length() % 8, 'big') + a
        else:
            a = a + zeros(8 - a.length() % 8, 'little')
    assert a.length() % 8 == 0
    b = a.tobytes()

    if _is_py2:
        c = bytearray(b)
        res = 0
        j = len(c) - 1 if big_endian else 0
        for x in c:
            res |= x << 8 * j
            j += -1 if big_endian else 1
        return res
    else: # py3
        return int.from_bytes(b, byteorder=endian)


def int2ba(i, length=None, endian='big'):
    """int2ba(int, /, length=None, endian='big') -> bitarray

Convert the given integer into a bitarray (with given endianness,
and no leading (big-endian) / trailing (little-endian) zeros).
If length is provided, the result will be of this length, and an
`OverflowError` will be raised, if the integer cannot be represented
within length bits.
"""
    if not isinstance(i, (int, long) if _is_py2 else int):
        raise TypeError("integer expected")
    if i < 0:
        raise ValueError("non-negative integer expected")
    if length is not None:
        if not isinstance(length, int):
            raise TypeError("integer expected for length")
        if length <= 0:
            raise ValueError("integer larger than 0 expected for length")
    if not isinstance(endian, str):
        raise TypeError("string expected for endian")
    if endian not in ('big', 'little'):
        raise ValueError("endian can only be 'big' or 'little'")

    if i == 0:
        # there a special cases for 0 which we'd rather not deal with below
        return zeros(length or 1, endian=endian)

    big_endian = bool(endian == 'big')
    if _is_py2:
        c = bytearray()
        while i:
            i, r = divmod(i, 256)
            c.append(r)
        if big_endian:
            c.reverse()
        b = bytes(c)
    else: # py3
        b = i.to_bytes(bits2bytes(i.bit_length()), byteorder=endian)

    a = bitarray(endian=endian)
    a.frombytes(b)
    la = a.length()
    if la == length:
        return a

    if length is None:
        return strip(a, 'left' if big_endian else 'right')

    if la > length:
        size = (la - a.index(1)) if big_endian else (rindex(a) + 1)
        if size > length:
            raise OverflowError("cannot represent %d bit integer in "
                                "%d bits" % (size, length))
        a = a[la - length:] if big_endian else a[:length - la]

    if la < length:
        if big_endian:
            a = zeros(length - la, 'big') + a
        else:
            a += zeros(length - la, 'little')

    assert a.length() == length
    return a


def huffman_code(freq_map, endian='big'):
    """huffman_code(dict, /, endian='big') -> dict

Given a frequency map, a dictionary mapping symbols to thier frequency,
calculate the Huffman code, i.e. a dict mapping those symbols to
bitarrays (with given endianness).  Note that the symbols may be any
hashable object (including `None`).
"""
    if not isinstance(freq_map, dict):
        raise TypeError("dict expected")
    if len(freq_map) == 0:
        raise ValueError("non-empty dict expected")

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

    def traverse(nd, prefix=bitarray(endian=endian)):
        if hasattr(nd, 'symbol'):  # leaf
            result[nd.symbol] = prefix
        else:  # parent, so traverse each of the children
            traverse(nd.child[0], prefix + bitarray([0]))
            traverse(nd.child[1], prefix + bitarray([1]))

    traverse(huff_tree(freq_map))
    return result
