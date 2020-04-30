"""
This package defines an object type which can efficiently represent
a bitarray.  Bitarrays are sequence types and behave very much like lists.

Please find a description of this package at:

    http://pypi.python.org/pypi/bitarray/

Author: Ilan Schnell
"""
from bitarray._bitarray import _bitarray, bitdiff, bits2bytes, _sysinfo

__version__ = '1.2.1'


class bitarray(_bitarray):
    """bitarray(initial=0, /, endian='big')

Return a new bitarray object whose items are bits initialized from
the optional initial object, and endianness.
If no initial object is provided, an empty bitarray (length zero) is created.
The initial object may be of the following types:

`int`: Create a bitarray of given integer length.  The initial values are
arbitrary.  If you want all values to be set, use the .setall() method.

`str`: Create bitarray from a string of `0` and `1`.

`list`, `tuple`, `iterable`: Create bitarray from a sequence, each
element in the sequence is converted to a bit using its truth value.

`bitarray`: Create bitarray from another bitarray.  This is done by
copying the memory holding the bitarray data, and is hence very fast.

The optional keyword arguments `endian` specifies the bit endianness of the
created bitarray object.
Allowed values are the strings `big` and `little` (default is `big`).

Note that setting the bit endianness only has an effect when accessing the
machine representation of the bitarray, i.e. when using the methods: tofile,
fromfile, tobytes, frombytes."""

    def fromstring(self, string):
        """fromstring(str)

Append from a string, interpreting the string as machine values.
Deprecated since version 0.4.0, use `.frombytes()` instead."""
        return self.frombytes(string.encode())

    def tostring(self):
        """tostring() -> str

Return the string representing (machine values) of the bitarray.
When the length of the bitarray is not a multiple of 8, the few remaining
bits (1..7) are set to 0.
Deprecated since version 0.4.0, use `.tobytes()` instead."""
        return self.tobytes().decode()


class frozenbitarray(_bitarray):
    """frozenbitarray(initial=0, /, endian='big')

Return a frozenbitarray object, which is initialized the same way a bitarray
object is initialized.  A frozenbitarray is immutable and hashable.
Its contents cannot be altered after is created; however, it can be used as
a dictionary key.
"""
    def __repr__(self):
        return 'frozen' + _bitarray.__repr__(self)

    def __hash__(self):
        if getattr(self, '_hash', None) is None:
            self._hash = hash((self.length(), self.tobytes()))
        return self._hash

    def __delitem__(self, *args, **kwargs):
        raise TypeError("'frozenbitarray' is immutable")

    append = bytereverse = extend = encode = fill = __delitem__
    frombytes = fromfile = insert = invert = pack = pop = __delitem__
    remove = reverse = setall = sort = __setitem__ = __delitem__
    __iand__ = __iadd__ = __imul__ = __ior__ = __ixor__ = __delitem__


def test(verbosity=1, repeat=1):
    """test(verbosity=1, repeat=1) -> TextTestResult

Run self-test, and return unittest.runner.TextTestResult object.
"""
    from bitarray import test_bitarray
    return test_bitarray.run(verbosity=verbosity, repeat=repeat)
