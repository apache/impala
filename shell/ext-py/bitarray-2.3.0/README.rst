bitarray: efficient arrays of booleans
======================================

This library provides an object type which efficiently represents an array
of booleans.  Bitarrays are sequence types and behave very much like usual
lists.  Eight bits are represented by one byte in a contiguous block of
memory.  The user can select between two representations: little-endian
and big-endian.  All of the functionality is implemented in C.
Methods for accessing the machine representation are provided.
This can be useful when bit level access to binary files is required,
such as portable bitmap image files (.pbm).  Also, when dealing with
compressed data which uses variable bit length encoding, you may find
this module useful.


Key features
------------

* The bit endianness can be specified for each bitarray object, see below.
* Sequence methods: slicing (including slice assignment and deletion),
  operations ``+``, ``*``, ``+=``, ``*=``, the ``in`` operator, ``len()``
* Fast methods for encoding and decoding variable bit length prefix codes
* Bitwise operations: ``~``, ``&``, ``|``, ``^``, ``<<``, ``>>`` (as well as
  their in-place versions ``&=``, ``|=``, ``^=``, ``<<=``, ``>>=``).
* Sequential search
* Packing and unpacking to other binary data formats, e.g. ``numpy.ndarray``.
* Pickling and unpickling of bitarray objects.
* Bitarray objects support the buffer protocol (both importing and
  exporting buffers)
* ``frozenbitarray`` objects which are hashable
* Extensive test suite with round 400 unittests
* Utility module ``bitarray.util``:

  * conversion to hexadecimal string
  * serialization
  * pretty printing
  * conversion to integers
  * creating Huffman codes
  * various count functions
  * other helpful functions


Installation
------------

If you have a working C compiler, you can simply:

.. code-block:: shell-session

    $ pip install bitarray

If you rather want to use precompiled binaries, you can:

* ``conda install bitarray`` (both the default Anaconda repository as well
  as conda-forge support bitarray)
* download Windows wheels from
  `Chris Gohlke <https://www.lfd.uci.edu/~gohlke/pythonlibs/#bitarray>`__

Once you have installed the package, you may want to test it:

.. code-block:: shell-session

    $ python -c 'import bitarray; bitarray.test()'
    bitarray is installed in: /Users/ilan/bitarray/bitarray
    bitarray version: 2.3.0
    sys.version: 2.7.15 (default, Mar  5 2020, 14:58:04) [GCC Clang 9.0.1]
    sys.prefix: /Users/ilan/Mini3/envs/py27
    pointer size: 64 bit
    sizeof(size_t): 8
    PY_UINT64_T defined: 1
    DEBUG: 0
    .........................................................................
    .........................................................................
    .............................................................
    ----------------------------------------------------------------------
    Ran 398 tests in 0.476s

    OK

You can always import the function test,
and ``test().wasSuccessful()`` will return ``True`` when the test went well.


Using the module
----------------

As mentioned above, bitarray objects behave very much like lists, so
there is not too much to learn.  The biggest difference from list
objects (except that bitarray are obviously homogeneous) is the ability
to access the machine representation of the object.
When doing so, the bit endianness is of importance; this issue is
explained in detail in the section below.  Here, we demonstrate the
basic usage of bitarray objects:

.. code-block:: python

    >>> from bitarray import bitarray
    >>> a = bitarray()         # create empty bitarray
    >>> a.append(1)
    >>> a.extend([1, 0])
    >>> a
    bitarray('110')
    >>> x = bitarray(2 ** 20)  # bitarray of length 1048576 (uninitialized)
    >>> len(x)
    1048576
    >>> bitarray('1001 011')   # initialize from string (whitespace is ignored)
    bitarray('1001011')
    >>> lst = [1, 0, False, True, True]
    >>> a = bitarray(lst)      # initialize from iterable
    >>> a
    bitarray('10011')
    >>> a.count(1)
    3
    >>> a.remove(0)            # removes first occurrence of 0
    >>> a
    bitarray('1011')

Like lists, bitarray objects support slice assignment and deletion:

.. code-block:: python

    >>> a = bitarray(50)
    >>> a.setall(0)            # set all elements in a to 0
    >>> a[11:37:3] = 9 * bitarray('1')
    >>> a
    bitarray('00000000000100100100100100100100100100000000000000')
    >>> del a[12::3]
    >>> a
    bitarray('0000000000010101010101010101000000000')
    >>> a[-6:] = bitarray('10011')
    >>> a
    bitarray('000000000001010101010101010100010011')
    >>> a += bitarray('000111')
    >>> a[9:]
    bitarray('001010101010101010100010011000111')

In addition, slices can be assigned to booleans, which is easier (and
faster) than assigning to a bitarray in which all values are the same:

.. code-block:: python

    >>> a = 20 * bitarray('0')
    >>> a[1:15:3] = True
    >>> a
    bitarray('01001001001001000000')

This is easier and faster than:

.. code-block:: python

    >>> a = 20 * bitarray('0')
    >>> a[1:15:3] = 5 * bitarray('1')
    >>> a
    bitarray('01001001001001000000')

Note that in the latter we have to create a temporary bitarray whose length
must be known or calculated.  Another example of assigning slices to Booleans,
is setting ranges:

.. code-block:: python

    >>> a = bitarray(30)
    >>> a[:] = 0         # set all elements to 0 - equivalent to a.setall(0)
    >>> a[10:25] = 1     # set elements in range(10, 25) to 1
    >>> a
    bitarray('000000000011111111111111100000')


Bitwise operators
-----------------

Bitarray objects support the bitwise operators ``~``, ``&``, ``|``, ``^``,
``<<``, ``>>`` (as well as their in-place versions ``&=``, ``|=``, ``^=``,
``<<=``, ``>>=``).  The behavior is very much what one would expect:

.. code-block:: python

    >>> a = bitarray('101110001')
    >>> ~a  # invert
    bitarray('010001110')
    >>> b = bitarray('111001011')
    >>> a ^ b
    bitarray('010111010')
    >>> a &= b
    >>> a
    bitarray('101000001')
    >>> a <<= 2
    >>> a
    bitarray('100000100')
    >>> b >> 1
    bitarray('011100101')

The C language does not specify the behavior of negative shifts and
of left shifts larger or equal than the width of the promoted left operand.
The exact behavior is compiler/machine specific.
This Python bitarray library specifies the behavior as follows:

* the length of the bitarray is never changed by any shift operation
* blanks are filled by 0
* negative shifts raise ``ValueError``
* shifts larger or equal to the length of the bitarray result in
  bitarrays with all values 0


Bit endianness
--------------

Unless explicitly converting to machine representation, using
the ``.tobytes()``, ``.frombytes()``, ``.tofile()`` and ``.fromfile()``
methods, as well as using ``memoryview``, the bit endianness will have no
effect on any computation, and one can skip this section.

Since bitarrays allows addressing individual bits, where the machine
represents 8 bits in one byte, there are two obvious choices for this
mapping: little-endian and big-endian.

When dealing with the machine representation of bitarray objects, it is
recommended to always explicitly specify the endianness.

By default, bitarrays use big-endian representation:

.. code-block:: python

    >>> a = bitarray()
    >>> a.endian()
    'big'
    >>> a.frombytes(b'A')
    >>> a
    bitarray('01000001')
    >>> a[6] = 1
    >>> a.tobytes()
    b'C'

Big-endian means that the most-significant bit comes first.
Here, ``a[0]`` is the lowest address (index) and most significant bit,
and ``a[7]`` is the highest address and least significant bit.

When creating a new bitarray object, the endianness can always be
specified explicitly:

.. code-block:: python

    >>> a = bitarray(endian='little')
    >>> a.frombytes(b'A')
    >>> a
    bitarray('10000010')
    >>> a.endian()
    'little'

Here, the low-bit comes first because little-endian means that increasing
numeric significance corresponds to an increasing address.
So ``a[0]`` is the lowest address and least significant bit,
and ``a[7]`` is the highest address and most significant bit.

The bit endianness is a property of the bitarray object.
The endianness cannot be changed once a bitarray object is created.
When comparing bitarray objects, the endianness (and hence the machine
representation) is irrelevant; what matters is the mapping from indices
to bits:

.. code-block:: python

    >>> bitarray('11001', endian='big') == bitarray('11001', endian='little')
    True

Bitwise operations (``|``, ``^``, ``&=``, ``|=``, ``^=``, ``~``) are
implemented efficiently using the corresponding byte operations in C, i.e. the
operators act on the machine representation of the bitarray objects.
Therefore, it is not possible to perform bitwise operators on bitarrays
with different endianness.

When converting to and from machine representation, using
the ``.tobytes()``, ``.frombytes()``, ``.tofile()`` and ``.fromfile()``
methods, the endianness matters:

.. code-block:: python

    >>> a = bitarray(endian='little')
    >>> a.frombytes(b'\x01')
    >>> a
    bitarray('10000000')
    >>> b = bitarray(endian='big')
    >>> b.frombytes(b'\x80')
    >>> b
    bitarray('10000000')
    >>> a == b
    True
    >>> a.tobytes() == b.tobytes()
    False

As mentioned above, the endianness can not be changed once an object is
created.  However, you can create a new bitarray with different endianness:

.. code-block:: python

    >>> a = bitarray('111000', endian='little')
    >>> b = bitarray(a, endian='big')
    >>> b
    bitarray('111000')
    >>> a == b
    True


Buffer protocol
---------------

Bitarray objects support the buffer protocol.  They can both export their
own buffer, as well as import another object's buffer.  To learn more about
this topic, please read `buffer protocol <https://github.com/ilanschnell/bitarray/blob/master/doc/buffer.rst>`__.  There is also an example that shows how
to memory-map a file to a bitarray: `mmapped-file.py <https://github.com/ilanschnell/bitarray/blob/master/examples/mmapped-file.py>`__


Variable bit length prefix codes
--------------------------------

The ``.encode()`` method takes a dictionary mapping symbols to bitarrays
and an iterable, and extends the bitarray object with the encoded symbols
found while iterating.  For example:

.. code-block:: python

    >>> d = {'H':bitarray('111'), 'e':bitarray('0'),
    ...      'l':bitarray('110'), 'o':bitarray('10')}
    ...
    >>> a = bitarray()
    >>> a.encode(d, 'Hello')
    >>> a
    bitarray('111011011010')

Note that the string ``'Hello'`` is an iterable, but the symbols are not
limited to characters, in fact any immutable Python object can be a symbol.
Taking the same dictionary, we can apply the ``.decode()`` method which will
return a list of the symbols:

.. code-block:: python

    >>> a.decode(d)
    ['H', 'e', 'l', 'l', 'o']
    >>> ''.join(a.decode(d))
    'Hello'

Since symbols are not limited to being characters, it is necessary to return
them as elements of a list, rather than simply returning the joined string.
The above dictionary ``d`` can be efficiently constructed using the function
``bitarray.util.huffman_code()``.  I also wrote `Huffman coding in Python
using bitarray <http://ilan.schnell-web.net/prog/huffman/>`__ for more
background information.

When the codes are large, and you have many decode calls, most time will
be spent creating the (same) internal decode tree objects.  In this case,
it will be much faster to create a ``decodetree`` object, which can be
passed to bitarray's ``.decode()`` and ``.iterdecode()`` methods, instead
of passing the prefix code dictionary to those methods itself:

.. code-block:: python

    >>> from bitarray import bitarray, decodetree
    >>> t = decodetree({'a': bitarray('0'), 'b': bitarray('1')})
    >>> a = bitarray('0110')
    >>> a.decode(t)
    ['a', 'b', 'b', 'a']
    >>> ''.join(a.iterdecode(t))
    'abba'

The ``decodetree`` object is immutable and unhashable, and it's sole purpose
is to be passed to bitarray's `.decode()` and `.iterdecode()` methods.


Frozenbitarrays
---------------

A ``frozenbitarray`` object is very similar to the bitarray object.
The difference is that this a ``frozenbitarray`` is immutable, and hashable,
and can therefore be used as a dictionary key:

.. code-block:: python

    >>> from bitarray import frozenbitarray
    >>> key = frozenbitarray('1100011')
    >>> {key: 'some value'}
    {frozenbitarray('1100011'): 'some value'}
    >>> key[3] = 1
    Traceback (most recent call last):
        ...
    TypeError: frozenbitarray is immutable


Reference
=========

bitarray version: 2.3.0 -- `change log <https://github.com/ilanschnell/bitarray/blob/master/doc/changelog.rst>`__

In the following, ``item`` and ``value`` are usually a single bit -
an integer 0 or 1.


The bitarray object:
--------------------

``bitarray(initializer=0, /, endian='big', buffer=None)`` -> bitarray
   Return a new bitarray object whose items are bits initialized from
   the optional initial object, and endianness.
   The initializer may be of the following types:

   ``int``: Create a bitarray of given integer length.  The initial values are
   uninitialized.

   ``str``: Create bitarray from a string of ``0`` and ``1``.

   ``iterable``: Create bitarray from iterable or sequence or integers 0 or 1.

   Optional keyword arguments:

   ``endian``: Specifies the bit endianness of the created bitarray object.
   Allowed values are ``big`` and ``little`` (the default is ``big``).
   The bit endianness effects the buffer representation of the bitarray.

   ``buffer``: Any object which exposes a buffer.  When provided, ``initializer``
   cannot be present (or has to be ``None``).  The imported buffer may be
   readonly or writable, depending on the object type.

   New in version 2.3: optional ``buffer`` argument.


**A bitarray object supports the following methods:**

``all()`` -> bool
   Return True when all bits in the array are True.
   Note that ``a.all()`` is faster than ``all(a)``.


``any()`` -> bool
   Return True when any bit in the array is True.
   Note that ``a.any()`` is faster than ``any(a)``.


``append(item, /)``
   Append ``item`` to the end of the bitarray.


``buffer_info()`` -> tuple
   Return a tuple containing:

   0. memory address of buffer
   1. buffer size (in bytes)
   2. bit endianness as a string
   3. number of unused padding bits
   4. allocated memory for the buffer (in bytes)
   5. memory is read-only
   6. buffer is imported
   7. number of buffer exports


``bytereverse(start=0, stop=<end of buffer>, /)``
   Reverse the bit order for the bytes in range(start, stop) in-place.
   The start and stop indices are given in terms of bytes (not bits).
   By default, all bytes in the buffer are reversed.
   Note: This method only changes the buffer; it does not change the
   endianness of the bitarray object.

   New in version 2.2.5: optional ``start`` and ``stop`` arguments.


``clear()``
   Remove all items from the bitarray.

   New in version 1.4.


``copy()`` -> bitarray
   Return a copy of the bitarray.


``count(value=1, start=0, stop=<end of array>, /)`` -> int
   Count the number of occurrences of ``value`` in the bitarray.

   New in version 1.1.0: optional ``start`` and ``stop`` arguments.


``decode(code, /)`` -> list
   Given a prefix code (a dict mapping symbols to bitarrays, or ``decodetree``
   object), decode the content of the bitarray and return it as a list of
   symbols.


``encode(code, iterable, /)``
   Given a prefix code (a dict mapping symbols to bitarrays),
   iterate over the iterable object with symbols, and extend the bitarray
   with the corresponding bitarray for each symbol.


``endian()`` -> str
   Return the bit endianness of the bitarray as a string (``little`` or ``big``).


``extend(iterable, /)``
   Append all the items from ``iterable`` to the end of the bitarray.
   If the iterable is a string, each ``0`` and ``1`` are appended as
   bits (ignoring whitespace and underscore).


``fill()`` -> int
   Add zeros to the end of the bitarray, such that the length of the bitarray
   will be a multiple of 8, and return the number of bits added (0..7).


``find(sub_bitarray, start=0, stop=<end of array>, /)`` -> int
   Return the lowest index where sub_bitarray is found, such that sub_bitarray
   is contained within ``[start:stop]``.
   Return -1 when sub_bitarray is not found.

   New in version 2.1.


``frombytes(bytes, /)``
   Extend bitarray with raw bytes.  That is, each append byte will add eight
   bits to the bitarray.


``fromfile(f, n=-1, /)``
   Extend bitarray with up to n bytes read from the file object f.
   When n is omitted or negative, reads all data until EOF.
   When n is provided and positive but exceeds the data available,
   ``EOFError`` is raised (but the available data is still read and appended.


``index(sub_bitarray, start=0, stop=<end of array>, /)`` -> int
   Return the lowest index where sub_bitarray is found, such that sub_bitarray
   is contained within ``[start:stop]``.
   Raises ``ValueError`` when the sub_bitarray is not present.


``insert(index, value, /)``
   Insert ``value`` into the bitarray before ``index``.


``invert(index=<all bits>, /)``
   Invert all bits in the array (in-place).
   When the optional ``index`` is given, only invert the single bit at index.

   New in version 1.5.3: optional ``index`` argument.


``iterdecode(code, /)`` -> iterator
   Given a prefix code (a dict mapping symbols to bitarrays, or ``decodetree``
   object), decode the content of the bitarray and return an iterator over
   the symbols.


``itersearch(sub_bitarray, /)`` -> iterator
   Searches for the given sub_bitarray in self, and return an iterator over
   the start positions where bitarray matches self.


``pack(bytes, /)``
   Extend the bitarray from bytes, where each byte corresponds to a single
   bit.  The byte ``b'\x00'`` maps to bit 0 and all other characters map to
   bit 1.
   This method, as well as the unpack method, are meant for efficient
   transfer of data between bitarray objects to other python objects
   (for example NumPy's ndarray object) which have a different memory view.


``pop(index=-1, /)`` -> item
   Return the i-th (default last) element and delete it from the bitarray.
   Raises ``IndexError`` if bitarray is empty or index is out of range.


``remove(value, /)``
   Remove the first occurrence of ``value`` in the bitarray.
   Raises ``ValueError`` if item is not present.


``reverse()``
   Reverse all bits in the array (in-place).


``search(sub_bitarray, limit=<none>, /)`` -> list
   Searches for the given sub_bitarray in self, and return the list of start
   positions.
   The optional argument limits the number of search results to the integer
   specified.  By default, all search results are returned.


``setall(value, /)``
   Set all elements in the bitarray to ``value``.
   Note that ``a.setall(value)`` is equivalent to ``a[:] = value``.


``sort(reverse=False)``
   Sort the bits in the array (in-place).


``to01()`` -> str
   Return a string containing '0's and '1's, representing the bits in the
   bitarray.


``tobytes()`` -> bytes
   Return the byte representation of the bitarray.


``tofile(f, /)``
   Write the byte representation of the bitarray to the file object f.


``tolist()`` -> list
   Return a list with the items (0 or 1) in the bitarray.
   Note that the list object being created will require 32 or 64 times more
   memory (depending on the machine architecture) than the bitarray object,
   which may cause a memory error if the bitarray is very large.


``unpack(zero=b'\x00', one=b'\x01')`` -> bytes
   Return bytes containing one character for each bit in the bitarray,
   using the specified mapping.


Other objects:
--------------

``frozenbitarray(initializer=0, /, endian='big', buffer=None)`` -> frozenbitarray
   Return a frozenbitarray object, which is initialized the same way a bitarray
   object is initialized.  A frozenbitarray is immutable and hashable.
   Its contents cannot be altered after it is created; however, it can be used
   as a dictionary key.

   New in version 1.1.


``decodetree(code, /)`` -> decodetree
   Given a prefix code (a dict mapping symbols to bitarrays),
   create a binary tree object to be passed to ``.decode()`` or ``.iterdecode()``.

   New in version 1.6.


Functions defined in the `bitarray` module:
-------------------------------------------

``bits2bytes(n, /)`` -> int
   Return the number of bytes necessary to store n bits.


``get_default_endian()`` -> string
   Return the default endianness for new bitarray objects being created.
   Unless ``_set_default_endian()`` is called, the return value is ``big``.

   New in version 1.3.


``test(verbosity=1, repeat=1)`` -> TextTestResult
   Run self-test, and return unittest.runner.TextTestResult object.


Functions defined in `bitarray.util` module:
--------------------------------------------

This sub-module was add in version 1.2.

``zeros(length, /, endian=None)`` -> bitarray
   Create a bitarray of length, with all values 0, and optional
   endianness, which may be 'big', 'little'.


``urandom(length, /, endian=None)`` -> bitarray
   Return a bitarray of ``length`` random bits (uses ``os.urandom``).

   New in version 1.7.


``pprint(bitarray, /, stream=None, group=8, indent=4, width=80)``
   Prints the formatted representation of object on ``stream``, followed by a
   newline.  If ``stream`` is ``None``, ``sys.stdout`` is used.  By default, elements
   are grouped in bytes (8 elements), and 8 bytes (64 elements) per line.
   Non-bitarray objects are printed by the standard library
   function ``pprint.pprint()``.

   New in version 1.8.


``make_endian(bitarray, endian, /)`` -> bitarray
   When the endianness of the given bitarray is different from ``endian``,
   return a new bitarray, with endianness ``endian`` and the same elements
   as the original bitarray.
   Otherwise (endianness is already ``endian``) the original bitarray is returned
   unchanged.

   New in version 1.3.


``rindex(bitarray, value=1, start=0, stop=<end of array>, /)`` -> int
   Return the rightmost (highest) index of ``value`` in bitarray.
   Raises ``ValueError`` if the value is not present.

   New in version 2.3.0: optional ``start`` and ``stop`` arguments.


``strip(bitarray, /, mode='right')`` -> bitarray
   Return a new bitarray with zeros stripped from left, right or both ends.
   Allowed values for mode are the strings: ``left``, ``right``, ``both``


``count_n(a, n, /)`` -> int
   Return lowest index ``i`` for which ``a[:i].count() == n``.
   Raises ``ValueError``, when n exceeds total count (``a.count()``).


``parity(a, /)`` -> int
   Return the parity of bitarray ``a``.
   This is equivalent to ``a.count() % 2`` (but more efficient).

   New in version 1.9.


``count_and(a, b, /)`` -> int
   Return ``(a & b).count()`` in a memory efficient manner,
   as no intermediate bitarray object gets created.


``count_or(a, b, /)`` -> int
   Return ``(a | b).count()`` in a memory efficient manner,
   as no intermediate bitarray object gets created.


``count_xor(a, b, /)`` -> int
   Return ``(a ^ b).count()`` in a memory efficient manner,
   as no intermediate bitarray object gets created.


``subset(a, b, /)`` -> bool
   Return ``True`` if bitarray ``a`` is a subset of bitarray ``b``.
   ``subset(a, b)`` is equivalent to ``(a & b).count() == a.count()`` but is more
   efficient since we can stop as soon as one mismatch is found, and no
   intermediate bitarray object gets created.


``ba2hex(bitarray, /)`` -> hexstr
   Return a string containing the hexadecimal representation of
   the bitarray (which has to be multiple of 4 in length).


``hex2ba(hexstr, /, endian=None)`` -> bitarray
   Bitarray of hexadecimal representation.  hexstr may contain any number
   (including odd numbers) of hex digits (upper or lower case).


``ba2base(n, bitarray, /)`` -> str
   Return a string containing the base ``n`` ASCII representation of
   the bitarray.  Allowed values for ``n`` are 2, 4, 8, 16, 32 and 64.
   The bitarray has to be multiple of length 1, 2, 3, 4, 5 or 6 respectively.
   For ``n=16`` (hexadecimal), ``ba2hex()`` will be much faster, as ``ba2base()``
   does not take advantage of byte level operations.
   For ``n=32`` the RFC 4648 Base32 alphabet is used, and for ``n=64`` the
   standard base 64 alphabet is used.

   See also: `Bitarray representations <https://github.com/ilanschnell/bitarray/blob/master/doc/represent.rst>`__

   New in version 1.9.


``base2ba(n, asciistr, /, endian=None)`` -> bitarray
   Bitarray of the base ``n`` ASCII representation.
   Allowed values for ``n`` are 2, 4, 8, 16, 32 and 64.
   For ``n=16`` (hexadecimal), ``hex2ba()`` will be much faster, as ``base2ba()``
   does not take advantage of byte level operations.
   For ``n=32`` the RFC 4648 Base32 alphabet is used, and for ``n=64`` the
   standard base 64 alphabet is used.

   See also: `Bitarray representations <https://github.com/ilanschnell/bitarray/blob/master/doc/represent.rst>`__

   New in version 1.9.


``ba2int(bitarray, /, signed=False)`` -> int
   Convert the given bitarray into an integer.
   The bit-endianness of the bitarray is respected.
   ``signed`` indicates whether two's complement is used to represent the integer.


``int2ba(int, /, length=None, endian=None, signed=False)`` -> bitarray
   Convert the given integer to a bitarray (with given endianness,
   and no leading (big-endian) / trailing (little-endian) zeros), unless
   the ``length`` of the bitarray is provided.  An ``OverflowError`` is raised
   if the integer is not representable with the given number of bits.
   ``signed`` determines whether two's complement is used to represent the integer,
   and requires ``length`` to be provided.


``serialize(bitarray, /)`` -> bytes
   Return a serialized representation of the bitarray, which may be passed to
   ``deserialize()``.  It efficiently represents the bitarray object (including
   its endianness) and is guaranteed not to change in future releases.

   See also: `Bitarray representations <https://github.com/ilanschnell/bitarray/blob/master/doc/represent.rst>`__

   New in version 1.8.


``deserialize(bytes, /)`` -> bitarray
   Return a bitarray given the bytes representation returned by ``serialize()``.

   See also: `Bitarray representations <https://github.com/ilanschnell/bitarray/blob/master/doc/represent.rst>`__

   New in version 1.8.


``vl_encode(bitarray, /)`` -> bytes
   Return variable length binary representation of bitarray.
   This representation is useful for efficiently storing small bitarray
   in a binary stream.  Use ``vl_decode()`` for decoding.

   See also: `Variable length bitarray format <https://github.com/ilanschnell/bitarray/blob/master/doc/variable_length.rst>`__

   New in version 2.2.


``vl_decode(stream, /, endian=None)`` -> bitarray
   Decode binary stream (an integer iterator, or bytes object), and return
   the decoded bitarray.  This function consumes only one bitarray and leaves
   the remaining stream untouched.  ``StopIteration`` is raised when no
   terminating byte is found.
   Use ``vl_encode()`` for encoding.

   See also: `Variable length bitarray format <https://github.com/ilanschnell/bitarray/blob/master/doc/variable_length.rst>`__

   New in version 2.2.


``huffman_code(dict, /, endian=None)`` -> dict
   Given a frequency map, a dictionary mapping symbols to their frequency,
   calculate the Huffman code, i.e. a dict mapping those symbols to
   bitarrays (with given endianness).  Note that the symbols are not limited
   to being strings.  Symbols may may be any hashable object (such as ``None``).


