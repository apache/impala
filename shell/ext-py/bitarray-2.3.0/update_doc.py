import sys
assert sys.version_info[0] == 3, "This program requires Python 3"

import re
import doctest
from io import StringIO

import bitarray
import bitarray.util


BASE_URL = "https://github.com/ilanschnell/bitarray"

NEW_IN = {
    'frozenbitarray':       '1.1',
    'get_default_endian':   '1.3',
    'util.make_endian':     '1.3',
    'bitarray':             '2.3: optional `buffer` argument',
    'bitarray.bytereverse': '2.2.5: optional `start` and `stop` arguments',
    'bitarray.count':       '1.1.0: optional `start` and `stop` arguments',
    'bitarray.clear':       '1.4',
    'bitarray.find':        '2.1',
    'bitarray.invert':      '1.5.3: optional `index` argument',
    'decodetree':           '1.6',
    'util.urandom':         '1.7',
    'util.pprint':          '1.8',
    'util.serialize':       '1.8',
    'util.deserialize':     '1.8',
    'util.ba2base':         '1.9',
    'util.base2ba':         '1.9',
    'util.parity':          '1.9',
    'util.rindex':          '2.3.0: optional `start` and `stop` arguments',
    'util.vl_encode':       '2.2',
    'util.vl_decode':       '2.2',
}

DOCS = {
    'rep': ('Bitarray representations', 'represent.rst'),
    'vlf': ('Variable length bitarray format', 'variable_length.rst'),
}

DOC_LINKS = {
    'util.ba2base':     'rep',
    'util.base2ba':     'rep',
    'util.serialize':   'rep',
    'util.deserialize': 'rep',
    'util.vl_encode':   'vlf',
    'util.vl_decode':   'vlf',
}

_NAMES = set()

sig_pat = re.compile(r'(\w+\([^()]*\))( -> (.+))?')
def write_doc(fo, name):
    _NAMES.add(name)
    doc = eval('bitarray.%s.__doc__' % name)
    assert doc, name
    lines = doc.splitlines()
    m = sig_pat.match(lines[0])
    if m is None:
        raise Exception("signature line invalid: %r" % lines[0])
    s = '``%s``' %  m.group(1)
    if m.group(3):
        s += ' -> %s' % m.group(3)
    fo.write('%s\n' % s)
    assert lines[1] == ''
    for line in lines[2:]:
        out = line.rstrip()
        fo.write("   %s\n" % out.replace('`', '``') if out else "\n")

    link = DOC_LINKS.get(name)
    if link:
        title, filename = DOCS[link]
        url = BASE_URL + '/blob/master/doc/' + filename
        fo.write("\n   See also: `%s <%s>`__\n" % (title, url))

    new_in = NEW_IN.get(name)
    if new_in:
        fo.write("\n   New in version %s.\n" % new_in.replace('`', '``'))

    fo.write('\n\n')


def write_reference(fo):
    fo.write("""\
Reference
=========

bitarray version: %s -- `change log <%s>`__

In the following, ``item`` and ``value`` are usually a single bit -
an integer 0 or 1.


The bitarray object:
--------------------

""" % (bitarray.__version__, BASE_URL + "/blob/master/doc/changelog.rst"))
    write_doc(fo, 'bitarray')

    fo.write("**A bitarray object supports the following methods:**\n\n")
    for method in sorted(dir(bitarray.bitarray)):
        if method.startswith('_'):
            continue
        write_doc(fo, 'bitarray.%s' % method)

    fo.write("Other objects:\n"
             "--------------\n\n")
    write_doc(fo, 'frozenbitarray')
    write_doc(fo, 'decodetree')

    fo.write("Functions defined in the `bitarray` module:\n"
             "-------------------------------------------\n\n")
    for func in sorted(['test', 'bits2bytes', 'get_default_endian']):
        write_doc(fo, func)

    fo.write("Functions defined in `bitarray.util` module:\n"
             "--------------------------------------------\n\n"
             "This sub-module was add in version 1.2.\n\n")
    for func in bitarray.util.__all__:
        write_doc(fo, 'util.%s' % func)

    for name in list(NEW_IN) + list(DOC_LINKS):
        assert name in _NAMES, name

def update_readme(path):
    ver_pat = re.compile(r'(bitarray.+?)(\d+\.\d+\.\d+)')

    with open(path, 'r') as fi:
        data = fi.read()

    with StringIO() as fo:
        for line in data.splitlines():
            if line == 'Reference':
                break
            line = ver_pat.sub(lambda m: m.group(1) + bitarray.__version__,
                               line)
            fo.write("%s\n" % line.rstrip())

        write_reference(fo)
        new_data = fo.getvalue()

    if new_data == data:
        print("already up-to-date")
    else:
        with open(path, 'w') as f:
            f.write(new_data)


def write_changelog(fo):
    ver_pat = re.compile(r'(\d{4}-\d{2}-\d{2})\s+(\d+\.\d+\.\d+)')
    issue_pat = re.compile(r'#(\d+)')
    link_pat = re.compile(r'\[(.+)\]\((.+)\)')

    def issue_replace(match):
        url = "%s/issues/%s" % (BASE_URL, match.group(1))
        return "`%s <%s>`__" % (match.group(0), url)

    fo.write("Change log\n"
             "==========\n\n")

    for line in open('./CHANGE_LOG'):
        line = line.rstrip()
        match = ver_pat.match(line)
        if match:
            line = match.expand(r'**\2** (\1):')
        elif line.startswith('-----'):
            line = ''
        elif line.startswith('  '):
            line = line[2:]
        line = line.replace('`', '``')
        line = issue_pat.sub(issue_replace, line)
        line = link_pat.sub(
                    lambda m: "`%s <%s>`__" % (m.group(1), m.group(2)), line)
        fo.write(line + '\n')


def main():
    if len(sys.argv) > 1:
        sys.exit("no arguments expected")

    update_readme('./README.rst')
    with open('./doc/reference.rst', 'w') as fo:
        write_reference(fo)
    with open('./doc/changelog.rst', 'w') as fo:
        write_changelog(fo)

    doctest.testfile('./README.rst')
    doctest.testfile('./doc/buffer.rst')
    doctest.testfile('./doc/represent.rst')
    doctest.testfile('./doc/variable_length.rst')


if __name__ == '__main__':
    main()
