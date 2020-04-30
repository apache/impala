import sys
if not sys.version_info[0] == 3:
    sys.exit("This program only runs with Python 3, sorry :-(")

import re
import doctest
from io import StringIO

import bitarray
import bitarray.util


fo = StringIO()


def write_changelog():
    fo.write("Change log\n"
             "----------\n\n")
    ver_pat = re.compile(r'(\d{4}-\d{2}-\d{2})\s+(\d+\.\d+\.\d+)')
    count = 0
    for line in open('CHANGE_LOG'):
        m = ver_pat.match(line)
        if m:
            if count == 3:
                break
            count += 1
            fo.write(m.expand(r'*\2* (\1):\n'))
        elif line.startswith('---'):
            fo.write('\n')
        else:
            fo.write(line)

    url = "https://github.com/ilanschnell/bitarray/blob/master/CHANGE_LOG"
    fo.write('Please find the complete change log\n'
             '<a href="%s">here</a>.\n' % url)


sig_pat = re.compile(r'(\w+\([^()]*\))( -> (.+))?')
def write_doc(name):
    doc = eval('bitarray.%s.__doc__' % name)
    lines = doc.splitlines()
    m = sig_pat.match(lines[0])
    if m is None:
        raise Exception("signature line invalid: %r" % lines[0])
    s = '`%s`' %  m.group(1)
    if m.group(3):
        s += ' -> %s' % m.group(3)
    fo.write(s + '\n\n')
    assert lines[1] == ''
    for line in lines[2:]:
        fo.write(line.rstrip() + '\n')
    fo.write('\n\n')


def write_reference():
    fo.write("Reference\n"
             "=========\n\n"
             "The bitarray object:\n"
             "--------------------\n\n")
    write_doc('bitarray')

    fo.write("**A bitarray object supports the following methods:**\n\n")
    for method in sorted(dir(bitarray.bitarray)):
        if method.startswith('_'):
            continue
        write_doc('bitarray.%s' % method)

    fo.write("The frozenbitarray object:\n"
             "--------------------------\n\n")
    write_doc('frozenbitarray')

    fo.write("Functions defined in the module:\n"
             "--------------------------------\n\n")
    write_doc('test')
    write_doc('bitdiff')
    write_doc('bits2bytes')

    fo.write("Functions defined in bitarray.util:\n"
             "-----------------------------------\n\n")
    for func in bitarray.util.__all__:
        write_doc('util.%s' % func)


def write_all(data):
    ver_pat = re.compile(r'(bitarray.+?)(\d+\.\d+\.\d+)')
    for line in data.splitlines():
        if line == 'Reference':
            break
        line = ver_pat.sub(lambda m: m.group(1) + bitarray.__version__, line)
        fo.write(line + '\n')

    write_reference()
    write_changelog()


def main():
    data = open('README.md').read()
    write_all(data)
    new_data = fo.getvalue()
    fo.close()

    if new_data == data:
        print("already up-to-date")
    else:
        with open('README.md', 'w') as f:
            f.write(new_data)

    doctest.testfile('README.md')


if __name__ == '__main__':
    main()
