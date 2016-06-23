#!/usr/bin/python

import io
import re
import sys
from os import SEEK_SET, SEEK_END
from mmap import mmap, PROT_READ

"""
    LDIF File Structure

    <file> ::= <rec> <file> | <rec>

    <rec> ::= <ent> <rec> | <ent> <END>
    <ent> ::= <key> <sep> <val> [<comment> <text>] <EOL> |
                <whitespace> <comment> <text>

    <END> ::= <END> <END> | <UDE> <EOL> | <whitespace> <EOL>

    <whitespace> ::= " " <whitespace> | "\t" <whitespace> | ""
    <key> ::= <text>
    <sep> ::= <text>
    <val> ::= <text>
    <EOL> ::= "\r" | "\r\n" | "\n"
    <UDE> ::= <text> ; "User Defined End"

    <text> ::= ; any alpha-numeric text is permitted
"""


class LDIFFile(object):

    PKEY_ERROR_STR = "Unable to determine a Primary Key."
    PKEY_DUP_ERROR_STR = "The Primary Key must be unique for all recs."
    RE_END = re.compile(r"^\s*$")

    def __init__(self, path, pkey=None, case_sensitive=False,
                 use_mmap=True, encoding=None):

        self.fd = io.open(path, 'r')

        if use_mmap:
            self.fd = mmap(self.fd.fileno(), 0, prot=PROT_READ)

        self.fd.seek(0, SEEK_END)
        self._file_size = self.fd.tell()
        self.fd.seek(0, SEEK_SET)

        self._iter_hold = None

        self.case_sensitive = case_sensitive
        self.sep = ": "
        self.re_eor = re.compile(r"^\s*$")
        self.re_ent = re.compile(r"^.*: .*$")

        pkey = "NETID"
        if pkey is None:
            pkey = self.locate_pkey()

        self.pkey = pkey

        self.create_index(self.pkey)

    def __getitem__(self, value):

        rec = None
        hold = self.fd.tell()

        self.seek(value)

        rec = self.read_rec()
        self.fd.seek(hold, SEEK_SET)

        return rec

    def __iter__(self):

        if self._iter_hold is None:
            self._iter_hold = self.fd.tell()

        return self

    def __next__(self):

        if self.fd.tell() >= self._file_size:
            self.fd.seek(0, self._iter_hold)
            self._iter_hold = None
            raise StopIteration

        return self.read_rec()

    # Handle Python 2 Iterators
    def next(self):
        return self.__next__()

    def EOF(self):
        return self._file_size == self.fd.tell()

    def seek(self, value):

        if isinstance(value, int):
            pos = self.int_index[value]
        else:
            pos = self.str_index[value]

        self.fd.seek(pos, SEEK_SET)

    # TODO!!
    def locate_pkey(self, interact=False):

        pkey = None
        candidates = None

        for rec in self:
            print(candidates)
            keys = set(rec.keys())
            if candidates is None:
                candidates = keys
            else:
                candidates = candidates.intersection(keys)

        if candidates is None:
            raise ValueError()

        count = len(candidates)

        if count == 1:
            pkey = candidates.pop()
        elif count == 0:
            raise ValueError(LDIFFile.PKEY_ERROR_STR)
        else:
            if interact is False:
                raise ValueError(LDIFFile.PKEY_ERROR_STR)
            else:
                print candidates
                sys.exit()
                """
                    TODO:
                        Select Primary Key From Candidates:
                        1. DN
                        2. NETID
                        3. UUID
                        ...
                        Enter Choice: 2
                """
                pass

        return pkey

    def read_rec(self):

        eor = False
        rec = dict()
        pos = self.fd.tell()

        while True:
            pos = self.fd.tell()
            line = self.fd.readline().strip()

            if self.re_eor.match(line):
                eor = True
            # if line is an ent
            elif self.re_ent.match(line):
                if eor:
                    self.fd.seek(pos, SEEK_SET)
                    break
                else:
                    key, value = line.split(self.sep, 1)

                    if not self.case_sensitive:
                        key = key.upper()

                    if key in rec:
                        rec[key].add(value)
                    else:
                        rec[key] = set([value])
            else:
                pass
#                raise ValueError(line)

            if self.EOF():
                break

        return rec

    def create_index(self, pkey):

        offset = 0
        self.fd.seek(0, SEEK_SET)
        self.str_index = dict({})
        self.int_index = list()

        for rec in self:

            tag = list(rec[self.pkey])[0]
            if tag in self.str_index:
                continue
                raise KeyError(LDIFFile.PKEY_DUP_ERROR_STR)
            self.str_index[tag] = offset
            self.int_index.append(offset)

            offset = self.fd.tell()

"""

    ldif rec diff

        {
            x: [('+', 'foo'), ('-', 'bar'), ('=', 'baz')],
            y: [('+', 'foo'), ('-', 'bar'), ('=', 'baz')]
        }


"""


class LDIFDiff(object):

    DIFF_ADD = '+'
    DIFF_DEL = '-'
    DIFF_EQU = '='

    def __init__(self, path_a, path_b, memory_map=True,
                 case_sensitive=False, ignore=None):

        self.a = LDIFFile(path_a)
        self.b = LDIFFile(path_b)

        if self.a.pkey != self.b.pkey:
            return None

        self.pkey = self.a.pkey
        self.case_sensitive = case_sensitive

        if ignore:
            self.ignore = ignore
        else:
            self.ignore = list()

    def print_diff(self, diff):

        result = False
        temp = io.BytesIO()

        for key, ops in diff.items():

            if key == self.pkey:
                continue

            if key in self.ignore:
                continue

            for (op, value) in ops:

                if op != LDIFDiff.DIFF_EQU:

                    temp.write("{op} {key}: {value}\n".format(op=op,
                                                              key=key,
                                                              value=value))

        if temp.tell():

            value = list(diff[self.pkey])[0][1]

            sys.stdout.write("{pkey}: {value}\n".format(pkey=self.pkey,
                                                        value=value))

            temp.seek(0, SEEK_SET)
            sys.stdout.write(temp.read())

            sys.stdout.write("\n")

            result = True

        temp.close()
        return result

    def diff_rec(self, a, b):

        a_keys = set(a.keys())
        b_keys = set(b.keys())

        same = True
        diff = {key: list() for key in a_keys.union(b_keys)}

        for key in a_keys.intersection(b_keys):

            a_values = a[key]
            b_values = b[key]

            if not self.case_sensitive:
                a_values = set(map(str.upper, a_values))
                b_values = set(map(str.upper, b_values))

            # noop
            for value in a_values.intersection(b_values):
                diff[key].append((LDIFDiff.DIFF_EQU, value))

            # delete
            for value in a_values.difference(b_values):
                diff[key].append((LDIFDiff.DIFF_DEL, value))
                same = False

            # create
            for value in b_values.difference(a_values):
                diff[key].append((LDIFDiff.DIFF_ADD, value))
                same = False

        # delete
        for key in a_keys.difference(b_keys):
            for value in a[key]:
                diff[key].append((LDIFDiff.DIFF_DEL, value))
                same = False

        # create
        for key in b_keys.difference(a_keys):
            for value in b[key]:
                diff[key].append((LDIFDiff.DIFF_ADD, value))
                same = False

        return (same, diff)

    def diff(self):

        pkey = self.a.pkey

        a_keys = set(self.a.str_index.keys())
        b_keys = set(self.b.str_index.keys())

        count = 0
        # modify
        for index in a_keys.intersection(b_keys):
            a_rec = self.a[index]
            b_rec = self.b[index]
            (same, diff) = self.diff_rec(a_rec, b_rec)

            if self.print_diff(diff):
                count += 1

        sys.stderr.write("MODIFY {0}\n".format(count))

        count = 0
        # create
        for index in b_keys.difference(a_keys):
            b_rec = self.b[index]
            (same, diff) = self.diff_rec({}, b_rec)

            count += 1
            self.print_diff(diff)

        sys.stderr.write("CREATE {0}\n".format(count))

        count = 0
        # delete
        for index in a_keys.difference(b_keys):
            a_rec = self.a[index]
            (same, diff) = self.diff_rec(a_rec, {})

            count += 1
            self.print_diff(diff)

        sys.stderr.write("DELETE {0}\n".format(count))


def main():
    ldd = LDIFDiff()


if __name__ == "__main__":
    main()
