
from datetime import datetime
from struct import Struct
from collections import namedtuple
from os import SEEK_SET, SEEK_END
import mmap
import io


def _decode_latin1(data):
    return data.decode('LATIN1')


def _encode_latin1(data):
    return data.encode('LATIN1')


def datetime_decoder(datetime_format, forbidden=None):

    UNIX_EPOCH = datetime.utcfromtimestamp(0).date()

    def parse_datetime(value):
        if forbidden and value in forbidden:
            return UNIX_EPOCH
        try:
            return datetime.strptime(value, datetime_format)
        except ValueError:
            return UNIX_EPOCH

    return parse_datetime


class DataTransforms(object):

    LATIN1 = {
        'DECODER': _decode_latin1,
        'ENCODER': _encode_latin1
    }


def IDENTITY_FUNCTION(x):
    return x


class FixedWidthParser(object):

    _padding = u" "

    def __init__(self, layout, name="FixedWidthData", strip=str.rstrip):

        self.name = name
        if strip is None:
            strip = IDENTITY_FUNCTION
        self.strip = strip

        self._decoders_enabled = False
        self._encoders_enabled = False

        self.__build_parser(layout)

    def __build_parser(self, layout):

        self._decoders = list()
        self._encoders = list()

        struct_fmt = str()
        members = list()

        for (name, length, options) in layout:

            # if name is None we 'skip' the object
            if name is None:
                struct_fmt += '{0}x'.format(length)
            else:
                struct_fmt += '{0}s'.format(length)
                members.append(name)

                decoder = IDENTITY_FUNCTION
                encoder = IDENTITY_FUNCTION

                # unpack build options
                if isinstance(options, dict):

                    # normalize option keys
                    options = dict((key.upper(), value)
                                   for key, value in options.items())

                    if 'DECODER' in options:
                        decoder = options['DECODER']
                    if 'ENCODER' in options:
                        encoder = options['ENCODER']

                self._decoders.append(decoder)
                self._encoders.append(encoder)

        self._struct = Struct(struct_fmt)
        self._object = namedtuple(self.name, members)

    def record_size(self):
        return self._struct.size

    def parse(self, data):

        size = self._struct.size
        length = len(data)
        delta = size - length

        # compute addtional padding needed for struct.unpack() to work
        padding = bytes(' ' * delta) if delta > 0 else bytes()

        data = bytes(data[:size] + padding)

        raw_record = self._struct.unpack(data)

        # for data which we want to interpret in flight we need to
        #  run the following loop

        # preallocate slots for the final record (faster than appending)
        record = [None] * len(raw_record)

        for index, value in enumerate(raw_record):
            # value = RE_STRIP_RIGHT.sub(r'', value)
            decoder = self._decoders[index]
            value = self.strip(value)
            if decoder is not IDENTITY_FUNCTION:
                value = decoder(value)

            record[index] = value

        # finally create the 'class' object which is returned
        return self._object._make(record)

    def unparse(self, data):

        if self._encoders is not None:
            record = [self._encoders[index](value)
                      if self._encoders[index] is not None else value
                      for (index, value) in enumerate(record)]

        return self._struct.pack(*record)


class FixedWidthFile(object):

    def __init__(self, path, layout, mode='r', name="FixedWidthFile",
                 line_sequential=True, memory_map=True, strip=str.rstrip):

        self.parser = FixedWidthParser(layout, name=name, strip=strip)

        buffering = 1 if line_sequential else self.parser.record_size()
        self.file = open(path, mode, self.parser.record_size())
        # io.open(path, mode, buffering=buffering, newline='\n')
        # self.parser.record_size())

        if memory_map is True:
            self.fd = mmap.mmap(self.file.fileno(), 0, prot=mmap.PROT_READ)
        else:
            self.fd = self.file

        # self.parser.set_stream(self.fd)
        self.line_sequential = line_sequential

        self.length_cache = None
        self.path = path

    # return the number of entries in the file
    def __len__(self):

        if self.length_cache is None:

            pos = self.fd.tell()

            if self.line_sequential:
                self.length_cache = sum(1 for _ in open(self.path, 'r'))
            else:
                self.fd.seek(0, SEEK_END)
                self.length_cache = self.fd.tell()

            self.fd.seek(pos, SEEK_SET)

        return self.length_cache

    def read(self):

        if self.line_sequential:
            data = self.fd.readline()
        else:
            data = self.fd.read()

        return self.parser.parse(data)

    def write(self, data):

        serialized = self.parser.unparse(data)

        self.fd.write(serialized)

        if self.line_sequential:
            self.fd.write("\n")

    # iteration

    _iter = None

    def __iter__(self):

        # hold current position to restore after iteration
        if self._iter is None:
            self._iter = self.fd.tell()

        self.fd.seek(0, SEEK_END)
        self.eof = self.fd.tell()
        self.fd.seek(0, SEEK_SET)

        return self

    def __next__(self):

        if self.fd.tell() >= self.eof:
            self.fd.seek(0, self._iter)
            self._iter = None
            raise StopIteration

        return self.read()

    def next(self):

        if self.fd.tell() >= self.eof:
            self.fd.seek(0, self._iter)
            self._iter = None
            raise StopIteration

        return self.read()


if __name__ == '__main__':
    pass
