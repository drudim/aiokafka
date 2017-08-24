import abc
from collections import namedtuple


Record = namedtuple(
    "Record", ["attrs", "timestamp", "offset", "key", "value", "headers",
               "timestamp_type"])


class CorruptRecordException(Exception):
    pass


class CrcCheckFailed(CorruptRecordException):

    def __init__(self, record_crc, calc_crc):
        message = "Record crc {} does not match calculated {}".format(
            record_crc, calc_crc)
        super().__init__(message)


class ABCRecordBatchWriter(abc.ABC):

    @abc.abstractmethod
    def append(self, offset, timestamp, key, value, headers):
        """ Writes record to internal buffer
        """

    @abc.abstractmethod
    def close(self):
        """ Stop appending new messages, write header, compress if needed and
        return compressed bytes ready to be sent.

            Returns: io.BytesIO buffer with ready to send data
        """


class ABCRecordBatchReader(abc.ABC):

    @abc.abstractmethod
    def __init__(self, buffer, validate_crc):
        """ Initialize with io.BytesIO buffer that can be read from
        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """ Return iterator over records
        """


class ABCRecord(abc.ABC):

    @abc.abstractproperty
    def offset(self):
        pass

    @abc.abstractproperty
    def timestamp(self):
        """ Epoch milliseconds
        """

    @abc.abstractproperty
    def timestamp_type(self):
        """ CREATE_TIME(0) or APPEND_TIME(1)
        """

    @abc.abstractproperty
    def key(self):
        """ Bytes key or None
        """

    @abc.abstractproperty
    def value(self):
        """ Bytes value or None
        """

    @abc.abstractproperty
    def checksum(self):
        """ Prior to v2 format CRC was contained in every message. This will
            be the checksum for v0 and v1 and None for v2 and above.
        """

    @abc.abstractproperty
    def headers(self):
        """ If supported by version list of key-value tuples, or empty list if
            not supported by format.
        """
