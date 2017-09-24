import io
import struct

from .util import calc_crc32
from kafka.codec import (
    gzip_encode, snappy_encode, lz4_encode, lz4_encode_old_kafka,
)

cdef extern from "Python.h":
    ssize_t PyByteArray_GET_SIZE(object)
    char* PyByteArray_AS_STRING(bytearray ba) except NULL
    int PyByteArray_Resize(object, ssize_t)

    object PyMemoryView_FromMemory(char *mem, ssize_t size, int flags)

from cpython cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_WRITABLE, \
                     PyBUF_SIMPLE, PyBUF_READ, Py_buffer
from libc.string cimport memcpy
from libc.stdint cimport int32_t, int64_t, uint32_t

from aiokafka.record cimport _hton as hton


# Those are used for fast size calculations
DEF RECORD_OVERHEAD_V0 = 14
DEF RECORD_OVERHEAD_V1 = 22
DEF KEY_OFFSET_V0 = 18
DEF KEY_OFFSET_V1 = 26
DEF LOG_OVERHEAD = 12
DEF KEY_LENGTH = 4
DEF VALUE_LENGTH = 4

# Field offsets
DEF LENGTH_OFFSET = 8
DEF CRC_OFFSET = LENGTH_OFFSET + 4
DEF MAGIC_OFFSET = CRC_OFFSET + 4
DEF ATTRIBUTES_OFFSET = MAGIC_OFFSET + 1
DEF TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + 1

# Compression flags
DEF ATTR_CODEC_MASK = 0x07
DEF ATTR_CODEC_GZIP = 0x01
DEF ATTR_CODEC_SNAPPY = 0x02
DEF ATTR_CODEC_LZ4 = 0x03


cdef class _LegacyRecordBatchBuilderCython:

    cdef:
        char _magic
        char _compression_type
        bytearray _buffer

    CODEC_GZIP = ATTR_CODEC_GZIP
    CODEC_SNAPPY = ATTR_CODEC_SNAPPY
    CODEC_LZ4 = ATTR_CODEC_LZ4

    def __init__(self, magic, compression_type):
        self._magic = magic
        self._compression_type = compression_type
        self._buffer = bytearray()

    def append(self, long offset, long timestamp, key, value):
        """ Append message to batch.
        """
        cdef:
            int pos
            int size
            char *buf

        # Allocate proper buffer length
        pos = PyByteArray_GET_SIZE(self._buffer)
        size = _size_in_bytes(self._magic, key, value)
        PyByteArray_Resize(self._buffer, pos + size)  # FIXME: except memory error?

        # Encode message
        buf = PyByteArray_AS_STRING(self._buffer)
        crc = _encode_msg(
            self._magic, pos, buf,
            offset, timestamp, key, value, 0)

        return crc, size

    def size(self):
        """ Return current size of data written to buffer
        """
        return PyByteArray_GET_SIZE(self._buffer)

    # Size calculations. Just copied Java's implementation

    def size_in_bytes(self, offset, timestamp, key, value):
        """ Actual size of message to add
        """
        return _size_in_bytes(self._magic, key, value)

    cdef int _maybe_compress(self) except -1:
        cdef:
            object compressed
            char *buf
            int size

        if self._compression_type != 0:
            if self._compression_type == ATTR_CODEC_GZIP:
                compressed = gzip_encode(self._buffer)
            elif self._compression_type == ATTR_CODEC_SNAPPY:
                compressed = snappy_encode(self._buffer)
            elif self._compression_type == ATTR_CODEC_LZ4:
                if self._magic == 0:
                    compressed = lz4_encode_old_kafka(bytes(self._buffer))
                else:
                    compressed = lz4_encode(bytes(self._buffer))
            size = _size_in_bytes(self._magic, key=None, value=compressed)
            # We will just write the result into the same memory space.
            PyByteArray_Resize(self._buffer, size)  # FIXME: except memory error?

            buf = PyByteArray_AS_STRING(self._buffer)
            _encode_msg(
                self._magic, 0, buf,
                offset=0, timestamp=0, key=None, value=compressed,
                attributes=self._compression_type)
            return 1
        return 0

    def build(self):
        """Compress batch to be ready for send"""
        self._maybe_compress()
        return self._buffer


cdef int _size_in_bytes(char magic, object key, object value) except -1:
    """ Actual size of message to add
    """
    cdef:
        Py_buffer buf
        int key_len
        int value_len

    if key is None:
        key_len = 0
    else:
        PyObject_GetBuffer(key, &buf, PyBUF_SIMPLE)
        key_len = buf.len
        PyBuffer_Release(&buf)

    if value is None:
        value_len = 0
    else:
        PyObject_GetBuffer(value, &buf, PyBUF_SIMPLE)
        value_len = buf.len
        PyBuffer_Release(&buf)

    if magic == 0:
        return LOG_OVERHEAD + RECORD_OVERHEAD_V0 + key_len + value_len
    else:
        return LOG_OVERHEAD + RECORD_OVERHEAD_V1 + key_len + value_len


cdef object _encode_msg(
        char magic, int start_pos, char *buf,
        long offset, long timestamp, object key, object value,
        char attributes):
    """ Encode msg data into the `msg_buffer`, which should be allocated
        to at least the size of this message.
    """
    cdef:
        Py_buffer key_val_buf
        int pos = start_pos
        int length
        object memview
        object crc

    # Write key and value
    pos += KEY_OFFSET_V0 if magic == 0 else KEY_OFFSET_V1

    if key is None:
        hton.pack_int32(&buf[pos], -1)
        pos += KEY_LENGTH
    else:
        PyObject_GetBuffer(key, &key_val_buf, PyBUF_SIMPLE)
        hton.pack_int32(&buf[pos], <int32_t>key_val_buf.len)
        pos += KEY_LENGTH
        memcpy(&buf[pos], <char*>key_val_buf.buf, <size_t>key_val_buf.len)
        pos += <int>key_val_buf.len
        PyBuffer_Release(&key_val_buf)

    if value is None:
        hton.pack_int32(&buf[pos], -1)
        pos += VALUE_LENGTH
    else:
        PyObject_GetBuffer(value, &key_val_buf, PyBUF_SIMPLE)
        hton.pack_int32(&buf[pos], <int32_t>key_val_buf.len)
        pos += VALUE_LENGTH
        memcpy(&buf[pos], <char*>key_val_buf.buf, <size_t>key_val_buf.len)
        pos += <int>key_val_buf.len
        PyBuffer_Release(&key_val_buf)
    length = (pos - start_pos) - LOG_OVERHEAD

    # Write msg header. Note, that Crc will be updated later
    hton.pack_int64(&buf[start_pos], <int64_t>offset)
    hton.pack_int32(&buf[start_pos + LENGTH_OFFSET], <int32_t>length)
    buf[start_pos + MAGIC_OFFSET] = magic
    buf[start_pos + ATTRIBUTES_OFFSET] = attributes
    if magic == 1:
        hton.pack_int64(&buf[start_pos + TIMESTAMP_OFFSET], <int64_t>timestamp)

    # Calculate CRC for msg
    memview = PyMemoryView_FromMemory(
        &buf[start_pos + MAGIC_OFFSET],
        pos - (start_pos + MAGIC_OFFSET),
        PyBUF_READ)
    crc = calc_crc32(memview)
    hton.pack_int32(&buf[start_pos + CRC_OFFSET], <uint32_t>crc)

    return crc