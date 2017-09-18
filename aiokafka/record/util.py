import binascii

from ._crc32c import crc as crc32c_py


def encode_varint(num):
    """ Encode an integer to a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            num (int): Value to encode

        Returns:
            bytearray: Encoded presentation of integer with length from 1 to 10
                 bytes
    """
    # Shift sign to the end of number
    num = (num << 1) ^ (num >> 63)
    # Max 10 bytes. We assert those are allocated
    buf = bytearray(10)

    for i in range(10):
        # 7 lowest bits from the number and set 8th if we still have pending
        # bits left to encode
        buf[i] = num & 0x7f | (0x80 if num > 0x7f else 0)
        num = num >> 7
        if num == 0:
            break
    else:
        # Max size of endcoded double is 10 bytes for unsigned values
        raise ValueError("Out of double range")
    return buf[:i + 1]


def size_of_varint(num):
    """ Number of bytes needed to encode an integer in variable-length format.
    """
    num = (num << 1) ^ (num >> 63)
    res = 0
    while True:
        res += 1
        num = num >> 7
        if num == 0:
            break
    return res


def decode_varint(buffer, pos=0):
    """ Decode an integer from a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            buffer (bytes-like): any object acceptable by ``memoryview``
            pos (int): optional position to read from

        Returns:
            (int, int): Decoded int value and next read position
    """
    value = 0
    shift = 0
    memview = memoryview(buffer)
    for i in range(pos, pos + 10):
        try:
            byte = memview[i]
        except IndexError:
            raise ValueError("End of byte stream")
        if byte & 0x80 != 0:
            value |= (byte & 0x7f) << shift
            shift += 7
        else:
            value |= byte << shift
            break
    else:
        # Max size of endcoded double is 10 bytes for unsigned values
        raise ValueError("Out of double range")
    # Normalize sign
    return (value >> 1) ^ -(value & 1), i + 1


def calc_crc32c(memview):
    """ Calculate CRC-32C (Castagnoli) checksum over a memoryview of data
    """
    crc = crc32c_py(memview)
    return crc


def calc_crc32(memview):
    """ Calculate simple CRC-32 checksum over a memoryview of data
    """
    crc = binascii.crc32(memview) & 0xffffffff
    return crc
