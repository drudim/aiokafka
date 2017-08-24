# Unit Tests for RecordBatch and Record classes

import io
import pytest

from aiokafka.record.default_records import (
    _encode_varint, _decode_varint, _calc_crc32c, RecordBatchReader,
    RecordBatchWriter
)
from aiokafka.record.legacy_records import (
    LegacyRecordBatchWriter, LegacyRecordBatchReader
)
from aiokafka.record.batch_iterator import BatchIterator

varint_data = [
    (b"\x00", 0),
    (b"\x01", -1),
    (b"\x02", 1),
    (b"\x7E", 63),
    (b"\x7F", -64),
    (b"\x80\x01", 64),
    (b"\x81\x01", -65),
    (b"\xFE\x7F", 8191),
    (b"\xFF\x7F", -8192),
    (b"\x80\x80\x01", 8192),
    (b"\x81\x80\x01", -8193),
    (b"\xFE\xFF\x7F", 1048575),
    (b"\xFF\xFF\x7F", -1048576),
    (b"\x80\x80\x80\x01", 1048576),
    (b"\x81\x80\x80\x01", -1048577),
    (b"\xFE\xFF\xFF\x7F", 134217727),
    (b"\xFF\xFF\xFF\x7F", -134217728),
    (b"\x80\x80\x80\x80\x01", 134217728),
    (b"\x81\x80\x80\x80\x01", -134217729),
    (b"\xFE\xFF\xFF\xFF\x7F", 17179869183),
    (b"\xFF\xFF\xFF\xFF\x7F", -17179869184),
    (b"\x80\x80\x80\x80\x80\x01", 17179869184),
    (b"\x81\x80\x80\x80\x80\x01", -17179869185),
    (b"\xFE\xFF\xFF\xFF\xFF\x7F", 2199023255551),
    (b"\xFF\xFF\xFF\xFF\xFF\x7F", -2199023255552),
    (b"\x80\x80\x80\x80\x80\x80\x01", 2199023255552),
    (b"\x81\x80\x80\x80\x80\x80\x01", -2199023255553),
    (b"\xFE\xFF\xFF\xFF\xFF\xFF\x7F", 281474976710655),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\x7F", -281474976710656),
    (b"\x80\x80\x80\x80\x80\x80\x80\x01", 281474976710656),
    (b"\x81\x80\x80\x80\x80\x80\x80\x01", -281474976710657),
    (b"\xFE\xFF\xFF\xFF\xFF\xFF\xFF\x7F", 36028797018963967),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F", -36028797018963968),
    (b"\x80\x80\x80\x80\x80\x80\x80\x80\x01", 36028797018963968),
    (b"\x81\x80\x80\x80\x80\x80\x80\x80\x01", -36028797018963969),
    (b"\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F", 4611686018427387903),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F", -4611686018427387904),
    (b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01", 4611686018427387904),
    (b"\x81\x80\x80\x80\x80\x80\x80\x80\x80\x01", -4611686018427387905),
]

# This is real live data from Kafka 11 broker
record_batch_data = [
    # First Batch value == "123"
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00;\x00\x00\x00\x01\x02\x03'
    b'\x18\xa2p\x00\x00\x00\x00\x00\x00\x00\x00\x01]\xff{\x06<\x00\x00\x01]'
    b'\xff{\x06<\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00'
    b'\x00\x00\x01\x12\x00\x00\x00\x01\x06123\x00',
    # Second Batch value = "" and value = "". 2 records
    b'\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00@\x00\x00\x00\x02\x02\xc8'
    b'\\\xbd#\x00\x00\x00\x00\x00\x01\x00\x00\x01]\xff|\xddl\x00\x00\x01]\xff'
    b'|\xde\x14\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00'
    b'\x00\x00\x02\x0c\x00\x00\x00\x01\x00\x00\x0e\x00\xd0\x02\x02\x01\x00'
    b'\x00',
    # Third batch value = "123"
    b'\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00;\x00\x00\x00\x02\x02.\x0b'
    b'\x85\xb7\x00\x00\x00\x00\x00\x00\x00\x00\x01]\xff|\xe7\x9d\x00\x00\x01]'
    b'\xff|\xe7\x9d\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'
    b'\x00\x00\x00\x01\x12\x00\x00\x00\x01\x06123\x00'
]

batch_expected_records = [
    (0, 1503229838908, 0, None, b'123', []),
    (0, 1503229959532, 1, None, b'', []),
    (0, 1503229959700, 2, None, b'', []),
    (0, 1503229962141, 3, None, b'123', []),
]

CODEC_MASK = 0x07
CODEC_GZIP = 0x01
CODEC_SNAPPY = 0x02
CODEC_LZ4 = 0x03


@pytest.mark.parametrize("encoded, decoded", varint_data)
def test__encode_varint(encoded, decoded):
    buffer = io.BytesIO()
    _encode_varint(buffer, decoded)
    assert buffer.getvalue() == encoded, decoded
    assert buffer.read() == b""


@pytest.mark.parametrize("encoded, decoded", varint_data)
def test__decode_varint(encoded, decoded):
    buffer = io.BytesIO(encoded)
    assert _decode_varint(buffer) == decoded, decoded
    assert buffer.read() == b""


def test__crc32c():
    def make_crc(from_bytes):
        return _calc_crc32c(from_bytes)
    assert make_crc(b"") == b"\x00\x00\x00\x00"
    assert make_crc(b"a") == b"\xc1\xd0\x43\x30"

    # Took from librdkafka testcase
    long_text = b"""\
  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the author be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution."""
    assert make_crc(long_text) == b"\x7d\xcd\xe1\x13"


@pytest.mark.parametrize("compression_type", [
    RecordBatchReader.CODEC_NONE,
    RecordBatchReader.CODEC_GZIP,
    RecordBatchReader.CODEC_SNAPPY,
    RecordBatchReader.CODEC_LZ4
])
def test_read_write_serde_v2(compression_type):
    writer = RecordBatchWriter(
        magic=2, compression_type=compression_type, is_transactional=1,
        producer_id=123456, producer_epoch=123, base_sequence=9999,
        batch_size=10100010)
    headers = [(b"header1", b"aaa"), (b"header2", b"bbb")]
    for offset in range(10):
        writer.append(
            offset, timestamp=9999999, key=b"test", value=b"Super",
            headers=headers)
    buffer = writer.close()

    reader = RecordBatchReader(buffer)
    msgs = list(reader)

    assert reader.is_transactional is True
    assert reader.compression_type == compression_type
    assert reader.magic == 2
    assert reader.timestamp_type == 0
    assert reader.base_offset == 0
    for offset, msg in enumerate(msgs):
        assert msg == (0, 9999999, offset, b"test", b"Super", headers)


@pytest.mark.parametrize("compression_type", [
    RecordBatchReader.CODEC_NONE,
    RecordBatchReader.CODEC_GZIP,
    RecordBatchReader.CODEC_SNAPPY,
    RecordBatchReader.CODEC_LZ4
])
@pytest.mark.parametrize("magic", [0, 1])
def test_read_write_legacy_serde_v0_v1(compression_type, magic):
    writer = LegacyRecordBatchWriter(
        magic=magic, compression_type=compression_type, batch_size=1000000)
    for offset in range(10):
        writer.append(
            offset, timestamp=9999999, key=b"test", value=b"Super")
    buffer = writer.close()

    reader = LegacyRecordBatchReader(buffer)
    msgs = list(reader)

    timestamp = 9999999 if magic == 1 else None
    for offset, msg in enumerate(msgs):
        assert msg == (0, timestamp, offset, b"test", b"Super", None)


def test_read_v2_stub_data():
    reader = RecordBatchReader(record_batch_data[0])
    msgs = list(reader)
    assert msgs == batch_expected_records[:1]
    reader = RecordBatchReader(record_batch_data[1])
    msgs = list(reader)
    assert msgs == batch_expected_records[1:3]
    reader = RecordBatchReader(record_batch_data[2])
    msgs = list(reader)
    assert msgs == batch_expected_records[3:]


def test_read_v2_stub_batch_of_data():
    batch_data = b"".join(record_batch_data)
    batch_iter = BatchIterator(batch_data)
    batch_iter = iter(batch_iter)
    msgs = list(next(batch_iter))
    assert msgs == batch_expected_records[:1]
    msgs = list(next(batch_iter))
    assert msgs == batch_expected_records[1:3]
    msgs = list(next(batch_iter))
    assert msgs == batch_expected_records[3:]
