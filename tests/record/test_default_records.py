import pytest
from aiokafka.record.default_records import (
    DefaultRecordBatch, DefaultRecordBatchBuilder
)


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
    DefaultRecordBatch.CODEC_SNAPPY,
    DefaultRecordBatch.CODEC_LZ4
])
def test_read_write_serde_v2(compression_type):
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type, is_transactional=1,
        producer_id=123456, producer_epoch=123, base_sequence=9999)
    headers = [("header1", b"aaa"), ("header2", b"bbb")]
    for offset in range(10):
        builder.append(
            offset, timestamp=9999999, key=b"test", value=b"Super",
            headers=headers)
    buffer = builder.build()

    reader = DefaultRecordBatch(buffer.getvalue())
    msgs = list(reader)

    assert reader.is_transactional is True
    assert reader.compression_type == compression_type
    assert reader.magic == 2
    assert reader.timestamp_type == 0
    assert reader.base_offset == 0
    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == 9999999
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.headers == headers


def test_written_bytes_equals_size_in_bytes_v2():
    key = b"test"
    value = b"Super"
    headers = [("header1", b"aaa"), ("header2", b"bbb"), ("xx", None)]
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1)

    size_in_bytes = builder.size_in_bytes(
        0, timestamp=9999999, key=key, value=value, headers=headers)

    pos = builder._buffer.tell()
    builder.append(0, timestamp=9999999, key=key, value=value, headers=headers)

    assert builder._buffer.tell() - pos == size_in_bytes


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
    DefaultRecordBatch.CODEC_SNAPPY,
    DefaultRecordBatch.CODEC_LZ4
])
def test_estimate_size_in_bytes_bigger_than_batch_v2(compression_type):
    key = b"Super Key"
    value = b"1" * 100
    headers = [("header1", b"aaa"), ("header2", b"bbb")]
    estimate_size = DefaultRecordBatchBuilder.estimate_size_in_bytes(
        key, value, headers)

    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1)
    builder.append(
        0, timestamp=9999999, key=key, value=value, headers=headers)
    buf = builder.build()
    assert len(buf.getvalue()) <= estimate_size, \
        "Estimate should always be upper bound"
