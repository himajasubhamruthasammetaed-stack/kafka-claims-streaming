import pytest
from confluent_kafka import KafkaError, KafkaException

from claims_stream.consumer import to_records


class FakeMessage:
    def __init__(self, offset, error=None):
        self._offset, self._error = offset, error

    def error(self):
        return self._error

    def topic(self):
        return "claims.events"

    def partition(self):
        return 1

    def offset(self):
        return self._offset

    def key(self):
        return b"CLM-000001"

    def value(self):
        return b"{}"


def test_messages_become_records():
    records = to_records([FakeMessage(5), FakeMessage(6)])
    assert [(r.partition, r.offset) for r in records] == [(1, 5), (1, 6)]


def test_partition_eof_is_skipped():
    assert to_records([FakeMessage(0, KafkaError(KafkaError._PARTITION_EOF))]) == []


def test_real_errors_raise():
    with pytest.raises(KafkaException):
        to_records([FakeMessage(0, KafkaError(KafkaError._TRANSPORT))])
