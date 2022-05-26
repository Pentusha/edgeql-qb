from edgeql_qb.types import int64


def test_int64_repr() -> None:
    assert repr(int64(5)) == '<int64>5'
