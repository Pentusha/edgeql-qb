from edgeql_qb.frozendict import FrozenDict


def test_frozendict_len() -> None:
    assert len(FrozenDict()) == 0
    assert len(FrozenDict(a=1, b=2)) == 2


def test_frozendict_hash() -> None:
    assert hash(FrozenDict()) == hash(FrozenDict())
    assert hash(FrozenDict(a=1)) == hash(FrozenDict(a=1))
