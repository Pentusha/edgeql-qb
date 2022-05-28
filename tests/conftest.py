from contextlib import suppress
from typing import Iterator

import pytest
from edgedb.blocking_client import create_client, Client


class Rollback(Exception):
    pass


@pytest.fixture
def client() -> Iterator[Client]:
    client = create_client()
    with suppress(Rollback):
        for tx in client.transaction():
            with tx:
                yield tx
                raise Rollback
