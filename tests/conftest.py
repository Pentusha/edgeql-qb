from collections.abc import Iterator
from contextlib import suppress

import pytest
from edgedb.blocking_client import Iteration, create_client


class RollbackError(Exception):
    pass


@pytest.fixture
def client() -> Iterator[Iteration]:
    client = create_client()
    with suppress(RollbackError):
        for tx in client.transaction():
            with tx:
                yield tx
                raise RollbackError
