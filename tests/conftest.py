from contextlib import suppress
from typing import AsyncIterator, Iterator

import pytest
from edgedb.asyncio_client import AsyncIOClient, create_async_client
from edgedb.blocking_client import Client, create_client


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


@pytest.fixture
async def async_client() -> AsyncIterator[AsyncIOClient]:
    client = create_async_client()
    with suppress(Rollback):
        async for tx in client.transaction():
            async with tx:
                yield tx
                raise Rollback
