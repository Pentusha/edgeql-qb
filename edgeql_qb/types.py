from typing import Generic, TypeVar

T = TypeVar('T')


class GenericHolder(Generic[T]):
    edgeql_name: str

    def __init__(self, value: T):
        self.value = value

    def __repr__(self) -> str:
        return f'<{self.edgeql_name}>{self.value}'


class int16(GenericHolder[int]):
    edgeql_name = 'int16'


class int32(GenericHolder[int]):
    edgeql_name = 'int32'


class int64(GenericHolder[int]):
    edgeql_name = 'int64'


class bigint(GenericHolder[int]):
    edgeql_name = 'bigint'


class float32(GenericHolder[float]):
    edgeql_name = 'float32'


class float64(GenericHolder[float]):
    edgeql_name = 'float64'
