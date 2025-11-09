import abc
from typing import Any

class PWEngine(abc.ABC):

    def select(
        self, field: str, table: str, conditions: dict[str, Any] | None = None
    ) -> list[Any]:
        pass

    def insert(self, table: str, data: list[tuple]):
        pass

    def migrate(self, schema: dict[str, Any]):
        pass
