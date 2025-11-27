import abc
from enum import Enum
from typing import Any

from dbtogo.exceptions import NoBindError


class SQLType(Enum):
    integer = "integer"
    date_time = "date-time"
    string = "string"
    number = "number"
    boolean = "boolean"
    byte_data = "bytes"


class SQLConstraint(Enum):
    primary = "primary"
    nullable = "nullable"
    unique = "unique"


class SQLColumn:
    def __init__(
        self,
        name: str,
        datatype: str,
        nullable: bool,
        default: Any,
        primary: bool = False,
        unique: bool = False,
    ):
        self.name: str = name
        self.datatype: str = datatype
        self.nullable: bool = nullable
        self.default: Any = default
        self.primary_key: bool = primary
        self.unique: bool = unique

    def __str__(self) -> str:
        representation = f"{self.name}: "
        representation += "nullable " if self.nullable else ""
        representation += "unique " if self.unique else ""
        representation += "primary " if self.primary_key else ""
        representation += f"{self.datatype} ({self.default})"
        return representation

    def signature(self) -> str:
        str_signature = f"{self.datatype}{self.nullable}{self.default}"
        str_signature += f"{self.primary_key}{self.unique}"
        return str_signature


class MigrationStep:
    _destructive: bool = False


class AddCol(MigrationStep):
    def __init__(self, column: SQLColumn):
        self.column = column

    def __str__(self) -> str:
        return f"ADD {self.column.name}"


class DropCol(MigrationStep):
    def __init__(self, column_name: str):
        self.column_name = column_name
        self._destructive = True

    def __str__(self) -> str:
        return f"DROP {self.column_name}"


class RenameCol(MigrationStep):
    def __init__(self, old_name: str, new_name: str):
        self.old_name = old_name
        self.new_name = new_name

    def __str__(self) -> str:
        return f"RENAME {self.old_name} to {self.new_name}"


class RetypeCol(MigrationStep):
    def __init__(self, column_name: str, old_type: str, new_type: str):
        self.column_name = column_name
        self.old_type = old_type
        self.new_type = new_type
        self._destructive = True

    def __str__(self) -> str:
        return f"RETYPE {self.column_name} from {self.old_type} to {self.new_type}"


class AddConstraint(MigrationStep):
    def __init__(self, column_name: str, constraint: str):
        self.column_name = column_name
        self.constraint = constraint

        if constraint == SQLConstraint.primary.value:
            self._destructive = True

    def __str__(self) -> str:
        return f"ADD {self.constraint} to {self.column_name}"


class RemoveConstraint(MigrationStep):
    def __init__(self, column_name: str, constraint: str):
        self.column_name = column_name
        self.constraint = constraint

        if constraint == SQLConstraint.primary.value:
            self._destructive = True

    def __str__(self) -> str:
        return f"REMOVE {self.constraint} from {self.column_name}"


class ChangeDefault(MigrationStep):
    def __init__(self, column_name: str, new: Any):
        self.column_name = column_name
        self.new_default = new

    def __str__(self) -> str:
        return f"DEFAULT {self.column_name} to {self.new_default}"


class Migration:
    def __init__(self, table: str, steps: list[MigrationStep]):
        self.table = table
        self.steps = steps

    def is_destructive(self) -> bool:
        return len([x for x in self.steps if x._destructive]) > 0

    @staticmethod
    def _step_key_function(step: MigrationStep) -> int:
        if type(step) is AddCol:
            return 1

        elif type(step) is RetypeCol:
            return 3

        elif type(step) is AddConstraint:
            return 3

        elif type(step) is RemoveConstraint:
            return 3

        elif type(step) is DropCol:
            return 4

        elif type(step) is RenameCol:
            return 5

        return 0

    def sort(self) -> None:
        self.steps.sort(key=self._step_key_function)


class DBEngine(abc.ABC):
    @abc.abstractmethod
    def select(
        self, field: str, table: str, conditions: dict[str, Any] | None = None
    ) -> list[Any]:
        pass

    @abc.abstractmethod
    def insert(self, table: str, obj_data: dict[str, Any]) -> int | None:
        pass

    @abc.abstractmethod
    def migrate(self, table: str, columns: list[SQLColumn]) -> None:
        pass

    @abc.abstractmethod
    def update(self, table: str, obj_data: dict[str, Any], primary_key: str) -> None:
        pass

    @abc.abstractmethod
    def delete(self, table: str, key: str, value: Any) -> None:
        pass

    @abc.abstractmethod
    def execute_migration(self, migration: Migration, force: bool = False) -> None:
        pass


class UnboundEngine(DBEngine):
    def select(
        self, field: str, table: str, conditions: dict[str, Any] | None = None
    ) -> list[Any]:
        raise NoBindError()

    def insert(self, table: str, obj_data: dict[str, Any]) -> int | None:
        raise NoBindError()

    def migrate(self, table: str, columns: list[SQLColumn]) -> None:
        raise NoBindError()

    def update(self, table: str, obj_data: dict[str, Any], primary_key: str) -> None:
        raise NoBindError()

    def delete(self, table: str, key: str, value: Any) -> None:
        raise NoBindError()

    def execute_migration(self, migration: Migration, force: bool = False) -> None:
        raise NoBindError()
