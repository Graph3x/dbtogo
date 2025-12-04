import sqlite3
from typing import Any, Self

from pydantic import BaseModel
from pydantic.fields import ModelPrivateAttr

from dbtogo.datatypes import DBEngine, UnboundEngine
from dbtogo.exceptions import BindViolationError, NoBindError, UnboundDeleteError
from dbtogo.serialization import GeneralSQLSerializer
from dbtogo.sqlite import SqliteEngine


class DBEngineFactory:
    @staticmethod
    def create_sqlite3_engine(database: str = "") -> DBEngine:
        conn = sqlite3.connect(database)
        return SqliteEngine(conn)


class DBModel(BaseModel):
    _db: DBEngine = UnboundEngine()
    _table: str = "table_not_set"
    _primary: str = "primary_not_set"

    @classmethod
    def bind(
        cls,
        db: DBEngine,
        primary_key: str | None = None,
        unique: list[str] = [],
        table: str | None = None,
    ) -> None:
        cls._db = db
        table = table if table is not None else cls.__name__

        columns = GeneralSQLSerializer().serialize_schema(
            cls.__name__, cls.model_json_schema(), primary_key, unique
        )

        if primary_key is None:
            raise NotImplementedError("Auto primary key is not implemented yet.")

        assert primary_key is not None

        cls._primary = primary_key
        cls._table = table
        db.migrate(table, columns)

    @classmethod
    def _is_bound(cls) -> bool:
        if isinstance(cls._db, UnboundEngine):
            raise NoBindError()

        if isinstance(cls._db, ModelPrivateAttr):
            raise NoBindError()

        return True

    @classmethod
    def _deserialize_object(cls, object_data: tuple) -> Self:
        py_object = GeneralSQLSerializer().deserialize_object(cls, object_data)

        pk = getattr(py_object, py_object.__class__._primary)
        py_object._data_bind = pk

        return py_object

    @classmethod
    def get(cls, **kwargs: dict[str, Any]) -> Self | None:
        assert cls._is_bound()

        data = cls._db.select("*", cls._table, kwargs)

        if len(data) < 1:
            return None

        return cls._deserialize_object(data[0])

    def _create(self) -> None:
        obj_data = GeneralSQLSerializer().serialize_object(self)
        insert_bind = self._db.insert(self.__class__._table, obj_data)

        if getattr(self, self.__class__._primary) is None:
            setattr(self, self.__class__._primary, insert_bind)

        self._data_bind = getattr(self, self.__class__._primary)

    def _update(self) -> None:
        bind = self._data_bind
        if getattr(self, self.__class__._primary) != bind:
            raise BindViolationError()

        obj_data = GeneralSQLSerializer().serialize_object(self)
        self._db.update(self.__class__._table, obj_data, self.__class__._primary)

    def save(self) -> None:
        assert self.__class__._is_bound()

        if getattr(self, "_data_bind", None) is None:
            return self._create()

        return self._update()

    def delete(self) -> None:
        assert self.__class__._is_bound()

        if getattr(self, "_data_bind", None) is None:
            raise UnboundDeleteError()

        bind = self._data_bind
        if getattr(self, self.__class__._primary) != bind:
            raise BindViolationError()

        primary_key = self.__class__._primary
        primary_value = self._data_bind

        self._db.delete(self.__class__._table, primary_key, primary_value)
        self._data_bind = None

    @classmethod
    def all(cls) -> list[Self]:
        assert cls._is_bound()

        data = cls._db.select("*", cls._table)
        return [cls._deserialize_object(x) for x in data]
