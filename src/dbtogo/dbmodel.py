import sqlite3
from typing import Self

from pydantic import BaseModel

from dbtogo.datatypes import DBEngine, SQLColumn, UnboundEngine
from dbtogo.exceptions import BindViolationError, NoBindError, UnboundDeleteError
from dbtogo.serialization import GeneralSQLSerializer
from dbtogo.sqlite import SqliteEngine

DEFAULT_PRIM_KEYS = ["id", "primary_key", "uuid"]


class DBEngineFactory:
    @staticmethod
    def create_sqlite3_engine(database: str = "") -> DBEngine:
        conn = sqlite3.connect(database)
        return SqliteEngine(conn)


def bound(func):
    def wrapper(cls: "DBModel", *args, **kwargs):
        if cls._db is None:
            raise NoBindError()

        return func(cls, *args, **kwargs)

    return wrapper


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
    ):
        cls._db = db
        table = table if table is not None else cls.__name__

        columns = GeneralSQLSerializer().serialize_schema(
            cls.__name__, cls.model_json_schema(), primary_key, unique
        )

        used_names = [x.name for x in columns]
        if primary_key is None:
            for prim in DEFAULT_PRIM_KEYS:
                if prim in used_names:
                    continue
                primary_key = prim
                columns.append(SQLColumn(prim, "int", False, None, True, True))

        assert(primary_key is not None)

        cls._primary = primary_key
        cls._table = table
        db.migrate(table, columns)

    @classmethod
    def _deserialize_object(cls, object_data: tuple) -> Self:
        py_object = GeneralSQLSerializer().deserialize_object(cls, object_data)

        pk = getattr(py_object, py_object.__class__._primary)
        py_object._data_bind = pk

        return py_object

    @classmethod
    @bound
    def get(cls, **kwargs) -> Self | None:
        data = cls._db.select("*", cls._table, kwargs)

        if len(data) < 1:
            return None

        return cls._deserialize_object(data[0])

    def _create(self):
        obj_data = GeneralSQLSerializer().serialize_object(self)

        insert_bind = self._db.insert(self.__class__._table, obj_data)
        bind_attr = getattr(self, self.__class__._primary)

        data_bind = bind_attr if bind_attr is not None else insert_bind
        self._data_bind = data_bind

    def _update(self):
        bind = self._data_bind

        if getattr(self, self.__class__._primary) != bind:
            raise BindViolationError()

        obj_data = GeneralSQLSerializer().serialize_object(self)
        self._db.update(
            self.__class__._table, obj_data, self.__class__._primary
        )

    @bound
    def save(self):
        if getattr(self, "_data_bind", None) is None:
            return self._create()
        return self._update()

    @bound
    def delete(self):
        if getattr(self, "_data_bind", None) is None:
            raise UnboundDeleteError()

        primary_key = self.__class__._primary
        primary_value = self._data_bind

        self._db.delete(self.__class__._table, primary_key, primary_value)
        self._data_bind = None

    @classmethod
    @bound
    def all(cls) -> list[Self]:
        data = cls._db.select("*", cls._table)
        return [cls._deserialize_object(x) for x in data]
