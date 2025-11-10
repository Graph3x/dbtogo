from pydantic import BaseModel
import abc
import sqlite3
from typing import Any, Self

from pwdantic.exceptions import *
from pwdantic.sqlite import SqliteEngine
from pwdantic.interfaces import PWEngine

from pwdantic.serialization import GeneralSQLSerializer, SQLColumn

DEFAULT_PRIM_KEYS = ["id", "primary_key", "uuid"]


class PWEngineFactory(abc.ABC):
    @classmethod
    def create_sqlite3_engine(cls, database: str = "") -> PWEngine:
        conn = sqlite3.connect(database)
        return SqliteEngine(conn)


def bound(func):
    def wrapper(cls, *args, **kwargs):
        if "db" not in dir(cls):
            raise PWNoBindError()

        return func(cls, *args, **kwargs)

    return wrapper


class PWModel(BaseModel):
    @classmethod
    def bind(
        cls,
        db: PWEngine,
        primary_key: str | None = None,
        unique: list[str] = [],
    ):
        cls.db = db

        columns = GeneralSQLSerializer().serialize_schema(
            cls.__name__, cls.model_json_schema(), primary_key, unique
        )

        if primary_key is None:
            for prim in DEFAULT_PRIM_KEYS:
                if prim in [x.name for x in columns]:
                    continue
                primary_key = prim
                columns.append(SQLColumn(prim, int, False, None, True, True))

        cls._primary = primary_key
        db.migrate(cls.__name__, columns)

    @classmethod
    @bound
    def get(cls, **kwargs) -> Self:
        data = cls.db.select("*", cls.__name__, kwargs)
        if len(data) < 1:
            return None
        object = GeneralSQLSerializer().deserialize_object(cls, data[0])
        setattr(
            object, "_data_bind", getattr(object, object.__class__._primary)
        )
        return object

    def _create(self):
        table = self.__class__.__name__
        obj_data = GeneralSQLSerializer().serialize_object(self)

        insert_bind = self.db.insert(table, obj_data)
        bind_attr = getattr(self, self.__class__._primary)
        data_bind = bind_attr if bind_attr != None else insert_bind
        setattr(self, "_data_bind", data_bind)

    def _update(self):
        bind = self._data_bind
        if getattr(self, self.__class__._primary) != bind:
            raise PWBindViolationError()

        table = self.__class__.__name__

        obj_data = GeneralSQLSerializer().serialize_object(self)

        self.db.update(table, obj_data, self.__class__._primary)

    @bound
    def save(self):
        if getattr(self, "_data_bind", None) is None:
            return self._create()
        return self._update()

    @bound
    def delete(self):
        if getattr(self, "_data_bind", None) is None:
            raise PWUnboundDeleteError()

        table = self.__class__.__name__
        primary_key = self.__class__._primary
        primary_value = self._data_bind

        self.db.delete(table, primary_key, primary_value)
        self._data_bind = None
