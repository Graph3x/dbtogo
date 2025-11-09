from pydantic import BaseModel
import abc
import sqlite3
from typing import Any

from pwdantic.exceptions import NO_BIND
from pwdantic.sqlite import SqliteEngine
from pwdantic.interfaces import PWEngine

from pwdantic.serialization import GeneralSQLSerializer, SQLColumn

DEFAULT_PRIM_KEYS = ["id", "primary_key", "uuid"]


class PWEngineFactory(abc.ABC):
    @classmethod
    def create_sqlite3_engine(cls, database: str = "") -> PWEngine:
        conn = sqlite3.connect(database)
        return SqliteEngine(conn)


def binded(func):
    def wrapper(cls, *args, **kwargs):
        if "db" not in dir(cls):
            raise NO_BIND

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
                columns.append(SQLColumn(prim, int, False, None, True, True))

        db.migrate(cls.__name__, columns)

    @classmethod
    @binded
    def get(cls, **kwargs) -> list[Any]:
        data = cls.db.select("*", cls.__name__)
        return data  # TODO -> object bound to db row
    
    def __init__(self):
        self.obj_bind = None

    @binded
    def save(self):
        table = self.__class__.__name__
        obj_data = GeneralSQLSerializer().serialize_object(self)
        self.db.insert(table, obj_data)
