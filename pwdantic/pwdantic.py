from pydantic import BaseModel
import abc
import sqlite3
from typing import Any

from pwdantic.exceptions import NO_BIND
from pwdantic.sqlite import SqliteEngine
from pwdantic.interfaces import PWEngine

class PWEngineFactory(abc.ABC):
    @classmethod
    def create_sqlite3_engine(cls, database: str = "") -> PWEngine:
        conn = sqlite3.connect(database)
        return SqliteEngine(conn)


def binded(func):
    def wrapper(cls, *args, **kwargs):
        if "db" not in dir(cls):
            raise NO_BIND

        func(cls, *args, **kwargs)

    return wrapper


class PWModel(BaseModel):
    @classmethod
    def bind(
        cls,
        db: PWEngine,
        primary_key: str | None = None,
        references: dict[str, str] = {},
        unique: list[str] = [],
    ):
        cls.db = db
        db.migrate(cls.model_json_schema(), primary_key, references, unique)

    @classmethod
    @binded
    def get(cls, **kwargs):
        data = cls.db.select("*", cls.__name__, kwargs)
        return data  # TODO -> object bound to db row

    @binded
    def save(self):
        schema = self.model_json_schema()
        table = schema["title"]
        obj_data = {}

        for property in schema["properties"].keys():
            obj_data[property] = self.__dict__.get(property, None)

        self.db.insert(table, obj_data)
