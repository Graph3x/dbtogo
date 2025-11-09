from pydantic import BaseModel
import abc
import sqlite3
from typing import Any

from pwdantic.exceptions import NO_BIND, INVALID_TYPES


class PWEngine(abc.ABC):

    def select(self, field: str, table: str, where: str):
        pass

    def migrate(self, schema: dict[str, Any]):
        pass


class SqliteEngine(PWEngine):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def select(
        self, field: str, table: str, condition: str, value: str
    ) -> list[Any]:
        self.cursor.execute(
            f"SELECT {field} FROM {table} WHERE {condition} = ?", (value,)
        )
        return self.cursor.fetchall()

    def _transfer_type(self, str_type: str) -> str:
        types = {
            "null": "NULL",
            "integer": "INTEGER",
            "string": "TEXT",
            "float": "REAL",
            "bytes": "BLOB",
        }

        return types[str_type]

    def _get_types(self, column: dict) -> tuple[str, str]:
        modifier = ""

        if "anyOf" in column.keys():
            if len(column["anyOf"]) > 2:
                raise INVALID_TYPES

            type1 = column["anyOf"][0]["type"]
            type2 = column["anyOf"][1]["type"]

            if type1 != "null" and type2 != "null":
                raise INVALID_TYPES

            modifier = "NULLABLE"
            str_type = (
                self._transfer_type(type1)
                if type1 != "null"
                else self._transfer_type(type2)
            )

        else:
            str_type = self._transfer_type(column["type"])

        return (str_type, modifier)

    def _create_new(self, schema: str):
        table = schema["title"]
        cols = []

        if "id" not in schema["properties"].keys():
            cols.append("id INTEGER PRIMARY KEY")

        for col_name, column in schema["properties"].items():

            str_type, modifier = self._get_types(column)

            if col_name == "id":
                modifier = "PRIMARY KEY"

            col = f"{col_name} {str_type} {modifier}"
            cols.append(col)

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ({','.join(cols)})"
        )

    def _migrate_from(self, schema: str):
        pass

    def migrate(self, schema: dict[str, Any]):
        table = schema["title"]

        matched_tables = self.cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"
        ).fetchall()

        if len(matched_tables) == 0:
            return self._create_new(schema)

        else:
            return self._migrate_from(schema)


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
    def bind(cls, db: PWEngine):
        cls.db = db
        db.migrate(cls.model_json_schema())

    @classmethod
    @binded
    def get(cls, **kwargs):
        data = cls.db.select("*", cls.__name__, "name", "*")
        print(data)

    @classmethod
    @binded
    def select(cls, field: str = "*"):
        if "db" not in dir(cls):
            raise NO_BIND

        return cls.db.select(field, cls.__name__, "1=1")
