import sqlite3
from typing import Any
from pwdantic.exceptions import INVALID_TYPES
from pwdantic.interfaces import PWEngine

DEFAULT_PRIM_KEYS = ["id", "primary_key", "uuid"]


class SqliteEngine(PWEngine):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def select(
        self, field: str, table: str, conditions: dict[str, Any] | None = None
    ) -> list[Any]:

        if conditions is None:
            query = f"SELECT {field} FROM {table}"
            self.cursor.execute(query)

        else:
            where_clause = " AND ".join(
                f"{key} = ?" for key in conditions.keys()
            )
            query = f"SELECT {field} FROM {table} WHERE {where_clause}"
            self.cursor.execute(query, tuple(conditions.values()))

        return self.cursor.fetchall()

    def insert(self, table: str, obj_data: dict[str, Any]):
        cols = [col for col, val in obj_data.items() if val != None]
        vals = [val for val in obj_data.values() if val != None]

        col_str = ", ".join(cols)
        val_str = ", ".join(["?"] * len(vals))
        query = f"INSERT INTO {table} ({col_str}) VALUES({val_str})"

        self.cursor.execute(query, tuple(vals))
        self.conn.commit()

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
            modifier = "NOT NULL"

        return (str_type, modifier)

    def _create_new(
        self,
        schema: str,
        primary_key: str | None,
        references: dict[str, str],
        unique: list[str],
    ):
        table = schema["title"]
        cols = []

        if primary_key is None:
            for prim in DEFAULT_PRIM_KEYS:
                if prim not in schema["properties"].keys():
                    cols.append("{prim} INTEGER PRIMARY KEY AUTOINCREMENT")

        for col_name, column_data in schema["properties"].items():
            str_type, modifier = self._get_types(column_data)

            if col_name == primary_key:
                modifier = "PRIMARY KEY NOT NULL"

            new_col = f"{col_name} {str_type} {modifier}"

            if col_name in unique:
                new_col += " UNIQUE"

            if column_data.get("default", None) is not None:
                new_col += f" DEFAULT {column_data["default"]}"

            cols.append(new_col)

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ({','.join(cols)})"
        )

    def _migrate_from(self, schema: str):
        pass  # TODO

    def migrate(
        self,
        schema: dict[str, Any],
        primary_key: str | None,
        references: dict[str, str],
        unique: list[str],
    ):
        table = schema["title"]

        matched_tables = self.cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"
        ).fetchall()

        if len(matched_tables) == 0:
            return self._create_new(schema, primary_key, references, unique)

        else:
            return self._migrate_from(schema)
