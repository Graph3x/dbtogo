import sqlite3
from typing import Any

from pwdantic.interfaces import PWEngine
from pwdantic.serialization import SQLColumn


class SqliteEngine(PWEngine):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def __del__(self):
        self.conn.close()

    def _represent_bytes(self, data: bytes) -> str:
        return f"X'{data.hex().upper()}'"

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
            "integer": "INTEGER",
            "date-time": "TIMESTAMP",
            "string": "TEXT",
            "number": "REAL",
            "boolean": "BOOLEAN",
            "bytes": "BLOB",
        }

        return types[str_type]

    def _create_new(self, classname: str, standard_cols: list[SQLColumn]):
        sqlite_cols = []
        for column in standard_cols:
            lite_col = f"{column.name} {self._transfer_type(column.datatype)}"

            if column.nullable and not column.primary_key:
                lite_col += " NULLABLE"
            else:
                lite_col += " NOT NULL"

            if column.primary_key:
                lite_col += " PRIMARY KEY AUTOINCREMENT"

            if column.unique:
                lite_col += " UNIQUE"

            if column.default is not None:
                if column.datatype != "bytes":
                    lite_col += f" DEFAULT {column.default}"
                else:
                    lite_col += (
                        f" DEFAULT {self._represent_bytes(column.default)}"
                    )

            sqlite_cols.append(lite_col)

        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {classname} ({','.join(sqlite_cols)})"
        )

        self.conn.commit()

    def _migrate_from(self):
        pass  # TODO

    def migrate(self, table: str, columns: list[SQLColumn]):

        matched_tables = self.cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"
        ).fetchall()

        if len(matched_tables) == 0:
            return self._create_new(table, columns)

        else:
            return self._migrate_from()
