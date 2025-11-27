import sqlite3
from typing import Any

from dbtogo.datatypes import DBEngine, SQLColumn
from dbtogo.exceptions import DestructiveMigrationError
from dbtogo.migrations import Migration, MigrationEngine

sqlite_column = tuple[int, str, str, int, Any, int]


class SQLiteEngineError(Exception):
    pass


class SqliteEngine(DBEngine):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def __del__(self) -> None:
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
            where_clause = " AND ".join(f"{key} = ?" for key in conditions.keys())
            query = f"SELECT {field} FROM {table} WHERE {where_clause}"
            self.cursor.execute(query, tuple(conditions.values()))

        return self.cursor.fetchall()

    def insert(self, table: str, obj_data: dict[str, Any]) -> int | None:
        cols = [col for col, val in obj_data.items() if val is not None]
        vals = [val for val in obj_data.values() if val is not None]

        col_str = ", ".join(cols)
        val_str = ", ".join(["?"] * len(vals))

        query = f"INSERT INTO {table} ({col_str}) VALUES({val_str})"

        self.cursor.execute(query, tuple(vals))
        self.conn.commit()
        return self.cursor.lastrowid

    def _transfer_type_from_standard(self, str_type: str) -> str:
        types = {
            "integer": "INTEGER",
            "date-time": "TIMESTAMP",
            "string": "TEXT",
            "number": "REAL",
            "boolean": "BOOLEAN",
            "bytes": "BLOB",
        }

        return types[str_type]

    def _transfer_type_to_standard(self, str_type: str) -> str:
        types = {
            "INTEGER": "integer",
            "TIMESTAMP": "date-time",
            "TEXT": "string",
            "REAL": "number",
            "BOOLEAN": "boolean",
            "BLOB": "bytes",
        }

        return types[str_type]

    def _create_table(self, tablename: str, standard_cols: list[SQLColumn]) -> None:
        sqlite_cols = []
        for column in standard_cols:
            lite_col = (
                f"{column.name} {self._transfer_type_from_standard(column.datatype)}"
            )

            if column.nullable and not column.primary_key:
                lite_col += " NULLABLE"
            else:
                lite_col += " NOT NULL"

            if column.primary_key:
                lite_col += " PRIMARY KEY AUTOINCREMENT"

            if column.unique:
                lite_col += " UNIQUE"

            if column.default is not None:
                if column.datatype == "string":
                    lite_col += (
                        f" DEFAULT '{column.default.replace("'", '').replace('"', '')}'"
                    )
                elif column.datatype != "bytes":
                    lite_col += f" DEFAULT {column.default}"
                else:
                    lite_col += f" DEFAULT {self._represent_bytes(column.default)}"

            sqlite_cols.append(lite_col)

        query = f"CREATE TABLE IF NOT EXISTS {tablename} ({','.join(sqlite_cols)})"
        self.cursor.execute(query)

        self.conn.commit()

    def _drop_table(self, table: str) -> None:
        query = f"DROP TABLE IF EXISTS {table}"
        self.cursor.execute(query)
        self.conn.commit()

    def _rename_table(self, old_table: str, new_table: str) -> None:
        query = f"ALTER TABLE {old_table} RENAME TO {new_table}"
        self.cursor.execute(query)
        self.conn.commit()

    def _parse_raw_column(self, column: str) -> SQLColumn:
        column_data: list[str] = column.split(" ")
        column_name = column_data.pop(0)
        column_type = self._transfer_type_to_standard(column_data.pop(0))

        nullable = False
        primary = False
        default = None
        unique = False

        while len(column_data) > 0:
            current = column_data.pop(0)
            match current:
                case "NOT":
                    column_data.pop(0)
                    nullable = False
                case "NULLABLE":
                    nullable = True
                case "DEFAULT":
                    default = column_data.pop(0)
                    if column_type == "string":
                        default = default.strip("'").strip('"')
                case "PRIMARY" | "KEY" | "AUTOINCREMENT":
                    primary = True
                case "UNIQUE":
                    unique = True
                case _:
                    raise SQLiteEngineError("Failed parsing current DB schema")

        return SQLColumn(column_name, column_type, nullable, default, primary, unique)

    def _get_SQLColumns(self, table: str) -> list[SQLColumn]:
        query = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}';"
        self.cursor.execute(query)
        current_schema = self.cursor.fetchone()[0]
        clean_schema: str = current_schema.split("(")[1].split(")")[0]
        raw_cols = clean_schema.split(",")

        standard_cols = []
        for raw_col in raw_cols:
            standard_cols.append(self._parse_raw_column(raw_col))

        return standard_cols

    def execute_migration(
        self,
        migration: Migration,
        force: bool = False,
        _current_cols: list[SQLColumn] | None = None,
    ) -> None:
        me = MigrationEngine()

        if len(migration.steps) < 1:
            return

        if not force and migration.is_destructive():
            raise DestructiveMigrationError()

        if _current_cols is None:
            _current_cols = self._get_SQLColumns(migration.table)

        new_cols = me.get_migrated_cols(_current_cols, migration)

        temp_table = f"_temp_migrate_{migration.table}"
        self._create_table(temp_table, new_cols)

        current = self.select("*", migration.table, None)
        val_string = ", ".join(["?"] * len(_current_cols))

        renamed = me.get_renamed_mapping(migration)
        not_dropped = [x.name for x in new_cols]
        col_str = ", ".join(
            [
                renamed.get(x.name, x.name)
                for x in _current_cols
                if renamed.get(x.name, x.name) in not_dropped
            ]
        )

        try:
            for row in current:
                query = f"INSERT INTO {temp_table} ({col_str}) VALUES({val_string})"
                self.cursor.execute(query, row)

            self.conn.commit()

        except Exception as e:
            self._drop_table(temp_table)
            raise e

        self._drop_table(migration.table)
        self._rename_table(temp_table, migration.table)

    def _migrate_from(self, table: str, new_columns: list[SQLColumn]) -> None:
        for col in new_columns:
            if col.datatype == "bytes" and col.default is not None:
                col.default = self._represent_bytes(col.default)

        current_columns = self._get_SQLColumns(table)

        migration = MigrationEngine().generate_migration(
            table, current_columns, new_columns
        )
        if len(migration.steps) > 0:
            self.execute_migration(migration)

    def migrate(self, table: str, columns: list[SQLColumn]) -> None:
        matched_tables = self.cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"
        ).fetchall()

        if len(matched_tables) == 0:
            return self._create_table(table, columns)

        else:
            return self._migrate_from(table, columns)

    def update(self, table: str, obj_data: dict[str, Any], primary_key: str) -> None:
        cols = [col for col, val in obj_data.items() if val is not None]
        vals = [val for val in obj_data.values() if val is not None]

        set_string = ""
        for col in cols:
            set_string += f"{col} = ?, "
        set_string = set_string[:-2]

        query = f"UPDATE {table} SET {set_string} WHERE {primary_key} = ?"
        vals.append(obj_data[primary_key])

        self.cursor.execute(query, tuple(vals))
        self.conn.commit()

    def delete(self, table: str, key: str, value: Any) -> None:
        query = f"DELETE FROM {table} WHERE {key} = ?"
        self.cursor.execute(query, (value,))
        self.conn.commit()
