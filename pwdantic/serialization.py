from pwdantic.exceptions import PWInvalidTypeError
from typing import Any
import pickle
from pydantic import BaseModel
from pwdantic.datatypes import SQLColumn


class GeneralSQLSerializer:
    def _get_col_type(self, col: dict[str, str]) -> str:
        has_format = col["type"] == "string" and "format" in col.keys()
        return col["format"] if has_format else col["type"]

    def _get_column_schema(self, name: str, column: dict) -> SQLColumn:
        default = column.get("default", None)
        nullable = False

        if "anyOf" in column.keys():
            types = column["anyOf"]

            if len(types) > 2:
                raise PWInvalidTypeError()

            if types[0]["type"] != "null" and types[1]["type"] != "null":
                raise PWInvalidTypeError()

            if types[0]["type"] == "null" and types[1]["type"] == "null":
                raise PWInvalidTypeError()

            nullable = True
            column = types[0] if types[0]["type"] != "null" else types[1]

        str_type = self._get_col_type(column)

        return SQLColumn(name, str_type, nullable, default)

    def _standardise_schema_col(self, col: SQLColumn) -> SQLColumn:
        basics = ["integer", "string", "number", "boolean", "date-time"]
        if col.datatype in basics:
            return col
        
        col.datatype = "bytes"

        if col.default is not None:
            col.default = pickle.dumps(col.default)

        return col

    def serialize_schema(
        self,
        table: str,
        schema: dict,
        primary: str = None,
        unique: list[str] = [],
    ) -> list[SQLColumn]:

        if "properties" in schema.keys():
            props = schema["properties"]
        else:
            props = schema["$defs"][table]["properties"]

        cols = []
        for prop in props:
            raw_col = self._get_column_schema(prop, props[prop])
            standard_col = self._standardise_schema_col(raw_col)

            if standard_col.name == primary:
                standard_col.primary_key = True
                standard_col.nullable = False

            if standard_col.name in unique:
                standard_col.unique = True

            cols.append(standard_col)

        return cols

    def serialize_object(
        self, obj: BaseModel, no_bind: bool = False
    ) -> dict[str, Any]:
        table = obj.__class__.table
        columns = self.serialize_schema(table, obj.model_json_schema())
        obj_data = {}

        for col in columns:
            if col.datatype != "bytes":
                obj_data[col.name] = obj.__dict__.get(col.name, None)
                continue

            raw_obj = obj.__dict__.get(col.name, None)
            obj_data[col.name] = pickle.dumps(raw_obj)

        return obj_data

    def deserialize_object(
        self, cls: BaseModel, obj_data: tuple[Any]
    ) -> BaseModel:

        columns = self.serialize_schema(cls.table, cls.model_json_schema())
        values = {}

        for i, col in enumerate(columns):
            value = obj_data[i]
            if col.datatype == "bytes":
                value = pickle.loads(value)

            values[col.name] = value

        result = cls(**values)
        return result
