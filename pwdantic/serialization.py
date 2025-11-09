from pwdantic.exceptions import INVALID_TYPES
from typing import Any
import pickle
from pydantic import BaseModel


class SQLColumn:
    def __init__(
        self,
        name: str,
        datatype: str,
        nullable: bool,
        default: Any,
        primary: bool = False,
        unique: bool = False,
    ):
        self.name: str = name
        self.datatype: str = datatype
        self.nullable: bool = nullable
        self.default: Any = default
        self.primary_key: bool = primary
        self.unique: bool = unique

    def __str__(self):
        return f"{self.name}: {("nullable " if self.nullable else "")}{("unique " if self.unique else "")}{("primary " if self.primary_key else "")}{self.datatype} ({self.default})"


class GeneralSQLSerializer:

    def _get_column_schema(self, name: str, column: dict) -> SQLColumn:
        if "anyOf" in column.keys():
            if len(column["anyOf"]) > 2:
                raise INVALID_TYPES

            type1 = column["anyOf"][0]["type"]
            type2 = column["anyOf"][1]["type"]

            if type1 != "null" and type2 != "null":
                raise INVALID_TYPES

            if type1 == "null" and type2 == "null":
                raise INVALID_TYPES

            nullable = True

            if type1 != "null":
                str_type = (
                    type1
                    if (
                        type1 != "string"
                        or column["anyOf"][0].get("format", None) is None
                    )
                    else column["anyOf"][0]["format"]
                )
            else:
                str_type = (
                    type2
                    if (
                        type2 != "string"
                        or column["anyOf"][1].get("format", None) is None
                    )
                    else column["anyOf"][1]["format"]
                )

        else:
            str_type = column["type"]
            str_type = (
                str_type
                if (
                    str_type != "string" or column.get("format", None) is None
                )
                else column["format"]
            )
            nullable = False

        default = column.get("default", None)

        return SQLColumn(name, str_type, nullable, default)

    def _standardise_schema_col(self, col: SQLColumn) -> SQLColumn:
        match col.datatype:
            case "integer" | "string" | "number" | "boolean" | "date-time":
                pass

            case "binary":
                col.datatype = "bytes"
                if col.default is not None:
                    col.default = pickle.dumps(col.default.encode("utf-8"))

            case _:
                col.datatype = "bytes"
                if col.default is not None:
                    col.default = pickle.dumps(col.default)

        return col

    def serialize_schema(
        self,
        classname: str,
        schema: dict,
        primary: str = None,
        unique: list[str] = [],
    ) -> list[SQLColumn]:

        if "properties" in schema.keys():
            props = schema["properties"]
        else:
            props = schema["$defs"][classname]["properties"]

        cols = []
        for prop in props:
            raw_col = self._get_column_schema(prop, props[prop])
            standard_col = self._standardise_schema_col(raw_col)

            if standard_col.name == primary:
                standard_col.primary_key = True

            if standard_col.name in unique:
                standard_col.unique = True

            cols.append(standard_col)

        return cols

    def serialize_object(self, obj: BaseModel, no_bind: bool = False) -> dict[str, Any]:
        table = obj.__class__.__name__
        columns = self.serialize_schema(table, obj.model_json_schema())

        obj_data = {}

        for col in columns:
            if col.datatype != "bytes":
                obj_data[col.name] = obj.__dict__.get(col.name, None)
                continue

            raw_obj = obj.__dict__.get(col.name, None)
            obj_data[col.name] = pickle.dumps(raw_obj)

        if no_bind:
            return obj_data
        
        return obj_data

    def deserialize_object(
        self, columns: list[SQLColumn], obj_data: str[Any]
    ) -> BaseModel:
        pass
