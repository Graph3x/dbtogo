import sqlite3
from typing import Any, Self

from pydantic import BaseModel
from pydantic.fields import ModelPrivateAttr

from dbtogo.datatypes import DBEngine, UnboundEngine
from dbtogo.exceptions import NoBindError, UnboundDeleteError
from dbtogo.serialization import GeneralSQLSerializer
from dbtogo.sqlite import SqliteEngine


class DBEngineFactory:
    @staticmethod
    def create_sqlite3_engine(database: str = "") -> DBEngine:
        conn = sqlite3.connect(database)
        return SqliteEngine(conn)


class IdentityCache[T: "DBModel", K]:
    def __init__(self) -> None:
        self._cache: dict[K, T] = {}
        self._soft_keys: dict[K, K] = {}

    def get(self, key: K) -> T | None:
        if key is None:
            return None

        hard_result = self._cache.get(key, None)
        if hard_result is not None:
            return hard_result

        hard_key = self._soft_keys.get(key, None)
        if hard_key is None:
            return None

        ret_val = self._cache.get(hard_key, None)
        return ret_val

    def set(self, key: K, value: T) -> None:
        self._cache[key] = value

    def set_soft(self, hard_key: K, soft_key: K) -> None:
        if hard_key in self._soft_keys.keys():
            self._soft_keys[soft_key] = self._soft_keys.pop(hard_key)
            return
        self._soft_keys[soft_key] = hard_key

    def get_hard(self, key: K) -> K:
        return self._soft_keys.get(key, key)

    def remove(self, key: K) -> None:
        hard_key = self.get_hard(key)
        if hard_key != key:
            self._soft_keys.pop(key)

        self._cache.pop(hard_key)

    def __str__(self) -> str:
        return str(self._cache)


class LazyQueryList[T: "DBModel"]:
    def __init__(self, cls: type[T], objects_data: list[Any]):
        self._objects_data = objects_data
        self._cls = cls

    def __len__(self) -> int:
        return len(self._objects_data)

    def __getitem__(self, position: int) -> T:
        gss = GeneralSQLSerializer()

        current_data = self._objects_data[position]

        new_object_values = gss.partially_deserialize_object(self._cls, current_data)
        pk_value = new_object_values[self._cls._primary]

        cached_obj = self._cls._cache.get(pk_value)
        if cached_obj is not None:
            return cached_obj

        return gss.build_object(self._cls, new_object_values)


class DBModel(BaseModel):
    _db: DBEngine = UnboundEngine()
    _table: str = "table_not_set"
    _primary: str = "primary_not_set"
    _cache = IdentityCache[Self, Any]()

    @classmethod
    def bind(
        cls,
        db: DBEngine,
        primary_key: str | None = None,
        unique: list[str] = [],
        table: str | None = None,
    ) -> None:
        cls._db = db
        cls._cache = IdentityCache[Self, Any]()

        table = table if table is not None else cls.__name__

        columns = GeneralSQLSerializer().serialize_schema(
            cls.__name__, cls.model_json_schema(), primary_key, unique
        )

        if primary_key is None:
            raise NotImplementedError("Auto primary key is not implemented yet.")

        assert primary_key is not None

        cls._primary = primary_key
        cls._table = table
        db.migrate(table, columns)

    @classmethod
    def _is_bound(cls) -> bool:
        if isinstance(cls._db, UnboundEngine):
            return False

        if isinstance(cls._db, ModelPrivateAttr):
            return False

        return True

    @classmethod
    def _deserialize_object(cls, object_data: tuple) -> Self:
        py_object = GeneralSQLSerializer().deserialize_object(cls, object_data)
        return py_object

    @classmethod
    def get(cls, **kwargs: dict[str, Any]) -> Self | None:
        if not cls._is_bound():
            raise NoBindError()

        data = cls._db.select("*", cls._table, kwargs)
        if len(data) < 1:
            return None

        gss = GeneralSQLSerializer()

        new_object_values = gss.partially_deserialize_object(cls, data[0])
        pk_value = new_object_values[cls._primary]

        cached_obj = cls._cache.get(pk_value)
        if cached_obj is not None:
            return cached_obj

        return gss.build_object(cls, new_object_values)

    def __del__(self) -> None:
        pk = self.__class__._primary
        pk_value = getattr(self, pk)

        if self._cache.get(pk_value) is None:
            return

        self.__class__._cache.remove(pk_value)

    def __setattr__(self, name: str, value: Any) -> None:
        cls = self.__class__
        if cls._primary == name:
            old_pk_val = getattr(self, cls._primary)
            if old_pk_val is None:
                return super().__setattr__(name, value)

            cls._cache.set_soft(old_pk_val, value)

        return super().__setattr__(name, value)

    def _create(self) -> None:
        obj_data = GeneralSQLSerializer().serialize_object(self)
        insert_bind = self._db.insert(self.__class__._table, obj_data)

        pk = self.__class__._primary

        if getattr(self, pk) is None:
            setattr(self, pk, insert_bind)

        self.__class__._cache.set(getattr(self, pk), self)

    def _update(self) -> None:
        obj_data = GeneralSQLSerializer().serialize_object(self)
        self._db.update(self.__class__._table, obj_data, self.__class__._primary)

    def save(self) -> None:
        if not self.__class__._is_bound():
            raise NoBindError()

        pk_value = getattr(self, self.__class__._primary, None)
        cached = self.__class__._cache.get(pk_value)

        if cached is None:
            return self._create()

        assert cached is self

        return self._update()

    def delete(self) -> None:
        if not self.__class__._is_bound():
            raise NoBindError()

        pk = self.__class__._primary
        pk_value = getattr(self, pk)
        cached = self._cache.get(pk_value)

        if cached is None:
            raise UnboundDeleteError()

        self._db.delete(self.__class__._table, pk, self._cache.get_hard(pk_value))
        self.__class__._cache.remove(pk_value)

    @classmethod
    def all(cls) -> LazyQueryList[Self]:
        if not cls._is_bound():
            raise NoBindError()

        data = cls._db.select("*", cls._table)
        return LazyQueryList(cls, data)
