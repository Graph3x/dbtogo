from pwdantic.pwdantic import PWModel, PWEngineFactory
from pwdantic.serialization import GeneralSQLSerializer
import datetime
import sqlite3


def exp():
    class Duck(PWModel):
        id: int | None = None
        name: str
        color: str = "Brown"
        age: int | None = None
        children: list["Duck"] = []

    class Duck2(PWModel):
        id: int | None = None
        name: str
        color: str = "Brown"
        age: int | None = None
        children: list["Duck"] = []

    class Duck3(PWModel):
        id: int | None = None
        name: str
        color: str = "Brown"
        age: int | None = None
        children: list[int] = []

    class Duck4(PWModel):
        id: int | None = None
        name: str
        color: str = "Brown"
        age: int | None = None
        children: int = 0

    print(Duck.model_json_schema())
    print("\n")
    print(Duck2.model_json_schema())
    print("\n")
    print(Duck3.model_json_schema())
    print("\n")
    print(Duck4.model_json_schema())


def basic_data_types():
    
    class List(PWModel):
        value: list[int] = [1, 2, 3]

    class Tuple(PWModel):
        value: tuple[int, str, int] = (1, "2", 3)

    class Bool(PWModel):
        value: bool = False

    print(Bool.model_json_schema())


def complex_object():

    class Other(PWModel):
        alpha: str = "default"
        beta: float | None
        gamma: list[int]
        delta: bytes | None = bytes([56, 43, 77])
        epsilon: datetime.datetime = datetime.datetime(2025, 10, 1, 1, 30, 12)
    class COMPL(PWModel):
        value: list[tuple[bool, int, "COMPL", Other]]
        flag: bool
        supra: set[int]
        valid: dict[int, str]

    class Duck(PWModel):
        id: int | None = None
        name: str
        color: str = "Brown"
        age: int | None = None
        children: list["Duck"] = []

    class Duck2(PWModel):
        id: int | None = None
        name: str
        color: str = "Brown"
        age: int | None = None
        children: list["Duck"] = []


    schema = COMPL.model_json_schema()
    simple_schema = Other.model_json_schema()

    d1_schema = Duck.model_json_schema()
    d2_schema = Duck2.model_json_schema()

    gss = GeneralSQLSerializer()

    sr = gss.serialize_schema(Other.__name__, simple_schema)
    print(sr)
    print()
    sr = gss.serialize_schema(COMPL.__name__, schema)
    print(sr)
    print()
    sr = gss.serialize_schema(Duck.__name__, d1_schema)
    print(sr)
    print()
    sr = gss.serialize_schema(Duck2.__name__, d2_schema)
    print(sr)


def main():
    engine = PWEngineFactory.create_sqlite3_engine("test.db")

    class Duck(PWModel):
        id: int | None = None
        name: str
        color: str = "Brown"
        age: int | None = None
        shopping_list: list = ["apple", "parizak"]

        @classmethod
        def bind(cls, engine):
            super().bind(engine, primary_key="id", unique=["name"])

    Duck.bind(engine)

    print(Duck.get(name="McDuck"))

    return

    mc_duck_junior = Duck(name="Junior", age=15)
    mc_duck_junior.save()

    print(Duck.get(name="McDuck"))


if __name__ == "__main__":
    #complex_object()
    main()
