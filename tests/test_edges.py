from dbtogo.dbmodel import DBEngineFactory, DBModel


class TestModel(DBModel):
    pk: int | None = None
    unq_string: str
    nullable_int: int | None = None
    bytes_test: bytes | None = None
    children: list["TestModel"] = []

    @classmethod
    def bind(cls, engine):
        super().bind(
            engine,
            primary_key="pk",
            unique=["unq_string"],
            table="test_edges",
        )


def test_edges():
    engine = DBEngineFactory.create_sqlite3_engine("test.db")
    TestModel.bind(engine)

    obj1 = TestModel(
        unq_string="OBJ1",
    )

    obj2 = TestModel(unq_string="OBJ2", nullable_int=5, bytes_test=b"deadbeef")

    obj1.children.append(obj1)
    obj1.save()

    obj2.save()

    obj1 = TestModel.get(unq_string="OBJ1")
    obj2 = TestModel.get(unq_string="OBJ2")

    assert len(obj1.children) != 0
    assert len(obj2.children) == 0

    obj1.delete()
    obj2.delete()

    assert len(TestModel.all()) == 0
