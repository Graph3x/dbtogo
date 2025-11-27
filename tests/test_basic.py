from dbtogo.dbmodel import DBEngineFactory, DBModel


class TestModel(DBModel):
    pk: int | None = None
    unq_string: str
    nullable_int: int | None = None
    bytes_test: bytes | None = None

    @classmethod
    def bind(cls, engine):
        super().bind(
            engine,
            primary_key="pk",
            unique=["unq_string"],
            table="test_basic",
        )


def test_crud():
    engine = DBEngineFactory.create_sqlite3_engine("test.db")
    TestModel.bind(engine)

    obj1 = TestModel(
        unq_string="OBJ1",
    )
    obj1.save()

    obj2 = TestModel(unq_string="OBJ2", nullable_int=5, bytes_test=b"deadbeef")
    obj2.save()

    obj1 = TestModel.get(unq_string="OBJ1")
    obj2 = TestModel.get(unq_string="OBJ2")

    assert obj2.bytes_test == b"deadbeef"

    assert obj1.unq_string == "OBJ1"
    assert obj2.unq_string == "OBJ2"

    assert obj1.pk is not None
    assert obj2.pk is not None

    assert obj1.nullable_int is None
    assert obj2.nullable_int == 5

    obj1.unq_string = "New_string"
    obj1.save()

    obj1 = TestModel.get(pk=obj1.pk)
    assert obj1.unq_string == "New_string"

    assert len(TestModel.all()) == 2

    obj1.delete()
    obj2.delete()

    assert len(TestModel.all()) == 0
