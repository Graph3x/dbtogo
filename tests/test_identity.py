from dbtogo.dbmodel import DBEngineFactory, DBModel


class SimpleDuck(DBModel):
    pk: int = None
    name: str

    @classmethod
    def bind(cls, engine):
        super().bind(engine, "pk", table="test_identity")


def test_identity():
    engine = DBEngineFactory.create_sqlite3_engine("test.db")

    SimpleDuck.bind(engine)
    duck = SimpleDuck(name="Duck")
    duck.save()

    duck2 = SimpleDuck.get(name="Duck")

    assert duck is duck2

    duck.pk = 67

    duck3 = SimpleDuck.get(name="Duck")
    assert duck is duck3

    duck.save()

    duck4 = SimpleDuck.get(name="Duck")
    assert duck is duck4

    ducks = SimpleDuck.all()

    assert len(ducks) == 1
    assert ducks[0] is duck

    duck.delete()

    assert len(SimpleDuck.all()) == 0


if __name__ == "__main__":
    test_identity()
