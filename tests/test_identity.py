from dbtogo.dbmodel import DBEngineFactory, DBModel


class SimpleDuck(DBModel):
    pk: int = None
    name: str

    @classmethod
    def bind(cls, engine):
        super().bind(engine, "pk", table="test_identity")


def dont_test_identity():
    engine = DBEngineFactory.create_sqlite3_engine("test.db")

    SimpleDuck.bind(engine)
    duck = SimpleDuck(name="McDuck")
    duck.save()

    duck2 = SimpleDuck.get(name="McDuck")

    print(duck is duck)
    print(duck is duck2)

    duck.delete()


dont_test_identity()
