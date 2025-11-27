from dbtogo.dbmodel import DBEngineFactory, DBModel


class Duck(DBModel):
    duck_id: int | None = None
    name: str
    color: str = "Brown"
    age: int | None = None
    shopping_list: list[str] = ["bread crumbs"]
    children: list["Duck"] = []

    @classmethod
    def bind(cls, engine):
        super().bind(engine, primary_key="duck_id", unique=["name"])

    def quack(self):
        print(f"Hi! I am {self.name} age {self.age} quack!")


def main():
    engine = DBEngineFactory.create_sqlite3_engine("test.db")

    Duck.bind(engine)

    mc_duck_junior = Duck(
        name="Junior",
        age=15,
        shopping_list=["rohlik", "gothaj"],
    )
    mc_duck_junior.save()

    mc_duck = Duck(name="McDuck", color="Yellow", age=45, children=[mc_duck_junior])
    mc_duck.save()

    mc_duck = Duck.get(name="McDuck")
    mc_duck.children[0].quack()

    mc_duck.name = "McDuckyDuck"
    mc_duck.save()

    mc_duck = Duck.get(duck_id=mc_duck.duck_id)
    print(mc_duck.name)

    print([x.name for x in Duck.all()])

    mc_duck.delete()
    mc_duck_junior.delete()


if __name__ == "__main__":
    main()
