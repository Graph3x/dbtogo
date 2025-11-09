from pwdantic.pwdantic import PWModel, PWEngineFactory

engine = PWEngineFactory.create_sqlite3_engine("test.db")


class Duck(PWModel):
    id: int | None = None
    name: str
    color: str
    age: int | None = None


Duck.bind(engine)
Duck.get(name="McDuck")

mc_duck = Duck(name="McDuck", color="Yellow", age=42)
mc_duck.save()