from pwdantic.pwdantic import PWModel, PWEngineFactory

engine = PWEngineFactory.create_sqlite3_engine("test.db")


class Hero(PWModel):
    id: int
    name: str
    secret_name: str
    age: int | None = None


Hero.bind(engine)
Hero.get(name="asdf")
