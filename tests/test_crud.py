import pytest

from dbtogo.dbmodel import DBEngineFactory, DBModel
from dbtogo.exceptions import NoBindError, UnboundDeleteError


class CryptoDuck(DBModel):
    pk: int | None = None
    name: str
    cash: int | None = 0
    bitcoin_wallet: bytes | None = None

    @classmethod
    def bind(cls, engine):
        super().bind(
            engine,
            primary_key="pk",
            unique=["name"],
            table="test_crud",
        )


class SimpleDuck(DBModel):
    name: str

    @classmethod
    def bind(cls, engine):
        super().bind(engine, table="test_crud_simple")


def test_crud():
    engine = DBEngineFactory.create_sqlite3_engine("test.db")

    with pytest.raises(NotImplementedError):
        SimpleDuck.bind(engine)

    bad_duck = CryptoDuck(name="Bad")

    with pytest.raises(NoBindError):
        CryptoDuck.get(name="a")

    with pytest.raises(NoBindError):
        CryptoDuck.all()

    with pytest.raises(NoBindError):
        bad_duck.save()

    with pytest.raises(NoBindError):
        bad_duck.delete()

    CryptoDuck.bind(engine)

    with pytest.raises(UnboundDeleteError):
        bad_duck.delete()

    duck = CryptoDuck(name="McDuck")
    duck.save()

    assert duck.name == "McDuck"
    assert duck.bitcoin_wallet is None
    assert duck.cash == 0

    db_duck = CryptoDuck.get(name="McDuck")

    assert db_duck is duck

    assert len(CryptoDuck.all()) == 1

    duck.cash = 100
    duck.save()

    assert duck.name == "McDuck"
    assert duck.bitcoin_wallet is None
    assert duck.cash == 100

    db_duck = CryptoDuck.get(name="McDuck")

    assert db_duck is not None
    assert db_duck.name == "McDuck"
    assert db_duck.bitcoin_wallet is None
    assert db_duck.cash == 100

    assert len(CryptoDuck.all()) == 1

    duck.name = "KentuckyFriedDuck"
    duck.save()

    assert duck.name == "KentuckyFriedDuck"
    assert duck.bitcoin_wallet is None
    assert duck.cash == 100

    db_duck = CryptoDuck.get(name="KentuckyFriedDuck")

    assert db_duck is not None
    assert db_duck.name == "KentuckyFriedDuck"
    assert db_duck.bitcoin_wallet is None
    assert db_duck.cash == 100

    assert len(CryptoDuck.all()) == 1

    duck.delete()

    db_duck = CryptoDuck.get(name="McDuck")
    assert db_duck is None

    assert len(CryptoDuck.all()) == 0
