from dbtogo.datatypes import (
    AddCol,
    AddConstraint,
    Migration,
    RemoveConstraint,
    RenameCol,
    SQLColumn,
    SQLConstraint,
    SQLType,
)
from dbtogo.dbmodel import DBEngineFactory, DBModel


class MigrationTestModelOld(DBModel):
    pk: int | None = None
    unq_string: str = "asdf"
    nullable_int: int | None = None
    modify_me: int

    @classmethod
    def bind(cls, engine):
        super().bind(
            engine,
            primary_key="pk",
            unique=["unq_string", "modify_me"],
            table="migration_test",
        )


class MigrationTestModelNew(DBModel):
    pk: int | None = None
    unq_string: str | None = "q"
    the_same_int: int | None = None
    modify_me: int = 7
    new_col: str | None = "default"

    @classmethod
    def bind(cls, engine):
        super().bind(
            engine,
            primary_key="pk",
            unique=["unq_string"],
            table="migration_test",
        )


def test_automatic_migration():
    engine = DBEngineFactory.create_sqlite3_engine("test.db")
    MigrationTestModelOld.bind(engine)

    a = MigrationTestModelOld(unq_string="hello", modify_me=8)
    b = MigrationTestModelOld(modify_me=5, nullable_int=1)

    a.save()
    b.save()

    MigrationTestModelNew.bind(engine)
    engine._drop_table("migration_test")


def test_manual_migration():
    engine = DBEngineFactory.create_sqlite3_engine("test.db")
    MigrationTestModelOld.bind(engine)

    a = MigrationTestModelOld(unq_string="hello", modify_me=8)
    b = MigrationTestModelOld(modify_me=5, nullable_int=1)

    a.save()
    b.save()

    valid_migration_steps = [
        RenameCol("nullable_int", "the_same_int"),
        RenameCol("modify_me", "i_am_modified"),
        AddConstraint("modify_me", SQLConstraint.nullable.value),
        RemoveConstraint("modify_me", SQLConstraint.unique.value),
        AddCol(SQLColumn("new_col", SQLType.string.value, True, "default")),
    ]

    valid_migration = Migration("migration_test", valid_migration_steps)

    engine.execute_migration(valid_migration, False)

    a.delete()
    b.delete()
    engine._drop_table("migration_test")
