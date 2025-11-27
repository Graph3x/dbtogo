from copy import deepcopy

from dbtogo.datatypes import (
    AddCol,
    AddConstraint,
    ChangeDefault,
    DropCol,
    Migration,
    MigrationStep,
    RemoveConstraint,
    RenameCol,
    RetypeCol,
    SQLConstraint,
)
from dbtogo.exceptions import InvalidMigrationError
from dbtogo.serialization import SQLColumn


class MigrationEngine:
    def _get_contraint_diff(
        self, old: SQLColumn, new: SQLColumn, constraint: str
    ) -> MigrationStep | None:
        if getattr(old, constraint) == getattr(new, constraint):
            return None

        if getattr(old, constraint):
            return RemoveConstraint(new.name, SQLConstraint.nullable.value)

        return AddConstraint(new.name, SQLConstraint.nullable.value)

    def get_col_diff(
        self, old_col: SQLColumn, new_col: SQLColumn
    ) -> list[MigrationStep]:
        steps: list[MigrationStep] = []

        if old_col.datatype != new_col.datatype:
            steps.append(RetypeCol(new_col.name, old_col.datatype, new_col.datatype))

        if old_col.default != new_col.default:
            steps.append(ChangeDefault(new_col.name, new_col.default))

        for constraint in ["nullable", "unique", "primary_key"]:
            step = self._get_contraint_diff(old_col, new_col, constraint)
            if step is not None:
                steps.append(step)

        return steps

    def generate_migration(
        self, table: str, original: list[SQLColumn], new: list[SQLColumn]
    ) -> Migration:
        steps = []

        current_names = [x.name for x in original]
        matched_cols = [x for x in new if x.name in current_names]

        for new_col in matched_cols:
            og_col = [x for x in original if x.name == new_col.name][0]
            steps += self.get_col_diff(og_col, new_col)

        matched_names = [x.name for x in matched_cols]

        added = [x for x in new if x.name not in matched_names]
        removed = [x for x in original if x.name not in matched_names]
        removed_sgn = [x.signature() for x in removed]

        for added_col in added:
            sgn = added_col.signature()
            if removed_sgn.count(sgn) == 1:
                rem_pos = removed_sgn.index(sgn)
                steps.append(RenameCol(removed[rem_pos].name, added_col.name))

                removed_sgn.pop(rem_pos)
                removed.pop(rem_pos)

            else:
                steps.append(AddCol(added_col))

        for to_be_dropped in removed:
            steps.append(DropCol(to_be_dropped.name))

        result = Migration(table, steps)
        result.sort()

        return result

    def get_renamed_mapping(self, migration: Migration):
        mapping = {}
        for step in migration.steps:
            if type(step) is RenameCol:
                mapping[step.old_name] = step.new_name
        return mapping

    def _execute_step(self, new_cols: dict[str, SQLColumn], step: MigrationStep):
        if type(step) is AddCol:
            new_cols[step.column.name] = step.column

        elif type(step) is DropCol:
            new_cols.pop(step.column_name)

        elif type(step) is RenameCol:
            col = new_cols.pop(step.old_name)
            col.name = step.new_name
            new_cols[col.name] = col

        elif type(step) is RetypeCol:
            new_cols[step.column_name].datatype = step.new_type

        elif type(step) is AddConstraint:
            col = new_cols[step.column_name]

            if step.constraint == SQLConstraint.nullable.value:
                col.nullable = True

            if step.constraint == SQLConstraint.unique.value:
                col.unique = True

            if step.constraint == SQLConstraint.primary.value:
                col.primary_key = True

        elif type(step) is RemoveConstraint:
            col = new_cols[step.column_name]

            if step.constraint == SQLConstraint.nullable.value:
                col.nullable = False

            if step.constraint == SQLConstraint.unique.value:
                col.unique = False

            if step.constraint == SQLConstraint.primary.value:
                col.primary_key = False

        elif type(step) is ChangeDefault:
            new_cols[step.column_name].default = step.new_default

    def get_migrated_cols(
        self, original: list[SQLColumn], migration: Migration
    ) -> list[SQLColumn]:
        new_cols = {}
        for col in original:
            new_cols[col.name] = deepcopy(col)

        migration.sort()

        for step in migration.steps:
            self._execute_step(new_cols, step)

        cols: list[SQLColumn] = list(new_cols.values())

        if len([x for x in cols if x.primary_key]) != 1:
            raise InvalidMigrationError()

        return cols
