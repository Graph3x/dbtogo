from pwdantic.serialization import SQLColumn
from pwdantic.datatypes import *
from copy import deepcopy
from pwdantic.exceptions import PWInvalidMigrationError


class MigrationEngine:
    def get_col_diff(
        self, old_col: SQLColumn, new_col: SQLColumn
    ) -> list[MigrationStep]:
        print(old_col)
        steps = []

        if old_col.datatype != new_col.datatype:
            steps.append(
                RetypeCol(new_col.name, old_col.datatype, new_col.datatype)
            )

        if old_col.default != new_col.default:
            steps.append(ChangeDefault(new_col.name, new_col.default))

        if old_col.nullable != new_col.nullable:
            if old_col.nullable:
                steps.append(
                    RemoveConstraint(
                        new_col.name, SQLConstraint.nullable.value
                    )
                )
            else:
                steps.append(
                    AddConstraint(new_col.name, SQLConstraint.nullable.value)
                )

        if old_col.unique != new_col.unique:
            if old_col.unique:
                steps.append(
                    RemoveConstraint(new_col.name, SQLConstraint.unique.value)
                )
            else:
                steps.append(
                    AddConstraint(new_col.name, SQLConstraint.unique.value)
                )

        if old_col.primary_key != new_col.primary_key:
            if old_col.primary_key:
                steps.append(
                    RemoveConstraint(
                        new_col.name, SQLConstraint.primary.value
                    )
                )
            else:
                steps.append(
                    AddConstraint(new_col.name, SQLConstraint.primary.value)
                )

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
            steps.append(DropCol(to_be_dropped))

        result = Migration(table, steps)
        result.sort()

        return result

    def get_renamed_mapping(self, migration: Migration):
        mapping = {}
        for step in migration.steps:
            if type(step) == RenameCol:
                mapping[step.old_name] = step.new_name
        return mapping

    def get_migrated_cols(
        self, original: list[SQLColumn], migration: Migration
    ) -> list[SQLColumn]:

        new_cols = [deepcopy(x) for x in original]
        migration.sort()

        for step in migration.steps:
            if type(step) == AddCol:
                new_cols.append(step.column)

            elif type(step) == DropCol:
                new_cols = filter(
                    lambda x: x.name != step.column_name, new_cols
                )

            elif type(step) == RenameCol:
                for col in new_cols:
                    if col.name == step.old_name:
                        col.name = step.new_name

            elif type(step) == RetypeCol:
                for col in new_cols:
                    if col.name == step.column_name:
                        col.datatype = step.new_type

            elif type(step) == AddConstraint:
                for col in new_cols:
                    if col.name != step.column_name:
                        continue

                    if step.constraint == SQLConstraint.nullable.value:
                        col.nullable = True
                    if step.constraint == SQLConstraint.unique.value:
                        col.unique = True
                    if step.constraint == SQLConstraint.primary.value:
                        if (
                            len(
                                filter(
                                    lambda x: type(x) == RemoveConstraint
                                    and x.constraint == SQLConstraint.primary,
                                    migration.steps,
                                )
                            )
                            != 1
                        ):
                            raise PWInvalidMigrationError()
                        col.primary_key = True

            elif type(step) == RemoveConstraint:
                for col in new_cols:
                    if col.name != step.column_name:
                        continue
                    if step.constraint == SQLConstraint.nullable:
                        col.nullable = False
                    if step.constraint == SQLConstraint.unique:
                        col.unique = False
                    if step.constraint == SQLConstraint.primary:
                        if (
                            len(
                                filter(
                                    lambda x: type(x) == AddConstraint
                                    and x.constraint == SQLConstraint.primary,
                                    migration.steps,
                                )
                            )
                            != 1
                        ):
                            raise PWInvalidMigrationError()
                        col.primary_key = False

            elif type(step) == ChangeDefault:
                for col in new_cols:
                    if col.name != step.column_name:
                        continue
                    col.default = step.new_default

        return new_cols
