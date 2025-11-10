class PWNoBindError(Exception):
    def __init__(self):
        super().__init__(
            "You need to bind this model to a db before first use"
        )


class PWInvalidTypeError(Exception):
    def __init__(self):
        super().__init__("Invalid types in your schema")


class PWBindViolationError(Exception):
    def __init__(self):
        super().__init__(
            "You cannot change the primary key of a bound object"
        )


class PWUnboundDeleteError(Exception):
    def __init__(self):
        super().__init__("You cannot delete an object that is not in the db")
