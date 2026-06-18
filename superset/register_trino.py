from sqlalchemy.dialects import registry
registry.register("trino", "trino.sqlalchemy.dialect", "TrinoDialect")