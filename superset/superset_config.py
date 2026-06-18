import sys
# Aseguramos que las librerías instaladas en el entorno virtual estén en el path
sys.path.append("/app/.venv/lib/python3.10/site-packages")

# Forzamos el registro del dialecto de trino
from sqlalchemy.dialects import registry
registry.register("trino", "trino.sqlalchemy.dialect", "TrinoDialect")