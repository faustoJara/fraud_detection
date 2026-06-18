FROM apache/superset:4.1.1

USER root

# Instalamos usando el 'pip' global, que la imagen oficial siempre reconoce
RUN pip install --no-cache-dir \
    trino==0.336.0 \
    sqlalchemy-trino==0.5.0 \
    sqlalchemy==1.4.54

USER superset