# Usamos la imagen oficial estable más reciente (2.10.3 es la actual estable recomendada)
# Si realmente necesitas la 3.x beta, cámbialo, pero esta es la segura para producción hoy.
FROM apache/airflow:2.10.3

# Cambiamos a root para instalar utilidades del sistema si hicieran falta
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Volvemos al usuario airflow para instalar tus librerías de Python
USER airflow

# Instalamos las librerías que usa tu práctica
RUN pip install --no-cache-dir \
    pandera \
    openpyxl \
    pyjwt \
    cryptography \
    pandas \
    requests \
    python-dotenv