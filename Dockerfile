# 1. Imagen base de Python 3.13 (versión ligera 'slim')
FROM python:3.13-slim

# 2. Instalamos 'uv' desde su imagen oficial (más rápido que pip)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 3. Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# 4. Copiamos los archivos de definición de dependencias primero
# Esto permite a Docker cachear las librerías si no has cambiado el uv.lock
COPY pyproject.toml uv.lock ./

# 5. Instalamos las dependencias
# --frozen: asegura que se usen las versiones exactas del lockfile
RUN uv sync --frozen

# 6. Copiamos el resto del código (scripts, carpeta Apikey, csv, etc.)
COPY . .

# 7. Comando por defecto al ejecutar el contenedor
CMD ["uv", "run", "main.py"]