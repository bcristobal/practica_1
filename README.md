# 🚗 Práctica de Integración: Pipeline de Datos Tráfico-Meteorología

Este proyecto implementa un **pipeline de datos ETL (Extract, Transform, Load)** que integra información heterogénea procedente de dos fuentes oficiales del Gobierno Vasco: la **Dirección de Tráfico** y la **Agencia Vasca de Meteorología (Euskalmet)**.

El objetivo es generar un dataset analítico que permita estudiar la correlación entre las condiciones climáticas y los accidentes de tráfico.

---

## 📋 Características del Proyecto

* **Arquitectura:** ETL Batch (Procesamiento mensual).
* **Fuentes de Datos:**
    * **API Tráfico (OpenData Euskadi):** Incidencias en tiempo real e históricas (JSON).
    * **API Euskalmet:** Predicciones y datos meteorológicos por zonas (JSON).
    * **Mapeo Local:** Archivo CSV (`mapping_municipios.csv`) para normalizar y cruzar ubicaciones.
* **Transformación y Calidad:**
    * Normalización de nombres de municipios.
    * Filtrado de incidencias (solo tipo "Accidente").
    * Validación de esquema y tipos de datos con **Pandera**.
* **DataOps:** Entorno contenerizado con **Docker** para garantizar la reproducibilidad.
* **Salida:** Reporte consolidado en formato Excel (`.xlsx`).

---

## 📂 Estructura del Proyecto

```text
practica_1/
├── Apikey/
│   ├── privateKey.pem       # (NECESARIO) Tu clave privada de Euskalmet
│   └── publicKey.pem        # Tu clave pública
├── main.py                  # Script principal del pipeline ETL
├── generate_mapping.py      # Herramienta auxiliar para generar el CSV de zonas
├── mapping_municipios.csv   # Maestro de datos para cruzar Tráfico y Clima
├── Dockerfile               # Definición de la imagen del contenedor
├── docker-compose.yml       # Orquestación del servicio
├── pyproject.toml / uv.lock # Gestión de dependencias (uv)
└── .env                     # Variables de entorno (Email, rutas)
```

---

## 🚀 Requisitos Previos

Para ejecutar este proyecto necesitas tener instalado:

1.  **Docker** y **Docker Compose**.
2.  Un par de claves (pública/privada) válidas para la API de Euskalmet.

-----

## 🛠️ Instrucciones de Ejecución (DataOps)

La forma recomendada de ejecutar el pipeline es mediante contenedores, asegurando que el entorno es idéntico al de desarrollo.

### 1\. Configuración de Credenciales

Asegúrate de colocar tu clave privada en la carpeta `Apikey`:

  * Ruta: `practica_1/Apikey/privateKey.pem`

### 2\. Configuración del Periodo (Opcional)

Por defecto, el script procesa un mes específico. Si deseas cambiar las fechas, edita las constantes en `main.py`:

```python
START_DATE = datetime(2025, 11, 1)
END_DATE = datetime(2025, 11, 30)
```

### 3\. Ejecución con Docker Compose

Abre una terminal en la carpeta del proyecto y ejecuta:

```bash
docker-compose up --build
```

**¿Qué sucederá?**

1.  Docker construirá la imagen con Python 3.13 y las librerías necesarias (`pandas`, `pandera`, `requests`, `pyjwt`, etc.).
2.  Se iniciará el contenedor `practica1_pipeline`.
3.  El script comenzará a descargar y procesar los datos día a día.
4.  Al finalizar, verás un mensaje de éxito y el contenedor se detendrá.

### 4\. Resultados

El reporte final aparecerá automáticamente en tu carpeta local (gracias al volumen montado en docker-compose) con el nombre:

  * 📄 `reporte_mensual_YYYYMM.xlsx`

-----

## ⚙️ Ejecución Manual (Local)

Si prefieres ejecutarlo sin Docker, necesitarás Python 3.13 y el gestor `uv` (o pip).

1.  Instalar dependencias:
    ```bash
    uv sync
    ```
2.  Ejecutar el script principal:
    ```bash
    uv run main.py
    ```
3.  (Opcional) Regenerar el archivo de mapeo si fuera necesario:
    ```bash
    uv run generate_mapping.py
    ```

-----

## ✅ Calidad del Dato

El proyecto utiliza la librería **Pandera** para validar la integridad de los datos antes de guardarlos. Se comprueba:

  * Que el campo `cityTown` (Municipio) no esté vacío.
  * Que el `incidenceType` sea estrictamente "Accidente".
  * Que las temperaturas (`temp_media`) estén dentro de rangos físicos lógicos (-15ºC a 50ºC).
  * Que existan fechas válidas.

-----

## 📝 Notas sobre la Optimización

El pipeline implementa una **optimización de consultas**:
En lugar de solicitar el clima de todos los municipios vascos cada día, el script identifica primero dónde ocurrieron los accidentes diarios y solicita información meteorológica **exclusivamente** para esas ubicaciones, reduciendo drásticamente el tiempo de ejecución y la carga sobre la API.