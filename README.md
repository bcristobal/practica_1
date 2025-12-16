# 🚗 Práctica de Integración: Pipeline DataOps con Apache Airflow

Este proyecto implementa una plataforma **ETL (Extract, Transform, Load)** orquestada profesionalmente para integrar datos de tráfico y meteorología del País Vasco.

El objetivo es generar un dataset analítico que permita estudiar la correlación entre accidentes y clima, utilizando una arquitectura moderna basada en **microservicios (Docker)** y **Apache Airflow**.

---

## 📋 Características del Proyecto

* **Arquitectura:** DataOps basada en Contenedores.
* **Orquestación:** **Apache Airflow 2.10** gestionando el ciclo de vida del dato.
* **Fuentes de Datos:**
    * **API Tráfico (OpenData Euskadi):** Incidencias reales.
    * **API Euskalmet:** Predicciones meteorológicas.
    * **Mapeo Local:** CSV para normalización de zonas.
* **Capacidades Avanzadas:**
    * **Backfilling:** Carga automática de datos históricos (mes completo) al activar el pipeline.
    * **Optimización:** Consultas meteorológicas inteligentes (solo zonas afectadas).
    * **Calidad del Dato:** Validación de esquemas con **Pandera**.
    * **Resiliencia:** Reintentos automáticos ante fallos de red.

---

## 📂 Estructura del Proyecto

La estructura sigue el estándar de Airflow en Docker:

```text
practica_1/
├── dags/                  # Lógica del Pipeline (Código Python)
│   └── etl_trafico.py     # DAG que define el flujo ETL
├── data/                  # Insumos estáticos
│   └── mapping_municipios.csv  # Maestro de datos para cruces
├── output/                # RESULTADOS (Aquí aparecen los Excel generados)
├── Apikey/                # Secretos
│   └── privateKey.pem     # (NECESARIO) Clave privada RSA
├── Dockerfile             # Imagen personalizada de Airflow
├── docker-compose.yaml    # Definición de la infraestructura (Webserver, Scheduler, DB)
├── logs/                  # Logs de ejecución (generado automáticamente)
├── plugins/               # Plugins de Airflow
└── .env                   # Variables de entorno
```

---

## 🚀 Requisitos Previos* **Docker** y **Docker Compose** instalados.
* Clave privada (`privateKey.pem`) colocada en la carpeta `Apikey/`. Puedes generar una desde [opendata.euskadi.eus](https://opendata.euskadi.eus/euskalmet-api/-/api-de-euskalmet/).

---

## 🛠️ Instrucciones de Despliegue (DataOps)Sigue estos pasos estrictos para levantar la plataforma sin errores de permisos.

### 1. Preparación del EntornoDesde la terminal en la carpeta raíz del proyecto (`practica_1`), asegúrate de que las carpetas de trabajo tengan los permisos correctos para que Docker pueda escribir en ellas (especialmente en Linux/Mac/WSL):

```bash
# Crear carpetas si no existen
mkdir -p logs plugins output data

# Asignar permisos de escritura (CRÍTICO para evitar errores de PermissionDenied)
sudo chmod -R 777 dags logs plugins output data Apikey

```

### 2. Construcción y ArranqueLevanta la infraestructura completa (Scheduler, Webserver, Postgres e Inicializador):

```bash
sudo docker compose up --build

```

*Espera unos instantes hasta que veas en los logs que el `airflow-webserver` está escuchando.*

---

## 🖥️ Uso de la Plataforma
👉 **http://localhost:8080**

* **Usuario:** `admin`
* **Contraseña:** `admin`

Los archivos generados aparecerán en tu carpeta local `output/` con el formato:
📄 `reporte_diario_YYYYMMDD.xlsx`

---

## ✅ Calidad y OptimizaciónEl pipeline incluye controles estrictos:

1. **Validación de Esquema (Pandera):** Cada ejecución verifica que:
* No falten municipios (`cityTown`).
* El tipo sea estrictamente "Accidente".
* La temperatura esté en rangos físicos lógicos (-15ºC a 50ºC).


2. **Optimización de API:**
El script analiza los accidentes del día y **solo descarga el clima de los municipios afectados**, reduciendo las peticiones a la API de Euskalmet de ~300 a ~10 por día.

---

## 🧹 LimpiezaPara detener la plataforma y limpiar los contenedores:

```bash
docker compose down
```