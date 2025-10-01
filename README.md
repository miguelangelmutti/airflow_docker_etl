# Data App de Espacios Culturales de Argentina

## 📋 Descripción del Proyecto

Aplicación de ETL (Extract, Transform, Load) orquestada con **Apache Airflow** y **Docker** que automatiza la recolección, procesamiento y normalización de datos sobre espacios culturales de Argentina. La aplicación integra información de:

- 🏛️ **Museos**
- 🎬 **Cines**
- 📚 **Bibliotecas**
- 📊 **Censo Nacional 2022 de Argentina**

Los datos son extraídos desde fuentes públicas, normalizados y almacenados en una base de datos **PostgreSQL** para su análisis y consulta.

---

## 🏗️ Arquitectura del Proyecto

### Tecnologías Principales

- **Apache Airflow 2.10.0**: Orquestación de workflows ETL
- **Docker & Docker Compose**: Containerización y gestión de servicios
- **PostgreSQL**: Base de datos relacional
- **Python 3.8+**: Lenguaje de programación principal
- **Pandas**: Procesamiento y transformación de datos
- **SQLAlchemy**: ORM para interacción con la base de datos

### Componentes del Sistema

```
┌─────────────────┐
│   Apache Airflow│
│   (Scheduler +  │
│    Webserver)   │
└────────┬────────┘
         │
         ├──────────> DAGs de ETL
         │            ├─ ETL-Data-Cultural.py
         │            └─ ETL-Raw-Data.py
         │
         ├──────────> Descarga de Datos
         │            (APIs públicas)
         │
         └──────────> PostgreSQL
                      (Almacenamiento)
```

---

## 📁 Estructura de Carpetas

```
airflow_docker_etl/
│
├── 📄 docker-compose.yaml          # Configuración de servicios Docker
├── 📄 Dockerfile                   # Imagen personalizada de Airflow
├── 📄 requirements.txt             # Dependencias Python
├── 📄 variables.json               # Variables de configuración de Airflow
├── 📄 README.md                    # Este archivo
│
├── 📂 dags/                        # DAGs de Airflow
│   ├── ETL-Data-Cultural.py        # Pipeline de datos normalizados
│   ├── ETL-Raw-Data.py             # Pipeline de descarga de datos raw
│   └── __pycache__/                # Cache de Python
│
├── 📂 data/                        # Almacenamiento de datos descargados
│   ├── bibliotecas/                # Datos de bibliotecas por fecha
│   │   ├── 2024-9/
│   │   ├── 2025-3/
│   │   ├── 2025-4/
│   │   └── 2025-8/
│   ├── censo/                      # Datos del censo nacional
│   │   └── censo.xlsx
│   ├── cines/                      # Datos de cines por fecha
│   │   ├── 2024-9/
│   │   ├── 2025-3/
│   │   ├── 2025-4/
│   │   └── 2025-8/
│   └── museos/                     # Datos de museos por fecha
│       ├── 2024-9/
│       ├── 2025-3/
│       ├── 2025-4/
│       └── 2025-8/
│
├── 📂 sql/                         # Scripts SQL
│   └── create_tables.sql           # Definición de tablas de BD
│
├── 📂 logs/                        # Logs de ejecución de Airflow
│   ├── dag_id=ETL_DATA_ESPACIOS_CULTURALES/
│   ├── dag_id=ETL_DATA_RAW_ESPACIOS_CULTURALES/
│   └── scheduler/
│
├── 📂 config/                      # Configuraciones adicionales
└── 📂 plugins/                     # Plugins personalizados de Airflow
```

---

## 🗄️ Esquema de Base de Datos

### Tablas Principales

#### Tablas de Dimensiones
- **`provincias`**: Provincias de Argentina
- **`localidades`**: Localidades con referencia a provincias
- **`categorias`**: Tipos de espacios culturales

#### Tablas de Hechos
- **`espacios_culturales`**: Información general de espacios culturales
- **`museos`**: Datos específicos de museos
- **`cines`**: Datos específicos de cines
- **`bibliotecas`**: Datos específicos de bibliotecas
- **`censo`**: Datos demográficos del Censo 2022

#### Tablas de Indicadores
- **`indicadores`**: Métricas agregadas por provincia y categoría
- **`cines_indicadores`**: Estadísticas específicas de cines

### Relaciones

```
provincias (1) ──< (N) localidades
                       │
                       ├──< museos
                       ├──< cines
                       └──< espacios_culturales
                       
categorias (1) ──< (N) espacios_culturales
```

---

## 🚀 Instalación y Configuración

### Prerequisitos

- Docker Desktop instalado
- Docker Compose instalado
- Al menos 4GB de RAM disponible
- Puertos 8080 (Airflow) y 5432 (PostgreSQL) disponibles

### Pasos de Instalación

1. **Clonar el repositorio**
   ```bash
   git clone <repository-url>
   cd airflow_docker_etl
   ```

2. **Construir la imagen personalizada de Airflow**
   
   El `Dockerfile` incluye las dependencias especificadas en `requirements.txt`
   
   Referencia: [Customizing Airflow Docker Image](https://airflow.apache.org/docs/docker-stack/build.html#customizing-the-image)

3. **Iniciar los servicios con Docker Compose**
   ```bash
   docker-compose up -d
   ```
   
   Esto iniciará:
   - Airflow Webserver
   - Airflow Scheduler
   - PostgreSQL
   - Redis (para Celery Executor)
   
   Referencia: [Airflow con Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

4. **Acceder a la interfaz de Airflow**
   
   Abrir el navegador en: http://localhost:8080/
   
   - **Usuario**: airflow
   - **Contraseña**: airflow

5. **Configurar Variables de Airflow**
   
   - Ir a `Admin` → `Variables`
   - Importar el archivo `variables.json` ubicado en la raíz del proyecto
   - Este archivo contiene las URLs de las APIs y rutas de almacenamiento

6. **Configurar la conexión a PostgreSQL**
   
   - Ir a `Admin` → `Connections`
   - Crear/verificar la conexión `data_db` con los parámetros de PostgreSQL

---

## 📊 DAGs Disponibles

### 1. `ETL_DATA_RAW_ESPACIOS_CULTURALES`
**Archivo**: `dags/ETL-Raw-Data.py`

**Descripción**: Pipeline para descarga de datos raw desde las APIs públicas

**Tareas**:
- Descarga de datos de museos
- Descarga de datos de cines
- Descarga de datos de bibliotecas
- Descarga de datos del Censo 2022
- Almacenamiento en carpetas `data/` organizadas por fecha

**Frecuencia**: Manual o programada

### 2. `ETL_DATA_ESPACIOS_CULTURALES`
**Archivo**: `dags/ETL-Data-Cultural.py`

**Descripción**: Pipeline de transformación y carga a la base de datos

**Tareas**:
- Lectura de datos raw descargados
- Limpieza y normalización de datos
- Transformación de campos
- Carga a tablas de PostgreSQL
- Generación de indicadores agregados
- Actualización de métricas

**Frecuencia**: Después de la descarga de datos

---

## 🔧 Dependencias Python

```
certifi                 # Certificados SSL
pandas                  # Manipulación de datos
psycopg2-binary         # Driver PostgreSQL
sqlalchemy              # ORM
requests                # Peticiones HTTP
openpyxl                # Lectura de archivos Excel
python-decouple         # Gestión de configuración
numpy                   # Operaciones numéricas
```

Ver archivo completo: `requirements.txt`

---

## 📝 Configuración de Variables

El archivo `variables.json` debe contener:

```json
{
  "data_path": "/opt/airflow/data",
  "data_url_censo": "<URL del censo>",
  "data_url_museos": "<URL de museos>",
  "data_url_cines": "<URL de cines>",
  "data_url_bibliotecas": "<URL de bibliotecas>",
  "categorias": "['Museos', 'Cines', 'Bibliotecas']"
}
```

---

## 🔍 Monitoreo y Logs

### Logs de Airflow
Los logs de cada ejecución se almacenan en:
```
logs/
├── dag_id=<NOMBRE_DEL_DAG>/
│   └── run_id=<TIMESTAMP>/
│       └── task_id=<NOMBRE_TAREA>/
```

### Verificar Estado de Contenedores
```bash
docker-compose ps
```

### Ver Logs en Tiempo Real
```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

---

## 🛠️ Comandos Útiles

### Detener los servicios
```bash
docker-compose down
```

### Reiniciar los servicios
```bash
docker-compose restart
```

### Limpiar volúmenes (⚠️ elimina datos)
```bash
docker-compose down -v
```

### Acceder al contenedor de Airflow
```bash
docker-compose exec airflow-scheduler bash
```

### Ejecutar un DAG manualmente
```bash
docker-compose exec airflow-scheduler airflow dags trigger <DAG_ID>
```

---

## 📈 Flujo de Datos

```
┌─────────────────────┐
│  APIs Públicas      │
│  (Datos Culturales  │
│   y Censo)          │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  ETL Raw Data       │
│  (Descarga)         │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Almacenamiento     │
│  data/ (CSV/Excel)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  ETL Data Cultural  │
│  (Transformación)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  PostgreSQL         │
│  (Base de Datos)    │
└─────────────────────┘
```

---

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

---

## 📄 Licencia

Este proyecto está bajo la Licencia Apache 2.0 (heredada de Apache Airflow).

---

## 📧 Contacto

Para preguntas o soporte, por favor abre un issue en el repositorio.

---

## 🔗 Referencias

- [Documentación oficial de Apache Airflow](https://airflow.apache.org/docs/)
- [Docker Compose para Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Customizing Airflow Image](https://airflow.apache.org/docs/docker-stack/build.html)
- [Datos Abiertos Argentina](https://datos.gob.ar/)