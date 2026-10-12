# Universidad del Valle
## LG15 - Tolerancia a Fallos y Replicación de Datos en HDFS

**Materia:** Big Data y Sistemas Distribuidos  
**Identificador de Grupo:** `grupo04`  

### Integrantes del Grupo 04:
- **Joel Saavedra**
- **Mauricio Linaja**
- **Rommel Gutierrez**

---

## 1. Objetivo

Implementar un entorno distribuido Hadoop HDFS compuesto por **1 NameNode y 3 DataNodes**, configurando un **factor de replicación de 2**, para analizar experimentalmente el comportamiento de HDFS ante la caída de un DataNode mediante la carga, lectura y recuperación de datasets de **1 GB** y **10 GB**.

Se demuestra de forma práctica y verificable la distribución de los bloques, las réplicas existentes y el comportamiento del sistema antes, durante y después de la caída de un DataNode, garantizando la alta disponibilidad del sistema.

---

## 2. Requerimientos Mínimos de la Práctica

| Componente | Requerimiento Mínimo | Implementación Grupo 04 | Estado |
| :--- | :---: | :---: | :---: |
| **NameNode** | 1 | 1 (`namenode` en puertos 9870 / 9000) | Cumplido |
| **DataNodes** | 3 | 3 (`datanode1`, `datanode2`, `datanode3`) | Cumplido |
| **Factor de Replicación** | $\ge 2$ | 2 (`dfs.replication=2` en `hdfs-site.xml`) | Cumplido |
| **Dataset 1** | 1 GB | `dataset_grupo04_1GB.csv` (1,073,741,824 bytes) | Cumplido |
| **Dataset 2** | 10 GB | `dataset_grupo04_10GB.csv` (10,737,418,240 bytes) | Cumplido |
| **Pruebas de Fallo** | 2 | 2 pruebas reales (Fallo en DN2 y Fallo en DN3) | Cumplido |
| **Evidencias** | Obligatorio | 10 archivos de logs reales en carpeta `evidencias/` | Cumplido |

---

## 3. Diagrama de Arquitectura

El siguiente diagrama detalla la interacción entre el cliente, el NameNode maestro y los tres DataNodes distribuidos, incluyendo el pipeline de replicación física entre nodos de almacenamiento:

```mermaid
flowchart TD
    Client["Cliente / Scripts / Terminal"] -->|"Consulta Metadatos & Ubicaciones"| NN["NameNode (:9870 / :9000)"]
    Client -->|"Streaming de Bloques I/O"| DN1["DataNode 1 (:9864)"]
    Client -->|"Streaming de Bloques I/O"| DN2["DataNode 2 (:9865)"]
    Client -->|"Streaming de Bloques I/O"| DN3["DataNode 3 (:9866)"]
    
    NN -.->|"Heartbeat (3s) & Block Reports"| DN1
    NN -.->|"Heartbeat (3s) & Block Reports"| DN2
    NN -.->|"Heartbeat (3s) & Block Reports"| DN3

    DN1 <-->|"Pipeline Replicación HDFS (Factor 2)"| DN2
    DN2 <-->|"Pipeline Replicación HDFS (Factor 2)"| DN3
    DN3 <-->|"Pipeline Replicación HDFS (Factor 2)"| DN1

    subgraph YARN["Capa de Gestión de Cómputo"]
        RM["ResourceManager (:8088)"]
        NM["NodeManager (:8042)"]
        JHS["JobHistoryServer (:8188)"]
        RM --- NM
        RM --- JHS
    end
```

---

## 4. Identificación del Grupo y Estructura en HDFS

La jerarquía de directorios en HDFS y los nombres de archivos utilizan el identificador oficial `grupo04`:

```text
/grupo04/
├── dataset_1GB/
│   └── dataset_grupo04_1GB.csv
├── dataset_10GB/
│   └── dataset_grupo04_10GB.csv
├── resultados/
└── evidencias/
```

### Formato de los Datasets Sintéticos
Los datasets fueron generados con el script [`scripts/02_generar_datasets.py`](scripts/02_generar_datasets.py), incorporando el identificador del grupo, integrantes, marca de tiempo y bloque de verificación de integridad:

```csv
id,grupo,integrante,timestamp,sensor_id,temperatura,presion,estado,checksum_data
00000001,grupo04,Joel Saavedra   ,1791753508.91,SENS_0000,20.00,1013.25,OK,GRUPO04_INTEGRITY_CHECK_BLOCK_00000001
00000002,grupo04,Mauricio Linaja ,1791753509.16,SENS_0001,20.10,1013.30,OK,GRUPO04_INTEGRITY_CHECK_BLOCK_00000002
00000003,grupo04,Rommel Gutierrez,1791753509.41,SENS_0002,20.20,1013.35,OK,GRUPO04_INTEGRITY_CHECK_BLOCK_00000003
```

---

## 5. Instalación y Despliegue del Entorno

### Requisitos Previos
- Docker Desktop instalado con WSL 2 backend (Windows) o Docker Engine (Linux).
- Python 3.8 o superior (para los scripts auxiliares de generación).
- 8 GB de memoria RAM libre y al menos 25 GB de espacio en disco.

### Pasos de Despliegue
1. Clonar el repositorio y acceder a la carpeta:
   ```bash
   git clone https://github.com/TU_USUARIO/bigdata-grupo-04.git
   cd bigdata-grupo-04
   ```

2. Iniciar el clúster con Docker Compose:
   ```bash
   docker compose up -d
   ```

3. Verificar que los 7 servicios estén activos y saludables:
   ```bash
   docker ps
   ```

4. Interfaces Web disponibles:
   - **NameNode UI:** `http://localhost:9870`
   - **YARN ResourceManager:** `http://localhost:8088`
   - **DataNode 1:** `http://localhost:9864`
   - **DataNode 2:** `http://localhost:9865`
   - **DataNode 3:** `http://localhost:9866`

---

## 6. Pruebas de Fallo y Evidencias Experimentales

### Experimento 1: Dataset de 1 GB (`dataset_grupo04_1GB.csv`)

#### A. Estado Inicial (Antes de la Falla)
- **Estado de los DataNodes:** DN1 (Activo), DN2 (Activo), DN3 (Activo).
- **Métricas HDFS (`fsck`):**
  - Tamaño: 1,073,741,824 bytes (1.00 GB).
  - Tamaño de Bloque: 128 MB (134,217,728 bytes).
  - Total de Bloques: **8 bloques** (`blk_1073741825` a `blk_1073741832`).
  - Total de Réplicas: **16 réplicas** (Factor de replicación = 2).
  - Distribución física: DN1: 6 réplicas, DN2: 5 réplicas, DN3: 5 réplicas.
  - Estado: `HEALTHY`.
- *Evidencias registradas:* [`evidencias/exp1_01_dfsadmin_report_inicial.txt`](evidencias/exp1_01_dfsadmin_report_inicial.txt), [`evidencias/exp1_02_fsck_inicial.txt`](evidencias/exp1_02_fsck_inicial.txt).

#### B. Simulación de Falla (Durante la Falla)
- **Acción ejecutada:** Se detuvo forzosamente el contenedor `datanode2`:
  ```bash
  docker stop datanode2
  ```
- **Verificaciones observadas:**
  - **Estado del DataNode 2:** Fuera de servicio (Exited / Dead en HDFS).
  - **Estado del Archivo:** Continúa 100% disponible (`HEALTHY`).
  - **Bloques afectados:** 5 bloques tenían réplica en `datanode2`.
  - **Bloques sub-replicados:** HDFS reporta `Under-replicated blocks: 5`.
  - **Prueba de Lectura:** Exitosa. Se leyeron registros con `hdfs dfs -cat ... | head` sin fallos ni demoras gracias a las réplicas en DN1 y DN3.
- *Evidencias registradas:* [`evidencias/exp1_03_dfsadmin_report_durante_fallo.txt`](evidencias/exp1_03_dfsadmin_report_durante_fallo.txt), [`evidencias/exp1_04_fsck_durante_fallo.txt`](evidencias/exp1_04_fsck_durante_fallo.txt), [`evidencias/exp1_05_lectura_durante_fallo.txt`](evidencias/exp1_05_lectura_durante_fallo.txt).

#### C. Recuperación (Después de la Falla)
- Se restauró el servicio con `docker start datanode2`.
- Al recibir los Heartbeats, HDFS verificó los bloques locales y el contador de bloques sub-replicados retornó a `0`.

---

### Experimento 2: Dataset de 10 GB (`dataset_grupo04_10GB.csv`)

#### A. Estado Inicial (Antes de la Falla)
- **Estado de los DataNodes:** DN1 (Activo), DN2 (Activo), DN3 (Activo).
- **Métricas HDFS (`fsck`):**
  - Tamaño: 10,737,418,240 bytes (10.00 GB).
  - Tamaño de Bloque: 128 MB.
  - Total de Bloques: **80 bloques** (`blk_1073741838` a `blk_1073741917`).
  - Total de Réplicas: **160 réplicas físicas** (Factor de replicación = 2).
  - Distribución física homogénea: DN1: 62 bloques, DN2: 63 bloques, DN3: 61 bloques (incluyendo archivos del sistema).
  - Estado: `HEALTHY`.
- *Evidencias registradas:* [`evidencias/exp2_01_dfsadmin_report_inicial.txt`](evidencias/exp2_01_dfsadmin_report_inicial.txt), [`evidencias/exp2_02_fsck_inicial.txt`](evidencias/exp2_02_fsck_inicial.txt).

#### B. Simulación de Falla (Durante la Falla)
- **Acción ejecutada:** Se detuvo forzosamente el contenedor `datanode3`:
  ```bash
  docker stop datanode3
  ```
- **Verificaciones observadas:**
  - **Estado del DataNode 3:** Inaccesible / Desconectado.
  - **Estado del Archivo Masivo:** Accesible y consistente (`HEALTHY`).
  - **Bloques afectados:** 53 bloques que contenían réplica en `datanode3`.
  - **Bloques sub-replicados:** HDFS marca los bloques como sub-replicados temporalmente hasta que se cumpla el ciclo de replicación o se reactive el nodo.
  - **Prueba de Lectura:** Se ejecutó lectura por flujo (`hdfs dfs -cat ... | head -n 25`) leyendo de manera instantánea registros con datos del Grupo 04 desde DN1 y DN2.
- *Evidencias registradas:* [`evidencias/exp2_03_dfsadmin_report_durante_fallo.txt`](evidencias/exp2_03_dfsadmin_report_durante_fallo.txt), [`evidencias/exp2_04_fsck_durante_fallo.txt`](evidencias/exp2_04_fsck_durante_fallo.txt), [`evidencias/exp2_05_lectura_durante_fallo.txt`](evidencias/exp2_05_lectura_durante_fallo.txt).

#### C. Recuperación (Después de la Falla)
- Se restauró el servicio con `docker start datanode3`.
- HDFS reincorporó el nodo sin necesidad de reiniciar el clúster.

---

## 7. Análisis del Comportamiento de HDFS

### 1. Antes de la Falla
- Cada archivo se divide estrictamente en bloques de 128 MB. Para 1 GB son exactamente 8 bloques; para 10 GB son exactamente 80 bloques.
- El NameNode asigna cada réplica en un DataNode físicamente distinto. Con factor de replicación 2, cada bloque posee 2 copias físicas, lo que distribuye uniformemente la carga en los discos de los tres DataNodes.

### 2. Durante la Falla
- Al detenerse un DataNode (`docker stop`), el NameNode deja de recibir los paquetes periódicos de señal vital (*Heartbeat*) de ese nodo.
- Al no responder dicho nodo, el NameNode no interrumpe el acceso: redirige transparentemente al cliente hacia el DataNode alternativo que almacena la réplica superviviente.
- HDFS actualiza su registro interno indicando que el archivo tiene bloques con sub-replicación (`Under-replicated`), pero reporta el estado general como `HEALTHY` porque ningún bloque ha quedado con cero réplicas (`Missing blocks: 0`).

### 3. Después de la Falla
- Si el nodo se reincorpora (`docker start`), reanuda el envío de Heartbeats y emite un *Block Report* que actualiza el inventario del NameNode.
- El NameNode valida la suma de comprobación (CRC32C) de los bloques locales y elimina el estado de sub-replicación, restableciendo la redundancia total sin intervención manual.

---

## 8. Comparación Experimental (Sección 6 de la Guía)

La siguiente tabla resume y contrasta de manera cuantitativa los resultados de ambos experimentos según lo establecido en el documento de la práctica:

| Característica | Dataset 1 GB | Dataset 10 GB |
| :--- | :---: | :---: |
| **Tamaño** | 1 GB (1,073,741,824 bytes) | 10 GB (10,737,418,240 bytes) |
| **Número de bloques** | 8 bloques | 80 bloques |
| **Factor de replicación** | 2 | 2 |
| **DataNode afectado** | `datanode2` | `datanode3` |
| **Bloques afectados** | 5 bloques | 53 bloques |
| **Bloques disponibles después de falla** | 8 bloques (100% disponibles) | 80 bloques (100% disponibles) |
| **¿Archivo accesible?** | Sí (Lectura continua sin interrupciones) | Sí (Lectura continua sin interrupciones) |
| **¿Existieron bloques subreplicados?** | Sí (5 bloques sub-replicados) | Sí (53 bloques sub-replicados) |
| **Tiempo de recuperación** | Inmediato para lectura (< 1 s); Re-sincronización: ~15 s | Inmediato para lectura (< 1 s); Re-sincronización: ~30 s |
| **Nuevas réplicas** | Sincronizadas en `datanode2` tras reinicio | Sincronizadas en `datanode3` tras reinicio |

---

## 9. Conclusiones

1. La configuración con factor de replicación 2 sobre 3 DataNodes permitió comprobar que HDFS mantiene la disponibilidad de los archivos ante la caída de un nodo. En ambos experimentos (1 GB y 10 GB), las operaciones de lectura respondieron sin errores al redirigirse hacia los DataNodes que conservaban las réplicas sanas.
2. El dataset de 10 GB permitió analizar un volumen más representativo del sistema distribuido (80 bloques de 128 MB y 160 réplicas físicas) en comparación con los 8 bloques del archivo de 1 GB, mostrando cómo el NameNode distribuye la carga entre los nodos disponibles.
3. La persistencia mediante volúmenes con nombre en Docker garantiza que tanto los bloques en los DataNodes como los metadatos del NameNode se mantengan en el disco local, permitiendo reiniciar o detener los contenedores sin perder los datos cargados en HDFS.

---

## 10. Cuestionario de Análisis

### Pregunta 1: ¿Por qué HDFS puede continuar proporcionando el archivo aunque un DataNode haya fallado?
Porque cada bloque del archivo cuenta con una copia idéntica almacenada en otro DataNode del clúster debido al factor de replicación 2. Cuando el cliente intenta leer y el nodo primario no responde, el NameNode redirige la solicitud hacia el nodo que conserva la réplica viva, permitiendo completar la lectura sin interrupción.

### Pregunta 2: ¿Qué habría ocurrido si el factor de replicación hubiera sido 1?
Cada bloque habría tenido una única copia física en todo el clúster. Al apagarse el DataNode que contenía parte de esos bloques, esos fragmentos habrían quedado inaccesibles (`Missing blocks`). El archivo habría quedado corrupto para cualquier operación de lectura que requiriera los sectores afectados.

### Pregunta 3: ¿Qué habría ocurrido si hubieran fallado dos DataNodes simultáneamente?
Con 3 DataNodes y factor de replicación 2, cada bloque se reparte entre 2 máquinas distintas. Si caen dos DataNodes al mismo tiempo, los bloques cuyas dos réplicas estaban alojadas justamente en esos dos nodos quedan con cero copias activas en la red. El archivo pierde su integridad y la lectura falla hasta que vuelva a encenderse al menos uno de los nodos caídos.

### Pregunta 4: ¿Por qué un factor de replicación de 2 no significa necesariamente que existan dos copias completas del archivo en dos DataNodes?
Porque HDFS no replica archivos enteros, sino bloques individuales de 128 MB. Cada bloque se ubica de forma independiente siguiendo la política de distribución del NameNode. En un archivo de 8 bloques, el bloque 0 puede estar repartido entre DN1 y DN3, mientras que el bloque 2 se encuentra en DN2 y DN3. Las copias quedan distribuidas entre los tres DataNodes, de modo que ningún par de nodos contiene necesariamente el archivo íntegro por sí solo.

### Pregunta 5: ¿Cuál es la relación entre tamaño de bloque y número de bloques?
Es una relación inversamente proporcional para un mismo volumen de datos:

$$\text{Número de bloques} = \left\lceil \frac{\text{Tamaño total del archivo}}{\text{Tamaño de bloque}} \right\rceil$$

Un tamaño de bloque grande produce menos bloques, lo que reduce el consumo de memoria RAM en el NameNode para gestionar metadatos, aunque disminuye la cantidad de tareas concurrentes si el archivo es pequeño. Un tamaño de bloque chico genera más bloques para procesar en paralelo, pero sobrecarga la tabla de metadatos del NameNode.

### Pregunta 6: ¿Por qué el dataset de 10 GB permite observar un comportamiento más representativo de un sistema distribuido que el dataset de 1 GB?
El archivo de 1 GB solo genera 8 bloques (16 réplicas físicas), una cantidad pequeña donde la asignación aleatoria puede no reflejar el comportamiento general del clúster. El archivo de 10 GB genera 80 bloques y 160 réplicas físicas distribuidas en disco, lo que permite evaluar el balanceo de carga real entre los tres nodos, el tráfico de red sostenido y el impacto sobre decenas de bloques sub-replicados al momento de inducir la falla.

### Pregunta 7: ¿Qué sucede con los bloques que tenían una réplica en el DataNode que falló?
Pasan al estado de sub-replicación (`Under-replicated`). Siguen disponibles para lectura porque conservan una copia activa en otro nodo, pero ya no cumplen con la política de tener 2 copias físicas. Si el DataNode permanece desconectado más allá del tiempo límite de espera (*Heartbeat timeout*, normalmente 10.5 minutos), el NameNode ordena copiar esos bloques hacia los DataNodes activos restantes para recuperar el factor de replicación 2.

### Pregunta 8: ¿Quién administra la ubicación y replicación de los bloques: el cliente, el DataNode o el NameNode?
El NameNode. Es el único nodo que mantiene en memoria el mapeo completo del sistema de archivos y decide en qué DataNodes se almacena cada réplica. El cliente únicamente solicita las direcciones de red para conectarse a leer o escribir datos, y los DataNodes se limitan a almacenar bloques en sus discos locales y reportar periódicamente su inventario al NameNode.

### Pregunta 9: ¿Qué diferencia existe entre disponibilidad del archivo y cumplimiento del factor de replicación después de una falla?
La disponibilidad significa que el cliente puede leer el archivo completo sin errores porque cada bloque cuenta con al menos una réplica viva. El cumplimiento del factor de replicación exige que cada bloque mantenga activas las 2 copias configuradas. Durante la caída de un nodo, el archivo mantiene su disponibilidad al 100%, pero pierde el cumplimiento del factor de replicación debido a la presencia de bloques sub-replicados.

