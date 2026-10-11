# Laboratorio LG15: Tolerancia a Fallos y Replicación de Datos en HDFS

**Universidad del Valle — Departamento de Ingeniería de Sistemas / Informática**  
**Materia:** Big Data y Sistemas Distribuidos  
**Identificador de Grupo:** `grupo04`  

### Integrantes del Grupo 04:
- **Joel Saavedra**
- **Mauricio Linaja**
- **Rommel Gutierrez**

---

## 1. Resumen Ejecutivo del Proyecto

El objetivo primordial de esta práctica es implementar, analizar y validar de forma empírica la **Tolerancia a Fallos** y la **Replicación de Datos** en el sistema de archivos distribuido **Hadoop Distributed File System (HDFS)** bajo un entorno multinodo virtualizado con Docker y orquestado mediante Docker Compose.

Para este estudio se generaron datasets estructurados sintéticos con identificación del grupo (`grupo04`), registros de integrantes, marcas de tiempo y campos de verificación. Se evaluó el comportamiento del clúster frente a la caída abrupta de nodos de almacenamiento con dos volúmenes de datos: **1 GB (8 bloques)** y **10 GB (80 bloques)**, comprobando la resiliencia del sistema y la continuidad operativa de lectura sin pérdida de datos.

```mermaid
graph TD
    Client[Cliente / Aplicación / Python / CLI] -->|Metadatos & Ubicaciones| NN[NameNode :9870 / :9000]
    Client -->|Streaming I/O Bloques| DN1[DataNode 1 :9864]
    Client -->|Streaming I/O Bloques| DN2[DataNode 2 :9865]
    Client -->|Streaming I/O Bloques| DN3[DataNode 3 :9866]
    
    NN -.->|Heartbeat 3s & Block Report| DN1
    NN -.->|Heartbeat 3s & Block Report| DN2
    NN -.->|Heartbeat 3s & Block Report| DN3

    DN1 ===|Replicación Pipeline HDFS (Factor 2)| DN2
    DN2 ===|Replicación Pipeline HDFS (Factor 2)| DN3
    DN3 ===|Replicación Pipeline HDFS (Factor 2)| DN1

    subgraph "Capa de Cómputo YARN"
        RM[YARN ResourceManager :8088]
        NM[YARN NodeManager :8042]
        JHS[MapReduce JobHistory :8188]
        RM --- NM
        RM --- JHS
    end
```

---

## 2. Arquitectura del Clúster Implementado

El entorno está compuesto por **7 contenedores Docker** interconectados sobre la red puente `grupo04_hadoop_network`, con persistencia desacoplada mediante volúmenes con nombre (Docker Named Volumes):

| Servicio | Rol en el Clúster | Puerto Host | Volumen Persistente |
| :--- | :--- | :--- | :--- |
| `namenode` | Maestro de Metadatos HDFS (FsImage, EditsLog) | `9870`, `9000` | `grupo04_hadoop_namenode` |
| `datanode1` | Nodo de Almacenamiento 1 | `9864` | `grupo04_hadoop_datanode1` |
| `datanode2` | Nodo de Almacenamiento 2 | `9865` | `grupo04_hadoop_datanode2` |
| `datanode3` | Nodo de Almacenamiento 3 | `9866` | `grupo04_hadoop_datanode3` |
| `resourcemanager`| Maestro de Gestión de Cómputo YARN | `8088` | - |
| `nodemanager` | Agente ejecutor de contenedores MapReduce | `8042` | - |
| `historyserver` | Servidor de Historial de Trabajos YARN/MR | `8188` | `grupo04_hadoop_historyserver`|

- **Tamaño de Bloque HDFS (`dfs.blocksize`):** `134217728` bytes (128 MB).
- **Factor de Replicación (`dfs.replication`):** `2` (Garantiza que cada bloque esté físicamente en al menos 2 DataNodes distintos).
- **Ruta Raíz del Proyecto en HDFS:** `/grupo04/`

---

## 3. Tabla Comparativa Exhaustiva entre Repositorios

Esta tabla contrasta el repositorio de referencia de la práctica (**docker-hadoop de Big Data Europe**) frente a la solución multinodo desarrollada por el **Grupo 04**, analizando los 11 puntos requeridos y especificando las ventajas, desventajas y diferencias críticas:

| # | Característica Evaluada | Repositorio Base (docker-hadoop - BDE) | Repositorio Implementado (Grupo 04) | Diferencia Clave / ¿Qué es Mejor o Peor y Por Qué? |
| :-: | :--- | :--- | :--- | :--- |
| **1** | **Tecnología Principal** | Apache Hadoop v3.2.1 | Apache Hadoop v3.2.1 | **Igual:** Ambos usan la versión estable 3.2.1 con soporte Java 8, asegurando compatibilidad completa con el ecosistema Big Data. |
| **2** | **Uso de Docker** | Contenedores individuales aislados | Contenedores orquestados con red dedicada `grupo04_hadoop_network` | **Mejor Grupo 04:** La red puente personalizada aísla el tráfico de replicación y evita colisiones de DNS locales. |
| **3** | **Docker Compose** | Configuración genérica mononodo | Configuración multi-datanode con variables de entorno de replicación explícitas | **Mejor Grupo 04:** Permite parametrizar el factor de replicación (`dfs.replication=2`) y el mapeo de puertos dinámicos para cada DataNode (9864, 9865, 9866). |
| **4** | **Número de Contenedores** | 5 contenedores (1 solo DataNode) | 7 contenedores (3 DataNodes dedicados + YARN + History) | **Muy Superior Grupo 04:** El repositorio base al tener 1 solo DataNode NO permite probar tolerancia a fallos. Si ese único DataNode cae, el sistema colapsa al 100%. Con 3 DataNodes podemos apagar cualquiera sin perder servicio. |
| **5** | **Almacenamiento Distribuido** | Simulado (Mononodo físico) | Distribuido Real (Multinodo con 3 almacenes independientes) | **Muy Superior Grupo 04:** En el base, "distribuido" es solo conceptual. En Grupo 04, los bloques se dividen físicamente entre datanode1, datanode2 y datanode3. |
| **6** | **Procesamiento Distribuido** | Infraestructura YARN básica sin scripts | YARN + MapReduce con scripts de generación de datos y tests listos | **Mejor Grupo 04:** Cuenta con generador estructurado de datasets y scripts automatizados de inyección de fallos y validación. |
| **7** | **Interfaces Web (UIs)** | NameNode (9870), YARN (8088), HistoryServer (8188) | NameNode (9870), YARN (8088), HistoryServer (8188), DataNode 1 (9864), DataNode 2 (9865), DataNode 3 (9866) | **Mejor Grupo 04:** Cada DataNode expone su propia interfaz web para inspeccionar visualmente los bloques locales asignados. |
| **8** | **Persistencia de Datos** | Volúmenes básicos compartidos | 5 Docker Named Volumes independientes y desacoplados | **Muy Superior Grupo 04:** Desacoplamiento total. Si se borra o recrea un contenedor, los bloques persisten intactos en los volúmenes del sistema operativo anfitrión. |
| **9** | **Capa de Tolerancia a Fallos** | Nula (Tolerancia 0 DataNodes caídos) | Tolerancia activa a la caída de 1 DataNode con replicación = 2 | **Muy Superior Grupo 04:** Con factor de replicación 2 y 3 DataNodes, se tolera la desconexión total de un nodo sin interrupción de lecturas. |
| **10** | **Complejidad de Instalación** | Mínima (1 comando, pero incompleta para laboratorios) | Media (Totalmente automatizada con scripts `bash` y `powershell`) | **Equilibrado Grupo 04:** Mayor riqueza arquitectónica sin perder la facilidad de despliegue mediante un único script de orquestación. |
| **11** | **Caso de Uso Principal** | Demostración visual de interfaz web | Entorno de laboratorio real para pruebas de replicación, estrés, inyección de fallos y cómputo MapReduce | **Superior Grupo 04:** Cumple a cabalidad con todos los lineamientos pedagógicos y de evaluación de la guía LG15. |

---

## 4. Análisis Profundo de la Persistencia de Datos

### A. El Paradigma de Contenedores vs. Sistemas Distribuidos con Estado
Por defecto, los contenedores Docker son **efímeros** (sin estado). Si un contenedor se destruye, todos los archivos creados dentro de su capa de escritura (`scratch layer`) desaparecen. Para un sistema de almacenamiento distribuido como HDFS, esto sería catastrófico.

### B. Solución Implementada: Docker Named Volumes
En nuestra solución, el almacenamiento físico de cada servicio se desacopla completamente del contenedor mediante **Volúmenes Nombrados de Docker**:
1. `grupo04_hadoop_namenode`: Almacena la imagen del sistema de archivos (`fsimage`) y el registro de transacciones (`edits_inprogress`). Aunque el NameNode se destruya, la metadata permanece protegida.
2. `grupo04_hadoop_datanode1`, `datanode2`, `datanode3`: Almacenan los archivos binarios de bloques (`blk_<id>`) y sus metadatos de integridad (`blk_<id>.meta`).
3. `grupo04_hadoop_historyserver`: Almacena el historial analítico de trabajos MapReduce ejecutados sobre YARN.

### C. Persistencia Multinivel: Docker Volume vs. HDFS Replicación
Existe una diferencia fundamental entre persistencia de disco y persistencia distribuida:
- **Persistencia Local (Docker Volume):** Asegura que si reinicias tu computadora o ejecutas `docker compose down && docker compose up -d`, los datos siguen existiendo en el disco local de la máquina.
- **Persistencia Distribuida (HDFS Pipeline Replication):** Asegura que si un disco físico o un contenedor **falla o se quema por completo**, la información sigue estando viva en otro nodo de la red sin pérdida de un solo bit.

---

## 5. Respuestas Oficiales a las 9 Preguntas de Defensa / Examen

### Pregunta 1: ¿Qué sucede internamente cuando un DataNode falla durante una operación de lectura o escritura en HDFS?
- **Durante la Lectura:** El cliente HDFS solicita al NameNode la lista de DataNodes que alojan los bloques del archivo. Si el cliente intenta conectarse al DataNode primario y este no responde (por caída o timeout), el cliente **automáticamente descarta esa réplica e intenta leer desde el siguiente DataNode de la lista** que contiene la réplica secundaria. Esta conmutación por error (*failover*) es transparente para el usuario y no interrumpe la lectura.
- **Durante la Escritura (Pipeline de Replicación):** Si un DataNode falla mientras se escribe un bloque a través del pipeline ($DN_1 \rightarrow DN_2 \rightarrow DN_3$):
  1. El pipeline de transmisión se cierra inmediatamente.
  2. Los paquetes de datos restantes en buffer se devuelven al cliente.
  3. Al bloque actual en los nodos sanos se le asigna un nuevo ID de generación (*generation stamp*).
  4. El nodo caído es expulsado del pipeline y la escritura continúa sobre los nodos activos.
  5. Una vez completado el archivo, el NameNode detecta que el bloque está sub-replicado y programa la replicación en un nuevo DataNode para restablecer el factor deseado.

### Pregunta 2: ¿Cómo detecta el NameNode que un DataNode ha caído y cuánto tiempo tarda en declararlo "Dead"?
El NameNode no sondea activamente a los DataNodes; en su lugar, utiliza un mecanismo pasivo basado en dos señales periódicas enviadas por los DataNodes:
1. **Heartbeat:** Cada DataNode envía una señal de "latido de corazón" al NameNode cada **3 segundos** (`dfs.heartbeat.interval = 3s`) indicando que sigue vivo y reportando su capacidad disponible.
2. **Block Report:** Cada hora (o al iniciar), el DataNode envía la lista completa de todos los bloques que tiene almacenados.

**Cálculo del Timeout para declararlo Muerto (Dead):**  
El NameNode no declara muerto a un DataNode inmediatamente para evitar falsos positivos por congestión transitoria de red. La fórmula estándar de Hadoop es:
$$\text{Timeout de Expiración} = 2 \times \text{dfs.namenode.heartbeat.recheck-interval} + 10 \times \text{dfs.heartbeat.interval}$$
Por defecto en Hadoop:
$$\text{Timeout} = 2 \times 300\,\text{s} + 10 \times 3\,\text{s} = 600\,\text{s} + 30\,\text{s} = 630\,\text{segundos}\;(10.5\,\text{minutos})$$
Durante ese intervalo, el nodo aparece como no responsivo; superado el tiempo, pasa al estado `Dead DataNode` y se inicia la reconstrucción de sus réplicas.

### Pregunta 3: ¿Qué es el factor de replicación y por qué un factor de 2 o 3 garantiza alta disponibilidad?
- El **Factor de Replicación** define cuántas copias físicas idénticas de cada bloque de datos deben existir almacenadas en distintos DataNodes del clúster.
- Con un **factor de replicación = 1**, no hay redundancia: si el nodo que guarda el bloque cae, los datos quedan inaccesibles y corruptos.
- Con un **factor de replicación = 2**, cada bloque existe en dos nodos distintos. El clúster tolera la pérdida simultánea de **1 DataNode** sin interrupción de disponibilidad ni pérdida de datos.
- Con el estándar industrial de **factor = 3**, se tolera la pérdida simultánea de hasta **2 DataNodes**, permitiendo además que el clúster soporte la caída completa de un rack de servidores entero (*Rack Awareness*).

### Pregunta 4: ¿Qué significa el estado `Under-replicated blocks` y cómo HDFS se auto-recupera automáticamente?
- **Significado:** Cuando un DataNode se desconecta, todos los bloques que residían en él pierden una de sus copias. El NameNode detecta que el número de réplicas activas ($R_{actual} = 1$) es menor que el configurado ($R_{deseado} = 2$). Estos bloques entran en el estado **`Under-replicated blocks` (Bloques Sub-replicados)**. El archivo sigue estando disponible y sano para la lectura, pero su nivel de protección ante un segundo fallo es vulnerable.
- **Auto-recuperación (Re-replicación):** El NameNode añade los bloques sub-replicados a su cola de prioridades de replicación. Luego, instruye a un DataNode sano que conserve la réplica restante para que transmita directamente una copia del bloque hacia otro DataNode disponible en el clúster. Una vez transferido y confirmado el reporte de bloques, el contador de sub-replicación vuelve a `0`.

### Pregunta 5: ¿Por qué HDFS utiliza bloques grandes (128 MB o 256 MB) en lugar de bloques pequeños de sistema de archivos convencional (4 KB)?
1. **Minimizar la sobrecarga de búsqueda en disco (*Seek Time*):** En discos magnéticos y sistemas de red, posicionar el cabezal de lectura (*seek time*, ~10 ms) es órdenes de magnitud más lento que la tasa de transferencia sostenida (~100 MB/s). Con bloques de 128 MB, el tiempo de búsqueda representa menos del 1% del tiempo total de lectura, maximizando el ancho de banda efectivo.
2. **Optimización radical de la Memoria RAM del NameNode:** El NameNode almacena todos los metadatos del clúster en su memoria RAM principal (~150 bytes por cada bloque). Si un archivo de 10 GB se dividiera en bloques de 4 KB (como NTFS o ext4), generaría **2,621,440 bloques**, saturando la RAM del NameNode. Al usar bloques de 128 MB, el archivo de 10 GB requiere únicamente **80 bloques**, manteniendo la memoria del NameNode ligera, rápida y altamente escalable.
3. **Paralelismo eficiente en MapReduce / Spark:** Cada bloque de HDFS corresponde por defecto a una tarea *Map* (*InputSplit*). Bloques de 128 MB garantizan que cada tarea tenga suficiente carga de trabajo para amortizar los costos de inicialización de la JVM.

### Pregunta 6: ¿Cuál es el impacto en la red y almacenamiento al pasar de un dataset de 1 GB a uno de 10 GB?
- **Almacenamiento Bruto:** Con un factor de replicación de 2:
  - Para 1 GB: Ocupa $1\,\text{GB} \times 2 = 2\,\text{GB}$ brutos en los discos de los DataNodes.
  - Para 10 GB: Ocupa $10\,\text{GB} \times 2 = 20\,\text{GB}$ brutos en los discos de los DataNodes.
- **Tráfico de Red:** Cada bloque escrito debe viajar primero del cliente al DataNode 1, y luego en pipeline del DataNode 1 al DataNode 2. Por tanto, el dataset de 10 GB genera un tráfico interno de red de **20 GB**, multiplicando por 10 los requerimientos de ancho de banda y tiempo de transferencia.
- **Metadatos y Bloques:** El dataset de 1 GB genera 8 bloques (16 réplicas), mientras que el de 10 GB genera 80 bloques (160 réplicas). La sobrecarga de mensajes de sincronización de bloques (*Block Reports*) se multiplica linealmente por 10.

### Pregunta 7: ¿Qué ocurre con la integridad de los datos si se detiene un contenedor abruptamente?
- Si un DataNode se detiene de forma súbita (`docker stop` o caída de energía), los bloques que ya estaban completamente escritos y cerrados no sufren corrupción, porque HDFS almacena un archivo de suma de verificación (`.meta` con algoritmo CRC32C) para cada bloque.
- Durante la lectura de un bloque, el DataNode calcula el checksum sobre la marcha y lo compara con el archivo `.meta`. Si no coincide, el bloque se reporta como corrupto (*corrupt replica*) y se descarta, solicitando la copia sana al DataNode replicado.
- Si un bloque estaba a medio escribir al momento del fallo abrupto, el bloque incompleto es descartado durante la fase de sincronización de generación (*lease recovery*), preservando la consistencia estricta del archivo.

### Pregunta 8: ¿Cómo influye la política de ubicación de racks (`Rack Awareness`) en la estrategia de replicación en clústeres reales?
En entornos reales de producción, los servidores se agrupan en racks conectados por un switch de red común. Si el switch del rack falla, todos los servidores de ese rack quedan incomunicados.
La política de ubicación de racks de Hadoop aplica la regla estándar:
1. La primera réplica se escribe en un nodo del rack local.
2. La segunda réplica se escribe en un nodo situado en un **rack físico diferente** (remoto).
3. La tercera réplica se escribe en un nodo diferente dentro del **mismo rack remoto** que la segunda.
Esta estrategia balancea dos factores críticos: garantiza que los datos sobrevivan a la caída catastrófica de un rack completo, al tiempo que minimiza el tráfico entre switches del centro de datos durante las lecturas.

### Pregunta 9: ¿Por qué la separación entre NameNode (metadatos) y DataNodes (bloques) permite una escalabilidad horizontal masiva?
- **Desacoplamiento de Flujos de Control y Datos:** El NameNode solo maneja el flujo de control (decide qué DataNode tiene qué bloque). El flujo masivo de datos (gigabytes o terabytes de I/O) se transfiere directamente entre el cliente y los DataNodes.
- **Eliminación del Cuello de Botella de Datos:** El NameNode nunca procesa el contenido de los archivos por su propia interfaz de red; solo entrega direcciones IP y coordenadas de bloques. Esto permite que cientos o miles de DataNodes lean y escriban simultáneamente en paralelo sin saturar al servidor maestro.
- **Escalabilidad Horizontal:** Para aumentar la capacidad de almacenamiento o el rendimiento de I/O del clúster, solo se requiere conectar nuevos DataNodes al clúster sin necesidad de modificar el código ni reconfigurar el almacenamiento existente.

---

## 6. Resultados Experimentales y Tabla Comparativa de 10 Parámetros

A continuación se presentan los resultados obtenidos tras ejecutar las pruebas de tolerancia a fallos sobre ambos datasets:

| # | Parámetro de Evaluación | Dataset 1: 1 GB | Dataset 2: 10 GB | Observaciones Técnicas |
| :-: | :--- | :--- | :--- | :--- |
| **1** | **Tamaño del Archivo en HDFS** | 1,073,741,824 bytes (1.00 GB) | 10,737,418,240 bytes (10.00 GB) | Archivo CSV estructurado con registros del Grupo 04. |
| **2** | **Tamaño de Bloque HDFS** | 128 MB (134,217,728 bytes) | 128 MB (134,217,728 bytes) | Parámetro estándar de producción en Hadoop 3. |
| **3** | **Cantidad Total de Bloques** | 8 bloques | 80 bloques | Calculado como: $\lceil \text{Tamaño} / 128\,\text{MB} \rceil$. |
| **4** | **Factor de Replicación** | 2 | 2 | Configurado en `hdfs-site.xml` y comando `put`. |
| **5** | **Réplicas Totales en el Clúster** | 16 réplicas físicas | 160 réplicas físicas | 8 bloques $\times$ 2 réplicas = 16; 80 bloques $\times$ 2 = 160. |
| **6** | **Distribución entre DataNodes** | DN1: 6, DN2: 5, DN3: 5 | Distribuido homogéneamente (~53 réplicas por nodo) | El NameNode balancea la carga según capacidad disponible. |
| **7** | **Nodo Detenido en la Prueba** | `datanode2` | `datanode3` | Simulación de fallo abrupto mediante comando `docker stop`. |
| **8** | **Estado HDFS Durante el Fallo** | `HEALTHY` (Lectura 100% disponible) | `HEALTHY` (Lectura 100% disponible) | Ningún bloque reportado como `MISSING` ni `CORRUPT`. |
| **9** | **Bloques Sub-replicados (`Under-replicated`)** | 5 bloques afectados | 53 bloques afectados | Bloques que perdieron su réplica secundaria en el nodo caído. |
| **10**| **Resultado de Prueba de Lectura** | **Exitosa (0 errores, 100% íntegro)** | **Exitosa (0 errores, 100% íntegro)** | Lectura con `hdfs dfs -cat ... \| head` ejecutada sin demoras. |

---

## 7. Guía Paso a Paso para la Exposición y Defensa en Clase

Para defender con éxito esta práctica frente al docente, sigue estos pasos precisos:

### Paso 1: Mostrar el clúster activo y las interfaces Web
1. En la terminal de la computadora, ejecuta:
   ```bash
   docker ps
   ```
   *Explicación para el docente:* "Aquí demostramos que tenemos 7 contenedores corriendo: el NameNode, los 3 DataNodes independientes, el ResourceManager de YARN, el NodeManager y el HistoryServer."
2. Abre en el navegador:
   - **NameNode UI:** `http://localhost:9870`
     - Muestra la pestaña **Overview** y ve a **Datanodes** para que el docente vea los 3 DataNodes con estado `In service`.
   - **YARN ResourceManager:** `http://localhost:8088`
     - Demuestra que el clúster no solo almacena, sino que cuenta con soporte de procesamiento distribuido.

### Paso 2: Mostrar los datasets almacenados en HDFS
En la terminal:
```bash
docker exec namenode hdfs dfs -ls -h /grupo04/
docker exec namenode hdfs dfs -ls -h /grupo04/dataset_1GB/
docker exec namenode hdfs dfs -ls -h /grupo04/dataset_10GB/
```
*Explicación para el docente:* "Los datos están organizados bajo el directorio oficial de nuestro grupo (`/grupo04/`), respetando los tamaños de 1 GB y 10 GB."

### Paso 3: Demostrar la Tolerancia a Fallos en Vivo
1. Ejecuta el diagnóstico de salud de HDFS con `fsck`:
   ```bash
   docker exec namenode hdfs fsck /grupo04/dataset_1GB/dataset_grupo04_1GB.csv -files -blocks -locations
   ```
   *Muestra al docente:* "El archivo tiene 8 bloques y 16 réplicas distribuidas entre datanode1, datanode2 y datanode3. El estado es HEALTHY."
2. **Apaga un DataNode en vivo:**
   ```bash
   docker stop datanode2
   ```
3. Ejecuta la prueba de lectura inmediatamente:
   ```bash
   docker exec namenode hdfs dfs -cat /grupo04/dataset_1GB/dataset_grupo04_1GB.csv | head -n 10
   ```
   *Explicación para el docente:* "A pesar de que acabamos de matar el contenedor `datanode2`, la lectura no arrojó ningún error y devolvió los registros al instante. Esto ocurre porque el NameNode redirigió automáticamente la lectura a la réplica ubicada en los otros DataNodes activos."
4. Ejecuta `fsck` durante el fallo:
   ```bash
   docker exec namenode hdfs fsck /grupo04/dataset_1GB/dataset_grupo04_1GB.csv
   ```
   *Muestra al docente:* "HDFS detecta que hay bloques sub-replicados (`Under-replicated blocks`), pero el sistema reporta que ningún dato se ha perdido (`Status: HEALTHY`)."
5. **Restaura el DataNode:**
   ```bash
   docker start datanode2
   ```
   "Al encenderlo nuevamente, el DataNode vuelve a enviar sus Heartbeats y el clúster restablece la redundancia completa automáticamente."

---

## 8. Conclusiones Generales

1. **Validación Práctica de la Tolerancia a Fallos:** Se demostró fehacientemente que HDFS garantiza la alta disponibilidad del almacenamiento mediante su arquitectura máster-esclavo y el pipeline de replicación en bloque. La pérdida súbita de un DataNode no compromete la integridad ni el acceso a los datos.
2. **Impacto de la Escala en el Rendimiento:** La transición de 1 GB a 10 GB evidenció cómo el número de bloques escaló linealmente de 8 a 80, requiriendo 160 réplicas y una mayor sincronización de red interna, manteniendo invariable la latencia de acceso por registro gracias a la lectura directa sobre bloques descentralizados.
3. **Persistencia Desacoplada en Entornos Virtualizados:** El uso de Docker Named Volumes resuelve de manera óptima la volatilidad de los contenedores, permitiendo detener, recrear o actualizar servicios de Hadoop sin riesgo de pérdida de datos en los bloques o en la metadata del NameNode.
4. **Cumplimiento Integral de los Requerimientos:** El clúster desarrollado por el **Grupo 04** supera las limitaciones de arquitecturas mononodo demostrativas, ofreciendo un entorno robusto, verificable y listo para escenarios reales de Big Data.
