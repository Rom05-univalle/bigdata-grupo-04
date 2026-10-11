#!/bin/bash
# ==============================================================================
# SCRIPT 04: PRUEBA DE FALLO Y REPLICACIÓN CON DATASET DE 10 GB (LG15)
# Grupo 04: Joel Saavedra, Mauricio Linaja, Rommel Gutierrez
# ==============================================================================

set -e
EVIDENCIAS_DIR="/evidencias"
mkdir -p "$EVIDENCIAS_DIR"
HDFS_PATH="/grupo04/dataset_10GB/dataset_grupo04_10GB.csv"

echo "======================================================================"
echo "  [EXPERIMENTO 2] TOLERANCIA A FALLOS CON DATASET DE 10 GB (GRUPO 04)"
echo "======================================================================"

# Salir de modo seguro si estuviera activo
hdfs dfsadmin -safemode leave

# Paso 1: Verificar si el archivo ya existe en HDFS, si no, generarlo y subirlo
if ! hdfs dfs -test -e "$HDFS_PATH"; then
    echo -e "\n[Paso 1] Generando y cargando dataset masivo de 10 GB a HDFS ($HDFS_PATH)..."
    python3 /scripts/02_generar_datasets.py 10GB | hdfs dfs -D dfs.replication=2 -put - "$HDFS_PATH"
    echo "[OK] Dataset de 10 GB cargado exitosamente con factor de replicación 2."
else
    echo -e "\n[Paso 1] El dataset de 10 GB ya se encuentra en HDFS."
fi

# Paso 2: Registrar Estado Inicial (Antes de la Falla)
echo -e "\n[Paso 2] Registrando ESTADO INICIAL (10 GB)..."
hdfs dfsadmin -report > "$EVIDENCIAS_DIR/exp2_01_dfsadmin_report_inicial.txt"
hdfs fsck "$HDFS_PATH" -files -blocks -locations > "$EVIDENCIAS_DIR/exp2_02_fsck_inicial.txt"

echo "----------------------------------------------------------------------"
echo "  RESUMEN FSCK ESTADO INICIAL (10 GB):"
grep -E "Total size|Total blocks|Total replicas|Number of data-nodes|Status" "$EVIDENCIAS_DIR/exp2_02_fsck_inicial.txt" || true
echo "----------------------------------------------------------------------"

echo -e "\n[Paso 3] Para simular la falla, ejecute en la terminal del host:"
echo "          docker stop datanode3"
echo "Luego ejecute '/scripts/04_verificar_fallo_10GB.sh'"
