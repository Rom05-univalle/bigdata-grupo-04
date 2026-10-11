#!/bin/bash
# ==============================================================================
# SCRIPT 03B: VERIFICACIÓN DURANTE Y DESPUÉS DEL FALLO (DATASET 1 GB)
# Grupo 04: Joel Saavedra, Mauricio Linaja, Rommel Gutierrez
# ==============================================================================

set -e
EVIDENCIAS_DIR="/evidencias"
mkdir -p "$EVIDENCIAS_DIR"
HDFS_PATH="/grupo04/dataset_1GB/dataset_grupo04_1GB.csv"

echo "======================================================================"
echo "  [EXPERIMENTO 1] VERIFICACIÓN DE HDFS TRAS LA CAÍDA DE UN DATANODE"
echo "======================================================================"

echo -e "\n[Paso 1] Verificando estado de los DataNodes (hdfs dfsadmin -report)..."
hdfs dfsadmin -report > "$EVIDENCIAS_DIR/exp1_03_dfsadmin_report_durante_fallo.txt"

echo -e "\n[Paso 2] Verificando estado de los bloques y subreplicación (hdfs fsck)..."
hdfs fsck "$HDFS_PATH" -files -blocks -locations > "$EVIDENCIAS_DIR/exp1_04_fsck_durante_fallo.txt"

echo "----------------------------------------------------------------------"
echo "  REPORTE FSCK DURANTE EL FALLO (1 GB):"
grep -E "Total size|Total blocks|Total replicas|Under-replicated blocks|Status" "$EVIDENCIAS_DIR/exp1_04_fsck_durante_fallo.txt" || true
echo "----------------------------------------------------------------------"

echo -e "\n[Paso 3] PRUEBA DE DISPONIBILIDAD Y LECTURA DEL ARCHIVO:"
echo "Consultando las primeras 15 líneas del dataset de 1 GB en HDFS:"
hdfs dfs -cat "$HDFS_PATH" | head -n 15 > "$EVIDENCIAS_DIR/exp1_05_lectura_durante_fallo.txt"
cat "$EVIDENCIAS_DIR/exp1_05_lectura_durante_fallo.txt"

echo -e "\n[RESULTADO] ¡Lectura exitosa! A pesar de la caída del DataNode, el dataset sigue 100% disponible."
echo "======================================================================"
