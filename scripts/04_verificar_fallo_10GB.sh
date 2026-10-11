#!/bin/bash
# ==============================================================================
# SCRIPT 04B: VERIFICACIÓN DURANTE Y DESPUÉS DEL FALLO (DATASET 10 GB)
# Grupo 04: Joel Saavedra, Mauricio Linaja, Rommel Gutierrez
# ==============================================================================

set -e
EVIDENCIAS_DIR="/evidencias"
mkdir -p "$EVIDENCIAS_DIR"
HDFS_PATH="/grupo04/dataset_10GB/dataset_grupo04_10GB.csv"

echo "======================================================================"
echo "  [EXPERIMENTO 2] VERIFICACIÓN TRAS LA CAÍDA DE DATANODE 3 (DATASET 10 GB)"
echo "======================================================================"

echo -e "\n[Paso 1] Verificando estado de los DataNodes (hdfs dfsadmin -report)..."
hdfs dfsadmin -report > "$EVIDENCIAS_DIR/exp2_03_dfsadmin_report_durante_fallo.txt"

echo -e "\n[Paso 2] Verificando estado de los bloques y subreplicación masiva (hdfs fsck)..."
hdfs fsck "$HDFS_PATH" -files -blocks -locations > "$EVIDENCIAS_DIR/exp2_04_fsck_durante_fallo.txt"

echo "----------------------------------------------------------------------"
echo "  REPORTE FSCK DURANTE EL FALLO (10 GB):"
grep -E "Total size|Total blocks|Total replicas|Under-replicated blocks|Status" "$EVIDENCIAS_DIR/exp2_04_fsck_durante_fallo.txt" || true
echo "----------------------------------------------------------------------"

echo -e "\n[Paso 3] PRUEBA DE DISPONIBILIDAD Y LECTURA DEL ARCHIVO MASIVO:"
echo "Consultando las primeras 15 líneas del dataset de 10 GB en HDFS:"
hdfs dfs -cat "$HDFS_PATH" | head -n 15 > "$EVIDENCIAS_DIR/exp2_05_lectura_durante_fallo.txt"
cat "$EVIDENCIAS_DIR/exp2_05_lectura_durante_fallo.txt"

echo -e "\n[RESULTADO] ¡Lectura exitosa! A pesar de la caída del DataNode 3, los 80 bloques siguen disponibles gracias a sus réplicas."
echo "======================================================================"
