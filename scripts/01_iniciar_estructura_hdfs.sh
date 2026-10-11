#!/bin/bash
# ==============================================================================
# SCRIPT 01: INICIALIZACIÓN DE ESTRUCTURA HDFS PARA EL GRUPO 04 (LG15)
# Universidad del Valle - Tecnologías Emergentes (Big Data)
# Integrantes: Joel Saavedra, Mauricio Linaja, Rommel Gutierrez
# ==============================================================================

echo "======================================================================"
echo "  CREANDO JERARQUÍA DE DIRECTORIOS HDFS CON IDENTIFICADOR: grupo04"
echo "======================================================================"

# Salir de modo seguro si estuviera activo
hdfs dfsadmin -safemode leave

# Creación de carpetas oficiales requeridas por la consigna
hdfs dfs -mkdir -p /grupo04/dataset_1GB
hdfs dfs -mkdir -p /grupo04/dataset_10GB
hdfs dfs -mkdir -p /grupo04/resultados
hdfs dfs -mkdir -p /grupo04/evidencias

# Asignar factor de replicación 2 al árbol completo del grupo
hdfs dfs -setrep -R 2 /grupo04

echo -e "\n[OK] Estructura creada con éxito. Listado de /grupo04/:"
hdfs dfs -ls /grupo04/

echo "======================================================================"
