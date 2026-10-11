# ==============================================================================
# SCRIPT MAESTRO DE AUTOMATIZACIÓN - LG15 (GRUPO 04)
# Universidad del Valle - Big Data
# Integrantes: Joel Saavedra, Mauricio Linaja, Rommel Gutierrez
# ==============================================================================

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  INICIANDO EJECUCIÓN AUTOMATIZADA DE LA PRÁCTICA LG15 (GRUPO 04)" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# 1. Levantar Clúster si no está arriba
Write-Host "`n[1/6] Verificando y levantando contenedores de Docker..." -ForegroundColor Yellow
docker compose up -d
Start-Sleep -Seconds 15

# Asegurar desactivación de Safe Mode
docker exec namenode hdfs dfsadmin -safemode leave

# 2. Inicializar carpetas HDFS
Write-Host "`n[2/6] Creando jerarquía /grupo04/ en HDFS..." -ForegroundColor Yellow
docker exec namenode bash /scripts/01_iniciar_estructura_hdfs.sh

# 3. Experimento 1 (Dataset 1 GB) - Estado Inicial
Write-Host "`n[3/6] Ejecutando Experimento 1 (1 GB) - Carga y Estado Inicial..." -ForegroundColor Yellow
docker exec namenode bash /scripts/03_prueba_fallo_1GB.sh

# Simular caída de DataNode 2
Write-Host "`n[3/6] Simulando caída de datanode2 (docker stop datanode2)..." -ForegroundColor Red
docker stop datanode2
Start-Sleep -Seconds 5

# Verificar fallo de 1 GB
Write-Host "`n[3/6] Verificando comportamiento durante la falla (1 GB)..." -ForegroundColor Yellow
docker exec namenode bash /scripts/03_verificar_fallo_1GB.sh

# Restaurar DataNode 2
Write-Host "`n[3/6] Restaurando datanode2 (docker start datanode2)..." -ForegroundColor Green
docker start datanode2
Start-Sleep -Seconds 10

# 4. Experimento 2 (Dataset 10 GB) - Estado Inicial
Write-Host "`n[4/6] Ejecutando Experimento 2 (10 GB) - Carga y Estado Inicial..." -ForegroundColor Yellow
docker exec namenode bash /scripts/04_prueba_fallo_10GB.sh

# Simular caída de DataNode 3
Write-Host "`n[4/6] Simulando caída de datanode3 (docker stop datanode3)..." -ForegroundColor Red
docker stop datanode3
Start-Sleep -Seconds 5

# Verificar fallo de 10 GB
Write-Host "`n[4/6] Verificando comportamiento durante la falla (10 GB)..." -ForegroundColor Yellow
docker exec namenode bash /scripts/04_verificar_fallo_10GB.sh

# Restaurar DataNode 3
Write-Host "`n[4/6] Restaurando datanode3 (docker start datanode3)..." -ForegroundColor Green
docker start datanode3
Start-Sleep -Seconds 10

Write-Host "`n======================================================================" -ForegroundColor Cyan
Write-Host "  ¡TODAS LAS PRUEBAS COMPLETADAS! Evidencias generadas en /evidencias" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Cyan
