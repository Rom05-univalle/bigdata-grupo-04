#!/usr/bin/env python3
"""
Script de generación de Datasets Estructurados para LG15 - Grupo 04
Universidad del Valle - Big Data
Integrantes: Joel Saavedra, Mauricio Linaja, Rommel Gutierrez
"""
import sys
import time
import os

def generate_stream(target_bytes, output_stream):
    header = "id,grupo,integrante,timestamp,sensor_id,temperatura,presion,estado,checksum_data\n".encode('utf-8')
    output_stream.write(header)
    bytes_written = len(header)

    integrantes = ["Joel Saavedra", "Mauricio Linaja", "Rommel Gutierrez"]
    row_template = "{:08d},grupo04,{:16s},{:.2f},SENS_{:04d},{:.2f},{:.2f},OK,GRUPO04_INTEGRITY_CHECK_BLOCK_{:08d}\n"
    
    # Pre-generar un lote de 25,000 registros estructurados (~3.1 MB)
    lines = []
    base_ts = time.time()
    for i in range(25000):
        integ = integrantes[i % 3]
        lines.append(row_template.format(
            i + 1,
            integ,
            base_ts + (i * 0.25),
            i % 500,
            20.0 + (i % 150) * 0.1,
            1013.25 + (i % 50) * 0.05,
            i + 1
        ))
    sample_chunk = "".join(lines).encode('utf-8')
    chunk_len = len(sample_chunk)

    t0 = time.time()
    last_report = t0

    while bytes_written + chunk_len <= target_bytes:
        output_stream.write(sample_chunk)
        bytes_written += chunk_len
        
        if time.time() - last_report > 2.0:
            mb_done = bytes_written / (1024 * 1024)
            mb_total = target_bytes / (1024 * 1024)
            speed = mb_done / (time.time() - t0)
            sys.stderr.write(f"\r[Progreso] {mb_done:.1f} MB / {mb_total:.1f} MB ({mb_done/mb_total*100:.1f}%) a {speed:.1f} MB/s")
            sys.stderr.flush()
            last_report = time.time()

    remaining = target_bytes - bytes_written
    if remaining > 0:
        output_stream.write(sample_chunk[:remaining])
        bytes_written += remaining

    t_total = time.time() - t0
    sys.stderr.write(f"\n[OK] Generados {bytes_written / (1024*1024):.2f} MB en {t_total:.2f} s ({bytes_written/(1024*1024)/(t_total if t_total > 0 else 0.001):.1f} MB/s)\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python 02_generar_datasets.py <1GB | 10GB | bytes> [archivo_salida]")
        sys.exit(1)

    arg = sys.argv[1].upper()
    if arg == "1GB":
        total_bytes = 1 * 1024 * 1024 * 1024 # 1,073,741,824 bytes
    elif arg == "10GB":
        total_bytes = 10 * 1024 * 1024 * 1024 # 10,737,418,240 bytes
    else:
        total_bytes = int(arg)

    if len(sys.argv) >= 3:
        out_path = sys.argv[2]
        with open(out_path, 'wb') as f:
            generate_stream(total_bytes, f)
    else:
        # Enviar directo a stdout (para pipe a hdfs dfs -put -)
        generate_stream(total_bytes, sys.stdout.buffer)
