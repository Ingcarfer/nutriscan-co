#!/usr/bin/env bash
# Ejecuta el flujo completo de NutriScan CO, paso a paso.
# Uso (desde la carpeta nutriscan-co, con el entorno activado o no):
#     bash ejecutar_flujo.sh            # todos los pasos
#     bash ejecutar_flujo.sh oro modelos  # solo algunos pasos
#
# Cada paso deja su registro (log) en reports/logs/<paso>.log, con la hora de
# inicio y fin: sirve como evidencia de ejecución.
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH=src
export PYTHONUNBUFFERED=1  # que el log se escriba línea a línea
PY=.venv/bin/python
mkdir -p reports/logs

PASOS=("$@")
[ ${#PASOS[@]} -eq 0 ] && PASOS=(descarga bronce plata oro modelos segmentacion)

for paso in "${PASOS[@]}"; do
  log="reports/logs/${paso}.log"
  echo "=== ${paso}: inicio $(date '+%F %T') ===" | tee "$log"
  $PY -m "nutriscan.${paso}" 2>&1 | grep -vE "WARN|^\s+at |NativeCodeLoader" | tee -a "$log"
  echo "=== ${paso}: fin $(date '+%F %T') ===" | tee -a "$log"
done
