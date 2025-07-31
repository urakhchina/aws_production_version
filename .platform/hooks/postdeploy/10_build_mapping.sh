#!/bin/bash
# Runs after code + venv are ready; rebuilds product_mapping

set -e                                      # abort deploy if anything fails
source /var/app/venv/*/bin/activate         # EB’s Python env

echo "[build_mapping] starting…"            # shows in EB logs
python /var/app/current/build_mapping.py
echo "[build_mapping] done."

exit 0
