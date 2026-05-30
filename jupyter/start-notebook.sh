#!/bin/bash
set -e
export JUPYTER_CONFIG_DIR="${JUPYTER_CONFIG_DIR:-/home/spark/.jupyter}"
export JUPYTER_TOKEN=""
exec jupyter notebook \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --notebook-dir=/opt/notebooks
