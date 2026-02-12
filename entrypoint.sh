# !/bin/bash
# #exec python -m streamlit run app.py --server.port=8501 --server.address=0.0.0.0
# exec python -m streamlit run 3_Home.py --server.address=0.0.0.0 --server.port=8080 "$@" 2>&1


#!/bin/bash
set -e

exec python -m streamlit run 3_Home.py \
  --server.address=0.0.0.0 \
  --server.port=8080 \
  2>&1
