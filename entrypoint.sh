#!/bin/bash
python -m streamlit run app.py --server.address=0.0.0.0 --server.port=8501 "$@" 2>&1
#exec python -m streamlit run app.py --server.port=8501 --server.address=0.0.0.0
