#!/bin/bash

source /opt/conda/etc/profile.d/conda.sh
conda activate fable_env

python -m uvicorn main:app --host 0.0.0.0 --port $PORT