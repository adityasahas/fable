#!/bin/bash
source activate fable_env
exec uvicorn main:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 9999 --log-level info --access-log