#!/bin/bash

# Google Cloud SDK의 Bundled Python 경로
PYTHON_EXEC="/home/bskim/hc/home/bskim/hc/google-cloud-sdk/platform/bundledpythonunix/bin/python3"
SCRIPT_PATH="$(dirname "$0")/run_bq_lib.py"

# 인증 확인 (ADC)
if [ ! -f "$HOME/.config/gcloud/application_default_credentials.json" ]; then
    echo "Warning: Application Default Credentials not found."
    echo "Please run: gcloud auth application-default login --no-browser"
    echo ""
fi

"$PYTHON_EXEC" "$SCRIPT_PATH" "$@"
